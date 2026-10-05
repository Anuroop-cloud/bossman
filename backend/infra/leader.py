"""
infra.leader
────────────
Lease-based leader election on top of CoordinationBackend TTL locks.

Exactly one BOSSman node holds the lease and runs the singleton duties
(watchdog, recovery engine). Others stay hot-standby and take over within
`lease_ms` if the leader dies or stops renewing.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import uuid
from typing import Awaitable, Callable

from infra.backend import CoordinationBackend

log = logging.getLogger(__name__)

LEADER_KEY = "bossman:leader"
LeaderCallback = Callable[[], Awaitable[None]]


class LeaderElector:
    def __init__(
        self,
        backend: CoordinationBackend,
        node_id: str | None = None,
        lease_ms: int = 3000,
        renew_interval_s: float | None = None,
        on_elected: LeaderCallback | None = None,
        on_demoted: LeaderCallback | None = None,
        key: str = LEADER_KEY,
    ) -> None:
        self.backend = backend
        self.node_id = node_id or f"node-{uuid.uuid4().hex[:8]}"
        self.lease_ms = lease_ms
        self.interval = renew_interval_s or lease_ms / 3000
        self.key = key
        self._on_elected = on_elected
        self._on_demoted = on_demoted
        self._is_leader = False
        self._task: asyncio.Task | None = None

    @property
    def is_leader(self) -> bool:
        return self._is_leader

    async def current_leader(self) -> str | None:
        return await self.backend.lock_owner(self.key)

    async def tick(self) -> bool:
        """One election/renewal round. Public so tests can drive it deterministically."""
        if self._is_leader:
            ok = await self.backend.renew_lock(self.key, self.node_id, self.lease_ms)
        else:
            ok = await self.backend.acquire_lock(self.key, self.node_id, self.lease_ms)

        if ok and not self._is_leader:
            self._is_leader = True
            log.info("%s elected leader", self.node_id)
            if self._on_elected:
                await self._on_elected()
        elif not ok and self._is_leader:
            self._is_leader = False
            log.warning("%s lost leadership", self.node_id)
            if self._on_demoted:
                await self._on_demoted()
        return self._is_leader

    async def start(self) -> None:
        if self._task is None:
            self._task = asyncio.create_task(self._loop(), name=f"leader:{self.node_id}")

    async def _loop(self) -> None:
        while True:
            try:
                await self.tick()
            except Exception:  # noqa: BLE001 - backend blip must not kill the loop
                log.exception("leader tick failed")
                if self._is_leader:
                    self._is_leader = False
                    if self._on_demoted:
                        await self._on_demoted()
            await asyncio.sleep(self.interval)

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._task = None
        if self._is_leader:
            await self.backend.release_lock(self.key, self.node_id)
            self._is_leader = False
            if self._on_demoted:
                await self._on_demoted()
