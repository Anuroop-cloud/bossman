"""
control.scheduler
─────────────────
Periodic leader duties that keep the fleet consistent:

  every 1s    task lease expiry (work of dead/hung workers is retried)
  every 2s    registry refresh (cluster) so the watchdog sees every node's heartbeats
  every 5s    deadlines, lock-lease sweep, probe scheduling, leading-indicator signals
  every 10s   approval expiry (deny by default)
  hourly      retention: old events / audit / idempotency records
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import time
from typing import Any, Awaitable, Callable

log = logging.getLogger(__name__)


class Scheduler:
    def __init__(self, rt: Any) -> None:
        self.rt = rt
        self._task: asyncio.Task | None = None
        self._last: dict[str, float] = {}
        self.runs: dict[str, int] = {}
        self.errors: dict[str, int] = {}
        self.retention_days = float(os.getenv("BOSSMAN_RETENTION_DAYS", "30"))

    async def start(self) -> None:
        if self._task is None:
            self._task = asyncio.create_task(self._loop(), name="bossman:scheduler")

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._task = None

    async def _every(self, name: str, period: float, fn: Callable[[], Awaitable[Any]]) -> None:
        now = time.monotonic()
        if now - self._last.get(name, 0.0) < period:
            return
        self._last[name] = now
        try:
            await fn()
            self.runs[name] = self.runs.get(name, 0) + 1
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001
            self.errors[name] = self.errors.get(name, 0) + 1
            log.exception("scheduler job %s failed", name)

    async def tick(self) -> None:
        rt = self.rt
        await self._every("leases", 1, rt.tasks.expire_leases)
        if hasattr(rt.registry, "refresh"):
            await self._every("registry_refresh", 2, rt.registry.refresh)
        await self._every("deadlines", 5, rt.tasks.enforce_deadlines)
        await self._every("lock_sweep", 5, rt.locks.sweep)
        await self._every("probes", 5, rt.quality.schedule_due)
        await self._every("signals", 5, rt.signals.tick)
        await self._every("approvals", 10, rt.approvals.sweep)
        await self._every("retention", 3600, self._retention)

    async def _retention(self) -> None:
        cutoff = time.time() - self.retention_days * 86400
        if self.rt.event_store is not None:
            await self.rt.event_store.prune(cutoff)
        for coll in ("audit", "idempotency", "grants", "checkpoints"):
            await self.rt.docs.prune(coll, cutoff)

    async def _loop(self) -> None:
        while True:
            await self.tick()
            await asyncio.sleep(0.5)
