"""
control.circuits
────────────────
CircuitService — circuit breakers shared by the whole fleet.

The in-process recovery.circuit_breaker protects calls made inside the
BOSSman process. Remote agents call their own dependencies (LLM APIs, tools,
databases); when the recovery engine opens a breaker for "openai" every agent
in the fleet must fail fast on it, not just one. State lives on the
coordination backend; agents check `allow` before calling and `report` after.

States: CLOSED → (failures ≥ threshold) → OPEN → (timeout) → HALF_OPEN
        → (successes ≥ threshold) → CLOSED   |   HALF_OPEN failure → OPEN
"""

from __future__ import annotations

import asyncio
import json
import logging
import random
import re
import time
import uuid
from typing import Any

from bossman_server.contracts.events import BossmanEvent, EventType
from bossman_server.core.event_bus import EventBus
from bossman_server.infra.backend import CoordinationBackend

log = logging.getLogger(__name__)

HASH = "bossman:circuits"
KEY = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/-]{0,127}$")
PROBE_SLOT_S = 30.0


class CircuitService:
    def __init__(
        self,
        backend: CoordinationBackend,
        bus: EventBus,
        *,
        failure_threshold: int = 5,
        success_threshold: int = 2,
        open_timeout_s: float = 60.0,
    ) -> None:
        self._backend = backend
        self._bus = bus
        self._failure_threshold = failure_threshold
        self._success_threshold = success_threshold
        self._open_timeout = open_timeout_s

    def _fresh(self, key: str) -> dict[str, Any]:
        return {"key": key, "state": "CLOSED", "failures": 0, "successes": 0, "opened_at": None,
                "open_timeout_s": self._open_timeout, "probe_until": 0.0, "last_error": None,
                "opened_total": 0, "reason": None}

    async def _load(self, key: str) -> dict[str, Any]:
        raw = await self._backend.hget(HASH, key)
        return json.loads(raw) if raw else self._fresh(key)

    async def _save(self, c: dict[str, Any]) -> None:
        await self._backend.hset(HASH, c["key"], json.dumps(c))

    async def _locked(self, key: str, fn):  # noqa: ANN001, ANN202
        if not KEY.match(key):
            raise ValueError(f"invalid circuit key {key!r}")
        mkey = f"bossman:circuit:{key}:mutex"
        owner = uuid.uuid4().hex
        for _ in range(200):
            if await self._backend.acquire_lock(mkey, owner, 2000):
                try:
                    return await fn()
                finally:
                    await self._backend.release_lock(mkey, owner)
            await asyncio.sleep(random.uniform(0.001, 0.01))
        raise RuntimeError(f"circuit {key!r} mutex contention")

    @staticmethod
    def _view(c: dict[str, Any]) -> dict[str, Any]:
        retry_after = 0.0
        if c["state"] == "OPEN" and c["opened_at"]:
            retry_after = max(0.0, c["opened_at"] + c["open_timeout_s"] - time.time())
        return {"key": c["key"], "state": c["state"], "failures": c["failures"],
                "retry_after_s": round(retry_after, 2), "last_error": c["last_error"],
                "opened_total": c["opened_total"], "reason": c.get("reason")}

    async def status(self, key: str) -> dict[str, Any]:
        c = await self._load(key)
        if c["state"] == "OPEN" and c["opened_at"] and time.time() - c["opened_at"] >= c["open_timeout_s"]:
            c = await self._locked(key, lambda: self._to_half_open(key))
        return self._view(c)

    async def _to_half_open(self, key: str) -> dict[str, Any]:
        c = await self._load(key)
        if c["state"] == "OPEN" and c["opened_at"] and time.time() - c["opened_at"] >= c["open_timeout_s"]:
            c["state"] = "HALF_OPEN"
            c["successes"] = 0
            await self._save(c)
            await self._emit(EventType.CIRCUIT_BREAKER_HALF_OPEN, c)
        return c

    async def allow(self, key: str) -> dict[str, Any]:
        """
        Should a caller attempt the dependency now? HALF_OPEN admits one probe
        at a time (a probe slot expires after PROBE_SLOT_S if never reported).
        """
        async def fn() -> dict[str, Any]:
            c = await self._load(key)
            now = time.time()
            if c["state"] == "OPEN" and c["opened_at"] and now - c["opened_at"] >= c["open_timeout_s"]:
                c["state"] = "HALF_OPEN"
                c["successes"] = 0
                await self._emit(EventType.CIRCUIT_BREAKER_HALF_OPEN, c)
            allowed = c["state"] == "CLOSED"
            if c["state"] == "HALF_OPEN" and c["probe_until"] < now:
                c["probe_until"] = now + PROBE_SLOT_S
                allowed = True
            await self._save(c)
            return {**self._view(c), "allowed": allowed}

        return await self._locked(key, fn)

    async def report(self, key: str, ok: bool, *, error: str | None = None,
                     agent_id: str | None = None) -> dict[str, Any]:
        async def fn() -> dict[str, Any]:
            c = await self._load(key)
            if ok:
                if c["state"] == "HALF_OPEN":
                    c["successes"] += 1
                    c["probe_until"] = 0.0
                    if c["successes"] >= self._success_threshold:
                        c.update(state="CLOSED", failures=0, successes=0, opened_at=None, reason=None)
                        await self._emit(EventType.CIRCUIT_BREAKER_CLOSED, c, agent_id)
                elif c["state"] == "CLOSED":
                    c["failures"] = 0
            else:
                c["failures"] += 1
                c["last_error"] = (error or "")[:300] or None
                if c["state"] == "HALF_OPEN" or (
                    c["state"] == "CLOSED" and c["failures"] >= self._failure_threshold
                ):
                    self._open(c, reason=f"{c['failures']} failures" if c["state"] == "CLOSED" else "probe failed")
                    await self._emit(EventType.CIRCUIT_BREAKER_OPENED, c, agent_id)
            await self._save(c)
            return self._view(c)

        return await self._locked(key, fn)

    def _open(self, c: dict[str, Any], reason: str, timeout_s: float | None = None) -> None:
        c.update(state="OPEN", opened_at=time.time(), successes=0, probe_until=0.0, reason=reason)
        c["opened_total"] += 1
        if timeout_s is not None:
            c["open_timeout_s"] = timeout_s

    async def force_open(self, key: str, *, reason: str = "opened by BOSSman",
                         open_timeout_s: float | None = None, agent_id: str | None = None) -> dict[str, Any]:
        async def fn() -> dict[str, Any]:
            c = await self._load(key)
            self._open(c, reason, open_timeout_s)
            await self._save(c)
            await self._emit(EventType.CIRCUIT_BREAKER_OPENED, c, agent_id)
            return self._view(c)

        return await self._locked(key, fn)

    async def force_close(self, key: str) -> dict[str, Any]:
        async def fn() -> dict[str, Any]:
            c = await self._load(key)
            c.update(state="CLOSED", failures=0, successes=0, opened_at=None, probe_until=0.0, reason=None)
            await self._save(c)
            await self._emit(EventType.CIRCUIT_BREAKER_CLOSED, c)
            return self._view(c)

        return await self._locked(key, fn)

    async def snapshot(self) -> list[dict[str, Any]]:
        items = [json.loads(v) for v in (await self._backend.hgetall(HASH)).values()]
        return [self._view(c) for c in sorted(items, key=lambda c: c["key"])]

    async def _emit(self, et: EventType, c: dict[str, Any], agent_id: str | None = None) -> None:
        await self._bus.publish(BossmanEvent.create(
            et, agent_id=agent_id, resource_name=c["key"],
            message=f"Circuit {c['key']!r} → {c['state']}" + (f" ({c['reason']})" if c.get("reason") else ""),
            payload={"circuit_key": c["key"], "state": c["state"], "failures": c["failures"],
                     "total_opened": c["opened_total"], "shared": True},
        ))
