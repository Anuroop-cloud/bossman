"""
control.locks
─────────────
LockService — leased, cluster-wide named locks with deadlock detection.

Why leases
──────────
An in-process asyncio.Lock dies with its process but *stays held* if the
holder hangs. Every lock here is a TTL lease on the coordination backend:
a crashed or hung agent stops renewing and the lock frees itself
(zylos.md §Part9 — "Bound everything").

Deadlock handling (zylos.md §Part3)
────────────────────────────────────
  * Prevention — a multi-resource request is acquired in one global order
    (sorted names) and all-or-nothing, so multi-acquirers cannot form a cycle.
  * Detection — while an agent waits, BOSSman walks the agent→agent wait-for
    graph. If the requester closes a cycle it is refused immediately with
    DeadlockError (DEADLOCK_SUSPECTED is published so the recovery engine
    can release the cycle) instead of waiting for a timeout.
  * Bounded waits — every acquire has a timeout.
"""

from __future__ import annotations

import asyncio
import json
import logging
import random
import re
import time
import uuid
from dataclasses import asdict, dataclass, field
from typing import Any

from bossman_server.contracts.events import BossmanEvent, EventType
from bossman_server.core.event_bus import EventBus
from bossman_server.infra.backend import CoordinationBackend, Unsubscribe

log = logging.getLogger(__name__)

LEASES = "bossman:lock-leases"
WAITS = "bossman:lock-waits"
RELEASED_CHANNEL = "bossman:lock-released"
RESOURCE_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/-]{0,127}$")
MAX_TTL_S = 3600.0


class LockError(Exception):
    code = "lock_error"


class DeadlockError(LockError):
    code = "deadlock"

    def __init__(self, cycle: list[str], wanted: str) -> None:
        super().__init__(f"Acquiring {wanted!r} would deadlock: wait-for cycle {' → '.join(cycle)}")
        self.cycle = cycle
        self.wanted = wanted


class LockTimeout(LockError):
    code = "lock_timeout"

    def __init__(self, resources: list[str], holders: dict[str, str | None], waited: float) -> None:
        super().__init__(f"Timed out after {waited:.1f}s waiting for {resources} (holders: {holders})")
        self.resources = resources
        self.holders = holders


class LeaseLost(LockError):
    code = "lease_lost"


@dataclass
class LockLease:
    lease_id: str
    agent_id: str
    namespace: str
    resources: list[str]
    ttl_s: float
    acquired_at: float = field(default_factory=time.time)
    expires_at: float = 0.0
    task_id: str | None = None

    def to_json(self) -> str:
        return json.dumps(asdict(self))

    @classmethod
    def from_json(cls, raw: str) -> LockLease:
        return cls(**json.loads(raw))


def _rkey(namespace: str, resource: str) -> str:
    return f"bossman:lock:{namespace}:{resource}"


class LockService:
    def __init__(self, backend: CoordinationBackend, bus: EventBus, registry: Any | None = None) -> None:
        self._backend = backend
        self._bus = bus
        self._registry = registry
        self._released = asyncio.Event()
        self._unsub: Unsubscribe | None = None
        self.stats = {"acquired": 0, "contended": 0, "timeouts": 0, "deadlocks": 0, "revoked": 0}

    async def start(self) -> None:
        if self._unsub is None:
            self._unsub = await self._backend.subscribe(RELEASED_CHANNEL, self._on_released)

    async def stop(self) -> None:
        if self._unsub:
            await self._unsub()
            self._unsub = None

    async def _on_released(self, _msg: str) -> None:
        self._released.set()

    async def _notify_released(self) -> None:
        self._released.set()
        try:
            await self._backend.publish(RELEASED_CHANNEL, "1")
        except Exception:  # noqa: BLE001
            pass

    # ── Acquire ──────────────────────────────────────────────────────────────

    async def acquire(
        self,
        agent_id: str,
        resources: list[str],
        *,
        namespace: str = "default",
        ttl_s: float = 30.0,
        wait_s: float = 10.0,
        task_id: str | None = None,
    ) -> LockLease:
        res = sorted(set(resources))
        if not res:
            raise ValueError("at least one resource is required")
        for r in res:
            if not RESOURCE_NAME.match(r):
                raise ValueError(f"invalid resource name {r!r}")
        ttl_s = max(1.0, min(ttl_s, MAX_TTL_S))
        lease = LockLease(lease_id=f"lk-{uuid.uuid4().hex[:16]}", agent_id=agent_id,
                          namespace=namespace, resources=res, ttl_s=ttl_s, task_id=task_id)
        loop = asyncio.get_running_loop()
        started = loop.time()
        deadline = started + max(0.0, wait_s)
        reported = False
        backoff = 0.05
        try:
            while True:
                got: list[str] = []
                blocked_on: str | None = None
                for r in res:  # global order → no cycles among multi-acquirers
                    if await self._backend.acquire_lock(_rkey(namespace, r), lease.lease_id, int(ttl_s * 1000)):
                        got.append(r)
                    else:
                        blocked_on = r
                        break
                if blocked_on is None:
                    lease.acquired_at = time.time()
                    lease.expires_at = lease.acquired_at + ttl_s
                    await self._backend.hset(LEASES, lease.lease_id, lease.to_json())
                    await self._backend.hdel(WAITS, agent_id)
                    await self._sync_agent(agent_id)
                    self.stats["acquired"] += 1
                    await self._emit(EventType.RESOURCE_ACQUIRED, agent_id, namespace, res,
                                     {"lease_id": lease.lease_id, "ttl_s": ttl_s,
                                      "waited_seconds": round(loop.time() - started, 3)})
                    return lease
                for r in got:  # all-or-nothing
                    await self._backend.release_lock(_rkey(namespace, r), lease.lease_id)

                await self._backend.hset(WAITS, agent_id, json.dumps(
                    {"namespace": namespace, "resources": res, "since": time.time()}))
                holder = await self._holder_agent(namespace, blocked_on)
                if not reported:
                    reported = True
                    self.stats["contended"] += 1
                    await self._sync_agent(agent_id)
                    await self._emit(EventType.RESOURCE_CONTENTION, agent_id, namespace, [blocked_on],
                                     {"holder": holder, "wanted": res})
                cycle = await self._find_cycle(agent_id)
                if cycle:
                    self.stats["deadlocks"] += 1
                    held = sorted({r for l in await self._leases_of(agent_id) for r in l.resources})
                    holder_waits = await self._waiting_for(holder) if holder else []
                    await self._emit(EventType.DEADLOCK_SUSPECTED, agent_id, namespace, [blocked_on], {
                        "requester": agent_id,
                        "requester_holds": held,
                        "wanted_resource": blocked_on,
                        "current_holder": holder,
                        "holder_waits_for": holder_waits,
                        "cycle_resources": sorted(set(held) & set(holder_waits)) or held,
                        "cycle": cycle,
                        "resources_held": held,
                        "resources_waiting": res,
                    })
                    raise DeadlockError(cycle, blocked_on)
                remaining = deadline - loop.time()
                if remaining <= 0:
                    self.stats["timeouts"] += 1
                    holders = {r: await self._holder_agent(namespace, r) for r in res}
                    await self._emit(EventType.RESOURCE_TIMEOUT, agent_id, namespace, res,
                                     {"waited_seconds": round(loop.time() - started, 3), "holders": holders,
                                      "timeout_configured": wait_s})
                    raise LockTimeout(res, holders, loop.time() - started)
                self._released.clear()
                try:
                    await asyncio.wait_for(self._released.wait(),
                                           timeout=min(remaining, random.uniform(0, backoff) + 0.02))
                except asyncio.TimeoutError:
                    pass
                backoff = min(backoff * 2, 1.0)
        except BaseException:
            await self._backend.hdel(WAITS, agent_id)
            await self._sync_agent(agent_id)
            raise

    # ── Renew / release / revoke ─────────────────────────────────────────────

    async def get(self, lease_id: str) -> LockLease | None:
        raw = await self._backend.hget(LEASES, lease_id)
        return LockLease.from_json(raw) if raw else None

    async def renew(self, lease_id: str, ttl_s: float | None = None) -> LockLease:
        lease = await self.get(lease_id)
        if lease is None:
            raise LeaseLost(f"lease {lease_id} not found (expired or revoked)")
        ttl = max(1.0, min(ttl_s or lease.ttl_s, MAX_TTL_S))
        for r in lease.resources:
            if not await self._backend.renew_lock(_rkey(lease.namespace, r), lease_id, int(ttl * 1000)):
                await self._drop(lease, reason="lost")
                raise LeaseLost(f"lease {lease_id} lost resource {r!r}")
        lease.ttl_s = ttl
        lease.expires_at = time.time() + ttl
        await self._backend.hset(LEASES, lease_id, lease.to_json())
        return lease

    async def release(self, lease_id: str) -> bool:
        lease = await self.get(lease_id)
        if lease is None:
            return False
        await self._drop(lease, reason="released")
        return True

    async def revoke_agent(self, agent_id: str, reason: str = "revoked by BOSSman") -> list[str]:
        """Force-release every lease the agent holds (RELEASE_RESOURCES)."""
        freed: list[str] = []
        for lease in await self._leases_of(agent_id):
            await self._drop(lease, reason=reason, revoked=True)
            freed.extend(lease.resources)
        await self._backend.hdel(WAITS, agent_id)
        await self._sync_agent(agent_id)
        return freed

    async def _drop(self, lease: LockLease, *, reason: str, revoked: bool = False) -> None:
        for r in lease.resources:
            await self._backend.release_lock(_rkey(lease.namespace, r), lease.lease_id)
        await self._backend.hdel(LEASES, lease.lease_id)
        await self._sync_agent(lease.agent_id)
        if revoked:
            self.stats["revoked"] += 1
        await self._emit(
            EventType.RESOURCE_REVOKED if revoked else EventType.RESOURCE_RELEASED,
            lease.agent_id, lease.namespace, lease.resources,
            {"lease_id": lease.lease_id, "reason": reason,
             "held_seconds": round(time.time() - lease.acquired_at, 3)},
        )
        await self._notify_released()

    async def sweep(self) -> int:
        """Drop lease records whose TTL lapsed (the backend already freed the keys)."""
        dropped = 0
        now = time.time()
        for lease in await self.leases():
            if lease.expires_at and lease.expires_at < now:
                owned = [await self._backend.lock_owner(_rkey(lease.namespace, r)) for r in lease.resources]
                if all(o != lease.lease_id for o in owned):
                    await self._drop(lease, reason="expired")
                    dropped += 1
        return dropped

    # ── Graph / snapshot ─────────────────────────────────────────────────────

    async def leases(self) -> list[LockLease]:
        return [LockLease.from_json(v) for v in (await self._backend.hgetall(LEASES)).values()]

    async def _leases_of(self, agent_id: str) -> list[LockLease]:
        return [l for l in await self.leases() if l.agent_id == agent_id]

    async def _waiting_for(self, agent_id: str | None) -> list[str]:
        if not agent_id:
            return []
        raw = await self._backend.hget(WAITS, agent_id)
        return json.loads(raw)["resources"] if raw else []

    async def _holder_agent(self, namespace: str, resource: str | None) -> str | None:
        if resource is None:
            return None
        owner = await self._backend.lock_owner(_rkey(namespace, resource))
        if owner is None:
            return None
        lease = await self.get(owner)
        return lease.agent_id if lease else None

    async def _find_cycle(self, start: str) -> list[str] | None:
        waits = {a: json.loads(v) for a, v in (await self._backend.hgetall(WAITS)).items()}
        holders: dict[tuple[str, str], str] = {}
        for lease in await self.leases():
            for r in lease.resources:
                holders[(lease.namespace, r)] = lease.agent_id
        graph: dict[str, set[str]] = {}
        for agent, w in waits.items():
            for r in w["resources"]:
                h = holders.get((w["namespace"], r))
                if h is not None:
                    graph.setdefault(agent, set()).add(h)
        # DFS for a path start → … → start
        stack: list[tuple[str, list[str]]] = [(start, [start])]
        seen: set[str] = set()
        while stack:
            node, path = stack.pop()
            for nxt in graph.get(node, ()):
                if nxt == start:
                    return path + [start]
                if nxt not in seen:
                    seen.add(nxt)
                    stack.append((nxt, path + [nxt]))
        return None

    async def snapshot(self, namespace: str | None = None) -> dict[str, Any]:
        leases = [l for l in await self.leases() if namespace is None or l.namespace == namespace]
        waits = [
            {"agent_id": a, **json.loads(v)}
            for a, v in (await self._backend.hgetall(WAITS)).items()
        ]
        waits = [w for w in waits if namespace is None or w["namespace"] == namespace]
        now = time.time()
        return {
            "leases": [{**asdict(l), "remaining_s": round(max(0.0, l.expires_at - now), 1)} for l in leases],
            "waits": waits,
            "stats": dict(self.stats),
        }

    async def _sync_agent(self, agent_id: str) -> None:
        """Mirror lease state onto AgentState so health checks and the graph see it."""
        if self._registry is None:
            return
        state = await self._registry.get(agent_id)
        if state is None:
            return
        held = sorted({r for l in await self._leases_of(agent_id) for r in l.resources})
        waiting = [r for r in await self._waiting_for(agent_id) if r not in held]
        if state.resource_usage.resources_held != held or state.resource_usage.resources_waiting != waiting:
            state.resource_usage.resources_held = held
            state.resource_usage.resources_waiting = waiting
            await self._registry.sync(agent_id)

    async def _emit(self, et: EventType, agent_id: str, namespace: str, resources: list[str],
                    payload: dict[str, Any]) -> None:
        await self._bus.publish(BossmanEvent.create(
            et, agent_id=agent_id, namespace=namespace,
            resource_name=",".join(resources)[:200],
            message=f"{et.value.replace('_', ' ').title()}: {', '.join(resources)} ({agent_id})",
            payload=payload,
        ))
