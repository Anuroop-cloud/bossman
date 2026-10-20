"""
core.directives
───────────────
DirectiveQueue — per-agent, at-least-once control channel.

Storage is a CoordinationBackend hash per agent (in-memory for one node,
Redis for a cluster), so a directive issued by the leader is delivered by
whichever node receives that agent's next heartbeat. A pub/sub notification
wakes long-polling agents on every node immediately.
"""

from __future__ import annotations

import asyncio
import logging
from collections import deque
from datetime import datetime, timedelta, timezone
from typing import Any

from bossman_server.contracts.agent_state import AgentId
from bossman_server.contracts.directives import (
    DEFAULT_DIRECTIVE_TTL_S,
    Directive,
    DirectiveAck,
    DirectiveStatus,
    DirectiveType,
)
from bossman_server.contracts.events import BossmanEvent, EventType
from bossman_server.core.event_bus import EventBus
from bossman_server.infra.backend import CoordinationBackend, Unsubscribe

log = logging.getLogger(__name__)

NOTIFY_CHANNEL = "bossman:directive-notify"
REDELIVER_AFTER_S = 15.0
HISTORY = 500


def _key(agent_id: str) -> str:
    return f"bossman:directives:{agent_id}"


class DirectiveQueue:
    def __init__(
        self,
        backend: CoordinationBackend,
        bus: EventBus,
        *,
        redeliver_after_s: float = REDELIVER_AFTER_S,
    ) -> None:
        self._backend = backend
        self._bus = bus
        self._redeliver = redeliver_after_s
        self._waiters: dict[str, asyncio.Event] = {}
        self._unsub: Unsubscribe | None = None
        self.history: deque[Directive] = deque(maxlen=HISTORY)
        self.issued_total = 0

    async def start(self) -> None:
        if self._unsub is None:
            self._unsub = await self._backend.subscribe(NOTIFY_CHANNEL, self._on_notify)

    async def stop(self) -> None:
        if self._unsub:
            await self._unsub()
            self._unsub = None

    async def _on_notify(self, agent_id: str) -> None:
        ev = self._waiters.get(agent_id)
        if ev:
            ev.set()

    # ── Issue ────────────────────────────────────────────────────────────────

    async def issue(
        self,
        agent_id: AgentId,
        dtype: DirectiveType,
        params: dict[str, Any] | None = None,
        *,
        reason: str = "",
        incident_id: str | None = None,
        issued_by: str = "bossman",
        ttl_s: float = DEFAULT_DIRECTIVE_TTL_S,
        namespace: str | None = None,
    ) -> Directive:
        params = params or {}
        # Coalesce identical outstanding directives (e.g. repeated COMPACT).
        for existing in await self.outstanding(agent_id):
            if existing.type == dtype and existing.params == params:
                return existing
        now = datetime.now(timezone.utc)
        d = Directive(
            agent_id=agent_id, type=dtype, params=params, reason=reason[:500],
            incident_id=incident_id, issued_by=issued_by,
            created_at=now, expires_at=now + timedelta(seconds=ttl_s),
        )
        await self._backend.hset(_key(agent_id), d.directive_id, d.model_dump_json())
        self.issued_total += 1
        await self._bus.publish(BossmanEvent.create(
            EventType.DIRECTIVE_ISSUED,
            message=f"Directive {dtype.value} → {agent_id}" + (f": {reason[:120]}" if reason else ""),
            agent_id=agent_id, namespace=namespace,
            payload={"directive_id": d.directive_id, "type": dtype.value, "params": params,
                     "incident_id": incident_id, "issued_by": issued_by},
        ))
        await self._notify(agent_id)
        return d

    async def _notify(self, agent_id: str) -> None:
        ev = self._waiters.get(agent_id)
        if ev:
            ev.set()
        try:
            await self._backend.publish(NOTIFY_CHANNEL, agent_id)
        except Exception:  # noqa: BLE001 - notification is an optimisation only
            log.debug("directive notify failed", exc_info=True)

    # ── Read / deliver ───────────────────────────────────────────────────────

    async def outstanding(self, agent_id: AgentId) -> list[Directive]:
        raw = await self._backend.hgetall(_key(agent_id))
        items = [Directive.model_validate_json(v) for v in raw.values()]
        return sorted(items, key=lambda d: d.created_at)

    async def deliver(self, agent_id: AgentId) -> list[Directive]:
        """Directives due for (re)delivery; marks them DELIVERED."""
        now = datetime.now(timezone.utc)
        out: list[Directive] = []
        for d in await self.outstanding(agent_id):
            if d.expired:
                await self._backend.hdel(_key(agent_id), d.directive_id)
                d.status = DirectiveStatus.EXPIRED
                self.history.append(d)
                await self._bus.publish(BossmanEvent.create(
                    EventType.DIRECTIVE_EXPIRED, agent_id=agent_id,
                    message=f"Directive {d.type.value} expired un-acked",
                    payload={"directive_id": d.directive_id, "type": d.type.value},
                ))
                continue
            due = d.status == DirectiveStatus.PENDING or (
                d.delivered_at is not None
                and (now - d.delivered_at).total_seconds() >= self._redeliver
            )
            if due:
                d.status = DirectiveStatus.DELIVERED
                d.delivered_at = now
                d.deliveries += 1
                await self._backend.hset(_key(agent_id), d.directive_id, d.model_dump_json())
                out.append(d)
        return out

    async def wait(self, agent_id: AgentId, timeout: float) -> list[Directive]:
        """Long-poll: return due directives, waiting up to `timeout` for one."""
        due = await self.deliver(agent_id)
        if due or timeout <= 0:
            return due
        ev = self._waiters.setdefault(agent_id, asyncio.Event())
        ev.clear()
        try:
            await asyncio.wait_for(ev.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            return []
        return await self.deliver(agent_id)

    # ── Ack ──────────────────────────────────────────────────────────────────

    async def ack(self, agent_id: AgentId, ack: DirectiveAck) -> Directive | None:
        raw = await self._backend.hget(_key(agent_id), ack.directive_id)
        if raw is None:
            return None  # already acked / expired — idempotent
        d = Directive.model_validate_json(raw)
        d.status = DirectiveStatus.ACKED if ack.ok else DirectiveStatus.FAILED
        d.acked_at = datetime.now(timezone.utc)
        d.result = ack.result
        await self._backend.hdel(_key(agent_id), ack.directive_id)
        self.history.append(d)
        await self._bus.publish(BossmanEvent.create(
            EventType.DIRECTIVE_ACKED, agent_id=agent_id,
            message=f"{d.type.value} {'acknowledged' if ack.ok else 'FAILED'} by {agent_id}",
            payload={"directive_id": d.directive_id, "type": d.type.value, "ok": ack.ok,
                     "result": ack.result or {}, "incident_id": d.incident_id},
        ))
        return d

    async def clear(self, agent_id: AgentId) -> None:
        for d in await self.outstanding(agent_id):
            await self._backend.hdel(_key(agent_id), d.directive_id)

    def recent(self, agent_id: AgentId | None = None, limit: int = 50) -> list[Directive]:
        items = [d for d in reversed(self.history) if agent_id is None or d.agent_id == agent_id]
        return items[:limit]
