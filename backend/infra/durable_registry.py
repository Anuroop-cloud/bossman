"""
infra.durable_registry
──────────────────────
DurableAgentRegistry — AgentRegistry that mirrors every state change to a
CoordinationBackend hash, so a restarted node or a newly elected leader
can `restore()` the workforce instead of starting blind.

Behaviour of the base class is untouched; persistence is best-effort
(a backend outage is logged, never raised into the supervisor).
"""

from __future__ import annotations

import logging

from contracts.agent_state import AgentId, AgentInfo, AgentState
from contracts.heartbeat import HeartbeatPayload
from core.event_bus import EventBus
from core.registry import AgentRegistry
from infra.backend import CoordinationBackend

log = logging.getLogger(__name__)

REGISTRY_HASH = "bossman:registry"


class DurableAgentRegistry(AgentRegistry):
    def __init__(
        self, bus: EventBus, backend: CoordinationBackend, name: str = REGISTRY_HASH
    ) -> None:
        super().__init__(bus)
        self._backend = backend
        self._hash = name

    async def _persist(self, agent_id: AgentId) -> None:
        state = self._agents.get(agent_id)
        if state is None:
            return
        try:
            await self._backend.hset(self._hash, agent_id, state.model_dump_json())
        except Exception:  # noqa: BLE001
            log.exception("registry persist failed for %s", agent_id)

    async def register(self, info: AgentInfo) -> AgentState:
        state = await super().register(info)
        await self._persist(info.agent_id)
        return state

    async def mark_ready(self, agent_id: AgentId) -> AgentState:
        state = await super().mark_ready(agent_id)
        await self._persist(agent_id)
        return state

    async def deregister(self, agent_id: AgentId, reason: str = "normal") -> None:
        await super().deregister(agent_id, reason)
        try:
            await self._backend.hdel(self._hash, agent_id)
        except Exception:  # noqa: BLE001
            log.exception("registry delete failed for %s", agent_id)

    async def record_heartbeat(self, payload: HeartbeatPayload):  # noqa: ANN201
        ack = await super().record_heartbeat(payload)
        await self._persist(payload.agent_id)
        return ack

    async def sync(self, agent_id: AgentId) -> None:
        """Call after mutating a state object directly (status changes etc.)."""
        await self._persist(agent_id)

    async def restore(self) -> int:
        """Load persisted agents (e.g. on leader takeover). Returns count restored."""
        raw = await self._backend.hgetall(self._hash)
        restored = 0
        async with self._lock:
            for agent_id, doc in raw.items():
                if agent_id in self._agents:
                    continue
                self._agents[agent_id] = AgentState.model_validate_json(doc)
                restored += 1
        log.info("Restored %d agent(s) from durable registry", restored)
        return restored
