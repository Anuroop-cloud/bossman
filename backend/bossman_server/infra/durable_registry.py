"""
infra.durable_registry
──────────────────────
DurableAgentRegistry — AgentRegistry that mirrors every state change to a
CoordinationBackend hash, so a restarted node or a newly elected leader
can `restore()` the workforce instead of starting blind.

Multi-node behaviour
────────────────────
Any node may receive an agent's API calls (load balancer), so:
  * every mutation is written through (`sync`),
  * `get()` reads through to the backend for agents this node has not seen,
  * `refresh()` (called by the leader before each watchdog sweep) merges the
    newest copy of every agent, so the leader sees heartbeats that landed on
    other nodes.

Persistence is best-effort: a backend outage is logged, never raised into
the supervisor.
"""

from __future__ import annotations

import logging

from bossman_server.contracts.agent_state import AgentId, AgentInfo, AgentState
from bossman_server.contracts.events import BossmanEvent, EventType
from bossman_server.core.event_bus import EventBus
from bossman_server.core.registry import AgentRegistry
from bossman_server.infra.backend import CoordinationBackend

log = logging.getLogger(__name__)

REGISTRY_HASH = "bossman:registry"


class DurableAgentRegistry(AgentRegistry):
    def __init__(
        self, bus: EventBus, backend: CoordinationBackend, name: str = REGISTRY_HASH, **kwargs
    ) -> None:
        super().__init__(bus, **kwargs)
        self._backend = backend
        self._hash = name
        # Deregistrations on other nodes arrive as events (refresh is additive only,
        # so a just-registered agent is never dropped before its first persist).
        bus.subscribe(self._on_terminated, EventType.AGENT_TERMINATED)

    async def _on_terminated(self, event: BossmanEvent) -> None:
        if event.agent_id and event.agent_id in self._agents:
            try:
                still_there = await self._backend.hget(self._hash, event.agent_id)
            except Exception:  # noqa: BLE001
                return
            if still_there is None:
                async with self._lock:
                    self._agents.pop(event.agent_id, None)

    async def _persist(self, agent_id: AgentId) -> None:
        state = self._agents.get(agent_id)
        if state is None:
            return
        try:
            await self._backend.hset(self._hash, agent_id, state.model_dump_json())
        except Exception:  # noqa: BLE001
            log.exception("registry persist failed for %s", agent_id)

    async def sync(self, agent_id: AgentId) -> None:
        await self._persist(agent_id)

    async def register(self, info: AgentInfo) -> AgentState:
        state = await super().register(info)
        await self._persist(info.agent_id)
        return state

    async def deregister(self, agent_id: AgentId, reason: str = "normal") -> None:
        if agent_id not in self._agents:
            await self.get(agent_id)  # pull it in so the event carries its identity
        await super().deregister(agent_id, reason)
        try:
            await self._backend.hdel(self._hash, agent_id)
        except Exception:  # noqa: BLE001
            log.exception("registry delete failed for %s", agent_id)

    async def get(self, agent_id: AgentId) -> AgentState | None:
        local = self._agents.get(agent_id)
        if local is not None:
            return local
        try:
            doc = await self._backend.hget(self._hash, agent_id)
        except Exception:  # noqa: BLE001
            return None
        if doc is None:
            return None
        state = AgentState.model_validate_json(doc)
        async with self._lock:
            self._agents.setdefault(agent_id, state)
        return self._agents[agent_id]

    async def _refresh_one(self, agent_id: AgentId) -> None:
        try:
            doc = await self._backend.hget(self._hash, agent_id)
        except Exception:  # noqa: BLE001
            return
        if doc is None:
            return
        remote = AgentState.model_validate_json(doc)
        async with self._lock:
            local = self._agents.get(agent_id)
            if local is None or remote.last_activity > local.last_activity:
                self._agents[agent_id] = remote

    async def record_heartbeat(self, payload):  # noqa: ANN001, ANN201
        # The leader may have changed this agent's status on another node.
        await self._refresh_one(payload.agent_id)
        return await super().record_heartbeat(payload)

    async def refresh(self) -> int:
        """Merge newer copies from the backend; drop agents deleted elsewhere."""
        try:
            raw = await self._backend.hgetall(self._hash)
        except Exception:  # noqa: BLE001
            log.warning("registry refresh failed", exc_info=True)
            return 0
        changed = 0
        async with self._lock:
            for agent_id, doc in raw.items():
                remote = AgentState.model_validate_json(doc)
                local = self._agents.get(agent_id)
                if local is None or remote.last_activity > local.last_activity:
                    self._agents[agent_id] = remote
                    changed += 1
        return changed

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
