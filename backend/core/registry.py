"""
core.registry
─────────────
AgentRegistry — the authoritative in-process store of all known agents.

BOSSman's single source of truth for:
  - Which agents exist
  - What state each agent is in
  - Whether an agent is alive / healthy / suspect
  - Querying the workforce by status, role, or permission tier

The registry holds AgentState objects in memory and publishes lifecycle
events to the EventBus on every state change. A Postgres persistence layer
(Phase 2+) will mirror these to the database for durability.

Thread/async safety
───────────────────
All public methods are async and protected by a single asyncio.Lock.
BOSSman runs as a single-process asyncio app in Phase 1, so this is
sufficient. Phase 3 introduces distributed locking via Redis.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Callable

from contracts.agent_state import AgentId, AgentInfo, AgentState, AgentStatus
from contracts.events import BossmanEvent, EventType
from contracts.heartbeat import HeartbeatAck, HeartbeatPayload
from core.event_bus import EventBus

log = logging.getLogger(__name__)


class AgentRegistry:
    """
    Register and manage the agent workforce.

    Lifecycle
    ─────────
        registry.register(info)         → AgentState (INITIALIZING)
        registry.mark_ready(agent_id)   → AgentState (IDLE)
        registry.deregister(agent_id)   → removes from registry

    Heartbeats
    ──────────
        registry.record_heartbeat(payload) → HeartbeatAck
    """

    def __init__(self, bus: EventBus) -> None:
        self._bus = bus
        self._agents: dict[AgentId, AgentState] = {}
        self._lock = asyncio.Lock()

    # ── Registration ──────────────────────────────────────────────────────────

    async def register(self, info: AgentInfo) -> AgentState:
        """
        Register a new agent. Returns the initial AgentState (INITIALIZING).
        Raises ValueError if agent_id already exists.
        """
        async with self._lock:
            if info.agent_id in self._agents:
                raise ValueError(f"Agent {info.agent_id!r} is already registered.")

            state = AgentState(info=info)
            self._agents[info.agent_id] = state
            log.info("Registered agent %s (%s / %s)", info.agent_id, info.name, info.role)

        await self._bus.publish(
            BossmanEvent.create(
                EventType.AGENT_REGISTERED,
                message=f"{info.name} ({info.role}) registered",
                agent_id=info.agent_id,
                payload={
                    "name": info.name,
                    "role": info.role,
                    "permission_tier": info.permission_tier.value,
                },
            )
        )
        return state

    async def mark_ready(self, agent_id: AgentId) -> AgentState:
        """Transition agent from INITIALIZING → IDLE (ready to accept tasks)."""
        state = await self._get_or_raise(agent_id)
        async with self._lock:
            state.transition_to(AgentStatus.IDLE)

        await self._bus.publish(
            BossmanEvent.create(
                EventType.AGENT_STARTED,
                message=f"{state.info.name} is ready",
                agent_id=agent_id,
            )
        )
        return state

    async def deregister(self, agent_id: AgentId, reason: str = "normal") -> None:
        """Remove an agent from the registry and emit AGENT_TERMINATED."""
        async with self._lock:
            state = self._agents.pop(agent_id, None)

        if state is None:
            log.warning("deregister called for unknown agent %s", agent_id)
            return

        log.info("Deregistered agent %s (reason: %s)", agent_id, reason)
        await self._bus.publish(
            BossmanEvent.create(
                EventType.AGENT_TERMINATED,
                message=f"{state.info.name} terminated ({reason})",
                agent_id=agent_id,
                payload={"reason": reason},
            )
        )

    # ── Heartbeats ────────────────────────────────────────────────────────────

    async def record_heartbeat(self, payload: HeartbeatPayload) -> HeartbeatAck:
        """
        Process an agent heartbeat. Updates AgentState and returns an Ack
        with any directives BOSSman wants to push back to the agent.
        """
        state = await self._get_or_raise(payload.agent_id)

        async with self._lock:
            state.record_heartbeat()
            if payload.current_step is not None:
                state.current_step = payload.current_step
            if payload.current_task_id is not None:
                state.current_task_id = payload.current_task_id
            state.resource_usage.tokens_used = payload.tokens_used
            state.resource_usage.context_utilisation = payload.context_utilisation
            state.resource_usage.api_calls_total += payload.api_calls_since_last_heartbeat

        await self._bus.publish(
            BossmanEvent.create(
                EventType.AGENT_HEARTBEAT,
                message=f"{state.info.name} heartbeat (step: {payload.current_step or '—'})",
                agent_id=payload.agent_id,
                payload={
                    "tokens_used": payload.tokens_used,
                    "context_utilisation": payload.context_utilisation,
                    "api_calls": payload.api_calls_since_last_heartbeat,
                },
            )
        )

        # Build ack — check if BOSSman should push any directives
        compact = state.resource_usage.near_context_limit
        if compact:
            await self._bus.publish(
                BossmanEvent.create(
                    EventType.CONTEXT_COMPACTION_TRIGGERED,
                    message=f"{state.info.name} context at "
                            f"{state.resource_usage.context_utilisation:.0%} — compaction triggered",
                    agent_id=payload.agent_id,
                )
            )

        budget_warn = state.resource_usage.near_token_budget
        if budget_warn:
            await self._bus.publish(
                BossmanEvent.create(
                    EventType.TOKEN_BUDGET_WARNING,
                    message=f"{state.info.name} has used "
                            f"{state.resource_usage.token_budget_fraction:.0%} of token budget",
                    agent_id=payload.agent_id,
                )
            )

        return HeartbeatAck(
            agent_id=payload.agent_id,
            compact_context=compact,
            token_budget_remaining=(
                state.resource_usage.tokens_budget - state.resource_usage.tokens_used
            ),
        )

    # ── State mutation (called by Watchdog / Recovery Engine) ─────────────────

    async def update_status(self, agent_id: AgentId, new_status: AgentStatus) -> AgentState:
        """Force a status transition (called by Watchdog or Recovery Engine)."""
        state = await self._get_or_raise(agent_id)
        async with self._lock:
            state.transition_to(new_status)
        log.info("Agent %s transitioned to %s", agent_id, new_status.value)
        return state

    async def record_failure(self, agent_id: AgentId, error: str) -> AgentState:
        state = await self._get_or_raise(agent_id)
        async with self._lock:
            state.record_failure(error)
        return state

    async def record_success(self, agent_id: AgentId) -> AgentState:
        state = await self._get_or_raise(agent_id)
        async with self._lock:
            state.record_success()
        return state

    # ── Queries ───────────────────────────────────────────────────────────────

    async def get(self, agent_id: AgentId) -> AgentState | None:
        return self._agents.get(agent_id)

    async def all(self) -> list[AgentState]:
        return list(self._agents.values())

    async def by_status(self, *statuses: AgentStatus) -> list[AgentState]:
        return [a for a in self._agents.values() if a.status in statuses]

    async def by_role(self, role: str) -> list[AgentState]:
        return [a for a in self._agents.values() if a.info.role == role]

    async def unhealthy(self) -> list[AgentState]:
        return await self.by_status(*AgentStatus.unhealthy())

    async def deadlock_suspects(self) -> list[AgentState]:
        """Agents holding ≥1 resource AND waiting on ≥1 resource."""
        return [a for a in self._agents.values() if a.is_deadlock_suspect]

    async def snapshot(self) -> dict[str, dict]:
        """Serialisable summary of the entire workforce — for the dashboard."""
        agents = await self.all()
        return {
            a.agent_id: {
                "name": a.info.name,
                "role": a.info.role,
                "status": a.status.value,
                "permission_tier": a.info.permission_tier.value,
                "current_task_id": a.current_task_id,
                "current_step": a.current_step,
                "heartbeat_age_s": round(a.heartbeat_age_seconds, 1),
                "is_healthy": a.is_healthy,
                "consecutive_failures": a.consecutive_failures,
                "tokens_used": a.resource_usage.tokens_used,
                "context_utilisation": a.resource_usage.context_utilisation,
                "resources_held": a.resource_usage.resources_held,
                "resources_waiting": a.resource_usage.resources_waiting,
            }
            for a in agents
        }

    # ── Internals ─────────────────────────────────────────────────────────────

    async def _get_or_raise(self, agent_id: AgentId) -> AgentState:
        state = self._agents.get(agent_id)
        if state is None:
            raise KeyError(f"Unknown agent: {agent_id!r}")
        return state
