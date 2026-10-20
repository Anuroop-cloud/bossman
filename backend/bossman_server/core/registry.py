"""
core.registry
─────────────
AgentRegistry — the authoritative store of all known agents.

BOSSman's single source of truth for:
  - Which agents exist (per namespace)
  - What state each agent is in
  - Whether an agent is alive / healthy / suspect
  - Querying the workforce by status, role, or permission tier

The registry holds AgentState objects in memory and publishes lifecycle
events to the EventBus on every state change. DurableAgentRegistry
(infra.durable_registry) mirrors every change to the coordination backend.

Heartbeats are the agent ↔ BOSSman control loop: the registry applies the
agent's telemetry and returns a HeartbeatAck carrying any pending
directives (core.directives) and feature flags (control.flags).

Thread/async safety
───────────────────
All public methods are async and protected by a single asyncio.Lock.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any

from bossman_server.contracts.agent_state import AgentId, AgentInfo, AgentState, AgentStatus
from bossman_server.contracts.directives import DirectiveAck, DirectiveType
from bossman_server.contracts.events import BossmanEvent, EventType
from bossman_server.contracts.heartbeat import HeartbeatAck, HeartbeatPayload
from bossman_server.core.event_bus import EventBus

log = logging.getLogger(__name__)


class FencedError(Exception):
    """A heartbeat/request came from a superseded process (stale epoch)."""

    def __init__(self, agent_id: str, presented: int, current: int) -> None:
        super().__init__(
            f"Agent {agent_id!r} epoch {presented} is stale (current epoch {current}). "
            "Another process took over this agent; stop working and exit."
        )
        self.agent_id = agent_id
        self.presented = presented
        self.current = current


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

    def __init__(self, bus: EventBus, *, directives: Any | None = None, flags: Any | None = None) -> None:
        self._bus = bus
        self._agents: dict[AgentId, AgentState] = {}
        self._lock = asyncio.Lock()
        self._directives = directives   # core.directives.DirectiveQueue (optional)
        self._flags = flags             # control.flags.FlagService (optional)
        # token-burn tracking: agent → (monotonic, tokens_used)
        self._token_marks: dict[AgentId, tuple[float, int]] = {}

    def attach(self, *, directives: Any | None = None, flags: Any | None = None) -> None:
        if directives is not None:
            self._directives = directives
        if flags is not None:
            self._flags = flags

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
                namespace=info.namespace,
                payload={
                    "name": info.name,
                    "role": info.role,
                    "permission_tier": info.permission_tier.value,
                    "transport": info.transport,
                    "supervisor": info.supervisor,
                    "managed_id": info.managed_id,
                    "epoch": info.epoch,
                },
            )
        )
        return state

    async def mark_ready(self, agent_id: AgentId) -> AgentState:
        """Transition agent from INITIALIZING → IDLE (ready to accept tasks)."""
        state = await self._get_or_raise(agent_id)
        async with self._lock:
            state.transition_to(AgentStatus.IDLE)
        await self.sync(agent_id)

        await self._bus.publish(
            BossmanEvent.create(
                EventType.AGENT_STARTED,
                message=f"{state.info.name} is ready",
                agent_id=agent_id,
                namespace=state.info.namespace,
            )
        )
        return state

    async def deregister(self, agent_id: AgentId, reason: str = "normal") -> None:
        """Remove an agent from the registry and emit AGENT_TERMINATED."""
        async with self._lock:
            state = self._agents.pop(agent_id, None)
            self._token_marks.pop(agent_id, None)

        if state is None:
            log.warning("deregister called for unknown agent %s", agent_id)
            return
        if self._directives is not None:
            await self._directives.clear(agent_id)

        log.info("Deregistered agent %s (reason: %s)", agent_id, reason)
        await self._bus.publish(
            BossmanEvent.create(
                EventType.AGENT_TERMINATED,
                message=f"{state.info.name} terminated ({reason})",
                agent_id=agent_id,
                namespace=state.info.namespace,
                payload={"reason": reason},
            )
        )

    async def takeover(self, agent_id: AgentId, info: AgentInfo) -> AgentState:
        """
        A new process takes over an existing agent identity: bump the epoch so
        the old process is fenced, reset runtime state, keep history counters.
        """
        state = await self._get_or_raise(agent_id)
        async with self._lock:
            new_info = info.model_copy(update={
                "agent_id": agent_id,
                "epoch": state.info.epoch + 1,
                "registered_at": state.info.registered_at,
            })
            fresh = AgentState(
                info=new_info,
                total_tasks_completed=state.total_tasks_completed,
                total_tasks_failed=state.total_tasks_failed,
            )
            self._agents[agent_id] = fresh
            self._token_marks.pop(agent_id, None)
        if self._directives is not None:
            await self._directives.clear(agent_id)
        await self.sync(agent_id)
        await self._bus.publish(BossmanEvent.create(
            EventType.AGENT_REGISTERED,
            message=f"{new_info.name} re-registered (takeover, epoch {new_info.epoch})",
            agent_id=agent_id, namespace=new_info.namespace,
            payload={"takeover": True, "epoch": new_info.epoch, "role": new_info.role},
        ))
        return fresh

    # ── Heartbeats ────────────────────────────────────────────────────────────

    async def record_heartbeat(self, payload: HeartbeatPayload) -> HeartbeatAck:
        """
        Process an agent heartbeat. Updates AgentState and returns an Ack
        with any directives BOSSman wants to push back to the agent.
        """
        state = await self._get_or_raise(payload.agent_id)
        if payload.epoch is not None and payload.epoch != state.info.epoch:
            await self._bus.publish(BossmanEvent.create(
                EventType.AGENT_FENCED, agent_id=payload.agent_id, namespace=state.info.namespace,
                message=f"Rejected heartbeat from stale epoch {payload.epoch} (current {state.info.epoch})",
                payload={"presented": payload.epoch, "current": state.info.epoch},
            ))
            raise FencedError(payload.agent_id, payload.epoch, state.info.epoch)

        async with self._lock:
            state.record_heartbeat()
            if payload.current_step is not None:
                state.current_step = payload.current_step
            if payload.current_task_id is not None:
                state.current_task_id = payload.current_task_id
            ru = state.resource_usage
            ru.context_utilisation = max(0.0, min(1.0, payload.context_utilisation))
            ru.api_calls_total += payload.api_calls_since_last_heartbeat
            self._update_rates(state, payload)
            ru.tokens_used = payload.tokens_used
            if payload.degradation_level is not None:
                state.degradation_level = payload.degradation_level
            if payload.model is not None:
                state.current_model = payload.model
            if payload.paused is not None:
                state.paused = payload.paused
            self._apply_reported_status(state, payload.status)

        # Directive acks piggy-backed on the heartbeat
        if self._directives is not None:
            for ack in payload.acked_directives:
                await self._directives.ack(payload.agent_id, ack)

        await self.sync(payload.agent_id)

        await self._bus.publish(
            BossmanEvent.create(
                EventType.AGENT_HEARTBEAT,
                message=f"{state.info.name} heartbeat (step: {payload.current_step or '—'})",
                agent_id=payload.agent_id,
                namespace=state.info.namespace,
                payload={
                    "tokens_used": payload.tokens_used,
                    "context_utilisation": payload.context_utilisation,
                    "api_calls": payload.api_calls_since_last_heartbeat,
                    "tokens_per_minute": round(state.tokens_per_minute, 1),
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
                    namespace=state.info.namespace,
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
                    namespace=state.info.namespace,
                )
            )

        directives = []
        if self._directives is not None:
            directives = await self._directives.deliver(payload.agent_id)
        types = {d.type for d in directives}

        flags: dict[str, bool] | None = None
        flags_version = 0
        if self._flags is not None:
            flags_version = self._flags.version
            if payload.flags_version != flags_version:
                flags = await self._flags.snapshot_for(state)

        return HeartbeatAck(
            agent_id=payload.agent_id,
            compact_context=compact or DirectiveType.COMPACT_CONTEXT in types,
            pause_task=DirectiveType.PAUSE in types,
            abort_task=DirectiveType.ABORT_TASK in types,
            token_budget_remaining=(
                state.resource_usage.tokens_budget - state.resource_usage.tokens_used
            ),
            directives=directives,
            flags=flags,
            flags_version=flags_version,
            epoch=state.info.epoch,
            degradation_level=state.degradation_level,
        )

    # BaseAgent (in-process) historically called this name.
    process_heartbeat = record_heartbeat

    async def ack_directive(self, agent_id: AgentId, ack: DirectiveAck) -> None:
        if self._directives is not None:
            await self._directives.ack(agent_id, ack)

    def _update_rates(self, state: AgentState, payload: HeartbeatPayload) -> None:
        now = time.monotonic()
        prev = self._token_marks.get(state.agent_id)
        self._token_marks[state.agent_id] = (now, payload.tokens_used)
        if prev is None:
            return
        dt = now - prev[0]
        if dt <= 0.5:
            return
        delta = max(0, payload.tokens_used - prev[1])
        per_min = delta * 60.0 / dt
        # EWMA so a single burst does not dominate
        state.tokens_per_minute = per_min if state.tokens_per_minute == 0 else (
            0.5 * state.tokens_per_minute + 0.5 * per_min
        )
        calls = payload.api_calls_since_last_heartbeat * 60.0 / dt
        state.resource_usage.api_calls_per_minute = round(calls, 2)

    @staticmethod
    def _apply_reported_status(state: AgentState, reported: AgentStatus) -> None:
        """
        Accept the agent's own lifecycle report only where BOSSman is not in
        charge: STUCK / FAILED / RECOVERING are owned by the watchdog and the
        recovery engine, TERMINATED by deregistration.
        """
        if reported == state.status or reported in AgentStatus.unhealthy() | AgentStatus.terminal():
            return
        if state.status not in {AgentStatus.INITIALIZING, AgentStatus.IDLE, AgentStatus.RUNNING}:
            return
        try:
            state.transition_to(reported)
        except ValueError:
            pass

    # ── State mutation (called by Watchdog / Recovery Engine) ─────────────────

    async def update_status(self, agent_id: AgentId, new_status: AgentStatus) -> AgentState:
        """Force a status transition (called by Watchdog or Recovery Engine)."""
        state = await self._get_or_raise(agent_id)
        async with self._lock:
            state.transition_to(new_status)
        log.info("Agent %s transitioned to %s", agent_id, new_status.value)
        await self.sync(agent_id)
        return state

    async def record_failure(self, agent_id: AgentId, error: str) -> AgentState:
        state = await self._get_or_raise(agent_id)
        async with self._lock:
            state.record_failure(error)
        await self.sync(agent_id)
        return state

    async def record_success(self, agent_id: AgentId) -> AgentState:
        state = await self._get_or_raise(agent_id)
        async with self._lock:
            state.record_success()
        await self.sync(agent_id)
        return state

    async def sync(self, agent_id: AgentId) -> None:
        """Persist after a direct mutation. No-op for the in-memory registry."""
        return None

    # ── Queries ───────────────────────────────────────────────────────────────

    async def get(self, agent_id: AgentId) -> AgentState | None:
        return self._agents.get(agent_id)

    async def all(self) -> list[AgentState]:
        return list(self._agents.values())

    async def by_status(self, *statuses: AgentStatus) -> list[AgentState]:
        return [a for a in self._agents.values() if a.status in statuses]

    async def by_role(self, role: str, namespace: str | None = None) -> list[AgentState]:
        return [
            a for a in self._agents.values()
            if a.info.role == role and (namespace is None or a.info.namespace == namespace)
        ]

    async def by_namespace(self, namespace: str) -> list[AgentState]:
        return [a for a in self._agents.values() if a.info.namespace == namespace]

    async def by_supervisor(self, supervisor: str) -> list[AgentState]:
        members = [a for a in self._agents.values() if a.info.supervisor == supervisor]
        return sorted(members, key=lambda a: (a.info.start_order, a.info.registered_at))

    async def find_singleton(self, namespace: str, key: str) -> AgentState | None:
        return next(
            (a for a in self._agents.values()
             if a.info.namespace == namespace and a.info.singleton_key == key and a.is_alive),
            None,
        )

    async def find_managed(self, managed_id: str) -> list[AgentState]:
        return [a for a in self._agents.values() if a.info.managed_id == managed_id]

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
                "namespace": a.info.namespace,
                "status": a.status.value,
                "permission_tier": a.info.permission_tier.value,
                "current_task_id": a.current_task_id,
                "current_step": a.current_step,
                "heartbeat_age_s": round(a.heartbeat_age_seconds, 1),
                "is_healthy": a.is_healthy,
                "consecutive_failures": a.consecutive_failures,
                "tokens_used": a.resource_usage.tokens_used,
                "tokens_per_minute": a.tokens_per_minute,
                "api_calls_per_minute": a.resource_usage.api_calls_per_minute,
                "context_utilisation": a.resource_usage.context_utilisation,
                "resources_held": a.resource_usage.resources_held,
                "resources_waiting": a.resource_usage.resources_waiting,
            }
            for a in agents
        }

    # ── Internals ─────────────────────────────────────────────────────────────

    async def _get_or_raise(self, agent_id: AgentId) -> AgentState:
        state = await self.get(agent_id)
        if state is None:
            raise KeyError(f"Unknown agent: {agent_id!r}")
        return state
