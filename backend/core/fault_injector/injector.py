"""
core.fault_injector.injector
─────────────────────────────
FaultInjector — controlled chaos for BOSSman.

Why this is Phase 1, not Phase 10
──────────────────────────────────
Building fault injection from day one forces every subsystem to be designed
for testability under failure. It also gives you the demo's killer feature
early: inject a fault, watch BOSSman detect and recover from it live.

The injector operates at BOSSman's internal layer — it manipulates AgentState
and TaskManager directly, then publishes a FAULT_INJECTED event so the
dashboard shows exactly what was triggered and when.

Fault Types
───────────
Each maps to one of the zylos.md failure categories:

  AGENT_HANG          → Simulate heartbeat timeout (stop updating heartbeat)
  AGENT_CRASH         → Hard terminate an agent
  RETRY_STORM         → Flood an agent's api_calls_per_minute counter
  CONTEXT_OVERFLOW    → Push context_utilisation above 0.75 threshold
  RESOURCE_DEADLOCK   → Put two agents in mutual hold/wait on resources
  TASK_FAILURE        → Force a running task to fail
  TOKEN_EXHAUSTION    → Set token_used = token_budget (budget blown)
  CASCADING_FAILURE   → Fail an agent and its downstream dependent
  SILENT_DEGRADATION  → Flag an agent as quality-degraded (no crash)
  DUPLICATE_SPAWN     → Trigger rapid duplicate subagent spawns (idempotency test)
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field

from contracts.agent_state import AgentId, AgentStatus, TaskId
from contracts.events import BossmanEvent, EventSeverity, EventType
from core.event_bus import EventBus
from core.registry import AgentRegistry
from core.task_manager import TaskManager

log = logging.getLogger(__name__)


class FaultType(str, Enum):
    AGENT_HANG = "AGENT_HANG"
    AGENT_CRASH = "AGENT_CRASH"
    RETRY_STORM = "RETRY_STORM"
    CONTEXT_OVERFLOW = "CONTEXT_OVERFLOW"
    RESOURCE_DEADLOCK = "RESOURCE_DEADLOCK"
    TASK_FAILURE = "TASK_FAILURE"
    TOKEN_EXHAUSTION = "TOKEN_EXHAUSTION"
    CASCADING_FAILURE = "CASCADING_FAILURE"
    SILENT_DEGRADATION = "SILENT_DEGRADATION"
    DUPLICATE_SPAWN = "DUPLICATE_SPAWN"


class FaultConfig(BaseModel):
    """
    Configuration for a fault injection. All fields are optional — sane
    defaults are applied per FaultType if not provided.
    """

    fault_type: FaultType
    agent_id: AgentId | None = None
    task_id: TaskId | None = None
    target_agent_id: AgentId | None = None   # second agent for deadlock scenarios
    severity_override: EventSeverity | None = None
    params: dict[str, Any] = Field(default_factory=dict)
    note: str = ""                            # shown on dashboard event feed


class FaultInjector:
    """
    Controlled chaos engine for BOSSman.

    Inject faults into the live system, watch BOSSman's subsystems respond,
    and verify recovery via the Evaluator.

    Usage
    ─────
        injector = FaultInjector(registry, task_manager, bus)

        # Kill an agent's heartbeat — watchdog should detect within timeout_seconds
        await injector.inject(FaultConfig(
            fault_type=FaultType.AGENT_HANG,
            agent_id=my_agent_id,
            note="Demo: agent hang scenario",
        ))

        # Force a task to fail
        await injector.inject(FaultConfig(
            fault_type=FaultType.TASK_FAILURE,
            agent_id=my_agent_id,
            task_id=my_task_id,
            params={"error": "Simulated external API failure"},
        ))
    """

    def __init__(
        self,
        registry: AgentRegistry,
        task_manager: TaskManager,
        bus: EventBus,
    ) -> None:
        self._registry = registry
        self._task_manager = task_manager
        self._bus = bus
        self._injection_log: list[dict[str, Any]] = []

    # ── Public API ────────────────────────────────────────────────────────────

    async def inject(self, config: FaultConfig) -> None:
        """
        Inject the specified fault. Always emits a FAULT_INJECTED event
        before executing so the dashboard logs it.
        """
        log.warning(
            "FAULT INJECTED: %s | agent=%s task=%s | %s",
            config.fault_type.value,
            config.agent_id,
            config.task_id,
            config.note,
        )

        await self._bus.publish(
            BossmanEvent.create(
                EventType.FAULT_INJECTED,
                message=f"[FAULT] {config.fault_type.value}"
                        + (f" — {config.note}" if config.note else ""),
                agent_id=config.agent_id,
                task_id=config.task_id,
                severity=config.severity_override,
                payload={
                    "fault_type": config.fault_type.value,
                    "params": config.params,
                    "note": config.note,
                },
            )
        )

        self._injection_log.append({
            "fault_type": config.fault_type.value,
            "agent_id": config.agent_id,
            "task_id": config.task_id,
            "injected_at": datetime.now(timezone.utc).isoformat(),
            "note": config.note,
        })

        handler = self._HANDLERS.get(config.fault_type)
        if handler is None:
            log.error("No handler for fault type: %s", config.fault_type)
            return

        try:
            await handler(self, config)
        except Exception:
            log.exception("Fault injection handler for %s raised", config.fault_type)

    # ── Fault handlers ────────────────────────────────────────────────────────

    async def _inject_agent_hang(self, config: FaultConfig) -> None:
        """
        Freeze an agent's heartbeat timestamp so the watchdog detects a timeout.
        The agent appears alive (not crashed) but stops sending heartbeats.
        """
        if not config.agent_id:
            log.error("AGENT_HANG requires agent_id")
            return

        state = await self._registry.get(config.agent_id)
        if state is None:
            log.error("AGENT_HANG: unknown agent %s", config.agent_id)
            return

        # Backdate the heartbeat so watchdog sees it as overdue immediately
        backdate_seconds = config.params.get("backdate_seconds", 60.0)
        from datetime import timedelta
        state.last_heartbeat = datetime.now(timezone.utc) - timedelta(seconds=backdate_seconds)
        log.warning(
            "Agent %s heartbeat backdated by %.0fs — watchdog will detect timeout",
            config.agent_id,
            backdate_seconds,
        )

    async def _inject_agent_crash(self, config: FaultConfig) -> None:
        """Hard-terminate an agent — transition to TERMINATED."""
        if not config.agent_id:
            log.error("AGENT_CRASH requires agent_id")
            return

        state = await self._registry.get(config.agent_id)
        if state is None:
            return

        reason = config.params.get("reason", "Simulated crash (fault injection)")
        await self._registry.record_failure(config.agent_id, reason)
        try:
            await self._registry.update_status(config.agent_id, AgentStatus.FAILED)
        except ValueError:
            pass
        log.warning("Agent %s crashed (simulated)", config.agent_id)

    async def _inject_retry_storm(self, config: FaultConfig) -> None:
        """
        Spike an agent's api_calls_per_minute to simulate a retry storm.
        The Failure Detector (Phase 4) watches this metric.
        """
        if not config.agent_id:
            log.error("RETRY_STORM requires agent_id")
            return

        state = await self._registry.get(config.agent_id)
        if state is None:
            return

        calls_per_minute = config.params.get("calls_per_minute", 80.0)
        state.resource_usage.api_calls_per_minute = calls_per_minute
        state.resource_usage.api_calls_total += int(calls_per_minute)
        log.warning(
            "Agent %s api_calls_per_minute spiked to %.0f (simulated retry storm)",
            config.agent_id,
            calls_per_minute,
        )

    async def _inject_context_overflow(self, config: FaultConfig) -> None:
        """Push context utilisation above the 0.75 compaction threshold."""
        if not config.agent_id:
            log.error("CONTEXT_OVERFLOW requires agent_id")
            return

        state = await self._registry.get(config.agent_id)
        if state is None:
            return

        utilisation = config.params.get("context_utilisation", 0.92)
        state.resource_usage.context_utilisation = utilisation
        log.warning(
            "Agent %s context_utilisation set to %.0f%% (simulated overflow)",
            config.agent_id,
            utilisation * 100,
        )

    async def _inject_resource_deadlock(self, config: FaultConfig) -> None:
        """
        Put two agents in a mutual hold/wait cycle:
          Agent A holds Resource X, waits for Resource Y
          Agent B holds Resource Y, waits for Resource X
        The Failure Detector's deadlock check (Phase 4) should catch this.
        """
        if not config.agent_id or not config.target_agent_id:
            log.error("RESOURCE_DEADLOCK requires agent_id AND target_agent_id")
            return

        state_a = await self._registry.get(config.agent_id)
        state_b = await self._registry.get(config.target_agent_id)
        if not state_a or not state_b:
            log.error("RESOURCE_DEADLOCK: one or both agents not found")
            return

        res_x = config.params.get("resource_x", "resource-X")
        res_y = config.params.get("resource_y", "resource-Y")

        state_a.resource_usage.resources_held = [res_x]
        state_a.resource_usage.resources_waiting = [res_y]
        state_b.resource_usage.resources_held = [res_y]
        state_b.resource_usage.resources_waiting = [res_x]

        log.warning(
            "Deadlock injected: %s holds %s waits %s | %s holds %s waits %s",
            config.agent_id, res_x, res_y,
            config.target_agent_id, res_y, res_x,
        )

    async def _inject_task_failure(self, config: FaultConfig) -> None:
        """Force a running task to fail."""
        if not config.task_id:
            log.error("TASK_FAILURE requires task_id")
            return

        error = config.params.get("error", "Simulated task failure (fault injection)")
        await self._task_manager.fail(config.task_id, error)

    async def _inject_token_exhaustion(self, config: FaultConfig) -> None:
        """Blow the token budget — should trigger TOKEN_BUDGET_WARNING."""
        if not config.agent_id:
            log.error("TOKEN_EXHAUSTION requires agent_id")
            return

        state = await self._registry.get(config.agent_id)
        if state is None:
            return

        state.resource_usage.tokens_used = state.resource_usage.tokens_budget
        log.warning("Agent %s token budget exhausted (simulated)", config.agent_id)

    async def _inject_cascading_failure(self, config: FaultConfig) -> None:
        """
        Fail the primary agent, then also fail its downstream dependent.
        Simulates the cascading failure scenario from zylos.md §Part1.
        """
        await self._inject_agent_crash(config)
        if config.target_agent_id:
            downstream_config = FaultConfig(
                fault_type=FaultType.AGENT_CRASH,
                agent_id=config.target_agent_id,
                params={"reason": f"Cascading failure from {config.agent_id}"},
            )
            await asyncio.sleep(0.5)  # slight delay to make cascade visible on timeline
            await self._inject_agent_crash(downstream_config)

    async def _inject_silent_degradation(self, config: FaultConfig) -> None:
        """
        Mark an agent as quality-degraded without crashing it.
        BOSSman's L4 health check (Phase 6 Evaluator) should detect this.
        """
        if not config.agent_id:
            log.error("SILENT_DEGRADATION requires agent_id")
            return

        state = await self._registry.get(config.agent_id)
        if state is None:
            return

        # Tag the agent's metadata with a degradation flag
        state.info.metadata["quality_degraded"] = True
        state.info.metadata["degradation_note"] = config.params.get(
            "note", "Simulated silent degradation"
        )
        log.warning("Agent %s flagged as quality-degraded (simulated)", config.agent_id)

    async def _inject_duplicate_spawn(self, config: FaultConfig) -> None:
        """
        Emit several rapid FAULT_INJECTED events simulating duplicate spawn attempts.
        Used to test the SubagentManager idempotency guard.
        Note: actual SubagentManager is tested directly in unit tests.
        """
        count = config.params.get("spawn_count", 5)
        task_type = config.params.get("task_type", "memory-sync")
        for i in range(count):
            await self._bus.publish(
                BossmanEvent.create(
                    EventType.DUPLICATE_SPAWN_BLOCKED,
                    message=f"[FAULT] Simulated duplicate spawn attempt {i+1}/{count} "
                            f"for task type {task_type!r}",
                    agent_id=config.agent_id,
                    payload={"attempt": i + 1, "task_type": task_type},
                )
            )
            await asyncio.sleep(0.1)

    # ── Handler dispatch table ────────────────────────────────────────────────

    _HANDLERS = {
        FaultType.AGENT_HANG:          _inject_agent_hang,
        FaultType.AGENT_CRASH:         _inject_agent_crash,
        FaultType.RETRY_STORM:         _inject_retry_storm,
        FaultType.CONTEXT_OVERFLOW:    _inject_context_overflow,
        FaultType.RESOURCE_DEADLOCK:   _inject_resource_deadlock,
        FaultType.TASK_FAILURE:        _inject_task_failure,
        FaultType.TOKEN_EXHAUSTION:    _inject_token_exhaustion,
        FaultType.CASCADING_FAILURE:   _inject_cascading_failure,
        FaultType.SILENT_DEGRADATION:  _inject_silent_degradation,
        FaultType.DUPLICATE_SPAWN:     _inject_duplicate_spawn,
    }

    # ── Introspection ─────────────────────────────────────────────────────────

    @property
    def injection_log(self) -> list[dict[str, Any]]:
        """All injections performed this session — for audit / evaluation."""
        return list(self._injection_log)
