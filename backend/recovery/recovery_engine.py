"""
recovery.recovery_engine
─────────────────────────
The BOSSman Recovery Engine — Phase 5.

The engine is the only component in the system that takes corrective action.
Everything before this point (detection, diagnosis) was read-only observation.

Architecture
────────────
RecoveryEngine:
  - Registers as FailureDetector.on_failure callback
  - Receives DetectedFailure from FailureDetector
  - Runs it through Diagnostician to get a Diagnosis
  - Dispatches to the appropriate strategy handler
  - Records every attempt in the in-memory RecoveryLedger
  - Publishes RECOVERY_* events throughout
  - Enforces restart budgets (max restarts per window) per zylos.md §Part2

Recovery strategies (from RecoveryStrategy enum):
──────────────────────────────────────────────────
  WAIT_AND_RETRY       — exponential backoff, then mark agent RECOVERING → IDLE
  RESTART_AGENT        — STUCK/FAILED → RECOVERING → IDLE (in-process reset)
  REASSIGN_TASK        — find healthy agent for same role, reassign task
  ROLLBACK_CHECKPOINT  — send checkpoint rollback directive via heartbeat ack
  CIRCUIT_BREAKER      — open circuit for the failing agent
  FALLBACK_MODEL       — emit signal for agent to degrade model (Phase 7 LLM)
  CONTEXT_COMPACTION   — send compact_context directive via heartbeat ack
  RELEASE_RESOURCES    — force-clear resources_held/waiting on agent state
  QUEUE_FOR_LATER      — send task to DLQ for later replay
  ESCALATE_HUMAN       — emit HUMAN_GATE_REQUIRED, do nothing else

Restart budget (zylos.md §Part2 — Supervisor Tree):
  "If the child restarts more than MaxRestarts times within MaxTime seconds,
   the supervisor itself terminates and propagates the failure to its parent."
  → We ESCALATE_HUMAN instead of terminating the supervisor.

Thread safety
─────────────
RecoveryEngine is a single-process asyncio component. All handlers are
async coroutines. No external locking needed.

Phase 5 boundary
─────────────────
RecoveryEngine uses ONLY the existing Phase 0–4 infrastructure:
  - AgentRegistry: read state + update_status
  - TaskManager: reassign + fail + replay_from_dlq
  - ResourceMediator: force-release via registry state mutation
  - CircuitBreakerRegistry: open/close circuits
  - EventBus: publish recovery events
  - RecoveryLedger: in-memory list (Postgres persistence is Phase 8)
  - Diagnostician: diagnose failures

It does NOT call any LLM (Phase 7). It does NOT render a dashboard (Phase 8).
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from collections import defaultdict, deque
from dataclasses import dataclass, field
from typing import Any

from contracts.agent_state import AgentId, AgentStatus, TaskId
from contracts.events import BossmanEvent, EventSeverity, EventType
from contracts.recovery_ledger import (
    FailureType,
    RecoveryAttempt,
    RecoveryOutcome,
    RecoveryStrategy,
    new_incident_id,
)
from core.event_bus import EventBus
from core.registry import AgentRegistry
from core.task_manager import TaskManager
from detection.diagnostician import Diagnosis, Diagnostician
from detection.failure_detector import DetectedFailure
from recovery.circuit_breaker import CircuitBreakerRegistry

log = logging.getLogger(__name__)

# Restart budget: per-agent max restarts within a sliding window
DEFAULT_MAX_RESTARTS: int = 3
DEFAULT_RESTART_WINDOW: float = 60.0

# Exponential backoff base delay (WAIT_AND_RETRY)
BACKOFF_BASE_SECONDS: float = 2.0
BACKOFF_MAX_SECONDS: float = 60.0


@dataclass
class RecoveryLedger:
    """
    In-memory recovery ledger. Append-only.
    Phase 8 will persist this to Postgres.
    """
    entries: list[RecoveryAttempt] = field(default_factory=list)

    def append(self, attempt: RecoveryAttempt) -> None:
        self.entries.append(attempt)
        log.info(
            "Ledger: [%s] %s → %s (%s) | %s",
            attempt.incident_id[:8],
            attempt.failure_type.value,
            attempt.strategy.value,
            attempt.outcome.value,
            attempt.failure_detail[:60],
        )

    def for_agent(self, agent_id: AgentId) -> list[RecoveryAttempt]:
        return [e for e in self.entries if e.agent_id == agent_id]

    def for_incident(self, incident_id: str) -> list[RecoveryAttempt]:
        return [e for e in self.entries if e.incident_id == incident_id]

    @property
    def stats(self) -> dict[str, Any]:
        by_outcome: dict[str, int] = defaultdict(int)
        by_strategy: dict[str, int] = defaultdict(int)
        for e in self.entries:
            by_outcome[e.outcome.value] += 1
            by_strategy[e.strategy.value] += 1
        return {
            "total_attempts": len(self.entries),
            "by_outcome": dict(by_outcome),
            "by_strategy": dict(by_strategy),
        }


class RecoveryEngine:
    """
    BOSSman's corrective action engine.

    Usage
    ─────
        engine = RecoveryEngine(
            registry=registry,
            task_manager=task_manager,
            bus=bus,
            diagnostician=Diagnostician(),
            circuit_breakers=CircuitBreakerRegistry(event_bus=bus),
        )
        # Wire to FailureDetector:
        detector = FailureDetector(bus, on_failure=engine.handle_failure)
    """

    def __init__(
        self,
        registry: AgentRegistry,
        task_manager: TaskManager,
        bus: EventBus,
        diagnostician: Diagnostician,
        circuit_breakers: CircuitBreakerRegistry,
        *,
        max_restarts: int = DEFAULT_MAX_RESTARTS,
        restart_window_seconds: float = DEFAULT_RESTART_WINDOW,
    ) -> None:
        self._registry = registry
        self._task_manager = task_manager
        self._bus = bus
        self._diagnostician = diagnostician
        self._circuit_breakers = circuit_breakers
        self._max_restarts = max_restarts
        self._restart_window = restart_window_seconds

        self.ledger = RecoveryLedger()

        # Restart budget tracking: agent_id → deque of restart timestamps
        self._restart_history: dict[AgentId, deque[float]] = defaultdict(deque)

        # In-flight recovery locks: prevent concurrent recovery for same agent
        self._active_recoveries: set[AgentId] = set()

        # Stats
        self._handled: int = 0
        self._skipped_duplicate: int = 0

    # ── Main entry point (wired to FailureDetector.on_failure) ────────────────

    async def handle_failure(self, detected: DetectedFailure) -> RecoveryAttempt | None:
        """
        Called by FailureDetector when a failure is detected.
        Diagnoses, selects strategy, executes, records in ledger.
        """
        self._handled += 1
        agent_id = detected.agent_id

        # Prevent concurrent duplicate recovery for the same agent
        if agent_id and agent_id in self._active_recoveries:
            self._skipped_duplicate += 1
            log.info("RecoveryEngine: skipping duplicate recovery for agent %s", agent_id)
            return None

        if agent_id:
            self._active_recoveries.add(agent_id)

        try:
            return await self._execute_recovery(detected)
        finally:
            if agent_id:
                self._active_recoveries.discard(agent_id)

    async def _execute_recovery(self, detected: DetectedFailure) -> RecoveryAttempt:
        """Diagnose → plan → execute → record."""
        diagnosis = self._diagnostician.diagnose(detected)

        attempt = RecoveryAttempt(
            incident_id=detected.incident_id,
            agent_id=detected.agent_id or AgentId("unknown"),
            task_id=None,
            failure_type=detected.failure_type,
            failure_detail=detected.detail,
            strategy=diagnosis.recommended_strategy,
            telemetry_snapshot=detected.telemetry,
        )

        await self._emit(EventType.RECOVERY_STARTED, attempt, diagnosis)
        await self._emit_strategy_chosen(attempt, diagnosis)

        try:
            await self._dispatch(diagnosis, attempt)
            attempt.mark_complete(RecoveryOutcome.IN_PROGRESS, notes="Strategy executed, awaiting evaluation")
        except Exception as exc:
            log.error("RecoveryEngine: strategy %s raised: %s", diagnosis.recommended_strategy, exc)
            attempt.mark_complete(RecoveryOutcome.FAILURE, notes=str(exc))
            await self._emit(EventType.RECOVERY_FAILED, attempt, diagnosis)

        self.ledger.append(attempt)
        return attempt

    # ── Strategy dispatcher ────────────────────────────────────────────────────

    async def _dispatch(self, diagnosis: Diagnosis, attempt: RecoveryAttempt) -> None:
        strategy = diagnosis.recommended_strategy
        handlers = {
            RecoveryStrategy.WAIT_AND_RETRY:       self._strategy_wait_and_retry,
            RecoveryStrategy.RESTART_AGENT:         self._strategy_restart_agent,
            RecoveryStrategy.REASSIGN_TASK:         self._strategy_reassign_task,
            RecoveryStrategy.ROLLBACK_CHECKPOINT:   self._strategy_rollback_checkpoint,
            RecoveryStrategy.CIRCUIT_BREAKER:       self._strategy_circuit_breaker,
            RecoveryStrategy.FALLBACK_MODEL:        self._strategy_fallback_model,
            RecoveryStrategy.CONTEXT_COMPACTION:    self._strategy_context_compaction,
            RecoveryStrategy.RELEASE_RESOURCES:     self._strategy_release_resources,
            RecoveryStrategy.QUEUE_FOR_LATER:       self._strategy_queue_for_later,
            RecoveryStrategy.ESCALATE_HUMAN:        self._strategy_escalate_human,
        }
        handler = handlers.get(strategy)
        if handler is None:
            log.error("RecoveryEngine: no handler for strategy %s", strategy)
            return
        await handler(diagnosis, attempt)

    # ── Strategy handlers ──────────────────────────────────────────────────────

    async def _strategy_wait_and_retry(
        self, diagnosis: Diagnosis, attempt: RecoveryAttempt
    ) -> None:
        """
        Exponential backoff + transition agent to RECOVERING.
        zylos.md §Part1: "exponential backoff with jitter reduces retry storms
        by 60-80% versus fixed-interval retries."
        """
        agent_id = diagnosis.agent_id
        if not agent_id:
            return

        # Compute attempt number for this agent from ledger
        prior = len(self.ledger.for_agent(agent_id))
        delay = min(BACKOFF_BASE_SECONDS * (2 ** prior), BACKOFF_MAX_SECONDS)

        await self._step(attempt, f"Backoff delay: {delay:.1f}s (attempt #{prior + 1})")
        log.info("RecoveryEngine: WAIT_AND_RETRY agent=%s delay=%.1fs", agent_id, delay)

        state = await self._registry.get(agent_id)
        if state and state.status in {AgentStatus.STUCK, AgentStatus.FAILED, AgentStatus.RUNNING}:
            try:
                await self._registry.update_status(agent_id, AgentStatus.RECOVERING)
            except ValueError:
                pass  # already in a compatible state

        await asyncio.sleep(delay)

        # Transition back to IDLE for retry
        state = await self._registry.get(agent_id)
        if state and state.status == AgentStatus.RECOVERING:
            try:
                await self._registry.update_status(agent_id, AgentStatus.IDLE)
                await self._step(attempt, f"Agent {agent_id} transitioned RECOVERING → IDLE for retry")
            except ValueError:
                pass

    async def _strategy_restart_agent(
        self, diagnosis: Diagnosis, attempt: RecoveryAttempt
    ) -> None:
        """
        Restart agent: STUCK/FAILED → RECOVERING → IDLE.
        Enforces restart budget. If budget exceeded → ESCALATE_HUMAN.

        zylos.md §Part2: "If the child restarts more than MaxRestarts times
        within MaxTime seconds, the supervisor propagates the failure upward."
        """
        agent_id = diagnosis.agent_id
        if not agent_id:
            return

        # Check restart budget
        if not self._check_restart_budget(agent_id):
            log.warning(
                "RecoveryEngine: restart budget exhausted for agent %s — escalating",
                agent_id,
            )
            await self._step(attempt, f"Restart budget exceeded for {agent_id} — escalating to human")
            # Override to ESCALATE
            escalate_diag = Diagnosis(
                failure_type=FailureType.HEARTBEAT_TIMEOUT,
                root_cause=f"Restart budget exhausted after {self._max_restarts} restarts in {self._restart_window}s",
                recommended_strategy=RecoveryStrategy.ESCALATE_HUMAN,
                confidence=1.0,
                agent_id=agent_id,
                incident_id=diagnosis.incident_id,
                evidence=diagnosis.evidence,
                rule_matched="restart_budget_exceeded",
            )
            attempt.strategy = RecoveryStrategy.ESCALATE_HUMAN
            await self._strategy_escalate_human(escalate_diag, attempt)
            return

        state = await self._registry.get(agent_id)
        if state is None:
            log.error("RecoveryEngine: agent %s not found in registry", agent_id)
            return

        await self._step(attempt, f"Transitioning agent {agent_id} to RECOVERING")
        try:
            await self._registry.update_status(agent_id, AgentStatus.RECOVERING)
        except ValueError as e:
            log.warning("RecoveryEngine: cannot transition to RECOVERING: %s", e)
            return

        # Clear resource accounting — agent is restarting clean
        state.resource_usage.resources_held = []
        state.resource_usage.resources_waiting = []
        state.resource_usage.context_utilisation = 0.0
        state.current_step = None

        await asyncio.sleep(0.1)  # brief pause — in prod this is agent process restart

        await self._registry.update_status(agent_id, AgentStatus.IDLE)
        await self._step(attempt, f"Agent {agent_id} restarted: RECOVERING → IDLE")

        await self._bus.publish(
            BossmanEvent.create(
                EventType.AGENT_RESTARTED,
                agent_id=agent_id,
                message=f"Agent {agent_id} restarted by RecoveryEngine (restart_count={self._restart_count(agent_id)})",
                payload={"restart_count": self._restart_count(agent_id)},
            )
        )

    async def _strategy_reassign_task(
        self, diagnosis: Diagnosis, attempt: RecoveryAttempt
    ) -> None:
        """
        Reassign the agent's current task to a healthy agent with the same role.
        If no healthy candidate found → QUEUE_FOR_LATER.
        """
        agent_id = diagnosis.agent_id
        if not agent_id:
            return

        state = await self._registry.get(agent_id)
        if state is None:
            return

        task_id = state.current_task_id
        if task_id is None:
            await self._step(attempt, f"Agent {agent_id} has no current task — nothing to reassign")
            return

        # Find a healthy candidate of the same role
        candidates = await self._registry.by_role(state.info.role)
        healthy = [
            c for c in candidates
            if c.agent_id != agent_id and c.status == AgentStatus.IDLE
        ]

        if not healthy:
            await self._step(attempt, f"No healthy {state.info.role!r} agents — queueing task for later")
            await self._strategy_queue_for_later(diagnosis, attempt)
            return

        target = healthy[0]
        await self._task_manager.reassign(task_id, target.agent_id, reason=f"Recovery: agent {agent_id} failed")
        await self._step(attempt, f"Task {task_id} reassigned from {agent_id} → {target.agent_id}")

    async def _strategy_rollback_checkpoint(
        self, diagnosis: Diagnosis, attempt: RecoveryAttempt
    ) -> None:
        """
        Signal agent to roll back to its last LangGraph checkpoint.
        In-process: we send a 'rollback' directive via the heartbeat ack system.
        The agent's _heartbeat_loop picks this up on next heartbeat.
        """
        agent_id = diagnosis.agent_id
        if not agent_id:
            return

        # Set rollback flag in agent metadata (heartbeat ack reads this)
        state = await self._registry.get(agent_id)
        if state:
            state.info.metadata["directive"] = "rollback_checkpoint"
            await self._step(attempt, f"Rollback checkpoint directive queued for agent {agent_id}")

    async def _strategy_circuit_breaker(
        self, diagnosis: Diagnosis, attempt: RecoveryAttempt
    ) -> None:
        """
        Open the circuit breaker for the failing agent.
        Future calls through the breaker will fail-fast until HALF_OPEN probe.

        zylos.md §Part2: "Failing fast when a model API is degraded — and
        routing to a fallback model — is far better than accumulating request
        timeouts that block the agent's execution pipeline."
        """
        agent_id = diagnosis.agent_id
        key = str(agent_id) if agent_id else "global"
        cb = self._circuit_breakers.open(key, agent_id=agent_id)
        await self._step(
            attempt,
            f"Circuit breaker '{key}' OPENED "
            f"(will probe after {cb._open_timeout:.0f}s)",
        )

    async def _strategy_fallback_model(
        self, diagnosis: Diagnosis, attempt: RecoveryAttempt
    ) -> None:
        """
        Signal agent to switch to a degraded/fallback model.
        In-process: sets a 'fallback_model' directive in agent metadata.
        Phase 7 LLM Diagnostician will act on this more intelligently.
        """
        agent_id = diagnosis.agent_id
        if not agent_id:
            return

        state = await self._registry.get(agent_id)
        if state:
            state.info.metadata["directive"] = "fallback_model"
            state.info.metadata["fallback_reason"] = diagnosis.root_cause[:100]
            await self._step(attempt, f"Fallback model directive queued for agent {agent_id}")

    async def _strategy_context_compaction(
        self, diagnosis: Diagnosis, attempt: RecoveryAttempt
    ) -> None:
        """
        Signal agent to compact its context immediately.
        Sends a compact_context directive via agent metadata.
        Agent's heartbeat loop delivers this on next tick.
        """
        agent_id = diagnosis.agent_id
        if not agent_id:
            return

        state = await self._registry.get(agent_id)
        if state:
            state.info.metadata["directive"] = "compact_context"
            await self._step(attempt, f"Context compaction directive queued for agent {agent_id}")

    async def _strategy_release_resources(
        self, diagnosis: Diagnosis, attempt: RecoveryAttempt
    ) -> None:
        """
        Force-release all resources held by the deadlocked agent.
        Clears resources_held and resources_waiting in AgentState.
        The ResourceMediator's lock is timeout-protected separately.
        """
        agent_id = diagnosis.agent_id
        if not agent_id:
            return

        state = await self._registry.get(agent_id)
        if state:
            held = list(state.resource_usage.resources_held)
            waiting = list(state.resource_usage.resources_waiting)
            state.resource_usage.resources_held = []
            state.resource_usage.resources_waiting = []
            await self._step(
                attempt,
                f"Force-released resources for {agent_id}: held={held}, waiting={waiting}",
            )

    async def _strategy_queue_for_later(
        self, diagnosis: Diagnosis, attempt: RecoveryAttempt
    ) -> None:
        """
        Move the agent's current task to the DLQ for later replay.
        Per zylos.md §Part8: "The DLQ ensures no work is silently dropped."
        """
        agent_id = diagnosis.agent_id
        if not agent_id:
            return

        state = await self._registry.get(agent_id)
        task_id = state.current_task_id if state else None

        if task_id:
            await self._task_manager.fail(task_id, f"Queued to DLQ by RecoveryEngine: {diagnosis.root_cause[:80]}")
            await self._step(attempt, f"Task {task_id} sent to DLQ for later replay")
        else:
            await self._step(attempt, f"Agent {agent_id} had no current task — nothing to DLQ")

    async def _strategy_escalate_human(
        self, diagnosis: Diagnosis, attempt: RecoveryAttempt
    ) -> None:
        """
        Emit HUMAN_GATE_REQUIRED event. BOSSman cannot resolve this automatically.
        Recovery stops here — a human must intervene.
        """
        agent_id = diagnosis.agent_id
        await self._bus.publish(
            BossmanEvent.create(
                EventType.HUMAN_GATE_REQUIRED,
                agent_id=agent_id,
                message=(
                    f"Human intervention required for agent {agent_id!r}: "
                    f"{diagnosis.root_cause[:120]}"
                ),
                severity=EventSeverity.CRITICAL,
                payload={
                    "failure_type": diagnosis.failure_type.value,
                    "root_cause": diagnosis.root_cause,
                    "incident_id": diagnosis.incident_id,
                    "rule_matched": diagnosis.rule_matched,
                    "needs_llm_review": diagnosis.needs_llm_review,
                },
            )
        )
        await self._step(attempt, f"Escalated to human: HUMAN_GATE_REQUIRED emitted")
        attempt.mark_complete(RecoveryOutcome.ESCALATED, notes=diagnosis.root_cause)

    # ── Restart budget ─────────────────────────────────────────────────────────

    def _check_restart_budget(self, agent_id: AgentId) -> bool:
        """Returns True if restart is within budget, False if budget exhausted."""
        now = time.monotonic()
        history = self._restart_history[agent_id]
        # Prune outside window
        while history and now - history[0] > self._restart_window:
            history.popleft()
        if len(history) >= self._max_restarts:
            return False
        history.append(now)
        return True

    def _restart_count(self, agent_id: AgentId) -> int:
        return len(self._restart_history[agent_id])

    # ── Event helpers ──────────────────────────────────────────────────────────

    async def _emit(
        self,
        event_type: EventType,
        attempt: RecoveryAttempt,
        diagnosis: Diagnosis,
    ) -> None:
        await self._bus.publish(
            BossmanEvent.create(
                event_type,
                agent_id=attempt.agent_id,
                message=(
                    f"[{attempt.failure_type.value}] {diagnosis.recommended_strategy.value} "
                    f"— incident {attempt.incident_id[:8]}"
                ),
                payload={
                    "incident_id": attempt.incident_id,
                    "failure_type": attempt.failure_type.value,
                    "strategy": attempt.strategy.value,
                    "confidence": diagnosis.confidence,
                },
            )
        )

    async def _emit_strategy_chosen(
        self, attempt: RecoveryAttempt, diagnosis: Diagnosis
    ) -> None:
        await self._bus.publish(
            BossmanEvent.create(
                EventType.RECOVERY_STRATEGY_CHOSEN,
                agent_id=attempt.agent_id,
                message=(
                    f"Strategy: {diagnosis.recommended_strategy.value} "
                    f"(confidence={diagnosis.confidence:.0%}, rule={diagnosis.rule_matched!r})"
                ),
                payload={
                    "incident_id": attempt.incident_id,
                    "strategy": diagnosis.recommended_strategy.value,
                    "confidence": diagnosis.confidence,
                    "rule_matched": diagnosis.rule_matched,
                    "needs_llm_review": diagnosis.needs_llm_review,
                },
            )
        )

    async def _step(self, attempt: RecoveryAttempt, message: str) -> None:
        """Publish a RECOVERY_STEP event for the dashboard timeline."""
        log.info("RecoveryEngine [%s]: %s", attempt.incident_id[:8], message)
        await self._bus.publish(
            BossmanEvent.create(
                EventType.RECOVERY_STEP,
                agent_id=attempt.agent_id,
                message=message,
                payload={"incident_id": attempt.incident_id, "step": message},
            )
        )

    # ── Introspection ──────────────────────────────────────────────────────────

    @property
    def stats(self) -> dict[str, Any]:
        return {
            "handled": self._handled,
            "skipped_duplicate": self._skipped_duplicate,
            "active_recoveries": list(self._active_recoveries),
            "circuit_breakers": self._circuit_breakers.snapshot(),
            "ledger": self.ledger.stats,
        }
