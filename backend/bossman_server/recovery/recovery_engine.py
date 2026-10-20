"""
recovery.recovery_engine
─────────────────────────
The BOSSman Recovery Engine.

The engine is the only component in the system that takes corrective action.
Everything before this point (detection, diagnosis) was read-only observation.

Architecture
────────────
RecoveryEngine:
  - Registers as FailureDetector.on_failure callback
  - Receives DetectedFailure from FailureDetector
  - Runs it through a Diagnostician (rule-based, or LLM-augmented — sync or async)
  - Dispatches to the appropriate strategy handler
  - Records every attempt in the RecoveryLedger
  - Publishes RECOVERY_* events throughout
  - Enforces restart budgets (max restarts per window) per zylos.md §Part2

Actuation
─────────
A remote agent cannot be fixed by editing BOSSman's copy of its state, so
every strategy that needs the agent's cooperation is delivered as a
Directive, and every strategy BOSSman can enforce on its own is enforced:

  WAIT_AND_RETRY       — exponential backoff with FULL JITTER, then RECOVERING → IDLE
  RESTART_AGENT        — supervisor-tree aware; managed/k8s agents are really
                         restarted, attached agents get a RESTART directive;
                         in-flight tasks are requeued and lock leases revoked
  REASSIGN_TASK        — move the task to a healthy agent of the same role
  ROLLBACK_CHECKPOINT  — ROLLBACK_CHECKPOINT directive (resume from checkpoint)
  CIRCUIT_BREAKER      — open the breaker for the failing dependency, fleet-wide
  FALLBACK_MODEL       — FALLBACK_MODEL directive (degradation ladder level 1)
  CONTEXT_COMPACTION   — COMPACT_CONTEXT directive
  RELEASE_RESOURCES    — revoke every lock lease / mediator lock the agent holds
  QUEUE_FOR_LATER      — current task → DLQ
  ESCALATE_HUMAN       — HUMAN_GATE_REQUIRED (and PAUSE for trust violations)

Restart budget (zylos.md §Part2 — Supervisor Tree):
  "If the child restarts more than MaxRestarts times within MaxTime seconds,
   the supervisor itself terminates and propagates the failure to its parent."
  → supervised agents escalate through control.supervisors; unsupervised
    agents ESCALATE_HUMAN when their own budget is exhausted.
"""

from __future__ import annotations

import asyncio
import inspect
import logging
import random
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable

from bossman_server.contracts.agent_state import AgentId, AgentState, AgentStatus
from bossman_server.contracts.directives import DirectiveType
from bossman_server.contracts.events import BossmanEvent, EventSeverity, EventType
from bossman_server.contracts.recovery_ledger import (
    FailureType,
    RecoveryAttempt,
    RecoveryOutcome,
    RecoveryStrategy,
)
from bossman_server.core.event_bus import EventBus
from bossman_server.core.registry import AgentRegistry
from bossman_server.core.task_manager import TaskManager
from bossman_server.detection.diagnostician import Diagnosis, Diagnostician
from bossman_server.detection.failure_detector import DetectedFailure
from bossman_server.recovery.circuit_breaker import CircuitBreakerRegistry

log = logging.getLogger(__name__)

# Restart budget: per-agent max restarts within a sliding window
DEFAULT_MAX_RESTARTS: int = 3
DEFAULT_RESTART_WINDOW: float = 60.0

# Exponential backoff (WAIT_AND_RETRY) — full jitter in [0, min(cap, base·2^n)]
BACKOFF_BASE_SECONDS: float = 2.0
BACKOFF_MAX_SECONDS: float = 60.0

# Failures where the agent itself may be acting outside its mandate.
_PAUSE_ON_ESCALATION = {FailureType.PERMISSION_VIOLATION, FailureType.CREDENTIAL_FAILURE}

# A restarter performs a *process-level* restart (managed process, k8s pod).
# Returns True when it handled the agent.
Restarter = Callable[[AgentState, str], Awaitable[bool]]


@dataclass
class RecoveryLedger:
    """
    In-memory recovery ledger. Append-only.
    infra.persistent_ledger.PersistentLedger mirrors it to PostgreSQL.
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
        diagnostician: Any,
        circuit_breakers: CircuitBreakerRegistry,
        *,
        max_restarts: int = DEFAULT_MAX_RESTARTS,
        restart_window_seconds: float = DEFAULT_RESTART_WINDOW,
        directives: Any | None = None,       # core.directives.DirectiveQueue
        lock_service: Any | None = None,     # control.locks.LockService
        mediator: Any | None = None,         # resources.mediator.ResourceMediator
        circuit_service: Any | None = None,  # control.circuits.CircuitService
        supervisors: Any | None = None,      # control.supervisors.SupervisorTree
        restarter: Restarter | None = None,
    ) -> None:
        self._registry = registry
        self._task_manager = task_manager
        self._bus = bus
        self._diagnostician = diagnostician
        self._circuit_breakers = circuit_breakers
        self._max_restarts = max_restarts
        self._restart_window = restart_window_seconds
        self._directives = directives
        self._locks = lock_service
        self._mediator = mediator
        self._circuit_service = circuit_service
        self._supervisors = supervisors
        self._restarter = restarter

        self.ledger = RecoveryLedger()

        # Restart budget tracking: agent_id → deque of restart timestamps
        self._restart_history: dict[AgentId, deque[float]] = defaultdict(deque)

        # In-flight recovery locks: prevent concurrent recovery for same agent
        self._active_recoveries: set[AgentId] = set()

        # Stats
        self._handled: int = 0
        self._skipped_duplicate: int = 0

    def attach(self, **deps: Any) -> None:
        """Late wiring of optional actuators (runtime composition)."""
        for name in ("directives", "locks", "mediator", "circuit_service", "supervisors", "restarter"):
            key = "lock_service" if name == "locks" else name
            if key in deps and deps[key] is not None:
                setattr(self, f"_{name}", deps[key])

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

    async def _diagnose(self, detected: DetectedFailure) -> Diagnosis:
        result = self._diagnostician.diagnose(detected)
        if inspect.isawaitable(result):
            result = await result
        return result

    async def _execute_recovery(self, detected: DetectedFailure) -> RecoveryAttempt:
        """Diagnose → plan → execute → record."""
        diagnosis = await self._diagnose(detected)
        state = await self._registry.get(detected.agent_id) if detected.agent_id else None

        attempt = RecoveryAttempt(
            incident_id=detected.incident_id,
            agent_id=detected.agent_id or AgentId("unknown"),
            task_id=state.current_task_id if state else detected.telemetry.get("task_id"),
            failure_type=detected.failure_type,
            failure_detail=detected.detail,
            strategy=diagnosis.recommended_strategy,
            attempt_number=len(self.ledger.for_incident(detected.incident_id)) + 1,
            telemetry_snapshot=detected.telemetry,
        )

        await self._emit(EventType.RECOVERY_STARTED, attempt, diagnosis)
        await self._emit_strategy_chosen(attempt, diagnosis)

        try:
            await self._dispatch(diagnosis, attempt)
            if attempt.outcome == RecoveryOutcome.IN_PROGRESS:
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

    async def _directive(self, agent_id: AgentId, dtype: DirectiveType, attempt: RecoveryAttempt,
                         reason: str, params: dict[str, Any] | None = None) -> None:
        if self._directives is None:
            return
        state = await self._registry.get(agent_id)
        if state is None or state.info.transport != "remote":
            return  # in-process agents read the flag from their own state
        await self._directives.issue(agent_id, dtype, params or {}, reason=reason,
                                     incident_id=attempt.incident_id, namespace=state.info.namespace)
        await self._step(attempt, f"{dtype.value} directive sent to {agent_id}")

    # ── Strategy handlers ──────────────────────────────────────────────────────

    async def _strategy_wait_and_retry(
        self, diagnosis: Diagnosis, attempt: RecoveryAttempt
    ) -> None:
        """
        Exponential backoff with full jitter + transition agent to RECOVERING.
        zylos.md §Part1: "exponential backoff with jitter reduces retry storms
        by 60-80% versus fixed-interval retries" — full jitter de-synchronises
        a herd of agents that all failed at the same moment.
        """
        agent_id = diagnosis.agent_id
        if not agent_id:
            return

        prior = len(self.ledger.for_agent(agent_id))
        cap = min(BACKOFF_BASE_SECONDS * (2 ** prior), BACKOFF_MAX_SECONDS)
        delay = random.uniform(0, cap)

        await self._step(attempt, f"Backoff delay: {delay:.1f}s (full jitter, cap {cap:.0f}s, attempt #{prior + 1})")
        log.info("RecoveryEngine: WAIT_AND_RETRY agent=%s delay=%.1fs", agent_id, delay)

        state = await self._registry.get(agent_id)
        if state and state.status in {AgentStatus.STUCK, AgentStatus.FAILED, AgentStatus.RUNNING}:
            try:
                await self._registry.update_status(agent_id, AgentStatus.RECOVERING)
            except ValueError:
                pass  # already in a compatible state

        await asyncio.sleep(delay)

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
        Supervised agents follow their supervisor's strategy and budget; the
        rest use the per-agent budget. Budget exhausted → ESCALATE_HUMAN.
        """
        agent_id = diagnosis.agent_id
        if not agent_id:
            return
        state = await self._registry.get(agent_id)

        if self._supervisors is not None and state is not None and state.info.supervisor:
            plan = await self._supervisors.on_child_failure(agent_id, diagnosis.root_cause)
            await self._step(attempt, plan.describe())
            if plan.escalate:
                attempt.strategy = RecoveryStrategy.ESCALATE_HUMAN
                await self._strategy_escalate_human(diagnosis, attempt, note=plan.describe())
                return
            for target in plan.restart:
                await self.restart_agent(target, attempt, reason=diagnosis.root_cause)
            return

        if not self._check_restart_budget(agent_id):
            log.warning(
                "RecoveryEngine: restart budget exhausted for agent %s — escalating",
                agent_id,
            )
            await self._step(attempt, f"Restart budget exceeded for {agent_id} — escalating to human")
            escalate_diag = Diagnosis(
                failure_type=diagnosis.failure_type,
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

        await self.restart_agent(agent_id, attempt, reason=diagnosis.root_cause)

    async def restart_agent(self, agent_id: AgentId, attempt: RecoveryAttempt | None, *, reason: str) -> bool:
        """Actuate one restart (no budget check — callers own the budget)."""
        state = await self._registry.get(agent_id)
        if state is None:
            log.error("RecoveryEngine: agent %s not found in registry", agent_id)
            return False

        step = (lambda m: self._step(attempt, m)) if attempt else (lambda m: asyncio.sleep(0))
        await step(f"Transitioning agent {agent_id} to RECOVERING")
        try:
            if state.status != AgentStatus.RECOVERING:
                await self._registry.update_status(agent_id, AgentStatus.RECOVERING)
        except ValueError as e:
            log.warning("RecoveryEngine: cannot transition to RECOVERING: %s", e)
            return False

        # Work and locks the agent held are no longer trustworthy.
        requeued = await self._task_manager.release_agent_tasks(agent_id, f"agent restarted: {reason[:120]}")
        if requeued:
            await step(f"Requeued {len(requeued)} in-flight task(s) of {agent_id}")
        await self._revoke(agent_id, step)

        state.resource_usage.resources_held = []
        state.resource_usage.resources_waiting = []
        state.resource_usage.context_utilisation = 0.0
        state.current_step = None
        state.current_task_id = None
        await self._registry.sync(agent_id)

        process_restart = False
        if self._restarter is not None:
            try:
                process_restart = await self._restarter(state, reason)
            except Exception as exc:  # noqa: BLE001
                await step(f"Process restart failed: {exc}")
        if process_restart:
            await step(f"Process for {agent_id} restarted (managed)")
        elif state.info.transport == "remote" and self._directives is not None:
            await self._directives.issue(agent_id, DirectiveType.RESTART, {}, reason=reason[:200],
                                         incident_id=attempt.incident_id if attempt else None,
                                         namespace=state.info.namespace)
            await step(f"RESTART directive sent to {agent_id}")
        else:
            await asyncio.sleep(0.1)  # in-process: state reset is the restart

        state = await self._registry.get(agent_id)
        if state is not None and state.status == AgentStatus.RECOVERING:
            await self._registry.update_status(agent_id, AgentStatus.IDLE)
        await step(f"Agent {agent_id} restarted: RECOVERING → IDLE")

        await self._bus.publish(
            BossmanEvent.create(
                EventType.AGENT_RESTARTED,
                agent_id=agent_id,
                namespace=state.info.namespace if state else None,
                message=f"Agent {agent_id} restarted by RecoveryEngine (restart_count={self._restart_count(agent_id)})",
                payload={"restart_count": self._restart_count(agent_id), "process_restart": process_restart,
                         "incident_id": attempt.incident_id if attempt else None},
            )
        )
        return True

    async def _revoke(self, agent_id: AgentId, step: Callable[[str], Awaitable[Any]]) -> list[str]:
        freed: list[str] = []
        if self._locks is not None:
            freed += await self._locks.revoke_agent(agent_id, reason="recovery")
        if self._mediator is not None:
            freed += await self._mediator.force_release(agent_id)
        if freed:
            await step(f"Revoked lock(s) held by {agent_id}: {sorted(set(freed))}")
        return freed

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

        candidates = await self._registry.by_role(state.info.role, state.info.namespace)
        healthy = [
            c for c in candidates
            if c.agent_id != agent_id and c.status == AgentStatus.IDLE and not c.paused
        ]

        if not healthy:
            await self._step(attempt, f"No healthy {state.info.role!r} agents — queueing task for later")
            await self._strategy_queue_for_later(diagnosis, attempt)
            return

        target = healthy[0]
        await self._task_manager.reassign(task_id, target.agent_id, reason=f"Recovery: agent {agent_id} failed")
        state.current_task_id = None
        await self._registry.sync(agent_id)
        await self._step(attempt, f"Task {task_id} reassigned from {agent_id} → {target.agent_id}")

    async def _strategy_rollback_checkpoint(
        self, diagnosis: Diagnosis, attempt: RecoveryAttempt
    ) -> None:
        """Signal the agent to resume from its last good checkpoint."""
        agent_id = diagnosis.agent_id
        if not agent_id:
            return
        state = await self._registry.get(agent_id)
        if state:
            state.info.metadata["directive"] = "rollback_checkpoint"
            await self._registry.sync(agent_id)
            await self._step(attempt, f"Rollback checkpoint directive queued for agent {agent_id}")
            await self._directive(agent_id, DirectiveType.ROLLBACK_CHECKPOINT, attempt, diagnosis.root_cause,
                                  {"task_id": state.current_task_id})

    async def _strategy_circuit_breaker(
        self, diagnosis: Diagnosis, attempt: RecoveryAttempt
    ) -> None:
        """
        Open the circuit breaker for the failing dependency (or the agent).

        zylos.md §Part2: "Failing fast when a model API is degraded — and
        routing to a fallback model — is far better than accumulating request
        timeouts that block the agent's execution pipeline."
        """
        agent_id = diagnosis.agent_id
        ev = diagnosis.evidence or {}
        dependency = ev.get("dependency") or ev.get("circuit_key") or ev.get("tool")
        key = str(dependency or agent_id or "global")
        cb = self._circuit_breakers.open(key, agent_id=agent_id)
        if self._circuit_service is not None:
            await self._circuit_service.force_open(key, reason=diagnosis.root_cause[:120], agent_id=agent_id)
        await self._step(
            attempt,
            f"Circuit breaker '{key}' OPENED "
            f"(will probe after {cb._open_timeout:.0f}s)",
        )

    async def _strategy_fallback_model(
        self, diagnosis: Diagnosis, attempt: RecoveryAttempt
    ) -> None:
        """Signal the agent to switch to its fallback model (degradation level 1)."""
        agent_id = diagnosis.agent_id
        if not agent_id:
            return

        state = await self._registry.get(agent_id)
        if state:
            state.info.metadata["directive"] = "fallback_model"
            state.info.metadata["fallback_reason"] = diagnosis.root_cause[:100]
            await self._registry.sync(agent_id)
            await self._step(attempt, f"Fallback model directive queued for agent {agent_id}")
            await self._directive(agent_id, DirectiveType.FALLBACK_MODEL, attempt, diagnosis.root_cause)

    async def _strategy_context_compaction(
        self, diagnosis: Diagnosis, attempt: RecoveryAttempt
    ) -> None:
        """Signal the agent to compact its context immediately."""
        agent_id = diagnosis.agent_id
        if not agent_id:
            return

        state = await self._registry.get(agent_id)
        if state:
            state.info.metadata["directive"] = "compact_context"
            await self._registry.sync(agent_id)
            await self._step(attempt, f"Context compaction directive queued for agent {agent_id}")
            await self._directive(agent_id, DirectiveType.COMPACT_CONTEXT, attempt, diagnosis.root_cause)

    async def _strategy_release_resources(
        self, diagnosis: Diagnosis, attempt: RecoveryAttempt
    ) -> None:
        """
        Force-release all resources held by the deadlocked agent — the real
        leases/locks, not just BOSSman's accounting of them.
        """
        agent_id = diagnosis.agent_id
        if not agent_id:
            return

        await self._revoke(agent_id, lambda m: self._step(attempt, m))
        state = await self._registry.get(agent_id)
        if state:
            held = list(state.resource_usage.resources_held)
            waiting = list(state.resource_usage.resources_waiting)
            state.resource_usage.resources_held = []
            state.resource_usage.resources_waiting = []
            await self._registry.sync(agent_id)
            await self._step(
                attempt,
                f"Force-released resources for {agent_id}: held={held}, waiting={waiting}",
            )
            await self._directive(agent_id, DirectiveType.RELEASE_RESOURCES, attempt, diagnosis.root_cause)

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
            await self._task_manager.send_to_dlq(task_id, f"Queued to DLQ by RecoveryEngine: {diagnosis.root_cause[:80]}")
            await self._step(attempt, f"Task {task_id} sent to DLQ for later replay")
        else:
            await self._step(attempt, f"Agent {agent_id} had no current task — nothing to DLQ")

    async def _strategy_escalate_human(
        self, diagnosis: Diagnosis, attempt: RecoveryAttempt, note: str | None = None
    ) -> None:
        """
        Emit HUMAN_GATE_REQUIRED event. BOSSman cannot resolve this automatically.
        Trust violations also pause the agent so it cannot keep acting.
        """
        agent_id = diagnosis.agent_id
        if agent_id and diagnosis.failure_type in _PAUSE_ON_ESCALATION:
            await self._directive(agent_id, DirectiveType.PAUSE, attempt,
                                  f"paused pending human review: {diagnosis.root_cause[:120]}")
        await self._bus.publish(
            BossmanEvent.create(
                EventType.HUMAN_GATE_REQUIRED,
                agent_id=agent_id,
                message=(
                    f"Human intervention required for agent {agent_id!r}: "
                    f"{(note or diagnosis.root_cause)[:120]}"
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
        await self._step(attempt, "Escalated to human: HUMAN_GATE_REQUIRED emitted")
        attempt.mark_complete(RecoveryOutcome.ESCALATED, notes=note or diagnosis.root_cause)

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
                task_id=attempt.task_id,
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

    async def _step(self, attempt: RecoveryAttempt | None, message: str) -> None:
        """Publish a RECOVERY_STEP event for the dashboard timeline."""
        if attempt is None:
            return
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
