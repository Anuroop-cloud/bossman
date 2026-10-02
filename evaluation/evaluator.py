"""
evaluation.evaluator
─────────────────────
The BOSSman Evaluator — recovery verification loop.

After RecoveryEngine executes a strategy and leaves an attempt as IN_PROGRESS,
the Evaluator's job is to answer:

  "Did the recovery actually work?"

It does this by:
  1. Subscribing to RECOVERY_STARTED events on the EventBus
  2. Waiting verification_delay_seconds for the strategy to take effect
  3. Re-running L1-L4 health checks on the affected agent
  4. Updating the RecoveryAttempt in the ledger: SUCCESS / FAILURE / PARTIAL
  5. Publishing EVALUATION_PASSED or EVALUATION_FAILED events

If the evaluation fails (recovery did not work), it emits EVALUATION_FAILED so
the RecoveryEngine or a human operator can decide on escalation.

zylos.md §Part9 — Verifiable Recovery:
  "Every recovery action must have a measurable success criterion that can
   be evaluated before and after. Without this, the system can loop
   indefinitely in a 'recovering' state."
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from typing import Any

from contracts.agent_state import AgentId
from contracts.events import BossmanEvent, EventType
from contracts.recovery_ledger import RecoveryAttempt, RecoveryOutcome
from core.event_bus import EventBus
from core.registry import AgentRegistry
from evaluation.health_checker import AgentHealthReport, HealthCheckStatus, HealthChecker
from recovery.recovery_engine import RecoveryLedger

log = logging.getLogger(__name__)

# How long to wait after RECOVERY_STARTED before checking health
DEFAULT_VERIFICATION_DELAY: float = 2.0
DEFAULT_VERIFICATION_RETRIES: int = 3
DEFAULT_RETRY_INTERVAL: float = 1.0


@dataclass
class EvaluationResult:
    """The Evaluator's verdict on a recovery attempt."""
    incident_id: str
    agent_id: AgentId
    passed: bool
    health_report: AgentHealthReport
    attempt: RecoveryAttempt | None = None
    notes: str = ""
    retry_count: int = 0


class Evaluator:
    """
    Recovery verification loop.

    Wired to the EventBus, subscribes to RECOVERY_STARTED, waits, then
    re-runs L1-L4 health checks on the recovered agent.

    Usage
    ─────
        evaluator = Evaluator(
            registry=registry,
            bus=bus,
            ledger=engine.ledger,
            checker=HealthChecker(),
        )
        await evaluator.start()
    """

    def __init__(
        self,
        registry: AgentRegistry,
        bus: EventBus,
        ledger: RecoveryLedger,
        checker: HealthChecker | None = None,
        *,
        verification_delay_seconds: float = DEFAULT_VERIFICATION_DELAY,
        verification_retries: int = DEFAULT_VERIFICATION_RETRIES,
        retry_interval_seconds: float = DEFAULT_RETRY_INTERVAL,
    ) -> None:
        self._registry = registry
        self._bus = bus
        self._ledger = ledger
        self._checker = checker or HealthChecker()
        self._delay = verification_delay_seconds
        self._retries = verification_retries
        self._retry_interval = retry_interval_seconds

        # All evaluation results this session
        self._results: list[EvaluationResult] = []
        self._pass_count = 0
        self._fail_count = 0
        self._running = False

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def start(self) -> None:
        self._bus.subscribe(self._on_event)
        self._running = True
        log.info("Evaluator started — monitoring RECOVERY_STARTED events")

    async def stop(self) -> None:
        self._bus.unsubscribe(self._on_event)
        self._running = False
        log.info("Evaluator stopped")

    # ── Event handler ─────────────────────────────────────────────────────────

    async def _on_event(self, event: BossmanEvent) -> None:
        if not self._running:
            return
        if event.event_type == EventType.RECOVERY_STARTED:
            # Schedule verification without blocking the subscriber
            asyncio.ensure_future(self._schedule_verification(event))

    async def _schedule_verification(self, event: BossmanEvent) -> None:
        """Wait for the recovery strategy to take effect, then verify."""
        agent_id = event.agent_id
        incident_id = event.payload.get("incident_id", "")

        if not agent_id:
            return

        log.info(
            "Evaluator: scheduled verification for agent=%s (incident=%s) in %.1fs",
            agent_id, incident_id[:8], self._delay,
        )
        await asyncio.sleep(self._delay)
        await self._verify(agent_id, incident_id)

    # ── Verification ──────────────────────────────────────────────────────────

    async def _verify(self, agent_id: AgentId, incident_id: str) -> EvaluationResult:
        """Run L1-L4 health checks with retry logic."""
        attempt = self._find_attempt(incident_id)

        for retry in range(self._retries + 1):
            state = await self._registry.get(agent_id)
            if state is None:
                log.warning("Evaluator: agent %s not found in registry", agent_id)
                # Agent was removed — treat as FAILURE
                break

            report = self._checker.check(state)

            if report.passed or report.overall_status == HealthCheckStatus.WARN:
                # PASS or just WARN — consider recovery successful
                result = EvaluationResult(
                    incident_id=incident_id,
                    agent_id=agent_id,
                    passed=True,
                    health_report=report,
                    attempt=attempt,
                    notes=f"L1-L4 check passed after {retry} retries",
                    retry_count=retry,
                )
                await self._record(result, attempt, RecoveryOutcome.SUCCESS)
                return result

            if retry < self._retries:
                log.info(
                    "Evaluator: agent=%s check FAILED (retry %d/%d) — waiting %.1fs",
                    agent_id, retry + 1, self._retries, self._retry_interval,
                )
                await asyncio.sleep(self._retry_interval)

        # All retries exhausted — recovery failed
        state = await self._registry.get(agent_id)
        report = self._checker.check(state) if state else AgentHealthReport(agent_id=agent_id)
        result = EvaluationResult(
            incident_id=incident_id,
            agent_id=agent_id,
            passed=False,
            health_report=report,
            attempt=attempt,
            notes=f"L1-L4 check failed after {self._retries} retries",
            retry_count=self._retries,
        )
        await self._record(result, attempt, RecoveryOutcome.FAILURE)
        return result

    # ── Direct evaluation (called from tests / manual triggers) ───────────────

    async def evaluate_now(self, agent_id: AgentId, incident_id: str = "") -> EvaluationResult:
        """
        Immediately run L1-L4 checks without waiting for RECOVERY_STARTED event.
        Used by tests and manual recovery triggers.
        """
        return await self._verify(agent_id, incident_id)

    # ── Record result ─────────────────────────────────────────────────────────

    async def _record(
        self,
        result: EvaluationResult,
        attempt: RecoveryAttempt | None,
        outcome: RecoveryOutcome,
    ) -> None:
        """Update ledger entry, publish event, append to results list."""
        self._results.append(result)

        if result.passed:
            self._pass_count += 1
        else:
            self._fail_count += 1

        # Update ledger entry if found
        if attempt is not None:
            attempt.mark_complete(
                outcome,
                notes=result.notes,
                verified=True,
            )

        # Publish evaluation event
        event_type = (
            EventType.EVALUATION_PASSED if result.passed else EventType.EVALUATION_FAILED
        )
        log.info(
            "Evaluator: agent=%s incident=%s → %s (retries=%d)",
            result.agent_id, result.incident_id[:8], outcome.value, result.retry_count,
        )
        await self._bus.publish(
            BossmanEvent.create(
                event_type,
                agent_id=result.agent_id,
                message=(
                    f"Evaluation {'PASSED' if result.passed else 'FAILED'} "
                    f"for agent {result.agent_id!r} "
                    f"(incident={result.incident_id[:8]}, retries={result.retry_count})"
                ),
                payload={
                    "incident_id": result.incident_id,
                    "passed": result.passed,
                    "outcome": outcome.value,
                    "health_summary": result.health_report.as_dict(),
                    "notes": result.notes,
                },
            )
        )

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _find_attempt(self, incident_id: str) -> RecoveryAttempt | None:
        entries = self._ledger.for_incident(incident_id)
        return entries[-1] if entries else None

    # ── Introspection ─────────────────────────────────────────────────────────

    @property
    def stats(self) -> dict[str, Any]:
        return {
            "total_evaluations": len(self._results),
            "pass_count": self._pass_count,
            "fail_count": self._fail_count,
            "pass_rate": (
                self._pass_count / len(self._results) if self._results else None
            ),
            "recent": [
                {
                    "incident_id": r.incident_id[:8],
                    "agent_id": r.agent_id,
                    "passed": r.passed,
                    "retries": r.retry_count,
                }
                for r in self._results[-5:]
            ],
        }
