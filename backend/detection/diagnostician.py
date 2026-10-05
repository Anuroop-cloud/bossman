"""
detection.diagnostician
────────────────────────
Rule-Based Diagnostician — maps failure signals to structured diagnoses.

Phase 4 spec: "Hard-coded diagnostic rules"

The Diagnostician receives:
  - A DetectedFailure (from FailureDetector)
  - An optional telemetry snapshot (from registry + resource manager)

And returns a Diagnosis containing:
  - Confirmed FailureType (may refine the detector's classification)
  - Root cause description (human-readable, for audit log)
  - Recommended RecoveryStrategy (consumed by Recovery Engine, Phase 5)
  - Confidence score (0.0–1.0) — low confidence → escalate to LLM Diagnostician
  - Structured evidence dict (input for Phase 7 LLM Diagnostician)

Design: zylos.md §Part9 — Design Principles
  "Fail Fast, Fail Loudly — every failure should either succeed visibly,
   fail with a structured error, or enter a defined degraded state."

The Diagnostician enforces this by always producing a structured output,
even for UNKNOWN failures (confidence=0.0 → escalate to LLM in Phase 7).

Rule table (hard-coded, Phase 4):
────────────────────────────────────────────────────────────────────────
  Signal                        │ FailureType         │ Strategy
  ──────────────────────────────┼─────────────────────┼───────────────
  RETRY_STORM                   │ RETRY_STORM         │ CIRCUIT_BREAKER
  AGENT_TIMEOUT + missing HB    │ HEARTBEAT_TIMEOUT   │ RESTART_AGENT
  RESOURCE_STARVATION × N       │ RESOURCE_STARVATION │ WAIT_AND_RETRY
  DEADLOCK_SUSPECTED (circular) │ DEADLOCK            │ RELEASE_RESOURCES
  CASCADING_FAILURE (≥N agents) │ CASCADING_FAILURE   │ CIRCUIT_BREAKER
  CONTEXT_OVERFLOW (≥95%)       │ CONTEXT_OVERFLOW    │ CONTEXT_COMPACTION
  consecutive_failures ≥ 3      │ UNKNOWN             │ ESCALATE_HUMAN
  permission_denied             │ PERMISSION_VIOLATION│ ESCALATE_HUMAN
────────────────────────────────────────────────────────────────────────

The LLM Diagnostician (Phase 7) handles novel patterns not in this table.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

from contracts.agent_state import AgentId
from contracts.recovery_ledger import FailureType, RecoveryStrategy
from detection.failure_detector import DetectedFailure

log = logging.getLogger(__name__)

# Confidence below this → flag for LLM Diagnostician (Phase 7)
LLM_ESCALATION_THRESHOLD: float = 0.5


@dataclass
class Diagnosis:
    """
    Structured output of the Diagnostician for one failure.

    Consumed by:
      - Recovery Engine (Phase 5) — reads failure_type + recommended_strategy
      - Evaluator (Phase 6) — checks outcome against this baseline
      - LLM Diagnostician (Phase 7) — if confidence < LLM_ESCALATION_THRESHOLD
      - Dashboard — displayed on the Recovery Timeline
    """
    failure_type: FailureType
    root_cause: str
    recommended_strategy: RecoveryStrategy
    confidence: float          # 0.0 (no idea) → 1.0 (certain)
    agent_id: AgentId | None = None
    incident_id: str = ""
    evidence: dict[str, Any] = field(default_factory=dict)
    needs_llm_review: bool = False   # Set True when confidence < threshold
    rule_matched: str = ""           # Which rule fired (for audit log)

    def __post_init__(self) -> None:
        self.needs_llm_review = self.confidence < LLM_ESCALATION_THRESHOLD


# ── Rule definitions ──────────────────────────────────────────────────────────

@dataclass
class DiagnosticRule:
    """One entry in the rule table."""
    name: str
    failure_types: frozenset[FailureType]   # which FailureTypes this rule handles
    strategy: RecoveryStrategy
    confidence: float
    root_cause_template: str                # format string, gets .format(**evidence)

    def matches(self, failure: DetectedFailure) -> bool:
        return failure.failure_type in self.failure_types


# The hard-coded rule table (ordered — first match wins)
_RULES: list[DiagnosticRule] = [
    DiagnosticRule(
        name="retry_storm_circuit_break",
        failure_types=frozenset({FailureType.RETRY_STORM}),
        strategy=RecoveryStrategy.CIRCUIT_BREAKER,
        confidence=0.90,
        root_cause_template=(
            "Retry storm: agent {agent_id} generated {failure_count_in_window} failures "
            "in {window_seconds}s. Root cause: repeated calls to a failing dependency "
            "without circuit breaking. Action: open circuit, apply exponential backoff."
        ),
    ),
    DiagnosticRule(
        name="heartbeat_timeout_restart",
        failure_types=frozenset({FailureType.HEARTBEAT_TIMEOUT}),
        strategy=RecoveryStrategy.RESTART_AGENT,
        confidence=0.85,
        root_cause_template=(
            "Heartbeat timeout: agent {agent_id} silent for {heartbeat_age_seconds}s "
            "(threshold {threshold_seconds}s). Likely causes: infinite loop, "
            "deadlock inside agent, or process hang. Action: restart agent from checkpoint."
        ),
    ),
    DiagnosticRule(
        name="resource_starvation_backoff",
        failure_types=frozenset({FailureType.RESOURCE_STARVATION}),
        strategy=RecoveryStrategy.WAIT_AND_RETRY,
        confidence=0.80,
        root_cause_template=(
            "Resource starvation on '{bucket}': {starvation_count_in_window} events in "
            "{window_seconds}s. System is rate-limited. Action: exponential backoff, "
            "reduce token bucket consumption rate."
        ),
    ),
    DiagnosticRule(
        name="deadlock_release_resources",
        failure_types=frozenset({FailureType.DEADLOCK}),
        strategy=RecoveryStrategy.RELEASE_RESOURCES,
        confidence=0.88,
        root_cause_template=(
            "Deadlock: agent {agent_id} holds {resources_held} and waits for "
            "{resources_waiting}. Circular wait confirmed. Action: force-release "
            "all locks held by involved agents, then retry."
        ),
    ),
    DiagnosticRule(
        name="cascading_circuit_break",
        failure_types=frozenset({FailureType.CASCADING_FAILURE}),
        strategy=RecoveryStrategy.CIRCUIT_BREAKER,
        confidence=0.75,
        root_cause_template=(
            "Cascading failure: {failed_agent_count} agents failed in {window_seconds}s. "
            "Systemic issue — not isolated to one agent. Action: open circuit breaker, "
            "halt new task assignment, investigate root dependency."
        ),
    ),
    DiagnosticRule(
        name="context_overflow_compact",
        failure_types=frozenset({FailureType.CONTEXT_OVERFLOW}),
        strategy=RecoveryStrategy.CONTEXT_COMPACTION,
        confidence=0.95,
        root_cause_template=(
            "Context overflow imminent: agent {agent_id} at {context_utilisation:.0%} "
            "context utilisation. Action: trigger context compaction immediately before "
            "the model loses coherent access to task objectives."
        ),
    ),
    DiagnosticRule(
        name="permission_escalate",
        failure_types=frozenset({FailureType.PERMISSION_VIOLATION}),
        strategy=RecoveryStrategy.ESCALATE_HUMAN,
        confidence=0.99,
        root_cause_template=(
            "Permission violation: agent {agent_id} attempted an operation above "
            "its tier. Kiro-incident class error. Human review required before "
            "granting elevated permissions."
        ),
    ),
    DiagnosticRule(
        name="unknown_escalate_human",
        failure_types=frozenset({FailureType.UNKNOWN, FailureType.SILENT_DEGRADATION}),
        strategy=RecoveryStrategy.ESCALATE_HUMAN,
        confidence=0.30,
        root_cause_template=(
            "Unknown failure pattern for agent {agent_id}. Telemetry is ambiguous. "
            "Escalating to LLM Diagnostician (Phase 7) and human review."
        ),
    ),
]


# ── Diagnostician ─────────────────────────────────────────────────────────────

class Diagnostician:
    """
    Rule-based failure diagnostician.

    Applies the hard-coded rule table to a DetectedFailure and produces
    a structured Diagnosis. Phase 7 will layer an LLM on top of this
    for failures that don't match any rule (confidence < 0.5).

    Usage
    ─────
        diag = Diagnostician()
        diagnosis = diag.diagnose(detected_failure, telemetry_snapshot)
    """

    def __init__(self) -> None:
        self._diagnosis_count: int = 0
        self._rule_hits: dict[str, int] = {}
        self._low_confidence_count: int = 0

    def diagnose(
        self,
        failure: DetectedFailure,
        extra_telemetry: dict[str, Any] | None = None,
    ) -> Diagnosis:
        """
        Apply diagnostic rules to a DetectedFailure.

        Parameters
        ──────────
        failure:          The failure detected by FailureDetector.
        extra_telemetry:  Additional context (registry snapshot, resource snapshot).
                          Merged with failure.telemetry for rule templates.

        Returns
        ───────
        Diagnosis — always returns something, even for UNKNOWN failures.
        """
        self._diagnosis_count += 1
        evidence = {**failure.telemetry, **(extra_telemetry or {})}

        # Safe format with fallback for missing keys
        def safe_fmt(template: str) -> str:
            try:
                return template.format(
                    agent_id=failure.agent_id or "?",
                    **evidence,
                )
            except (KeyError, ValueError):
                return template  # return unformatted if keys missing

        # Walk the rule table — first match wins
        for rule in _RULES:
            if rule.matches(failure):
                self._rule_hits[rule.name] = self._rule_hits.get(rule.name, 0) + 1

                root_cause = safe_fmt(rule.root_cause_template)
                diag = Diagnosis(
                    failure_type=failure.failure_type,
                    root_cause=root_cause,
                    recommended_strategy=rule.strategy,
                    confidence=rule.confidence,
                    agent_id=failure.agent_id,
                    incident_id=failure.incident_id,
                    evidence=evidence,
                    rule_matched=rule.name,
                )
                if diag.needs_llm_review:
                    self._low_confidence_count += 1
                    log.info(
                        "Diagnostician: low confidence (%.2f) — flagged for LLM review (rule=%s)",
                        diag.confidence, rule.name,
                    )

                log.info(
                    "Diagnostician: rule=%r matched failure=%s → strategy=%s (confidence=%.2f)",
                    rule.name,
                    failure.failure_type.value,
                    rule.strategy.value,
                    rule.confidence,
                )
                return diag

        # No rule matched — fallback
        self._low_confidence_count += 1
        fallback = Diagnosis(
            failure_type=FailureType.UNKNOWN,
            root_cause=(
                f"No rule matched failure_type={failure.failure_type.value} "
                f"for agent {failure.agent_id!r}. Escalating to LLM Diagnostician."
            ),
            recommended_strategy=RecoveryStrategy.ESCALATE_HUMAN,
            confidence=0.0,
            agent_id=failure.agent_id,
            incident_id=failure.incident_id,
            evidence=evidence,
            rule_matched="none",
        )
        log.warning(
            "Diagnostician: no rule matched failure_type=%s for agent=%s",
            failure.failure_type.value, failure.agent_id,
        )
        return fallback

    def batch_diagnose(
        self,
        failures: list[DetectedFailure],
        extra_telemetry: dict[str, Any] | None = None,
    ) -> list[Diagnosis]:
        """Diagnose a batch of failures. Returns in same order."""
        return [self.diagnose(f, extra_telemetry) for f in failures]

    # ── Introspection ─────────────────────────────────────────────────────────

    @property
    def stats(self) -> dict[str, Any]:
        return {
            "total_diagnoses": self._diagnosis_count,
            "rule_hits": dict(self._rule_hits),
            "low_confidence_count": self._low_confidence_count,
            "llm_escalation_threshold": LLM_ESCALATION_THRESHOLD,
        }
