"""
detection.llm_diagnostician
─────────────────────────────
Phase 7: LLM Diagnostician — structured telemetry → LLM interpretation.

When and why
────────────
The rule-based Diagnostician (Phase 4) covers all known failure patterns
with high confidence. But production AI systems produce novel failures that
no rule table anticipates — new tool behaviors, unexpected LLM responses,
emergent multi-agent coordination patterns.

The LLM Diagnostician activates ONLY when:
  diagnosis.needs_llm_review is True  (confidence < 0.5 from rule table)

It does NOT replace the rule-based diagnostician — it augments it:
  Known pattern      → rule table (fast, deterministic, free)
  Novel/ambiguous    → LLM Diagnostician (slower, non-deterministic, costs tokens)

Architecture
────────────
  LLMDiagnostician:
    1. Receives DetectedFailure + base Diagnosis from rule-based diagnostician
    2. Builds a structured JSON prompt (all telemetry, failure type, evidence)
    3. Calls the injected llm_call coroutine (decoupled from any specific LLM)
    4. Parses the structured JSON response
    5. Returns an enhanced Diagnosis with:
       - Possibly refined FailureType
       - Possibly refined RecoveryStrategy
       - LLM root cause narrative
       - Updated confidence (from LLM self-assessment)
       - llm_used=True flag
    6. On any LLM failure (timeout, malformed response, rate limit):
       - Falls back to the base diagnosis
       - Sets llm_error field for audit trail
       - Never raises to caller

Prompt design
─────────────
The prompt is deterministic and structured. It produces a JSON response.
No free-form narrative extraction — the LLM must respond in schema.

zylos.md §Part7 — LLM as Diagnostician:
  "The LLM's role in recovery is not to replace structured rules but to
   handle the long tail of failure patterns that rules cannot anticipate."

Injected interface
──────────────────
The llm_call parameter is a coroutine:
  async def llm_call(prompt: str) -> str

This decouples LLMDiagnostician from any specific provider (Anthropic, OpenAI,
local model). In tests, inject a mock. In production, inject a real client.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Any, Awaitable, Callable

from contracts.recovery_ledger import FailureType, RecoveryStrategy
from detection.diagnostician import Diagnosis, Diagnostician
from detection.failure_detector import DetectedFailure

log = logging.getLogger(__name__)

# Type alias for the injected LLM callable
LLMCall = Callable[[str], Awaitable[str]]

# Timeout for LLM calls (seconds) — fail fast, fall back to rule-based result
LLM_CALL_TIMEOUT_SECONDS: float = 15.0

# System prompt injected before user content
_SYSTEM_PROMPT = """\
You are BOSSman's LLM Diagnostician — an expert in AI agent failure analysis.
You receive structured telemetry from a self-healing AI agent supervisor.
Your job: diagnose ambiguous or novel failures that the rule-based system could not classify with high confidence.

You MUST respond with valid JSON only — no explanation outside the JSON block.
Schema:
{
  "failure_type": "<one of the FailureType enum values>",
  "recovery_strategy": "<one of the RecoveryStrategy enum values>",
  "root_cause": "<concise human-readable root cause, max 200 chars>",
  "confidence": <float 0.0-1.0>,
  "reasoning": "<brief chain-of-thought, max 300 chars>"
}

FailureType values: HEARTBEAT_TIMEOUT, TASK_TIMEOUT, RETRY_STORM, DEADLOCK,
RESOURCE_STARVATION, CONTEXT_OVERFLOW, CASCADING_FAILURE, SILENT_DEGRADATION,
EXTERNAL_SERVICE_FAILURE, PERMISSION_VIOLATION, DUPLICATE_SPAWN, UNKNOWN

RecoveryStrategy values: WAIT_AND_RETRY, RESTART_AGENT, REASSIGN_TASK,
ROLLBACK_CHECKPOINT, CIRCUIT_BREAKER, FALLBACK_MODEL, CONTEXT_COMPACTION,
RELEASE_RESOURCES, QUEUE_FOR_LATER, ESCALATE_HUMAN
"""


def build_prompt(
    failure: DetectedFailure,
    base_diagnosis: Diagnosis,
    extra_telemetry: dict[str, Any] | None = None,
) -> str:
    """
    Build a deterministic, structured prompt for the LLM Diagnostician.

    The prompt contains:
    - Detected failure type and agent ID
    - Full telemetry snapshot from detection
    - Base diagnosis from rule table (what the rules said, why confidence is low)
    - Instructions to respond in JSON schema
    """
    telemetry = {**failure.telemetry, **(extra_telemetry or {})}

    payload = {
        "detected_failure": {
            "failure_type": failure.failure_type.value,
            "agent_id": failure.agent_id,
            "detail": failure.detail,
            "incident_id": failure.incident_id,
        },
        "telemetry_snapshot": telemetry,
        "base_diagnosis": {
            "failure_type": base_diagnosis.failure_type.value,
            "root_cause": base_diagnosis.root_cause,
            "recommended_strategy": base_diagnosis.recommended_strategy.value,
            "confidence": base_diagnosis.confidence,
            "rule_matched": base_diagnosis.rule_matched,
            "needs_llm_review": base_diagnosis.needs_llm_review,
        },
    }

    return (
        f"{_SYSTEM_PROMPT}\n\n"
        f"--- TELEMETRY ---\n"
        f"{json.dumps(payload, indent=2, default=str)}\n"
        f"--- END TELEMETRY ---\n\n"
        f"Diagnose this failure and respond with JSON only."
    )


def _parse_llm_response(
    response: str,
    base_diagnosis: Diagnosis,
    failure: DetectedFailure,
) -> Diagnosis:
    """
    Parse the LLM's JSON response into an enhanced Diagnosis.
    Falls back to base diagnosis values for any missing/invalid fields.
    """
    try:
        # Strip markdown code fences if present
        text = response.strip()
        if text.startswith("```"):
            lines = text.split("\n")
            text = "\n".join(
                line for line in lines
                if not line.startswith("```")
            )

        data = json.loads(text)
    except json.JSONDecodeError as exc:
        log.warning("LLMDiagnostician: JSON parse failed: %s", exc)
        raise ValueError(f"LLM returned non-JSON response: {response[:80]}") from exc

    # Parse FailureType with fallback
    raw_ft = data.get("failure_type", base_diagnosis.failure_type.value)
    try:
        failure_type = FailureType(raw_ft)
    except ValueError:
        log.warning("LLMDiagnostician: unknown FailureType %r — using base", raw_ft)
        failure_type = base_diagnosis.failure_type

    # Parse RecoveryStrategy with fallback
    raw_rs = data.get("recovery_strategy", base_diagnosis.recommended_strategy.value)
    try:
        strategy = RecoveryStrategy(raw_rs)
    except ValueError:
        log.warning("LLMDiagnostician: unknown RecoveryStrategy %r — using base", raw_rs)
        strategy = base_diagnosis.recommended_strategy

    # Clamp confidence to [0.0, 1.0]
    raw_conf = data.get("confidence", base_diagnosis.confidence)
    try:
        confidence = max(0.0, min(1.0, float(raw_conf)))
    except (TypeError, ValueError):
        confidence = base_diagnosis.confidence

    root_cause = str(data.get("root_cause", base_diagnosis.root_cause))[:300]
    reasoning = str(data.get("reasoning", ""))[:300]

    return Diagnosis(
        failure_type=failure_type,
        root_cause=root_cause,
        recommended_strategy=strategy,
        confidence=confidence,
        agent_id=failure.agent_id,
        incident_id=failure.incident_id,
        evidence={
            **base_diagnosis.evidence,
            "llm_used": True,
            "llm_reasoning": reasoning,
            "base_rule_matched": base_diagnosis.rule_matched,
            "base_confidence": base_diagnosis.confidence,
        },
        rule_matched=f"llm:{base_diagnosis.rule_matched}",
    )


class LLMDiagnostician:
    """
    LLM-augmented failure diagnostician.

    Drop-in replacement for Diagnostician — has the same diagnose() interface,
    but for low-confidence cases it calls the injected LLM before returning.

    Usage
    ─────
        from langchain_anthropic import ChatAnthropic

        llm = ChatAnthropic(model="claude-haiku-3-5")

        async def llm_call(prompt: str) -> str:
            response = await llm.ainvoke(prompt)
            return response.content

        diagnostician = LLMDiagnostician(
            llm_call=llm_call,
            base_diagnostician=Diagnostician(),
        )

        diagnosis = await diagnostician.diagnose(failure, extra_telemetry={...})

    The llm_call is awaitable — any provider or local model works.
    In tests, inject a mock that returns a valid JSON string.
    """

    def __init__(
        self,
        llm_call: LLMCall,
        base_diagnostician: Diagnostician | None = None,
        *,
        timeout_seconds: float = LLM_CALL_TIMEOUT_SECONDS,
    ) -> None:
        self._llm_call = llm_call
        self._base = base_diagnostician or Diagnostician()
        self._timeout = timeout_seconds

        # Telemetry
        self._total_calls: int = 0
        self._llm_invocations: int = 0
        self._llm_errors: int = 0
        self._fallback_count: int = 0

    async def diagnose(
        self,
        failure: DetectedFailure,
        extra_telemetry: dict[str, Any] | None = None,
    ) -> Diagnosis:
        """
        Diagnose a failure — rule-based first, LLM only if confidence is low.

        Always returns a Diagnosis, never raises.
        """
        self._total_calls += 1

        # Step 1: Rule-based diagnosis
        base_diag = self._base.diagnose(failure, extra_telemetry)

        # Step 2: If high confidence, return immediately — no LLM needed
        if not base_diag.needs_llm_review:
            log.debug(
                "LLMDiagnostician: rule table sufficient "
                "(confidence=%.2f, rule=%r) — skipping LLM",
                base_diag.confidence,
                base_diag.rule_matched,
            )
            return base_diag

        # Step 3: Low confidence — invoke LLM
        self._llm_invocations += 1
        log.info(
            "LLMDiagnostician: invoking LLM for failure=%s agent=%s (base_confidence=%.2f)",
            failure.failure_type.value,
            failure.agent_id,
            base_diag.confidence,
        )

        try:
            prompt = build_prompt(failure, base_diag, extra_telemetry)
            import asyncio
            response = await asyncio.wait_for(
                self._llm_call(prompt), timeout=self._timeout
            )
            enhanced = _parse_llm_response(response, base_diag, failure)
            log.info(
                "LLMDiagnostician: LLM response — type=%s strategy=%s confidence=%.2f",
                enhanced.failure_type.value,
                enhanced.recommended_strategy.value,
                enhanced.confidence,
            )
            return enhanced

        except Exception as exc:
            self._llm_errors += 1
            self._fallback_count += 1
            log.warning(
                "LLMDiagnostician: LLM call failed (%s) — falling back to rule-based diagnosis",
                exc,
            )
            # Augment base diagnosis with error information for audit trail
            base_diag.evidence["llm_used"] = False
            base_diag.evidence["llm_error"] = str(exc)[:100]
            return base_diag

    def batch_diagnose(
        self,
        failures: list[DetectedFailure],
        extra_telemetry: dict[str, Any] | None = None,
    ) -> list[Any]:
        """Sync wrapper for batch — returns coroutines for awaiting."""
        return [self.diagnose(f, extra_telemetry) for f in failures]

    @property
    def stats(self) -> dict[str, Any]:
        return {
            "total_calls": self._total_calls,
            "llm_invocations": self._llm_invocations,
            "llm_errors": self._llm_errors,
            "fallback_count": self._fallback_count,
            "llm_hit_rate": (
                self._llm_invocations / self._total_calls
                if self._total_calls else 0.0
            ),
        }
