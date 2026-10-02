"""
tests.test_phase7_llm_diagnostician
─────────────────────────────────────
Phase 7: LLM Diagnostician tests.

Tests:
  Prompt building
    1.  build_prompt contains failure_type, agent_id, incident_id
    2.  build_prompt includes base diagnosis values
    3.  build_prompt includes telemetry snapshot
    4.  build_prompt includes system prompt
    5.  Extra telemetry is merged into snapshot

  Response parsing
    6.  Valid JSON response → enhanced Diagnosis with all fields
    7.  Markdown code fence stripped before parsing
    8.  Unknown FailureType in response falls back to base type
    9.  Unknown RecoveryStrategy in response falls back to base strategy
    10. Confidence clamped to [0.0, 1.0]
    11. Missing fields fall back to base diagnosis values
    12. Malformed JSON raises ValueError

  LLMDiagnostician — high-confidence path
    13. High-confidence failure (>= threshold) skips LLM entirely
    14. stats llm_invocations stays 0 when LLM not invoked

  LLMDiagnostician — low-confidence path
    15. UNKNOWN failure → LLM invoked
    16. LLM response refines FailureType correctly
    17. LLM response refines RecoveryStrategy correctly
    18. LLM response sets llm_used=True in evidence
    19. rule_matched prefixed with "llm:" for audit trail
    20. needs_llm_review recalculated on enhanced Diagnosis

  LLMDiagnostician — error handling
    21. LLM timeout → fallback to base diagnosis, no raise
    22. LLM returns malformed JSON → fallback, no raise
    23. LLM raises arbitrary exception → fallback, no raise
    24. llm_error recorded in evidence after fallback
    25. fallback_count incremented on error

  LLMDiagnostician — stats
    26. stats() tracks total_calls correctly
    27. stats() tracks llm_invocations only when LLM used
    28. stats() tracks llm_errors correctly
    29. llm_hit_rate calculated correctly
    30. Multiple calls accumulate stats correctly
"""

from __future__ import annotations

import asyncio
import json
from typing import Any

import pytest

from contracts.agent_state import AgentId
from contracts.recovery_ledger import FailureType, RecoveryStrategy
from detection.diagnostician import Diagnosis, Diagnostician
from detection.failure_detector import DetectedFailure
from detection.llm_diagnostician import (
    LLMDiagnostician,
    _parse_llm_response,
    build_prompt,
)


# ── Helpers ───────────────────────────────────────────────────────────────────

def make_failure(failure_type: FailureType, **tel: Any) -> DetectedFailure:
    return DetectedFailure(
        failure_type=failure_type,
        agent_id=AgentId("agent-test"),
        detail=f"Test: {failure_type.value}",
        telemetry=tel,
    )


def make_base_diagnosis(
    failure_type: FailureType = FailureType.UNKNOWN,
    strategy: RecoveryStrategy = RecoveryStrategy.ESCALATE_HUMAN,
    confidence: float = 0.30,
    rule: str = "none",
) -> Diagnosis:
    return Diagnosis(
        failure_type=failure_type,
        root_cause="Fallback root cause",
        recommended_strategy=strategy,
        confidence=confidence,
        agent_id=AgentId("agent-test"),
        incident_id="inc-test",
        rule_matched=rule,
    )


def make_llm_response(
    failure_type: str = "RETRY_STORM",
    strategy: str = "CIRCUIT_BREAKER",
    confidence: float = 0.85,
    root_cause: str = "LLM identified retry storm on external API",
    reasoning: str = "High call frequency with consistent failures",
) -> str:
    return json.dumps({
        "failure_type": failure_type,
        "recovery_strategy": strategy,
        "confidence": confidence,
        "root_cause": root_cause,
        "reasoning": reasoning,
    })


async def mock_llm_call(response: str):
    async def call(prompt: str) -> str:
        return response
    return call


# ── Prompt building ───────────────────────────────────────────────────────────

class TestBuildPrompt:

    def test_contains_failure_type(self):
        failure = make_failure(FailureType.UNKNOWN)
        base = make_base_diagnosis()
        prompt = build_prompt(failure, base)
        assert "UNKNOWN" in prompt

    def test_contains_agent_id(self):
        failure = make_failure(FailureType.UNKNOWN)
        base = make_base_diagnosis()
        prompt = build_prompt(failure, base)
        assert "agent-test" in prompt

    def test_contains_incident_id(self):
        failure = make_failure(FailureType.UNKNOWN)
        failure.incident_id = "inc-abc123"
        base = make_base_diagnosis()
        prompt = build_prompt(failure, base)
        assert "inc-abc123" in prompt

    def test_contains_base_diagnosis(self):
        failure = make_failure(FailureType.UNKNOWN)
        base = make_base_diagnosis(confidence=0.30, rule="none")
        prompt = build_prompt(failure, base)
        assert "0.3" in prompt or "0.30" in prompt
        assert "none" in prompt

    def test_contains_telemetry(self):
        failure = make_failure(FailureType.UNKNOWN, api_calls=99, agent_role="researcher")
        base = make_base_diagnosis()
        prompt = build_prompt(failure, base)
        assert "api_calls" in prompt
        assert "99" in prompt

    def test_contains_system_prompt(self):
        failure = make_failure(FailureType.UNKNOWN)
        base = make_base_diagnosis()
        prompt = build_prompt(failure, base)
        assert "BOSSman's LLM Diagnostician" in prompt

    def test_extra_telemetry_merged(self):
        failure = make_failure(FailureType.UNKNOWN)
        base = make_base_diagnosis()
        prompt = build_prompt(failure, base, extra_telemetry={"extra_key": "extra_val"})
        assert "extra_key" in prompt
        assert "extra_val" in prompt


# ── Response parsing ──────────────────────────────────────────────────────────

class TestParseResponse:

    def test_valid_json_produces_enhanced_diagnosis(self):
        failure = make_failure(FailureType.UNKNOWN)
        base = make_base_diagnosis()
        response = make_llm_response()
        result = _parse_llm_response(response, base, failure)
        assert result.failure_type == FailureType.RETRY_STORM
        assert result.recommended_strategy == RecoveryStrategy.CIRCUIT_BREAKER
        assert abs(result.confidence - 0.85) < 0.01
        assert "LLM identified" in result.root_cause

    def test_markdown_fences_stripped(self):
        failure = make_failure(FailureType.UNKNOWN)
        base = make_base_diagnosis()
        response = "```json\n" + make_llm_response() + "\n```"
        result = _parse_llm_response(response, base, failure)
        assert result.failure_type == FailureType.RETRY_STORM

    def test_unknown_failure_type_falls_back(self):
        failure = make_failure(FailureType.UNKNOWN)
        base = make_base_diagnosis(failure_type=FailureType.UNKNOWN)
        response = make_llm_response(failure_type="NOT_A_REAL_TYPE")
        result = _parse_llm_response(response, base, failure)
        assert result.failure_type == FailureType.UNKNOWN  # fell back to base

    def test_unknown_strategy_falls_back(self):
        failure = make_failure(FailureType.UNKNOWN)
        base = make_base_diagnosis(strategy=RecoveryStrategy.ESCALATE_HUMAN)
        response = make_llm_response(strategy="NOT_A_REAL_STRATEGY")
        result = _parse_llm_response(response, base, failure)
        assert result.recommended_strategy == RecoveryStrategy.ESCALATE_HUMAN

    def test_confidence_clamped_high(self):
        failure = make_failure(FailureType.UNKNOWN)
        base = make_base_diagnosis()
        response = make_llm_response(confidence=2.5)
        result = _parse_llm_response(response, base, failure)
        assert result.confidence <= 1.0

    def test_confidence_clamped_low(self):
        failure = make_failure(FailureType.UNKNOWN)
        base = make_base_diagnosis()
        response = make_llm_response(confidence=-0.5)
        result = _parse_llm_response(response, base, failure)
        assert result.confidence >= 0.0

    def test_missing_fields_use_base(self):
        failure = make_failure(FailureType.UNKNOWN)
        base = make_base_diagnosis(confidence=0.30)
        response = json.dumps({"failure_type": "DEADLOCK"})  # missing many fields
        result = _parse_llm_response(response, base, failure)
        assert result.failure_type == FailureType.DEADLOCK
        # Confidence from base since missing in response
        assert result.confidence == base.confidence

    def test_malformed_json_raises(self):
        failure = make_failure(FailureType.UNKNOWN)
        base = make_base_diagnosis()
        with pytest.raises(ValueError):
            _parse_llm_response("this is not json at all", base, failure)


# ── LLMDiagnostician — high-confidence path ───────────────────────────────────

class TestHighConfidencePath:

    @pytest.mark.asyncio
    async def test_high_confidence_skips_llm(self):
        """RETRY_STORM has 0.90 confidence — LLM should NOT be called."""
        called = [False]

        async def llm(prompt: str) -> str:
            called[0] = True
            return make_llm_response()

        diag = LLMDiagnostician(llm_call=llm, base_diagnostician=Diagnostician())
        failure = make_failure(FailureType.RETRY_STORM,
                               failure_count_in_window=5, window_seconds=60.0, threshold=5)
        result = await diag.diagnose(failure)

        assert called[0] is False
        assert result.failure_type == FailureType.RETRY_STORM

    @pytest.mark.asyncio
    async def test_stats_no_llm_invocations_when_skipped(self):
        async def llm(prompt: str) -> str:
            return make_llm_response()

        diag = LLMDiagnostician(llm_call=llm)
        failure = make_failure(FailureType.RETRY_STORM,
                               failure_count_in_window=5, window_seconds=60.0, threshold=5)
        await diag.diagnose(failure)

        assert diag.stats["llm_invocations"] == 0


# ── LLMDiagnostician — low-confidence path ────────────────────────────────────

class TestLowConfidencePath:

    @pytest.mark.asyncio
    async def test_unknown_invokes_llm(self):
        """UNKNOWN failure → confidence 0.30 → LLM invoked."""
        async def llm(prompt: str) -> str:
            return make_llm_response(failure_type="DEADLOCK", strategy="RELEASE_RESOURCES")

        diag = LLMDiagnostician(llm_call=llm)
        failure = make_failure(FailureType.UNKNOWN)
        result = await diag.diagnose(failure)

        assert result.failure_type == FailureType.DEADLOCK

    @pytest.mark.asyncio
    async def test_llm_refines_failure_type(self):
        async def llm(prompt: str) -> str:
            return make_llm_response(failure_type="CASCADING_FAILURE", strategy="CIRCUIT_BREAKER")

        diag = LLMDiagnostician(llm_call=llm)
        failure = make_failure(FailureType.UNKNOWN)
        result = await diag.diagnose(failure)

        assert result.failure_type == FailureType.CASCADING_FAILURE

    @pytest.mark.asyncio
    async def test_llm_refines_strategy(self):
        async def llm(prompt: str) -> str:
            return make_llm_response(strategy="ROLLBACK_CHECKPOINT")

        diag = LLMDiagnostician(llm_call=llm)
        failure = make_failure(FailureType.UNKNOWN)
        result = await diag.diagnose(failure)

        assert result.recommended_strategy == RecoveryStrategy.ROLLBACK_CHECKPOINT

    @pytest.mark.asyncio
    async def test_llm_sets_used_flag(self):
        async def llm(prompt: str) -> str:
            return make_llm_response()

        diag = LLMDiagnostician(llm_call=llm)
        failure = make_failure(FailureType.UNKNOWN)
        result = await diag.diagnose(failure)

        assert result.evidence.get("llm_used") is True

    @pytest.mark.asyncio
    async def test_rule_matched_prefixed_llm(self):
        async def llm(prompt: str) -> str:
            return make_llm_response()

        diag = LLMDiagnostician(llm_call=llm)
        failure = make_failure(FailureType.UNKNOWN)
        result = await diag.diagnose(failure)

        assert result.rule_matched.startswith("llm:")

    @pytest.mark.asyncio
    async def test_needs_llm_review_recalculated(self):
        """LLM returns 0.85 confidence → needs_llm_review should be False."""
        async def llm(prompt: str) -> str:
            return make_llm_response(confidence=0.85)

        diag = LLMDiagnostician(llm_call=llm)
        failure = make_failure(FailureType.UNKNOWN)
        result = await diag.diagnose(failure)

        assert result.confidence >= 0.5
        assert result.needs_llm_review is False


# ── LLMDiagnostician — error handling ────────────────────────────────────────

class TestErrorHandling:

    @pytest.mark.asyncio
    async def test_timeout_falls_back(self):
        """LLM call timeout → fallback to base diagnosis, no exception."""
        async def llm(prompt: str) -> str:
            await asyncio.sleep(100)
            return ""

        diag = LLMDiagnostician(llm_call=llm, timeout_seconds=0.05)
        failure = make_failure(FailureType.UNKNOWN)
        result = await diag.diagnose(failure)  # must not raise

        assert result.failure_type == FailureType.UNKNOWN  # base fallback
        assert result.evidence.get("llm_used") is False

    @pytest.mark.asyncio
    async def test_malformed_json_falls_back(self):
        """LLM returns garbage → fallback, no exception."""
        async def llm(prompt: str) -> str:
            return "this is not json"

        diag = LLMDiagnostician(llm_call=llm)
        failure = make_failure(FailureType.UNKNOWN)
        result = await diag.diagnose(failure)

        assert result.failure_type == FailureType.UNKNOWN

    @pytest.mark.asyncio
    async def test_arbitrary_exception_falls_back(self):
        """LLM raises RuntimeError → fallback, no exception."""
        async def llm(prompt: str) -> str:
            raise RuntimeError("API unavailable")

        diag = LLMDiagnostician(llm_call=llm)
        failure = make_failure(FailureType.UNKNOWN)
        result = await diag.diagnose(failure)

        assert result.failure_type == FailureType.UNKNOWN

    @pytest.mark.asyncio
    async def test_llm_error_recorded_in_evidence(self):
        async def llm(prompt: str) -> str:
            raise RuntimeError("Connection refused")

        diag = LLMDiagnostician(llm_call=llm)
        failure = make_failure(FailureType.UNKNOWN)
        result = await diag.diagnose(failure)

        assert "llm_error" in result.evidence
        assert "Connection refused" in result.evidence["llm_error"]

    @pytest.mark.asyncio
    async def test_fallback_count_incremented(self):
        async def llm(prompt: str) -> str:
            raise RuntimeError("rate limited")

        diag = LLMDiagnostician(llm_call=llm)
        failure = make_failure(FailureType.UNKNOWN)
        await diag.diagnose(failure)

        assert diag.stats["fallback_count"] == 1


# ── LLMDiagnostician — stats ──────────────────────────────────────────────────

class TestStats:

    @pytest.mark.asyncio
    async def test_total_calls_tracked(self):
        async def llm(prompt: str) -> str:
            return make_llm_response()

        diag = LLMDiagnostician(llm_call=llm)
        for _ in range(3):
            await diag.diagnose(make_failure(FailureType.RETRY_STORM,
                                             failure_count_in_window=5, window_seconds=60.0, threshold=5))
        assert diag.stats["total_calls"] == 3

    @pytest.mark.asyncio
    async def test_llm_invocations_only_when_low_confidence(self):
        call_count = [0]

        async def llm(prompt: str) -> str:
            call_count[0] += 1
            return make_llm_response()

        diag = LLMDiagnostician(llm_call=llm)
        # High confidence — skip LLM
        await diag.diagnose(make_failure(FailureType.RETRY_STORM,
                                         failure_count_in_window=5, window_seconds=60.0, threshold=5))
        # Low confidence — invoke LLM
        await diag.diagnose(make_failure(FailureType.UNKNOWN))

        assert call_count[0] == 1
        assert diag.stats["llm_invocations"] == 1

    @pytest.mark.asyncio
    async def test_llm_errors_tracked(self):
        async def llm(prompt: str) -> str:
            raise RuntimeError("boom")

        diag = LLMDiagnostician(llm_call=llm)
        await diag.diagnose(make_failure(FailureType.UNKNOWN))
        await diag.diagnose(make_failure(FailureType.UNKNOWN))

        assert diag.stats["llm_errors"] == 2

    @pytest.mark.asyncio
    async def test_hit_rate_correct(self):
        call_count = [0]

        async def llm(prompt: str) -> str:
            call_count[0] += 1
            return make_llm_response()

        diag = LLMDiagnostician(llm_call=llm)
        # 2 high-confidence (no LLM)
        for _ in range(2):
            await diag.diagnose(make_failure(FailureType.RETRY_STORM,
                                             failure_count_in_window=5, window_seconds=60.0, threshold=5))
        # 1 low-confidence (LLM)
        await diag.diagnose(make_failure(FailureType.UNKNOWN))

        stats = diag.stats
        assert stats["total_calls"] == 3
        assert stats["llm_invocations"] == 1
        assert abs(stats["llm_hit_rate"] - (1 / 3)) < 0.01

    @pytest.mark.asyncio
    async def test_accumulation_across_calls(self):
        results = []

        async def llm(prompt: str) -> str:
            return make_llm_response(failure_type="DEADLOCK", strategy="RELEASE_RESOURCES", confidence=0.88)

        diag = LLMDiagnostician(llm_call=llm)
        for _ in range(3):
            r = await diag.diagnose(make_failure(FailureType.UNKNOWN))
            results.append(r)

        assert len(results) == 3
        assert all(r.failure_type == FailureType.DEADLOCK for r in results)
        assert diag.stats["llm_invocations"] == 3
