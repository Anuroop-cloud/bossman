"""
tests.test_phase4_detection
────────────────────────────
Phase 4 test suite: Failure Detection + Rule-Based Diagnostician.

Tests cover:

  FailureDetector
    1.  Retry storm fires FAILURE_DETECTED after N failures in window
    2.  Retry storm does NOT fire below threshold
    3.  Retry storm resets window after firing (no repeated events)
    4.  Heartbeat timeout wraps AGENT_TIMEOUT as FAILURE_DETECTED
    5.  Resource starvation fires after N RESOURCE_STARVATION events
    6.  Resource starvation does NOT fire below threshold
    7.  Deadlock fires on DEADLOCK_SUSPECTED events
    8.  Cascading failure fires when ≥N agents fail in window
    9.  on_failure callback invoked when failure detected
   10.  detect_now() finds context overflow from registry snapshot
   11.  detect_now() finds deadlock from registry snapshot
   12.  stats() tracks per-type counts correctly

  Diagnostician
   13.  RETRY_STORM → CIRCUIT_BREAKER, confidence 0.90
   14.  HEARTBEAT_TIMEOUT → RESTART_AGENT, confidence 0.85
   15.  RESOURCE_STARVATION → WAIT_AND_RETRY, confidence 0.80
   16.  DEADLOCK → RELEASE_RESOURCES, confidence 0.88
   17.  CASCADING_FAILURE → CIRCUIT_BREAKER, confidence 0.75
   18.  CONTEXT_OVERFLOW → CONTEXT_COMPACTION, confidence 0.95
   19.  PERMISSION_VIOLATION → ESCALATE_HUMAN, confidence 0.99
   20.  UNKNOWN → ESCALATE_HUMAN, confidence 0.30, needs_llm_review=True
   21.  Unmatched failure type → fallback with confidence 0.0
   22.  batch_diagnose processes multiple failures in order
   23.  Diagnosis.needs_llm_review flag correct at threshold boundary
   24.  rule_matched field set correctly per rule
   25.  stats() tracks rule hits and low-confidence count
"""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock

import pytest

from contracts.agent_state import AgentId
from contracts.events import BossmanEvent, EventType
from contracts.recovery_ledger import FailureType, RecoveryStrategy
from core.event_bus import EventBus
from detection.failure_detector import (
    DetectedFailure,
    DetectorConfig,
    FailureDetector,
)
from detection.diagnostician import (
    Diagnosis,
    Diagnostician,
    LLM_ESCALATION_THRESHOLD,
)


# ── Helpers ───────────────────────────────────────────────────────────────────

def make_bus_and_detector(config: DetectorConfig | None = None) -> tuple[EventBus, FailureDetector, list[BossmanEvent]]:
    bus = EventBus()
    emitted: list[BossmanEvent] = []

    async def capture(event: BossmanEvent) -> None:
        emitted.append(event)

    bus.subscribe(capture)
    detector = FailureDetector(bus, config=config)
    return bus, detector, emitted


def failure_events(emitted: list[BossmanEvent]) -> list[BossmanEvent]:
    return [e for e in emitted if e.event_type == EventType.FAILURE_DETECTED]


async def fire_event(bus: EventBus, event_type: EventType, **kwargs: Any) -> None:
    event = BossmanEvent.create(event_type, **kwargs)
    await bus.publish(event)
    await asyncio.sleep(0)  # yield to let subscriber run


def make_detected(failure_type: FailureType, agent_id: str = "agent-x", **telemetry: Any) -> DetectedFailure:
    return DetectedFailure(
        failure_type=failure_type,
        agent_id=AgentId(agent_id),
        detail=f"Test failure: {failure_type.value}",
        telemetry=telemetry,
    )


# ── FailureDetector tests ─────────────────────────────────────────────────────

class TestFailureDetector:

    @pytest.mark.asyncio
    async def test_retry_storm_fires_after_threshold(self):
        """FAILURE_DETECTED emitted after N task failures in window."""
        cfg = DetectorConfig(retry_storm_threshold=3, retry_storm_window_seconds=10.0)
        bus, detector, emitted = make_bus_and_detector(cfg)
        await detector.start()

        for _ in range(3):
            await fire_event(bus, EventType.TASK_FAILED,
                             agent_id=AgentId("agent-1"),
                             payload={"error": "timeout"})

        await asyncio.sleep(0.01)
        detected = failure_events(emitted)
        assert len(detected) >= 1
        assert any(
            e.payload.get("failure_type") == FailureType.RETRY_STORM.value
            for e in detected
        )
        await detector.stop()

    @pytest.mark.asyncio
    async def test_retry_storm_does_not_fire_below_threshold(self):
        """No FAILURE_DETECTED when failures < threshold."""
        cfg = DetectorConfig(retry_storm_threshold=5, retry_storm_window_seconds=10.0)
        bus, detector, emitted = make_bus_and_detector(cfg)
        await detector.start()

        for _ in range(4):
            await fire_event(bus, EventType.TASK_FAILED,
                             agent_id=AgentId("agent-2"),
                             payload={"error": "err"})

        await asyncio.sleep(0.01)
        detected = [e for e in failure_events(emitted)
                    if e.payload.get("failure_type") == FailureType.RETRY_STORM.value]
        assert len(detected) == 0
        await detector.stop()

    @pytest.mark.asyncio
    async def test_retry_storm_resets_after_firing(self):
        """After storm fires, window resets — need N more failures to fire again."""
        cfg = DetectorConfig(retry_storm_threshold=3, retry_storm_window_seconds=10.0)
        bus, detector, emitted = make_bus_and_detector(cfg)
        await detector.start()

        # First storm
        for _ in range(3):
            await fire_event(bus, EventType.TASK_FAILED,
                             agent_id=AgentId("agent-3"), payload={})

        await asyncio.sleep(0.01)
        first_count = len(failure_events(emitted))
        assert first_count >= 1

        # One more failure — should NOT fire again (window reset)
        await fire_event(bus, EventType.TASK_FAILED, agent_id=AgentId("agent-3"), payload={})
        await asyncio.sleep(0.01)
        assert len(failure_events(emitted)) == first_count  # no new events

        await detector.stop()

    @pytest.mark.asyncio
    async def test_heartbeat_timeout_wraps_as_failure(self):
        """AGENT_TIMEOUT → FAILURE_DETECTED with HEARTBEAT_TIMEOUT type."""
        bus, detector, emitted = make_bus_and_detector()
        await detector.start()

        await fire_event(
            bus, EventType.AGENT_TIMEOUT,
            agent_id=AgentId("agent-t"),
            payload={"heartbeat_age_seconds": 45.0, "threshold_seconds": 30.0},
        )

        await asyncio.sleep(0.01)
        detected = [e for e in failure_events(emitted)
                    if e.payload.get("failure_type") == FailureType.HEARTBEAT_TIMEOUT.value]
        assert len(detected) >= 1
        await detector.stop()

    @pytest.mark.asyncio
    async def test_resource_starvation_fires(self):
        """FAILURE_DETECTED(RESOURCE_STARVATION) after N starvation events."""
        cfg = DetectorConfig(starvation_threshold=3, starvation_window_seconds=10.0)
        bus, detector, emitted = make_bus_and_detector(cfg)
        await detector.start()

        for _ in range(3):
            await fire_event(
                bus, EventType.RESOURCE_STARVATION,
                agent_id=AgentId("agent-s"),
                payload={"bucket": "llm-rpm", "tokens_needed": 1.0, "tokens_available": 0.0},
            )

        await asyncio.sleep(0.01)
        detected = [e for e in failure_events(emitted)
                    if e.payload.get("failure_type") == FailureType.RESOURCE_STARVATION.value]
        assert len(detected) >= 1
        await detector.stop()

    @pytest.mark.asyncio
    async def test_resource_starvation_below_threshold(self):
        """No starvation failure below threshold."""
        cfg = DetectorConfig(starvation_threshold=5, starvation_window_seconds=10.0)
        bus, detector, emitted = make_bus_and_detector(cfg)
        await detector.start()

        for _ in range(4):
            await fire_event(bus, EventType.RESOURCE_STARVATION, payload={})

        await asyncio.sleep(0.01)
        detected = [e for e in failure_events(emitted)
                    if e.payload.get("failure_type") == FailureType.RESOURCE_STARVATION.value]
        assert len(detected) == 0
        await detector.stop()

    @pytest.mark.asyncio
    async def test_deadlock_suspected_fires(self):
        """DEADLOCK_SUSPECTED → FAILURE_DETECTED(DEADLOCK)."""
        bus, detector, emitted = make_bus_and_detector()
        await detector.start()

        await fire_event(
            bus, EventType.DEADLOCK_SUSPECTED,
            agent_id=AgentId("agent-d"),
            payload={
                "requester_holds": ["lock-x"],
                "wanted_resource": "lock-y",
                "current_holder": "agent-e",
                "cycle_resources": ["lock-x"],
            },
        )

        await asyncio.sleep(0.01)
        detected = [e for e in failure_events(emitted)
                    if e.payload.get("failure_type") == FailureType.DEADLOCK.value]
        assert len(detected) >= 1
        await detector.stop()

    @pytest.mark.asyncio
    async def test_cascading_failure_fires(self):
        """≥N agent timeouts in window → CASCADING_FAILURE."""
        cfg = DetectorConfig(cascade_agent_threshold=3, cascade_window_seconds=10.0)
        bus, detector, emitted = make_bus_and_detector(cfg)
        await detector.start()

        for i in range(3):
            await fire_event(
                bus, EventType.AGENT_TIMEOUT,
                agent_id=AgentId(f"agent-c{i}"),
                payload={"heartbeat_age_seconds": 40.0, "threshold_seconds": 30.0},
            )

        await asyncio.sleep(0.01)
        detected = [e for e in failure_events(emitted)
                    if e.payload.get("failure_type") == FailureType.CASCADING_FAILURE.value]
        assert len(detected) >= 1
        await detector.stop()

    @pytest.mark.asyncio
    async def test_on_failure_callback_invoked(self):
        """on_failure async callback called when failure detected."""
        callback_args: list[DetectedFailure] = []

        async def callback(failure: DetectedFailure) -> None:
            callback_args.append(failure)

        cfg = DetectorConfig(retry_storm_threshold=2, retry_storm_window_seconds=10.0)
        bus = EventBus()
        detector = FailureDetector(bus, config=cfg, on_failure=callback)
        await detector.start()

        for _ in range(2):
            await fire_event(bus, EventType.TASK_FAILED, agent_id=AgentId("agent-cb"), payload={})

        await asyncio.sleep(0.01)
        assert len(callback_args) >= 1
        assert callback_args[0].failure_type == FailureType.RETRY_STORM
        await detector.stop()

    @pytest.mark.asyncio
    async def test_detect_now_finds_context_overflow(self):
        """detect_now() detects context overflow from registry snapshot."""
        bus, detector, emitted = make_bus_and_detector()
        await detector.start()

        registry_snap = {
            "agent-overflow": {
                "context_utilisation": 0.97,
                "resources_held": [],
                "resources_waiting": [],
            }
        }
        results = await detector.detect_now(registry_snapshot=registry_snap)
        overflow = [f for f in results if f.failure_type == FailureType.CONTEXT_OVERFLOW]
        assert len(overflow) >= 1
        assert overflow[0].agent_id == "agent-overflow"
        await detector.stop()

    @pytest.mark.asyncio
    async def test_detect_now_finds_deadlock_from_snapshot(self):
        """detect_now() catches hold+wait deadlock candidates from snapshots."""
        bus, detector, emitted = make_bus_and_detector()
        await detector.start()

        registry_snap = {
            "agent-dl": {
                "context_utilisation": 0.3,
                "resources_held": ["lock-x"],
                "resources_waiting": ["lock-y"],
            }
        }
        resource_snap = {"named_resources": {"lock-x": {}, "lock-y": {}}}
        results = await detector.detect_now(
            resource_snapshot=resource_snap,
            registry_snapshot=registry_snap,
        )
        dl = [f for f in results if f.failure_type == FailureType.DEADLOCK]
        assert len(dl) >= 1
        await detector.stop()

    @pytest.mark.asyncio
    async def test_stats_track_per_type_counts(self):
        """stats() returns correct per-type detection counts."""
        cfg = DetectorConfig(retry_storm_threshold=2, retry_storm_window_seconds=10.0)
        bus, detector, emitted = make_bus_and_detector(cfg)
        await detector.start()

        for _ in range(2):
            await fire_event(bus, EventType.TASK_FAILED, agent_id=AgentId("agent-st"), payload={})
        await asyncio.sleep(0.01)

        stats = detector.stats
        assert stats["total_detected"] >= 1
        assert FailureType.RETRY_STORM.value in stats["by_type"]
        await detector.stop()


# ── Diagnostician tests ───────────────────────────────────────────────────────

class TestDiagnostician:

    def test_retry_storm_circuit_breaker(self):
        """RETRY_STORM → CIRCUIT_BREAKER at 0.90 confidence."""
        diag = Diagnostician()
        failure = make_detected(
            FailureType.RETRY_STORM,
            failure_count_in_window=5, window_seconds=60.0, threshold=5,
        )
        result = diag.diagnose(failure)
        assert result.failure_type == FailureType.RETRY_STORM
        assert result.recommended_strategy == RecoveryStrategy.CIRCUIT_BREAKER
        assert abs(result.confidence - 0.90) < 0.01
        assert result.rule_matched == "retry_storm_circuit_break"
        assert not result.needs_llm_review

    def test_heartbeat_timeout_restart_agent(self):
        """HEARTBEAT_TIMEOUT → RESTART_AGENT at 0.85 confidence."""
        diag = Diagnostician()
        failure = make_detected(
            FailureType.HEARTBEAT_TIMEOUT,
            heartbeat_age_seconds=45.0, threshold_seconds=30.0,
        )
        result = diag.diagnose(failure)
        assert result.recommended_strategy == RecoveryStrategy.RESTART_AGENT
        assert abs(result.confidence - 0.85) < 0.01
        assert result.rule_matched == "heartbeat_timeout_restart"

    def test_resource_starvation_wait_and_retry(self):
        """RESOURCE_STARVATION → WAIT_AND_RETRY at 0.80 confidence."""
        diag = Diagnostician()
        failure = make_detected(
            FailureType.RESOURCE_STARVATION,
            bucket="llm-rpm", starvation_count_in_window=3, window_seconds=30.0,
        )
        result = diag.diagnose(failure)
        assert result.recommended_strategy == RecoveryStrategy.WAIT_AND_RETRY
        assert abs(result.confidence - 0.80) < 0.01

    def test_deadlock_release_resources(self):
        """DEADLOCK → RELEASE_RESOURCES at 0.88 confidence."""
        diag = Diagnostician()
        failure = make_detected(
            FailureType.DEADLOCK,
            resources_held=["lock-x"], resources_waiting=["lock-y"],
        )
        result = diag.diagnose(failure)
        assert result.recommended_strategy == RecoveryStrategy.RELEASE_RESOURCES
        assert abs(result.confidence - 0.88) < 0.01

    def test_cascading_failure_circuit_breaker(self):
        """CASCADING_FAILURE → CIRCUIT_BREAKER at 0.75 confidence."""
        diag = Diagnostician()
        failure = make_detected(
            FailureType.CASCADING_FAILURE,
            failed_agent_count=3, window_seconds=60.0,
        )
        result = diag.diagnose(failure)
        assert result.recommended_strategy == RecoveryStrategy.CIRCUIT_BREAKER
        assert abs(result.confidence - 0.75) < 0.01

    def test_context_overflow_compaction(self):
        """CONTEXT_OVERFLOW → CONTEXT_COMPACTION at 0.95 confidence."""
        diag = Diagnostician()
        failure = make_detected(
            FailureType.CONTEXT_OVERFLOW,
            context_utilisation=0.97,
        )
        result = diag.diagnose(failure)
        assert result.recommended_strategy == RecoveryStrategy.CONTEXT_COMPACTION
        assert abs(result.confidence - 0.95) < 0.01
        assert not result.needs_llm_review

    def test_permission_violation_escalate_human(self):
        """PERMISSION_VIOLATION → ESCALATE_HUMAN at 0.99 confidence."""
        diag = Diagnostician()
        failure = make_detected(FailureType.PERMISSION_VIOLATION)
        result = diag.diagnose(failure)
        assert result.recommended_strategy == RecoveryStrategy.ESCALATE_HUMAN
        assert result.confidence >= 0.99
        assert result.rule_matched == "permission_escalate"

    def test_unknown_escalates_with_low_confidence(self):
        """UNKNOWN → ESCALATE_HUMAN, confidence 0.30, needs_llm_review=True."""
        diag = Diagnostician()
        failure = make_detected(FailureType.UNKNOWN)
        result = diag.diagnose(failure)
        assert result.recommended_strategy == RecoveryStrategy.ESCALATE_HUMAN
        assert result.confidence < LLM_ESCALATION_THRESHOLD
        assert result.needs_llm_review is True

    def test_unmatched_failure_type_fallback(self):
        """Failure type not in any rule → fallback confidence=0.0."""
        diag = Diagnostician()
        # Use EXTERNAL_SERVICE_FAILURE which has no specific rule
        failure = make_detected(FailureType.EXTERNAL_SERVICE_FAILURE)
        result = diag.diagnose(failure)
        assert result.confidence == 0.0
        assert result.needs_llm_review is True
        assert result.rule_matched == "none"
        assert result.recommended_strategy == RecoveryStrategy.ESCALATE_HUMAN

    def test_batch_diagnose_order_preserved(self):
        """batch_diagnose returns diagnoses in the same order as inputs."""
        diag = Diagnostician()
        failures = [
            make_detected(FailureType.RETRY_STORM),
            make_detected(FailureType.DEADLOCK),
            make_detected(FailureType.CONTEXT_OVERFLOW, context_utilisation=0.96),
        ]
        results = diag.batch_diagnose(failures)
        assert len(results) == 3
        assert results[0].failure_type == FailureType.RETRY_STORM
        assert results[1].failure_type == FailureType.DEADLOCK
        assert results[2].failure_type == FailureType.CONTEXT_OVERFLOW

    def test_needs_llm_review_flag_at_threshold_boundary(self):
        """needs_llm_review=False at exactly threshold, True below it."""
        diag = Diagnostician()
        # CASCADING_FAILURE has confidence 0.75 > 0.5 threshold → no LLM
        failure = make_detected(FailureType.CASCADING_FAILURE,
                                failed_agent_count=3, window_seconds=60.0)
        result = diag.diagnose(failure)
        assert result.confidence > LLM_ESCALATION_THRESHOLD
        assert result.needs_llm_review is False

        # UNKNOWN has confidence 0.30 < 0.5 → needs LLM
        failure2 = make_detected(FailureType.UNKNOWN)
        result2 = diag.diagnose(failure2)
        assert result2.needs_llm_review is True

    def test_rule_matched_set_per_rule(self):
        """rule_matched field identifies the exact rule that fired."""
        diag = Diagnostician()
        cases = [
            (FailureType.HEARTBEAT_TIMEOUT, "heartbeat_timeout_restart"),
            (FailureType.RESOURCE_STARVATION, "resource_starvation_backoff"),
            (FailureType.PERMISSION_VIOLATION, "permission_escalate"),
        ]
        for failure_type, expected_rule in cases:
            failure = make_detected(failure_type)
            result = diag.diagnose(failure)
            assert result.rule_matched == expected_rule, (
                f"Expected rule {expected_rule!r} for {failure_type}, got {result.rule_matched!r}"
            )

    def test_stats_track_rule_hits_and_low_confidence(self):
        """stats() tracks rule_hits per rule name and low_confidence_count."""
        diag = Diagnostician()
        diag.diagnose(make_detected(FailureType.RETRY_STORM))
        diag.diagnose(make_detected(FailureType.RETRY_STORM))
        diag.diagnose(make_detected(FailureType.UNKNOWN))

        stats = diag.stats
        assert stats["total_diagnoses"] == 3
        assert stats["rule_hits"].get("retry_storm_circuit_break", 0) == 2
        assert stats["low_confidence_count"] >= 1  # UNKNOWN is low confidence
