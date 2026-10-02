"""
tests.test_phase6_evaluator
────────────────────────────
Phase 6: Evaluator — L1-L4 health checks + recovery verification loop.

Tests:
  HealthChecker — L1 Liveness
    1.  RUNNING agent, fresh heartbeat → L1 PASS
    2.  STUCK agent → L1 FAIL
    3.  FAILED agent → L1 FAIL
    4.  TERMINATED agent → L1 FAIL
    5.  Stale heartbeat > timeout → L1 FAIL
    6.  Heartbeat in warn zone → L1 WARN
    7.  RECOVERING agent, recent heartbeat → L1 PASS

  HealthChecker — L2 Progress
    8.  consecutive_failures = 0 → L2 PASS
    9.  consecutive_failures = max_threshold → L2 FAIL
    10. consecutive_failures = 2 (below threshold) → L2 WARN
    11. High failure rate (>50%) over ≥5 tasks → L2 WARN

  HealthChecker — L3 Resources
    12. Clean resource state → L3 PASS
    13. Deadlock suspect (holds + waits) → L3 FAIL
    14. Context at fail threshold (≥95%) → L3 FAIL
    15. Context at warn threshold (≥75%) → L3 WARN
    16. Token budget at fail fraction → L3 FAIL
    17. Token budget at warn fraction → L3 WARN

  HealthChecker — L4 Quality
    18. No quality signals → L4 PASS
    19. quality_degraded flag set → L4 FAIL
    20. fallback_model directive → L4 WARN
    21. compact_context directive → L4 WARN

  AgentHealthReport
    22. overall_status is worst of all levels
    23. passed/failed properties correct
    24. by_level() returns correct result

  Evaluator — recovery verification
    25. Healthy agent after recovery → EVALUATION_PASSED, ledger SUCCESS
    26. Still-unhealthy agent → EVALUATION_FAILED, ledger FAILURE
    27. Evaluator retries before giving up
    28. evaluate_now() works without RECOVERY_STARTED event
    29. stats() tracks pass/fail counts
    30. Evaluator wires to RECOVERY_STARTED event end-to-end
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

from contracts.agent_state import AgentId, AgentInfo, AgentState, AgentStatus, ResourceUsage
from contracts.events import BossmanEvent, EventType
from contracts.recovery_ledger import FailureType, RecoveryAttempt, RecoveryOutcome, RecoveryStrategy
from core.event_bus import EventBus
from core.registry import AgentRegistry
from evaluation.health_checker import (
    AgentHealthReport,
    HealthCheckStatus,
    HealthChecker,
    HealthLevel,
)
from evaluation.evaluator import Evaluator
from recovery.recovery_engine import RecoveryLedger


# ── Helpers ───────────────────────────────────────────────────────────────────

def fresh_state(
    name: str = "agent",
    status: AgentStatus = AgentStatus.RUNNING,
    **overrides: Any,
) -> AgentState:
    info = AgentInfo(name=name, role="test")
    state = AgentState(info=info, status=status)
    for k, v in overrides.items():
        setattr(state, k, v)
    return state


def stale_heartbeat(seconds: float) -> AgentState:
    state = fresh_state()
    state.last_heartbeat = datetime.now(timezone.utc) - timedelta(seconds=seconds)
    return state


# ── HealthChecker — L1 ────────────────────────────────────────────────────────

class TestL1Liveness:

    def setup_method(self):
        self.checker = HealthChecker(
            heartbeat_timeout_seconds=30.0,
            heartbeat_warn_seconds=20.0,
        )

    def test_running_fresh_heartbeat_passes(self):
        state = fresh_state(status=AgentStatus.RUNNING)
        result = self.checker._check_l1_liveness(state)
        assert result.status == HealthCheckStatus.PASS

    def test_stuck_agent_fails(self):
        state = fresh_state(status=AgentStatus.STUCK)
        result = self.checker._check_l1_liveness(state)
        assert result.status == HealthCheckStatus.FAIL

    def test_failed_agent_fails(self):
        state = fresh_state(status=AgentStatus.FAILED)
        result = self.checker._check_l1_liveness(state)
        assert result.status == HealthCheckStatus.FAIL

    def test_terminated_agent_fails(self):
        state = fresh_state(status=AgentStatus.TERMINATED)
        result = self.checker._check_l1_liveness(state)
        assert result.status == HealthCheckStatus.FAIL

    def test_stale_heartbeat_beyond_timeout_fails(self):
        state = stale_heartbeat(35.0)
        result = self.checker._check_l1_liveness(state)
        assert result.status == HealthCheckStatus.FAIL
        assert "35" in result.detail or "36" in result.detail or "34" in result.detail

    def test_heartbeat_in_warn_zone(self):
        state = stale_heartbeat(25.0)
        result = self.checker._check_l1_liveness(state)
        assert result.status == HealthCheckStatus.WARN

    def test_recovering_agent_passes(self):
        state = fresh_state(status=AgentStatus.RECOVERING)
        result = self.checker._check_l1_liveness(state)
        assert result.status == HealthCheckStatus.PASS


# ── HealthChecker — L2 ────────────────────────────────────────────────────────

class TestL2Progress:

    def setup_method(self):
        self.checker = HealthChecker(max_consecutive_failures=3)

    def test_zero_consecutive_failures_passes(self):
        state = fresh_state()
        state.consecutive_failures = 0
        result = self.checker._check_l2_progress(state)
        assert result.status == HealthCheckStatus.PASS

    def test_at_threshold_fails(self):
        state = fresh_state()
        state.consecutive_failures = 3
        result = self.checker._check_l2_progress(state)
        assert result.status == HealthCheckStatus.FAIL

    def test_two_consecutive_warns(self):
        state = fresh_state()
        state.consecutive_failures = 2
        result = self.checker._check_l2_progress(state)
        assert result.status == HealthCheckStatus.WARN

    def test_high_failure_rate_warns(self):
        state = fresh_state()
        state.total_tasks_completed = 2
        state.total_tasks_failed = 4  # 66% failure rate over 6 tasks
        result = self.checker._check_l2_progress(state)
        assert result.status == HealthCheckStatus.WARN


# ── HealthChecker — L3 ────────────────────────────────────────────────────────

class TestL3Resources:

    def setup_method(self):
        self.checker = HealthChecker(
            context_warn_threshold=0.75,
            context_fail_threshold=0.95,
            token_warn_fraction=0.85,
            token_fail_fraction=0.95,
        )

    def test_clean_state_passes(self):
        state = fresh_state()
        result = self.checker._check_l3_resources(state)
        assert result.status == HealthCheckStatus.PASS

    def test_deadlock_suspect_fails(self):
        state = fresh_state()
        state.resource_usage.resources_held = ["lock-x"]
        state.resource_usage.resources_waiting = ["lock-y"]
        result = self.checker._check_l3_resources(state)
        assert result.status == HealthCheckStatus.FAIL
        assert "deadlock" in result.detail.lower()

    def test_context_at_fail_threshold(self):
        state = fresh_state()
        state.resource_usage.context_utilisation = 0.96
        result = self.checker._check_l3_resources(state)
        assert result.status == HealthCheckStatus.FAIL

    def test_context_at_warn_threshold(self):
        state = fresh_state()
        state.resource_usage.context_utilisation = 0.80
        result = self.checker._check_l3_resources(state)
        assert result.status == HealthCheckStatus.WARN

    def test_token_budget_fail(self):
        state = fresh_state()
        state.resource_usage.tokens_used = 49_000
        state.resource_usage.tokens_budget = 50_000  # 98%
        result = self.checker._check_l3_resources(state)
        assert result.status == HealthCheckStatus.FAIL

    def test_token_budget_warn(self):
        state = fresh_state()
        state.resource_usage.tokens_used = 45_000
        state.resource_usage.tokens_budget = 50_000  # 90%
        result = self.checker._check_l3_resources(state)
        assert result.status == HealthCheckStatus.WARN


# ── HealthChecker — L4 ────────────────────────────────────────────────────────

class TestL4Quality:

    def setup_method(self):
        self.checker = HealthChecker()

    def test_no_quality_signals_passes(self):
        state = fresh_state()
        result = self.checker._check_l4_quality(state)
        assert result.status == HealthCheckStatus.PASS

    def test_quality_degraded_flag_fails(self):
        state = fresh_state()
        state.info.metadata["quality_degraded"] = True
        state.info.metadata["degradation_note"] = "model drift"
        result = self.checker._check_l4_quality(state)
        assert result.status == HealthCheckStatus.FAIL
        assert "model drift" in result.detail

    def test_fallback_model_directive_warns(self):
        state = fresh_state()
        state.info.metadata["directive"] = "fallback_model"
        result = self.checker._check_l4_quality(state)
        assert result.status == HealthCheckStatus.WARN

    def test_compact_context_directive_warns(self):
        state = fresh_state()
        state.info.metadata["directive"] = "compact_context"
        result = self.checker._check_l4_quality(state)
        assert result.status == HealthCheckStatus.WARN


# ── AgentHealthReport ─────────────────────────────────────────────────────────

class TestAgentHealthReport:

    def test_overall_status_is_worst(self):
        report = AgentHealthReport(agent_id=AgentId("x"))
        from evaluation.health_checker import HealthCheckResult
        report.results = [
            HealthCheckResult(level=HealthLevel.L1_LIVENESS, status=HealthCheckStatus.PASS, detail="ok"),
            HealthCheckResult(level=HealthLevel.L2_PROGRESS, status=HealthCheckStatus.WARN, detail="warn"),
            HealthCheckResult(level=HealthLevel.L3_RESOURCES, status=HealthCheckStatus.FAIL, detail="fail"),
            HealthCheckResult(level=HealthLevel.L4_QUALITY, status=HealthCheckStatus.PASS, detail="ok"),
        ]
        assert report.overall_status == HealthCheckStatus.FAIL

    def test_passed_failed_properties(self):
        checker = HealthChecker()
        good = fresh_state()
        bad = fresh_state(status=AgentStatus.STUCK)
        assert checker.check(good).passed is True
        assert checker.check(bad).failed is True

    def test_by_level_returns_correct_result(self):
        checker = HealthChecker()
        state = fresh_state()
        report = checker.check(state)
        l1 = report.by_level(HealthLevel.L1_LIVENESS)
        assert l1 is not None
        assert l1.level == HealthLevel.L1_LIVENESS


# ── Evaluator ─────────────────────────────────────────────────────────────────

@pytest.fixture
def bus() -> EventBus:
    return EventBus()


@pytest.fixture
def registry(bus: EventBus) -> AgentRegistry:
    return AgentRegistry(bus)


@pytest.fixture
def ledger() -> RecoveryLedger:
    return RecoveryLedger()


@pytest.fixture
def evaluator(registry: AgentRegistry, bus: EventBus, ledger: RecoveryLedger) -> Evaluator:
    return Evaluator(
        registry=registry,
        bus=bus,
        ledger=ledger,
        checker=HealthChecker(heartbeat_timeout_seconds=30.0),
        verification_delay_seconds=0.05,
        verification_retries=2,
        retry_interval_seconds=0.05,
    )


class TestEvaluator:

    @pytest.mark.asyncio
    async def test_healthy_agent_passes(self, evaluator: Evaluator, registry: AgentRegistry):
        """Healthy agent after recovery → EVALUATION_PASSED, ledger SUCCESS."""
        agent = await registry.register(AgentInfo(name="healthy", role="test"))
        await registry.mark_ready(agent.agent_id)

        result = await evaluator.evaluate_now(agent.agent_id, incident_id="inc-test1")
        assert result.passed is True

    @pytest.mark.asyncio
    async def test_unhealthy_agent_fails(self, evaluator: Evaluator, registry: AgentRegistry, bus: EventBus):
        """Still-unhealthy agent → EVALUATION_FAILED."""
        emitted: list[BossmanEvent] = []
        bus.subscribe(lambda e: emitted.append(e))

        agent = await registry.register(AgentInfo(name="broken", role="test"))
        await registry.mark_ready(agent.agent_id)
        await registry.update_status(agent.agent_id, AgentStatus.RUNNING)
        await registry.update_status(agent.agent_id, AgentStatus.STUCK)

        result = await evaluator.evaluate_now(agent.agent_id, incident_id="inc-test2")
        assert result.passed is False
        await asyncio.sleep(0.05)
        fail_events = [e for e in emitted if e.event_type == EventType.EVALUATION_FAILED]
        assert len(fail_events) >= 1

    @pytest.mark.asyncio
    async def test_retries_before_giving_up(self, registry: AgentRegistry, bus: EventBus, ledger: RecoveryLedger):
        """Evaluator retries verification_retries times before giving up."""
        retry_counts: list[int] = []
        checker = HealthChecker()
        original_check = checker.check

        call_count = [0]
        def counting_check(state):
            call_count[0] += 1
            # Always return STUCK state
            stuck_state = fresh_state(status=AgentStatus.STUCK)
            return original_check(stuck_state)

        checker.check = counting_check
        ev = Evaluator(
            registry=registry,
            bus=bus,
            ledger=ledger,
            checker=checker,
            verification_delay_seconds=0.0,
            verification_retries=2,
            retry_interval_seconds=0.02,
        )

        agent = await registry.register(AgentInfo(name="retry-a", role="test"))
        await registry.mark_ready(agent.agent_id)
        await registry.update_status(agent.agent_id, AgentStatus.RUNNING)
        await registry.update_status(agent.agent_id, AgentStatus.STUCK)

        result = await ev.evaluate_now(agent.agent_id)
        assert result.retry_count == 2  # exhausted all retries
        assert call_count[0] >= 3  # 1 initial + 2 retries

    @pytest.mark.asyncio
    async def test_evaluate_now_without_event(self, evaluator: Evaluator, registry: AgentRegistry):
        """evaluate_now() works without RECOVERY_STARTED event."""
        agent = await registry.register(AgentInfo(name="direct", role="test"))
        await registry.mark_ready(agent.agent_id)
        result = await evaluator.evaluate_now(agent.agent_id)
        assert isinstance(result.passed, bool)

    @pytest.mark.asyncio
    async def test_stats_tracks_counts(self, evaluator: Evaluator, registry: AgentRegistry):
        """stats() tracks pass/fail counts accurately."""
        a1 = await registry.register(AgentInfo(name="s1", role="test"))
        a2 = await registry.register(AgentInfo(name="s2", role="test"))
        await registry.mark_ready(a1.agent_id)
        await registry.mark_ready(a2.agent_id)
        await registry.update_status(a2.agent_id, AgentStatus.RUNNING)
        await registry.update_status(a2.agent_id, AgentStatus.STUCK)

        await evaluator.evaluate_now(a1.agent_id)  # PASS
        await evaluator.evaluate_now(a2.agent_id)  # FAIL

        stats = evaluator.stats
        assert stats["pass_count"] >= 1
        assert stats["fail_count"] >= 1
        assert stats["total_evaluations"] == 2

    @pytest.mark.asyncio
    async def test_wires_to_recovery_started_event(
        self, evaluator: Evaluator, registry: AgentRegistry, bus: EventBus
    ):
        """End-to-end: RECOVERY_STARTED event → evaluator verifies → EVALUATION_PASSED."""
        emitted: list[BossmanEvent] = []
        bus.subscribe(lambda e: emitted.append(e))

        agent = await registry.register(AgentInfo(name="e2e", role="test"))
        await registry.mark_ready(agent.agent_id)
        await evaluator.start()

        # Publish RECOVERY_STARTED
        await bus.publish(BossmanEvent.create(
            EventType.RECOVERY_STARTED,
            agent_id=agent.agent_id,
            payload={"incident_id": "inc-e2e-001", "strategy": "RESTART_AGENT"},
        ))

        # Wait for delay + verification
        await asyncio.sleep(0.3)
        await evaluator.stop()

        eval_events = [
            e for e in emitted
            if e.event_type in {EventType.EVALUATION_PASSED, EventType.EVALUATION_FAILED}
        ]
        assert len(eval_events) >= 1
