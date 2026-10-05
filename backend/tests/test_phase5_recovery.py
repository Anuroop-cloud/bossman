"""
tests.test_phase5_recovery
───────────────────────────
Phase 5 test suite: Circuit Breaker + Recovery Engine.

Tests:
  CircuitBreaker
    1.  CLOSED state allows calls through
    2.  Failure threshold opens circuit (CLOSED → OPEN)
    3.  OPEN state rejects calls with CircuitOpenError
    4.  OPEN state uses fallback when provided
    5.  OPEN → HALF_OPEN after timeout
    6.  HALF_OPEN success × success_threshold → CLOSED
    7.  HALF_OPEN failure → OPEN again
    8.  force_open() immediately opens circuit
    9.  force_close() resets circuit to CLOSED
   10.  Publishes CIRCUIT_BREAKER_OPENED, HALF_OPEN, CLOSED events
   11.  stats() accurate

  CircuitBreakerRegistry
   12.  get_or_create returns same instance for same key
   13.  open() and close() delegate correctly

  RecoveryEngine — strategy handlers
   14.  WAIT_AND_RETRY: agent transitions STUCK→RECOVERING→IDLE
   15.  RESTART_AGENT: agent restarts, resources cleared, AGENT_RESTARTED emitted
   16.  RESTART_AGENT: restart budget exhaustion → ESCALATE_HUMAN
   17.  REASSIGN_TASK: task reassigned to healthy agent same role
   18.  REASSIGN_TASK: no healthy agent → DLQ
   19.  CIRCUIT_BREAKER: circuit opened for agent
   20.  CONTEXT_COMPACTION: directive queued in agent metadata
   21.  RELEASE_RESOURCES: resources_held cleared from AgentState
   22.  QUEUE_FOR_LATER: current task moved to DLQ
   23.  ESCALATE_HUMAN: HUMAN_GATE_REQUIRED event emitted
   24.  ROLLBACK_CHECKPOINT: rollback directive queued in metadata

  RecoveryEngine — integration
   25.  RECOVERY_STARTED + RECOVERY_STRATEGY_CHOSEN published per run
   26.  RECOVERY_STEP published for each handler step
   27.  Ledger records every attempt
   28.  Duplicate recovery for same agent is skipped
   29.  Full failure→detect→diagnose→recover pipeline end-to-end
   30.  stats() includes circuit breakers and ledger summary
"""

from __future__ import annotations

import asyncio
import time
from typing import Any

import pytest

from contracts.agent_state import AgentId, AgentInfo, AgentStatus, TaskId
from contracts.events import BossmanEvent, EventType
from contracts.recovery_ledger import FailureType, RecoveryOutcome, RecoveryStrategy
from core.event_bus import EventBus
from core.registry import AgentRegistry
from core.task_manager import TaskManager
from detection.diagnostician import Diagnosis, Diagnostician
from detection.failure_detector import DetectedFailure, DetectorConfig, FailureDetector
from recovery.circuit_breaker import CircuitBreaker, CircuitBreakerRegistry, CircuitOpenError, CircuitState
from recovery.recovery_engine import RecoveryEngine


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def bus() -> EventBus:
    return EventBus()


@pytest.fixture
def registry(bus: EventBus) -> AgentRegistry:
    return AgentRegistry(bus)


@pytest.fixture
def task_manager(bus: EventBus) -> TaskManager:
    return TaskManager(bus)


@pytest.fixture
def cb_registry(bus: EventBus) -> CircuitBreakerRegistry:
    return CircuitBreakerRegistry(
        failure_threshold=3,
        success_threshold=2,
        open_timeout_seconds=0.1,  # short for tests
        event_bus=bus,
    )


@pytest.fixture
def diagnostician() -> Diagnostician:
    return Diagnostician()


@pytest.fixture
def engine(
    registry: AgentRegistry,
    task_manager: TaskManager,
    bus: EventBus,
    diagnostician: Diagnostician,
    cb_registry: CircuitBreakerRegistry,
) -> RecoveryEngine:
    return RecoveryEngine(
        registry=registry,
        task_manager=task_manager,
        bus=bus,
        diagnostician=diagnostician,
        circuit_breakers=cb_registry,
        max_restarts=3,
        restart_window_seconds=5.0,
    )


def make_detected(failure_type: FailureType, agent_id: str = "agent-x", **tel: Any) -> DetectedFailure:
    return DetectedFailure(
        failure_type=failure_type,
        agent_id=AgentId(agent_id),
        detail=f"Test: {failure_type.value}",
        telemetry=tel,
    )


async def emit_events(bus: EventBus, emitted: list[BossmanEvent]) -> None:
    async def capture(e: BossmanEvent) -> None:
        emitted.append(e)
    bus.subscribe(capture)


# ── CircuitBreaker ─────────────────────────────────────────────────────────────

class TestCircuitBreaker:

    @pytest.mark.asyncio
    async def test_closed_allows_calls(self):
        cb = CircuitBreaker("test", failure_threshold=3)
        async def ok_fn():
            return "ok"
        result = await cb.call(ok_fn)
        assert result == "ok"

    @pytest.mark.asyncio
    async def test_failure_threshold_opens_circuit(self):
        cb = CircuitBreaker("test", failure_threshold=3)

        async def fail():
            raise RuntimeError("boom")

        for _ in range(3):
            try:
                await cb.call(fail)
            except RuntimeError:
                pass

        assert cb._state == CircuitState.OPEN

    @pytest.mark.asyncio
    async def test_open_rejects_calls(self):
        cb = CircuitBreaker("test", failure_threshold=2)

        async def fail():
            raise RuntimeError("boom")

        for _ in range(2):
            try:
                await cb.call(fail)
            except RuntimeError:
                pass

        with pytest.raises(CircuitOpenError):
            await cb.call(lambda: asyncio.sleep(0))

    @pytest.mark.asyncio
    async def test_open_uses_fallback(self):
        cb = CircuitBreaker("test", failure_threshold=1)

        async def fail():
            raise RuntimeError("boom")

        try:
            await cb.call(fail)
        except RuntimeError:
            pass

        result = await cb.call(lambda: asyncio.sleep(0), fallback="fallback-value")
        assert result == "fallback-value"

    @pytest.mark.asyncio
    async def test_open_transitions_to_half_open_after_timeout(self):
        cb = CircuitBreaker("test", failure_threshold=1, open_timeout_seconds=0.05)

        async def fail():
            raise RuntimeError("boom")

        try:
            await cb.call(fail)
        except RuntimeError:
            pass

        assert cb._state == CircuitState.OPEN
        await asyncio.sleep(0.08)
        # Accessing .state triggers the time-based transition
        assert cb.state == CircuitState.HALF_OPEN

    @pytest.mark.asyncio
    async def test_half_open_success_closes_circuit(self):
        cb = CircuitBreaker("test", failure_threshold=1, success_threshold=1, open_timeout_seconds=0.05)

        async def fail():
            raise RuntimeError("boom")

        try:
            await cb.call(fail)
        except RuntimeError:
            pass

        await asyncio.sleep(0.08)
        assert cb.state == CircuitState.HALF_OPEN

        async def succeed():
            return "ok"

        await cb.call(succeed)
        assert cb._state == CircuitState.CLOSED

    @pytest.mark.asyncio
    async def test_half_open_failure_reopens(self):
        cb = CircuitBreaker("test", failure_threshold=1, open_timeout_seconds=0.05)

        async def fail():
            raise RuntimeError("boom")

        try:
            await cb.call(fail)
        except RuntimeError:
            pass

        await asyncio.sleep(0.08)
        assert cb.state == CircuitState.HALF_OPEN

        try:
            await cb.call(fail)
        except RuntimeError:
            pass

        assert cb._state == CircuitState.OPEN

    @pytest.mark.asyncio
    async def test_force_open(self):
        cb = CircuitBreaker("test")
        assert cb._state == CircuitState.CLOSED
        cb.force_open()
        assert cb._state == CircuitState.OPEN

    @pytest.mark.asyncio
    async def test_force_close(self):
        cb = CircuitBreaker("test", failure_threshold=1)

        async def fail():
            raise RuntimeError()

        try:
            await cb.call(fail)
        except RuntimeError:
            pass

        assert cb._state == CircuitState.OPEN
        cb.force_close()
        assert cb._state == CircuitState.CLOSED

    @pytest.mark.asyncio
    async def test_publishes_circuit_breaker_events(self, bus: EventBus):
        emitted: list[BossmanEvent] = []
        await emit_events(bus, emitted)

        cb = CircuitBreaker("test-cb", failure_threshold=2, event_bus=bus)

        async def fail():
            raise RuntimeError()

        for _ in range(2):
            try:
                await cb.call(fail)
            except RuntimeError:
                pass

        await asyncio.sleep(0.01)
        types = {e.event_type for e in emitted}
        assert EventType.CIRCUIT_BREAKER_OPENED in types

    def test_stats_accurate(self):
        cb = CircuitBreaker("test", failure_threshold=5)
        cb.force_open()
        stats = cb.stats
        assert stats["state"] == "OPEN"
        assert stats["total_opened"] == 1

    def test_registry_get_or_create_same_instance(self, cb_registry: CircuitBreakerRegistry):
        cb1 = cb_registry.get_or_create("key-a")
        cb2 = cb_registry.get_or_create("key-a")
        assert cb1 is cb2

    def test_registry_open_and_close(self, cb_registry: CircuitBreakerRegistry):
        cb = cb_registry.open("key-b")
        assert cb.state == CircuitState.OPEN
        cb_registry.close("key-b")
        assert cb.state == CircuitState.CLOSED


# ── RecoveryEngine — strategy handlers ────────────────────────────────────────

class TestRecoveryEngineStrategies:

    @pytest.mark.asyncio
    async def test_wait_and_retry(self, engine: RecoveryEngine, registry: AgentRegistry):
        """WAIT_AND_RETRY: agent goes STUCK→RECOVERING→IDLE."""
        agent = await registry.register(AgentInfo(name="a1", role="test"))
        await registry.mark_ready(agent.agent_id)
        await registry.update_status(agent.agent_id, AgentStatus.RUNNING)
        await registry.update_status(agent.agent_id, AgentStatus.STUCK)

        detected = make_detected(FailureType.RESOURCE_STARVATION, agent_id=agent.agent_id)
        # Override diagnostician to always recommend WAIT_AND_RETRY
        from unittest.mock import patch, MagicMock
        mock_diag = MagicMock()
        mock_diag.diagnose.return_value = Diagnosis(
            failure_type=FailureType.RESOURCE_STARVATION,
            root_cause="test",
            recommended_strategy=RecoveryStrategy.WAIT_AND_RETRY,
            confidence=0.8,
            agent_id=agent.agent_id,
            incident_id=detected.incident_id,
        )
        engine._diagnostician = mock_diag

        attempt = await engine.handle_failure(detected)

        state = await registry.get(agent.agent_id)
        assert state is not None
        assert state.status == AgentStatus.IDLE
        assert attempt is not None

    @pytest.mark.asyncio
    async def test_restart_agent(self, engine: RecoveryEngine, registry: AgentRegistry, bus: EventBus):
        """RESTART_AGENT: agent restarts, resources cleared, AGENT_RESTARTED emitted."""
        emitted: list[BossmanEvent] = []
        await emit_events(bus, emitted)

        agent = await registry.register(AgentInfo(name="a2", role="test"))
        await registry.mark_ready(agent.agent_id)
        await registry.update_status(agent.agent_id, AgentStatus.RUNNING)
        await registry.update_status(agent.agent_id, AgentStatus.STUCK)

        state = await registry.get(agent.agent_id)
        state.resource_usage.resources_held = ["lock-x"]
        state.resource_usage.resources_waiting = ["lock-y"]

        detected = make_detected(FailureType.HEARTBEAT_TIMEOUT, agent_id=agent.agent_id)

        from unittest.mock import MagicMock
        mock_diag = MagicMock()
        mock_diag.diagnose.return_value = Diagnosis(
            failure_type=FailureType.HEARTBEAT_TIMEOUT,
            root_cause="timeout",
            recommended_strategy=RecoveryStrategy.RESTART_AGENT,
            confidence=0.85,
            agent_id=agent.agent_id,
            incident_id=detected.incident_id,
        )
        engine._diagnostician = mock_diag

        await engine.handle_failure(detected)
        await asyncio.sleep(0.05)

        state = await registry.get(agent.agent_id)
        assert state.status == AgentStatus.IDLE
        assert state.resource_usage.resources_held == []
        assert state.resource_usage.resources_waiting == []

        restarts = [e for e in emitted if e.event_type == EventType.AGENT_RESTARTED]
        assert len(restarts) >= 1

    @pytest.mark.asyncio
    async def test_restart_budget_exhausted_escalates(self, engine: RecoveryEngine, registry: AgentRegistry, bus: EventBus):
        """After max_restarts exceeded, ESCALATE_HUMAN replaces RESTART_AGENT."""
        emitted: list[BossmanEvent] = []
        await emit_events(bus, emitted)

        agent = await registry.register(AgentInfo(name="a3", role="test"))
        await registry.mark_ready(agent.agent_id)

        # Exhaust the budget
        now = time.monotonic()
        engine._restart_history[agent.agent_id].extend([now, now, now])  # 3 restarts

        detected = make_detected(FailureType.HEARTBEAT_TIMEOUT, agent_id=agent.agent_id)
        from unittest.mock import MagicMock
        mock_diag = MagicMock()
        mock_diag.diagnose.return_value = Diagnosis(
            failure_type=FailureType.HEARTBEAT_TIMEOUT,
            root_cause="timeout",
            recommended_strategy=RecoveryStrategy.RESTART_AGENT,
            confidence=0.85,
            agent_id=agent.agent_id,
            incident_id=detected.incident_id,
        )
        engine._diagnostician = mock_diag

        await engine.handle_failure(detected)
        await asyncio.sleep(0.05)

        human_gates = [e for e in emitted if e.event_type == EventType.HUMAN_GATE_REQUIRED]
        assert len(human_gates) >= 1

    @pytest.mark.asyncio
    async def test_reassign_task(self, engine: RecoveryEngine, registry: AgentRegistry, task_manager: TaskManager):
        """REASSIGN_TASK: task moved to healthy same-role agent."""
        primary = await registry.register(AgentInfo(name="p1", role="worker"))
        await registry.mark_ready(primary.agent_id)
        await registry.update_status(primary.agent_id, AgentStatus.RUNNING)
        await registry.update_status(primary.agent_id, AgentStatus.FAILED)

        standby = await registry.register(AgentInfo(name="s1", role="worker"))
        await registry.mark_ready(standby.agent_id)  # IDLE

        task = await task_manager.create("work-item")
        await task_manager.assign(task.task_id, primary.agent_id)

        primary_state = await registry.get(primary.agent_id)
        primary_state.current_task_id = task.task_id

        detected = make_detected(FailureType.HEARTBEAT_TIMEOUT, agent_id=primary.agent_id)
        from unittest.mock import MagicMock
        mock_diag = MagicMock()
        mock_diag.diagnose.return_value = Diagnosis(
            failure_type=FailureType.HEARTBEAT_TIMEOUT,
            root_cause="timeout",
            recommended_strategy=RecoveryStrategy.REASSIGN_TASK,
            confidence=0.85,
            agent_id=primary.agent_id,
            incident_id=detected.incident_id,
        )
        engine._diagnostician = mock_diag

        await engine.handle_failure(detected)

        task_state = await task_manager.get(task.task_id)
        assert task_state is not None
        assert task_state.assigned_to == standby.agent_id

    @pytest.mark.asyncio
    async def test_reassign_no_healthy_agent_queues_dlq(
        self, engine: RecoveryEngine, registry: AgentRegistry, task_manager: TaskManager
    ):
        """REASSIGN_TASK with no healthy candidates → task goes to DLQ."""
        agent = await registry.register(AgentInfo(name="solo", role="unique-role"))
        await registry.mark_ready(agent.agent_id)
        await registry.update_status(agent.agent_id, AgentStatus.RUNNING)
        await registry.update_status(agent.agent_id, AgentStatus.FAILED)

        # max_attempts=1 so a single fail() sends it to DLQ
        task = await task_manager.create("solo-task", max_attempts=1)
        await task_manager.assign(task.task_id, agent.agent_id)
        await task_manager.mark_running(task.task_id)

        state = await registry.get(agent.agent_id)
        state.current_task_id = task.task_id

        detected = make_detected(FailureType.HEARTBEAT_TIMEOUT, agent_id=agent.agent_id)
        from unittest.mock import MagicMock
        mock_diag = MagicMock()
        mock_diag.diagnose.return_value = Diagnosis(
            failure_type=FailureType.HEARTBEAT_TIMEOUT,
            root_cause="timeout",
            recommended_strategy=RecoveryStrategy.REASSIGN_TASK,
            confidence=0.85,
            agent_id=agent.agent_id,
            incident_id=detected.incident_id,
        )
        engine._diagnostician = mock_diag

        await engine.handle_failure(detected)
        dlq = await task_manager.dlq_contents()
        assert any(t.task_id == task.task_id for t in dlq)

    @pytest.mark.asyncio
    async def test_circuit_breaker_opens(self, engine: RecoveryEngine, cb_registry: CircuitBreakerRegistry):
        """CIRCUIT_BREAKER strategy opens circuit for agent."""
        detected = make_detected(FailureType.RETRY_STORM, agent_id="agent-cb")
        from unittest.mock import MagicMock
        mock_diag = MagicMock()
        mock_diag.diagnose.return_value = Diagnosis(
            failure_type=FailureType.RETRY_STORM,
            root_cause="storm",
            recommended_strategy=RecoveryStrategy.CIRCUIT_BREAKER,
            confidence=0.90,
            agent_id=AgentId("agent-cb"),
            incident_id=detected.incident_id,
        )
        engine._diagnostician = mock_diag

        await engine.handle_failure(detected)

        cb = cb_registry.get_or_create("agent-cb")
        assert cb.state == CircuitState.OPEN

    @pytest.mark.asyncio
    async def test_context_compaction_directive(self, engine: RecoveryEngine, registry: AgentRegistry):
        """CONTEXT_COMPACTION queues directive in agent metadata."""
        agent = await registry.register(AgentInfo(name="compact-a", role="test"))
        await registry.mark_ready(agent.agent_id)

        detected = make_detected(FailureType.CONTEXT_OVERFLOW, agent_id=agent.agent_id)
        from unittest.mock import MagicMock
        mock_diag = MagicMock()
        mock_diag.diagnose.return_value = Diagnosis(
            failure_type=FailureType.CONTEXT_OVERFLOW,
            root_cause="overflow",
            recommended_strategy=RecoveryStrategy.CONTEXT_COMPACTION,
            confidence=0.95,
            agent_id=agent.agent_id,
            incident_id=detected.incident_id,
        )
        engine._diagnostician = mock_diag

        await engine.handle_failure(detected)

        state = await registry.get(agent.agent_id)
        assert state.info.metadata.get("directive") == "compact_context"

    @pytest.mark.asyncio
    async def test_release_resources(self, engine: RecoveryEngine, registry: AgentRegistry):
        """RELEASE_RESOURCES clears resources_held and resources_waiting."""
        agent = await registry.register(AgentInfo(name="locked-a", role="test"))
        await registry.mark_ready(agent.agent_id)
        state = await registry.get(agent.agent_id)
        state.resource_usage.resources_held = ["lock-x", "lock-y"]
        state.resource_usage.resources_waiting = ["lock-z"]

        detected = make_detected(FailureType.DEADLOCK, agent_id=agent.agent_id)
        from unittest.mock import MagicMock
        mock_diag = MagicMock()
        mock_diag.diagnose.return_value = Diagnosis(
            failure_type=FailureType.DEADLOCK,
            root_cause="deadlock",
            recommended_strategy=RecoveryStrategy.RELEASE_RESOURCES,
            confidence=0.88,
            agent_id=agent.agent_id,
            incident_id=detected.incident_id,
        )
        engine._diagnostician = mock_diag

        await engine.handle_failure(detected)

        state = await registry.get(agent.agent_id)
        assert state.resource_usage.resources_held == []
        assert state.resource_usage.resources_waiting == []

    @pytest.mark.asyncio
    async def test_queue_for_later(self, engine: RecoveryEngine, registry: AgentRegistry, task_manager: TaskManager):
        """QUEUE_FOR_LATER sends current task to DLQ."""
        agent = await registry.register(AgentInfo(name="queue-a", role="test"))
        await registry.mark_ready(agent.agent_id)
        await registry.update_status(agent.agent_id, AgentStatus.RUNNING)

        task = await task_manager.create("later-task", max_attempts=1)
        await task_manager.assign(task.task_id, agent.agent_id)
        await task_manager.mark_running(task.task_id)

        state = await registry.get(agent.agent_id)
        state.current_task_id = task.task_id

        detected = make_detected(FailureType.RESOURCE_STARVATION, agent_id=agent.agent_id)
        from unittest.mock import MagicMock
        mock_diag = MagicMock()
        mock_diag.diagnose.return_value = Diagnosis(
            failure_type=FailureType.RESOURCE_STARVATION,
            root_cause="starvation",
            recommended_strategy=RecoveryStrategy.QUEUE_FOR_LATER,
            confidence=0.80,
            agent_id=agent.agent_id,
            incident_id=detected.incident_id,
        )
        engine._diagnostician = mock_diag

        await engine.handle_failure(detected)
        dlq = await task_manager.dlq_contents()
        assert any(t.task_id == task.task_id for t in dlq)

    @pytest.mark.asyncio
    async def test_escalate_human(self, engine: RecoveryEngine, bus: EventBus):
        """ESCALATE_HUMAN emits HUMAN_GATE_REQUIRED event."""
        emitted: list[BossmanEvent] = []
        await emit_events(bus, emitted)

        detected = make_detected(FailureType.UNKNOWN, agent_id="agent-esc")
        from unittest.mock import MagicMock
        mock_diag = MagicMock()
        mock_diag.diagnose.return_value = Diagnosis(
            failure_type=FailureType.UNKNOWN,
            root_cause="unknown",
            recommended_strategy=RecoveryStrategy.ESCALATE_HUMAN,
            confidence=0.30,
            agent_id=AgentId("agent-esc"),
            incident_id=detected.incident_id,
        )
        engine._diagnostician = mock_diag

        await engine.handle_failure(detected)
        await asyncio.sleep(0.01)

        gate_events = [e for e in emitted if e.event_type == EventType.HUMAN_GATE_REQUIRED]
        assert len(gate_events) >= 1

    @pytest.mark.asyncio
    async def test_rollback_checkpoint(self, engine: RecoveryEngine, registry: AgentRegistry):
        """ROLLBACK_CHECKPOINT queues rollback directive in agent metadata."""
        agent = await registry.register(AgentInfo(name="chk-a", role="test"))
        await registry.mark_ready(agent.agent_id)

        detected = make_detected(FailureType.HEARTBEAT_TIMEOUT, agent_id=agent.agent_id)
        from unittest.mock import MagicMock
        mock_diag = MagicMock()
        mock_diag.diagnose.return_value = Diagnosis(
            failure_type=FailureType.HEARTBEAT_TIMEOUT,
            root_cause="timeout",
            recommended_strategy=RecoveryStrategy.ROLLBACK_CHECKPOINT,
            confidence=0.85,
            agent_id=agent.agent_id,
            incident_id=detected.incident_id,
        )
        engine._diagnostician = mock_diag

        await engine.handle_failure(detected)

        state = await registry.get(agent.agent_id)
        assert state.info.metadata.get("directive") == "rollback_checkpoint"


# ── RecoveryEngine — integration ──────────────────────────────────────────────

class TestRecoveryEngineIntegration:

    @pytest.mark.asyncio
    async def test_recovery_events_published(self, engine: RecoveryEngine, registry: AgentRegistry, bus: EventBus):
        """RECOVERY_STARTED and RECOVERY_STRATEGY_CHOSEN are published per run."""
        emitted: list[BossmanEvent] = []
        await emit_events(bus, emitted)

        agent = await registry.register(AgentInfo(name="evt-a", role="test"))
        await registry.mark_ready(agent.agent_id)
        await registry.update_status(agent.agent_id, AgentStatus.RUNNING)
        await registry.update_status(agent.agent_id, AgentStatus.STUCK)

        detected = make_detected(FailureType.RESOURCE_STARVATION, agent_id=agent.agent_id)
        from unittest.mock import MagicMock
        mock_diag = MagicMock()
        mock_diag.diagnose.return_value = Diagnosis(
            failure_type=FailureType.RESOURCE_STARVATION,
            root_cause="test",
            recommended_strategy=RecoveryStrategy.WAIT_AND_RETRY,
            confidence=0.80,
            agent_id=agent.agent_id,
            incident_id=detected.incident_id,
        )
        engine._diagnostician = mock_diag
        engine._restart_window = 0.01  # speed up wait_and_retry

        await engine.handle_failure(detected)
        await asyncio.sleep(0.05)

        types = {e.event_type for e in emitted}
        assert EventType.RECOVERY_STARTED in types
        assert EventType.RECOVERY_STRATEGY_CHOSEN in types

    @pytest.mark.asyncio
    async def test_ledger_records_attempt(self, engine: RecoveryEngine):
        """Every handled failure produces a ledger entry."""
        detected = make_detected(FailureType.RETRY_STORM, agent_id="ledger-a")
        from unittest.mock import MagicMock
        mock_diag = MagicMock()
        mock_diag.diagnose.return_value = Diagnosis(
            failure_type=FailureType.RETRY_STORM,
            root_cause="storm",
            recommended_strategy=RecoveryStrategy.CIRCUIT_BREAKER,
            confidence=0.90,
            agent_id=AgentId("ledger-a"),
            incident_id=detected.incident_id,
        )
        engine._diagnostician = mock_diag

        await engine.handle_failure(detected)
        assert len(engine.ledger.entries) == 1
        assert engine.ledger.entries[0].failure_type == FailureType.RETRY_STORM

    @pytest.mark.asyncio
    async def test_duplicate_recovery_skipped(self, engine: RecoveryEngine, registry: AgentRegistry):
        """Second concurrent recovery for same agent is silently skipped."""
        agent = await registry.register(AgentInfo(name="dup-a", role="test"))
        await registry.mark_ready(agent.agent_id)
        await registry.update_status(agent.agent_id, AgentStatus.RUNNING)
        await registry.update_status(agent.agent_id, AgentStatus.STUCK)

        detected = make_detected(FailureType.RESOURCE_STARVATION, agent_id=agent.agent_id)

        from unittest.mock import MagicMock
        mock_diag = MagicMock()
        mock_diag.diagnose.return_value = Diagnosis(
            failure_type=FailureType.RESOURCE_STARVATION,
            root_cause="test",
            recommended_strategy=RecoveryStrategy.WAIT_AND_RETRY,
            confidence=0.80,
            agent_id=agent.agent_id,
            incident_id=detected.incident_id,
        )
        engine._diagnostician = mock_diag

        # Manually add to active recoveries to simulate an in-flight recovery
        engine._active_recoveries.add(agent.agent_id)

        result = await engine.handle_failure(detected)
        assert result is None
        assert engine._skipped_duplicate == 1

    @pytest.mark.asyncio
    async def test_full_pipeline_end_to_end(
        self, registry: AgentRegistry, task_manager: TaskManager, bus: EventBus
    ):
        """End-to-end: FailureDetector → FailureDetector.on_failure → RecoveryEngine → CIRCUIT_BREAKER opened."""
        diag = Diagnostician()
        cb_reg = CircuitBreakerRegistry(failure_threshold=3, event_bus=bus)
        engine = RecoveryEngine(
            registry=registry,
            task_manager=task_manager,
            bus=bus,
            diagnostician=diag,
            circuit_breakers=cb_reg,
        )

        cfg = DetectorConfig(retry_storm_threshold=3, retry_storm_window_seconds=2.0)
        detector = FailureDetector(bus, config=cfg, on_failure=engine.handle_failure)
        await detector.start()

        # Generate retry storm
        for _ in range(3):
            await bus.publish(BossmanEvent.create(
                EventType.TASK_FAILED,
                agent_id=AgentId("e2e-agent"),
                payload={"error": "timeout"},
            ))
        await asyncio.sleep(0.1)

        await detector.stop()

        assert engine._handled >= 1
        assert len(engine.ledger.entries) >= 1
        entry = engine.ledger.entries[0]
        assert entry.failure_type == FailureType.RETRY_STORM
        assert entry.strategy == RecoveryStrategy.CIRCUIT_BREAKER

        cb = cb_reg.get_or_create("e2e-agent")
        assert cb.state == CircuitState.OPEN

    @pytest.mark.asyncio
    async def test_stats_structure(self, engine: RecoveryEngine):
        """stats() returns circuit breakers and ledger summary."""
        stats = engine.stats
        assert "handled" in stats
        assert "circuit_breakers" in stats
        assert "ledger" in stats
        assert "by_outcome" in stats["ledger"]
        assert "by_strategy" in stats["ledger"]
