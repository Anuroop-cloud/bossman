"""
tests/test_phase1_core.py
─────────────────────────
Smoke tests for Phase 1: EventBus, AgentRegistry, TaskManager,
SubagentManager, Watchdog, and FaultInjector.

Run with:  python -m pytest tests/ -v
"""

import asyncio
import pytest
from datetime import timedelta, timezone, datetime

from contracts.agent_state import AgentInfo, AgentStatus, PermissionTier
from contracts.events import EventType
from contracts.heartbeat import HeartbeatPayload
from contracts.recovery_ledger import RecoveryAttempt, RecoveryStrategy, FailureType, new_incident_id
from core.event_bus import EventBus
from core.registry import AgentRegistry
from core.task_manager import TaskManager, TaskPriority, TaskStatus
from core.subagent_manager import SubagentManager
from core.watchdog import Watchdog
from core.fault_injector import FaultInjector, FaultType, FaultConfig


# ── Fixtures ──────────────────────────────────────────────────────────────────


@pytest.fixture
def bus():
    return EventBus()


@pytest.fixture
def registry(bus):
    return AgentRegistry(bus)


@pytest.fixture
def task_manager(bus):
    return TaskManager(bus)


@pytest.fixture
def subagent_manager(bus):
    return SubagentManager(bus)


@pytest.fixture
def fault_injector(registry, task_manager, bus):
    return FaultInjector(registry, task_manager, bus)


# ── EventBus tests ────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_event_bus_wildcard_subscriber(bus):
    received = []
    async def handler(event):
        received.append(event.event_type)

    bus.subscribe(handler)
    from contracts.events import BossmanEvent
    await bus.publish(BossmanEvent.create(EventType.BOSSMAN_STARTED))
    await bus.publish(BossmanEvent.create(EventType.AGENT_REGISTERED))
    assert EventType.BOSSMAN_STARTED in received
    assert EventType.AGENT_REGISTERED in received


@pytest.mark.asyncio
async def test_event_bus_typed_subscriber(bus):
    received = []
    async def handler(event):
        received.append(event)

    bus.subscribe(handler, EventType.AGENT_TIMEOUT)
    from contracts.events import BossmanEvent
    await bus.publish(BossmanEvent.create(EventType.BOSSMAN_STARTED))
    await bus.publish(BossmanEvent.create(EventType.AGENT_TIMEOUT))
    assert len(received) == 1
    assert received[0].event_type == EventType.AGENT_TIMEOUT


@pytest.mark.asyncio
async def test_event_bus_subscriber_error_isolation(bus):
    """A broken subscriber must not prevent others from receiving the event."""
    good_received = []

    async def bad_handler(event):
        raise RuntimeError("Subscriber is broken")

    async def good_handler(event):
        good_received.append(event)

    bus.subscribe(bad_handler)
    bus.subscribe(good_handler)

    from contracts.events import BossmanEvent
    await bus.publish(BossmanEvent.create(EventType.BOSSMAN_STARTED))
    assert len(good_received) == 1
    assert bus.stats["subscriber_errors"] == 1


# ── AgentRegistry tests ───────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_register_and_ready(registry):
    info = AgentInfo(name="Research Agent", role="researcher")
    state = await registry.register(info)
    assert state.status == AgentStatus.INITIALIZING

    state = await registry.mark_ready(info.agent_id)
    assert state.status == AgentStatus.IDLE


@pytest.mark.asyncio
async def test_duplicate_registration_raises(registry):
    info = AgentInfo(name="Coder", role="coder")
    await registry.register(info)
    with pytest.raises(ValueError, match="already registered"):
        await registry.register(info)


@pytest.mark.asyncio
async def test_heartbeat_triggers_compaction_directive(registry):
    info = AgentInfo(name="Analyst", role="analyst")
    state = await registry.register(info)
    await registry.mark_ready(info.agent_id)
    await registry.update_status(info.agent_id, AgentStatus.RUNNING)

    hb = HeartbeatPayload(
        agent_id=info.agent_id,
        status=AgentStatus.RUNNING,
        context_utilisation=0.85,  # above 0.75 threshold
        current_step="processing data",
    )
    ack = await registry.record_heartbeat(hb)
    assert ack.compact_context is True


@pytest.mark.asyncio
async def test_deadlock_suspect_detection(registry):
    info = AgentInfo(name="Worker", role="worker")
    await registry.register(info)
    state = await registry.get(info.agent_id)
    state.resource_usage.resources_held = ["db-conn-1"]
    state.resource_usage.resources_waiting = ["file-lock-A"]
    assert state.is_deadlock_suspect is True

    suspects = await registry.deadlock_suspects()
    assert any(s.agent_id == info.agent_id for s in suspects)


# ── TaskManager tests ─────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_task_lifecycle(task_manager):
    task = await task_manager.create("Summarise research papers", priority=TaskPriority.HIGH)
    assert task.status == TaskStatus.PENDING

    from contracts.agent_state import new_agent_id
    agent_id = new_agent_id()
    task = await task_manager.assign(task.task_id, agent_id)
    assert task.status == TaskStatus.ASSIGNED

    task = await task_manager.mark_running(task.task_id)
    assert task.status == TaskStatus.RUNNING

    task = await task_manager.complete(task.task_id, output={"summary": "done"})
    assert task.status == TaskStatus.COMPLETED
    assert task.output_data["summary"] == "done"


@pytest.mark.asyncio
async def test_task_dlq_after_max_attempts(task_manager):
    task = await task_manager.create("Risky operation", max_attempts=2)
    from contracts.agent_state import new_agent_id
    agent_id = new_agent_id()

    # Fail twice — should land in DLQ
    await task_manager.assign(task.task_id, agent_id)
    await task_manager.fail(task.task_id, "error 1")
    await task_manager.assign(task.task_id, agent_id)
    await task_manager.fail(task.task_id, "error 2")

    dlq = await task_manager.dlq_contents()
    assert any(t.task_id == task.task_id for t in dlq)

    # Replay from DLQ
    replayed = await task_manager.replay_from_dlq(task.task_id)
    assert replayed is not None
    assert replayed.status == TaskStatus.PENDING
    assert replayed.attempt_count == 0


# ── SubagentManager tests ─────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_idempotency_guard_blocks_duplicate(subagent_manager):
    """Only the first spawn should succeed; subsequent ones are blocked."""
    barrier = asyncio.Event()
    results = []

    async def slow_task():
        await barrier.wait()

    first = await subagent_manager.spawn_if_not_running("memory-sync", lambda: slow_task())
    second = await subagent_manager.spawn_if_not_running("memory-sync", lambda: slow_task())
    third = await subagent_manager.spawn_if_not_running("memory-sync", lambda: slow_task())

    assert first is True
    assert second is False
    assert third is False
    assert subagent_manager.stats["total_blocked"] == 2

    barrier.set()
    await asyncio.sleep(0.05)  # let the task finish


@pytest.mark.asyncio
async def test_different_task_ids_spawn_independently(subagent_manager):
    barrier = asyncio.Event()

    async def slow():
        await barrier.wait()

    r1 = await subagent_manager.spawn_if_not_running("task-A", lambda: slow())
    r2 = await subagent_manager.spawn_if_not_running("task-B", lambda: slow())
    assert r1 is True
    assert r2 is True

    barrier.set()
    await asyncio.sleep(0.05)


# ── Watchdog tests ────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_watchdog_detects_timeout(registry, bus):
    detected = []
    async def on_event(event):
        if event.event_type == EventType.AGENT_TIMEOUT:
            detected.append(event)
    bus.subscribe(on_event, EventType.AGENT_TIMEOUT)

    info = AgentInfo(name="Slow Agent", role="worker")
    state = await registry.register(info)
    await registry.mark_ready(info.agent_id)
    await registry.update_status(info.agent_id, AgentStatus.RUNNING)
    state.current_step = "doing stuff"

    # Backdate heartbeat so watchdog sees it as 60s old
    state.last_heartbeat = datetime.now(timezone.utc) - timedelta(seconds=60)

    watchdog = Watchdog(
        registry, bus,
        check_interval_seconds=0.05,
        timeout_seconds=30.0,
    )
    await watchdog.start()
    await asyncio.sleep(0.2)
    await watchdog.stop()

    assert len(detected) >= 1
    assert detected[0].agent_id == info.agent_id


# ── FaultInjector tests ───────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_fault_injector_emits_event(registry, task_manager, bus):
    events = []
    async def collect(event):
        events.append(event)
    bus.subscribe(collect, EventType.FAULT_INJECTED)

    info = AgentInfo(name="Guinea Pig", role="worker")
    await registry.register(info)
    await registry.mark_ready(info.agent_id)
    await registry.update_status(info.agent_id, AgentStatus.RUNNING)

    injector = FaultInjector(registry, task_manager, bus)
    await injector.inject(FaultConfig(
        fault_type=FaultType.AGENT_HANG,
        agent_id=info.agent_id,
        note="pytest: hang test",
    ))

    assert len(events) == 1
    assert events[0].payload["fault_type"] == FaultType.AGENT_HANG.value


@pytest.mark.asyncio
async def test_fault_injector_deadlock(registry, task_manager, bus):
    injector = FaultInjector(registry, task_manager, bus)

    info_a = AgentInfo(name="Agent A", role="worker")
    info_b = AgentInfo(name="Agent B", role="worker")
    await registry.register(info_a)
    await registry.register(info_b)

    await injector.inject(FaultConfig(
        fault_type=FaultType.RESOURCE_DEADLOCK,
        agent_id=info_a.agent_id,
        target_agent_id=info_b.agent_id,
    ))

    state_a = await registry.get(info_a.agent_id)
    state_b = await registry.get(info_b.agent_id)
    assert state_a.is_deadlock_suspect
    assert state_b.is_deadlock_suspect


# ── Recovery Ledger smoke test ────────────────────────────────────────────────


def test_recovery_ledger_model():
    from contracts.agent_state import new_agent_id
    attempt = RecoveryAttempt(
        incident_id=new_incident_id(),
        agent_id=new_agent_id(),
        failure_type=FailureType.RETRY_STORM,
        failure_detail="47 API calls, 0 task progress",
        strategy=RecoveryStrategy.CIRCUIT_BREAKER,
    )
    assert not attempt.is_terminal
    attempt.mark_complete("SUCCESS", verified=True)
    from contracts.recovery_ledger import RecoveryOutcome
    assert attempt.outcome == RecoveryOutcome.SUCCESS
    assert attempt.verified_by_evaluator is True
    assert attempt.duration_seconds is not None
