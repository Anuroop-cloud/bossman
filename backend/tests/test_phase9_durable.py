"""Phase 9 — durable / distributed control plane tests."""

import asyncio

import fakeredis
import pytest

from contracts.agent_state import AgentInfo, AgentStatus
from contracts.events import BossmanEvent, EventType
from contracts.heartbeat import HeartbeatPayload
from contracts.recovery_ledger import (
    FailureType,
    RecoveryAttempt,
    RecoveryOutcome,
    RecoveryStrategy,
    new_incident_id,
)
from infra import (
    DistributedEventBus,
    DurableAgentRegistry,
    InMemoryBackend,
    InMemoryLedgerStore,
    LeaderElector,
    RedisBackend,
    SqlLedgerStore,
)


def _backends():
    return [
        pytest.param(lambda: InMemoryBackend(), id="memory"),
        pytest.param(
            lambda: RedisBackend(client=fakeredis.FakeAsyncRedis(decode_responses=True)),
            id="redis",
        ),
    ]


@pytest.fixture(params=_backends())
def backend(request):
    return request.param()


# ── Locks ────────────────────────────────────────────────────────────────────


async def test_lock_exclusive_and_owner_only(backend):
    assert await backend.acquire_lock("k", "a", 5000)
    assert not await backend.acquire_lock("k", "b", 5000)
    assert await backend.acquire_lock("k", "a", 5000)  # re-entrant
    assert not await backend.release_lock("k", "b")
    assert await backend.lock_owner("k") == "a"
    assert await backend.release_lock("k", "a")
    assert await backend.acquire_lock("k", "b", 5000)


async def test_lock_expires(backend):
    assert await backend.acquire_lock("k", "a", 50)
    await asyncio.sleep(0.12)
    assert await backend.acquire_lock("k", "b", 5000)
    assert not await backend.renew_lock("k", "a", 5000)


# ── Leader election ──────────────────────────────────────────────────────────


async def test_single_leader_and_failover(backend):
    events = []

    async def up():
        events.append("up")

    async def down():
        events.append("down")

    a = LeaderElector(backend, "a", lease_ms=100, on_elected=up, on_demoted=down)
    b = LeaderElector(backend, "b", lease_ms=100)

    assert await a.tick() is True
    assert await b.tick() is False
    assert await a.tick() is True  # renew
    assert await b.current_leader() == "a"

    # a "crashes" (stops renewing) → b takes over after the lease lapses
    await asyncio.sleep(0.15)
    assert await b.tick() is True
    # a returns and discovers it has been superseded
    assert await a.tick() is False
    assert events == ["up", "down"]


async def test_graceful_stop_hands_over_immediately(backend):
    a = LeaderElector(backend, "a", lease_ms=10_000)
    b = LeaderElector(backend, "b", lease_ms=10_000)
    await a.tick()
    await a.stop()
    assert await b.tick() is True


async def test_background_loop_elects():
    be = InMemoryBackend()
    a = LeaderElector(be, "a", lease_ms=100, renew_interval_s=0.02)
    await a.start()
    await asyncio.sleep(0.1)
    assert a.is_leader
    await a.stop()
    assert not a.is_leader


# ── Distributed bus ──────────────────────────────────────────────────────────


async def test_events_cross_nodes_without_duplicates(backend):
    n1 = DistributedEventBus(backend, "n1")
    n2 = DistributedEventBus(backend, "n2")
    got1, got2 = [], []

    async def h1(e):
        got1.append(e)

    async def h2(e):
        got2.append(e)

    n1.subscribe(h1)
    n2.subscribe(h2)
    await n1.start()
    await n2.start()
    await asyncio.sleep(0.05)  # let redis subscriptions attach

    ev = BossmanEvent.create(EventType.AGENT_STARTED, message="hi", agent_id="x")
    await n1.publish(ev)
    await asyncio.sleep(0.1)

    assert [e.event_id for e in got1] == [ev.event_id]  # local once, not echoed
    assert [e.event_id for e in got2] == [ev.event_id]  # remote delivered
    assert n2.stats["remote_received"] == 1
    await n1.stop()
    await n2.stop()


async def test_publish_survives_broker_failure():
    class Broken(InMemoryBackend):
        async def publish(self, channel, message):
            raise ConnectionError("down")

    bus = DistributedEventBus(Broken(), "n1")
    got = []

    async def h(e):
        got.append(e)

    bus.subscribe(h)
    await bus.publish(BossmanEvent.create(EventType.AGENT_STARTED))
    assert len(got) == 1  # local delivery unaffected


# ── Durable registry ─────────────────────────────────────────────────────────


async def test_registry_restores_after_failover(backend):
    r1 = DurableAgentRegistry(DistributedEventBus(backend, "n1"), backend)
    info = AgentInfo(name="alpha", role="coder")
    await r1.register(info)
    await r1.mark_ready(info.agent_id)
    await r1.record_heartbeat(
        HeartbeatPayload(
            agent_id=info.agent_id,
            status=AgentStatus.IDLE,
            current_step="s1",
            tokens_used=42,
        )
    )

    # new leader node, empty memory
    r2 = DurableAgentRegistry(DistributedEventBus(backend, "n2"), backend)
    assert await r2.restore() == 1
    state = r2._agents[info.agent_id]
    assert state.status == AgentStatus.IDLE
    assert state.current_step == "s1"
    assert state.resource_usage.tokens_used == 42

    await r1.deregister(info.agent_id)
    r3 = DurableAgentRegistry(DistributedEventBus(backend, "n3"), backend)
    assert await r3.restore() == 0


# ── Recovery ledger ──────────────────────────────────────────────────────────


def _attempt(incident, n=1, agent="a1"):
    return RecoveryAttempt(
        incident_id=incident,
        agent_id=agent,
        failure_type=FailureType.HEARTBEAT_TIMEOUT,
        failure_detail="no heartbeat",
        strategy=RecoveryStrategy.RESTART_AGENT,
        attempt_number=n,
        telemetry_snapshot={"hb_age": 31.5},
    )


@pytest.fixture(params=["memory", "sql"])
async def store(request, tmp_path):
    if request.param == "memory":
        yield InMemoryLedgerStore()
    else:
        s = SqlLedgerStore(f"sqlite+aiosqlite:///{tmp_path / 'l.db'}")
        await s.init_schema()
        yield s
        await s.close()


async def test_ledger_roundtrip_and_update(store):
    inc = new_incident_id()
    a1, a2 = _attempt(inc, 1), _attempt(inc, 2)
    await store.append(a2)
    await store.append(a1)

    assert [r.attempt_number for r in await store.by_incident(inc)] == [1, 2]

    a1.mark_complete(RecoveryOutcome.SUCCESS, "ok", verified=True)
    await store.update_outcome(a1)
    got = await store.get(a1.attempt_id)
    assert got.outcome == RecoveryOutcome.SUCCESS
    assert got.verified_by_evaluator
    assert got.telemetry_snapshot == {"hb_age": 31.5}
    assert len(await store.by_agent("a1")) == 2
    assert await store.get("nope") is None


async def test_ledger_survives_restart(tmp_path):
    url = f"sqlite+aiosqlite:///{tmp_path / 'durable.db'}"
    s1 = SqlLedgerStore(url)
    await s1.init_schema()
    att = _attempt("inc-1")
    await s1.append(att)
    await s1.close()

    s2 = SqlLedgerStore(url)
    assert (await s2.get(att.attempt_id)).incident_id == "inc-1"
    await s2.close()
