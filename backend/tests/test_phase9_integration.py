"""
Phase 9 — integration tests against REAL Redis and PostgreSQL.

Start the services first:   docker compose up -d --wait
Then:                       pytest -m integration

Skipped automatically when the services are unreachable. Override targets
with BOSSMAN_TEST_REDIS_URL / BOSSMAN_TEST_DATABASE_URL. The tests use
Redis DB 15 and flush it — never point them at data you care about.
"""

from __future__ import annotations

import asyncio
import os

import pytest

from api.runtime import BossmanRuntime
from contracts.agent_state import AgentInfo, AgentStatus
from contracts.events import BossmanEvent, EventType
from contracts.recovery_ledger import (
    FailureType,
    RecoveryAttempt,
    RecoveryOutcome,
    RecoveryStrategy,
)
from infra import DistributedEventBus, LeaderElector, RedisBackend, SqlLedgerStore
from infra.settings import InfraSettings

pytestmark = pytest.mark.integration

REDIS_URL = os.getenv("BOSSMAN_TEST_REDIS_URL", "redis://127.0.0.1:6390/15")
PG_URL = os.getenv(
    "BOSSMAN_TEST_DATABASE_URL", "postgresql+asyncpg://bossman:bossman@127.0.0.1:5440/bossman"
)


@pytest.fixture
async def redis_backend():
    be = RedisBackend(REDIS_URL)
    try:
        await be._r.ping()
    except Exception as exc:  # noqa: BLE001
        await be.close()
        pytest.skip(f"Redis unavailable at {REDIS_URL}: {exc}")
    await be._r.flushdb()
    yield be
    await be._r.flushdb()
    await be.close()


@pytest.fixture
async def pg_store():
    s = SqlLedgerStore(PG_URL)
    try:
        await s.init_schema()
    except Exception as exc:  # noqa: BLE001
        await s.close()
        pytest.skip(f"PostgreSQL unavailable: {exc}")
    from sqlalchemy import text

    async with s.engine.begin() as conn:
        await conn.execute(text("TRUNCATE recovery_ledger"))
    yield s
    await s.close()


def _attempt(n=1):
    return RecoveryAttempt(
        incident_id="inc-pg",
        agent_id="agent-pg",
        failure_type=FailureType.HEARTBEAT_TIMEOUT,
        failure_detail="no heartbeat",
        strategy=RecoveryStrategy.RESTART_AGENT,
        attempt_number=n,
        telemetry_snapshot={"nested": {"k": [1, 2, 3]}},
    )


async def wait_for(cond, timeout=5.0):
    end = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < end:
        if cond():
            return True
        await asyncio.sleep(0.05)
    return False


# ── Redis ────────────────────────────────────────────────────────────────────


async def test_redis_leader_election_failover(redis_backend):
    a = LeaderElector(redis_backend, "a", lease_ms=300)
    b = LeaderElector(redis_backend, "b", lease_ms=300)
    assert await a.tick() and not await b.tick()
    await asyncio.sleep(0.45)  # a stops renewing (crash)
    assert await b.tick()
    assert not await a.tick()
    assert await a.current_leader() == "b"


async def test_redis_events_cross_nodes(redis_backend):
    n1, n2 = DistributedEventBus(redis_backend, "n1"), DistributedEventBus(redis_backend, "n2")
    got = []

    async def h(e):
        got.append(e)

    n2.subscribe(h)
    await n1.start()
    await n2.start()
    await asyncio.sleep(0.2)
    ev = BossmanEvent.create(EventType.AGENT_STARTED, message="x", agent_id="a")
    await n1.publish(ev)
    assert await wait_for(lambda: len(got) == 1)
    assert got[0].event_id == ev.event_id
    await n1.stop()
    await n2.stop()


# ── PostgreSQL ───────────────────────────────────────────────────────────────


async def test_postgres_ledger_roundtrip_and_update(pg_store):
    a1, a2 = _attempt(1), _attempt(2)
    await pg_store.append(a2)
    await pg_store.append(a1)
    assert [r.attempt_number for r in await pg_store.by_incident("inc-pg")] == [1, 2]
    a1.mark_complete(RecoveryOutcome.SUCCESS, "ok", verified=True)
    await pg_store.update_outcome(a1)
    got = await pg_store.get(a1.attempt_id)
    assert got.outcome == RecoveryOutcome.SUCCESS and got.verified_by_evaluator
    assert got.telemetry_snapshot == {"nested": {"k": [1, 2, 3]}}
    assert len(await pg_store.recent()) == 2


async def test_postgres_ledger_survives_reconnect(pg_store):
    att = _attempt()
    await pg_store.append(att)
    fresh = SqlLedgerStore(PG_URL)
    assert (await fresh.get(att.attempt_id)).incident_id == "inc-pg"
    await fresh.close()


# ── Full runtimes on real infra ──────────────────────────────────────────────


async def test_two_runtimes_real_failover(redis_backend, pg_store):
    def cfg(node):
        return InfraSettings(
            mode="distributed", node_id=node, redis_url=REDIS_URL,
            database_url=PG_URL, lease_ms=500, ledger_sync_s=0.1,
        )

    a = BossmanRuntime(cfg("a"), backend=RedisBackend(REDIS_URL), ledger_store=pg_store)
    b = BossmanRuntime(cfg("b"), backend=RedisBackend(REDIS_URL), ledger_store=pg_store)
    await a.start()
    await b.start()
    try:
        assert a.is_leader and not b.is_leader

        info = AgentInfo(name="alpha", role="coder")
        await a.registry.register(info)
        await a.registry.mark_ready(info.agent_id)
        att = _attempt()
        a.engine.ledger.append(att)
        att.mark_complete(RecoveryOutcome.SUCCESS, "ok", verified=True)

        assert await wait_for(lambda: b.store.last_seq > 0)  # events crossed Redis

        await a.stop()  # graceful: releases lease
        assert await wait_for(lambda: b.is_leader and b._duties_active)
        restored = await b.registry.get(info.agent_id)
        assert restored is not None and restored.status == AgentStatus.IDLE
        rows = [e for e in b.engine.ledger.entries if e.attempt_id == att.attempt_id]
        assert rows and rows[0].outcome == RecoveryOutcome.SUCCESS
        assert (await pg_store.get(att.attempt_id)).verified_by_evaluator
    finally:
        await b.stop()
