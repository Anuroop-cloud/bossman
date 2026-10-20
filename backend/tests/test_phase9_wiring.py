"""Phase 9 — application wiring (no external services required)."""

from __future__ import annotations

import asyncio

import pytest
from fastapi.testclient import TestClient

from bossman_server.api.app import create_app
from bossman_server.api.runtime import BossmanRuntime
from bossman_server.contracts.agent_state import AgentInfo, AgentStatus
from bossman_server.contracts.recovery_ledger import (
    FailureType,
    RecoveryAttempt,
    RecoveryOutcome,
    RecoveryStrategy,
)
from bossman_server.infra import InMemoryBackend, InMemoryLedgerStore
from bossman_server.infra.persistent_ledger import PersistentLedger
from bossman_server.infra.settings import InfraSettings


def dist(node: str) -> InfraSettings:
    return InfraSettings(mode="distributed", node_id=node, lease_ms=300, ledger_sync_s=0.05)


async def wait_for(cond, timeout=3.0):
    end = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < end:
        if cond():
            return True
        await asyncio.sleep(0.02)
    return False


def test_settings_default_standalone_and_validation(monkeypatch):
    monkeypatch.delenv("BOSSMAN_MODE", raising=False)
    assert not InfraSettings.from_env().distributed
    monkeypatch.setenv("BOSSMAN_MODE", "distributed")
    assert InfraSettings.from_env().distributed
    monkeypatch.setenv("BOSSMAN_MODE", "nonsense")
    with pytest.raises(ValueError):
        InfraSettings.from_env()


def test_cluster_endpoint_standalone(monkeypatch):
    monkeypatch.delenv("BOSSMAN_MODE", raising=False)
    with TestClient(create_app(BossmanRuntime())) as c:
        body = c.get("/api/cluster").json()
    assert body["mode"] == "standalone"
    assert body["is_leader"] is True
    assert body["durable_ledger"] is False


async def test_standalone_runs_duties_immediately():
    rt = BossmanRuntime(InfraSettings())
    await rt.start()
    assert rt.is_leader and rt._duties_active and rt.elector is None
    await rt.stop()


async def test_only_leader_runs_duties_and_failover_restores_state():
    be, store = InMemoryBackend(), InMemoryLedgerStore()
    a = BossmanRuntime(dist("a"), backend=be, ledger_store=store)
    b = BossmanRuntime(dist("b"), backend=be, ledger_store=store)
    await a.start()
    await b.start()
    try:
        assert a.is_leader and not b.is_leader
        assert a._duties_active and not b._duties_active

        # State created on the leader
        info = AgentInfo(name="alpha", role="coder")
        await a.registry.register(info)
        await a.registry.mark_ready(info.agent_id)
        attempt = RecoveryAttempt(
            incident_id="inc-1",
            agent_id=info.agent_id,
            failure_type=FailureType.HEARTBEAT_TIMEOUT,
            failure_detail="x",
            strategy=RecoveryStrategy.RESTART_AGENT,
        )
        a.engine.ledger.append(attempt)
        attempt.mark_complete(RecoveryOutcome.SUCCESS, "ok", verified=True)  # later mutation

        # Cross-node visibility on the event stream (standby sees leader's events)
        assert await wait_for(lambda: b.store.last_seq > 0)

        # Leader goes away → standby takes over with registry + ledger restored
        await a.stop()
        assert await wait_for(lambda: b.is_leader and b._duties_active)
        restored = await b.registry.get(info.agent_id)
        assert restored is not None and restored.status == AgentStatus.IDLE
        got = [e for e in b.engine.ledger.entries if e.attempt_id == attempt.attempt_id]
        assert got and got[0].outcome == RecoveryOutcome.SUCCESS  # mutation was flushed
        assert (await b.cluster_info())["leader_id"] == "b"
    finally:
        await b.stop()


async def test_persistent_ledger_retries_after_store_failure():
    class Flaky(InMemoryLedgerStore):
        fail = True

        async def upsert(self, attempt):
            if Flaky.fail:
                raise ConnectionError("db down")
            await super().upsert(attempt)

    store = Flaky()
    ledger = PersistentLedger(store)
    att = RecoveryAttempt(
        incident_id="i",
        agent_id="a",
        failure_type=FailureType.UNKNOWN,
        failure_detail="d",
        strategy=RecoveryStrategy.WAIT_AND_RETRY,
    )
    ledger.append(att)  # must not raise
    await asyncio.sleep(0.05)
    assert await store.get(att.attempt_id) is None
    Flaky.fail = False
    assert await ledger.sync() == 1
    assert await store.get(att.attempt_id) is not None
