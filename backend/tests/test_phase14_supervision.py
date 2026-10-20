"""
Phase 14 — supervision trees and managed processes (real subprocesses).
"""

from __future__ import annotations

import sys

import pytest

from bossman_server.contracts.agent_state import AgentInfo, AgentStatus
from bossman_server.control.managed import ManagedSpec, ProcState
from bossman_server.control.supervisors import SupervisorSpec
from bossman_server.detection.failure_detector import DetectedFailure
from bossman_server.contracts.recovery_ledger import FailureType, RecoveryStrategy
from tests.helpers import ADMIN, OPERATOR, http, server, set_env, until


@pytest.fixture(autouse=True)
def env(monkeypatch):
    set_env(monkeypatch)


async def _member(rt, name, sup, order):
    st = await rt.registry.register(AgentInfo(name=name, role="stage", supervisor=sup, start_order=order))
    await rt.registry.mark_ready(st.agent_id)
    return st.agent_id


async def test_supervisor_strategies():
    async with server() as (_, rt):
        await rt.supervisors.put(SupervisorSpec(name="pipe", strategy="rest_for_one", max_restarts=5))
        a = await _member(rt, "extract", "pipe", 0)
        b = await _member(rt, "transform", "pipe", 1)
        c = await _member(rt, "load", "pipe", 2)
        plan = await rt.supervisors.on_child_failure(b, "boom")
        assert plan.restart == [b, c] and not plan.escalate

        await rt.supervisors.put(SupervisorSpec(name="pipe", strategy="one_for_all", max_restarts=5))
        plan = await rt.supervisors.on_child_failure(c, "boom")
        assert set(plan.restart) == {a, b, c}

        await rt.supervisors.put(SupervisorSpec(name="pipe", strategy="one_for_one", max_restarts=5))
        assert (await rt.supervisors.on_child_failure(a, "boom")).restart == [a]


async def test_budget_exhaustion_escalates_to_parent_then_human():
    async with server() as (_, rt):
        await rt.supervisors.put(SupervisorSpec(name="root", strategy="one_for_one", max_restarts=1))
        await rt.supervisors.put(SupervisorSpec(name="team", strategy="one_for_one", max_restarts=1, parent="root"))
        x = await _member(rt, "x", "team", 0)
        y = await _member(rt, "y", "team", 1)
        p1 = await rt.supervisors.on_child_failure(x, "1")
        assert p1.restart == [x] and p1.path == ["team"]
        p2 = await rt.supervisors.on_child_failure(x, "2")  # team budget gone → root restarts the group
        assert p2.path == ["team", "root"] and set(p2.restart) == {x, y}
        p3 = await rt.supervisors.on_child_failure(y, "3")
        assert p3.escalate

        with pytest.raises(ValueError, match="cycle"):
            await rt.supervisors.put(SupervisorSpec(name="root", parent="team"))


async def test_recovery_engine_uses_supervisor_plan():
    async with server() as (_, rt):
        await rt.supervisors.put(SupervisorSpec(name="pair", strategy="one_for_all", max_restarts=3))
        a = await _member(rt, "a", "pair", 0)
        b = await _member(rt, "b", "pair", 1)
        for aid in (a, b):
            await rt.registry.update_status(aid, AgentStatus.RUNNING)
        await rt.registry.update_status(a, AgentStatus.STUCK)
        attempt = await rt.engine.handle_failure(DetectedFailure(
            failure_type=FailureType.HEARTBEAT_TIMEOUT, agent_id=a, detail="silent",
            recommended_strategy=RecoveryStrategy.RESTART_AGENT))
        assert attempt.strategy == RecoveryStrategy.RESTART_AGENT
        restarted = [e.agent_id for _, e in rt.store.query(event_type="AGENT_RESTARTED")]
        assert set(restarted) == {a, b}


SCRIPT_CRASH = "import sys; print('starting', flush=True); sys.exit(3)"
SCRIPT_SLEEP = "import time\nprint('up', flush=True)\nwhile True: time.sleep(0.1)"


async def test_managed_process_restart_crashloop_and_graceful_stop():
    async with server() as (app, rt):
        # crash-looping process: restarted with backoff until the budget is spent
        crash = await rt.managed.put(ManagedSpec(name="crasher", command=[sys.executable, "-c", SCRIPT_CRASH],
                                                 max_restarts=2, window_s=60))
        rt.managed._procs.setdefault(crash.managed_id, ProcState())

        async def fast_forward():
            st = rt.managed._procs[crash.managed_id]
            st.next_start_at = 0
            await rt.managed.reconcile()
            return st.state == "crashloop"

        assert await until(fast_forward, timeout=20, interval=0.1)
        assert any("starting" in line for line in rt.managed.logs(crash.managed_id))
        assert [e for _, e in rt.store.query(event_type="MANAGED_CRASHLOOP")]

        # long-running process: start, restart via API, stop gracefully
        svc = await rt.managed.put(ManagedSpec(name="sleeper", command=[sys.executable, "-c", SCRIPT_SLEEP],
                                               graceful_timeout_s=2))
        await rt.managed.reconcile()
        assert await until(lambda: rt.managed._procs[svc.managed_id].state == "running", timeout=10)
        pid1 = rt.managed._procs[svc.managed_id].pid
        async with http(app, OPERATOR) as op:
            assert (await op.post(f"/api/managed/{svc.managed_id}/restart", json={})).status_code == 200
            assert await until(lambda: rt.managed._procs[svc.managed_id].pid not in (None, pid1), timeout=15)
            await op.post(f"/api/managed/{svc.managed_id}/stop", json={})
        assert rt.managed._procs[svc.managed_id].state == "stopped"
        listing = rt.managed.status()
        assert {m["name"] for m in listing} == {"crasher", "sleeper"}
        async with http(app, ADMIN) as admin:
            assert (await admin.delete(f"/api/managed/{svc.managed_id}")).status_code == 200


async def test_managed_env_never_leaks_control_plane_secrets(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "postgresql://u:hunter2@db/x")
    async with server() as (_, rt):
        script = ("import os\nfor k in ('BOSSMAN_ADMIN_TOKEN','BOSSMAN_OPERATOR_TOKEN','DATABASE_URL',"
                  "'BOSSMAN_API_KEY','BOSSMAN_MANAGED_ID'):\n    print(k, '=', os.getenv(k))")
        spec = await rt.managed.put(ManagedSpec(name="envcheck", command=[sys.executable, "-c", script],
                                                restart_policy="never"))
        await rt.managed.reconcile()
        assert await until(lambda: rt.managed._procs[spec.managed_id].state == "stopped", timeout=10)
        out = "\n".join(rt.managed.logs(spec.managed_id))
        assert "BOSSMAN_ADMIN_TOKEN = None" in out and "DATABASE_URL = None" in out
        assert "hunter2" not in out
        assert f"BOSSMAN_MANAGED_ID = {spec.managed_id}" in out
        assert "BOSSMAN_API_KEY = test-agent-token" in out
