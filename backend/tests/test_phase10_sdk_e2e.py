"""
Phase 10 — the Python SDK driving a real in-process server end to end:
serve loop, checkpoints, child tasks/traces, PAUSE/RESUME/ABORT directives,
remote-agent recovery, idempotent side effects, compensation.
"""

from __future__ import annotations

import asyncio

import pytest

pytest.importorskip("bossman")

from bossman import errors  # noqa: E402

from tests.helpers import OPERATOR, http, sdk, server, set_env, until  # noqa: E402


@pytest.fixture(autouse=True)
def env(monkeypatch):
    set_env(monkeypatch)


async def test_serve_completes_tasks_with_checkpoints_and_child_trace():
    async with server() as (app, rt):
        async with sdk(app) as bm:
            async with bm.agent(name="planner", role="planner", heartbeat_interval=0.1) as planner:
                async def handle(task):
                    await task.progress("planning")
                    await task.checkpoint("planned", {"steps": 2})
                    child = await task.spawn("research step", required_role="researcher", input={"n": 1})
                    return {"child": child["task_id"]}

                root = await bm.submit("plan the report", required_role="planner")
                runner = asyncio.create_task(planner.serve(handle, wait_s=0.2))
                done = await bm.wait(root["task_id"], timeout=10)
                planner.stopping.set()
                await runner
        assert done["status"] == "COMPLETED"
        child = await rt.tasks.get(done["output_data"]["child"])
        assert child.parent_task_id == root["task_id"] and child.trace_id == root["trace_id"]
        cps = await rt.checkpoints.list(root["task_id"])
        assert [c["phase"] for c in cps] == ["planned"]
        async with http(app, OPERATOR) as op:
            trace = (await op.get(f"/api/traces/{root['trace_id']}")).json()
        assert {t["task_id"] for t in trace["tasks"]} == {root["task_id"], child.task_id}
        assert any(e["event_type"] == "TASK_CHECKPOINTED" for e in trace["events"])


async def test_failure_is_retried_then_dead_lettered():
    async with server() as (app, rt):
        async with sdk(app) as bm, bm.agent(name="w", role="worker", heartbeat_interval=0.1) as agent:
            calls = 0

            async def handle(task):
                nonlocal calls
                calls += 1
                raise TimeoutError("upstream timed out")  # transient → retried

            t = await bm.submit("flaky", max_attempts=2)
            for task in await rt.tasks.all():
                task.not_before = None
            runner = asyncio.create_task(agent.serve(handle, wait_s=0.2))

            async def drain_backoff():
                for task in await rt.tasks.all():
                    task.not_before = None
                cur = await rt.tasks.get(t["task_id"])
                return cur.status.value == "DEAD_LETTER"

            assert await until(drain_backoff, timeout=10)
            agent.stopping.set()
            await runner
        assert calls == 2
        task = await rt.tasks.get(t["task_id"])
        assert task.error_class == "timeout" and "Exhausted 2 attempts" in task.dlq_reason


async def test_pause_resume_and_abort_directives():
    async with server() as (app, rt):
        async with sdk(app) as bm, bm.agent(name="w", role="worker", heartbeat_interval=0.1) as agent:
            started = asyncio.Event()

            async def handle(task):
                started.set()
                await asyncio.sleep(30)  # long work — will be aborted

            t = await bm.submit("long job")
            runner = asyncio.create_task(agent.serve(handle, wait_s=0.2))
            await asyncio.wait_for(started.wait(), 5)
            async with http(app, OPERATOR) as op:
                await op.post(f"/api/agents/{agent.agent_id}/directives", json={"type": "PAUSE"})
                assert await until(lambda: not agent._running.is_set())
                await op.post(f"/api/agents/{agent.agent_id}/directives", json={"type": "RESUME"})
                assert await until(lambda: agent._running.is_set())
                await op.post(f"/api/agents/{agent.agent_id}/directives", json={"type": "ABORT_TASK"})

            async def aborted():
                cur = await rt.tasks.get(t["task_id"])
                return cur.status.value in ("PENDING", "RUNNING") and cur.error_class == "aborted"

            assert await until(aborted, timeout=5)
            agent.stopping.set()
            await runner
            assert await until(lambda: rt.directives.recent(agent.agent_id), timeout=2)
            acked = {d.type.value for d in rt.directives.recent(agent.agent_id)}
            assert {"PAUSE", "RESUME", "ABORT_TASK"} <= acked


async def test_heartbeat_timeout_recovers_remote_agent_with_restart_directive():
    async with server() as (app, rt):
        async with sdk(app) as bm, bm.agent(name="w", role="worker", heartbeat_interval=0.1) as agent:
            restarted = asyncio.Event()
            agent.on("RESTART")(lambda d: restarted.set())
            t = await bm.submit("job")
            claimed = await agent.claim(wait_s=1)
            assert claimed.task_id == t["task_id"]
            # Simulate the agent hanging: watchdog sees a stale heartbeat
            from datetime import datetime, timedelta, timezone

            state = await rt.registry.get(agent.agent_id)
            state.last_heartbeat = datetime.now(timezone.utc) - timedelta(seconds=120)
            await rt.watchdog._check_workforce()
            await asyncio.wait_for(restarted.wait(), 5)
            task = await rt.tasks.get(t["task_id"])
            assert task.status.value == "PENDING" and task.error_class == "agent_restarted"
            assert (await rt.registry.get(agent.agent_id)).status.value == "IDLE"
            inc = [i for i in rt.engine.ledger.entries if i.agent_id == agent.agent_id]
            assert inc and inc[0].strategy.value == "RESTART_AGENT"


async def test_once_is_exactly_once_across_agents():
    async with server() as (app, rt):
        async with sdk(app) as bm:
            async with bm.agent(name="a", role="w") as a1, bm.agent(name="b", role="w") as a2:
                effects = []

                async def charge():
                    effects.append(1)
                    return {"charge_id": "ch_1"}

                r1 = await a1.once("charge-order-42", charge)
                r2 = await a2.once("charge-order-42", charge)
        assert r1 == r2 == {"charge_id": "ch_1"} and len(effects) == 1


async def test_call_tool_compensation_retries_with_longer_timeout_then_substitutes():
    async with server() as (app, rt):
        async with sdk(app) as bm, bm.agent(name="t", role="w") as agent:
            seen = []

            async def flaky(params):
                seen.append(params.get("timeout"))
                raise asyncio.TimeoutError()

            async def backup(params):
                return "from-backup"

            # speed: no real backoff sleeps
            import bossman_server.control.compensation as comp

            orig = comp._backoff
            comp._backoff = lambda *a, **k: 0.0
            try:
                out = await agent.call_tool("search", flaky, {"timeout": 10}, alternatives={"search-b": backup})
            finally:
                comp._backoff = orig
        assert out == "from-backup"
        assert seen[:2] == [10, 20]  # second attempt doubled the timeout


async def test_circuit_guard_fails_fast_with_fallback():
    async with server() as (app, rt):
        async with sdk(app) as bm, bm.agent(name="g", role="w") as agent:
            async def boom():
                raise ConnectionError("down")

            for _ in range(5):
                with pytest.raises(ConnectionError):
                    await agent.guarded("vendor-api", boom)
            assert await agent.guarded("vendor-api", boom, fallback="cached") == "cached"
            with pytest.raises(errors.CircuitOpen):
                await agent.guarded("vendor-api", boom)


async def test_sdk_reregisters_after_control_plane_forgets_it():
    async with server() as (app, rt):
        async with sdk(app) as bm, bm.agent(name="survivor", role="w", heartbeat_interval=0.05) as agent:
            old = agent.agent_id
            await rt.registry.deregister(old, "simulated control-plane state loss")
            assert await until(lambda: agent.agent_id != old, timeout=5)
            assert await rt.registry.get(agent.agent_id) is not None
