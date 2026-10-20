"""
Phase 11 — durable work on SQL (state survives a control-plane restart).
Phase 15 — probes, quality scores, leading-indicator signals, OpenTelemetry
spans, Prometheus metrics, degradation visibility.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from bossman_server.contracts.agent_state import AgentInfo
from bossman_server.contracts.events import EventType
from bossman_server.control.quality import ProbeSpec
from bossman_server.infra.settings import InfraSettings
from tests.helpers import ADMIN, AGENT, OPERATOR, http, server, set_env, until


@pytest.fixture(autouse=True)
def env(monkeypatch):
    set_env(monkeypatch)


def sql_settings(path: Path) -> InfraSettings:
    return InfraSettings(persistence="sql", database_url=f"sqlite+aiosqlite:///{path.as_posix()}",
                         heartbeat_interval_s=0.2, heartbeat_timeout_s=30)


async def test_state_survives_restart_on_sql(tmp_path):
    db = tmp_path / "bossman.db"
    async with server(sql_settings(db)) as (app, rt):
        assert (await rt.cluster_info())["persistence"] == "sql"
        async with http(app, ADMIN) as admin:
            key = (await admin.post("/api/keys", json={"name": "ci", "role": "agent"})).json()["key"]
            await admin.put("/api/flags/tool.web", json={"enabled": False})
            await admin.put("/api/supervisors/pipe", json={"strategy": "one_for_all"})
        async with http(app, key) as c:
            t = (await c.post("/v1/tasks", json={"title": "durable", "idempotency_key": "job-1"})).json()
            dup = (await c.post("/v1/tasks", json={"title": "durable", "idempotency_key": "job-1"})).json()
            assert dup["task_id"] == t["task_id"]
        await asyncio.sleep(1.2)  # let the event persister flush
    async with server(sql_settings(db)) as (app, rt):
        task = await rt.tasks.get(t["task_id"])
        assert task is not None and task.status.value == "PENDING"
        assert not rt.flags.evaluate("tool.web")
        assert rt.supervisors.spec("pipe").strategy == "one_for_all"
        async with http(app, key) as c:  # API key survived the restart
            assert (await c.get(f"/v1/tasks/{t['task_id']}")).status_code == 200
        events = await rt.event_store.query(task_ids=[t["task_id"]])
        assert any(e.event_type == EventType.TASK_CREATED for e in events)


async def test_concurrent_claims_never_double_assign_on_sql(tmp_path):
    async with server(sql_settings(tmp_path / "c.db")) as (app, rt):
        ids = [(await rt.tasks.create(f"t{i}")).task_id for i in range(10)]
        agents = []
        for i in range(5):
            st = await rt.registry.register(AgentInfo(name=f"w{i}", role="worker"))
            await rt.registry.mark_ready(st.agent_id)
            agents.append(st.agent_id)

        async def drain(aid):
            got = []
            while True:
                t = await rt.tasks.claim(aid, roles={"worker"})
                if t is None:
                    return got
                got.append(t.task_id)

        results = await asyncio.gather(*(drain(a) for a in agents))
        claimed = [tid for r in results for tid in r]
        assert sorted(claimed) == sorted(ids)  # every task exactly once


async def test_probes_and_quality_drive_health_and_incidents():
    async with server() as (app, rt):
        async with http(app, AGENT) as c:
            a = (await c.post("/v1/agents", json={"name": "solver", "role": "solver"})).json()
            aid = a["agent_id"]
            r = await c.put(f"/v1/agents/{aid}/probes", json={"probes": [
                {"name": "math", "input": "2+2?", "expect": {"contains": "4"}, "interval_s": 10}]})
            assert r.status_code == 200
            assert await rt.quality.schedule_due() == 1
            hb = (await c.post(f"/v1/agents/{aid}/heartbeat", json={"epoch": a["epoch"]})).json()
            probe = [d for d in hb["directives"] if d["type"] == "RUN_PROBE"][0]
            assert probe["params"] == {"probe": "math", "input": "2+2?"}
            res = (await c.post(f"/v1/agents/{aid}/probes/math/result", json={"output": "five"})).json()
            assert res["passed"] is False
            state = await rt.registry.get(aid)
            health = rt.checker.check(state).as_dict()
            assert health["levels"]["L3_FUNCTIONAL"]["status"] == "FAIL"
            assert [e for _, e in rt.store.query(event_type="PROBE_FAILED")]

            for s in (0.4, 0.3, 0.2):
                await c.post(f"/v1/agents/{aid}/quality", json={"score": s, "evaluator": "human"})
            assert state.quality_score == pytest.approx(0.3)
            assert await until(lambda: any(f.failure_type.value == "SILENT_DEGRADATION"
                                           for f in rt.detector.detected_failures))
            assert rt.checker.check(state).as_dict()["levels"]["L4_QUALITY"]["status"] == "FAIL"


async def test_signal_monitor_detects_token_burn_and_dlq_growth():
    async with server() as (app, rt):
        st = await rt.registry.register(AgentInfo(name="looper", role="r", transport="remote"))
        await rt.registry.mark_ready(st.agent_id)
        st.tokens_per_minute = 2_000
        await rt.signals.tick()  # learn baseline
        st.tokens_per_minute = 50_000
        await rt.signals.tick()
        anomalies = [a["signal"] for a in rt.signals.anomalies]
        assert "token_burn" in anomalies
        assert await until(lambda: any(f.failure_type.value == "TOKEN_BURN_ANOMALY"
                                       for f in rt.detector.detected_failures))
        att = [a for a in rt.engine.ledger.entries if a.failure_type.value == "TOKEN_BURN_ANOMALY"][0]
        assert att.strategy.value == "ROLLBACK_CHECKPOINT"

        for i in range(6):
            t = await rt.tasks.create(f"doomed{i}", max_attempts=1)
            await rt.tasks.send_to_dlq(t.task_id, "test")
        await rt.signals.tick()
        assert "dlq_growth" in [a["signal"] for a in rt.signals.anomalies]


async def test_otel_spans_follow_task_trace_ids():
    from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

    from bossman_server.control.tracing import OtelExporter, format_traceparent, parse_traceparent

    async with server() as (app, rt):
        mem = InMemorySpanExporter()
        exporter = OtelExporter(rt.bus, span_exporter=mem)
        trace_id = "4bf92f3577b34da6a3ce929d0e0e4736"
        async with http(app, AGENT) as c:
            a = (await c.post("/v1/agents", json={"name": "w", "role": "w"})).json()
            t = (await c.post("/v1/tasks", json={
                "title": "traced", "traceparent": format_traceparent(trace_id)})).json()
            assert t["trace_id"] == trace_id and parse_traceparent(t["traceparent"])[0] == trace_id
            cl = (await c.post(f"/v1/agents/{a['agent_id']}/tasks/claim", json={})).json()
            await c.post(f"/v1/tasks/{t['task_id']}/complete", json={"lease_id": cl["lease_id"], "output": {}})
        spans = mem.get_finished_spans()
        assert spans and format(spans[0].context.trace_id, "032x") == trace_id
        exporter.shutdown()


async def test_metrics_and_system_views():
    async with server() as (app, rt):
        async with http(app, AGENT) as c:
            await c.post("/v1/agents", json={"name": "m", "role": "w"})
        async with http(app, None) as anon:
            text = (await anon.get("/metrics")).text
            assert 'bossman_agents{namespace="default",status="IDLE"} 1.0' in text
            assert "bossman_is_leader 1.0" in text
            assert (await anon.get("/healthz")).json() == {"status": "ok"}
            ready = await anon.get("/readyz")
            assert ready.status_code == 200 and ready.json()["ready"] is True
            sysv = (await anon.get("/api/system")).json()
            assert sysv["database"]["configured"] is False and "governance" in sysv
            res = (await anon.get("/api/resources")).json()
            assert res["limits"][0]["name"] == "llm" and "leases" in res


async def test_degradation_directives_and_visibility():
    async with server() as (app, rt):
        async with http(app, AGENT) as c, http(app, OPERATOR) as op:
            a = (await c.post("/v1/agents", json={"name": "d", "role": "w"})).json()
            await op.post(f"/api/agents/{a['agent_id']}/directives",
                          json={"type": "SET_DEGRADATION", "params": {"level": 2}})
            ack = (await c.post(f"/v1/agents/{a['agent_id']}/heartbeat", json={
                "epoch": a["epoch"], "degradation_level": 2, "model": "claude-haiku-5-5"})).json()
            assert ack["directives"][0]["type"] == "SET_DEGRADATION"
            state = await rt.registry.get(a["agent_id"])
            assert state.degradation_level == 2 and state.current_model == "claude-haiku-5-5"
            assert rt.checker.check(state).as_dict()["levels"]["L4_QUALITY"]["status"] == "WARN"


async def test_bossmand_migrate_and_keys(tmp_path, monkeypatch, capsys):
    from bossman_server.cli import main

    url = f"sqlite+aiosqlite:///{(tmp_path / 'cli.db').as_posix()}"
    monkeypatch.setenv("DATABASE_URL", url)
    monkeypatch.setenv("BOSSMAN_PERSISTENCE", "sql")
    # the CLI is a synchronous program: run it the way a shell would (no loop running)
    assert await asyncio.to_thread(main, ["migrate"]) == 0
    assert "revision 0001" in capsys.readouterr().out
    assert await asyncio.to_thread(main, ["keys", "create", "--role", "agent", "--namespace", "team-x",
                                          "--name", "ci"]) == 0
    out = capsys.readouterr().out
    assert '"key": "bm_' in out
    assert await asyncio.to_thread(main, ["keys", "list"]) == 0
    assert "team-x" in capsys.readouterr().out
