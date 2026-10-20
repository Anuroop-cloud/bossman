"""Phase 8A — API layer integration tests (REST, auth, incidents, WebSocket)."""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

from bossman_server.api.app import create_app
from bossman_server.api.runtime import BossmanRuntime
from bossman_server.contracts.agent_state import AgentInfo, PermissionTier
from bossman_server.core.task_manager import TaskStatus


@pytest.fixture
def client(monkeypatch):
    monkeypatch.delenv("BOSSMAN_OPERATOR_TOKEN", raising=False)
    monkeypatch.delenv("BOSSMAN_ADMIN_TOKEN", raising=False)
    monkeypatch.delenv("BOSSMAN_DEMO_SEED", raising=False)
    with TestClient(create_app(BossmanRuntime())) as c:
        yield c


def run(client: TestClient, coro):
    return client.portal.call(lambda: coro)


@pytest.mark.parametrize(
    "path",
    ["/api/health", "/api/session", "/api/overview", "/api/agents", "/api/tasks",
     "/api/incidents", "/api/resources", "/api/system", "/api/events"],
)
def test_all_reads_ok_when_empty(client, path):
    assert client.get(path).status_code == 200


@pytest.mark.parametrize("path", ["/api/agents/nope", "/api/tasks/nope", "/api/incidents/nope"])
def test_missing_resources_404_with_envelope(client, path):
    r = client.get(path)
    assert r.status_code == 404
    assert r.json()["error"]["code"] == "not_found"
    assert "Traceback" not in r.text


def test_system_never_exposes_secrets(client, monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "sk-super-secret-value")
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-ant-super-secret-value")
    monkeypatch.setenv("DATABASE_URL", "postgresql://user:hunter2@db/x")
    body = client.get("/api/system").text
    assert "sk-super-secret-value" not in body
    assert "sk-ant-super-secret-value" not in body
    assert "hunter2" not in body
    assert client.get("/api/system").json()["llm"]["configured"] is True


async def _seed_dlq(rt: BossmanRuntime):
    t = await rt.tasks.create("doomed", max_attempts=1)
    await rt.tasks.assign(t.task_id, "agent-x")  # assign() counts the attempt
    await rt.tasks.fail(t.task_id, "boom")
    return t.task_id


def test_actions_disabled_without_tokens(client):
    tid = run(client, _seed_dlq(client.app.state.rt))
    r = client.post(f"/api/tasks/{tid}/replay", json={})
    assert r.status_code == 403


def test_dlq_actions_are_authorised_server_side(client, monkeypatch):
    monkeypatch.setenv("BOSSMAN_OPERATOR_TOKEN", "op-token")
    monkeypatch.setenv("BOSSMAN_ADMIN_TOKEN", "admin-token")
    rt = client.app.state.rt
    t1 = run(client, _seed_dlq(rt))
    t2 = run(client, _seed_dlq(rt))
    assert len(client.get("/api/tasks").json()["dlq"]) == 2

    # no / wrong token → 401
    assert client.post(f"/api/tasks/{t1}/replay", json={}).status_code == 401
    assert client.post(f"/api/tasks/{t1}/replay", json={},
                       headers={"X-BOSSman-Token": "wrong"}).status_code == 401
    # operator (WRITE) may replay but not discard
    op = {"X-BOSSman-Token": "op-token"}
    assert client.post(f"/api/tasks/{t2}/discard", json={}, headers=op).status_code == 403
    assert client.post(f"/api/tasks/{t1}/replay", json={"reason": "retry"}, headers=op).status_code == 200
    assert client.get("/api/session", headers=op).json()["tier"] == PermissionTier.WRITE.value
    # admin (PRIVILEGED) may discard
    ad = {"X-BOSSman-Token": "admin-token"}
    assert client.post(f"/api/tasks/{t2}/discard", json={}, headers=ad).status_code == 200
    # not in DLQ anymore → 404
    assert client.post(f"/api/tasks/{t2}/discard", json={}, headers=ad).status_code == 404
    assert client.get("/api/tasks").json()["dlq"] == []
    task = run(client, rt.tasks.get(t2))
    assert task.status == TaskStatus.CANCELLED
    # reason length validated
    assert client.post(f"/api/tasks/{t1}/replay", json={"reason": "x" * 500}, headers=op).status_code == 422


async def _make_incident(rt: BossmanRuntime):
    st = await rt.registry.register(AgentInfo(name="Atlas", role="researcher"))
    await rt.registry.mark_ready(st.agent_id)
    st.resource_usage.resources_held = ["a"]
    st.resource_usage.resources_waiting = ["b"]
    failures = await rt.detector.detect_now(
        resource_snapshot=rt.resources.snapshot(),
        registry_snapshot=await rt.registry.snapshot(),
    )
    return st.agent_id, failures[0].incident_id


def test_incident_lifecycle_and_agent_detail(client):
    rt = client.app.state.rt
    agent_id, iid = run(client, _make_incident(rt))

    listing = client.get("/api/incidents").json()["incidents"]
    assert [i["incident_id"] for i in listing] == [iid]

    inc = client.get(f"/api/incidents/{iid}").json()
    assert [s["stage"] for s in inc["stages"]] == ["DETECTED", "DIAGNOSED", "RECOVERY", "EVALUATION", "RESOLVED"]
    assert inc["failure_type"] == "DEADLOCK"
    assert inc["diagnosis"]["rule_matched"]
    assert inc["attempts"] and inc["attempts"][0]["strategy"]
    assert inc["timeline"] and inc["raw"]["events"]

    detail = client.get(f"/api/agents/{agent_id}").json()
    assert detail["health"]["levels"].keys() >= {"L1_LIVENESS", "L2_PROGRESS", "L3_RESOURCES", "L4_QUALITY"}
    assert any(i["incident_id"] == iid for i in detail["incidents"])
    assert client.get("/api/overview").json()["incidents"]["total"] == 1


def test_resources_graph_marks_suspected_deadlock(client):
    rt = client.app.state.rt

    async def seed():
        a = await rt.registry.register(AgentInfo(name="A", role="r"))
        b = await rt.registry.register(AgentInfo(name="B", role="r"))
        a.resource_usage.resources_held, a.resource_usage.resources_waiting = ["x"], ["y"]
        b.resource_usage.resources_held, b.resource_usage.resources_waiting = ["y"], ["x"]

    run(client, seed())
    g = client.get("/api/resources").json()["graph"]
    assert len(g["cycles"]) == 1
    assert {n["ref"] for n in g["nodes"] if n.get("suspected_deadlock")} == set(g["cycles"][0])
    assert {e["kind"] for e in g["edges"]} == {"holds", "waits"}


def test_websocket_streams_events_and_checks_origin(client):
    rt = client.app.state.rt
    with client.websocket_connect("/ws", headers={"origin": "http://localhost:5173"}) as ws:
        hello = ws.receive_json()
        assert hello["type"] == "hello"
        run(client, rt.registry.register(AgentInfo(name="Live", role="r")))
        msg = ws.receive_json()
        assert msg["type"] == "event"
        assert msg["event"]["event_type"] == "AGENT_REGISTERED"
    with pytest.raises(WebSocketDisconnect):
        with client.websocket_connect("/ws", headers={"origin": "http://evil.example"}) as ws:
            ws.receive_json()
