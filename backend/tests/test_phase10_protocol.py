"""
Phase 10 — the agent protocol over HTTP (/v1): registration guards, fencing,
directives, leased tasks, dependencies/quarantine, locks, limits, circuits.
"""

from __future__ import annotations

import pytest

from tests.helpers import ADMIN, AGENT, OPERATOR, http, server, set_env


@pytest.fixture(autouse=True)
def env(monkeypatch):
    set_env(monkeypatch)


async def register(c, **kw):
    body = {"name": kw.pop("name", "a1"), "role": kw.pop("role", "worker"), **kw}
    r = await c.post("/v1/agents", json=body)
    assert r.status_code == 201, r.text
    return r.json()


async def test_agent_api_requires_agent_credentials():
    async with server() as (app, _):
        async with http(app, None) as c:
            assert (await c.post("/v1/agents", json={"name": "x", "role": "r"})).status_code == 401
        async with http(app, "wrong") as c:
            assert (await c.post("/v1/agents", json={"name": "x", "role": "r"})).status_code == 401
        async with http(app, OPERATOR) as c:
            assert (await c.post("/v1/agents", json={"name": "x", "role": "r"})).status_code == 403


async def test_tier_ceiling_and_namespace_isolation():
    async with server() as (app, rt):
        async with http(app, ADMIN) as admin:
            key = (await admin.post("/api/keys", json={"name": "team-a", "role": "agent", "namespaces": ["team-a"],
                                                       "max_tier": "WRITE"})).json()["key"]
        async with http(app, key) as c:
            r = await c.post("/v1/agents", json={"name": "x", "role": "r", "permission_tier": "DESTRUCTIVE"})
            assert r.status_code == 403 and "ceiling" in r.json()["error"]["message"]
            r = await c.post("/v1/agents", json={"name": "x", "role": "r", "namespace": "team-b"})
            assert r.status_code == 403
            a = await register(c, name="ok")
            assert a["namespace"] == "team-a"
        async with http(app, AGENT) as other:
            other_agent = await register(other, name="other", namespace="default")
        async with http(app, key) as c:
            # team-a key cannot see or drive an agent in another namespace
            assert (await c.get(f"/v1/agents/{other_agent['agent_id']}")).status_code == 404


async def test_singleton_guard_takeover_and_fencing():
    async with server() as (app, rt):
        async with http(app, AGENT) as c:
            first = await register(c, singleton_key="memory-sync")
            dup = await c.post("/v1/agents", json={"name": "a2", "role": "worker", "singleton_key": "memory-sync"})
            assert dup.status_code == 409
            assert dup.json()["error"]["code"] == "duplicate_agent"
            assert dup.json()["error"]["existing_agent_id"] == first["agent_id"]

            second = await register(c, name="a2", singleton_key="memory-sync", takeover=True)
            assert second["agent_id"] == first["agent_id"] and second["epoch"] == first["epoch"] + 1
            assert second["took_over"] is True
            stale = await c.post(f"/v1/agents/{first['agent_id']}/heartbeat", json={"epoch": first["epoch"]})
            assert stale.status_code == 409 and stale.json()["error"]["code"] == "fenced"
            ok = await c.post(f"/v1/agents/{first['agent_id']}/heartbeat", json={"epoch": second["epoch"]})
            assert ok.status_code == 200


async def test_spawn_cap_per_role():
    async with server() as (app, rt):
        await rt.role_limits.set("default", "syncer", 1)
        async with http(app, AGENT) as c:
            await register(c, role="syncer", name="s1")
            r = await c.post("/v1/agents", json={"name": "s2", "role": "syncer"})
            assert r.status_code == 429 and r.json()["error"]["code"] == "spawn_limit"


async def test_operator_directive_delivered_on_heartbeat_and_acked():
    async with server() as (app, rt):
        async with http(app, AGENT) as c, http(app, OPERATOR) as op:
            a = await register(c)
            aid = a["agent_id"]
            d = (await op.post(f"/api/agents/{aid}/directives", json={"type": "PAUSE", "reason": "maintenance"})).json()
            ack = (await c.post(f"/v1/agents/{aid}/heartbeat", json={"epoch": a["epoch"]})).json()
            assert [x["type"] for x in ack["directives"]] == ["PAUSE"]
            assert ack["pause_task"] is True
            # redelivered until acknowledged…
            polled = (await c.get(f"/v1/agents/{aid}/directives")).json()["directives"]
            assert polled == []  # not yet due for redelivery
            await c.post(f"/v1/agents/{aid}/heartbeat", json={
                "epoch": a["epoch"], "acked_directives": [{"directive_id": d["directive_id"], "ok": True}]})
            assert await rt.directives.outstanding(aid) == []
            assert rt.directives.recent(aid)[0].status.value == "ACKED"


async def test_task_lease_lifecycle_and_fencing():
    async with server() as (app, rt):
        async with http(app, AGENT) as c:
            a = await register(c, role="researcher")
            aid = a["agent_id"]
            t = (await c.post("/v1/tasks", json={"title": "summarise", "required_role": "researcher",
                                                 "input": {"q": "x"}})).json()
            assert t["status"] == "PENDING" and t["trace_id"]
            claimed = (await c.post(f"/v1/agents/{aid}/tasks/claim", json={"lease_s": 30})).json()
            assert claimed["task_id"] == t["task_id"] and claimed["lease_id"]
            assert (await rt.registry.get(aid)).status.value == "RUNNING"
            # Nothing else to claim
            assert (await c.post(f"/v1/agents/{aid}/tasks/claim", json={})).status_code == 204
            hb = (await c.post(f"/v1/tasks/{t['task_id']}/heartbeat",
                               json={"lease_id": claimed["lease_id"], "step": "reading"})).json()
            assert hb["status"] == "RUNNING"
            stale = await c.post(f"/v1/tasks/{t['task_id']}/complete", json={"lease_id": "lease-bogus"})
            assert stale.status_code == 409 and stale.json()["error"]["code"] == "lease_conflict"
            done = (await c.post(f"/v1/tasks/{t['task_id']}/complete",
                                 json={"lease_id": claimed["lease_id"], "output": {"s": "ok"}})).json()
            assert done["status"] == "COMPLETED"
            state = await rt.registry.get(aid)
            assert state.status.value == "IDLE" and state.total_tasks_completed == 1


async def test_output_schema_enforced_and_failure_classification():
    async with server() as (app, rt):
        schema = {"type": "object", "required": ["answer"], "properties": {"answer": {"type": "string"}}}
        async with http(app, AGENT) as c:
            a = await register(c)
            aid = a["agent_id"]
            t = (await c.post("/v1/tasks", json={"title": "q", "output_schema": schema, "max_attempts": 3})).json()
            cl = (await c.post(f"/v1/agents/{aid}/tasks/claim", json={})).json()
            bad = await c.post(f"/v1/tasks/{t['task_id']}/complete", json={"lease_id": cl["lease_id"],
                                                                         "output": {"answer": 42}})
            assert bad.status_code == 422 and bad.json()["error"]["code"] == "output_invalid"
            task = await rt.tasks.get(t["task_id"])
            assert task.status.value == "PENDING" and task.not_before is not None  # retried with backoff
            # non-transient failure → DLQ immediately (fail fast)
            t2 = (await c.post("/v1/tasks", json={"title": "q2", "max_attempts": 5})).json()
            task2 = await rt.tasks.get(t2["task_id"])
            task2.not_before = None
            cl2 = None
            for _ in range(3):
                r = await c.post(f"/v1/agents/{aid}/tasks/claim", json={})
                if r.status_code == 200 and r.json()["task_id"] == t2["task_id"]:
                    cl2 = r.json()
                    break
            assert cl2 is not None
            r = await c.post(f"/v1/tasks/{t2['task_id']}/fail", json={"lease_id": cl2["lease_id"],
                                                                     "error": "bad input", "status_code": 422})
            assert r.json()["status"] == "DEAD_LETTER"


async def test_dependencies_quarantine_and_replay():
    async with server() as (app, rt):
        async with http(app, AGENT) as c, http(app, OPERATOR) as op:
            a = await register(c)
            up = (await c.post("/v1/tasks", json={"title": "extract", "max_attempts": 1})).json()
            down = (await c.post("/v1/tasks", json={"title": "summarise", "depends_on": [up["task_id"]]})).json()
            assert down["status"] == "BLOCKED"
            cl = (await c.post(f"/v1/agents/{a['agent_id']}/tasks/claim", json={})).json()
            assert cl["task_id"] == up["task_id"]  # blocked task is not claimable
            await c.post(f"/v1/tasks/{up['task_id']}/fail", json={"lease_id": cl["lease_id"], "error": "boom"})
            assert (await rt.tasks.get(down["task_id"])).status.value == "QUARANTINED"
            # replaying the upstream lifts the quarantine downstream
            assert (await op.post(f"/api/tasks/{up['task_id']}/replay", json={})).status_code == 200
            assert (await rt.tasks.get(down["task_id"])).status.value == "BLOCKED"
            cl = (await c.post(f"/v1/agents/{a['agent_id']}/tasks/claim", json={})).json()
            await c.post(f"/v1/tasks/{up['task_id']}/complete", json={"lease_id": cl["lease_id"], "output": {}})
            assert (await rt.tasks.get(down["task_id"])).status.value == "PENDING"


async def test_expired_lease_is_retried():
    async with server() as (app, rt):
        async with http(app, AGENT) as c:
            a = await register(c)
            t = (await c.post("/v1/tasks", json={"title": "slow"})).json()
            await c.post(f"/v1/agents/{a['agent_id']}/tasks/claim", json={"lease_s": 5})
            task = await rt.tasks.get(t["task_id"])
            from datetime import datetime, timedelta, timezone

            task.lease_expires_at = datetime.now(timezone.utc) - timedelta(seconds=1)
            await rt.tasks.expire_leases()
            task = await rt.tasks.get(t["task_id"])
            assert task.status.value == "PENDING" and task.error_class == "lease_expired"


async def test_locks_contention_deadlock_and_revoke_on_leave():
    async with server() as (app, rt):
        async with http(app, AGENT) as c:
            a = (await register(c, name="A"))["agent_id"]
            b = (await register(c, name="B"))["agent_id"]
            la = (await c.post("/v1/locks/acquire", json={"agent_id": a, "resources": ["x"]})).json()
            lb = (await c.post("/v1/locks/acquire", json={"agent_id": b, "resources": ["y"]})).json()
            assert (await rt.registry.get(a)).resource_usage.resources_held == ["x"]
            timeout = await c.post("/v1/locks/acquire", json={"agent_id": b, "resources": ["x"], "wait_s": 0.2})
            assert timeout.status_code == 423

            import asyncio

            waiter = asyncio.create_task(c.post("/v1/locks/acquire",
                                                json={"agent_id": a, "resources": ["y"], "wait_s": 5}))
            await asyncio.sleep(0.2)  # A now waits on y (held by B)
            dl = await c.post("/v1/locks/acquire", json={"agent_id": b, "resources": ["x"], "wait_s": 5})
            assert dl.status_code == 409 and dl.json()["error"]["code"] == "deadlock"
            assert set(dl.json()["error"]["cycle"]) == {a, b}
            # B backs off: releases y → A gets it
            await c.post(f"/v1/locks/{lb['lease_id']}/release")
            got = await waiter
            assert got.status_code == 201
            # A leaves → all its leases are revoked
            await c.delete(f"/v1/agents/{a}")
            assert (await rt.locks.snapshot())["leases"] == []
            assert la["lease_id"] != got.json()["lease_id"]


async def test_limits_and_circuits():
    async with server() as (app, rt):
        async with http(app, AGENT) as c, http(app, ADMIN) as admin:
            a = (await register(c))["agent_id"]
            await admin.put("/api/limits/gpu", json={"max_concurrent": 1})
            p1 = await c.post("/v1/limits/gpu/acquire", json={"agent_id": a, "wait_s": 0})
            assert p1.status_code == 201
            p2 = await c.post("/v1/limits/gpu/acquire", json={"agent_id": a, "wait_s": 0.1})
            assert p2.status_code == 429 and "retry-after" in {k.lower() for k in p2.headers}
            await c.post(f"/v1/limits/permits/{p1.json()['permit_id']}/release")
            assert (await c.post("/v1/limits/gpu/acquire", json={"agent_id": a, "wait_s": 0})).status_code == 201

            for _ in range(5):
                await c.post("/v1/circuits/openai/report", json={"ok": False, "error": "503"})
            st = (await c.post("/v1/circuits/openai/allow")).json()
            assert st["state"] == "OPEN" and st["allowed"] is False


async def test_session_reports_roles():
    async with server() as (app, _):
        async with http(app, OPERATOR) as op:
            s = (await op.get("/api/session")).json()
            assert s["role"] == "operator" and s["can_operate"] and not s["is_admin"]
        async with http(app, ADMIN) as ad:
            assert (await ad.get("/api/session")).json()["is_admin"] is True
