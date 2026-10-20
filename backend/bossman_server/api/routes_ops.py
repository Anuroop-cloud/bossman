"""
api.routes_ops
──────────────
Operator / admin API used by the dashboard and the `bossman` CLI.

Reads need a viewer credential (or public reads); actions need an operator
credential; configuration of trust (keys, secrets, policies, managed
processes, supervisors, limits) needs an admin. Every action is written to
the audit log and announced as an OPERATOR_ACTION event.
"""

from __future__ import annotations

import os
from typing import Any

import yaml
from fastapi import APIRouter, Depends, HTTPException, Query, Request

from bossman_server.api import auth, views
from bossman_server.api.routes_agent import _task_out, submit_task
from bossman_server.api.schemas import (
    ActionBody,
    DecisionBody,
    DirectiveBody,
    FlagBody,
    KeyBody,
    LimitBody,
    ManagedBody,
    PolicyBody,
    RoleLimitBody,
    SecretBody,
    SubmitTaskBody,
    SupervisorBody,
)
from bossman_server.contracts.events import BossmanEvent, EventType
from bossman_server.control.auth import Principal, Role
from bossman_server.control.limits import LimitSpec
from bossman_server.control.managed import ManagedSpec
from bossman_server.control.policy import ActionRequest, PolicyError
from bossman_server.control.supervisors import SupervisorSpec

router = APIRouter(prefix="/api", tags=["operations"])


def _rt(request: Request) -> Any:
    return request.app.state.rt


async def _act(request: Request, p: Principal, action: str, data: dict[str, Any] | None = None,
               agent_id: str | None = None, namespace: str | None = None) -> None:
    rt = _rt(request)
    await rt.audit.record("operator_action", p.actor, agent_id=agent_id, namespace=namespace,
                          data={"action": action, **(data or {})})
    await rt.bus.publish(BossmanEvent.create(
        EventType.OPERATOR_ACTION, agent_id=agent_id, namespace=namespace,
        message=f"{p.actor}: {action}", payload={"action": action, "by": p.actor, **(data or {})}))


# ── Tasks ────────────────────────────────────────────────────────────────────


@router.post("/tasks", status_code=201)
async def submit(body: SubmitTaskBody, request: Request, p: Principal = Depends(auth.operator)):
    ns = body.namespace or "default"
    t = await submit_task(_rt(request), body, ns, p.actor)
    await _act(request, p, "submit_task", {"task_id": t.task_id, "title": t.title}, namespace=ns)
    return _task_out(t)


@router.post("/tasks/{task_id}/cancel")
async def cancel(task_id: str, body: ActionBody, request: Request, p: Principal = Depends(auth.operator)):
    rt = _rt(request)
    if await rt.tasks.get(task_id) is None:
        raise HTTPException(404, "Task not found.")
    t = await rt.tasks.cancel(task_id, body.reason or f"cancelled by {p.actor}")
    await _act(request, p, "cancel_task", {"task_id": task_id, "reason": body.reason}, namespace=t.namespace)
    return _task_out(t)


# ── Agents: directives & actions ─────────────────────────────────────────────


async def _agent(request: Request, agent_id: str) -> Any:
    state = await _rt(request).registry.get(agent_id)
    if state is None:
        raise HTTPException(404, "Agent not found.")
    return state


@router.post("/agents/{agent_id}/directives", status_code=201)
async def send_directive(agent_id: str, body: DirectiveBody, request: Request,
                         p: Principal = Depends(auth.operator)):
    state = await _agent(request, agent_id)
    d = await _rt(request).directives.issue(agent_id, body.type, body.params, reason=body.reason or p.actor,
                                            issued_by=p.actor, ttl_s=body.ttl_s, namespace=state.info.namespace)
    await _act(request, p, f"directive:{body.type.value}", {"directive_id": d.directive_id,
                                                            "reason": body.reason},
               agent_id=agent_id, namespace=state.info.namespace)
    return d.model_dump(mode="json")


@router.get("/agents/{agent_id}/directives")
async def list_directives(agent_id: str, request: Request, p: Principal = Depends(auth.viewer)):
    rt = _rt(request)
    return {
        "outstanding": [d.model_dump(mode="json") for d in await rt.directives.outstanding(agent_id)],
        "recent": [d.model_dump(mode="json") for d in rt.directives.recent(agent_id)],
    }


@router.post("/agents/{agent_id}/restart")
async def restart_agent(agent_id: str, body: ActionBody, request: Request, p: Principal = Depends(auth.operator)):
    state = await _agent(request, agent_id)
    ok = await _rt(request).engine.restart_agent(agent_id, None, reason=body.reason or f"restart by {p.actor}")
    await _act(request, p, "restart_agent", {"reason": body.reason, "ok": ok}, agent_id=agent_id,
               namespace=state.info.namespace)
    if not ok:
        raise HTTPException(409, f"Agent cannot be restarted from status {state.status.value}.")
    return {"ok": True}


@router.post("/agents/{agent_id}/release-resources")
async def release_resources(agent_id: str, body: ActionBody, request: Request,
                            p: Principal = Depends(auth.operator)):
    rt = _rt(request)
    state = await _agent(request, agent_id)
    freed = await rt.locks.revoke_agent(agent_id, reason=body.reason or f"released by {p.actor}")
    freed += await rt.resources.mediator.force_release(agent_id)
    await _act(request, p, "release_resources", {"freed": freed}, agent_id=agent_id,
               namespace=state.info.namespace)
    return {"freed": freed}


@router.post("/agents/{agent_id}/deregister")
async def deregister(agent_id: str, body: ActionBody, request: Request, p: Principal = Depends(auth.admin)):
    state = await _agent(request, agent_id)
    await _rt(request).agents.deregister(agent_id, body.reason or f"removed by {p.actor}")
    await _act(request, p, "deregister_agent", {"reason": body.reason}, agent_id=agent_id,
               namespace=state.info.namespace)
    return {"ok": True}


# ── Incidents ────────────────────────────────────────────────────────────────


@router.post("/incidents/{incident_id}/resolve")
async def resolve_incident(incident_id: str, body: ActionBody, request: Request,
                           p: Principal = Depends(auth.operator)):
    rt = _rt(request)
    if views.incident_detail(rt, incident_id) is None:
        raise HTTPException(404, "Incident not found.")
    await rt.bus.publish(BossmanEvent.create(
        EventType.INCIDENT_RESOLVED, message=f"Incident {incident_id} resolved by {p.actor}",
        payload={"incident_id": incident_id, "by": p.actor, "reason": body.reason}))
    await rt.audit.record("operator_action", p.actor, data={"action": "resolve_incident",
                                                             "incident_id": incident_id, "reason": body.reason})
    return {"ok": True}


# ── Approvals ────────────────────────────────────────────────────────────────


@router.get("/approvals")
async def approvals(request: Request, status: str | None = Query(None, max_length=16),
                    p: Principal = Depends(auth.viewer)):
    return {"approvals": await _rt(request).approvals.list(status=status)}


@router.post("/approvals/{approval_id}/approve")
async def approve(approval_id: str, body: DecisionBody, request: Request, p: Principal = Depends(auth.operator)):
    rt = _rt(request)
    return await rt.approvals.decide(approval_id, approve=True, actor=p.actor, reason=body.reason,
                                     grant_ttl_s=float(rt.policy.settings.get("grant_ttl_s", 300)))


@router.post("/approvals/{approval_id}/deny")
async def deny(approval_id: str, body: DecisionBody, request: Request, p: Principal = Depends(auth.operator)):
    return await _rt(request).approvals.decide(approval_id, approve=False, actor=p.actor, reason=body.reason)


# ── Policy / flags ───────────────────────────────────────────────────────────


@router.get("/policies")
async def get_policy(request: Request, p: Principal = Depends(auth.viewer)):
    rt = _rt(request)
    return {"revision": rt.policy.revision, "policy": rt.policy.policy,
            "yaml": yaml.safe_dump(rt.policy.policy, sort_keys=False), "stats": rt.policy.stats}


@router.put("/policies")
async def put_policy(body: PolicyBody, request: Request, p: Principal = Depends(auth.admin)):
    rt = _rt(request)
    doc = body.policy
    if doc is None and body.yaml is not None:
        try:
            doc = yaml.safe_load(body.yaml)
        except yaml.YAMLError as exc:
            raise HTTPException(400, f"Invalid YAML: {exc}") from None
    if doc is None:
        raise HTTPException(400, "Provide `policy` (object) or `yaml` (string).")
    try:
        revision = await rt.policy.apply(doc, by=p.actor)
    except PolicyError as exc:
        raise HTTPException(400, str(exc)) from None
    await _act(request, p, "apply_policy", {"revision": revision})
    return {"revision": revision}


@router.post("/policies/evaluate")
async def evaluate_policy(request: Request, body: dict[str, Any], p: Principal = Depends(auth.operator)):
    """Dry-run: what would the policy decide for this agent and action?"""
    rt = _rt(request)
    state = await _agent(request, str(body.get("agent_id", "")))
    try:
        req = ActionRequest(**{k: v for k, v in (body.get("request") or {}).items()
                               if k in ActionRequest.__dataclass_fields__})
    except TypeError as exc:
        raise HTTPException(400, str(exc)) from None
    decision = rt.policy.evaluate(state, req)
    rt.policy.stats[decision.decision] -= 1  # dry runs are not real decisions
    return decision.to_dict()


@router.get("/flags")
async def flags(request: Request, p: Principal = Depends(auth.viewer)):
    rt = _rt(request)
    return {"version": rt.flags.version, "rules": rt.flags.list()}


@router.put("/flags/{key}")
async def set_flag(key: str, body: FlagBody, request: Request, p: Principal = Depends(auth.operator)):
    rule = await _rt(request).flags.set(key, body.enabled, namespace=body.namespace, role=body.role,
                                        agent_id=body.agent_id, rollout_percent=body.rollout_percent,
                                        note=body.note, by=p.actor)
    await _act(request, p, "set_flag", {"rule": rule})
    return rule


@router.delete("/flags/rules/{rule_id:path}")
async def delete_flag(rule_id: str, request: Request, p: Principal = Depends(auth.operator)):
    ok = await _rt(request).flags.delete(rule_id, by=p.actor)
    if not ok:
        raise HTTPException(404, "Flag rule not found.")
    await _act(request, p, "delete_flag", {"rule_id": rule_id})
    return {"ok": True}


# ── Keys / secrets ───────────────────────────────────────────────────────────


@router.get("/keys")
async def keys(request: Request, p: Principal = Depends(auth.admin)):
    return {"keys": await _rt(request).keys.list()}


@router.post("/keys", status_code=201)
async def create_key(body: KeyBody, request: Request, p: Principal = Depends(auth.admin)):
    token, record = await _rt(request).keys.create(
        name=body.name, role=Role(body.role), namespaces=body.namespaces or None, max_tier=body.max_tier,
        created_by=p.actor, ttl_days=body.ttl_days)
    await _act(request, p, "create_key", {"key_id": record["key_id"], "role": body.role})
    return {"key": token, "record": record, "note": "Store this key now — it is never shown again."}


@router.post("/keys/{key_id}/revoke")
async def revoke_key(key_id: str, request: Request, p: Principal = Depends(auth.admin)):
    if not await _rt(request).keys.revoke(key_id, by=p.actor):
        raise HTTPException(404, "Key not found or already revoked.")
    return {"ok": True}


@router.get("/secrets")
async def secrets(request: Request, p: Principal = Depends(auth.admin)):
    return {"secrets": await _rt(request).secrets.list()}


@router.put("/secrets/{name}")
async def put_secret(name: str, body: SecretBody, request: Request, p: Principal = Depends(auth.admin)):
    return await _rt(request).secrets.put(name, by=p.actor, **body.model_dump(mode="json"))


@router.delete("/secrets/{name}")
async def delete_secret(name: str, request: Request, p: Principal = Depends(auth.admin)):
    if not await _rt(request).secrets.delete(name, by=p.actor):
        raise HTTPException(404, "Secret not found.")
    return {"ok": True}


# ── Managed processes / supervisors ──────────────────────────────────────────


@router.get("/managed")
async def managed(request: Request, p: Principal = Depends(auth.viewer)):
    return {"managed": _rt(request).managed.status(), "active": _rt(request).managed.active}


@router.post("/managed", status_code=201)
async def create_managed(body: ManagedBody, request: Request, p: Principal = Depends(auth.admin)):
    rt = _rt(request)
    spec = await rt.managed.put(ManagedSpec(**body.model_dump(), created_by=p.actor))
    await _act(request, p, "create_managed", {"managed_id": spec.managed_id, "name": spec.name},
               namespace=spec.namespace)
    if rt.managed.active:
        await rt.managed.reconcile()
    return {"managed_id": spec.managed_id}


@router.post("/managed/{managed_id}/{action}")
async def managed_action(managed_id: str, action: str, body: ActionBody, request: Request,
                         p: Principal = Depends(auth.operator)):
    rt = _rt(request)
    if rt.managed.get_spec(managed_id) is None:
        raise HTTPException(404, "Managed agent not found.")
    if action == "restart":
        await rt.managed.restart(managed_id, body.reason or f"restart by {p.actor}")
    elif action in ("start", "stop"):
        await rt.managed.set_desired(managed_id, "running" if action == "start" else "stopped")
        if action == "start" and rt.managed.active:
            await rt.managed.reconcile()
    else:
        raise HTTPException(404, "Unknown action.")
    await _act(request, p, f"managed_{action}", {"managed_id": managed_id, "reason": body.reason})
    return {"ok": True}


@router.delete("/managed/{managed_id}")
async def delete_managed(managed_id: str, request: Request, p: Principal = Depends(auth.admin)):
    if not await _rt(request).managed.delete(managed_id):
        raise HTTPException(404, "Managed agent not found.")
    await _act(request, p, "delete_managed", {"managed_id": managed_id})
    return {"ok": True}


@router.get("/managed/{managed_id}/logs")
async def managed_logs(managed_id: str, request: Request, limit: int = Query(200, ge=1, le=300),
                       p: Principal = Depends(auth.operator)):
    return {"lines": _rt(request).managed.logs(managed_id, limit)}


@router.get("/supervisors")
async def supervisors(request: Request, p: Principal = Depends(auth.viewer)):
    rt = _rt(request)
    out = []
    for s in rt.supervisors.status():
        s["members"] = [a.agent_id for a in await rt.registry.by_supervisor(s["name"])]
        out.append(s)
    return {"supervisors": out}


@router.put("/supervisors/{name}")
async def put_supervisor(name: str, body: SupervisorBody, request: Request, p: Principal = Depends(auth.admin)):
    spec = await _rt(request).supervisors.put(SupervisorSpec(name=name, **body.model_dump()))
    await _act(request, p, "put_supervisor", {"name": name, "strategy": spec.strategy})
    return spec.__dict__


@router.delete("/supervisors/{name}")
async def delete_supervisor(name: str, request: Request, p: Principal = Depends(auth.admin)):
    await _rt(request).supervisors.delete(name)
    return {"ok": True}


# ── Limits / circuits / locks ────────────────────────────────────────────────


@router.get("/limits")
async def limits(request: Request, p: Principal = Depends(auth.viewer)):
    rt = _rt(request)
    return {"limits": await rt.limits.snapshot(), "role_limits": rt.role_limits.all()}


@router.put("/limits/{name}")
async def put_limit(name: str, body: LimitBody, request: Request, p: Principal = Depends(auth.admin)):
    spec = await _rt(request).limits.define(LimitSpec(name=name, **body.model_dump()))
    await _act(request, p, "put_limit", {"name": name})
    return spec.__dict__


@router.delete("/limits/{name}")
async def delete_limit(name: str, request: Request, p: Principal = Depends(auth.admin)):
    return {"ok": await _rt(request).limits.remove(name)}


@router.put("/role-limits/{namespace}/{role}")
async def put_role_limit(namespace: str, role: str, body: RoleLimitBody, request: Request,
                         p: Principal = Depends(auth.admin)):
    await _rt(request).role_limits.set(namespace, role, body.max_instances)
    await _act(request, p, "set_role_limit", {"namespace": namespace, "role": role, "max": body.max_instances})
    return {"ok": True}


@router.get("/circuits")
async def circuits(request: Request, p: Principal = Depends(auth.viewer)):
    rt = _rt(request)
    return {"shared": await rt.circuits.snapshot(), "local": rt.circuit_breakers.snapshot()}


@router.post("/circuits/{key:path}/open")
async def open_circuit(key: str, body: ActionBody, request: Request, p: Principal = Depends(auth.operator)):
    out = await _rt(request).circuits.force_open(key, reason=body.reason or f"opened by {p.actor}")
    await _act(request, p, "open_circuit", {"key": key})
    return out


@router.post("/circuits/{key:path}/close")
async def close_circuit(key: str, request: Request, p: Principal = Depends(auth.operator)):
    rt = _rt(request)
    out = await rt.circuits.force_close(key)
    rt.circuit_breakers.close(key)
    await _act(request, p, "close_circuit", {"key": key})
    return out


@router.get("/locks")
async def locks(request: Request, p: Principal = Depends(auth.viewer)):
    return await _rt(request).locks.snapshot()


# ── Traces / audit / signals ─────────────────────────────────────────────────


@router.get("/traces/{trace_id}")
async def trace(trace_id: str, request: Request, p: Principal = Depends(auth.viewer)):
    rt = _rt(request)
    tasks = await rt.tasks.by_trace(trace_id)
    if not tasks:
        raise HTTPException(404, "Trace not found.")
    task_ids = [t.task_id for t in tasks]
    events: dict[str, Any] = {}
    if rt.event_store is not None:
        for e in await rt.event_store.query(trace_id=trace_id, task_ids=task_ids, limit=2000):
            events[e.event_id] = e
    for _seq, e in rt.store.query(limit=5000):
        if e.trace_id == trace_id or e.task_id in task_ids:
            events[e.event_id] = e
    ordered = sorted(events.values(), key=lambda e: e.occurred_at)
    return {
        "trace_id": trace_id,
        "tasks": [_task_out(t) for t in tasks],
        "edges": [{"from": d, "to": t.task_id, "kind": "depends_on"} for t in tasks for d in t.depends_on]
        + [{"from": t.parent_task_id, "to": t.task_id, "kind": "parent"} for t in tasks if t.parent_task_id],
        "events": [views.event_view(i, e) for i, e in enumerate(ordered, 1)],
    }


@router.get("/audit")
async def audit(request: Request, kind: str | None = Query(None, max_length=32),
                limit: int = Query(200, ge=1, le=1000), p: Principal = Depends(auth.operator)):
    return {"entries": await _rt(request).audit.list(kind=kind, limit=limit)}


@router.get("/signals")
async def signals(request: Request, p: Principal = Depends(auth.viewer)):
    rt = _rt(request)
    return {"anomalies": list(rt.signals.anomalies), "scheduler": {"runs": rt.scheduler.runs,
                                                                    "errors": rt.scheduler.errors}}


# ── Chaos (fault injection) ──────────────────────────────────────────────────


@router.post("/faults")
async def inject_fault(request: Request, body: dict[str, Any], p: Principal = Depends(auth.admin)):
    """Deliberate fault injection for game days. Off unless BOSSMAN_ENABLE_FAULTS=1."""
    if os.getenv("BOSSMAN_ENABLE_FAULTS") != "1":
        raise HTTPException(403, "Fault injection is disabled (set BOSSMAN_ENABLE_FAULTS=1).")
    from bossman_server.core.fault_injector.injector import FaultConfig, FaultInjector

    rt = _rt(request)
    try:
        cfg = FaultConfig(**body)
    except Exception as exc:  # noqa: BLE001 - validation message is safe to return
        raise HTTPException(422, f"Invalid fault: {exc}") from None
    await FaultInjector(rt.registry, rt.tasks, rt.bus).inject(cfg)
    await _act(request, p, "inject_fault", {"fault": cfg.fault_type.value, "agent_id": cfg.agent_id})
    return {"ok": True}
