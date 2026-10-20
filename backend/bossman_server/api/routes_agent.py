"""
api.routes_agent
────────────────
The BOSSman agent protocol — /v1. Language-neutral HTTP+JSON; the Python
and TypeScript SDKs are thin clients over these routes.

  Agents      register · heartbeat (directives back) · directives long-poll ·
              ack · events · quality · probes · deregister
  Tasks       submit · get (long-poll) · list · cancel · claim (lease) ·
              renew · complete · fail · checkpoints
  Resources   leased locks · fleet-wide limits · shared circuits
  Governance  authorize · approvals (long-poll) · tool gateway proxy ·
              compensation · idempotency records
"""

from __future__ import annotations

import time
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response

from bossman_server.api import auth, views
from bossman_server.api.schemas import (
    AckBody,
    AuthorizeBody,
    CheckpointBody,
    CircuitReportBody,
    ClaimBody,
    CompensateBody,
    CompleteBody,
    EventsBody,
    FailBody,
    HeartbeatBody,
    IdempotencyBeginBody,
    IdempotencyCompleteBody,
    LockBody,
    PermitBody,
    ProbeResultBody,
    ProbesBody,
    ProxyBody,
    QualityBody,
    RegisterBody,
    RenewBody,
    SubmitTaskBody,
    TaskHeartbeatBody,
)
from bossman_server.contracts.agent_state import AgentState, AgentStatus
from bossman_server.contracts.directives import DirectiveAck
from bossman_server.contracts.events import BossmanEvent, EventType
from bossman_server.contracts.heartbeat import HeartbeatPayload
from bossman_server.control import compensation
from bossman_server.control.agents import RegisterRequest
from bossman_server.control.auth import Principal
from bossman_server.control.policy import ActionRequest
from bossman_server.control.quality import ProbeSpec
from bossman_server.control.secrets import SecretError
from bossman_server.control.tracing import format_traceparent, parse_traceparent
from bossman_server.core.registry import FencedError
from bossman_server.core.task_manager import Task, TaskStatus, is_transient

router = APIRouter(prefix="/v1", tags=["agent-protocol"])


def _rt(request: Request) -> Any:
    return request.app.state.rt


async def _agent(request: Request, p: Principal, agent_id: str, epoch: int | None = None) -> AgentState:
    state = await _rt(request).registry.get(agent_id)
    if state is None or not p.in_namespace(state.info.namespace):
        raise HTTPException(404, "Agent not found.")
    if epoch is not None and epoch != state.info.epoch:
        raise FencedError(agent_id, epoch, state.info.epoch)
    return state


async def _task(request: Request, p: Principal, task_id: str) -> Task:
    t = await _rt(request).tasks.get(task_id)
    if t is None or not p.in_namespace(t.namespace):
        raise HTTPException(404, "Task not found.")
    return t


def _agent_out(rt: Any, s: AgentState) -> dict[str, Any]:
    return {
        **views.agent_view(s),
        "namespace": s.info.namespace,
        "epoch": s.info.epoch,
        "heartbeat_interval_s": rt.heartbeat_interval_s,
        "heartbeat_timeout_s": rt.heartbeat_timeout_s,
    }


def _task_out(t: Task) -> dict[str, Any]:
    out = views.task_view(t, detail=True)
    out.update({
        "namespace": t.namespace, "lease_id": t.lease_id,
        "lease_expires_at": t.lease_expires_at.isoformat() if t.lease_expires_at else None,
        "trace_id": t.trace_id, "traceparent": format_traceparent(t.trace_id) if t.trace_id else None,
        "depends_on": t.depends_on, "parent_task_id": t.parent_task_id,
        "cancel_requested": t.cancel_requested, "output_schema": t.output_schema,
        "quarantine_reason": t.quarantine_reason, "error_class": t.error_class,
        "is_terminal": t.is_terminal, "labels": t.labels,
    })
    return out


# ── Agents ───────────────────────────────────────────────────────────────────


@router.post("/agents", status_code=201)
async def register(body: RegisterBody, request: Request, p: Principal = Depends(auth.agent_principal)):
    rt = _rt(request)
    state, took_over = await rt.agents.register(p, RegisterRequest(**body.model_dump()))
    flags = await rt.flags.snapshot_for(state)
    return {**_agent_out(rt, state), "took_over": took_over, "flags": flags,
            "flags_version": rt.flags.version, "server_version": views._project_version()}


@router.get("/agents")
async def list_agents(request: Request, role: str | None = None, namespace: str | None = None,
                      p: Principal = Depends(auth.agent_principal)):
    rt = _rt(request)
    ns = namespace or p.default_namespace
    if not p.in_namespace(ns):
        raise HTTPException(403, "Namespace not permitted for this key.")
    agents = await rt.registry.by_namespace(ns)
    return {"agents": [_agent_out(rt, a) for a in agents if role is None or a.info.role == role]}


@router.get("/agents/{agent_id}")
async def get_agent(agent_id: str, request: Request, p: Principal = Depends(auth.agent_principal)):
    rt = _rt(request)
    return _agent_out(rt, await _agent(request, p, agent_id))


@router.delete("/agents/{agent_id}")
async def deregister(agent_id: str, request: Request, reason: str = Query("shutdown", max_length=200),
                     epoch: int | None = None, p: Principal = Depends(auth.agent_principal)):
    await _agent(request, p, agent_id, epoch)
    await _rt(request).agents.deregister(agent_id, reason)
    return {"ok": True}


@router.post("/agents/{agent_id}/heartbeat")
async def heartbeat(agent_id: str, body: HeartbeatBody, request: Request,
                    p: Principal = Depends(auth.agent_principal)):
    await _agent(request, p, agent_id)
    ack = await _rt(request).registry.record_heartbeat(HeartbeatPayload(agent_id=agent_id, **body.model_dump()))
    return ack.model_dump(mode="json")


@router.get("/agents/{agent_id}/directives")
async def poll_directives(agent_id: str, request: Request, wait: float = Query(0.0, ge=0, le=60),
                          epoch: int | None = None, p: Principal = Depends(auth.agent_principal)):
    await _agent(request, p, agent_id, epoch)
    items = await _rt(request).directives.wait(agent_id, wait)
    return {"directives": [d.model_dump(mode="json") for d in items]}


@router.post("/agents/{agent_id}/directives/{directive_id}/ack")
async def ack_directive(agent_id: str, directive_id: str, body: AckBody, request: Request,
                        p: Principal = Depends(auth.agent_principal)):
    await _agent(request, p, agent_id)
    d = await _rt(request).directives.ack(agent_id, DirectiveAck(directive_id=directive_id, **body.model_dump()))
    return {"ok": True, "found": d is not None}


@router.post("/agents/{agent_id}/events")
async def agent_events(agent_id: str, body: EventsBody, request: Request,
                       p: Principal = Depends(auth.agent_principal)):
    rt = _rt(request)
    state = await _agent(request, p, agent_id)
    for e in body.events:
        common = dict(agent_id=agent_id, namespace=state.info.namespace, task_id=e.task_id,
                      trace_id=e.trace_id, span_id=e.span_id)
        if e.type in ("tool_call", "llm_call"):
            transient = e.transient if e.transient is not None else is_transient(e.error_class, e.status_code)
            payload = {"tool": e.name, "kind": e.type, "dependency": e.dependency or e.name,
                       "duration_ms": e.duration_ms, "status_code": e.status_code, "tokens": e.tokens,
                       "error_class": e.error_class, "transient": transient, "error": e.error, **e.data}
            await rt.bus.publish(BossmanEvent.create(
                EventType.TOOL_SUCCEEDED if e.ok else EventType.TOOL_FAILED,
                message=f"{state.info.name} {e.type.replace('_', ' ')} {e.name} "
                        + ("ok" if e.ok else f"failed ({e.status_code or e.error_class or 'error'})"),
                payload=payload, **common))
        else:
            await rt.bus.publish(BossmanEvent.create(
                EventType.AGENT_LOG, message=f"{state.info.name}: {e.message or e.name}"[:500],
                payload={"name": e.name, **e.data}, **common))
    return {"accepted": len(body.events)}


@router.post("/agents/{agent_id}/quality")
async def report_quality(agent_id: str, body: QualityBody, request: Request,
                         p: Principal = Depends(auth.agent_principal)):
    await _agent(request, p, agent_id)
    return await _rt(request).quality.report(agent_id, body.score, task_id=body.task_id,
                                             evaluator=body.evaluator, note=body.note)


@router.put("/agents/{agent_id}/probes")
async def register_probes(agent_id: str, body: ProbesBody, request: Request,
                          p: Principal = Depends(auth.agent_principal)):
    await _agent(request, p, agent_id)
    specs = [ProbeSpec(**pb.model_dump()) for pb in body.probes]
    return {"probes": await _rt(request).quality.register_probes(agent_id, specs)}


@router.post("/agents/{agent_id}/probes/{name}/result")
async def probe_result(agent_id: str, name: str, body: ProbeResultBody, request: Request,
                       p: Principal = Depends(auth.agent_principal)):
    await _agent(request, p, agent_id)
    return await _rt(request).quality.submit_result(agent_id, name, output=body.output, error=body.error)


# ── Tasks ────────────────────────────────────────────────────────────────────


async def submit_task(rt: Any, body: SubmitTaskBody, namespace: str, submitted_by: str) -> Task:
    trace = parse_traceparent(body.traceparent)
    for dep in body.depends_on:
        t = await rt.tasks.get(dep)
        if t is None or t.namespace != namespace:
            raise HTTPException(404, f"Dependency task {dep!r} not found in namespace {namespace!r}.")
    return await rt.tasks.create(
        body.title, body.description, priority=body.priority_enum(), required_role=body.required_role,
        max_attempts=body.max_attempts, input_data=body.input, deadline=body.deadline,
        namespace=namespace, idempotency_key=body.idempotency_key, parent_task_id=body.parent_task_id,
        depends_on=body.depends_on, output_schema=body.output_schema,
        trace_id=trace[0] if trace else None, labels=body.labels, submitted_by=submitted_by,
    )


@router.post("/tasks", status_code=201)
async def create_task(body: SubmitTaskBody, request: Request, p: Principal = Depends(auth.agent_principal)):
    ns = body.namespace or p.default_namespace
    if not p.in_namespace(ns):
        raise HTTPException(403, "Namespace not permitted for this key.")
    return _task_out(await submit_task(_rt(request), body, ns, p.actor))


@router.get("/tasks")
async def list_tasks(request: Request, status: str | None = None, namespace: str | None = None,
                     limit: int = Query(100, ge=1, le=1000), p: Principal = Depends(auth.agent_principal)):
    ns = namespace or p.default_namespace
    if not p.in_namespace(ns):
        raise HTTPException(403, "Namespace not permitted for this key.")
    statuses = [TaskStatus(s.strip().upper()) for s in status.split(",")] if status else None
    items = await _rt(request).tasks.query(namespace=ns, statuses=statuses)
    return {"tasks": [_task_out(t) for t in items[-limit:]]}


@router.get("/tasks/{task_id}")
async def get_task(task_id: str, request: Request, wait: float = Query(0.0, ge=0, le=60),
                   p: Principal = Depends(auth.agent_principal)):
    await _task(request, p, task_id)
    return _task_out(await _rt(request).tasks.wait_terminal(task_id, wait))


@router.post("/tasks/{task_id}/cancel")
async def cancel_task(task_id: str, request: Request, p: Principal = Depends(auth.agent_principal)):
    await _task(request, p, task_id)
    return _task_out(await _rt(request).tasks.cancel(task_id, f"cancelled by {p.actor}"))


@router.post("/agents/{agent_id}/tasks/claim")
async def claim(agent_id: str, body: ClaimBody, request: Request, response: Response,
                epoch: int | None = None, p: Principal = Depends(auth.agent_principal)):
    rt = _rt(request)
    state = await _agent(request, p, agent_id, epoch)
    if state.paused or state.status not in (AgentStatus.IDLE, AgentStatus.RUNNING):
        response.status_code = 204
        return None
    roles = {state.info.role, *body.roles}
    t = await rt.tasks.claim_wait(agent_id, roles=roles, namespace=state.info.namespace,
                                  lease_s=body.lease_s, wait_s=body.wait_s)
    if t is None:
        response.status_code = 204
        return None
    state = await rt.registry.get(agent_id)
    if state is not None:
        if state.status == AgentStatus.IDLE:
            await rt.registry.update_status(agent_id, AgentStatus.RUNNING)
        state.current_task_id = t.task_id
        state.current_step = "claimed"
        await rt.registry.sync(agent_id)
    checkpoint = await rt.checkpoints.latest(t.task_id)
    return {**_task_out(t), "checkpoint": checkpoint}


@router.post("/tasks/{task_id}/heartbeat")
async def task_heartbeat(task_id: str, body: TaskHeartbeatBody, request: Request,
                         p: Principal = Depends(auth.agent_principal)):
    await _task(request, p, task_id)
    t = await _rt(request).tasks.renew_lease(task_id, body.lease_id, body.lease_s, body.step)
    return {"status": t.status.value, "cancel_requested": t.cancel_requested or t.status == TaskStatus.CANCELLED,
            "lease_expires_at": t.lease_expires_at.isoformat() if t.lease_expires_at else None}


async def _settle_agent(rt: Any, task: Task, ok: bool, error: str | None = None) -> None:
    if not task.assigned_to:
        return
    state = await rt.registry.get(task.assigned_to)
    if state is None:
        return
    if ok:
        await rt.registry.record_success(task.assigned_to)
    else:
        await rt.registry.record_failure(task.assigned_to, error or "task failed")
        state.current_task_id = None
        state.current_step = None
    if state.status == AgentStatus.RUNNING:
        others = await rt.tasks.query(statuses=[TaskStatus.RUNNING], assigned_to=task.assigned_to)
        if not others:
            await rt.registry.update_status(task.assigned_to, AgentStatus.IDLE)
    await rt.registry.sync(task.assigned_to)


@router.post("/tasks/{task_id}/complete")
async def complete(task_id: str, body: CompleteBody, request: Request, p: Principal = Depends(auth.agent_principal)):
    rt = _rt(request)
    await _task(request, p, task_id)
    t = await rt.tasks.complete(task_id, body.output, lease_id=body.lease_id)
    await _settle_agent(rt, t, ok=True)
    return _task_out(t)


@router.post("/tasks/{task_id}/fail")
async def fail(task_id: str, body: FailBody, request: Request, p: Principal = Depends(auth.agent_principal)):
    rt = _rt(request)
    await _task(request, p, task_id)
    t = await rt.tasks.fail(task_id, body.error, lease_id=body.lease_id, transient=body.retryable,
                            error_class=body.error_class, status_code=body.status_code)
    await _settle_agent(rt, t, ok=False, error=body.error)
    return _task_out(t)


@router.put("/tasks/{task_id}/checkpoint", status_code=201)
async def put_checkpoint(task_id: str, body: CheckpointBody, request: Request,
                         p: Principal = Depends(auth.agent_principal)):
    rt = _rt(request)
    t = await _task(request, p, task_id)
    if body.lease_id is not None and t.lease_id != body.lease_id:
        raise HTTPException(409, "Lease is not current for this task.")
    return await rt.checkpoints.save(task_id, agent_id=t.assigned_to or "unknown", phase=body.phase,
                                     state=body.state, namespace=t.namespace, trace_id=t.trace_id)


@router.get("/tasks/{task_id}/checkpoint")
async def get_checkpoint(task_id: str, request: Request, before_seq: int | None = None, all: bool = False,  # noqa: A002
                         p: Principal = Depends(auth.agent_principal)):
    rt = _rt(request)
    await _task(request, p, task_id)
    if all:
        return {"checkpoints": await rt.checkpoints.list(task_id)}
    return {"checkpoint": await rt.checkpoints.latest(task_id, before_seq=before_seq)}


# ── Locks / limits / circuits ────────────────────────────────────────────────


@router.post("/locks/acquire", status_code=201)
async def lock_acquire(body: LockBody, request: Request, p: Principal = Depends(auth.agent_principal)):
    state = await _agent(request, p, body.agent_id)
    lease = await _rt(request).locks.acquire(body.agent_id, body.resources, namespace=state.info.namespace,
                                             ttl_s=body.ttl_s, wait_s=body.wait_s, task_id=body.task_id)
    return lease.__dict__


async def _lease(request: Request, p: Principal, lease_id: str) -> Any:
    lease = await _rt(request).locks.get(lease_id)
    if lease is None or not p.in_namespace(lease.namespace):
        raise HTTPException(404, "Lease not found (expired or revoked).")
    return lease


@router.post("/locks/{lease_id}/renew")
async def lock_renew(lease_id: str, body: RenewBody, request: Request, p: Principal = Depends(auth.agent_principal)):
    await _lease(request, p, lease_id)
    return (await _rt(request).locks.renew(lease_id, body.ttl_s)).__dict__


@router.post("/locks/{lease_id}/release")
async def lock_release(lease_id: str, request: Request, p: Principal = Depends(auth.agent_principal)):
    lease = await _rt(request).locks.get(lease_id)
    if lease is not None and not p.in_namespace(lease.namespace):
        raise HTTPException(404, "Lease not found.")
    return {"released": await _rt(request).locks.release(lease_id)}


@router.get("/locks")
async def lock_snapshot(request: Request, p: Principal = Depends(auth.agent_principal)):
    return await _rt(request).locks.snapshot(None if "*" in p.namespaces else p.default_namespace)


@router.post("/limits/{name}/acquire", status_code=201)
async def limit_acquire(name: str, body: PermitBody, request: Request, p: Principal = Depends(auth.agent_principal)):
    state = await _agent(request, p, body.agent_id)
    permit = await _rt(request).limits.acquire(name, body.agent_id, cost=body.cost, wait_s=body.wait_s,
                                               namespace=state.info.namespace)
    return permit.__dict__


@router.post("/limits/permits/{permit_id}/release")
async def limit_release(permit_id: str, request: Request, p: Principal = Depends(auth.agent_principal)):
    return {"released": await _rt(request).limits.release(permit_id)}


@router.get("/limits")
async def limits(request: Request, p: Principal = Depends(auth.agent_principal)):
    return {"limits": await _rt(request).limits.snapshot()}


@router.get("/circuits/{key:path}/status")
async def circuit_status(key: str, request: Request, p: Principal = Depends(auth.agent_principal)):
    return await _rt(request).circuits.status(key)


@router.post("/circuits/{key:path}/allow")
async def circuit_allow(key: str, request: Request, p: Principal = Depends(auth.agent_principal)):
    return await _rt(request).circuits.allow(key)


@router.post("/circuits/{key:path}/report")
async def circuit_report(key: str, body: CircuitReportBody, request: Request,
                         p: Principal = Depends(auth.agent_principal)):
    return await _rt(request).circuits.report(key, body.ok, error=body.error, agent_id=body.agent_id)


# ── Governance ───────────────────────────────────────────────────────────────


@router.post("/agents/{agent_id}/authorize")
async def authorize(agent_id: str, body: AuthorizeBody, request: Request, epoch: int | None = None,
                    p: Principal = Depends(auth.agent_principal)):
    rt = _rt(request)
    state = await _agent(request, p, agent_id, epoch)
    req = ActionRequest(**body.model_dump())
    decision = rt.policy.evaluate(state, req)
    ns = state.info.namespace
    await rt.audit.record("policy_decision", f"agent:{agent_id}", namespace=ns, agent_id=agent_id,
                          data={"request": req.to_dict(), "decision": decision.to_dict()})
    await rt.bus.publish(BossmanEvent.create(
        EventType.POLICY_DECISION, agent_id=agent_id, namespace=ns, task_id=body.task_id,
        message=f"{state.info.name} → {body.action}{' via ' + body.tool if body.tool else ''}: "
                f"{decision.decision.upper()} ({decision.reason})"[:300],
        payload={"action": body.action, "tool": body.tool, "decision": decision.decision,
                 "rule": decision.rule, "reason": decision.reason}))
    out: dict[str, Any] = decision.to_dict()
    if decision.decision == "deny":
        if decision.rule in ("tier", "capabilities"):
            await rt.bus.publish(BossmanEvent.create(
                EventType.PERMISSION_DENIED, agent_id=agent_id, namespace=ns,
                message=f"{state.info.name} denied {body.action}: {decision.reason}"[:300],
                payload={"action": body.action, "tool": body.tool, "reason": decision.reason}))
        else:
            await rt.bus.publish(BossmanEvent.create(
                EventType.TOOL_GATED, agent_id=agent_id, namespace=ns,
                message=f"{state.info.name} blocked from {body.action}: {decision.reason}"[:300],
                payload={"action": body.action, "tool": body.tool, "rule": decision.rule}))
        return out
    if decision.decision == "require_approval":
        approval = await rt.approvals.create(
            agent_id=agent_id, agent_name=state.info.name, namespace=ns, request=req.to_dict(),
            decision=decision.to_dict(), ttl_s=float(rt.policy.settings.get("approval_ttl_s", 3600)))
        out["approval_id"] = approval["approval_id"]
        out["expires_at"] = approval["expires_at"]
        return out
    grant = await rt.approvals.issue_grant(agent_id=agent_id, namespace=ns, request=req.to_dict(),
                                           ttl_s=float(rt.policy.settings.get("grant_ttl_s", 300)))
    out["grant_id"] = grant["grant_id"]
    if body.credentials:
        try:
            out["credentials"] = await rt.secrets.release_for(body.credentials, state, tool=body.tool,
                                                              action=body.action, grant_id=grant["grant_id"])
        except SecretError as exc:
            return {**decision.to_dict(), "decision": "deny", "reason": str(exc), "rule": "credentials"}
    return out


@router.get("/approvals/{approval_id}")
async def get_approval(approval_id: str, request: Request, wait: float = Query(0.0, ge=0, le=60),
                       p: Principal = Depends(auth.agent_principal)):
    rt = _rt(request)
    a = await rt.approvals.get(approval_id)
    if a is None or not p.in_namespace(a["namespace"]):
        raise HTTPException(404, "Approval not found.")
    a = await rt.approvals.wait(approval_id, wait)
    out = {k: a[k] for k in ("approval_id", "status", "required", "grant_id", "expires_at", "agent_id")}
    out["approvals"] = [{"by": v["by"], "at": v["at"]} for v in a["approvals"]]
    out["denials"] = [{"by": v["by"], "at": v["at"], "reason": v["reason"]} for v in a["denials"]]
    creds = a["request"].get("credentials") or []
    if a["status"] == "approved" and creds:
        state = await rt.registry.get(a["agent_id"])
        if state is not None:
            try:
                out["credentials"] = await rt.secrets.release_for(
                    creds, state, tool=a["request"].get("tool"), action=a["request"].get("action"),
                    grant_id=a["grant_id"])
            except SecretError as exc:
                out["credentials_error"] = str(exc)
    return out


@router.post("/agents/{agent_id}/proxy")
async def proxy(agent_id: str, body: ProxyBody, request: Request, p: Principal = Depends(auth.agent_principal)):
    rt = _rt(request)
    state = await _agent(request, p, agent_id)
    grant = await rt.approvals.use_grant(body.grant_id, agent_id)
    return await rt.secrets.proxy(state, grant, credential=body.credential, method=body.method, url=body.url,
                                  headers=body.headers, body=body.body, timeout_s=body.timeout_s,
                                  transport=getattr(request.app.state, "proxy_transport", None))


@router.post("/agents/{agent_id}/compensate")
async def compensate(agent_id: str, body: CompensateBody, request: Request,
                     p: Principal = Depends(auth.agent_principal)):
    rt = _rt(request)
    state = await _agent(request, p, agent_id)
    circuit = await rt.circuits.status(body.dependency or body.tool)
    d = compensation.decide(compensation.CompensationRequest(
        tool=body.tool, error_class=body.error_class, status_code=body.status_code, error=body.error,
        params=body.params, attempt=body.attempt, alternatives=body.alternatives,
        retry_after_s=body.retry_after_s), circuit_state=circuit["state"])
    await rt.bus.publish(BossmanEvent.create(
        EventType.COMPENSATION_DECIDED, agent_id=agent_id, namespace=state.info.namespace,
        message=f"{state.info.name} {body.tool} attempt {body.attempt}: {d.action} ({d.reason})"[:300],
        payload={"tool": body.tool, **d.to_dict()}))
    return d.to_dict()


@router.post("/idempotency/{key}/begin")
async def idem_begin(key: str, body: IdempotencyBeginBody, request: Request,
                     p: Principal = Depends(auth.agent_principal)):
    state = await _agent(request, p, body.agent_id)
    return await _rt(request).idempotency.begin(key[:200], agent_id=body.agent_id,
                                                namespace=state.info.namespace, ttl_s=body.ttl_s)


@router.post("/idempotency/{key}/complete")
async def idem_complete(key: str, body: IdempotencyCompleteBody, request: Request,
                        p: Principal = Depends(auth.agent_principal)):
    state = await _agent(request, p, body.agent_id)
    try:
        await _rt(request).idempotency.complete(key[:200], agent_id=body.agent_id,
                                                namespace=state.info.namespace, result=body.result)
    except PermissionError as exc:
        raise HTTPException(409, str(exc)) from None
    return {"ok": True}


@router.post("/idempotency/{key}/abort")
async def idem_abort(key: str, body: IdempotencyBeginBody, request: Request,
                     p: Principal = Depends(auth.agent_principal)):
    state = await _agent(request, p, body.agent_id)
    return {"aborted": await _rt(request).idempotency.abort(key[:200], agent_id=body.agent_id,
                                                            namespace=state.info.namespace)}


@router.get("/time")
async def server_time() -> dict[str, float]:
    return {"time": time.time()}
