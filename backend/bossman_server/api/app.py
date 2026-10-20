"""
api.app
───────
FastAPI application: the BOSSman control-plane API.

  /v1/*        agent protocol (SDKs)                    routes_agent
  /api/*       dashboard reads + operator/admin actions  this module + routes_ops
  /ws          live event stream for the dashboard
  /metrics     Prometheus
  /healthz     liveness · /readyz readiness (k8s probes)
  /            the built dashboard (single port, like a daemon)

Security posture
  * Reads are public only when BOSSMAN_PUBLIC_READS allows (never by default
    in production); every mutation is authorised server-side (api.auth).
  * CORS + WebSocket Origin are restricted to BOSSMAN_CORS_ORIGINS.
  * Errors never leak stack traces; responses use a stable error envelope.
"""

from __future__ import annotations

import asyncio
import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from fastapi import Depends, FastAPI, HTTPException, Query, Request, WebSocket, WebSocketDisconnect
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, Response

from bossman_server.api import auth, views
from bossman_server.api.routes_agent import router as agent_router
from bossman_server.api.routes_ops import router as ops_router
from bossman_server.api.runtime import BossmanRuntime
from bossman_server.api.schemas import ActionBody
from bossman_server.contracts.events import BossmanEvent, EventType
from bossman_server.control.agents import RegistrationError
from bossman_server.control.approvals import ApprovalError
from bossman_server.control.auth import Principal
from bossman_server.control.limits import LimitTimeout, UnknownLimit
from bossman_server.control.locks import DeadlockError, LeaseLost, LockTimeout
from bossman_server.control.secrets import SecretError
from bossman_server.core.documents import VersionConflict
from bossman_server.core.registry import FencedError
from bossman_server.core.task_manager import LeaseError, OutputInvalid

log = logging.getLogger("bossman.api")


def _allowed_origins() -> list[str]:
    raw = os.getenv("BOSSMAN_CORS_ORIGINS", "http://localhost:5173,http://127.0.0.1:5173,"
                                            "http://localhost:3010,http://127.0.0.1:3010")
    return [o.strip() for o in raw.split(",") if o.strip()]


def _error(status: int, code: str, message: str, **extra: Any) -> JSONResponse:
    headers = {}
    if "retry_after_s" in extra:
        headers["Retry-After"] = str(max(1, round(extra["retry_after_s"])))
    return JSONResponse(status_code=status, content={"error": {"code": code, "message": message, **extra}},
                        headers=headers)


def _static_dir() -> Path | None:
    candidates = [os.getenv("BOSSMAN_DASHBOARD_DIR"), str(Path(__file__).resolve().parent.parent / "static")]
    for c in candidates:
        if c and (Path(c) / "index.html").is_file():
            return Path(c)
    return None


def create_app(runtime: BossmanRuntime | None = None, *, start_runtime: bool = True) -> FastAPI:
    rt_holder: dict[str, BossmanRuntime] = {}

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        rt = runtime or BossmanRuntime()
        rt_holder["rt"] = rt
        app.state.rt = rt
        if start_runtime:
            await rt.start()
        demo_task = None
        if os.getenv("BOSSMAN_ENV", "development") == "development" and os.getenv("BOSSMAN_DEMO_SEED") == "1":
            from bossman_server.api.demo import run_demo

            demo_task = asyncio.create_task(run_demo(rt), name="bossman:demo-seed")
        try:
            yield
        finally:
            if demo_task:
                demo_task.cancel()
            if start_runtime:
                await rt.stop()

    expose_docs = os.getenv("BOSSMAN_EXPOSE_DOCS", "0" if os.getenv("BOSSMAN_ENV") == "production" else "1") == "1"
    app = FastAPI(
        title="BOSSman API", version=views._project_version(), lifespan=lifespan,
        docs_url="/api/docs" if expose_docs else None, redoc_url=None,
        openapi_url="/api/openapi.json" if expose_docs else None,
        description="Control plane for AI agent fleets: supervision, recovery, resource arbitration, governance.",
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=_allowed_origins(),
        allow_methods=["GET", "POST", "PUT", "DELETE"],
        allow_headers=[auth.TOKEN_HEADER, "Authorization", "Content-Type"],
    )

    # ── Error handling: no stack traces, stable error envelope ────────────────

    @app.exception_handler(HTTPException)
    async def _http_exc(_: Request, exc: HTTPException):
        code = {400: "invalid_request", 401: "unauthorized", 403: "forbidden", 404: "not_found",
                409: "conflict", 422: "invalid_request", 429: "rate_limited"}.get(exc.status_code, "error")
        return _error(exc.status_code, code, str(exc.detail))

    @app.exception_handler(RequestValidationError)
    async def _validation(_: Request, exc: RequestValidationError):
        fields = [".".join(str(x) for x in e.get("loc", [])[1:]) for e in exc.errors()][:10]
        return _error(422, "invalid_request", "Request validation failed.", fields=fields)

    @app.exception_handler(FencedError)
    async def _fenced(_: Request, exc: FencedError):
        return _error(409, "fenced", str(exc), current_epoch=exc.current)

    @app.exception_handler(LeaseError)
    async def _lease(_: Request, exc: LeaseError):
        return _error(409, "lease_conflict", str(exc))

    @app.exception_handler(OutputInvalid)
    async def _output_invalid(_: Request, exc: OutputInvalid):
        return _error(422, "output_invalid", f"Output rejected by the task's output_schema: {exc}",
                      task_status=exc.task.status.value)

    @app.exception_handler(RegistrationError)
    async def _registration(_: Request, exc: RegistrationError):
        extra = {"existing_agent_id": exc.existing.agent_id} if hasattr(exc, "existing") else {}
        return _error(exc.status, exc.code, str(exc), **extra)

    @app.exception_handler(DeadlockError)
    async def _deadlock(_: Request, exc: DeadlockError):
        return _error(409, "deadlock", str(exc), cycle=exc.cycle)

    @app.exception_handler(LockTimeout)
    async def _lock_timeout(_: Request, exc: LockTimeout):
        return _error(423, "lock_timeout", str(exc), holders=exc.holders)

    @app.exception_handler(LeaseLost)
    async def _lease_lost(_: Request, exc: LeaseLost):
        return _error(409, "lease_lost", str(exc))

    @app.exception_handler(LimitTimeout)
    async def _limit(_: Request, exc: LimitTimeout):
        return _error(429, "rate_limited", str(exc), retry_after_s=exc.retry_after_s)

    @app.exception_handler(UnknownLimit)
    async def _unknown_limit(_: Request, exc: UnknownLimit):
        return _error(404, "not_found", str(exc))

    @app.exception_handler(ApprovalError)
    async def _approval(_: Request, exc: ApprovalError):
        return _error(409, "conflict", str(exc))

    @app.exception_handler(SecretError)
    async def _secret(_: Request, exc: SecretError):
        return _error(403, "forbidden", str(exc))

    @app.exception_handler(VersionConflict)
    async def _conflict(_: Request, exc: VersionConflict):
        return _error(409, "conflict", "Concurrent update — retry.")

    @app.exception_handler(KeyError)
    async def _missing(_: Request, exc: KeyError):
        return _error(404, "not_found", str(exc.args[0] if exc.args else "Not found."))

    @app.exception_handler(ValueError)
    async def _value(_: Request, exc: ValueError):
        return _error(400, "invalid_request", str(exc)[:500])

    @app.exception_handler(Exception)
    async def _unhandled(_: Request, exc: Exception):
        try:
            from jsonschema.exceptions import SchemaError

            if isinstance(exc, SchemaError):
                return _error(400, "invalid_request", f"Invalid output_schema: {exc.message}")
        except ImportError:  # pragma: no cover
            pass
        log.exception("Unhandled API error")  # stack trace stays server-side
        return _error(500, "internal_error", "Internal server error.")

    def rt() -> BossmanRuntime:
        return rt_holder["rt"]

    app.include_router(agent_router)
    app.include_router(ops_router)

    # ── Probes / metrics ──────────────────────────────────────────────────────

    @app.get("/healthz", include_in_schema=False)
    async def healthz():
        return {"status": "ok"}

    @app.get("/readyz", include_in_schema=False)
    async def readyz():
        r = rt()
        checks: dict[str, bool] = {"runtime": r.running}
        if r.db is not None:
            checks["database"] = await r.db.ping()
        if r.settings.distributed:
            checks["redis"] = await r.backend.ping()
        ok = all(checks.values())
        return JSONResponse(status_code=200 if ok else 503, content={"ready": ok, "checks": checks})

    @app.get("/metrics", include_in_schema=False)
    async def metrics(request: Request):
        if os.getenv("BOSSMAN_METRICS_PUBLIC", "1") != "1":
            await auth.viewer(request)
        return Response(await rt().metrics.render(rt()), media_type="text/plain; version=0.0.4")

    # ── Session / health ──────────────────────────────────────────────────────

    @app.get("/api/health")
    async def health():
        return {"status": "ok", "runtime": "RUNNING" if rt().running else "STOPPED",
                "server_time": datetime.now(timezone.utc).isoformat()}

    @app.get("/api/session")
    async def session(request: Request, p: Principal | None = Depends(auth.optional_principal)):
        tier = p.tier if p else auth.PermissionTier.READ_ONLY
        return {
            "tier": tier.value,
            "role": p.role.value if p else "anonymous",
            "name": p.name if p else None,
            "namespaces": list(p.namespaces) if p else [],
            "actions_enabled": auth.actions_enabled(request),
            "public_reads": auth.public_reads(),
            "can_replay_task": tier.can_write(),
            "can_discard_task": tier.can_destroy(),
            "can_operate": bool(p and p.can_operate),
            "is_admin": bool(p and p.is_admin),
            "unsupported_actions": [],
        }

    @app.get("/api/overview")
    async def overview(_: Principal = Depends(auth.viewer)):
        return await views.overview(rt())

    # ── Agents ────────────────────────────────────────────────────────────────

    @app.get("/api/agents")
    async def agents(namespace: str | None = None, _: Principal = Depends(auth.viewer)):
        r = rt()
        items = await r.registry.all()
        return {"agents": [views.agent_view(a) for a in items
                           if namespace is None or a.info.namespace == namespace]}

    @app.get("/api/agents/{agent_id}")
    async def agent_detail(agent_id: str, _: Principal = Depends(auth.viewer)):
        r = rt()
        a = await r.registry.get(agent_id)  # type: ignore[arg-type]
        if a is None:
            raise HTTPException(404, "Agent not found.")
        amap = {x.agent_id: x for x in await r.registry.all()}
        tasks = await r.tasks.for_agent(a.agent_id)
        current = await r.tasks.get(a.current_task_id) if a.current_task_id else None
        metadata = views.redact(dict(a.info.metadata))
        incidents = [i for i in views.incident_list(r) if i["agent_id"] == agent_id]
        managed = r.managed.get_spec(a.info.managed_id) if a.info.managed_id else None
        return {
            "agent": views.agent_view(a),
            "health": views.health_view(r, a),
            "current_task": views.task_view(current, amap) if current else None,
            "tasks": [views.task_view(t, amap) for t in tasks][-50:],
            "events": [views.event_view(s, e) for s, e in r.store.query(limit=100, agent_id=agent_id)],
            "incidents": incidents,
            "recovery_attempts": [views._attempt_view(x) for x in r.engine.ledger.for_agent(a.agent_id)][-20:],
            "configuration": {
                "metadata": metadata, "permission_tier": a.info.permission_tier.value,
                "namespace": a.info.namespace, "capabilities": a.info.capabilities, "labels": a.info.labels,
                "supervisor": a.info.supervisor, "singleton_key": a.info.singleton_key,
                "managed_id": a.info.managed_id, "managed_name": managed.name if managed else None,
                "transport": a.info.transport, "epoch": a.info.epoch, "sdk": a.info.sdk,
            },
            "directives": {
                "outstanding": [d.model_dump(mode="json") for d in await r.directives.outstanding(agent_id)],
                "recent": [d.model_dump(mode="json") for d in r.directives.recent(agent_id, 20)],
            },
            "flags": await r.flags.snapshot_for(a),
            "probes": a.probe_status,
            "restart_count_in_window": r.engine._restart_count(a.agent_id),
            "restart_budget": r.engine._max_restarts,
        }

    # ── Tasks ─────────────────────────────────────────────────────────────────

    @app.get("/api/tasks")
    async def tasks(namespace: str | None = None, _: Principal = Depends(auth.viewer)):
        r = rt()
        amap = {x.agent_id: x for x in await r.registry.all()}
        items = await r.tasks.all(namespace)
        items.sort(key=lambda t: t.created_at, reverse=True)
        dlq = await r.tasks.dlq_contents(namespace)
        stats = await r.tasks.stats(namespace)
        return {
            "tasks": [views.task_view(t, amap) for t in items[:1000]],
            "dlq": [views.task_view(t, amap) for t in dlq],
            "stats": {
                "total": len(items),
                "running": stats["RUNNING"],
                "queued": stats["PENDING"] + stats["ASSIGNED"],
                "blocked": stats.get("BLOCKED", 0),
                "failed": stats["FAILED"],
                "dlq": stats["DLQ_depth"],
                "quarantined": stats.get("QUARANTINED", 0),
                "completed": stats["COMPLETED"],
                "cancelled": stats["CANCELLED"],
            },
        }

    @app.get("/api/tasks/{task_id}")
    async def task_detail(task_id: str, _: Principal = Depends(auth.viewer)):
        r = rt()
        t = await r.tasks.get(task_id)  # type: ignore[arg-type]
        if t is None:
            raise HTTPException(404, "Task not found.")
        amap = {x.agent_id: x for x in await r.registry.all()}
        detail = views.task_view(t, amap, detail=True)
        detail.update({"trace_id": t.trace_id, "depends_on": t.depends_on, "parent_task_id": t.parent_task_id,
                       "namespace": t.namespace, "attempts": t.attempts_log,
                       "quarantine_reason": t.quarantine_reason, "output_schema": t.output_schema,
                       "not_before": t.not_before.isoformat() if t.not_before else None,
                       "lease_expires_at": t.lease_expires_at.isoformat() if t.lease_expires_at else None})
        return {
            "task": detail,
            "events": [views.event_view(s, e) for s, e in r.store.query(limit=100, task_id=task_id)],
            "checkpoints": await r.checkpoints.list(task_id),
        }

    @app.post("/api/tasks/{task_id}/replay")
    async def replay_task(task_id: str, body: ActionBody, p: Principal = Depends(auth.operator)):
        r = rt()
        t = await r.tasks.replay_from_dlq(task_id)  # type: ignore[arg-type]
        if t is None:
            raise HTTPException(404, "Task is not in the dead-letter queue.")
        log.warning("AUDIT replay task=%s by=%s reason=%r", task_id, p.actor, body.reason)
        await r.audit.record("operator_action", p.actor, namespace=t.namespace,
                             data={"action": "replay_task", "task_id": task_id, "reason": body.reason})
        await r.bus.publish(BossmanEvent.create(
            EventType.TASK_CREATED, message=f"Task {task_id!r} replayed from DLQ by operator",
            task_id=t.task_id, namespace=t.namespace, trace_id=t.trace_id,
            payload={"replayed": True, "tier": p.tier.value, "reason": body.reason},
        ))
        return {"ok": True, "task": views.task_view(t)}

    @app.post("/api/tasks/{task_id}/discard")
    async def discard_task(task_id: str, body: ActionBody, p: Principal = Depends(auth.admin)):
        r = rt()
        t = await r.tasks.discard_from_dlq(task_id, reason=body.reason or "discarded by operator")  # type: ignore[arg-type]
        if t is None:
            raise HTTPException(404, "Task is not in the dead-letter queue.")
        log.warning("AUDIT discard task=%s by=%s reason=%r", task_id, p.actor, body.reason)
        await r.audit.record("operator_action", p.actor, namespace=t.namespace,
                             data={"action": "discard_task", "task_id": task_id, "reason": body.reason})
        return {"ok": True, "task": views.task_view(t)}

    # ── Incidents ─────────────────────────────────────────────────────────────

    @app.get("/api/incidents")
    async def incidents(_: Principal = Depends(auth.viewer)):
        return {"incidents": views.incident_list(rt())}

    @app.get("/api/incidents/{incident_id}")
    async def incident(incident_id: str, _: Principal = Depends(auth.viewer)):
        inc = views.incident_detail(rt(), incident_id)
        if inc is None:
            raise HTTPException(404, "Incident not found.")
        return inc

    # ── Resources / system / events ───────────────────────────────────────────

    @app.get("/api/resources")
    async def resources(_: Principal = Depends(auth.viewer)):
        r = rt()
        return await views.resources_view_full(r, await r.registry.all())

    @app.get("/api/system")
    async def system(_: Principal = Depends(auth.viewer)):
        return await views.system_view_full(rt())

    @app.get("/api/cluster")
    async def cluster(_: Principal = Depends(auth.viewer)):
        return await rt().cluster_info()

    @app.get("/api/events")
    async def events(
        limit: int = Query(100, ge=1, le=500),
        since_seq: int = Query(0, ge=0),
        agent_id: str | None = Query(None, max_length=64),
        event_type: str | None = Query(None, max_length=64),
        _: Principal = Depends(auth.viewer),
    ):
        r = rt()
        rows = r.store.query(limit=limit, since_seq=since_seq, agent_id=agent_id, event_type=event_type)
        return {"events": [views.event_view(s, e) for s, e in rows], "last_seq": r.store.last_seq}

    # ── WebSocket ─────────────────────────────────────────────────────────────

    @app.websocket("/ws")
    async def ws_endpoint(ws: WebSocket):
        origin = ws.headers.get("origin")
        if origin is not None and origin not in _allowed_origins() and not _same_origin(ws, origin):
            await ws.close(code=1008)  # policy violation
            return
        if not auth.public_reads():
            token = ws.query_params.get("token") or ws.headers.get(auth.TOKEN_HEADER.lower())
            p = await rt().keys.authenticate(token)
            if p is None or not p.can_read:
                await ws.close(code=1008)
                return
        await ws.accept()
        r = rt()
        q = r.store.subscribe()
        keepalive = float(os.getenv("WS_HEARTBEAT_INTERVAL", "5"))

        async def drain_client() -> None:
            try:
                while True:
                    await ws.receive_text()  # ignore content; detects disconnect
            except (WebSocketDisconnect, RuntimeError):
                return

        reader = asyncio.create_task(drain_client())
        try:
            await ws.send_json({"type": "hello", "last_seq": r.store.last_seq, "keepalive_s": keepalive,
                                "server_time": datetime.now(timezone.utc).isoformat()})
            while not reader.done():
                try:
                    item = await asyncio.wait_for(q.get(), timeout=keepalive)
                except asyncio.TimeoutError:
                    await ws.send_json({"type": "tick", "last_seq": r.store.last_seq,
                                        "server_time": datetime.now(timezone.utc).isoformat()})
                    continue
                if item is None:  # dropped as slow consumer
                    await ws.close(code=1013)
                    break
                seq, ev = item
                await ws.send_json({"type": "event", "event": views.event_view(seq, ev)})
        except (WebSocketDisconnect, RuntimeError):
            pass
        finally:
            reader.cancel()
            r.store.unsubscribe(q)

    # ── Dashboard (single-port deployment) ────────────────────────────────────

    static = _static_dir()
    if static is not None:
        root = static.resolve()

        @app.get("/{path:path}", include_in_schema=False)
        async def dashboard(path: str):
            if path.startswith(("api/", "v1/", "ws")):
                raise HTTPException(404, "Not found.")
            target = (root / path).resolve()
            if path and target.is_file() and root in target.parents:
                return FileResponse(target)
            return FileResponse(root / "index.html")  # SPA fallback

    return app


def _same_origin(ws: WebSocket, origin: str) -> bool:
    """The bundled dashboard is served from the API's own origin."""
    host = ws.headers.get("host")
    return bool(host) and origin.split("://", 1)[-1] == host


app = create_app()
