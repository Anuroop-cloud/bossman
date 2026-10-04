"""
api.app
───────
FastAPI application: REST + WebSocket for the BOSSman dashboard.

Security posture
  * Reads are unauthenticated (bind to localhost / put behind your proxy).
  * Every mutation is authorised server-side via api.auth (PermissionTier).
  * CORS + WebSocket Origin are restricted to BOSSMAN_CORS_ORIGINS.
  * Errors never leak stack traces; unexpected errors return a generic 500.
  * Responses are JSON only; the UI renders text nodes, never raw HTML.
"""

from __future__ import annotations

import asyncio
import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any

from fastapi import Depends, FastAPI, HTTPException, Query, Request, WebSocket, WebSocketDisconnect
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from api import auth, views
from api.runtime import BossmanRuntime
from contracts.agent_state import PermissionTier
from contracts.events import BossmanEvent, EventType
from core.task_manager import TaskStatus

log = logging.getLogger("bossman.api")


class ActionBody(BaseModel):
    reason: str = Field(default="", max_length=200)


def _allowed_origins() -> list[str]:
    raw = os.getenv("BOSSMAN_CORS_ORIGINS", "http://localhost:5173,http://127.0.0.1:5173")
    return [o.strip() for o in raw.split(",") if o.strip()]


def _error(status: int, code: str, message: str) -> JSONResponse:
    return JSONResponse(status_code=status, content={"error": {"code": code, "message": message}})


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
            from api.demo import run_demo

            demo_task = asyncio.create_task(run_demo(rt), name="bossman:demo-seed")
        try:
            yield
        finally:
            if demo_task:
                demo_task.cancel()
            if start_runtime:
                await rt.stop()

    app = FastAPI(title="BOSSman API", version=views._project_version(), lifespan=lifespan,
                  docs_url=None, redoc_url=None, openapi_url=None)

    app.add_middleware(
        CORSMiddleware,
        allow_origins=_allowed_origins(),
        allow_methods=["GET", "POST"],
        allow_headers=[auth.TOKEN_HEADER, "Content-Type"],
    )

    # ── Error handling: no stack traces, stable error envelope ────────────────

    @app.exception_handler(HTTPException)
    async def _http_exc(_: Request, exc: HTTPException):
        code = {401: "unauthorized", 403: "forbidden", 404: "not_found", 409: "conflict"}.get(
            exc.status_code, "error"
        )
        return _error(exc.status_code, code, str(exc.detail))

    @app.exception_handler(RequestValidationError)
    async def _validation(_: Request, __: RequestValidationError):
        return _error(422, "invalid_request", "Request validation failed.")

    @app.exception_handler(Exception)
    async def _unhandled(_: Request, exc: Exception):
        log.exception("Unhandled API error")  # stack trace stays server-side
        return _error(500, "internal_error", "Internal server error.")

    def rt() -> BossmanRuntime:
        return rt_holder["rt"]

    # ── Session / health ──────────────────────────────────────────────────────

    @app.get("/api/health")
    async def health():
        return {"status": "ok", "runtime": "RUNNING" if rt().running else "STOPPED",
                "server_time": datetime.now(timezone.utc).isoformat()}

    @app.get("/api/session")
    async def session(tier: PermissionTier = Depends(auth.caller_tier)):
        return {
            "tier": tier.value,
            "actions_enabled": auth.actions_enabled(),
            "can_replay_task": tier.can_write(),
            "can_discard_task": tier.can_destroy(),
            "unsupported_actions": ["restart_agent", "release_resource", "resolve_incident", "configure"],
        }

    @app.get("/api/overview")
    async def overview():
        return await views.overview(rt())

    # ── Agents ────────────────────────────────────────────────────────────────

    @app.get("/api/agents")
    async def agents():
        r = rt()
        items = await r.registry.all()
        return {"agents": [views.agent_view(a) for a in items]}

    @app.get("/api/agents/{agent_id}")
    async def agent_detail(agent_id: str):
        r = rt()
        a = await r.registry.get(agent_id)  # type: ignore[arg-type]
        if a is None:
            raise HTTPException(404, "Agent not found.")
        amap = {x.agent_id: x for x in await r.registry.all()}
        tasks = await r.tasks.for_agent(a.agent_id)
        current = await r.tasks.get(a.current_task_id) if a.current_task_id else None
        metadata = views.redact(dict(a.info.metadata))
        incidents = [i for i in views.incident_list(r) if i["agent_id"] == agent_id]
        return {
            "agent": views.agent_view(a),
            "health": views.health_view(r, a),
            "current_task": views.task_view(current, amap) if current else None,
            "tasks": [views.task_view(t, amap) for t in tasks][-50:],
            "events": [views.event_view(s, e) for s, e in r.store.query(limit=100, agent_id=agent_id)],
            "incidents": incidents,
            "recovery_attempts": [views._attempt_view(x) for x in r.engine.ledger.for_agent(a.agent_id)][-20:],
            "configuration": {"metadata": metadata, "permission_tier": a.info.permission_tier.value},
            "restart_count_in_window": r.engine._restart_count(a.agent_id),
            "restart_budget": r.engine._max_restarts,
        }

    # ── Tasks ─────────────────────────────────────────────────────────────────

    @app.get("/api/tasks")
    async def tasks():
        r = rt()
        amap = {x.agent_id: x for x in await r.registry.all()}
        items = await r.tasks.all()
        items.sort(key=lambda t: t.created_at, reverse=True)
        dlq = await r.tasks.dlq_contents()
        stats = await r.tasks.stats()
        return {
            "tasks": [views.task_view(t, amap) for t in items],
            "dlq": [views.task_view(t, amap) for t in dlq],
            "stats": {
                "total": len(items),
                "running": stats["RUNNING"],
                "queued": stats["PENDING"] + stats["ASSIGNED"],
                "failed": stats["FAILED"],
                "dlq": stats["DLQ_depth"],
                "completed": stats["COMPLETED"],
                "cancelled": stats["CANCELLED"],
            },
        }

    @app.get("/api/tasks/{task_id}")
    async def task_detail(task_id: str):
        r = rt()
        t = await r.tasks.get(task_id)  # type: ignore[arg-type]
        if t is None:
            raise HTTPException(404, "Task not found.")
        amap = {x.agent_id: x for x in await r.registry.all()}
        return {
            "task": views.task_view(t, amap, detail=True),
            "events": [views.event_view(s, e) for s, e in r.store.query(limit=100, task_id=task_id)],
        }

    @app.post("/api/tasks/{task_id}/replay")
    async def replay_task(task_id: str, body: ActionBody, tier: PermissionTier = Depends(auth.require_write)):
        r = rt()
        t = await r.tasks.replay_from_dlq(task_id)  # type: ignore[arg-type]
        if t is None:
            raise HTTPException(404, "Task is not in the dead-letter queue.")
        log.warning("AUDIT replay task=%s tier=%s reason=%r", task_id, tier.value, body.reason)
        await r.bus.publish(BossmanEvent.create(
            EventType.TASK_CREATED, message=f"Task {task_id!r} replayed from DLQ by operator",
            task_id=t.task_id, payload={"replayed": True, "tier": tier.value, "reason": body.reason},
        ))
        return {"ok": True, "task": views.task_view(t)}

    @app.post("/api/tasks/{task_id}/discard")
    async def discard_task(task_id: str, body: ActionBody, tier: PermissionTier = Depends(auth.require_privileged)):
        r = rt()
        t = await r.tasks.discard_from_dlq(task_id, reason=body.reason or "discarded by operator")  # type: ignore[arg-type]
        if t is None:
            raise HTTPException(404, "Task is not in the dead-letter queue.")
        log.warning("AUDIT discard task=%s tier=%s reason=%r", task_id, tier.value, body.reason)
        return {"ok": True, "task": views.task_view(t)}

    # ── Incidents ─────────────────────────────────────────────────────────────

    @app.get("/api/incidents")
    async def incidents():
        return {"incidents": views.incident_list(rt())}

    @app.get("/api/incidents/{incident_id}")
    async def incident(incident_id: str):
        inc = views.incident_detail(rt(), incident_id)
        if inc is None:
            raise HTTPException(404, "Incident not found.")
        return inc

    # ── Resources / system / events ───────────────────────────────────────────

    @app.get("/api/resources")
    async def resources():
        r = rt()
        return views.resources_view(r, await r.registry.all())

    @app.get("/api/system")
    async def system():
        return views.system_view(rt())

    @app.get("/api/events")
    async def events(
        limit: int = Query(100, ge=1, le=500),
        since_seq: int = Query(0, ge=0),
        agent_id: str | None = Query(None, max_length=64),
        event_type: str | None = Query(None, max_length=64),
    ):
        r = rt()
        rows = r.store.query(limit=limit, since_seq=since_seq, agent_id=agent_id, event_type=event_type)
        return {"events": [views.event_view(s, e) for s, e in rows], "last_seq": r.store.last_seq}

    # ── WebSocket ─────────────────────────────────────────────────────────────

    @app.websocket("/ws")
    async def ws_endpoint(ws: WebSocket):
        origin = ws.headers.get("origin")
        if origin is not None and origin not in _allowed_origins():
            await ws.close(code=1008)  # policy violation
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

    return app


app = create_app()
