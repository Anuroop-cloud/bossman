"""
api.views
─────────
Pure read-side projections of existing BOSSman state into JSON-safe dicts.
No business logic: counts, groupings and graph shaping only.
"""

from __future__ import annotations

import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import psutil

from bossman_server.api import auth
from bossman_server.api.runtime import BossmanRuntime
from bossman_server.contracts.agent_state import AgentState, AgentStatus
from bossman_server.contracts.events import BossmanEvent, EventSeverity, EventType
from bossman_server.contracts.recovery_ledger import RecoveryAttempt, RecoveryOutcome
from bossman_server.core.task_manager import Task, TaskStatus

# ── Sanitising ────────────────────────────────────────────────────────────────

_SECRET_KEY = re.compile(
    r"(secret|password|passwd|api[_-]?key|authorization|credential|bearer|private[_-]?key"
    r"|access[_-]?token|auth[_-]?token|refresh[_-]?token)",
    re.IGNORECASE,
)
_MAX_STR = 2000
_MAX_DEPTH = 6


def redact(value: Any, _depth: int = 0) -> Any:
    """Recursively strip secret-looking keys, truncate long strings, cap depth."""
    if _depth > _MAX_DEPTH:
        return "…"
    if isinstance(value, dict):
        return {
            str(k): ("[redacted]" if _SECRET_KEY.search(str(k)) else redact(v, _depth + 1))
            for k, v in value.items()
        }
    if isinstance(value, (list, tuple, set, frozenset)):
        return [redact(v, _depth + 1) for v in list(value)[:200]]
    if isinstance(value, str):
        return value if len(value) <= _MAX_STR else value[:_MAX_STR] + "…"
    if isinstance(value, (int, float, bool)) or value is None:
        return value
    if isinstance(value, datetime):
        return value.isoformat()
    if hasattr(value, "value"):  # Enum
        return value.value
    return str(value)[:_MAX_STR]


def _iso(dt: datetime | None) -> str | None:
    return dt.isoformat() if dt else None


def _now() -> datetime:
    return datetime.now(timezone.utc)


# ── Events ────────────────────────────────────────────────────────────────────


def event_view(seq: int, ev: BossmanEvent) -> dict[str, Any]:
    return {
        "seq": seq,
        "event_id": ev.event_id,
        "event_type": ev.event_type.value,
        "severity": ev.severity.value,
        "occurred_at": ev.occurred_at.isoformat(),
        "agent_id": ev.agent_id,
        "task_id": ev.task_id,
        "resource_name": ev.resource_name,
        "message": ev.message[:500],
        "payload": redact(ev.payload),
    }


# ── Agents ────────────────────────────────────────────────────────────────────


def agent_view(a: AgentState) -> dict[str, Any]:
    ru = a.resource_usage
    now = _now()
    return {
        "agent_id": a.agent_id,
        "name": a.info.name,
        "role": a.info.role,
        "permission_tier": a.info.permission_tier.value,
        "status": a.status.value,
        "is_healthy": a.is_healthy,
        "is_alive": a.is_alive,
        "current_task_id": a.current_task_id,
        "current_step": a.current_step,
        "heartbeat_age_s": round(a.heartbeat_age_seconds, 1),
        "uptime_s": round((now - a.started_at).total_seconds(), 1),
        "started_at": _iso(a.started_at),
        "registered_at": _iso(a.info.registered_at),
        "last_activity": _iso(a.last_activity),
        "consecutive_failures": a.consecutive_failures,
        "total_tasks_completed": a.total_tasks_completed,
        "total_tasks_failed": a.total_tasks_failed,
        "tokens_used": ru.tokens_used,
        "tokens_budget": ru.tokens_budget,
        "token_fraction": round(ru.token_budget_fraction, 4),
        "context_utilisation": ru.context_utilisation,
        "api_calls_total": ru.api_calls_total,
        "api_calls_per_minute": ru.api_calls_per_minute,
        "resources_held": list(ru.resources_held),
        "resources_waiting": list(ru.resources_waiting),
        "is_deadlock_suspect": a.is_deadlock_suspect,
        "last_error": a.last_error[:500] if a.last_error else None,
        "last_error_at": _iso(a.last_error_at),
    }


def health_view(rt: BossmanRuntime, a: AgentState) -> dict[str, Any]:
    return redact(rt.checker.check(a).as_dict())


# ── Tasks ─────────────────────────────────────────────────────────────────────

_STAGE_PCT = {
    TaskStatus.PENDING: 0,
    TaskStatus.ASSIGNED: 25,
    TaskStatus.RUNNING: 60,
    TaskStatus.COMPLETED: 100,
}


def task_view(t: Task, agents: dict[str, AgentState] | None = None, *, detail: bool = False) -> dict[str, Any]:
    step = None
    if agents and t.assigned_to and t.status == TaskStatus.RUNNING:
        ag = agents.get(t.assigned_to)
        if ag and ag.current_task_id == t.task_id:
            step = ag.current_step
    d: dict[str, Any] = {
        "task_id": t.task_id,
        "title": t.title,
        "description": t.description[:1000],
        "priority": t.priority.name,
        "priority_value": int(t.priority.value),
        "status": t.status.value,
        "assigned_to": t.assigned_to,
        "required_role": t.required_role,
        "attempt_count": t.attempt_count,
        "max_attempts": t.max_attempts,
        "last_error": t.last_error[:500] if t.last_error else None,
        "created_at": _iso(t.created_at),
        "assigned_at": _iso(t.assigned_at),
        "started_at": _iso(t.started_at),
        "completed_at": _iso(t.completed_at),
        "deadline": _iso(t.deadline),
        "dlq_reason": t.dlq_reason,
        "dlq_at": _iso(t.dlq_at),
        "current_step": step,
        # The backend tracks lifecycle stage, not fractional completion.
        "progress_pct": _STAGE_PCT.get(t.status),
        "progress_basis": "lifecycle-stage",
    }
    if detail:
        d["input_data"] = redact(t.input_data)
        d["output_data"] = redact(t.output_data)
    return d


# ── Resources / graph ─────────────────────────────────────────────────────────


def _find_cycles(wait_for: dict[str, set[str]]) -> list[list[str]]:
    """Cycles in an agent→agent wait-for graph (display-level analysis only)."""
    cycles: list[list[str]] = []
    seen_sets: set[frozenset[str]] = set()
    WHITE, GREY, BLACK = 0, 1, 2
    color: dict[str, int] = {}

    def dfs(node: str, stack: list[str]) -> None:
        color[node] = GREY
        stack.append(node)
        for nxt in wait_for.get(node, ()):  # noqa: B007
            if color.get(nxt, WHITE) == GREY:
                cyc = stack[stack.index(nxt):]
                key = frozenset(cyc)
                if key not in seen_sets:
                    seen_sets.add(key)
                    cycles.append(list(cyc))
            elif color.get(nxt, WHITE) == WHITE:
                dfs(nxt, stack)
        stack.pop()
        color[node] = BLACK

    for n in list(wait_for):
        if color.get(n, WHITE) == WHITE:
            dfs(n, [])
    return cycles


def resource_graph(rt: BossmanRuntime, agents: list[AgentState]) -> dict[str, Any]:
    named = rt.resources.mediator.snapshot()
    nodes: dict[str, dict[str, Any]] = {}
    edges: dict[tuple[str, str, str], dict[str, Any]] = {}
    amap = {a.agent_id: a for a in agents}

    def agent_node(aid: str) -> None:
        nid = f"agent:{aid}"
        if nid not in nodes:
            a = amap.get(aid)
            nodes[nid] = {
                "id": nid, "kind": "agent", "ref": aid,
                "label": a.info.name if a else aid,
                "status": a.status.value if a else "UNKNOWN",
            }

    def res_node(name: str) -> None:
        nid = f"resource:{name}"
        if nid not in nodes:
            r = named.get(name, {})
            nodes[nid] = {
                "id": nid, "kind": "resource", "ref": name, "label": name,
                "locked": bool(r.get("currently_locked")),
                "waiters": len(r.get("waiters", [])),
                "contention_count": r.get("contention_count", 0),
            }

    def edge(kind: str, aid: str, res: str, source: str) -> None:
        agent_node(aid)
        res_node(res)
        key = (kind, aid, res)
        if key in edges:
            if source not in edges[key]["sources"]:
                edges[key]["sources"].append(source)
            return
        edges[key] = {
            "id": f"{kind}:{aid}:{res}", "kind": kind,
            "source": f"agent:{aid}", "target": f"resource:{res}",
            "sources": [source],
        }

    holder_of: dict[str, str] = {}
    for name, r in named.items():
        res_node(name)
        if r.get("holder"):
            holder_of[name] = r["holder"]
            edge("holds", r["holder"], name, "mediator")
        for w in r.get("waiters", []):
            edge("waits", w, name, "mediator")
    for a in agents:
        source = "lease" if a.info.transport == "remote" else "agent-reported"
        for name in a.resource_usage.resources_held:
            holder_of.setdefault(name, a.agent_id)
            edge("holds", a.agent_id, name, source)
        for name in a.resource_usage.resources_waiting:
            edge("waits", a.agent_id, name, source)

    wait_for: dict[str, set[str]] = {}
    for (kind, aid, res) in edges:
        if kind == "waits" and res in holder_of and holder_of[res] != aid:
            wait_for.setdefault(aid, set()).add(holder_of[res])
    cycles = _find_cycles(wait_for)
    in_cycle = {n for c in cycles for n in c}
    for n in nodes.values():
        n["suspected_deadlock"] = n["kind"] == "agent" and n["ref"] in in_cycle
    for e in edges.values():
        aid = e["source"].split(":", 1)[1]
        res = e["target"].split(":", 1)[1]
        holder = holder_of.get(res)
        e["suspected_deadlock"] = (
            aid in in_cycle and (
                (e["kind"] == "waits" and holder in in_cycle)
                or (e["kind"] == "holds")
            )
        )
    return {
        "nodes": list(nodes.values()),
        "edges": list(edges.values()),
        "wait_for": [{"waiter": a, "holder": h} for a, hs in wait_for.items() for h in hs],
        "cycles": cycles,
    }


def resources_view(rt: BossmanRuntime, agents: list[AgentState]) -> dict[str, Any]:
    snap = rt.resources.snapshot()
    sem = snap["llm_semaphore"]
    bucket = snap["token_bucket"]
    named = []
    for name, r in snap["named_resources"].items():
        named.append({
            "name": name,
            "holder": r["holder"],
            "waiters": r["waiters"],
            "currently_locked": r["currently_locked"],
            "acquire_count": r["acquire_count"],
            "contention_count": r["contention_count"],
            "total_hold_seconds": r["total_hold_seconds"],
        })
    cap = sem.get("max_concurrent") or 1
    recent_deadlock = [
        event_view(s, e)
        for s, e in rt.store.query(limit=20, event_type=EventType.DEADLOCK_SUSPECTED.value)
    ]
    return {
        "llm_semaphore": {**sem, "utilisation": round(sem["current_holders"] / cap, 4),
                          "available": cap - sem["current_holders"]},
        "token_bucket": {**bucket, "utilisation": round(1 - bucket["fill_fraction"], 4)},
        "named_resources": named,
        "graph": resource_graph(rt, agents),
        "recent_deadlock_events": recent_deadlock,
        "capabilities": {
            # Honest capability flags so the UI never offers unsupported actions.
            "semaphore_holder_ids": False,
            "release_resource": True,
            "database_pool": False,
        },
    }


async def resources_view_full(rt: BossmanRuntime, agents: list[AgentState]) -> dict[str, Any]:
    """resources_view + fleet-wide leases, limits and shared circuits."""
    out = resources_view(rt, agents)
    locks = await rt.locks.snapshot()
    out["leases"] = locks["leases"]
    out["lock_waits"] = locks["waits"]
    out["lock_stats"] = locks["stats"]
    out["limits"] = await rt.limits.snapshot()
    out["circuits"] = await rt.circuits.snapshot()
    seen = {r["name"] for r in out["named_resources"]}
    for lease in locks["leases"]:
        for name in lease["resources"]:
            if name not in seen:
                seen.add(name)
                waiters = [w["agent_id"] for w in locks["waits"] if name in w["resources"]]
                out["named_resources"].append({
                    "name": name, "holder": lease["agent_id"], "waiters": waiters, "currently_locked": True,
                    "acquire_count": None, "contention_count": len(waiters), "total_hold_seconds": None,
                    "lease_id": lease["lease_id"], "remaining_s": lease["remaining_s"],
                })
    return out


# ── Incidents ─────────────────────────────────────────────────────────────────

_SEV_RANK = {
    EventSeverity.DEBUG: 0, EventSeverity.INFO: 1, EventSeverity.WARNING: 2,
    EventSeverity.ERROR: 3, EventSeverity.CRITICAL: 4,
}
STAGES = ["DETECTED", "DIAGNOSED", "RECOVERY", "EVALUATION", "RESOLVED"]


def _attempt_view(a: RecoveryAttempt, *, detail: bool = False) -> dict[str, Any]:
    d = {
        "attempt_id": a.attempt_id,
        "incident_id": a.incident_id,
        "attempt_number": a.attempt_number,
        "agent_id": a.agent_id,
        "task_id": a.task_id,
        "failure_type": a.failure_type.value,
        "failure_detail": a.failure_detail[:500],
        "strategy": a.strategy.value,
        "outcome": a.outcome.value,
        "recovery_started_at": _iso(a.recovery_started_at),
        "recovery_completed_at": _iso(a.recovery_completed_at),
        "duration_s": a.duration_seconds,
        "verified_by_evaluator": a.verified_by_evaluator,
        "evaluator_notes": a.evaluator_notes,
        "recovery_notes": a.recovery_notes[:500] if a.recovery_notes else None,
    }
    if detail:
        d["telemetry_snapshot"] = redact(a.telemetry_snapshot)
    return d


def _group_incident_events(rt: BossmanRuntime) -> dict[str, list[tuple[int, BossmanEvent]]]:
    groups: dict[str, list[tuple[int, BossmanEvent]]] = {}
    for seq, ev in rt.store.query(limit=10**9):
        iid = ev.payload.get("incident_id")
        if iid:
            groups.setdefault(iid, []).append((seq, ev))
    for g in groups.values():
        g.sort(key=lambda p: p[0])  # oldest first
    return groups


def _build_incident(
    rt: BossmanRuntime,
    incident_id: str,
    evs: list[tuple[int, BossmanEvent]],
    *,
    detail: bool,
) -> dict[str, Any] | None:
    failure = next((f for f in rt.detector.detected_failures if f.incident_id == incident_id), None)
    attempts = rt.engine.ledger.for_incident(incident_id)
    if failure is None and not attempts and not evs:
        return None
    diag = rt.diagnostician.by_incident.get(incident_id)

    def first(t: EventType) -> BossmanEvent | None:
        return next((e for _, e in evs if e.event_type == t), None)

    def last(*ts: EventType) -> BossmanEvent | None:
        return next((e for _, e in reversed(evs) if e.event_type in ts), None)

    ev_detected = first(EventType.FAILURE_DETECTED)
    ev_chosen = first(EventType.RECOVERY_STRATEGY_CHOSEN)
    ev_started = first(EventType.RECOVERY_STARTED)
    ev_eval = last(EventType.EVALUATION_PASSED, EventType.EVALUATION_FAILED)
    ev_escalated = last(EventType.HUMAN_GATE_REQUIRED, EventType.RECOVERY_ESCALATED)
    ev_resolved = last(EventType.INCIDENT_RESOLVED)

    agent_id = (failure.agent_id if failure else None) or (attempts[0].agent_id if attempts else None)
    ftype = failure.failure_type.value if failure else (attempts[0].failure_type.value if attempts else "UNKNOWN")
    detail_txt = failure.detail if failure else (attempts[0].failure_detail if attempts else "")
    detected_at = (
        ev_detected.occurred_at if ev_detected
        else (attempts[0].failure_detected_at if attempts else None)
    )

    latest = attempts[-1] if attempts else None
    eval_passed = bool(ev_eval and ev_eval.event_type == EventType.EVALUATION_PASSED) or (
        latest is not None and latest.outcome == RecoveryOutcome.SUCCESS and latest.verified_by_evaluator
    ) or ev_resolved is not None
    eval_failed = bool(ev_eval and ev_eval.event_type == EventType.EVALUATION_FAILED) or (
        latest is not None and latest.outcome == RecoveryOutcome.FAILURE and latest.verified_by_evaluator
    )
    escalated = ev_escalated is not None or (latest is not None and latest.outcome == RecoveryOutcome.ESCALATED)

    if eval_passed:
        state = "resolved"
    elif escalated:
        state = "escalated"
    elif eval_failed or (latest is not None and latest.outcome == RecoveryOutcome.FAILURE):
        state = "failed"
    else:
        state = "active"

    recovery_started = bool(attempts or ev_started)
    stages = [
        {"stage": "DETECTED", "status": "done", "at": _iso(detected_at)},
        {
            "stage": "DIAGNOSED",
            "status": "done" if (diag or ev_chosen) else "active",
            "at": _iso(ev_chosen.occurred_at) if ev_chosen else None,
        },
        {
            "stage": "RECOVERY",
            "status": (
                "pending" if not (diag or ev_chosen) and not recovery_started
                else "failed" if state in ("failed", "escalated") and not eval_passed and ev_eval is None
                else "done" if recovery_started else "active"
            ),
            "at": _iso(ev_started.occurred_at) if ev_started else (_iso(latest.recovery_started_at) if latest else None),
        },
        {
            "stage": "EVALUATION",
            "status": (
                "done" if eval_passed
                else "failed" if eval_failed
                else "active" if recovery_started and state == "active"
                else "pending"
            ),
            "at": _iso(ev_eval.occurred_at) if ev_eval else None,
        },
        {
            "stage": "RESOLVED",
            "status": "done" if eval_passed else "pending",
            "at": _iso(ev_eval.occurred_at) if eval_passed and ev_eval else None,
        },
    ]

    sev = max((e.severity for _, e in evs), key=lambda s: _SEV_RANK[s], default=EventSeverity.ERROR)
    if escalated:
        sev = EventSeverity.CRITICAL

    strategy = (
        diag.recommended_strategy.value if diag
        else (latest.strategy.value if latest else (failure.recommended_strategy.value if failure else None))
    )
    out: dict[str, Any] = {
        "incident_id": incident_id,
        "agent_id": agent_id,
        "failure_type": ftype,
        "detail": detail_txt[:500],
        "state": state,
        "severity": sev.value,
        "is_critical": sev == EventSeverity.CRITICAL,
        "stage": next((s["stage"] for s in reversed(stages) if s["status"] in ("done", "active", "failed")), "DETECTED"),
        "stages": stages,
        "detected_at": _iso(detected_at),
        "updated_at": evs[-1][1].occurred_at.isoformat() if evs else _iso(detected_at),
        "diagnosis_summary": diag.root_cause[:300] if diag else None,
        "confidence": diag.confidence if diag else None,
        "recovery_strategy": strategy,
        "attempt_count": len(attempts),
        "resolved_by": ev_resolved.payload.get("by") if ev_resolved else None,
    }
    if detail:
        out["diagnosis"] = (
            {
                "root_cause": diag.root_cause,
                "confidence": diag.confidence,
                "rule_matched": diag.rule_matched,
                "needs_llm_review": diag.needs_llm_review,
                "recommended_strategy": diag.recommended_strategy.value,
                "evidence": redact(diag.evidence),
            }
            if diag else None
        )
        out["attempts"] = [_attempt_view(a, detail=True) for a in attempts]
        out["evaluation"] = (
            {
                "passed": ev_eval.event_type == EventType.EVALUATION_PASSED,
                "at": ev_eval.occurred_at.isoformat(),
                "notes": ev_eval.payload.get("notes"),
                "outcome": ev_eval.payload.get("outcome"),
                "health": redact(ev_eval.payload.get("health_summary")),
            }
            if ev_eval else None
        )
        out["timeline"] = [event_view(s, e) for s, e in evs]
        agent = rt.registry._agents.get(agent_id) if agent_id else None  # read-only peek
        res_events = (
            [event_view(s, e) for s, e in rt.store.query(limit=20, agent_id=agent_id)
             if e.resource_name]
            if agent_id else []
        )
        out["resource_impact"] = {
            "held": list(agent.resource_usage.resources_held) if agent else [],
            "waiting": list(agent.resource_usage.resources_waiting) if agent else [],
            "recent_resource_events": res_events,
        }
        out["raw"] = {
            "failure": {
                "failure_type": ftype,
                "recommended_strategy": failure.recommended_strategy.value if failure else None,
                "telemetry": redact(failure.telemetry) if failure else None,
            },
            "events": [redact(e.model_dump(mode="json")) for _, e in evs],
        }
    return out


def incident_list(rt: BossmanRuntime) -> list[dict[str, Any]]:
    groups = _group_incident_events(rt)
    ids: list[str] = []
    for f in rt.detector.detected_failures:
        ids.append(f.incident_id)
    for a in rt.engine.ledger.entries:
        if a.incident_id not in ids:
            ids.append(a.incident_id)
    for iid in groups:
        if iid not in ids:
            ids.append(iid)
    out = [i for iid in ids if (i := _build_incident(rt, iid, groups.get(iid, []), detail=False))]
    out.sort(key=lambda i: i["detected_at"] or "", reverse=True)
    return out


def incident_detail(rt: BossmanRuntime, incident_id: str) -> dict[str, Any] | None:
    groups = _group_incident_events(rt)
    return _build_incident(rt, incident_id, groups.get(incident_id, []), detail=True)


# ── Overview / system ─────────────────────────────────────────────────────────


def _project_version() -> str:
    try:
        import tomllib

        p = Path(__file__).resolve().parent.parent / "pyproject.toml"
        return tomllib.loads(p.read_text(encoding="utf-8"))["project"]["version"]
    except Exception:
        return "unknown"


def overview_status(agents: list[AgentState], incidents: list[dict], dlq: int, rt: BossmanRuntime) -> str:
    active = [i for i in incidents if i["state"] in ("active", "escalated", "failed")]
    if any(i["is_critical"] for i in active):
        return "CRITICAL"
    if not rt.running or active or dlq or any(not a.is_healthy for a in agents):
        return "DEGRADED"
    return "HEALTHY"


async def overview(rt: BossmanRuntime) -> dict[str, Any]:
    agents = await rt.registry.all()
    tasks = await rt.tasks.all()
    tstats = await rt.tasks.stats()
    incidents = incident_list(rt)
    res = resources_view(rt, agents)
    amap = {a.agent_id: a for a in agents}

    by_status: dict[str, int] = {}
    health_dist = {"PASS": 0, "WARN": 0, "FAIL": 0}
    for a in agents:
        by_status[a.status.value] = by_status.get(a.status.value, 0) + 1
        health_dist[rt.checker.check(a).overall_status.value] += 1

    active_inc = [i for i in incidents if i["state"] in ("active", "escalated", "failed")]
    proc = psutil.Process()
    return {
        "status": overview_status(agents, incidents, tstats["DLQ_depth"], rt),
        "server_time": _now().isoformat(),
        "agents": {"total": len(agents), "by_status": by_status, "health": health_dist},
        "tasks": {
            "total": len(tasks),
            "running": tstats["RUNNING"],
            "queued": tstats["PENDING"] + tstats["ASSIGNED"],
            "failed": tstats["FAILED"],
            "dlq": tstats["DLQ_depth"],
            "completed": tstats["COMPLETED"],
        },
        "resources": {
            "llm_utilisation": res["llm_semaphore"]["utilisation"],
            "token_bucket_fill": res["token_bucket"]["fill_fraction"],
            "contended": sum(1 for r in res["named_resources"] if r["waiters"] or r["contention_count"]),
            "suspected_deadlocks": len(res["graph"]["cycles"]),
        },
        "incidents": {
            "total": len(incidents),
            "active": len(active_inc),
            "critical": sum(1 for i in active_inc if i["is_critical"]),
            "resolved": sum(1 for i in incidents if i["state"] == "resolved"),
            "recent": incidents[:6],
        },
        "metrics": {
            "events_per_minute": rt.store.events_per_minute(),
            "events_published": rt.bus.stats["published"],
            "cpu_percent": psutil.cpu_percent(interval=None),
            "process_rss_mb": round(proc.memory_info().rss / 1_048_576, 1),
            "ws_clients": rt.store.subscriber_count,
            "uptime_s": round(time.monotonic() - rt.started_monotonic, 1),
        },
        "topology": topology(rt, agents, amap),
        "last_event_seq": rt.store.last_seq,
        "governance": {
            "approvals_pending": await rt.approvals.pending_count(),
            "circuits_open": sum(1 for c in await rt.circuits.snapshot() if c["state"] == "OPEN"),
            "managed": len(rt.managed.status()),
            "leases": len((await rt.locks.snapshot())["leases"]),
        },
    }


def topology(rt: BossmanRuntime, agents: list[AgentState], amap: dict[str, AgentState]) -> dict[str, Any]:
    g = resource_graph(rt, agents)
    nodes = [{"id": "bossman", "kind": "supervisor", "ref": "bossman", "label": "BOSSman",
              "status": "RUNNING" if rt.running else "STOPPED"}]
    edges: list[dict[str, Any]] = []
    present = {n["id"] for n in g["nodes"]}
    for a in agents:
        nid = f"agent:{a.agent_id}"
        nodes.append({"id": nid, "kind": "agent", "ref": a.agent_id, "label": a.info.name,
                      "status": a.status.value, "role": a.info.role,
                      "suspected_deadlock": a.is_deadlock_suspect})
        edges.append({"id": f"sup:{a.agent_id}", "kind": "supervises", "source": "bossman", "target": nid,
                      "sources": ["registry"], "suspected_deadlock": False})
        if a.current_task_id:
            tid = f"task:{a.current_task_id}"
            nodes.append({"id": tid, "kind": "task", "ref": a.current_task_id,
                          "label": a.current_step or a.current_task_id, "status": "RUNNING"})
            edges.append({"id": f"work:{a.agent_id}", "kind": "works_on", "source": nid, "target": tid,
                          "sources": ["registry"], "suspected_deadlock": False})
    for n in g["nodes"]:
        if n["kind"] == "resource":
            nodes.append(n)
    for e in g["edges"]:
        if e["source"] in {n["id"] for n in nodes} and e["target"] in present:
            edges.append(e)
    return {"nodes": nodes, "edges": edges}


def system_view(rt: BossmanRuntime) -> dict[str, Any]:
    snap = rt.resources.snapshot()
    env = os.getenv("BOSSMAN_ENV", "development")
    # Only booleans/non-secret config — never values of keys or URLs with credentials.
    openai_configured = bool(os.getenv("OPENAI_API_KEY", "").strip()) and not os.getenv(
        "OPENAI_API_KEY", ""
    ).startswith("your_")
    return {
        "version": _project_version(),
        "environment": env,
        "runtime": {
            "status": "RUNNING" if rt.running else "STOPPED",
            "uptime_s": round(time.monotonic() - rt.started_monotonic, 1),
            "python": sys.version.split()[0],
            "components": {
                "watchdog": rt.watchdog.stats,
                "failure_detector": {k: v for k, v in rt.detector.stats.items() if k != "recent"},
                "recovery_engine": {
                    "handled": rt.engine.stats["handled"],
                    "skipped_duplicate": rt.engine.stats["skipped_duplicate"],
                    "ledger": rt.engine.stats["ledger"],
                },
                "evaluator": rt.evaluator.stats,
                "diagnostician": rt.diagnostician.stats,
            },
        },
        "limits": {
            "llm_max_concurrent": snap["llm_semaphore"]["max_concurrent"],
            "llm_rate_per_second": snap["token_bucket"]["rate_per_second"],
            "llm_burst_capacity": snap["token_bucket"]["capacity"],
            "max_agent_restarts": rt.engine._max_restarts,
            "restart_window_s": rt.engine._restart_window,
        },
        "llm": {
            "provider": "openai",
            "configured": openai_configured,
            "model": os.getenv("LLM_PRIMARY_MODEL") or None,
            "diagnostician": "rule-based (LLM escalation not wired into the API runtime)",
            "note": "LLM calls are server-side only; no credentials are exposed to the browser.",
        },
        "database": {
            "configured": bool(os.getenv("DATABASE_URL")),
            "connected": None,
            "note": "Persistence is not wired in this runtime — state is in-memory (Postgres is a later phase).",
        },
        "event_bus": {**rt.bus.stats, "buffered_events": len(rt.store._events), "last_seq": rt.store.last_seq,
                      "events_per_minute": rt.store.events_per_minute()},
        "websocket": {
            "connected_clients": rt.store.subscriber_count,
            "keepalive_interval_s": int(os.getenv("WS_HEARTBEAT_INTERVAL", "5")),
        },
        "security": {"operator_actions_enabled": auth.actions_enabled()},
    }


async def system_view_full(rt: BossmanRuntime) -> dict[str, Any]:
    """system_view with the live state of every Phase 10–15 subsystem."""
    from bossman_server.control.llm import llm_enabled, model_name

    out = system_view(rt)
    cluster = await rt.cluster_info()
    llm_on = rt.llm_call is not None
    out["cluster"] = cluster
    key = os.getenv("ANTHROPIC_API_KEY", "")
    out["llm"] = {
        "provider": "anthropic",
        "configured": bool(key.strip()) and not key.startswith("your_"),
        "enabled": llm_on or llm_enabled(),
        "model": model_name() if llm_on else None,
        "diagnostician": "rule-based + LLM for low-confidence failures" if llm_on else "rule-based",
        "quality_judge": bool(rt.quality.judge),
        "note": "LLM calls are server-side only; no credentials are exposed to the browser.",
    }
    db_ok = await rt.db.ping() if rt.db is not None else None
    out["database"] = {
        "configured": rt.db is not None,
        "connected": db_ok,
        "dialect": rt.db.url.split(":", 1)[0].split("+", 1)[0] if rt.db is not None else None,
        "events_persisted": rt.persister.written if rt.persister else 0,
        "note": ("Tasks, ledger, keys, approvals, policies and events are durable."
                 if rt.db is not None else "In-memory persistence: state is lost on restart "
                 "(set BOSSMAN_PERSISTENCE=sql)."),
    }
    out["security"].update({
        "operator_actions_enabled": rt.keys.actions_enabled,
        "public_reads": auth.public_reads(),
        "api_keys": len(await rt.keys.list()),
        "secrets": len(await rt.secrets.list()),
        "policy_revision": rt.policy.revision,
        "policy_stats": dict(rt.policy.stats),
    })
    out["runtime"]["components"].update({
        "scheduler": {"runs": rt.scheduler.runs, "errors": rt.scheduler.errors},
        "locks": rt.locks.stats,
        "directives": {"issued": rt.directives.issued_total},
        "managed": {"active": rt.managed.active, "count": len(rt.managed.status())},
        "otel": {"exporting": rt.otel is not None},
    })
    out["governance"] = {
        "approvals_pending": await rt.approvals.pending_count(),
        "flags": len(rt.flags.list()),
        "supervisors": len(rt.supervisors.list()),
        "role_limits": rt.role_limits.all(),
    }
    out["heartbeat"] = {"interval_s": rt.heartbeat_interval_s, "timeout_s": rt.heartbeat_timeout_s}
    return out
