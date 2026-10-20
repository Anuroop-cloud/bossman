"""
control.approvals
─────────────────
Human-in-the-loop gates for irreversible actions, with an N-person rule.

zylos.md §Part5 (Kiro): "Approval workflows not extended to AI actors … the
agent could delete production infrastructure faster than a human could read
a confirmation prompt." Here the agent cannot proceed until enough distinct
humans approve; each approver counts once; any denial rejects; unanswered
requests expire (deny by default).

An approval produces a single-use *grant* — the token the agent presents
to the tool gateway or credential broker to actually perform the action.
"""

from __future__ import annotations

import asyncio
import time
import uuid
from typing import Any

from bossman_server.contracts.events import BossmanEvent, EventSeverity, EventType
from bossman_server.core.documents import DocumentStore, VersionConflict
from bossman_server.core.event_bus import EventBus

APPROVALS = "approvals"
GRANTS = "grants"


class ApprovalError(Exception):
    code = "conflict"


class ApprovalService:
    def __init__(self, docs: DocumentStore, bus: EventBus, audit: Any) -> None:
        self._docs = docs
        self._bus = bus
        self._audit = audit
        self._waiters: dict[str, asyncio.Event] = {}
        bus.subscribe(self._on_event, EventType.HUMAN_GATE_APPROVED, EventType.HUMAN_GATE_REJECTED,
                      EventType.HUMAN_GATE_EXPIRED)

    async def _on_event(self, event: BossmanEvent) -> None:
        ev = self._waiters.get(event.payload.get("approval_id", ""))
        if ev:
            ev.set()

    # ── Grants ───────────────────────────────────────────────────────────────

    async def issue_grant(self, *, agent_id: str, namespace: str, request: dict[str, Any],
                          ttl_s: float, approval_id: str | None = None, max_uses: int = 1) -> dict[str, Any]:
        grant = {
            "grant_id": f"gr-{uuid.uuid4().hex[:20]}",
            "agent_id": agent_id, "namespace": namespace, "request": request,
            "approval_id": approval_id, "issued_at": time.time(), "expires_at": time.time() + ttl_s,
            "max_uses": max_uses, "uses": 0,
        }
        await self._docs.put(GRANTS, grant["grant_id"], grant, namespace=namespace, status="active")
        return grant

    async def use_grant(self, grant_id: str, agent_id: str) -> dict[str, Any]:
        for _ in range(5):
            g = await self._docs.get(GRANTS, grant_id)
            if g is None or g["agent_id"] != agent_id:
                raise ApprovalError("grant not found for this agent")
            if g["expires_at"] < time.time():
                raise ApprovalError("grant expired")
            if g["uses"] >= g["max_uses"]:
                raise ApprovalError("grant already used")
            version = g.pop("_version")
            g = {k: v for k, v in g.items() if not k.startswith("_")}
            g["uses"] += 1
            try:
                await self._docs.put(GRANTS, grant_id, g, namespace=g["namespace"],
                                     status="used" if g["uses"] >= g["max_uses"] else "active",
                                     expected_version=version)
                return g
            except VersionConflict:
                continue
        raise ApprovalError("grant contention")

    async def get_grant(self, grant_id: str) -> dict[str, Any] | None:
        return await self._docs.get(GRANTS, grant_id)

    # ── Approvals ────────────────────────────────────────────────────────────

    async def create(self, *, agent_id: str, agent_name: str, namespace: str, request: dict[str, Any],
                     decision: dict[str, Any], ttl_s: float) -> dict[str, Any]:
        approval = {
            "approval_id": f"ap-{uuid.uuid4().hex[:16]}",
            "agent_id": agent_id, "agent_name": agent_name, "namespace": namespace,
            "request": request, "decision": decision,
            "required": max(1, int(decision.get("approvals_required") or 1)),
            "approvals": [], "denials": [], "status": "pending",
            "created_at": time.time(), "expires_at": time.time() + ttl_s, "grant_id": None,
        }
        await self._docs.put(APPROVALS, approval["approval_id"], approval, namespace=namespace, status="pending")
        await self._bus.publish(BossmanEvent.create(
            EventType.HUMAN_GATE_REQUIRED, agent_id=agent_id, namespace=namespace, task_id=request.get("task_id"),
            severity=EventSeverity.WARNING,
            message=f"Approval needed: {agent_name} wants to {request.get('action')}"
                    + (f" via {request['tool']}" if request.get("tool") else "")
                    + f" ({approval['required']} approver(s))",
            payload={"approval_id": approval["approval_id"], "action": request.get("action"),
                     "tool": request.get("tool"), "required": approval["required"],
                     "reason": decision.get("reason")},
        ))
        return approval

    async def get(self, approval_id: str) -> dict[str, Any] | None:
        a = await self._docs.get(APPROVALS, approval_id)
        if a and a["status"] == "pending" and a["expires_at"] < time.time():
            a = await self._expire(a)
        return a

    async def list(self, *, status: str | None = None, namespace: str | None = None,
                   limit: int = 200) -> list[dict[str, Any]]:
        rows = await self._docs.list(APPROVALS, status=status, namespace=namespace, limit=limit)
        out = []
        for a in rows:
            if a["status"] == "pending" and a["expires_at"] < time.time():
                a = await self._expire(a)
                if status == "pending":
                    continue
            out.append(a)
        return out

    async def wait(self, approval_id: str, timeout: float) -> dict[str, Any] | None:
        a = await self.get(approval_id)
        if a is None or a["status"] != "pending" or timeout <= 0:
            return a
        ev = self._waiters.setdefault(approval_id, asyncio.Event())
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        try:
            while True:
                remaining = deadline - loop.time()
                if remaining <= 0:
                    return a
                ev.clear()
                try:
                    await asyncio.wait_for(ev.wait(), timeout=min(remaining, 1.0))
                except asyncio.TimeoutError:
                    pass
                a = await self.get(approval_id)
                if a is None or a["status"] != "pending":
                    return a
        finally:
            self._waiters.pop(approval_id, None)

    async def decide(self, approval_id: str, *, approve: bool, actor: str, reason: str = "",
                     grant_ttl_s: float = 300.0) -> dict[str, Any]:
        for _ in range(5):
            a = await self.get(approval_id)
            if a is None:
                raise KeyError(approval_id)
            if a["status"] != "pending":
                raise ApprovalError(f"approval is already {a['status']}")
            if any(x["by"] == actor for x in a["approvals"] + a["denials"]):
                raise ApprovalError(f"{actor} has already voted on this approval")
            version = a["_version"]
            a = {k: v for k, v in a.items() if not k.startswith("_")}
            vote = {"by": actor, "at": time.time(), "reason": reason[:500]}
            (a["approvals"] if approve else a["denials"]).append(vote)
            if not approve:
                a["status"] = "denied"
            elif len(a["approvals"]) >= a["required"]:
                a["status"] = "approved"
            try:
                if a["status"] == "approved":
                    grant = await self.issue_grant(agent_id=a["agent_id"], namespace=a["namespace"],
                                                   request=a["request"], ttl_s=grant_ttl_s,
                                                   approval_id=approval_id)
                    a["grant_id"] = grant["grant_id"]
                await self._docs.put(APPROVALS, approval_id, a, namespace=a["namespace"], status=a["status"],
                                     expected_version=version)
            except VersionConflict:
                continue
            await self._audit.record("approval_vote", actor, namespace=a["namespace"], agent_id=a["agent_id"],
                                     data={"approval_id": approval_id, "approve": approve, "reason": reason,
                                           "status": a["status"], "request": a["request"]})
            if a["status"] != "pending":
                et = EventType.HUMAN_GATE_APPROVED if a["status"] == "approved" else EventType.HUMAN_GATE_REJECTED
                await self._bus.publish(BossmanEvent.create(
                    et, agent_id=a["agent_id"], namespace=a["namespace"],
                    message=f"Approval {approval_id} {a['status']} ({actor})",
                    payload={"approval_id": approval_id, "status": a["status"],
                             "approvals": len(a["approvals"]), "required": a["required"]},
                ))
            return a
        raise ApprovalError("approval contention, retry")

    async def _expire(self, a: dict[str, Any]) -> dict[str, Any]:
        a = {k: v for k, v in a.items() if not k.startswith("_")}
        a["status"] = "expired"
        await self._docs.put(APPROVALS, a["approval_id"], a, namespace=a["namespace"], status="expired")
        await self._bus.publish(BossmanEvent.create(
            EventType.HUMAN_GATE_EXPIRED, agent_id=a["agent_id"], namespace=a["namespace"],
            message=f"Approval {a['approval_id']} expired without a decision (denied by default)",
            payload={"approval_id": a["approval_id"]},
        ))
        return a

    async def sweep(self) -> int:
        n = 0
        for a in await self._docs.list(APPROVALS, status="pending", limit=10_000):
            if a["expires_at"] < time.time():
                await self._expire(a)
                n += 1
        return n

    async def pending_count(self) -> int:
        return await self._docs.count(APPROVALS, status="pending")
