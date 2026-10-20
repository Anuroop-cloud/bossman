"""
control.checkpoints
───────────────────
"Design for resume, not restart" (zylos.md §Part9):

  * Task checkpoints — an agent records phase markers + state as it works.
    A reassigned or restarted worker resumes from the latest checkpoint;
    ROLLBACK_CHECKPOINT resumes from the one before it.
  * Idempotency records — side effects are claimed under a key before they
    run and their result stored after, so a replayed step returns the stored
    result instead of charging the card twice.
"""

from __future__ import annotations

import time
from typing import Any

from bossman_server.contracts.events import BossmanEvent, EventType
from bossman_server.core.documents import DocumentStore, VersionConflict
from bossman_server.core.event_bus import EventBus

CHECKPOINTS = "checkpoints"
IDEMPOTENCY = "idempotency"
MAX_STATE_BYTES = 256_000
KEEP = 20


class CheckpointService:
    def __init__(self, docs: DocumentStore, bus: EventBus) -> None:
        self._docs = docs
        self._bus = bus

    async def save(self, task_id: str, *, agent_id: str, phase: str, state: dict[str, Any],
                   namespace: str | None = None, trace_id: str | None = None) -> dict[str, Any]:
        import json

        if len(json.dumps(state, default=str)) > MAX_STATE_BYTES:
            raise ValueError(f"checkpoint state exceeds {MAX_STATE_BYTES} bytes — store large blobs externally")
        existing = await self._docs.list(CHECKPOINTS, id_prefix=f"{task_id}:", limit=KEEP + 5)
        seq = max((c["seq"] for c in existing), default=0) + 1
        cp = {"task_id": task_id, "seq": seq, "phase": phase[:120], "state": state, "agent_id": agent_id,
              "at": time.time()}
        await self._docs.put(CHECKPOINTS, f"{task_id}:{seq:08d}", cp, namespace=namespace)
        for old in sorted(existing, key=lambda c: c["seq"])[: max(0, len(existing) + 1 - KEEP)]:
            await self._docs.delete(CHECKPOINTS, f"{task_id}:{old['seq']:08d}")
        await self._bus.publish(BossmanEvent.create(
            EventType.TASK_CHECKPOINTED, task_id=task_id, agent_id=agent_id, namespace=namespace,
            trace_id=trace_id, message=f"Task {task_id} checkpoint #{seq} ({phase})",
            payload={"seq": seq, "phase": phase}))
        return cp

    async def list(self, task_id: str) -> list[dict[str, Any]]:
        rows = await self._docs.list(CHECKPOINTS, id_prefix=f"{task_id}:", limit=KEEP + 5)
        return sorted(({k: v for k, v in r.items() if not k.startswith("_")} for r in rows),
                      key=lambda c: c["seq"])

    async def latest(self, task_id: str, *, before_seq: int | None = None) -> dict[str, Any] | None:
        items = await self.list(task_id)
        if before_seq is not None:
            items = [c for c in items if c["seq"] < before_seq]
        return items[-1] if items else None


class IdempotencyService:
    def __init__(self, docs: DocumentStore) -> None:
        self._docs = docs

    async def begin(self, key: str, *, agent_id: str, namespace: str, ttl_s: float = 300.0) -> dict[str, Any]:
        """
        new         — caller owns the key and must complete/abort it
        in_progress — another worker is executing it right now (back off)
        completed   — return the stored result; do NOT run the side effect again
        """
        doc_id = f"{namespace}:{key}"
        for _ in range(5):
            cur = await self._docs.get(IDEMPOTENCY, doc_id)
            now = time.time()
            if cur and cur["state"] == "completed":
                return {"state": "completed", "result": cur.get("result"), "completed_at": cur.get("completed_at")}
            if cur and cur["state"] == "in_progress" and cur["expires_at"] > now and cur["agent_id"] != agent_id:
                return {"state": "in_progress", "owner": cur["agent_id"], "retry_after_s": round(cur["expires_at"] - now, 1)}
            rec = {"key": key, "state": "in_progress", "agent_id": agent_id, "started_at": now,
                   "expires_at": now + ttl_s, "result": None}
            try:
                await self._docs.put(IDEMPOTENCY, doc_id, rec, namespace=namespace, status="in_progress",
                                     expected_version=cur["_version"] if cur else 0)
                return {"state": "new"}
            except VersionConflict:
                continue
        return {"state": "in_progress", "owner": None, "retry_after_s": 1.0}

    async def complete(self, key: str, *, agent_id: str, namespace: str, result: Any) -> None:
        doc_id = f"{namespace}:{key}"
        cur = await self._docs.get(IDEMPOTENCY, doc_id)
        if cur is None or cur["agent_id"] != agent_id:
            raise PermissionError("key is not owned by this agent")
        await self._docs.put(IDEMPOTENCY, doc_id, {
            "key": key, "state": "completed", "agent_id": agent_id, "started_at": cur["started_at"],
            "completed_at": time.time(), "expires_at": cur["expires_at"], "result": result,
        }, namespace=namespace, status="completed")

    async def abort(self, key: str, *, agent_id: str, namespace: str) -> bool:
        doc_id = f"{namespace}:{key}"
        cur = await self._docs.get(IDEMPOTENCY, doc_id)
        if cur is None or cur["state"] != "in_progress" or cur["agent_id"] != agent_id:
            return False
        return await self._docs.delete(IDEMPOTENCY, doc_id)
