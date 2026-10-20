"""
control.audit
─────────────
Append-only audit trail for everything a human or policy decided: policy
decisions, approvals, operator actions, key/secret/flag changes, credential
releases and proxied tool calls. zylos.md §Part9 — "auditable decision
trails"; the Kiro incident is the reason it exists.
"""

from __future__ import annotations

import time
import uuid
from typing import Any

from bossman_server.core.documents import DocumentStore

COLLECTION = "audit"


class AuditLog:
    def __init__(self, docs: DocumentStore) -> None:
        self._docs = docs

    async def record(self, kind: str, actor: str, *, namespace: str | None = None,
                     agent_id: str | None = None, data: dict[str, Any] | None = None) -> dict[str, Any]:
        now = time.time()
        entry = {
            "audit_id": f"{now:.6f}-{uuid.uuid4().hex[:8]}",
            "kind": kind,
            "actor": actor,
            "namespace": namespace,
            "agent_id": agent_id,
            "at": now,
            "data": data or {},
        }
        await self._docs.put(COLLECTION, entry["audit_id"], entry, namespace=namespace, status=kind)
        return entry

    async def list(self, *, kind: str | None = None, namespace: str | None = None,
                   limit: int = 200) -> list[dict[str, Any]]:
        rows = await self._docs.list(COLLECTION, status=kind, namespace=namespace, limit=limit)
        return [{k: v for k, v in r.items() if not k.startswith("_")} for r in rows]
