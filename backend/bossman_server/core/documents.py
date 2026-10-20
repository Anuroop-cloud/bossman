"""
core.documents
──────────────
DocumentStore — a tiny versioned JSON document store used by the control
plane for records that need durability but not bespoke tables: API keys,
approvals, policies, flags, secrets, managed-process specs, supervisor
specs, task checkpoints, idempotency records and the audit log.

  InMemoryDocumentStore   single node / tests
  SqlDocumentStore        infra.sql_stores (PostgreSQL / SQLite)

Every document has a version; `put(expected_version=…)` is a compare-and-set
so concurrent writers on different nodes cannot silently clobber each other.
"""

from __future__ import annotations

import copy
import time
from typing import Any, Protocol


class VersionConflict(Exception):
    pass


class DocumentStore(Protocol):
    async def put(self, collection: str, doc_id: str, doc: dict[str, Any], *,
                  namespace: str | None = None, status: str | None = None,
                  expected_version: int | None = None) -> int: ...
    async def get(self, collection: str, doc_id: str) -> dict[str, Any] | None: ...
    async def delete(self, collection: str, doc_id: str) -> bool: ...
    async def list(self, collection: str, *, namespace: str | None = None, status: str | None = None,
                   limit: int = 1000, newest_first: bool = True,
                   id_prefix: str | None = None) -> list[dict[str, Any]]: ...
    async def count(self, collection: str, *, status: str | None = None) -> int: ...
    async def prune(self, collection: str, older_than_ts: float) -> int: ...


class InMemoryDocumentStore:
    def __init__(self) -> None:
        # (collection, id) → (meta, doc)
        self._docs: dict[tuple[str, str], tuple[dict[str, Any], dict[str, Any]]] = {}

    async def put(self, collection: str, doc_id: str, doc: dict[str, Any], *,
                  namespace: str | None = None, status: str | None = None,
                  expected_version: int | None = None) -> int:
        key = (collection, doc_id)
        now = time.time()
        cur = self._docs.get(key)
        cur_version = cur[0]["version"] if cur else 0
        if expected_version is not None and expected_version != cur_version:
            raise VersionConflict(f"{collection}/{doc_id}: expected v{expected_version}, found v{cur_version}")
        meta = {
            "namespace": namespace, "status": status, "version": cur_version + 1,
            "created_ts": cur[0]["created_ts"] if cur else now, "updated_ts": now,
        }
        self._docs[key] = (meta, copy.deepcopy(doc))
        return meta["version"]

    def _out(self, meta: dict[str, Any], doc: dict[str, Any]) -> dict[str, Any]:
        return {**copy.deepcopy(doc), "_version": meta["version"], "_created_ts": meta["created_ts"],
                "_updated_ts": meta["updated_ts"]}

    async def get(self, collection: str, doc_id: str) -> dict[str, Any] | None:
        cur = self._docs.get((collection, doc_id))
        return self._out(*cur) if cur else None

    async def delete(self, collection: str, doc_id: str) -> bool:
        return self._docs.pop((collection, doc_id), None) is not None

    async def list(self, collection: str, *, namespace: str | None = None, status: str | None = None,
                   limit: int = 1000, newest_first: bool = True,
                   id_prefix: str | None = None) -> list[dict[str, Any]]:
        rows = [
            (meta, doc) for (c, did), (meta, doc) in self._docs.items()
            if c == collection
            and (namespace is None or meta["namespace"] == namespace)
            and (status is None or meta["status"] == status)
            and (id_prefix is None or did.startswith(id_prefix))
        ]
        rows.sort(key=lambda r: r[0]["created_ts"], reverse=newest_first)
        return [self._out(m, d) for m, d in rows[:limit]]

    async def count(self, collection: str, *, status: str | None = None) -> int:
        return sum(1 for (c, _), (m, _d) in self._docs.items()
                   if c == collection and (status is None or m["status"] == status))

    async def prune(self, collection: str, older_than_ts: float) -> int:
        dead = [k for k, (m, _d) in self._docs.items() if k[0] == collection and m["updated_ts"] < older_than_ts]
        for k in dead:
            self._docs.pop(k, None)
        return len(dead)
