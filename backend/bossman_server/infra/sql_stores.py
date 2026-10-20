"""
infra.sql_stores
────────────────
SQL implementations of the stores used in distributed / persistent mode.

  SqlTaskStore       source of truth for tasks across nodes. Claims and every
                     other transition are compare-and-set on `version`
                     (UPDATE … WHERE version = :expected), so two nodes can
                     never both win the same task — portable to SQLite.
  SqlDocumentStore   versioned JSON documents (core.documents).
  SqlEventStore      append-only persisted events for audit and trace queries.
"""

from __future__ import annotations

import json
import time
from datetime import datetime
from typing import Any, Iterable

from sqlalchemy import delete, func, or_, select, update
from sqlalchemy.exc import IntegrityError

from bossman_server.contracts.events import BossmanEvent
from bossman_server.core.documents import VersionConflict
from bossman_server.core.task_manager import Task, TaskStatus
from bossman_server.core.task_store import DuplicateTaskKey
from bossman_server.infra.db import Database, DocumentRow, EventRow, TaskRow


def _ts(dt: datetime | None) -> float | None:
    return dt.timestamp() if dt is not None else None


class SqlTaskStore:
    def __init__(self, db: Database) -> None:
        self._db = db

    @staticmethod
    def _columns(t: Task) -> dict[str, Any]:
        return {
            "namespace": t.namespace,
            "status": t.status.value,
            "priority": int(t.priority),
            "required_role": t.required_role,
            "assigned_to": t.assigned_to,
            "created_ts": t.created_at.timestamp(),
            "not_before_ts": _ts(t.not_before),
            "lease_expires_ts": _ts(t.lease_expires_at),
            "idempotency_key": t.idempotency_key,
            "parent_task_id": t.parent_task_id,
            "trace_id": t.trace_id,
            "version": t.version,
            "doc": t.model_dump_json(),
        }

    async def insert(self, task: Task) -> None:
        try:
            async with self._db.sessions() as s, s.begin():
                s.add(TaskRow(task_id=task.task_id, **self._columns(task)))
        except IntegrityError:
            if task.idempotency_key:
                existing = await self.find_by_idempotency(task.namespace, task.idempotency_key)
                if existing is not None:
                    raise DuplicateTaskKey(existing.task_id) from None
            raise

    async def get(self, task_id: str) -> Task | None:
        async with self._db.sessions() as s:
            row = await s.get(TaskRow, task_id)
            return Task.model_validate_json(row.doc) if row else None

    async def save(self, task: Task, expected_version: int) -> bool:
        task.version = expected_version + 1
        cols = self._columns(task)
        async with self._db.sessions() as s, s.begin():
            res = await s.execute(
                update(TaskRow)
                .where(TaskRow.task_id == task.task_id, TaskRow.version == expected_version)
                .values(**cols)
            )
        if res.rowcount != 1:
            task.version = expected_version
            return False
        return True

    async def find_by_idempotency(self, namespace: str, key: str) -> Task | None:
        async with self._db.sessions() as s:
            row = (await s.scalars(select(TaskRow).where(
                TaskRow.namespace == namespace, TaskRow.idempotency_key == key))).first()
            return Task.model_validate_json(row.doc) if row else None

    async def query(
        self,
        *,
        namespace: str | None = None,
        statuses: Iterable[TaskStatus] | None = None,
        assigned_to: str | None = None,
        parent_task_id: str | None = None,
        trace_id: str | None = None,
        limit: int | None = None,
    ) -> list[Task]:
        q = select(TaskRow)
        if namespace is not None:
            q = q.where(TaskRow.namespace == namespace)
        if statuses is not None:
            q = q.where(TaskRow.status.in_([s.value for s in statuses]))
        if assigned_to is not None:
            q = q.where(TaskRow.assigned_to == assigned_to)
        if parent_task_id is not None:
            q = q.where(TaskRow.parent_task_id == parent_task_id)
        if trace_id is not None:
            q = q.where(TaskRow.trace_id == trace_id)
        q = q.order_by(TaskRow.created_ts)
        if limit:
            q = q.limit(limit)
        async with self._db.sessions() as s:
            return [Task.model_validate_json(r.doc) for r in await s.scalars(q)]

    async def claimable(self, *, namespace: str, roles: set[str], now: datetime, limit: int = 20) -> list[Task]:
        q = (
            select(TaskRow)
            .where(
                TaskRow.status == TaskStatus.PENDING.value,
                TaskRow.namespace == namespace,
                or_(TaskRow.required_role.is_(None), TaskRow.required_role.in_(sorted(roles) or [""])),
                or_(TaskRow.not_before_ts.is_(None), TaskRow.not_before_ts <= now.timestamp()),
            )
            .order_by(TaskRow.priority, TaskRow.created_ts)
            .limit(limit)
        )
        async with self._db.sessions() as s:
            return [Task.model_validate_json(r.doc) for r in await s.scalars(q)]

    async def expired_leases(self, now: datetime) -> list[Task]:
        q = select(TaskRow).where(
            TaskRow.status.in_([TaskStatus.RUNNING.value, TaskStatus.ASSIGNED.value]),
            TaskRow.lease_expires_ts.is_not(None),
            TaskRow.lease_expires_ts < now.timestamp(),
        )
        async with self._db.sessions() as s:
            return [Task.model_validate_json(r.doc) for r in await s.scalars(q)]

    async def counts(self, namespace: str | None = None) -> dict[str, int]:
        q = select(TaskRow.status, func.count()).group_by(TaskRow.status)
        if namespace is not None:
            q = q.where(TaskRow.namespace == namespace)
        out = {s.value: 0 for s in TaskStatus}
        async with self._db.sessions() as s:
            for status, n in (await s.execute(q)).all():
                out[status] = int(n)
        return out


class SqlDocumentStore:
    def __init__(self, db: Database) -> None:
        self._db = db

    async def put(self, collection: str, doc_id: str, doc: dict[str, Any], *,
                  namespace: str | None = None, status: str | None = None,
                  expected_version: int | None = None) -> int:
        now = time.time()
        body = json.dumps({k: v for k, v in doc.items() if not k.startswith("_")}, default=str)
        async with self._db.sessions() as s, s.begin():
            row = await s.get(DocumentRow, (collection, doc_id))
            current = row.version if row else 0
            if expected_version is not None and expected_version != current:
                raise VersionConflict(f"{collection}/{doc_id}: expected v{expected_version}, found v{current}")
            if row is None:
                s.add(DocumentRow(collection=collection, doc_id=doc_id, namespace=namespace, status=status,
                                  created_ts=now, updated_ts=now, version=1, doc=body))
                return 1
            res = await s.execute(
                update(DocumentRow)
                .where(DocumentRow.collection == collection, DocumentRow.doc_id == doc_id,
                       DocumentRow.version == current)
                .values(namespace=namespace, status=status, updated_ts=now, version=current + 1, doc=body)
            )
            if res.rowcount != 1:
                raise VersionConflict(f"{collection}/{doc_id}: concurrent update")
            return current + 1

    @staticmethod
    def _out(r: DocumentRow) -> dict[str, Any]:
        return {**json.loads(r.doc), "_version": r.version, "_created_ts": r.created_ts,
                "_updated_ts": r.updated_ts}

    async def get(self, collection: str, doc_id: str) -> dict[str, Any] | None:
        async with self._db.sessions() as s:
            row = await s.get(DocumentRow, (collection, doc_id))
            return self._out(row) if row else None

    async def delete(self, collection: str, doc_id: str) -> bool:
        async with self._db.sessions() as s, s.begin():
            res = await s.execute(delete(DocumentRow).where(
                DocumentRow.collection == collection, DocumentRow.doc_id == doc_id))
            return res.rowcount > 0

    async def list(self, collection: str, *, namespace: str | None = None, status: str | None = None,
                   limit: int = 1000, newest_first: bool = True,
                   id_prefix: str | None = None) -> list[dict[str, Any]]:
        q = select(DocumentRow).where(DocumentRow.collection == collection)
        if namespace is not None:
            q = q.where(DocumentRow.namespace == namespace)
        if status is not None:
            q = q.where(DocumentRow.status == status)
        if id_prefix is not None:
            q = q.where(DocumentRow.doc_id.startswith(id_prefix))
        q = q.order_by(DocumentRow.created_ts.desc() if newest_first else DocumentRow.created_ts).limit(limit)
        async with self._db.sessions() as s:
            return [self._out(r) for r in await s.scalars(q)]

    async def count(self, collection: str, *, status: str | None = None) -> int:
        q = select(func.count()).select_from(DocumentRow).where(DocumentRow.collection == collection)
        if status is not None:
            q = q.where(DocumentRow.status == status)
        async with self._db.sessions() as s:
            return int((await s.execute(q)).scalar_one())

    async def prune(self, collection: str, older_than_ts: float) -> int:
        async with self._db.sessions() as s, s.begin():
            res = await s.execute(delete(DocumentRow).where(
                DocumentRow.collection == collection, DocumentRow.updated_ts < older_than_ts))
            return res.rowcount or 0


class SqlEventStore:
    """Persisted events. Writes are batched by the runtime's EventPersister."""

    def __init__(self, db: Database) -> None:
        self._db = db

    async def append_many(self, events: list[BossmanEvent]) -> int:
        if not events:
            return 0
        ids = [e.event_id for e in events]
        async with self._db.sessions() as s, s.begin():
            existing = set(await s.scalars(select(EventRow.event_id).where(EventRow.event_id.in_(ids))))
            for e in events:
                if e.event_id in existing:
                    continue
                s.add(EventRow(
                    event_id=e.event_id, occurred_ts=e.occurred_at.timestamp(),
                    event_type=e.event_type.value, severity=e.severity.value,
                    agent_id=e.agent_id, task_id=e.task_id, namespace=e.namespace,
                    trace_id=e.trace_id, doc=e.model_dump_json(),
                ))
        return len(events) - len(existing)

    async def query(self, *, trace_id: str | None = None, agent_id: str | None = None,
                    task_ids: list[str] | None = None, event_type: str | None = None,
                    since_ts: float | None = None, limit: int = 500) -> list[BossmanEvent]:
        q = select(EventRow)
        if trace_id is not None and task_ids:
            q = q.where(or_(EventRow.trace_id == trace_id, EventRow.task_id.in_(task_ids)))
        elif trace_id is not None:
            q = q.where(EventRow.trace_id == trace_id)
        elif task_ids:
            q = q.where(EventRow.task_id.in_(task_ids))
        if agent_id is not None:
            q = q.where(EventRow.agent_id == agent_id)
        if event_type is not None:
            q = q.where(EventRow.event_type == event_type)
        if since_ts is not None:
            q = q.where(EventRow.occurred_ts >= since_ts)
        q = q.order_by(EventRow.occurred_ts.desc()).limit(limit)
        async with self._db.sessions() as s:
            rows = list(await s.scalars(q))
        return [BossmanEvent.model_validate_json(r.doc) for r in reversed(rows)]

    async def prune(self, older_than_ts: float) -> int:
        async with self._db.sessions() as s, s.begin():
            res = await s.execute(delete(EventRow).where(EventRow.occurred_ts < older_than_ts))
            return res.rowcount or 0

    async def count(self) -> int:
        async with self._db.sessions() as s:
            return int((await s.execute(select(func.count()).select_from(EventRow))).scalar_one())
