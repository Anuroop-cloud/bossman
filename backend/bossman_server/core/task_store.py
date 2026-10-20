"""
core.task_store
───────────────
Storage behind TaskManager.

  InMemoryTaskStore   single node / tests (returns live objects)
  SqlTaskStore        infra.sql_stores — the cluster source of truth; claims
                      are made atomic with optimistic concurrency (version CAS),
                      which works identically on PostgreSQL and SQLite.
"""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Iterable, Protocol

if TYPE_CHECKING:  # pragma: no cover
    from bossman_server.core.task_manager import Task, TaskStatus


class DuplicateTaskKey(Exception):
    def __init__(self, existing_task_id: str) -> None:
        super().__init__(f"idempotency key already used by {existing_task_id}")
        self.existing_task_id = existing_task_id


class TaskStore(Protocol):
    async def insert(self, task: Task) -> None: ...
    async def get(self, task_id: str) -> Task | None: ...
    async def save(self, task: Task, expected_version: int) -> bool: ...
    async def find_by_idempotency(self, namespace: str, key: str) -> Task | None: ...
    async def query(
        self,
        *,
        namespace: str | None = None,
        statuses: Iterable[TaskStatus] | None = None,
        assigned_to: str | None = None,
        parent_task_id: str | None = None,
        trace_id: str | None = None,
        limit: int | None = None,
    ) -> list[Task]: ...
    async def claimable(
        self, *, namespace: str, roles: set[str], now: datetime, limit: int = 20
    ) -> list[Task]: ...
    async def expired_leases(self, now: datetime) -> list[Task]: ...
    async def counts(self, namespace: str | None = None) -> dict[str, int]: ...


class InMemoryTaskStore:
    def __init__(self) -> None:
        self._tasks: dict[str, Task] = {}
        self._idem: dict[tuple[str, str], str] = {}

    async def insert(self, task: Task) -> None:
        if task.idempotency_key:
            k = (task.namespace, task.idempotency_key)
            if k in self._idem:
                raise DuplicateTaskKey(self._idem[k])
            self._idem[k] = task.task_id
        self._tasks[task.task_id] = task

    async def get(self, task_id: str) -> Task | None:
        return self._tasks.get(task_id)

    async def save(self, task: Task, expected_version: int) -> bool:
        cur = self._tasks.get(task.task_id)
        if cur is None:
            return False
        if cur is not task and cur.version != expected_version:
            return False
        task.version = expected_version + 1
        self._tasks[task.task_id] = task
        return True

    async def find_by_idempotency(self, namespace: str, key: str) -> Task | None:
        tid = self._idem.get((namespace, key))
        return self._tasks.get(tid) if tid else None

    async def query(
        self,
        *,
        namespace: str | None = None,
        statuses=None,  # noqa: ANN001
        assigned_to: str | None = None,
        parent_task_id: str | None = None,
        trace_id: str | None = None,
        limit: int | None = None,
    ) -> list[Task]:
        st = set(statuses) if statuses is not None else None
        out = [
            t for t in self._tasks.values()
            if (namespace is None or t.namespace == namespace)
            and (st is None or t.status in st)
            and (assigned_to is None or t.assigned_to == assigned_to)
            and (parent_task_id is None or t.parent_task_id == parent_task_id)
            and (trace_id is None or t.trace_id == trace_id)
        ]
        out.sort(key=lambda t: t.created_at)
        return out[:limit] if limit else out

    async def claimable(
        self, *, namespace: str, roles: set[str], now: datetime, limit: int = 20
    ) -> list[Task]:
        from bossman_server.core.task_manager import TaskStatus

        out = [
            t for t in self._tasks.values()
            if t.status == TaskStatus.PENDING
            and t.namespace == namespace
            and (t.required_role is None or t.required_role in roles)
            and (t.not_before is None or t.not_before <= now)
        ]
        out.sort(key=lambda t: (int(t.priority), t.created_at))
        return out[:limit]

    async def expired_leases(self, now: datetime) -> list[Task]:
        from bossman_server.core.task_manager import TaskStatus

        return [
            t for t in self._tasks.values()
            if t.status in (TaskStatus.RUNNING, TaskStatus.ASSIGNED)
            and t.lease_expires_at is not None and t.lease_expires_at < now
        ]

    async def counts(self, namespace: str | None = None) -> dict[str, int]:
        from bossman_server.core.task_manager import TaskStatus

        c = {s.value: 0 for s in TaskStatus}
        for t in self._tasks.values():
            if namespace is None or t.namespace == namespace:
                c[t.status.value] += 1
        return c
