"""
core.task_manager
─────────────────
TaskManager — create, hand out, and track work items across the workforce.

Tasks are the unit of work BOSSman hands to agents. The task manager:
  - Creates tasks with priorities, deadlines, dependencies and output schemas
  - Hands tasks to agents through *leases* (claim → renew → complete/fail);
    a lease that is not renewed expires and the task is retried elsewhere,
    and a stale worker presenting an old lease is fenced off
  - Retries transient failures with exponential backoff + full jitter and
    dead-letters non-transient ones immediately (fail fast)
  - Contains cascades: a task whose upstream dependency died is QUARANTINED
    instead of running on poisoned input (zylos.md §Part1 — 85%^10 ≈ 20%)
  - Moves exhausted tasks to the Dead Letter Queue (DLQ)
  - Publishes events at every state change

The DLQ (zylos.md §Part8) ensures that no work is silently dropped.
Every task that fails after exhausting retries lands in the DLQ for
inspection, replay, or manual discard — never disappears.
"""

from __future__ import annotations

import asyncio
import logging
import os
import random
import uuid
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, Callable

from pydantic import BaseModel, Field

from bossman_server.contracts.agent_state import DEFAULT_NAMESPACE, AgentId, TaskId, new_task_id
from bossman_server.contracts.events import BossmanEvent, EventType
from bossman_server.core.event_bus import EventBus
from bossman_server.core.task_store import DuplicateTaskKey, InMemoryTaskStore, TaskStore

log = logging.getLogger(__name__)

RETRY_BASE_S = float(os.getenv("BOSSMAN_TASK_RETRY_BASE_S", "1.0"))
RETRY_CAP_S = float(os.getenv("BOSSMAN_TASK_RETRY_CAP_S", "60.0"))
DEFAULT_LEASE_S = 60.0
MAX_ATTEMPT_LOG = 20

# HTTP statuses / error classes that retrying cannot fix (zylos.md §Part2 —
# "Only retries on transient errors (rate limits, server errors, timeouts)").
NON_TRANSIENT_STATUS = {400, 401, 403, 404, 405, 409, 410, 413, 422}
NON_TRANSIENT_CLASSES = {
    "validation", "bad_request", "permission", "permission_denied", "credential",
    "unauthorized", "forbidden", "not_found", "deadline_exceeded", "cancelled", "fatal",
}


def is_transient(error_class: str | None, status_code: int | None) -> bool:
    if status_code is not None:
        if status_code == 429 or status_code >= 500:
            return True
        if status_code in NON_TRANSIENT_STATUS:
            return False
    if error_class and error_class.lower() in NON_TRANSIENT_CLASSES:
        return False
    return True


def full_jitter(attempt: int, base: float = RETRY_BASE_S, cap: float = RETRY_CAP_S) -> float:
    """AWS 'full jitter': uniform(0, min(cap, base·2^attempt))."""
    return random.uniform(0, min(cap, base * (2 ** max(0, attempt))))


def _now() -> datetime:
    return datetime.now(timezone.utc)


class TaskStatus(str, Enum):
    PENDING = "PENDING"          # Waiting for an agent
    BLOCKED = "BLOCKED"          # Waiting on upstream dependencies
    ASSIGNED = "ASSIGNED"        # Pushed to a specific agent
    RUNNING = "RUNNING"          # Agent holds a lease and is working
    COMPLETED = "COMPLETED"      # Finished successfully
    FAILED = "FAILED"            # Failed (transitional; retried or dead-lettered)
    CANCELLED = "CANCELLED"      # Explicitly cancelled
    DEAD_LETTER = "DEAD_LETTER"  # Exhausted retries / non-transient → DLQ
    QUARANTINED = "QUARANTINED"  # Upstream dependency died → not run


TERMINAL = {TaskStatus.COMPLETED, TaskStatus.CANCELLED, TaskStatus.DEAD_LETTER, TaskStatus.QUARANTINED}
DEAD = {TaskStatus.CANCELLED, TaskStatus.DEAD_LETTER, TaskStatus.QUARANTINED}


class TaskPriority(int, Enum):
    CRITICAL = 0   # Must run immediately
    HIGH = 1
    NORMAL = 2
    LOW = 3
    BACKGROUND = 4


class Task(BaseModel):
    """A unit of work managed by BOSSman."""

    task_id: TaskId = Field(default_factory=new_task_id)
    title: str = Field(..., description="Short human-readable description")
    description: str = ""
    priority: TaskPriority = TaskPriority.NORMAL
    namespace: str = DEFAULT_NAMESPACE

    # Assignment
    required_role: str | None = Field(
        default=None,
        description="If set, only agents with this role can be assigned this task.",
    )
    assigned_to: AgentId | None = None

    # State
    status: TaskStatus = TaskStatus.PENDING
    attempt_count: int = 0
    max_attempts: int = 3
    last_error: str | None = None
    error_class: str | None = None

    # Timing
    created_at: datetime = Field(default_factory=_now)
    assigned_at: datetime | None = None
    started_at: datetime | None = None
    completed_at: datetime | None = None
    deadline: datetime | None = None
    not_before: datetime | None = None      # retry backoff gate

    # Leases (fencing): only the holder of lease_id may report on this attempt
    lease_id: str | None = None
    lease_expires_at: datetime | None = None
    cancel_requested: bool = False

    # Task payload — arbitrary data the agent receives
    input_data: dict[str, Any] = Field(default_factory=dict)
    output_data: dict[str, Any] = Field(default_factory=dict)
    output_schema: dict[str, Any] | None = None

    # Pipelines / causality
    idempotency_key: str | None = None
    parent_task_id: TaskId | None = None
    depends_on: list[TaskId] = Field(default_factory=list)
    trace_id: str | None = None
    labels: dict[str, str] = Field(default_factory=dict)
    submitted_by: str | None = None
    current_step: str | None = None
    attempts_log: list[dict[str, Any]] = Field(default_factory=list)

    # DLQ / quarantine metadata
    dlq_reason: str | None = None
    dlq_at: datetime | None = None
    quarantine_reason: str | None = None

    version: int = 0

    @property
    def is_terminal(self) -> bool:
        return self.status in TERMINAL

    @property
    def can_retry(self) -> bool:
        return self.attempt_count < self.max_attempts and self.status == TaskStatus.FAILED

    @property
    def age_seconds(self) -> float:
        return (_now() - self.created_at).total_seconds()


class LeaseError(Exception):
    """The caller does not hold the current lease (stale worker / reassigned)."""


class OutputInvalid(Exception):
    def __init__(self, task: Task, message: str) -> None:
        super().__init__(message)
        self.task = task


class TaskManager:
    """
    Create, assign, and lifecycle-manage tasks across the BOSSman workforce.

    Usage
    ─────
        task = await tm.create("Research quantum computing", required_role="researcher")
        await tm.assign(task.task_id, agent_id)
        await tm.mark_running(task.task_id)
        await tm.complete(task.task_id, output={"summary": "..."})

    Remote agents use claim → renew_lease → complete/fail with a lease_id.
    """

    def __init__(self, bus: EventBus, store: TaskStore | None = None) -> None:
        self._bus = bus
        self._store: TaskStore = store or InMemoryTaskStore()
        self._lock = asyncio.Lock()
        self._work = asyncio.Event()           # wakes long-polling claimers on this node
        self._done: dict[str, asyncio.Event] = {}  # wakes waiters on task completion

    @property
    def store(self) -> TaskStore:
        return self._store

    # ── Event helper ──────────────────────────────────────────────────────────

    async def _emit(self, et: EventType, t: Task, message: str, agent_id: AgentId | None = None,
                    payload: dict[str, Any] | None = None) -> None:
        await self._bus.publish(BossmanEvent.create(
            et, message=message, task_id=t.task_id,
            agent_id=agent_id if agent_id is not None else t.assigned_to,
            namespace=t.namespace, trace_id=t.trace_id, payload=payload or {},
        ))

    def _signal_work(self) -> None:
        self._work.set()

    def _signal_done(self, task_id: str) -> None:
        ev = self._done.get(task_id)
        if ev:
            ev.set()

    async def _mutate(self, task_id: TaskId, fn: Callable[[Task], Any], retries: int = 8) -> Task:
        """Load → mutate → compare-and-set, retrying on concurrent writers."""
        for _ in range(retries):
            task = await self._store.get(task_id)
            if task is None:
                raise KeyError(f"Unknown task: {task_id!r}")
            expected = task.version
            fn(task)
            if await self._store.save(task, expected):
                return task
            await asyncio.sleep(random.uniform(0, 0.02))
        raise RuntimeError(f"task {task_id} update conflicted {retries} times")

    # ── Creation ──────────────────────────────────────────────────────────────

    async def create(
        self,
        title: str,
        description: str = "",
        priority: TaskPriority = TaskPriority.NORMAL,
        required_role: str | None = None,
        max_attempts: int = 3,
        input_data: dict[str, Any] | None = None,
        deadline: datetime | None = None,
        *,
        namespace: str = DEFAULT_NAMESPACE,
        idempotency_key: str | None = None,
        parent_task_id: TaskId | None = None,
        depends_on: list[TaskId] | None = None,
        output_schema: dict[str, Any] | None = None,
        trace_id: str | None = None,
        labels: dict[str, str] | None = None,
        submitted_by: str | None = None,
    ) -> Task:
        if idempotency_key:
            existing = await self._store.find_by_idempotency(namespace, idempotency_key)
            if existing is not None:
                return existing
        if output_schema is not None:
            from jsonschema import Draft202012Validator

            Draft202012Validator.check_schema(output_schema)  # raises SchemaError

        if trace_id is None and parent_task_id:
            parent = await self._store.get(parent_task_id)
            trace_id = parent.trace_id if parent else None

        task = Task(
            title=title,
            description=description,
            priority=priority,
            required_role=required_role,
            max_attempts=max_attempts,
            input_data=input_data or {},
            deadline=deadline,
            namespace=namespace,
            idempotency_key=idempotency_key,
            parent_task_id=parent_task_id,
            depends_on=list(dict.fromkeys(depends_on or [])),
            output_schema=output_schema,
            trace_id=trace_id or uuid.uuid4().hex,
            labels=labels or {},
            submitted_by=submitted_by,
        )
        dead_dep = None
        if task.depends_on:
            deps = [await self._store.get(d) for d in task.depends_on]
            missing = [d for d, t in zip(task.depends_on, deps) if t is None]
            if missing:
                raise KeyError(f"Unknown dependency task(s): {missing}")
            dead_dep = next((t for t in deps if t and t.status in DEAD), None)
            if dead_dep is not None:
                task.status = TaskStatus.QUARANTINED
                task.quarantine_reason = f"upstream {dead_dep.task_id} is {dead_dep.status.value}"
            elif any(t and t.status != TaskStatus.COMPLETED for t in deps):
                task.status = TaskStatus.BLOCKED
        try:
            await self._store.insert(task)
        except DuplicateTaskKey as dup:  # lost an idempotency race
            existing = await self._store.get(dup.existing_task_id)
            if existing is not None:
                return existing
            raise

        log.info("Task created: %s [%s]", task.task_id, title)
        await self._emit(
            EventType.TASK_CREATED, task,
            f"Task created: {title!r} (priority={priority.name})",
            payload={"title": title, "priority": priority.name, "required_role": required_role,
                     "depends_on": task.depends_on, "parent_task_id": parent_task_id},
        )
        if task.status == TaskStatus.BLOCKED:
            await self._emit(EventType.TASK_BLOCKED, task, f"Task {task.task_id!r} waiting on {task.depends_on}")
        elif task.status == TaskStatus.QUARANTINED:
            await self._emit(EventType.TASK_QUARANTINED, task,
                             f"Task {task.task_id!r} quarantined: {task.quarantine_reason}",
                             payload={"reason": task.quarantine_reason})
        else:
            self._signal_work()
        return task

    # ── Assignment (push) ─────────────────────────────────────────────────────

    async def assign(self, task_id: TaskId, agent_id: AgentId) -> Task:
        def fn(t: Task) -> None:
            t.assigned_to = agent_id
            t.assigned_at = _now()
            t.status = TaskStatus.ASSIGNED
            t.attempt_count += 1
            t.lease_id = None
            t.lease_expires_at = None

        task = await self._mutate(task_id, fn)
        await self._emit(
            EventType.TASK_ASSIGNED, task,
            f"Task {task_id!r} assigned to agent {agent_id!r} "
            f"(attempt {task.attempt_count}/{task.max_attempts})",
            agent_id=agent_id,
        )
        self._signal_work()
        return task

    async def mark_running(self, task_id: TaskId) -> Task:
        def fn(t: Task) -> None:
            t.status = TaskStatus.RUNNING
            t.started_at = _now()

        task = await self._mutate(task_id, fn)
        await self._emit(EventType.TASK_STARTED, task, f"Task {task_id!r} is now running")
        return task

    # ── Leases (pull) ─────────────────────────────────────────────────────────

    async def claim(
        self,
        agent_id: AgentId,
        *,
        roles: set[str],
        namespace: str = DEFAULT_NAMESPACE,
        lease_s: float = DEFAULT_LEASE_S,
    ) -> Task | None:
        """Take the next task for this agent: pushed assignments first, then the queue."""
        now = _now()
        pushed = await self._store.query(namespace=namespace, statuses=[TaskStatus.ASSIGNED],
                                         assigned_to=agent_id)
        for t in pushed:
            won = await self._try_start(t.task_id, agent_id, lease_s, count_attempt=False,
                                        expect={TaskStatus.ASSIGNED})
            if won:
                return won
        for t in await self._store.claimable(namespace=namespace, roles=roles, now=now):
            won = await self._try_start(t.task_id, agent_id, lease_s, count_attempt=True,
                                        expect={TaskStatus.PENDING})
            if won:
                return won
        return None

    async def claim_wait(
        self,
        agent_id: AgentId,
        *,
        roles: set[str],
        namespace: str = DEFAULT_NAMESPACE,
        lease_s: float = DEFAULT_LEASE_S,
        wait_s: float = 0.0,
    ) -> Task | None:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + max(0.0, wait_s)
        while True:
            self._work.clear()
            t = await self.claim(agent_id, roles=roles, namespace=namespace, lease_s=lease_s)
            remaining = deadline - loop.time()
            if t is not None or remaining <= 0:
                return t
            try:
                # Wake on local work; poll at least every second for other nodes.
                await asyncio.wait_for(self._work.wait(), timeout=min(remaining, 1.0))
            except asyncio.TimeoutError:
                pass

    async def _try_start(self, task_id: TaskId, agent_id: AgentId, lease_s: float, *,
                         count_attempt: bool, expect: set[TaskStatus]) -> Task | None:
        task = await self._store.get(task_id)
        if task is None or task.status not in expect:
            return None
        expected = task.version
        now = _now()
        task.status = TaskStatus.RUNNING
        task.assigned_to = agent_id
        task.assigned_at = task.assigned_at if not count_attempt else now
        task.started_at = now
        task.lease_id = f"lease-{uuid.uuid4().hex[:16]}"
        task.lease_expires_at = now + timedelta(seconds=lease_s)
        task.cancel_requested = False
        if count_attempt:
            task.attempt_count += 1
        task.attempts_log = (task.attempts_log + [{
            "attempt": task.attempt_count, "agent_id": agent_id, "started_at": now.isoformat(),
            "outcome": "running",
        }])[-MAX_ATTEMPT_LOG:]
        if not await self._store.save(task, expected):
            return None
        await self._emit(
            EventType.TASK_ASSIGNED, task,
            f"Task {task_id!r} claimed by {agent_id!r} (attempt {task.attempt_count}/{task.max_attempts})",
            agent_id=agent_id, payload={"lease_id": task.lease_id, "lease_s": lease_s},
        )
        await self._emit(EventType.TASK_STARTED, task, f"Task {task_id!r} is now running",
                         agent_id=agent_id)
        return task

    def _check_lease(self, t: Task, lease_id: str | None) -> None:
        if lease_id is not None and t.lease_id != lease_id:
            raise LeaseError(
                f"Lease {lease_id!r} is not current for task {t.task_id!r} "
                f"(task is {t.status.value}, held by {t.assigned_to!r})"
            )

    async def renew_lease(self, task_id: TaskId, lease_id: str, lease_s: float = DEFAULT_LEASE_S,
                          step: str | None = None) -> Task:
        def fn(t: Task) -> None:
            if t.status == TaskStatus.CANCELLED:
                return  # caller sees status and stops
            self._check_lease(t, lease_id)
            if t.status != TaskStatus.RUNNING:
                raise LeaseError(f"Task {task_id!r} is {t.status.value}")
            t.lease_expires_at = _now() + timedelta(seconds=lease_s)
            if step is not None:
                t.current_step = step[:200]

        task = await self._mutate(task_id, fn)
        if step is not None and task.status == TaskStatus.RUNNING:
            await self.update_progress(task_id, step)
        return task

    async def update_progress(self, task_id: TaskId, step: str) -> None:
        task = await self._get_or_raise(task_id)
        await self._emit(EventType.TASK_PROGRESS, task, f"Task {task_id!r}: {step}",
                         payload={"step": step})

    # ── Completion ────────────────────────────────────────────────────────────

    async def complete(self, task_id: TaskId, output: dict[str, Any] | None = None, *,
                       lease_id: str | None = None) -> Task:
        output = output or {}
        current = await self._get_or_raise(task_id)
        if current.status == TaskStatus.COMPLETED and lease_id is not None and current.lease_id == lease_id:
            return current  # idempotent retry of the same completion
        self._check_lease(current, lease_id)
        if current.output_schema:
            from jsonschema import Draft202012Validator

            errors = sorted(Draft202012Validator(current.output_schema).iter_errors(output),
                            key=lambda e: list(e.path))
            if errors:
                msg = "; ".join(
                    f"{'/'.join(map(str, e.path)) or '<root>'}: {e.message}" for e in errors[:5]
                )[:500]
                await self._emit(EventType.TASK_OUTPUT_INVALID, current,
                                 f"Task {task_id!r} output rejected: {msg}", payload={"errors": msg})
                failed = await self.fail(task_id, f"output_invalid: {msg}", lease_id=lease_id,
                                         error_class="output_invalid")
                raise OutputInvalid(failed, msg)

        def fn(t: Task) -> None:
            self._check_lease(t, lease_id)
            t.status = TaskStatus.COMPLETED
            t.completed_at = _now()
            t.output_data = output
            t.lease_expires_at = None
            t.not_before = None
            self._close_attempt(t, "completed")

        task = await self._mutate(task_id, fn)
        await self._emit(EventType.TASK_COMPLETED, task, f"Task {task_id!r} completed successfully")
        self._signal_done(task_id)
        await self._resolve_dependents(task)
        return task

    async def fail(
        self,
        task_id: TaskId,
        error: str,
        *,
        lease_id: str | None = None,
        transient: bool | None = None,
        error_class: str | None = None,
        status_code: int | None = None,
    ) -> Task:
        """
        Record a failed attempt. Transient failures are retried with full-jitter
        backoff until max_attempts; non-transient ones go straight to the DLQ.
        """
        retryable = transient if transient is not None else is_transient(error_class, status_code)
        attempt_before = 0

        def fn(t: Task) -> None:
            nonlocal attempt_before
            self._check_lease(t, lease_id)
            attempt_before = t.attempt_count
            t.status = TaskStatus.FAILED
            t.last_error = error[:2000]
            t.error_class = error_class
            t.lease_id = None
            t.lease_expires_at = None
            self._close_attempt(t, "failed", error=error[:300])

        task = await self._mutate(task_id, fn)
        await self._emit(
            EventType.TASK_FAILED, task, f"Task {task_id!r} failed: {error[:300]}",
            payload={"error": error[:500], "attempt": task.attempt_count, "max": task.max_attempts,
                     "error_class": error_class, "status_code": status_code, "transient": retryable},
        )

        if not retryable:
            return await self.send_to_dlq(task_id, f"Non-transient failure ({error_class or status_code}): {error[:300]}")
        if task.attempt_count >= task.max_attempts:
            return await self.send_to_dlq(
                task_id, f"Exhausted {task.max_attempts} attempts. Last error: {error[:300]}"
            )

        delay = full_jitter(attempt_before)

        def requeue(t: Task) -> None:
            if t.status != TaskStatus.FAILED:
                return
            # assigned_to is kept as "last worker" (attribution for storm
            # detection and the dashboard); the next claim overwrites it.
            t.status = TaskStatus.PENDING
            t.not_before = _now() + timedelta(seconds=delay)

        task = await self._mutate(task_id, requeue)
        await self._emit(
            EventType.TASK_RETRY_SCHEDULED, task,
            f"Task {task_id!r} retry {task.attempt_count + 1}/{task.max_attempts} in {delay:.1f}s",
            payload={"delay_s": round(delay, 3), "attempt": task.attempt_count},
        )
        self._signal_work()
        return task

    async def cancel(self, task_id: TaskId, reason: str = "cancelled") -> Task:
        def fn(t: Task) -> None:
            if t.status in TERMINAL:
                return
            t.cancel_requested = t.status == TaskStatus.RUNNING
            t.status = TaskStatus.CANCELLED
            t.last_error = reason[:500]
            self._close_attempt(t, "cancelled")

        task = await self._mutate(task_id, fn)
        await self._emit(EventType.TASK_CANCELLED, task, f"Task {task_id!r} cancelled: {reason}",
                         payload={"reason": reason})
        self._signal_done(task_id)
        await self._resolve_dependents(task)
        return task

    async def reassign(self, task_id: TaskId, new_agent_id: AgentId, reason: str) -> Task:
        old_agent: AgentId | None = None

        def fn(t: Task) -> None:
            nonlocal old_agent
            old_agent = t.assigned_to
            self._close_attempt(t, "reassigned")
            t.assigned_to = new_agent_id
            t.status = TaskStatus.ASSIGNED
            t.assigned_at = _now()
            t.attempt_count += 1
            t.lease_id = None
            t.lease_expires_at = None

        task = await self._mutate(task_id, fn)
        await self._emit(
            EventType.TASK_REASSIGNED, task,
            f"Task {task_id!r} reassigned from {old_agent} → {new_agent_id}: {reason}",
            agent_id=new_agent_id,
            payload={"from_agent": old_agent, "to_agent": new_agent_id, "reason": reason},
        )
        self._signal_work()
        return task

    async def release_agent_tasks(self, agent_id: AgentId, reason: str) -> list[Task]:
        """Requeue every in-flight task of an agent that restarted or left."""
        out = []
        for t in await self._store.query(statuses=[TaskStatus.RUNNING, TaskStatus.ASSIGNED],
                                         assigned_to=agent_id):
            out.append(await self.fail(t.task_id, reason, transient=True, error_class="agent_restarted"))
        return out

    @staticmethod
    def _close_attempt(t: Task, outcome: str, error: str | None = None) -> None:
        if t.attempts_log and t.attempts_log[-1].get("outcome") == "running":
            entry = dict(t.attempts_log[-1])
            entry.update({"outcome": outcome, "ended_at": _now().isoformat()})
            if error:
                entry["error"] = error
            t.attempts_log = t.attempts_log[:-1] + [entry]

    # ── Dependencies / quarantine ─────────────────────────────────────────────

    async def _resolve_dependents(self, root: Task) -> None:
        """Unblock dependents of a completed task; quarantine dependents of a dead one."""
        frontier = [root]
        while frontier:
            up = frontier.pop()
            blocked = [
                t for t in await self._store.query(namespace=up.namespace,
                                                   statuses=[TaskStatus.BLOCKED, TaskStatus.PENDING])
                if up.task_id in t.depends_on
            ]
            for dep in blocked:
                if up.status in DEAD:
                    reason = f"upstream {up.task_id} is {up.status.value}"

                    def q(t: Task, reason: str = reason) -> None:
                        if t.status in (TaskStatus.BLOCKED, TaskStatus.PENDING):
                            t.status = TaskStatus.QUARANTINED
                            t.quarantine_reason = reason

                    moved = await self._mutate(dep.task_id, q)
                    if moved.status == TaskStatus.QUARANTINED:
                        await self._emit(EventType.TASK_QUARANTINED, moved,
                                         f"Task {moved.task_id!r} quarantined: {reason}",
                                         payload={"reason": reason, "upstream": up.task_id})
                        self._signal_done(moved.task_id)
                        frontier.append(moved)
                elif up.status == TaskStatus.COMPLETED and dep.status == TaskStatus.BLOCKED:
                    deps = [await self._store.get(d) for d in dep.depends_on]
                    if all(d is not None and d.status == TaskStatus.COMPLETED for d in deps):
                        def u(t: Task) -> None:
                            if t.status == TaskStatus.BLOCKED:
                                t.status = TaskStatus.PENDING

                        moved = await self._mutate(dep.task_id, u)
                        await self._emit(EventType.TASK_UNBLOCKED, moved,
                                         f"Task {moved.task_id!r} unblocked (dependencies complete)")
                        self._signal_work()

    # ── Maintenance (leader duties, driven by control.scheduler) ─────────────

    async def expire_leases(self) -> list[Task]:
        out = []
        for t in await self._store.expired_leases(_now()):
            await self._emit(EventType.TASK_LEASE_EXPIRED, t,
                             f"Lease on task {t.task_id!r} held by {t.assigned_to!r} expired",
                             payload={"lease_id": t.lease_id})
            try:
                out.append(await self.fail(t.task_id, "lease expired (worker stopped renewing)",
                                           lease_id=t.lease_id, transient=True,
                                           error_class="lease_expired"))
            except (LeaseError, KeyError):
                continue
        return out

    async def enforce_deadlines(self) -> list[Task]:
        now = _now()
        out = []
        live = [TaskStatus.PENDING, TaskStatus.BLOCKED, TaskStatus.ASSIGNED, TaskStatus.RUNNING]
        for t in await self._store.query(statuses=live):
            if t.deadline is not None and t.deadline < now:
                out.append(await self.fail(t.task_id, f"deadline {t.deadline.isoformat()} exceeded",
                                           transient=False, error_class="deadline_exceeded"))
        return out

    async def wait_terminal(self, task_id: TaskId, timeout: float) -> Task:
        """Block until the task is terminal (or timeout); returns its latest state."""
        task = await self._get_or_raise(task_id)
        if task.is_terminal or timeout <= 0:
            return task
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        ev = self._done.setdefault(task_id, asyncio.Event())
        try:
            while True:
                remaining = deadline - loop.time()
                if remaining <= 0:
                    break
                ev.clear()
                try:
                    await asyncio.wait_for(ev.wait(), timeout=min(remaining, 1.0))
                except asyncio.TimeoutError:
                    pass
                task = await self._get_or_raise(task_id)
                if task.is_terminal:
                    break
        finally:
            self._done.pop(task_id, None)
        return task

    # ── DLQ ───────────────────────────────────────────────────────────────────

    async def send_to_dlq(self, task_id: TaskId, reason: str) -> Task:
        def fn(t: Task) -> None:
            t.status = TaskStatus.DEAD_LETTER
            t.dlq_reason = reason[:1000]
            t.dlq_at = _now()
            t.lease_id = None
            t.lease_expires_at = None
            self._close_attempt(t, "dead_letter")

        task = await self._mutate(task_id, fn)
        log.warning("Task %s → DLQ: %s", task.task_id, reason)
        await self._emit(EventType.TASK_QUEUED_DLQ, task,
                         f"Task {task.task_id!r} → Dead Letter Queue: {reason[:300]}",
                         payload={"reason": reason[:500]})
        self._signal_done(task_id)
        await self._resolve_dependents(task)
        return task

    # Back-compat name used by older call sites.
    async def _send_to_dlq(self, task: Task, reason: str) -> None:
        await self.send_to_dlq(task.task_id, reason)

    async def replay_from_dlq(self, task_id: TaskId, max_attempts: int = 3) -> Task | None:
        """Pull a dead-lettered (or quarantined) task out and reset it for replay."""
        task = await self._store.get(task_id)
        if task is None or task.status not in (TaskStatus.DEAD_LETTER, TaskStatus.QUARANTINED):
            return None
        deps = [await self._store.get(d) for d in task.depends_on]
        ready = all(d is not None and d.status == TaskStatus.COMPLETED for d in deps)

        def fn(t: Task) -> None:
            t.status = TaskStatus.PENDING if ready else TaskStatus.BLOCKED
            t.attempt_count = 0
            t.max_attempts = max_attempts
            t.dlq_reason = None
            t.dlq_at = None
            t.quarantine_reason = None
            t.assigned_to = None
            t.not_before = None

        task = await self._mutate(task_id, fn)
        # Lift quarantine on downstream work that only died because of this task.
        for dep in await self._store.query(namespace=task.namespace, statuses=[TaskStatus.QUARANTINED]):
            if task_id in dep.depends_on and dep.quarantine_reason and task_id in dep.quarantine_reason:
                await self.replay_from_dlq(dep.task_id, max_attempts=dep.max_attempts or max_attempts)
        log.info("Task %s replayed from DLQ", task_id)
        self._signal_work()
        return task

    async def discard_from_dlq(self, task_id: TaskId, reason: str = "discarded by operator") -> Task | None:
        """Permanently drop a dead-lettered task (marks it CANCELLED). None if not in DLQ."""
        task = await self._store.get(task_id)
        if task is None or task.status not in (TaskStatus.DEAD_LETTER, TaskStatus.QUARANTINED):
            return None

        def fn(t: Task) -> None:
            t.status = TaskStatus.CANCELLED
            t.dlq_reason = None
            t.dlq_at = None

        task = await self._mutate(task_id, fn)
        log.info("Task %s discarded from DLQ: %s", task_id, reason)
        await self._emit(EventType.TASK_CANCELLED, task, f"Task {task_id!r} discarded from DLQ: {reason}",
                         payload={"reason": reason, "from_dlq": True})
        return task

    # ── Queries ───────────────────────────────────────────────────────────────

    async def get(self, task_id: TaskId) -> Task | None:
        return await self._store.get(task_id)

    async def all(self, namespace: str | None = None) -> list[Task]:
        return await self._store.query(namespace=namespace)

    async def query(self, **kw: Any) -> list[Task]:
        return await self._store.query(**kw)

    async def pending(self) -> list[Task]:
        """Return pending tasks sorted by priority then creation time."""
        tasks = await self._store.query(statuses=[TaskStatus.PENDING])
        return sorted(tasks, key=lambda t: (t.priority, t.created_at))

    async def running(self) -> list[Task]:
        return await self._store.query(statuses=[TaskStatus.RUNNING])

    async def dlq_contents(self, namespace: str | None = None) -> list[Task]:
        items = await self._store.query(namespace=namespace,
                                        statuses=[TaskStatus.DEAD_LETTER, TaskStatus.QUARANTINED])
        return sorted(items, key=lambda t: t.dlq_at or t.created_at)

    async def for_agent(self, agent_id: AgentId) -> list[Task]:
        return await self._store.query(assigned_to=agent_id)

    async def by_trace(self, trace_id: str) -> list[Task]:
        return await self._store.query(trace_id=trace_id)

    async def stats(self, namespace: str | None = None) -> dict[str, int]:
        counts = await self._store.counts(namespace)
        counts["DLQ_depth"] = counts.get("DEAD_LETTER", 0) + counts.get("QUARANTINED", 0)
        return counts

    # ── Internals ─────────────────────────────────────────────────────────────

    async def _get_or_raise(self, task_id: TaskId) -> Task:
        task = await self._store.get(task_id)
        if task is None:
            raise KeyError(f"Unknown task: {task_id!r}")
        return task
