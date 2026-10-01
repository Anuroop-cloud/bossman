"""
core.task_manager
─────────────────
TaskManager — create, assign, and track work items across the workforce.

Tasks are the unit of work BOSSman hands to agents. The task manager:
  - Creates tasks with priorities and deadlines
  - Assigns tasks to available agents
  - Tracks task state through its lifecycle
  - Moves exhausted tasks to the Dead Letter Queue (DLQ)
  - Publishes events at every state change

The DLQ (zylos.md §Part8) ensures that no work is silently dropped.
Every task that fails after exhausting retries lands in the DLQ for
inspection, replay, or manual discard — never disappears.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from datetime import datetime, timezone
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field

from contracts.agent_state import AgentId, TaskId, new_task_id
from contracts.events import BossmanEvent, EventType
from core.event_bus import EventBus

log = logging.getLogger(__name__)


class TaskStatus(str, Enum):
    PENDING = "PENDING"          # Created, waiting for assignment
    ASSIGNED = "ASSIGNED"        # Handed to an agent
    RUNNING = "RUNNING"          # Agent has picked it up
    COMPLETED = "COMPLETED"      # Finished successfully
    FAILED = "FAILED"            # Failed, may be retried
    CANCELLED = "CANCELLED"      # Explicitly cancelled
    DEAD_LETTER = "DEAD_LETTER"  # Exhausted retries → DLQ


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

    # Timing
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    assigned_at: datetime | None = None
    started_at: datetime | None = None
    completed_at: datetime | None = None
    deadline: datetime | None = None

    # Task payload — arbitrary data the agent receives
    input_data: dict[str, Any] = Field(default_factory=dict)
    output_data: dict[str, Any] = Field(default_factory=dict)

    # DLQ metadata
    dlq_reason: str | None = None
    dlq_at: datetime | None = None

    @property
    def is_terminal(self) -> bool:
        return self.status in {TaskStatus.COMPLETED, TaskStatus.CANCELLED, TaskStatus.DEAD_LETTER}

    @property
    def can_retry(self) -> bool:
        return self.attempt_count < self.max_attempts and self.status == TaskStatus.FAILED

    @property
    def age_seconds(self) -> float:
        return (datetime.now(timezone.utc) - self.created_at).total_seconds()


class TaskManager:
    """
    Create, assign, and lifecycle-manage tasks across the BOSSman workforce.

    Usage
    ─────
        task = await tm.create("Research quantum computing", required_role="researcher")
        await tm.assign(task.task_id, agent_id)
        await tm.mark_running(task.task_id)
        await tm.complete(task.task_id, output={"summary": "..."})
    """

    def __init__(self, bus: EventBus) -> None:
        self._bus = bus
        self._tasks: dict[TaskId, Task] = {}
        self._dlq: list[Task] = []
        self._lock = asyncio.Lock()

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
    ) -> Task:
        task = Task(
            title=title,
            description=description,
            priority=priority,
            required_role=required_role,
            max_attempts=max_attempts,
            input_data=input_data or {},
            deadline=deadline,
        )
        async with self._lock:
            self._tasks[task.task_id] = task

        log.info("Task created: %s [%s]", task.task_id, title)
        await self._bus.publish(
            BossmanEvent.create(
                EventType.TASK_CREATED,
                message=f"Task created: {title!r} (priority={priority.name})",
                task_id=task.task_id,
                payload={"title": title, "priority": priority.name, "required_role": required_role},
            )
        )
        return task

    # ── Assignment ────────────────────────────────────────────────────────────

    async def assign(self, task_id: TaskId, agent_id: AgentId) -> Task:
        task = await self._get_or_raise(task_id)
        async with self._lock:
            task.assigned_to = agent_id
            task.assigned_at = datetime.now(timezone.utc)
            task.status = TaskStatus.ASSIGNED
            task.attempt_count += 1

        await self._bus.publish(
            BossmanEvent.create(
                EventType.TASK_ASSIGNED,
                message=f"Task {task_id!r} assigned to agent {agent_id!r} "
                        f"(attempt {task.attempt_count}/{task.max_attempts})",
                task_id=task_id,
                agent_id=agent_id,
            )
        )
        return task

    async def mark_running(self, task_id: TaskId) -> Task:
        task = await self._get_or_raise(task_id)
        async with self._lock:
            task.status = TaskStatus.RUNNING
            task.started_at = datetime.now(timezone.utc)

        await self._bus.publish(
            BossmanEvent.create(
                EventType.TASK_STARTED,
                message=f"Task {task_id!r} is now running",
                task_id=task_id,
                agent_id=task.assigned_to,
            )
        )
        return task

    async def update_progress(self, task_id: TaskId, step: str) -> None:
        task = await self._get_or_raise(task_id)
        await self._bus.publish(
            BossmanEvent.create(
                EventType.TASK_PROGRESS,
                message=f"Task {task_id!r}: {step}",
                task_id=task_id,
                agent_id=task.assigned_to,
                payload={"step": step},
            )
        )

    # ── Completion ────────────────────────────────────────────────────────────

    async def complete(self, task_id: TaskId, output: dict[str, Any] | None = None) -> Task:
        task = await self._get_or_raise(task_id)
        async with self._lock:
            task.status = TaskStatus.COMPLETED
            task.completed_at = datetime.now(timezone.utc)
            task.output_data = output or {}

        await self._bus.publish(
            BossmanEvent.create(
                EventType.TASK_COMPLETED,
                message=f"Task {task_id!r} completed successfully",
                task_id=task_id,
                agent_id=task.assigned_to,
            )
        )
        return task

    async def fail(self, task_id: TaskId, error: str) -> Task:
        """
        Mark task as failed. If retries are exhausted, move to DLQ.
        """
        task = await self._get_or_raise(task_id)
        async with self._lock:
            task.status = TaskStatus.FAILED
            task.last_error = error

        await self._bus.publish(
            BossmanEvent.create(
                EventType.TASK_FAILED,
                message=f"Task {task_id!r} failed: {error}",
                task_id=task_id,
                agent_id=task.assigned_to,
                payload={"error": error, "attempt": task.attempt_count, "max": task.max_attempts},
            )
        )

        if not task.can_retry:
            await self._send_to_dlq(task, reason=f"Exhausted {task.max_attempts} attempts. Last error: {error}")

        return task

    async def cancel(self, task_id: TaskId, reason: str = "cancelled") -> Task:
        task = await self._get_or_raise(task_id)
        async with self._lock:
            task.status = TaskStatus.CANCELLED

        await self._bus.publish(
            BossmanEvent.create(
                EventType.TASK_CANCELLED,
                message=f"Task {task_id!r} cancelled: {reason}",
                task_id=task_id,
                agent_id=task.assigned_to,
                payload={"reason": reason},
            )
        )
        return task

    async def reassign(self, task_id: TaskId, new_agent_id: AgentId, reason: str) -> Task:
        task = await self._get_or_raise(task_id)
        old_agent = task.assigned_to
        async with self._lock:
            task.assigned_to = new_agent_id
            task.status = TaskStatus.ASSIGNED
            task.assigned_at = datetime.now(timezone.utc)
            task.attempt_count += 1

        await self._bus.publish(
            BossmanEvent.create(
                EventType.TASK_REASSIGNED,
                message=f"Task {task_id!r} reassigned from {old_agent} → {new_agent_id}: {reason}",
                task_id=task_id,
                agent_id=new_agent_id,
                payload={"from_agent": old_agent, "to_agent": new_agent_id, "reason": reason},
            )
        )
        return task

    # ── DLQ ───────────────────────────────────────────────────────────────────

    async def _send_to_dlq(self, task: Task, reason: str) -> None:
        async with self._lock:
            task.status = TaskStatus.DEAD_LETTER
            task.dlq_reason = reason
            task.dlq_at = datetime.now(timezone.utc)
            self._dlq.append(task)

        log.warning("Task %s → DLQ: %s", task.task_id, reason)
        await self._bus.publish(
            BossmanEvent.create(
                EventType.TASK_QUEUED_DLQ,
                message=f"Task {task.task_id!r} → Dead Letter Queue: {reason}",
                task_id=task.task_id,
                agent_id=task.assigned_to,
                payload={"reason": reason},
            )
        )

    async def replay_from_dlq(self, task_id: TaskId, max_attempts: int = 3) -> Task | None:
        """Pull a task out of the DLQ and reset it for replay."""
        async with self._lock:
            task = next((t for t in self._dlq if t.task_id == task_id), None)
            if task is None:
                return None
            self._dlq.remove(task)
            task.status = TaskStatus.PENDING
            task.attempt_count = 0
            task.max_attempts = max_attempts
            task.dlq_reason = None
            task.dlq_at = None
            task.assigned_to = None

        log.info("Task %s replayed from DLQ", task_id)
        return task

    # ── Queries ───────────────────────────────────────────────────────────────

    async def get(self, task_id: TaskId) -> Task | None:
        return self._tasks.get(task_id)

    async def pending(self) -> list[Task]:
        """Return pending tasks sorted by priority then creation time."""
        tasks = [t for t in self._tasks.values() if t.status == TaskStatus.PENDING]
        return sorted(tasks, key=lambda t: (t.priority, t.created_at))

    async def running(self) -> list[Task]:
        return [t for t in self._tasks.values() if t.status == TaskStatus.RUNNING]

    async def dlq_contents(self) -> list[Task]:
        return list(self._dlq)

    async def for_agent(self, agent_id: AgentId) -> list[Task]:
        return [t for t in self._tasks.values() if t.assigned_to == agent_id]

    async def stats(self) -> dict[str, int]:
        counts: dict[str, int] = {s.value: 0 for s in TaskStatus}
        for t in self._tasks.values():
            counts[t.status.value] += 1
        counts["DLQ_depth"] = len(self._dlq)
        return counts

    # ── Internals ─────────────────────────────────────────────────────────────

    async def _get_or_raise(self, task_id: TaskId) -> Task:
        task = self._tasks.get(task_id)
        if task is None:
            raise KeyError(f"Unknown task: {task_id!r}")
        return task
