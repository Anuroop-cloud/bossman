"""
core.subagent_manager
──────────────────────
Idempotency guard for subagent spawning.

This is the single most impactful fix for the most common class of agent
deadlock. From zylos.md §Part2:

  "The most impactful single change for preventing deadlock in multi-agent
   systems is often the simplest: checking whether the work is already
   being done before starting it again."

  Real incident: context-monitor fires every 6 minutes, spawns a memory-sync
  subagent each time without checking if one is running. Two subagents contend
  for the same memory files, token budget, and session state. System hangs
  for 35 minutes. Fix: one idempotency check. O(1) overhead.

SubagentManager ensures:
  - Only ONE coroutine runs per logical task_id at a time
  - Duplicate spawn attempts are blocked and logged as events
  - Completed tasks are cleaned up automatically
  - The manager tracks spawn history for the Failure Detector
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any, Callable, Coroutine

from contracts.agent_state import AgentId, TaskId
from contracts.events import BossmanEvent, EventType
from core.event_bus import EventBus

log = logging.getLogger(__name__)

CoroFactory = Callable[[], Coroutine[Any, Any, Any]]


class SpawnRecord:
    """Metadata about a running spawned coroutine."""

    def __init__(self, task_id: str, agent_id: AgentId | None, asyncio_task: asyncio.Task) -> None:
        self.task_id = task_id
        self.agent_id = agent_id
        self.asyncio_task = asyncio_task
        self.spawned_at: datetime = datetime.now(timezone.utc)
        self.blocked_attempts: int = 0  # how many duplicate spawns were blocked


class SubagentManager:
    """
    Idempotency guard for subagent spawning.

    Usage
    ─────
        manager = SubagentManager(bus)

        spawned = await manager.spawn_if_not_running(
            task_id="memory-sync",
            coro_factory=lambda: run_memory_sync(),
        )
        if not spawned:
            log.info("memory-sync already running, skipping")

    The task_id is keyed on LOGICAL task type, not invocation parameters.
    One "memory-sync" task should run at a time regardless of how many
    events trigger it.
    """

    def __init__(self, bus: EventBus) -> None:
        self._bus = bus
        self._running: dict[str, SpawnRecord] = {}
        self._lock = asyncio.Lock()
        self._total_spawned: int = 0
        self._total_blocked: int = 0

    async def spawn_if_not_running(
        self,
        task_id: str,
        coro_factory: CoroFactory,
        agent_id: AgentId | None = None,
    ) -> bool:
        """
        Spawn a coroutine only if no task with the same task_id is running.

        Returns True  → spawned successfully.
        Returns False → already running, spawn was blocked.

        The task_id is a logical key (e.g. "memory-sync", "health-probe-agent-7").
        """
        async with self._lock:
            # Clean up any completed tasks first
            self._reap_completed()

            if task_id in self._running:
                # Duplicate spawn blocked — this is the idempotency guard
                record = self._running[task_id]
                record.blocked_attempts += 1
                self._total_blocked += 1
                blocked = record.blocked_attempts
                spawned_at = record.spawned_at.strftime("%H:%M:%S")
                log.info(
                    "Duplicate spawn blocked: %r already running (blocked_attempts=%d)",
                    task_id,
                    blocked,
                )
                # Publish OUTSIDE the lock to avoid deadlock with async subscribers
                # but we capture values inside the lock first
                _should_block = True
                _block_payload = {
                    "task_id": task_id,
                    "blocked_attempts": blocked,
                    "running_since": spawned_at,
                }
            else:
                _should_block = False
                _block_payload = {}

        if _should_block:
            await self._bus.publish(
                BossmanEvent.create(
                    EventType.DUPLICATE_SPAWN_BLOCKED,
                    message=f"Duplicate spawn of {task_id!r} blocked "
                            f"(running since {_block_payload['running_since']})",
                    agent_id=agent_id,
                    payload=_block_payload,
                )
            )
            return False

        asyncio_task = asyncio.create_task(
            self._run_with_cleanup(task_id, coro_factory),
            name=f"subagent:{task_id}",
        )
        record = SpawnRecord(task_id=task_id, agent_id=agent_id, asyncio_task=asyncio_task)

        async with self._lock:
            self._running[task_id] = record
            self._total_spawned += 1

        log.info("Spawned subagent task %r (total_spawned=%d)", task_id, self._total_spawned)
        return True

    async def _run_with_cleanup(self, task_id: str, coro_factory: CoroFactory) -> Any:
        """Run the coroutine and remove the record when done regardless of outcome."""
        try:
            return await coro_factory()
        except Exception:
            log.exception("Subagent task %r raised an exception", task_id)
            raise
        finally:
            async with self._lock:
                self._running.pop(task_id, None)
            log.debug("Subagent task %r finished and removed from registry", task_id)

    def _reap_completed(self) -> None:
        """Remove records for done asyncio tasks (call inside lock)."""
        done = [tid for tid, rec in self._running.items() if rec.asyncio_task.done()]
        for tid in done:
            self._running.pop(tid)

    async def cancel(self, task_id: str) -> bool:
        """Cancel a running subagent task by logical task_id."""
        async with self._lock:
            record = self._running.get(task_id)
            if record is None:
                return False
            record.asyncio_task.cancel()
        log.info("Cancelled subagent task %r", task_id)
        return True

    def is_running(self, task_id: str) -> bool:
        self._reap_completed()  # best-effort without lock, fine for read
        return task_id in self._running

    @property
    def running_task_ids(self) -> list[str]:
        return list(self._running.keys())

    @property
    def stats(self) -> dict[str, int]:
        return {
            "currently_running": len(self._running),
            "total_spawned": self._total_spawned,
            "total_blocked": self._total_blocked,
        }
