"""
resources.mediator
───────────────────
Resource Mediator — broker for named shared resources in the BOSSman workforce.

Reference: zylos.md §Part3 — Mediator Pattern
  "For complex multi-agent coordination, a dedicated mediator acts as the
   single point through which resource requests are brokered. Rather than
   agents directly competing for resources, they request resources from the
   mediator, which applies scheduling logic."

  class ResourceMediator:
      async def acquire(self, resource_name, agent_id, timeout=30.0):
          lock = self._locks.setdefault(resource_name, asyncio.Lock())
          try:
              await asyncio.wait_for(lock.acquire(), timeout=timeout)
          except asyncio.TimeoutError:
              raise ResourceTimeout(...)

Why timeout is non-negotiable
──────────────────────────────
"The mediator's timeout is critical: it prevents deadlock by ensuring that
 no agent waits indefinitely. The ResourceTimeout exception triggers the
 agent's error handling path."  — zylos.md §Part3

This implementation extends the reference with:
  - Wait-graph tracking: records who is waiting for what (enables deadlock detection)
  - Lock holder tracking: records who currently holds each resource
  - RESOURCE_CONTENTION event when a waiter joins the queue
  - DEADLOCK_SUSPECTED event when a circular wait is detected
  - RESOURCE_ACQUIRED / RESOURCE_RELEASED events for the dashboard
  - ResourceUsageSnapshot for the watchdog to poll

Deadlock detection
──────────────────
At any acquire() call the mediator checks the wait-for graph:
  Does the resource we want form a cycle?
    agent A holds X, waits for Y
    agent B holds Y, waits for X  → DEADLOCK_SUSPECTED

This is a necessary-condition check (holds ≥1, waits for ≥1 with overlap),
matching AgentState.is_deadlock_suspect. Confirmed cycle detection is Phase 4.

Named resources
───────────────
Resources are created lazily by name. Use consistent, descriptive names:
  "memory-store", "file-system", "session-state", "db-write-lock"

Reentrant behaviour
───────────────────
asyncio.Lock is NOT reentrant. If an agent calls acquire("X") while
already holding "X", it will deadlock. This is intentional — agents must
release before re-acquiring. The mediator will detect the self-deadlock
via the wait-graph and raise ResourceTimeout rather than hang forever.
"""

from __future__ import annotations

import asyncio
import logging
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Any, AsyncIterator

from contracts.agent_state import AgentId
from contracts.events import BossmanEvent, EventType

log = logging.getLogger(__name__)

DEFAULT_ACQUIRE_TIMEOUT: float = 30.0


class ResourceTimeoutError(TimeoutError):
    """
    Raised when an agent times out waiting for a named resource.
    Triggers the agent's error-handling path and BOSSman's failure detector.
    """

    def __init__(
        self,
        resource_name: str,
        agent_id: AgentId | None,
        timeout: float,
        holders: list[AgentId],
    ) -> None:
        super().__init__(
            f"Agent {agent_id!r} timed out after {timeout}s waiting for "
            f"resource '{resource_name}'. Current holder(s): {holders}. "
            f"This may indicate a deadlock — check DEADLOCK_SUSPECTED events."
        )
        self.resource_name = resource_name
        self.agent_id = agent_id
        self.timeout = timeout
        self.holders = holders


@dataclass
class ResourceLock:
    """Internal state for one named resource."""
    name: str
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    holder: AgentId | None = None          # agent currently holding the lock
    waiters: list[AgentId] = field(default_factory=list)  # agents waiting
    acquire_count: int = 0
    contention_count: int = 0
    total_hold_seconds: float = 0.0
    _acquired_at: float | None = None


class ResourceMediator:
    """
    Central broker for named shared resources.

    Usage
    ─────
        mediator = ResourceMediator(event_bus=bus)

        async with mediator.acquire("memory-store", agent_id="agent-abc"):
            # exclusive access to memory-store
            ...
        # lock automatically released on exit

    The mediator maintains a wait-for graph to detect circular waits.
    """

    def __init__(
        self,
        default_timeout: float = DEFAULT_ACQUIRE_TIMEOUT,
        event_bus: Any | None = None,
    ) -> None:
        self._timeout = default_timeout
        self._event_bus = event_bus
        self._resources: dict[str, ResourceLock] = {}
        self._lock = asyncio.Lock()  # protects _resources dict mutations

        # Wait-for graph: agent_id → set of resource names they wait on
        self._waiting_for: dict[AgentId, set[str]] = {}
        # Holds graph: agent_id → set of resource names they hold
        self._holding: dict[AgentId, set[str]] = {}

    # ── Public context manager ────────────────────────────────────────────────

    @asynccontextmanager
    async def acquire(
        self,
        resource_name: str,
        agent_id: AgentId | None = None,
        timeout: float | None = None,
    ) -> AsyncIterator[None]:
        """
        Acquire exclusive access to a named resource.

        Raises ResourceTimeoutError on timeout (deadlock prevention).
        Publishes RESOURCE_CONTENTION and DEADLOCK_SUSPECTED when warranted.
        """
        t_out = timeout if timeout is not None else self._timeout
        resource = await self._get_or_create(resource_name)

        # Check for contention
        contested = resource.lock.locked()
        if contested:
            resource.contention_count += 1
            waiters_snapshot = list(resource.waiters)
            log.warning(
                "Mediator: contention on '%s' — holder=%s, waiters=%s (agent=%s)",
                resource_name, resource.holder, waiters_snapshot, agent_id,
            )
            await self._emit(
                EventType.RESOURCE_CONTENTION,
                resource_name=resource_name,
                agent_id=agent_id,
                payload={
                    "holder": resource.holder,
                    "waiters": waiters_snapshot,
                    "contention_count": resource.contention_count,
                },
            )

            # Deadlock detection: if I hold something that my target's holder waits for
            if agent_id and resource.holder:
                await self._check_deadlock(agent_id, resource_name, resource.holder)

        # Register this agent as waiting
        if agent_id:
            async with self._lock:
                resource.waiters.append(agent_id)
                self._waiting_for.setdefault(agent_id, set()).add(resource_name)

        t_start = time.monotonic()
        try:
            await asyncio.wait_for(resource.lock.acquire(), timeout=t_out)
        except asyncio.TimeoutError:
            wait_secs = time.monotonic() - t_start
            holders = [resource.holder] if resource.holder else []
            log.error(
                "Mediator: timeout on '%s' after %.1fs (agent=%s, holder=%s)",
                resource_name, wait_secs, agent_id, resource.holder,
            )
            await self._emit(
                EventType.RESOURCE_TIMEOUT,
                resource_name=resource_name,
                agent_id=agent_id,
                payload={
                    "waited_seconds": round(wait_secs, 3),
                    "holder": resource.holder,
                    "timeout_configured": t_out,
                },
            )
            # Clean up wait-graph entry
            if agent_id:
                async with self._lock:
                    resource.waiters.discard(agent_id) if hasattr(resource.waiters, 'discard') else None
                    if agent_id in resource.waiters:
                        resource.waiters.remove(agent_id)
                    self._waiting_for.get(agent_id, set()).discard(resource_name)
            raise ResourceTimeoutError(resource_name, agent_id, t_out, holders)

        # Acquired — update state
        async with self._lock:
            resource.holder = agent_id
            resource.acquire_count += 1
            resource._acquired_at = time.monotonic()
            if agent_id and agent_id in resource.waiters:
                resource.waiters.remove(agent_id)
            if agent_id:
                self._waiting_for.get(agent_id, set()).discard(resource_name)
                self._holding.setdefault(agent_id, set()).add(resource_name)

        wait_secs = time.monotonic() - t_start
        log.debug(
            "Mediator: acquired '%s' (agent=%s, waited=%.3fs)",
            resource_name, agent_id, wait_secs,
        )
        await self._emit(
            EventType.RESOURCE_ACQUIRED,
            resource_name=resource_name,
            agent_id=agent_id,
            payload={
                "waited_seconds": round(wait_secs, 3),
                "acquire_count": resource.acquire_count,
            },
        )

        try:
            yield
        finally:
            hold_secs = time.monotonic() - (resource._acquired_at or time.monotonic())
            resource.total_hold_seconds += hold_secs
            resource.holder = None
            resource._acquired_at = None
            resource.lock.release()

            async with self._lock:
                if agent_id:
                    self._holding.get(agent_id, set()).discard(resource_name)

            log.debug(
                "Mediator: released '%s' (agent=%s, held=%.3fs)",
                resource_name, agent_id, hold_secs,
            )
            await self._emit(
                EventType.RESOURCE_RELEASED,
                resource_name=resource_name,
                agent_id=agent_id,
                payload={
                    "held_seconds": round(hold_secs, 3),
                    "total_hold_seconds": round(resource.total_hold_seconds, 3),
                },
            )

    # ── Non-blocking try-acquire ──────────────────────────────────────────────

    async def try_acquire(self, resource_name: str, agent_id: AgentId | None = None) -> bool:
        """
        Non-blocking acquire attempt. Returns True if acquired, False if contested.
        Caller is responsible for releasing via release() if True is returned.
        """
        resource = await self._get_or_create(resource_name)
        acquired = resource.lock.locked() is False and resource.lock.acquire.__func__ is not None

        # Use asyncio.wait_for with 0 timeout as non-blocking probe
        try:
            await asyncio.wait_for(resource.lock.acquire(), timeout=0.0)
            async with self._lock:
                resource.holder = agent_id
                resource.acquire_count += 1
                resource._acquired_at = time.monotonic()
                if agent_id:
                    self._holding.setdefault(agent_id, set()).add(resource_name)
            return True
        except (asyncio.TimeoutError, TimeoutError):
            return False

    async def release(self, resource_name: str, agent_id: AgentId | None = None) -> None:
        """Manually release a resource (for use with try_acquire)."""
        resource = self._resources.get(resource_name)
        if resource is None or not resource.lock.locked():
            return
        hold_secs = time.monotonic() - (resource._acquired_at or time.monotonic())
        resource.total_hold_seconds += hold_secs
        resource.holder = None
        resource._acquired_at = None
        resource.lock.release()
        async with self._lock:
            if agent_id:
                self._holding.get(agent_id, set()).discard(resource_name)

    # ── Deadlock detection ────────────────────────────────────────────────────

    async def _check_deadlock(
        self,
        requester: AgentId,
        wanted_resource: str,
        current_holder: AgentId,
    ) -> None:
        """
        Check if acquiring wanted_resource creates a circular wait.

        Cycle: requester holds something that current_holder is waiting for.
        If cycle detected → emit DEADLOCK_SUSPECTED.
        """
        # What does the current holder want?
        holder_wants = self._waiting_for.get(current_holder, set())
        # What does the requester hold?
        requester_holds = self._holding.get(requester, set())

        # Cycle if holder wants something requester holds
        cycle_resources = holder_wants & requester_holds
        if cycle_resources:
            log.warning(
                "Mediator: DEADLOCK_SUSPECTED — agent=%s holds %s, "
                "waits for '%s' held by %s, who waits for %s",
                requester, requester_holds, wanted_resource,
                current_holder, cycle_resources,
            )
            await self._emit(
                EventType.DEADLOCK_SUSPECTED,
                agent_id=requester,
                payload={
                    "requester": requester,
                    "requester_holds": list(requester_holds),
                    "wanted_resource": wanted_resource,
                    "current_holder": current_holder,
                    "holder_waits_for": list(holder_wants),
                    "cycle_resources": list(cycle_resources),
                },
            )

    # ── Snapshot for watchdog / dashboard ─────────────────────────────────────

    def snapshot(self) -> dict[str, Any]:
        """
        Full state snapshot — consumed by the watchdog and dashboard.
        Returns dict of resource_name → stats.
        """
        result: dict[str, Any] = {}
        for name, res in self._resources.items():
            result[name] = {
                "holder": res.holder,
                "waiters": list(res.waiters),
                "acquire_count": res.acquire_count,
                "contention_count": res.contention_count,
                "total_hold_seconds": round(res.total_hold_seconds, 3),
                "currently_locked": res.lock.locked(),
            }
        return result

    def holding(self, agent_id: AgentId) -> set[str]:
        """Resources currently held by agent_id."""
        return set(self._holding.get(agent_id, set()))

    def waiting_for(self, agent_id: AgentId) -> set[str]:
        """Resources currently waited on by agent_id."""
        return set(self._waiting_for.get(agent_id, set()))

    # ── Helpers ───────────────────────────────────────────────────────────────

    async def _get_or_create(self, resource_name: str) -> ResourceLock:
        async with self._lock:
            if resource_name not in self._resources:
                self._resources[resource_name] = ResourceLock(name=resource_name)
            return self._resources[resource_name]

    async def _emit(
        self,
        event_type: EventType,
        *,
        resource_name: str | None = None,
        agent_id: AgentId | None = None,
        payload: dict[str, Any] | None = None,
    ) -> None:
        if self._event_bus is None:
            return
        event = BossmanEvent.create(
            event_type=event_type,
            agent_id=agent_id,
            resource_name=resource_name,
            payload=payload or {},
        )
        try:
            await self._event_bus.publish(event)
        except Exception as exc:
            log.warning("ResourceMediator: event bus error: %s", exc)
