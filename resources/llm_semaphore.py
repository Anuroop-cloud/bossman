"""
resources.llm_semaphore
────────────────────────
Concurrency cap for all LLM calls made by BOSSman-managed agents.

Reference: zylos.md §Part3 — Semaphores and Token Buckets
  "Semaphores bound the number of concurrent operations:
   llm_semaphore = asyncio.Semaphore(20)  # Anthropic rate limit tier
   async with llm_semaphore:
       return await anthropic_client.messages.create(...)"

Why this matters
────────────────
When many agents run concurrently each making LLM calls, the system can
saturate the provider's concurrency tier and start receiving 429s. The
semaphore ensures at most N calls are in-flight at any time, independent
of how many agents are running.

Relationship to TokenBucket
────────────────────────────
Semaphore:   controls *concurrent slots* (how many calls are in flight).
TokenBucket: controls *throughput rate* (calls per second / tokens per second).
Both are needed for complete protection — see resources.token_bucket.

Events published
────────────────
RESOURCE_ACQUIRED   — a slot was acquired (caller may proceed)
RESOURCE_CONTENTION — caller had to wait (semaphore was at capacity)
RESOURCE_RELEASED   — slot returned after call completes
"""

from __future__ import annotations

import asyncio
import logging
import time
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator

from contracts.agent_state import AgentId
from contracts.events import BossmanEvent, EventType

log = logging.getLogger(__name__)

# How long (seconds) to wait for a semaphore slot before giving up
DEFAULT_ACQUIRE_TIMEOUT: float = 60.0


class SemaphoreTimeoutError(TimeoutError):
    """Raised when a caller times out waiting for an LLM semaphore slot."""

    def __init__(self, resource: str, agent_id: AgentId | None, timeout: float) -> None:
        super().__init__(
            f"Agent {agent_id!r} timed out after {timeout}s waiting for semaphore '{resource}'. "
            f"System may be overloaded — consider raising the concurrency cap or backing off."
        )
        self.resource = resource
        self.agent_id = agent_id
        self.timeout = timeout


class LLMSemaphore:
    """
    Named concurrency cap for LLM API calls.

    Usage
    ─────
        sem = LLMSemaphore(max_concurrent=20, name="anthropic")

        async with sem.acquire(agent_id="agent-abc"):
            response = await anthropic_client.messages.create(...)

    The context manager handles acquire + release + event publishing.
    Raises SemaphoreTimeoutError if no slot is available within timeout.
    """

    def __init__(
        self,
        max_concurrent: int = 20,
        *,
        name: str = "llm",
        acquire_timeout: float = DEFAULT_ACQUIRE_TIMEOUT,
        event_bus: Any | None = None,
    ) -> None:
        if max_concurrent < 1:
            raise ValueError(f"max_concurrent must be ≥ 1, got {max_concurrent}")
        self._sem = asyncio.Semaphore(max_concurrent)
        self._max = max_concurrent
        self._name = name
        self._timeout = acquire_timeout
        self._event_bus = event_bus

        # Metrics
        self._total_acquired: int = 0
        self._total_contentions: int = 0
        self._current_holders: int = 0
        self._peak_holders: int = 0

    # ── Public context manager ────────────────────────────────────────────────

    @asynccontextmanager
    async def acquire(
        self,
        agent_id: AgentId | None = None,
        timeout: float | None = None,
    ) -> AsyncIterator[None]:
        """
        Acquire one LLM concurrency slot.

        Publishes RESOURCE_CONTENTION if the semaphore is at capacity.
        Publishes RESOURCE_ACQUIRED once the slot is obtained.
        Publishes RESOURCE_RELEASED on exit.
        Raises SemaphoreTimeoutError on timeout.
        """
        t_out = timeout if timeout is not None else self._timeout
        was_contested = self._current_holders >= self._max

        if was_contested:
            self._total_contentions += 1
            log.warning(
                "LLMSemaphore[%s]: contention — %d/%d slots in use (agent=%s)",
                self._name, self._current_holders, self._max, agent_id,
            )
            await self._emit(
                EventType.RESOURCE_CONTENTION,
                agent_id=agent_id,
                payload={
                    "semaphore": self._name,
                    "current_holders": self._current_holders,
                    "max_concurrent": self._max,
                },
            )

        t_start = time.monotonic()
        try:
            acquired = await asyncio.wait_for(self._sem.acquire(), timeout=t_out)
        except asyncio.TimeoutError:
            wait_secs = time.monotonic() - t_start
            log.error(
                "LLMSemaphore[%s]: timeout after %.1fs (agent=%s)",
                self._name, wait_secs, agent_id,
            )
            await self._emit(
                EventType.RESOURCE_TIMEOUT,
                agent_id=agent_id,
                payload={
                    "semaphore": self._name,
                    "waited_seconds": round(wait_secs, 3),
                    "timeout_configured": t_out,
                },
            )
            raise SemaphoreTimeoutError(self._name, agent_id, t_out)

        # Slot acquired
        self._total_acquired += 1
        self._current_holders += 1
        self._peak_holders = max(self._peak_holders, self._current_holders)
        wait_secs = time.monotonic() - t_start

        log.debug(
            "LLMSemaphore[%s]: acquired (agent=%s, waited=%.3fs, holders=%d/%d)",
            self._name, agent_id, wait_secs, self._current_holders, self._max,
        )
        await self._emit(
            EventType.RESOURCE_ACQUIRED,
            agent_id=agent_id,
            payload={
                "semaphore": self._name,
                "waited_seconds": round(wait_secs, 3),
                "current_holders": self._current_holders,
            },
        )

        try:
            yield
        finally:
            self._sem.release()
            self._current_holders -= 1
            log.debug(
                "LLMSemaphore[%s]: released (agent=%s, holders=%d/%d)",
                self._name, agent_id, self._current_holders, self._max,
            )
            await self._emit(
                EventType.RESOURCE_RELEASED,
                agent_id=agent_id,
                payload={
                    "semaphore": self._name,
                    "current_holders": self._current_holders,
                },
            )

    # ── Metrics ───────────────────────────────────────────────────────────────

    @property
    def stats(self) -> dict[str, Any]:
        """Live metrics snapshot for the dashboard."""
        return {
            "name": self._name,
            "max_concurrent": self._max,
            "current_holders": self._current_holders,
            "peak_holders": self._peak_holders,
            "total_acquired": self._total_acquired,
            "total_contentions": self._total_contentions,
            "contention_rate": (
                self._total_contentions / self._total_acquired
                if self._total_acquired else 0.0
            ),
        }

    @property
    def available_slots(self) -> int:
        return self._max - self._current_holders

    # ── Event helper ──────────────────────────────────────────────────────────

    async def _emit(
        self,
        event_type: EventType,
        *,
        agent_id: AgentId | None = None,
        payload: dict[str, Any] | None = None,
    ) -> None:
        if self._event_bus is None:
            return
        event = BossmanEvent.create(
            event_type=event_type,
            agent_id=agent_id,
            resource_name=self._name,
            payload=payload or {},
        )
        try:
            await self._event_bus.publish(event)
        except Exception as exc:
            log.warning("LLMSemaphore: event bus error: %s", exc)
