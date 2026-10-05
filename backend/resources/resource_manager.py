"""
resources.resource_manager
───────────────────────────
Unified Resource Manager facade for BOSSman.

The ResourceManager is the single object every BOSSman agent and subsystem
interacts with for resource control. It composes:

  LLMSemaphore    — concurrency cap (default: 20 concurrent LLM calls)
  TokenBucket     — rate cap (default: 60 calls/minute = 1/second continuous)
  ResourceMediator — named lock broker with deadlock prevention

Usage in an agent
─────────────────
    rm = ResourceManager(event_bus=bus)

    # Gate an LLM call through both the semaphore AND the token bucket:
    async with rm.llm_call(agent_id="agent-abc"):
        response = await anthropic_client.messages.create(...)

    # Acquire an exclusive named resource:
    async with rm.resource("memory-store", agent_id="agent-abc"):
        write_to_memory(...)

Usage in BaseAgent
──────────────────
    response = await self._resource_manager.llm_call_fn(
        agent_id=self.agent_id,
        call=lambda: anthropic.messages.create(...)
    )

Design
──────
- ResourceManager is injected into BaseAgent at construction.
- If no ResourceManager is provided, agents run without rate limiting
  (safe for tests and single-agent scenarios).
- All components share the same EventBus instance.
"""

from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Callable, Awaitable, TypeVar

from contracts.agent_state import AgentId
from resources.llm_semaphore import LLMSemaphore, SemaphoreTimeoutError
from resources.token_bucket import TokenBucket, TokenBucketExhaustedError
from resources.mediator import ResourceMediator, ResourceTimeoutError

log = logging.getLogger(__name__)

T = TypeVar("T")


class ResourceManager:
    """
    Unified resource control facade.

    Parameters
    ──────────
    max_concurrent_llm:
        Max simultaneous LLM calls across all agents (semaphore cap).
        Default: env LLM_MAX_CONCURRENT or 20.
    llm_calls_per_second:
        Rate limit for LLM calls per second (token bucket rate).
        Default: env LLM_RATE_PER_SECOND or 2.0 (120 RPM).
    llm_burst:
        Max burst size for LLM rate limiter.
        Default: env LLM_BURST or 10.
    mediator_timeout:
        Seconds before named resource acquire times out (deadlock prevention).
        Default: 30.0.
    event_bus:
        Shared EventBus. If None, events are suppressed (tests / standalone).
    """

    def __init__(
        self,
        *,
        max_concurrent_llm: int | None = None,
        llm_calls_per_second: float | None = None,
        llm_burst: float | None = None,
        mediator_timeout: float = 30.0,
        event_bus: Any | None = None,
    ) -> None:
        _max_concurrent = max_concurrent_llm or int(os.getenv("LLM_MAX_CONCURRENT", "20"))
        _rate = llm_calls_per_second or float(os.getenv("LLM_RATE_PER_SECOND", "2.0"))
        _burst = llm_burst or float(os.getenv("LLM_BURST", "10.0"))

        self.semaphore = LLMSemaphore(
            max_concurrent=_max_concurrent,
            name="llm-global",
            event_bus=event_bus,
        )
        self.token_bucket = TokenBucket(
            rate=_rate,
            capacity=_burst,
            name="llm-rpm",
            event_bus=event_bus,
        )
        self.mediator = ResourceMediator(
            default_timeout=mediator_timeout,
            event_bus=event_bus,
        )

        log.info(
            "ResourceManager: initialised (max_concurrent=%d, rate=%.1f/s, burst=%.0f, timeout=%.0fs)",
            _max_concurrent, _rate, _burst, mediator_timeout,
        )

    # ── LLM call gate (semaphore + token bucket combined) ─────────────────────

    @asynccontextmanager
    async def llm_call(
        self,
        agent_id: AgentId | None = None,
        token_cost: float = 1.0,
    ) -> AsyncIterator[None]:
        """
        Context manager that gates an LLM call through:
          1. TokenBucket  — rate limit (prevents thundering-herd retry storms)
          2. LLMSemaphore — concurrency cap (prevents API saturation)

        Usage:
            async with rm.llm_call(agent_id=self.agent_id):
                response = await llm.invoke(...)
        """
        # Rate check first (fast-fail on rate limit before consuming a slot)
        await self.token_bucket.consume(amount=token_cost, agent_id=agent_id)

        # Then concurrency slot
        async with self.semaphore.acquire(agent_id=agent_id):
            yield

    async def llm_call_fn(
        self,
        call: Callable[[], Awaitable[T]],
        agent_id: AgentId | None = None,
        token_cost: float = 1.0,
    ) -> T:
        """
        Convenience wrapper: gate + execute an LLM call in one shot.

        Usage:
            result = await rm.llm_call_fn(
                call=lambda: anthropic.messages.create(...),
                agent_id=self.agent_id,
            )
        """
        async with self.llm_call(agent_id=agent_id, token_cost=token_cost):
            return await call()

    # ── Named resource lock ───────────────────────────────────────────────────

    @asynccontextmanager
    async def resource(
        self,
        resource_name: str,
        agent_id: AgentId | None = None,
        timeout: float | None = None,
    ) -> AsyncIterator[None]:
        """
        Acquire a named exclusive resource lock through the mediator.

        Usage:
            async with rm.resource("memory-store", agent_id=self.agent_id):
                update_memory(...)
        """
        async with self.mediator.acquire(
            resource_name, agent_id=agent_id, timeout=timeout
        ):
            yield

    # ── Snapshot for dashboard / watchdog ─────────────────────────────────────

    def snapshot(self) -> dict[str, Any]:
        """Full resource state for the dashboard and watchdog."""
        return {
            "llm_semaphore": self.semaphore.stats,
            "token_bucket": self.token_bucket.stats,
            "named_resources": self.mediator.snapshot(),
        }
