"""
resources.token_bucket
───────────────────────
Rate-limiter for LLM API calls — controls *throughput*, not concurrency.

Reference: zylos.md §Part3 — Semaphores and Token Buckets
  "Token buckets manage throughput rate independently of concurrency.
   The combination of a semaphore (concurrency cap) and a token bucket
   (rate cap) handles the two orthogonal dimensions of resource contention:
   how many things are happening simultaneously, and how fast they happen."

Algorithm
─────────
Classic leaky-bucket / token-bucket:
  - The bucket holds up to `capacity` tokens.
  - Tokens refill at `rate` tokens/second (continuous).
  - Each call consumes `amount` tokens (default 1.0).
  - If the bucket doesn't have enough tokens, the caller waits until
    enough have refilled.

Usage
─────
    bucket = TokenBucket(rate=10.0, capacity=20.0, name="anthropic-tpm")

    # Consume 1 call token before each LLM call:
    await bucket.consume(agent_id="agent-abc")
    response = await anthropic_client.messages.create(...)

    # For token-level rate limiting (e.g., 100k TPM):
    bucket_tpm = TokenBucket(rate=100_000/60, capacity=100_000, name="token-budget")
    await bucket_tpm.consume(amount=estimated_tokens, agent_id="agent-abc")

Events published
────────────────
RESOURCE_STARVATION  — caller had to wait (bucket was empty)
RESOURCE_ACQUIRED    — tokens consumed, call may proceed
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any

from contracts.agent_state import AgentId
from contracts.events import BossmanEvent, EventType

log = logging.getLogger(__name__)

# Maximum wait time before giving up
DEFAULT_CONSUME_TIMEOUT: float = 120.0

# Minimum wait before re-checking the bucket (prevents busy-loop)
_MIN_SLEEP: float = 0.005


class TokenBucketExhaustedError(TimeoutError):
    """
    Raised when a caller exceeds the max wait time for token bucket replenishment.
    Typically signals the system is rate-limited harder than the bucket models.
    """

    def __init__(self, name: str, agent_id: AgentId | None, timeout: float) -> None:
        super().__init__(
            f"TokenBucket '{name}' exhausted: agent {agent_id!r} waited {timeout}s "
            f"with no tokens replenished. Check provider rate limits."
        )
        self.name = name
        self.agent_id = agent_id
        self.timeout = timeout


class TokenBucket:
    """
    Async token-bucket rate limiter.

    Thread-safe for async use (single asyncio event loop). For multi-process
    deployments, replace this with a Redis-backed implementation (Phase 3+).

    Parameters
    ──────────
    rate:     Tokens added per second (steady-state throughput).
    capacity: Maximum burst size (bucket max level).
    name:     Identifier for logging and events (e.g. "anthropic-rpm").
    consume_timeout: Max seconds a caller will wait before raising.
    """

    def __init__(
        self,
        rate: float,
        capacity: float,
        *,
        name: str = "default",
        consume_timeout: float = DEFAULT_CONSUME_TIMEOUT,
        event_bus: Any | None = None,
    ) -> None:
        if rate <= 0:
            raise ValueError(f"rate must be > 0, got {rate}")
        if capacity <= 0:
            raise ValueError(f"capacity must be > 0, got {capacity}")

        self._rate = rate
        self._capacity = capacity
        self._name = name
        self._timeout = consume_timeout
        self._event_bus = event_bus

        # Start full
        self._tokens: float = capacity
        self._last_refill: float = time.monotonic()

        # Metrics
        self._total_consumed: float = 0.0
        self._total_wait_seconds: float = 0.0
        self._total_starvation_events: int = 0
        self._calls_made: int = 0

        # Lock to serialise refill + consume
        self._lock = asyncio.Lock()

    # ── Public API ────────────────────────────────────────────────────────────

    async def consume(
        self,
        amount: float = 1.0,
        agent_id: AgentId | None = None,
        timeout: float | None = None,
    ) -> None:
        """
        Block until `amount` tokens are available, then consume them.

        Raises TokenBucketExhaustedError if timeout is exceeded.
        """
        if amount <= 0:
            raise ValueError(f"amount must be > 0, got {amount}")
        if amount > self._capacity:
            raise ValueError(
                f"amount={amount} exceeds bucket capacity={self._capacity}. "
                f"Increase capacity or reduce request size."
            )

        t_out = timeout if timeout is not None else self._timeout
        t_start = time.monotonic()
        first_wait = True

        while True:
            async with self._lock:
                self._refill()
                if self._tokens >= amount:
                    self._tokens -= amount
                    self._total_consumed += amount
                    self._calls_made += 1
                    wait_secs = time.monotonic() - t_start

                    if wait_secs > 0.01:  # only log if we actually waited
                        log.debug(
                            "TokenBucket[%s]: consumed %.1f tokens (waited=%.3fs, remaining=%.1f, agent=%s)",
                            self._name, amount, wait_secs, self._tokens, agent_id,
                        )
                        self._total_wait_seconds += wait_secs

                    await self._emit(
                        EventType.RESOURCE_ACQUIRED,
                        agent_id=agent_id,
                        payload={
                            "bucket": self._name,
                            "tokens_consumed": amount,
                            "tokens_remaining": round(self._tokens, 2),
                            "waited_seconds": round(wait_secs, 3),
                        },
                    )
                    return

                # Not enough tokens — compute wait time
                deficit = amount - self._tokens
                wait_needed = deficit / self._rate
                elapsed = time.monotonic() - t_start

                if elapsed + wait_needed > t_out:
                    raise TokenBucketExhaustedError(self._name, agent_id, t_out)

                if first_wait:
                    first_wait = False
                    self._total_starvation_events += 1
                    log.warning(
                        "TokenBucket[%s]: starvation — need %.1f tokens, have %.1f, wait=%.3fs (agent=%s)",
                        self._name, amount, self._tokens, wait_needed, agent_id,
                    )
                    await self._emit(
                        EventType.RESOURCE_STARVATION,
                        agent_id=agent_id,
                        payload={
                            "bucket": self._name,
                            "tokens_needed": amount,
                            "tokens_available": round(self._tokens, 2),
                            "estimated_wait_seconds": round(wait_needed, 3),
                        },
                    )

                sleep_time = max(_MIN_SLEEP, min(wait_needed, 1.0))

            # Release lock while waiting
            await asyncio.sleep(sleep_time)

    # ── Refill ────────────────────────────────────────────────────────────────

    def _refill(self) -> None:
        """Refill tokens based on elapsed time since last refill. Called under lock."""
        now = time.monotonic()
        elapsed = now - self._last_refill
        added = elapsed * self._rate
        self._tokens = min(self._capacity, self._tokens + added)
        self._last_refill = now

    # ── Metrics / introspection ───────────────────────────────────────────────

    @property
    def stats(self) -> dict[str, Any]:
        """Live metrics snapshot."""
        self._refill()  # ensure fresh reading (no lock — metrics are approximate)
        return {
            "name": self._name,
            "rate_per_second": self._rate,
            "capacity": self._capacity,
            "tokens_available": round(self._tokens, 2),
            "fill_fraction": round(self._tokens / self._capacity, 3),
            "total_consumed": round(self._total_consumed, 2),
            "total_calls": self._calls_made,
            "total_wait_seconds": round(self._total_wait_seconds, 3),
            "starvation_events": self._total_starvation_events,
        }

    @property
    def tokens_available(self) -> float:
        """Current token level (approximate — no lock)."""
        self._refill()
        return self._tokens

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
            log.warning("TokenBucket: event bus error: %s", exc)
