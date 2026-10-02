"""
recovery.circuit_breaker
─────────────────────────
Three-state circuit breaker for BOSSman agent/resource protection.

Reference: zylos.md §Part2 — Circuit Breaker Pattern
  "The circuit breaker, popularized by Michael Nygard's Release It! and
   adopted universally in microservices, is directly applicable to AI agent
   tool calls."

States
──────
  CLOSED    — normal operation. Failures are counted. Requests go through.
  OPEN      — failure threshold crossed. Requests are immediately rejected
              (fast-fail) without touching the failing dependency.
  HALF_OPEN — recovery probe. After open_timeout_seconds, one probe call is
              allowed. Success → CLOSED. Failure → OPEN again.

Per zylos.md: "Failing fast when a model API is degraded — and routing to a
fallback model — is far better than accumulating request timeouts that block
the agent's execution pipeline."

BOSSman mapping
───────────────
  One CircuitBreaker per protected entity (agent_id, resource name, tool name).
  The CircuitBreakerRegistry manages the per-key lifecycle.
  RecoveryEngine creates/opens/probes breakers.

Events published
────────────────
  CIRCUIT_BREAKER_OPENED    — threshold crossed, circuit opens
  CIRCUIT_BREAKER_HALF_OPEN — probe period started
  CIRCUIT_BREAKER_CLOSED    — circuit restored to normal
"""

from __future__ import annotations

import asyncio
import logging
import time
from enum import Enum
from typing import Any, Awaitable, Callable, TypeVar

from contracts.agent_state import AgentId
from contracts.events import BossmanEvent, EventType

log = logging.getLogger(__name__)

T = TypeVar("T")


class CircuitState(str, Enum):
    CLOSED = "CLOSED"
    OPEN = "OPEN"
    HALF_OPEN = "HALF_OPEN"


class CircuitOpenError(Exception):
    """Raised when a call is attempted against an OPEN circuit."""

    def __init__(self, key: str, retry_after: float) -> None:
        super().__init__(
            f"Circuit '{key}' is OPEN. Retry after {retry_after:.1f}s. "
            f"Use fallback or wait for HALF_OPEN probe window."
        )
        self.key = key
        self.retry_after = retry_after


class CircuitBreaker:
    """
    Per-key circuit breaker.

    Usage
    ─────
        cb = CircuitBreaker("agent-abc", failure_threshold=5, open_timeout=60.0)

        try:
            result = await cb.call(my_coroutine)
        except CircuitOpenError:
            result = fallback_result

    The breaker tracks failures automatically — do NOT call record_failure
    externally. Just call cb.call() and it handles state transitions.
    """

    def __init__(
        self,
        key: str,
        failure_threshold: int = 5,
        success_threshold: int = 2,
        open_timeout_seconds: float = 60.0,
        event_bus: Any | None = None,
        agent_id: AgentId | None = None,
    ) -> None:
        self.key = key
        self._failure_threshold = failure_threshold
        self._success_threshold = success_threshold
        self._open_timeout = open_timeout_seconds
        self._event_bus = event_bus
        self._agent_id = agent_id

        self._state = CircuitState.CLOSED
        self._failures: int = 0
        self._successes: int = 0
        self._opened_at: float | None = None

        # Stats
        self._total_calls: int = 0
        self._total_rejected: int = 0
        self._total_opened: int = 0

    # ── Properties ────────────────────────────────────────────────────────────

    @property
    def state(self) -> CircuitState:
        self._maybe_transition_to_half_open()
        return self._state

    @property
    def is_open(self) -> bool:
        return self.state == CircuitState.OPEN

    @property
    def is_closed(self) -> bool:
        return self.state == CircuitState.CLOSED

    @property
    def retry_after_seconds(self) -> float:
        """Seconds until the OPEN circuit enters HALF_OPEN probe window."""
        if self._state != CircuitState.OPEN or self._opened_at is None:
            return 0.0
        elapsed = time.monotonic() - self._opened_at
        return max(0.0, self._open_timeout - elapsed)

    # ── Public API ────────────────────────────────────────────────────────────

    async def call(
        self,
        fn: Callable[[], Awaitable[T]],
        *,
        fallback: Callable[[], T] | T | None = None,
    ) -> T:
        """
        Execute fn through the circuit breaker.

        If OPEN: raise CircuitOpenError (or return fallback if provided).
        If HALF_OPEN: allow one probe; success→CLOSED, failure→OPEN.
        If CLOSED: call fn, track failures.
        """
        self._total_calls += 1
        current_state = self.state  # triggers time-based probe check

        if current_state == CircuitState.OPEN:
            self._total_rejected += 1
            log.debug("CircuitBreaker[%s] OPEN — rejecting call (retry_after=%.1fs)",
                      self.key, self.retry_after_seconds)
            if fallback is not None:
                return fallback() if callable(fallback) else fallback  # type: ignore[return-value]
            raise CircuitOpenError(self.key, self.retry_after_seconds)

        try:
            result = await fn()
            await self._on_success()
            return result
        except Exception:
            await self._on_failure()
            raise

    def force_open(self) -> None:
        """Force the circuit OPEN immediately (used by RecoveryEngine)."""
        self._state = CircuitState.OPEN
        self._opened_at = time.monotonic()
        self._total_opened += 1
        log.warning("CircuitBreaker[%s] force-OPEN by RecoveryEngine", self.key)

    def force_close(self) -> None:
        """Force the circuit CLOSED (used after successful recovery)."""
        self._state = CircuitState.CLOSED
        self._failures = 0
        self._successes = 0
        self._opened_at = None
        log.info("CircuitBreaker[%s] force-CLOSED by RecoveryEngine", self.key)

    # ── State transitions ─────────────────────────────────────────────────────

    def _maybe_transition_to_half_open(self) -> None:
        """If OPEN and timeout elapsed, move to HALF_OPEN probe window."""
        if (
            self._state == CircuitState.OPEN
            and self._opened_at is not None
            and time.monotonic() - self._opened_at >= self._open_timeout
        ):
            self._state = CircuitState.HALF_OPEN
            self._successes = 0
            log.info("CircuitBreaker[%s] OPEN → HALF_OPEN (probe window)", self.key)
            asyncio.ensure_future(self._emit(EventType.CIRCUIT_BREAKER_HALF_OPEN))

    async def _on_success(self) -> None:
        if self._state == CircuitState.HALF_OPEN:
            self._successes += 1
            if self._successes >= self._success_threshold:
                self._state = CircuitState.CLOSED
                self._failures = 0
                self._successes = 0
                self._opened_at = None
                log.info("CircuitBreaker[%s] HALF_OPEN → CLOSED (recovered)", self.key)
                await self._emit(EventType.CIRCUIT_BREAKER_CLOSED)
        elif self._state == CircuitState.CLOSED:
            self._failures = 0  # reset on success

    async def _on_failure(self) -> None:
        self._failures += 1
        if self._state == CircuitState.HALF_OPEN:
            # Probe failed — go back OPEN
            self._state = CircuitState.OPEN
            self._opened_at = time.monotonic()
            self._total_opened += 1
            log.warning("CircuitBreaker[%s] HALF_OPEN → OPEN (probe failed)", self.key)
            await self._emit(EventType.CIRCUIT_BREAKER_OPENED)
        elif self._state == CircuitState.CLOSED and self._failures >= self._failure_threshold:
            self._state = CircuitState.OPEN
            self._opened_at = time.monotonic()
            self._total_opened += 1
            log.warning(
                "CircuitBreaker[%s] CLOSED → OPEN (%d failures, threshold=%d)",
                self.key, self._failures, self._failure_threshold,
            )
            await self._emit(EventType.CIRCUIT_BREAKER_OPENED)

    async def _emit(self, event_type: EventType) -> None:
        if self._event_bus is None:
            return
        event = BossmanEvent.create(
            event_type,
            agent_id=self._agent_id,
            resource_name=self.key,
            payload={
                "circuit_key": self.key,
                "state": self._state.value,
                "failures": self._failures,
                "total_opened": self._total_opened,
            },
        )
        try:
            await self._event_bus.publish(event)
        except Exception as exc:
            log.warning("CircuitBreaker: event bus error: %s", exc)

    @property
    def stats(self) -> dict[str, Any]:
        return {
            "key": self.key,
            "state": self._state.value,
            "failures": self._failures,
            "total_calls": self._total_calls,
            "total_rejected": self._total_rejected,
            "total_opened": self._total_opened,
            "retry_after_seconds": self.retry_after_seconds,
        }


# ── Registry ──────────────────────────────────────────────────────────────────

class CircuitBreakerRegistry:
    """
    Manages one CircuitBreaker per key (agent_id, resource name, tool name).
    RecoveryEngine uses this to open/close breakers during recovery.
    """

    def __init__(
        self,
        failure_threshold: int = 5,
        success_threshold: int = 2,
        open_timeout_seconds: float = 60.0,
        event_bus: Any | None = None,
    ) -> None:
        self._default_failure_threshold = failure_threshold
        self._default_success_threshold = success_threshold
        self._default_open_timeout = open_timeout_seconds
        self._event_bus = event_bus
        self._breakers: dict[str, CircuitBreaker] = {}

    def get_or_create(
        self,
        key: str,
        agent_id: AgentId | None = None,
    ) -> CircuitBreaker:
        if key not in self._breakers:
            self._breakers[key] = CircuitBreaker(
                key=key,
                failure_threshold=self._default_failure_threshold,
                success_threshold=self._default_success_threshold,
                open_timeout_seconds=self._default_open_timeout,
                event_bus=self._event_bus,
                agent_id=agent_id,
            )
        return self._breakers[key]

    def open(self, key: str, agent_id: AgentId | None = None) -> CircuitBreaker:
        cb = self.get_or_create(key, agent_id=agent_id)
        cb.force_open()
        return cb

    def close(self, key: str) -> CircuitBreaker | None:
        cb = self._breakers.get(key)
        if cb:
            cb.force_close()
        return cb

    def snapshot(self) -> dict[str, Any]:
        return {k: v.stats for k, v in self._breakers.items()}
