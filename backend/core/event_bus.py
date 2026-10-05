"""
core.event_bus
──────────────
Internal async pub/sub event bus for BOSSman.

Every subsystem (registry, watchdog, task manager, recovery engine, …)
publishes BossmanEvents here. Any number of subscribers receive them.

Consumers:
  - Postgres persistence layer  (appends every event to the events table)
  - WebSocket broadcast layer   (streams to connected dashboard clients)
  - Failure Detector            (watches for anomaly patterns)
  - Evaluator                   (listens for RECOVERY_COMPLETED)

Design
──────
- Fully in-process async queue — no Redis or external broker in Phase 1.
  Redis pub/sub is a Phase 3+ concern once multi-process deployment is needed.
- Subscribers register a coroutine callback; they are called concurrently
  via asyncio.gather so one slow subscriber cannot block others.
- The bus never raises into publishers — subscriber exceptions are logged
  and swallowed so a broken listener cannot crash the supervisor.
"""

from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from typing import Callable, Coroutine, Any

from contracts.events import BossmanEvent, EventType

log = logging.getLogger(__name__)

# Type alias for async subscriber callbacks
Subscriber = Callable[[BossmanEvent], Coroutine[Any, Any, None]]


class EventBus:
    """
    Async event bus. Instantiate one per BOSSman process and pass it
    to every subsystem that needs to publish or subscribe.

    Usage
    ─────
        bus = EventBus()

        # Subscribe to ALL events
        bus.subscribe(my_handler)

        # Subscribe to specific event types only
        bus.subscribe(on_failure, EventType.FAILURE_DETECTED)
        bus.subscribe(on_timeout, EventType.AGENT_TIMEOUT)

        # Publish
        await bus.publish(BossmanEvent.create(EventType.AGENT_STARTED, ...))
    """

    def __init__(self) -> None:
        # Subscribers keyed by event type (None = wildcard / all events)
        self._subscribers: dict[EventType | None, list[Subscriber]] = defaultdict(list)
        self._publish_count: int = 0
        self._error_count: int = 0

    # ── Subscription ──────────────────────────────────────────────────────────

    def subscribe(
        self,
        callback: Subscriber,
        *event_types: EventType,
    ) -> None:
        """
        Register an async callback.

        If no event_types are given, the callback receives ALL events.
        If event_types are given, the callback receives only those types.
        """
        if not event_types:
            self._subscribers[None].append(callback)
        else:
            for et in event_types:
                self._subscribers[et].append(callback)

    def unsubscribe(self, callback: Subscriber) -> None:
        """Remove a callback from all subscriptions."""
        for subscribers in self._subscribers.values():
            try:
                subscribers.remove(callback)
            except ValueError:
                pass

    # ── Publishing ────────────────────────────────────────────────────────────

    async def publish(self, event: BossmanEvent) -> None:
        """
        Broadcast an event to all matching subscribers concurrently.
        Never raises — subscriber exceptions are caught and logged.
        """
        self._publish_count += 1

        # Gather: wildcard subscribers + type-specific subscribers
        targets: list[Subscriber] = list(self._subscribers[None])
        targets.extend(self._subscribers.get(event.event_type, []))

        if not targets:
            return

        results = await asyncio.gather(
            *[self._safe_call(cb, event) for cb in targets],
            return_exceptions=True,
        )

        for r in results:
            if isinstance(r, Exception):
                self._error_count += 1
                log.error("EventBus subscriber error: %s", r, exc_info=r)

    async def _safe_call(self, cb: Subscriber, event: BossmanEvent) -> None:
        await cb(event)

    # ── Stats ─────────────────────────────────────────────────────────────────

    @property
    def stats(self) -> dict[str, int]:
        return {
            "published": self._publish_count,
            "subscriber_errors": self._error_count,
            "subscriber_count": sum(len(v) for v in self._subscribers.values()),
        }
