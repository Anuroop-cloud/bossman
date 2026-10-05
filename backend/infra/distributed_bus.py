"""
infra.distributed_bus
─────────────────────
DistributedEventBus — drop-in replacement for core.event_bus.EventBus.

Local publish still dispatches to in-process subscribers (identical
semantics), and additionally broadcasts the event on a shared channel.
Events arriving from *other* nodes are dispatched to local subscribers
only (never re-broadcast), so no loops and no duplicates.
"""

from __future__ import annotations

import logging
import uuid

from contracts.events import BossmanEvent
from core.event_bus import EventBus
from infra.backend import CoordinationBackend, Unsubscribe

log = logging.getLogger(__name__)

EVENTS_CHANNEL = "bossman:events"


class DistributedEventBus(EventBus):
    def __init__(
        self,
        backend: CoordinationBackend,
        node_id: str | None = None,
        channel: str = EVENTS_CHANNEL,
    ) -> None:
        super().__init__()
        self.backend = backend
        self.node_id = node_id or f"node-{uuid.uuid4().hex[:8]}"
        self.channel = channel
        self._unsub: Unsubscribe | None = None
        self._remote_count = 0

    async def start(self) -> None:
        if self._unsub is None:
            self._unsub = await self.backend.subscribe(self.channel, self._on_remote)

    async def stop(self) -> None:
        if self._unsub:
            await self._unsub()
            self._unsub = None

    async def publish(self, event: BossmanEvent) -> None:
        await super().publish(event)  # local fan-out first; never raises
        try:
            envelope = f"{self.node_id}|{event.model_dump_json()}"
            await self.backend.publish(self.channel, envelope)
        except Exception:  # noqa: BLE001 - broker outage must not crash publishers
            log.exception("distributed publish failed")
            self._error_count += 1

    async def _on_remote(self, raw: str) -> None:
        origin, _, body = raw.partition("|")
        if origin == self.node_id:
            return
        self._remote_count += 1
        await super().publish(BossmanEvent.model_validate_json(body))

    @property
    def stats(self) -> dict[str, int]:
        return {**super().stats, "remote_received": self._remote_count}
