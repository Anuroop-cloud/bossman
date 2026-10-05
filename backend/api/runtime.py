"""
api.runtime
───────────
Composition root: wires the existing Phase 0-7 components into one in-process
BOSSman runtime and adds two passive observers:

  * EventStore — bounded ring buffer of every BossmanEvent (sequence-numbered)
                 + fan-out to WebSocket subscribers.
  * RecordingDiagnostician — the stock rule-based Diagnostician, recording each
                 Diagnosis by incident_id so the UI can show root cause/evidence
                 (RecoveryEngine does not retain Diagnosis objects itself).
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from collections import OrderedDict, deque
from typing import Any

from contracts.events import BossmanEvent
from core.event_bus import EventBus
from core.registry import AgentRegistry
from core.task_manager import TaskManager
from core.watchdog import Watchdog
from detection.diagnostician import Diagnosis, Diagnostician
from detection.failure_detector import DetectedFailure, FailureDetector
from evaluation.evaluator import Evaluator
from evaluation.health_checker import HealthChecker
from recovery.circuit_breaker import CircuitBreakerRegistry
from recovery.recovery_engine import RecoveryEngine
from resources.resource_manager import ResourceManager
from infra.backend import CoordinationBackend, RedisBackend
from infra.distributed_bus import DistributedEventBus
from infra.durable_registry import DurableAgentRegistry
from infra.leader import LeaderElector
from infra.persistent_ledger import PersistentLedger
from infra.settings import InfraSettings

log = logging.getLogger(__name__)

EVENT_BUFFER = 5000
SUBSCRIBER_QUEUE = 500


class RecordingDiagnostician(Diagnostician):
    """Diagnostician that remembers its (bounded) output keyed by incident_id."""

    def __init__(self, cap: int = 1000) -> None:
        super().__init__()
        self._cap = cap
        self.by_incident: OrderedDict[str, Diagnosis] = OrderedDict()

    def diagnose(self, failure: DetectedFailure, extra_telemetry: dict[str, Any] | None = None) -> Diagnosis:
        d = super().diagnose(failure, extra_telemetry)
        self.by_incident[d.incident_id] = d
        while len(self.by_incident) > self._cap:
            self.by_incident.popitem(last=False)
        return d


class EventStore:
    """Ring buffer of events + subscriber fan-out. Subscribes to the EventBus once."""

    def __init__(self, bus: EventBus) -> None:
        self._events: deque[tuple[int, BossmanEvent]] = deque(maxlen=EVENT_BUFFER)
        self._seq = 0
        self._subs: set[asyncio.Queue] = set()
        self._stamps: deque[float] = deque(maxlen=EVENT_BUFFER)
        bus.subscribe(self._on_event)

    async def _on_event(self, event: BossmanEvent) -> None:
        self._seq += 1
        self._events.append((self._seq, event))
        self._stamps.append(time.monotonic())
        for q in list(self._subs):
            try:
                q.put_nowait((self._seq, event))
            except asyncio.QueueFull:
                # Slow consumer: drop it; client reconnects and re-syncs via REST.
                self._subs.discard(q)
                try:
                    q.put_nowait(None)
                except asyncio.QueueFull:
                    pass

    @property
    def last_seq(self) -> int:
        return self._seq

    @property
    def subscriber_count(self) -> int:
        return len(self._subs)

    def events_per_minute(self) -> int:
        cutoff = time.monotonic() - 60
        return sum(1 for t in self._stamps if t >= cutoff)

    def subscribe(self) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue(maxsize=SUBSCRIBER_QUEUE)
        self._subs.add(q)
        return q

    def unsubscribe(self, q: asyncio.Queue) -> None:
        self._subs.discard(q)

    def query(
        self,
        *,
        limit: int = 100,
        since_seq: int = 0,
        agent_id: str | None = None,
        task_id: str | None = None,
        incident_id: str | None = None,
        resource_name: str | None = None,
        event_type: str | None = None,
    ) -> list[tuple[int, BossmanEvent]]:
        out: list[tuple[int, BossmanEvent]] = []
        for seq, ev in reversed(self._events):
            if seq <= since_seq:
                break
            if agent_id and ev.agent_id != agent_id:
                continue
            if task_id and ev.task_id != task_id:
                continue
            if resource_name and ev.resource_name != resource_name:
                continue
            if event_type and ev.event_type.value != event_type:
                continue
            if incident_id and ev.payload.get("incident_id") != incident_id:
                continue
            out.append((seq, ev))
            if len(out) >= limit:
                break
        return out  # newest first


class BossmanRuntime:
    def __init__(
        self,
        settings: InfraSettings | None = None,
        *,
        backend: CoordinationBackend | None = None,
        ledger_store: Any = None,
    ) -> None:
        self.settings = settings or InfraSettings.from_env()
        self.started_monotonic = time.monotonic()
        self.backend: CoordinationBackend | None = None
        self.elector: LeaderElector | None = None
        self._ledger_store = ledger_store
        self._owns_ledger_store = False

        if self.settings.distributed:
            self.backend = backend or RedisBackend(self.settings.redis_url)
            self.bus: EventBus = DistributedEventBus(self.backend, self.settings.node_id)
        else:
            self.bus = EventBus()
        self.store = EventStore(self.bus)
        if self.settings.distributed:
            assert self.backend is not None
            self.registry: AgentRegistry = DurableAgentRegistry(self.bus, self.backend)
        else:
            self.registry = AgentRegistry(self.bus)
        self.tasks = TaskManager(self.bus)
        self.resources = ResourceManager(event_bus=self.bus)
        self.checker = HealthChecker()
        self.diagnostician = RecordingDiagnostician()
        self.circuit_breakers = CircuitBreakerRegistry(event_bus=self.bus)
        self.engine = RecoveryEngine(
            registry=self.registry,
            task_manager=self.tasks,
            bus=self.bus,
            diagnostician=self.diagnostician,
            circuit_breakers=self.circuit_breakers,
        )
        self.ledger: PersistentLedger | None = None
        if self._ledger_store is None and self.settings.distributed:
            from infra.ledger_store import SqlLedgerStore

            self._ledger_store = SqlLedgerStore(self.settings.database_url)
            self._owns_ledger_store = True
        if self._ledger_store is not None:
            # Swap before the Evaluator captures the ledger reference.
            self.ledger = PersistentLedger(self._ledger_store)
            self.engine.ledger = self.ledger
        self.detector = FailureDetector(self.bus, on_failure=self.engine.handle_failure)
        self.watchdog = Watchdog(
            self.registry,
            self.bus,
            check_interval_seconds=float(os.getenv("HEARTBEAT_INTERVAL_SECONDS", "10")),
            timeout_seconds=float(os.getenv("HEARTBEAT_TIMEOUT_SECONDS", "30")),
        )
        self.evaluator = Evaluator(
            registry=self.registry, bus=self.bus, ledger=self.engine.ledger, checker=self.checker
        )
        self._running = False
        self._duties_active = False

    @property
    def running(self) -> bool:
        return self._running

    @property
    def is_leader(self) -> bool:
        """Standalone nodes are always their own leader."""
        return self.elector.is_leader if self.elector else True

    async def cluster_info(self) -> dict[str, Any]:
        return {
            "mode": self.settings.mode,
            "node_id": self.settings.node_id or "standalone",
            "is_leader": self.is_leader,
            "leader_id": (await self.elector.current_leader()) if self.elector else None,
            "duties_active": self._duties_active,
            "durable_ledger": self.ledger is not None,
        }

    # ── Singleton duties (watchdog / detector / evaluator) ────────────────────
    # Exactly one node runs these. In standalone mode that is always us.

    async def _become_leader(self) -> None:
        if self._duties_active:
            return
        if isinstance(self.registry, DurableAgentRegistry):
            await self.registry.restore()
        if self.ledger is not None:
            await self.ledger.restore()
            await self.ledger.start_sync_loop(self.settings.ledger_sync_s)
        await self.detector.start()
        await self.evaluator.start()
        await self.watchdog.start()
        self._duties_active = True

    async def _step_down(self) -> None:
        if not self._duties_active:
            return
        self._duties_active = False
        await self.watchdog.stop()
        await self.evaluator.stop()
        await self.detector.stop()
        if self.ledger is not None:
            await self.ledger.stop()

    async def start(self) -> None:
        if self._ledger_store is not None and hasattr(self._ledger_store, "init_schema"):
            await self._ledger_store.init_schema()
        if self.settings.distributed:
            assert self.backend is not None and isinstance(self.bus, DistributedEventBus)
            await self.bus.start()
            self.elector = LeaderElector(
                self.backend,
                self.settings.node_id,
                lease_ms=self.settings.lease_ms,
                on_elected=self._become_leader,
                on_demoted=self._step_down,
            )
            await self.elector.tick()  # decide immediately so first requests are accurate
            await self.elector.start()
        else:
            await self._become_leader()
        self._running = True

    async def stop(self) -> None:
        self._running = False
        if self.elector:
            await self.elector.stop()  # releases lease + steps down
        await self._step_down()
        if isinstance(self.bus, DistributedEventBus):
            await self.bus.stop()
        if self._owns_ledger_store and hasattr(self._ledger_store, "close"):
            await self._ledger_store.close()
        if self.backend is not None:
            await self.backend.close()
