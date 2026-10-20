"""
api.runtime
───────────
Composition root: wires every BOSSman component into one runtime.

Every node (standalone or one of N in a cluster) runs the API-facing
services — registry, tasks, locks, limits, circuits, directives, policy,
approvals, secrets, keys. Exactly one node (the elected leader) also runs
the singleton duties: watchdog, failure detector, evaluator, scheduler
(lease expiry, deadlines, probes, signals, approval expiry), managed
processes and event persistence.

Passive observers kept from Phase 8:
  * EventStore — bounded ring buffer of every BossmanEvent (sequence-numbered)
                 + fan-out to WebSocket subscribers.
  * RecordingDiagnostician — remembers each Diagnosis by incident_id so the
                 UI can show root cause/evidence.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import time
from collections import OrderedDict, deque
from typing import Any

from bossman_server.contracts.agent_state import AgentState
from bossman_server.contracts.events import BossmanEvent, EventType
from bossman_server.control.agents import AgentService, RoleLimits
from bossman_server.control.approvals import ApprovalService
from bossman_server.control.audit import AuditLog
from bossman_server.control.auth import KeyService
from bossman_server.control.checkpoints import CheckpointService, IdempotencyService
from bossman_server.control.circuits import CircuitService
from bossman_server.control.flags import FlagService
from bossman_server.control.limits import LimitService
from bossman_server.control.locks import LockService
from bossman_server.control.managed import KubernetesRestarter, ProcessManager
from bossman_server.control.metrics import Metrics
from bossman_server.control.policy import PolicyEngine
from bossman_server.control.quality import QualityJudge, QualityService
from bossman_server.control.scheduler import Scheduler
from bossman_server.control.secrets import SecretService
from bossman_server.control.signals import SignalMonitor
from bossman_server.control.supervisors import SupervisorTree
from bossman_server.control.tracing import OtelExporter
from bossman_server.core.directives import DirectiveQueue
from bossman_server.core.documents import InMemoryDocumentStore
from bossman_server.core.event_bus import EventBus
from bossman_server.core.registry import AgentRegistry
from bossman_server.core.task_manager import TaskManager
from bossman_server.core.watchdog import Watchdog
from bossman_server.detection.diagnostician import Diagnosis, Diagnostician
from bossman_server.detection.failure_detector import DetectedFailure, FailureDetector
from bossman_server.evaluation.evaluator import Evaluator
from bossman_server.evaluation.health_checker import HealthChecker
from bossman_server.infra.backend import CoordinationBackend, InMemoryBackend, RedisBackend
from bossman_server.infra.distributed_bus import DistributedEventBus
from bossman_server.infra.durable_registry import DurableAgentRegistry
from bossman_server.infra.leader import LeaderElector
from bossman_server.infra.persistent_ledger import PersistentLedger
from bossman_server.infra.settings import InfraSettings
from bossman_server.recovery.circuit_breaker import CircuitBreakerRegistry
from bossman_server.recovery.recovery_engine import RecoveryEngine
from bossman_server.resources.resource_manager import ResourceManager

log = logging.getLogger(__name__)

EVENT_BUFFER = 5000
SUBSCRIBER_QUEUE = 500


class RecordingDiagnostician(Diagnostician):
    """Diagnostician that remembers its (bounded) output keyed by incident_id."""

    def __init__(self, cap: int = 1000) -> None:
        super().__init__()
        self._cap = cap
        self.by_incident: OrderedDict[str, Diagnosis] = OrderedDict()

    def record(self, d: Diagnosis) -> Diagnosis:
        self.by_incident[d.incident_id] = d
        while len(self.by_incident) > self._cap:
            self.by_incident.popitem(last=False)
        return d

    def diagnose(self, failure: DetectedFailure, extra_telemetry: dict[str, Any] | None = None) -> Diagnosis:
        return self.record(super().diagnose(failure, extra_telemetry))


class RecordingLLMDiagnostician:
    """Rules first; the LLM only for low-confidence cases. Records the final answer."""

    def __init__(self, base: RecordingDiagnostician, llm_call: Any) -> None:
        from bossman_server.detection.llm_diagnostician import LLMDiagnostician

        self.base = base
        self.llm = LLMDiagnostician(llm_call, base_diagnostician=base)
        self.by_incident = base.by_incident

    async def diagnose(self, failure: DetectedFailure, extra_telemetry: dict[str, Any] | None = None) -> Diagnosis:
        return self.base.record(await self.llm.diagnose(failure, extra_telemetry))

    @property
    def stats(self) -> dict[str, Any]:
        return {**self.base.stats, "llm": self.llm.stats}


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
        namespace: str | None = None,
        trace_id: str | None = None,
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
            if namespace and ev.namespace not in (None, namespace):
                continue
            if trace_id and ev.trace_id != trace_id:
                continue
            out.append((seq, ev))
            if len(out) >= limit:
                break
        return out  # newest first


class EventPersister:
    """Batches events into the SQL event store (leader only, so no duplicates)."""

    SKIP = {EventType.AGENT_HEARTBEAT}

    def __init__(self, bus: EventBus, store: Any) -> None:
        self._store = store
        self._buf: list[BossmanEvent] = []
        self.active = False
        self.written = 0
        self._task: asyncio.Task | None = None
        bus.subscribe(self._on_event)

    async def _on_event(self, e: BossmanEvent) -> None:
        if self.active and e.event_type not in self.SKIP:
            self._buf.append(e)

    async def start(self) -> None:
        self.active = True
        if self._task is None:
            self._task = asyncio.create_task(self._loop(), name="bossman:event-persister")

    async def stop(self) -> None:
        self.active = False
        if self._task:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._task = None
        await self.flush()

    async def flush(self) -> None:
        if not self._buf:
            return
        batch, self._buf = self._buf[:1000], self._buf[1000:]
        try:
            self.written += await self._store.append_many(batch)
        except Exception:  # noqa: BLE001
            log.warning("event persistence failed (%d events re-queued)", len(batch), exc_info=True)
            self._buf = batch + self._buf

    async def _loop(self) -> None:
        while True:
            await asyncio.sleep(1.0)
            await self.flush()


class BossmanRuntime:
    def __init__(
        self,
        settings: InfraSettings | None = None,
        *,
        backend: CoordinationBackend | None = None,
        ledger_store: Any = None,
        database_url: str | None = None,
    ) -> None:
        self.settings = settings or InfraSettings.from_env()
        s = self.settings
        self.started_monotonic = time.monotonic()
        self.elector: LeaderElector | None = None
        self._ledger_store = ledger_store
        self._owns_ledger_store = False
        self.heartbeat_timeout_s = s.heartbeat_timeout_s
        self.heartbeat_interval_s = s.heartbeat_interval_s

        # ── Coordination ─────────────────────────────────────────────────────
        if s.distributed:
            self.backend: CoordinationBackend = backend or RedisBackend(s.redis_url)
            self.bus: EventBus = DistributedEventBus(self.backend, s.node_id)
        else:
            self.backend = backend or InMemoryBackend()
            self.bus = EventBus()
        self._owns_backend = backend is None
        self.store = EventStore(self.bus)

        # ── Persistence ──────────────────────────────────────────────────────
        self.db = None
        self.event_store = None
        task_store = None
        if s.sql or database_url:
            from bossman_server.infra.db import Database
            from bossman_server.infra.sql_stores import SqlDocumentStore, SqlEventStore, SqlTaskStore

            self.db = Database(database_url or s.database_url)
            self.docs: Any = SqlDocumentStore(self.db)
            task_store = SqlTaskStore(self.db)
            self.event_store = SqlEventStore(self.db)
        else:
            self.docs = InMemoryDocumentStore()
        self.persister = EventPersister(self.bus, self.event_store) if self.event_store else None

        # ── Core ─────────────────────────────────────────────────────────────
        self.directives = DirectiveQueue(self.backend, self.bus)
        self.flags = FlagService(self.docs, self.bus)
        if s.distributed:
            self.registry: AgentRegistry = DurableAgentRegistry(self.bus, self.backend)
        else:
            self.registry = AgentRegistry(self.bus)
        self.registry.attach(directives=self.directives, flags=self.flags)
        self.tasks = TaskManager(self.bus, task_store)
        self.resources = ResourceManager(event_bus=self.bus)
        self.locks = LockService(self.backend, self.bus, self.registry)
        self.limits = LimitService(self.backend, self.bus)
        self.circuits = CircuitService(self.backend, self.bus)
        self.checker = HealthChecker(heartbeat_timeout_seconds=s.heartbeat_timeout_s,
                                     heartbeat_warn_seconds=s.heartbeat_timeout_s * 2 / 3)

        # ── Governance ───────────────────────────────────────────────────────
        self.audit = AuditLog(self.docs)
        self.keys = KeyService(self.docs, self.bus)
        self.policy = PolicyEngine(self.docs, self.bus, self.flags)
        self.approvals = ApprovalService(self.docs, self.bus, self.audit)
        self.secrets = SecretService(self.docs, self.bus, self.audit)
        self.role_limits = RoleLimits(self.docs)
        self.agents = AgentService(self)

        # ── Supervision / resume / quality ───────────────────────────────────
        self.supervisors = SupervisorTree(self.docs, self.bus, self.registry)
        self.managed = ProcessManager(self.docs, self.bus, directives=self.directives, registry=self.registry,
                                      base_url=s.public_url)
        self.k8s = KubernetesRestarter()
        self.checkpoints = CheckpointService(self.docs, self.bus)
        self.idempotency = IdempotencyService(self.docs)
        self.quality = QualityService(self.docs, self.bus, self.registry, self.directives)
        self.signals = SignalMonitor(self.bus, self.registry, self.tasks, self.role_limits.get)

        # ── Detection / recovery ─────────────────────────────────────────────
        self.diagnostician: Any = RecordingDiagnostician()
        self.llm_call = None
        from bossman_server.control.llm import llm_enabled, make_anthropic_call

        if llm_enabled():
            try:
                self.llm_call = make_anthropic_call()
                self.diagnostician = RecordingLLMDiagnostician(self.diagnostician, self.llm_call)
                rate = float(os.getenv("BOSSMAN_JUDGE_SAMPLE_RATE", "0"))
                if rate > 0:
                    self.quality.judge = QualityJudge(self.llm_call, self.quality, rate)
            except Exception as exc:  # noqa: BLE001
                log.warning("LLM features disabled: %s", exc)
        self.circuit_breakers = CircuitBreakerRegistry(event_bus=self.bus)
        self.engine = RecoveryEngine(
            registry=self.registry,
            task_manager=self.tasks,
            bus=self.bus,
            diagnostician=self.diagnostician,
            circuit_breakers=self.circuit_breakers,
            max_restarts=int(os.getenv("MAX_AGENT_RESTARTS", "3")),
            restart_window_seconds=float(os.getenv("RESTART_WINDOW_SECONDS", "60")),
            directives=self.directives,
            lock_service=self.locks,
            mediator=self.resources.mediator,
            circuit_service=self.circuits,
            supervisors=self.supervisors,
            restarter=self._restart_process,
        )
        self.ledger: PersistentLedger | None = None
        if self._ledger_store is None and self.db is not None:
            from bossman_server.infra.ledger_store import SqlLedgerStore

            self._ledger_store = SqlLedgerStore(db=self.db)
        if self._ledger_store is not None:
            # Swap before the Evaluator captures the ledger reference.
            self.ledger = PersistentLedger(self._ledger_store)
            self.engine.ledger = self.ledger
        self.detector = FailureDetector(self.bus, on_failure=self.engine.handle_failure)
        self.watchdog = Watchdog(
            self.registry,
            self.bus,
            check_interval_seconds=float(os.getenv("WATCHDOG_INTERVAL_SECONDS",
                                                   os.getenv("HEARTBEAT_INTERVAL_SECONDS", "10"))),
            timeout_seconds=s.heartbeat_timeout_s,
            on_idle_lost=lambda aid: self.agents.deregister(aid, "heartbeat lost while idle"),
        )
        self.evaluator = Evaluator(
            registry=self.registry, bus=self.bus, ledger=self.engine.ledger, checker=self.checker
        )
        self.scheduler = Scheduler(self)
        self.metrics = Metrics()
        self.otel = OtelExporter.from_env(self.bus)

        self.bus.subscribe(self._on_directive_acked, EventType.DIRECTIVE_ACKED)
        self.bus.subscribe(self._on_task_completed, EventType.TASK_COMPLETED)

        self._running = False
        self._duties_active = False

    # ── Glue ─────────────────────────────────────────────────────────────────

    async def _restart_process(self, state: AgentState, reason: str) -> bool:
        if await self.managed.restart_agent(state, reason):
            return True
        return await self.k8s.restart_agent(state, reason)

    async def _on_directive_acked(self, event: BossmanEvent) -> None:
        """Clear the legacy metadata flag once the agent confirmed the directive."""
        if not event.agent_id or not event.payload.get("ok"):
            return
        state = self.registry._agents.get(event.agent_id)
        if state is None:
            return
        dtype = str(event.payload.get("type", "")).lower()
        legacy = {"compact_context": "compact_context", "fallback_model": "fallback_model",
                  "rollback_checkpoint": "rollback_checkpoint"}
        if state.info.metadata.get("directive") == legacy.get(dtype):
            state.info.metadata.pop("directive", None)
            state.info.metadata.pop("fallback_reason", None)
            await self.registry.sync(event.agent_id)

    async def _on_task_completed(self, event: BossmanEvent) -> None:
        if self.quality.judge is None or not event.task_id or not self.is_leader:
            return
        task = await self.tasks.get(event.task_id)
        if task is not None:
            asyncio.ensure_future(self.quality.judge.maybe_judge(task))

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
            "persistence": "sql" if self.db is not None else "memory",
        }

    # ── Singleton duties (watchdog / detector / evaluator / scheduler) ───────
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
        await self.scheduler.start()
        await self.managed.start()
        if self.persister is not None:
            await self.persister.start()
        self._duties_active = True

    async def _step_down(self) -> None:
        if not self._duties_active:
            return
        self._duties_active = False
        await self.managed.stop()
        await self.scheduler.stop()
        await self.watchdog.stop()
        await self.evaluator.stop()
        await self.detector.stop()
        if self.persister is not None:
            await self.persister.stop()
        if self.ledger is not None:
            await self.ledger.stop()

    async def start(self) -> None:
        if self.db is not None:
            if self.settings.auto_migrate:
                await self.db.migrate()
            else:
                await self.db.init_schema()
        elif self._ledger_store is not None and hasattr(self._ledger_store, "init_schema"):
            await self._ledger_store.init_schema()
        await self.directives.start()
        await self.locks.start()
        await self.limits.start()
        await self.keys.load()
        await self.flags.load()
        await self.policy.load()
        await self.supervisors.load()
        await self.role_limits.load()
        await self.managed.load()
        if self.settings.distributed:
            assert isinstance(self.bus, DistributedEventBus)
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
        await self.directives.stop()
        await self.locks.stop()
        if isinstance(self.bus, DistributedEventBus):
            await self.bus.stop()
        if self.otel is not None:
            self.otel.shutdown()
        if self._owns_ledger_store and hasattr(self._ledger_store, "close"):
            await self._ledger_store.close()
        if self.db is not None:
            await self.db.close()
        if self.settings.distributed:
            await self.backend.close()
