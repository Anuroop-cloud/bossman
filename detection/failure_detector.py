"""
detection.failure_detector
───────────────────────────
The BOSSman Failure Detector — event-driven, real-time failure identification.

Reference: zylos.md §Part8 — Detection Before Cascades
  "Deadlocks and resource contention are often detectable before they
   become critical by monitoring:
   - Task queue depth: Growing queue = either slow processing or blocked agents
   - Subagent count: increasing count of same type = spawning without completion
   - Context token usage rate: unusually high = a loop
   - API call rate: spike in calls to the same endpoint = retry loop"

Architecture
────────────
The FailureDetector is a pure EVENT CONSUMER — it subscribes to the EventBus
and maintains sliding-window state. It does NOT poll and does NOT reach into
the registry or resource manager directly (those are sampled via snapshots
injected into detect_now() for proactive scans).

It detects four failure categories from the dev-phases spec:

  1. RETRY STORM     — TASK_FAILED rate exceeds threshold within a window
  2. HEARTBEAT TIMEOUT — AGENT_TIMEOUT events (already emitted by Watchdog)
                         wrapped in FAILURE_DETECTED with FailureType
  3. RESOURCE STARVATION — RESOURCE_STARVATION events + mediator contention
  4. DEADLOCK SUSPECTED  — DEADLOCK_SUSPECTED events from ResourceMediator
                           confirmed by checking agent wait-graph

For each detected failure it:
  - Emits FAILURE_DETECTED on the EventBus with a structured payload
  - Creates a RecoveryAttempt in the RecoveryLedger (outcome=IN_PROGRESS)
  - Returns the attempt for the Recovery Engine (Phase 5) to act on

Windows and thresholds
──────────────────────
All thresholds are configurable at construction. Defaults from zylos.md:
  retry_storm: ≥5 TASK_FAILED events in 60 seconds = storm
  resource starvation: ≥3 RESOURCE_STARVATION events in 30 seconds
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from typing import Any

from contracts.agent_state import AgentId
from contracts.events import BossmanEvent, EventType
from contracts.recovery_ledger import (
    FailureType,
    RecoveryAttempt,
    RecoveryOutcome,
    RecoveryStrategy,
    new_incident_id,
)
from core.event_bus import EventBus

log = logging.getLogger(__name__)


# ── Detection config ──────────────────────────────────────────────────────────

@dataclass
class DetectorConfig:
    """Thresholds for all four detection rules."""
    # Retry storm
    retry_storm_threshold: int = 5      # N failures within window = storm
    retry_storm_window_seconds: float = 60.0

    # Resource starvation
    starvation_threshold: int = 3       # N starvation events within window
    starvation_window_seconds: float = 30.0

    # Cascading failure — N agents STUCK/FAILED within a short window
    cascade_agent_threshold: int = 3
    cascade_window_seconds: float = 60.0

    # Deadlock — how long a DEADLOCK_SUSPECTED must be unresolved before escalating
    deadlock_escalation_seconds: float = 30.0


# ── Detected failure record ───────────────────────────────────────────────────

@dataclass
class DetectedFailure:
    """Output of a single detection rule firing."""
    failure_type: FailureType
    agent_id: AgentId | None
    detail: str
    telemetry: dict[str, Any] = field(default_factory=dict)
    recommended_strategy: RecoveryStrategy = RecoveryStrategy.WAIT_AND_RETRY
    incident_id: str = field(default_factory=new_incident_id)


# ── Failure Detector ──────────────────────────────────────────────────────────

class FailureDetector:
    """
    Subscribes to the EventBus and maintains sliding-window counters
    for all failure detection rules.

    Usage
    ─────
        detector = FailureDetector(bus, config=DetectorConfig())
        await detector.start()   # registers event subscriptions
        ...
        await detector.stop()
    """

    def __init__(
        self,
        bus: EventBus,
        config: DetectorConfig | None = None,
        *,
        on_failure: Any | None = None,  # async callback: (DetectedFailure) → None
    ) -> None:
        self._bus = bus
        self._cfg = config or DetectorConfig()
        self._on_failure = on_failure  # injected by Recovery Engine in Phase 5

        # Sliding-window state for retry storm detection
        # Maps agent_id → deque of failure timestamps
        self._failure_timestamps: dict[AgentId | str, deque[float]] = defaultdict(deque)

        # Sliding-window state for resource starvation
        self._starvation_timestamps: deque[float] = deque()

        # Sliding-window state for cascading failures (any agent going STUCK/FAILED)
        self._cascade_timestamps: deque[float] = deque()

        # Deadlock tracking — suspected deadlocks waiting for resolution
        # Maps incident_id → (timestamp, payload)
        self._pending_deadlocks: dict[str, tuple[float, dict[str, Any]]] = {}

        # All confirmed failures this session (for introspection)
        self._detected: list[DetectedFailure] = []
        self._detection_counts: dict[FailureType, int] = defaultdict(int)

        self._running = False

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def start(self) -> None:
        """Register event subscriptions on the bus."""
        self._bus.subscribe(self._on_event)
        self._running = True
        log.info("FailureDetector started — monitoring EventBus")

    async def stop(self) -> None:
        self._bus.unsubscribe(self._on_event)
        self._running = False
        log.info("FailureDetector stopped")

    # ── Event handler (all events route here) ─────────────────────────────────

    async def _on_event(self, event: BossmanEvent) -> None:
        """Dispatch incoming events to the appropriate detection rule."""
        if not self._running:
            return

        if event.event_type == EventType.TASK_FAILED:
            await self._check_retry_storm(event)

        elif event.event_type == EventType.AGENT_TIMEOUT:
            await self._handle_heartbeat_timeout(event)

        elif event.event_type == EventType.RESOURCE_STARVATION:
            await self._check_resource_starvation(event)

        elif event.event_type == EventType.DEADLOCK_SUSPECTED:
            await self._handle_deadlock_suspected(event)

        elif event.event_type in {EventType.AGENT_STUCK, EventType.AGENT_TIMEOUT}:
            await self._check_cascade(event)

    # ── Rule 1: Retry storm detection ─────────────────────────────────────────

    async def _check_retry_storm(self, event: BossmanEvent) -> None:
        """
        Retry storm: ≥N TASK_FAILED events for the same agent within window.
        Also detects system-wide storms (all agents combined).

        zylos.md §Part8: "API call rate: spike in calls to the same endpoint
        may indicate a retry loop."
        """
        now = time.monotonic()
        key = event.agent_id or "_global_"
        window = self._cfg.retry_storm_window_seconds
        threshold = self._cfg.retry_storm_threshold

        timestamps = self._failure_timestamps[key]
        timestamps.append(now)
        # Prune old entries
        while timestamps and now - timestamps[0] > window:
            timestamps.popleft()

        if len(timestamps) >= threshold:
            failure = DetectedFailure(
                failure_type=FailureType.RETRY_STORM,
                agent_id=event.agent_id,
                detail=(
                    f"Agent {event.agent_id!r} has failed {len(timestamps)} times "
                    f"in {window:.0f}s (threshold={threshold}). Retry storm detected."
                ),
                telemetry={
                    "failure_count_in_window": len(timestamps),
                    "window_seconds": window,
                    "threshold": threshold,
                    "task_id": event.task_id,
                    "last_error": event.payload.get("error"),
                },
                recommended_strategy=RecoveryStrategy.CIRCUIT_BREAKER,
            )
            # Reset the window to avoid repeat-firing on every subsequent failure
            self._failure_timestamps[key].clear()
            await self._emit_failure(failure)

    # ── Rule 2: Heartbeat timeout ─────────────────────────────────────────────

    async def _handle_heartbeat_timeout(self, event: BossmanEvent) -> None:
        """
        Heartbeat timeout: Watchdog already emits AGENT_TIMEOUT.
        Failure detector wraps it as a FAILURE_DETECTED with FailureType.

        zylos.md §Part1: "An agent may silently loop for 35 minutes...
        The fix: watchdog intervention."
        """
        failure = DetectedFailure(
            failure_type=FailureType.HEARTBEAT_TIMEOUT,
            agent_id=event.agent_id,
            detail=(
                f"Agent {event.agent_id!r} missed heartbeat for "
                f"{event.payload.get('heartbeat_age_seconds', '?')}s "
                f"(threshold: {event.payload.get('threshold_seconds', '?')}s)"
            ),
            telemetry={
                "heartbeat_age_seconds": event.payload.get("heartbeat_age_seconds"),
                "threshold_seconds": event.payload.get("threshold_seconds"),
            },
            recommended_strategy=RecoveryStrategy.RESTART_AGENT,
        )
        # Also feed cascade detector
        await self._check_cascade(event)
        await self._emit_failure(failure)

    # ── Rule 3: Resource starvation ───────────────────────────────────────────

    async def _check_resource_starvation(self, event: BossmanEvent) -> None:
        """
        Resource starvation: ≥N RESOURCE_STARVATION events within window
        across any agents — indicates system-wide token-budget exhaustion.

        zylos.md §Part8: "Resource starvation signal" as a detection target.
        """
        now = time.monotonic()
        window = self._cfg.starvation_window_seconds
        threshold = self._cfg.starvation_threshold

        self._starvation_timestamps.append(now)
        while self._starvation_timestamps and now - self._starvation_timestamps[0] > window:
            self._starvation_timestamps.popleft()

        if len(self._starvation_timestamps) >= threshold:
            failure = DetectedFailure(
                failure_type=FailureType.RESOURCE_STARVATION,
                agent_id=event.agent_id,
                detail=(
                    f"{len(self._starvation_timestamps)} resource starvation events in "
                    f"{window:.0f}s (threshold={threshold}). System may be rate-limited."
                ),
                telemetry={
                    "starvation_count_in_window": len(self._starvation_timestamps),
                    "window_seconds": window,
                    "bucket": event.payload.get("bucket"),
                    "tokens_needed": event.payload.get("tokens_needed"),
                    "tokens_available": event.payload.get("tokens_available"),
                },
                recommended_strategy=RecoveryStrategy.WAIT_AND_RETRY,
            )
            self._starvation_timestamps.clear()
            await self._emit_failure(failure)

    # ── Rule 4: Deadlock detection ────────────────────────────────────────────

    async def _handle_deadlock_suspected(self, event: BossmanEvent) -> None:
        """
        Deadlock: ResourceMediator emits DEADLOCK_SUSPECTED when a circular
        wait is detected. Failure Detector wraps it as a full FAILURE_DETECTED.

        zylos.md §Part3: "The mediator's timeout is critical: it prevents
        deadlock by ensuring no agent waits indefinitely."
        """
        failure = DetectedFailure(
            failure_type=FailureType.DEADLOCK,
            agent_id=event.agent_id,
            detail=(
                f"Circular wait detected: agent {event.agent_id!r} holds "
                f"{event.payload.get('requester_holds')} and waits for "
                f"{event.payload.get('wanted_resource')!r}, "
                f"while holder {event.payload.get('current_holder')!r} "
                f"waits for {event.payload.get('cycle_resources')}"
            ),
            telemetry=dict(event.payload),
            recommended_strategy=RecoveryStrategy.RELEASE_RESOURCES,
        )
        await self._emit_failure(failure)

    # ── Rule 5: Cascading failures ────────────────────────────────────────────

    async def _check_cascade(self, event: BossmanEvent) -> None:
        """
        Cascading failure: ≥N different agents becoming STUCK/FAILED/TIMEOUT
        within a short window — indicates a systemic problem, not an isolated one.

        zylos.md §Part1: "In a pipeline where agents hand off work, a failure
        at step 2 can silently corrupt every subsequent step... a 10-step
        pipeline where each step has 85% reliability succeeds only ~20%."
        """
        now = time.monotonic()
        window = self._cfg.cascade_window_seconds
        threshold = self._cfg.cascade_agent_threshold

        self._cascade_timestamps.append(now)
        while self._cascade_timestamps and now - self._cascade_timestamps[0] > window:
            self._cascade_timestamps.popleft()

        if len(self._cascade_timestamps) >= threshold:
            failure = DetectedFailure(
                failure_type=FailureType.CASCADING_FAILURE,
                agent_id=event.agent_id,
                detail=(
                    f"{len(self._cascade_timestamps)} agents failed/stuck in "
                    f"{window:.0f}s (threshold={threshold}). Cascading failure suspected."
                ),
                telemetry={
                    "failed_agent_count": len(self._cascade_timestamps),
                    "window_seconds": window,
                    "triggering_agent": event.agent_id,
                    "triggering_event": event.event_type.value,
                },
                recommended_strategy=RecoveryStrategy.CIRCUIT_BREAKER,
            )
            self._cascade_timestamps.clear()
            await self._emit_failure(failure)

    # ── Proactive scan (called on-demand, not event-driven) ───────────────────

    async def detect_now(
        self,
        *,
        resource_snapshot: dict[str, Any] | None = None,
        registry_snapshot: dict[str, dict] | None = None,
    ) -> list[DetectedFailure]:
        """
        Proactive point-in-time scan of snapshots.
        Called by BOSSman's main loop or the watchdog to catch things that
        don't generate events (e.g., silent quality degradation signals).

        Returns a list of newly detected failures (may be empty).
        """
        detected: list[DetectedFailure] = []

        if resource_snapshot and registry_snapshot:
            detected.extend(
                await self._scan_for_deadlock_from_snapshot(
                    resource_snapshot, registry_snapshot
                )
            )

        if registry_snapshot:
            detected.extend(
                await self._scan_for_context_overflow(registry_snapshot)
            )

        for failure in detected:
            await self._emit_failure(failure)

        return detected

    async def _scan_for_deadlock_from_snapshot(
        self,
        resource_snap: dict[str, Any],
        registry_snap: dict[str, dict],
    ) -> list[DetectedFailure]:
        """
        Check if any agent in the registry simultaneously holds ≥1 resource
        AND has resources_waiting ≥1 — necessary condition for deadlock.
        Supplements the event-driven DEADLOCK_SUSPECTED check.
        """
        detected = []
        named = resource_snap.get("named_resources", {})

        for agent_id, agent_data in registry_snap.items():
            held = agent_data.get("resources_held", [])
            waiting = agent_data.get("resources_waiting", [])
            if held and waiting:
                detected.append(DetectedFailure(
                    failure_type=FailureType.DEADLOCK,
                    agent_id=agent_id,  # type: ignore[arg-type]
                    detail=(
                        f"Agent {agent_id!r} holds {held} and waits for {waiting} "
                        f"— deadlock candidate (snapshot scan)"
                    ),
                    telemetry={
                        "resources_held": held,
                        "resources_waiting": waiting,
                        "named_resources": named,
                    },
                    recommended_strategy=RecoveryStrategy.RELEASE_RESOURCES,
                ))
        return detected

    async def _scan_for_context_overflow(
        self, registry_snap: dict[str, dict]
    ) -> list[DetectedFailure]:
        """
        Detect agents at ≥95% context utilisation — overflow is imminent.
        zylos.md §Part2: "overflow is almost never sudden... tips the model
        over its limit" at high accumulation.
        """
        detected = []
        for agent_id, agent_data in registry_snap.items():
            util = agent_data.get("context_utilisation", 0.0)
            if util >= 0.95:
                detected.append(DetectedFailure(
                    failure_type=FailureType.CONTEXT_OVERFLOW,
                    agent_id=agent_id,  # type: ignore[arg-type]
                    detail=(
                        f"Agent {agent_id!r} context at {util:.0%} — "
                        f"overflow imminent (threshold=95%)"
                    ),
                    telemetry={"context_utilisation": util},
                    recommended_strategy=RecoveryStrategy.CONTEXT_COMPACTION,
                ))
        return detected

    # ── Emit FAILURE_DETECTED event ───────────────────────────────────────────

    async def _emit_failure(self, failure: DetectedFailure) -> None:
        """Publish FAILURE_DETECTED and invoke the on_failure callback."""
        self._detected.append(failure)
        self._detection_counts[failure.failure_type] += 1

        log.warning(
            "FailureDetector: %s detected (agent=%s): %s",
            failure.failure_type.value,
            failure.agent_id,
            failure.detail[:120],
        )

        event = BossmanEvent.create(
            EventType.FAILURE_DETECTED,
            agent_id=failure.agent_id,
            message=f"[{failure.failure_type.value}] {failure.detail[:200]}",
            payload={
                "failure_type": failure.failure_type.value,
                "recommended_strategy": failure.recommended_strategy.value,
                "incident_id": failure.incident_id,
                "telemetry": failure.telemetry,
            },
        )
        await self._bus.publish(event)

        # Notify Recovery Engine (Phase 5) if callback registered
        if self._on_failure is not None:
            try:
                await self._on_failure(failure)
            except Exception as exc:
                log.error("FailureDetector: on_failure callback error: %s", exc)

    # ── Introspection ─────────────────────────────────────────────────────────

    @property
    def stats(self) -> dict[str, Any]:
        return {
            "total_detected": len(self._detected),
            "by_type": {k.value: v for k, v in self._detection_counts.items()},
            "recent": [
                {
                    "failure_type": f.failure_type.value,
                    "agent_id": f.agent_id,
                    "detail": f.detail[:80],
                    "recommended": f.recommended_strategy.value,
                }
                for f in self._detected[-5:]
            ],
        }

    @property
    def detected_failures(self) -> list[DetectedFailure]:
        """All detected failures this session."""
        return list(self._detected)
