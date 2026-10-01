"""
core.watchdog
─────────────
The BOSSman Watchdog — heartbeat monitor and stuck-agent detector.

From zylos.md §Part2/Watchdog:
  "A watchdog timer is a hardware or software mechanism that resets a system
   if a heartbeat signal is not received within a defined interval. For AI
   agents, watchdog timers provide a backstop against infinite loops and hangs
   that the agent itself cannot detect."

The watchdog runs as a background asyncio task and checks the entire
workforce on every tick. It detects two conditions:

  1. HEARTBEAT TIMEOUT  — no heartbeat received within `timeout_seconds`
     → emit AGENT_TIMEOUT, transition agent to STUCK
     → Recovery Engine picks this up and decides what to do

  2. NO PROGRESS       — heartbeat is alive but current_step hasn't changed
     across `stale_steps_threshold` consecutive checks
     → emit AGENT_STUCK
     → Recovery Engine evaluates

The watchdog does NOT perform recovery itself — that's the Recovery Engine's
job (Phase 5). The watchdog only detects and signals.

SIGUSR1 / SIGTERM graceful shutdown pattern from zylos.md is mapped here
to an asyncio-friendly cancel + cleanup flow.
"""

from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from datetime import datetime, timezone

from contracts.agent_state import AgentId, AgentStatus
from contracts.events import BossmanEvent, EventType
from core.event_bus import EventBus
from core.registry import AgentRegistry

log = logging.getLogger(__name__)


class Watchdog:
    """
    Background async task that polls the agent workforce and fires alerts
    when agents miss heartbeats or show no progress.

    Configuration
    ─────────────
    check_interval_seconds  : how often the watchdog polls (default: 10s)
    timeout_seconds         : heartbeat age that triggers AGENT_TIMEOUT (default: 30s)
    stale_steps_threshold   : consecutive no-progress checks before AGENT_STUCK (default: 3)

    Usage
    ─────
        watchdog = Watchdog(registry, bus)
        await watchdog.start()       # launches background task
        ...
        await watchdog.stop()        # graceful shutdown
    """

    def __init__(
        self,
        registry: AgentRegistry,
        bus: EventBus,
        check_interval_seconds: float = 10.0,
        timeout_seconds: float = 30.0,
        stale_steps_threshold: int = 3,
    ) -> None:
        self._registry = registry
        self._bus = bus
        self._check_interval = check_interval_seconds
        self._timeout = timeout_seconds
        self._stale_threshold = stale_steps_threshold

        # Track the last-seen step for each agent to detect stagnation
        self._last_steps: dict[AgentId, str | None] = {}
        self._stale_counts: dict[AgentId, int] = defaultdict(int)

        self._task: asyncio.Task | None = None
        self._running = False

        # Stats
        self._checks_performed: int = 0
        self._timeouts_detected: int = 0
        self._stucks_detected: int = 0

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def start(self) -> None:
        if self._running:
            log.warning("Watchdog already running")
            return
        self._running = True
        self._task = asyncio.create_task(self._loop(), name="bossman:watchdog")
        log.info(
            "Watchdog started (interval=%.0fs, timeout=%.0fs, stale_threshold=%d)",
            self._check_interval,
            self._timeout,
            self._stale_threshold,
        )
        await self._bus.publish(
            BossmanEvent.create(EventType.BOSSMAN_STARTED, message="Watchdog online")
        )

    async def stop(self) -> None:
        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        log.info("Watchdog stopped")

    # ── Main loop ─────────────────────────────────────────────────────────────

    async def _loop(self) -> None:
        while self._running:
            try:
                await asyncio.sleep(self._check_interval)
                await self._check_workforce()
            except asyncio.CancelledError:
                break
            except Exception:
                log.exception("Watchdog loop error — continuing")

    async def _check_workforce(self) -> None:
        self._checks_performed += 1
        agents = await self._registry.all()

        for agent in agents:
            if not agent.is_alive:
                continue  # Already terminated — skip

            if agent.status in {AgentStatus.INITIALIZING, AgentStatus.IDLE}:
                # Reset stale tracking for idle agents
                self._stale_counts[agent.agent_id] = 0
                self._last_steps[agent.agent_id] = None
                continue

            age = agent.heartbeat_age_seconds

            # ── Check 1: Heartbeat timeout ────────────────────────────────────
            if age > self._timeout:
                await self._handle_timeout(agent.agent_id, age)
                continue  # Don't also check progress for a timed-out agent

            # ── Check 2: No progress (stagnation) ────────────────────────────
            if agent.status == AgentStatus.RUNNING:
                await self._check_progress(agent.agent_id, agent.current_step)

    async def _handle_timeout(self, agent_id: AgentId, age_seconds: float) -> None:
        self._timeouts_detected += 1
        log.warning(
            "Agent %s heartbeat timeout: %.0fs since last heartbeat (threshold=%.0fs)",
            agent_id,
            age_seconds,
            self._timeout,
        )

        try:
            await self._registry.update_status(agent_id, AgentStatus.STUCK)
        except (KeyError, ValueError) as e:
            log.error("Could not transition agent %s to STUCK: %s", agent_id, e)
            return

        await self._bus.publish(
            BossmanEvent.create(
                EventType.AGENT_TIMEOUT,
                message=f"Agent {agent_id} missed heartbeat for {age_seconds:.0f}s "
                        f"(threshold: {self._timeout:.0f}s)",
                agent_id=agent_id,
                payload={
                    "heartbeat_age_seconds": age_seconds,
                    "threshold_seconds": self._timeout,
                },
            )
        )

    async def _check_progress(self, agent_id: AgentId, current_step: str | None) -> None:
        last_step = self._last_steps.get(agent_id)

        if current_step == last_step:
            self._stale_counts[agent_id] += 1
        else:
            # Progress detected — reset counter
            self._stale_counts[agent_id] = 0
            self._last_steps[agent_id] = current_step
            return

        self._last_steps[agent_id] = current_step

        if self._stale_counts[agent_id] >= self._stale_threshold:
            self._stucks_detected += 1
            stale_duration = self._stale_counts[agent_id] * self._check_interval

            log.warning(
                "Agent %s stuck: step %r unchanged for %.0fs (%d checks)",
                agent_id,
                current_step,
                stale_duration,
                self._stale_counts[agent_id],
            )

            try:
                await self._registry.update_status(agent_id, AgentStatus.STUCK)
            except (KeyError, ValueError):
                return

            await self._bus.publish(
                BossmanEvent.create(
                    EventType.AGENT_STUCK,
                    message=f"Agent {agent_id} stuck at step {current_step!r} "
                            f"for ~{stale_duration:.0f}s",
                    agent_id=agent_id,
                    payload={
                        "stuck_step": current_step,
                        "stale_checks": self._stale_counts[agent_id],
                        "estimated_stale_seconds": stale_duration,
                    },
                )
            )

            # Reset counter so we don't spam events
            self._stale_counts[agent_id] = 0

    # ── Stats / introspection ─────────────────────────────────────────────────

    @property
    def stats(self) -> dict[str, int | float]:
        return {
            "checks_performed": self._checks_performed,
            "timeouts_detected": self._timeouts_detected,
            "stucks_detected": self._stucks_detected,
            "check_interval_seconds": self._check_interval,
            "timeout_seconds": self._timeout,
        }
