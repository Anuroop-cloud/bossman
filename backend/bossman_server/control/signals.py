"""
control.signals
───────────────
Leading-indicator monitor — "these metrics should trigger alerts before
thresholds are reached, not after a hang has been in progress for 35
minutes" (zylos.md §Part8). Runs on the leader every few seconds:

  token burn     tokens/min per agent vs an absolute ceiling and vs the
                 agent's own baseline (EWMA)            → TOKEN_BURN_ANOMALY
  API call rate  calls/min per agent                    → RETRY_STORM
  queue backlog  pending grows N samples in a row while
                 nothing completes                      → QUEUE_BACKLOG
  DLQ growth     DLQ depth up by ≥ N within the window   → DLQ_GROWTH
  spawn storm    live agents of a role above its cap     → DUPLICATE_SPAWN

Anomalies are published as SIGNAL_ANOMALY events; the FailureDetector turns
them into incidents. Each (signal, subject) has a cooldown.
"""

from __future__ import annotations

import os
import time
from collections import defaultdict, deque
from typing import Any

from bossman_server.contracts.agent_state import AgentStatus
from bossman_server.contracts.events import BossmanEvent, EventType
from bossman_server.contracts.recovery_ledger import FailureType, RecoveryStrategy
from bossman_server.core.event_bus import EventBus


def _env(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, default))
    except ValueError:
        return default


class SignalMonitor:
    def __init__(self, bus: EventBus, registry: Any, tasks: Any, role_limits: Any | None = None) -> None:
        self._bus = bus
        self._registry = registry
        self._tasks = tasks
        self._role_limits = role_limits  # callable(namespace, role) -> int | None
        self.token_burn_ceiling = _env("BOSSMAN_TOKEN_BURN_PER_MIN", 200_000)
        self.token_burn_factor = _env("BOSSMAN_TOKEN_BURN_FACTOR", 5.0)
        self.api_calls_ceiling = _env("BOSSMAN_API_CALLS_PER_MIN", 120)
        self.backlog_min_pending = int(_env("BOSSMAN_BACKLOG_MIN_PENDING", 50))
        self.backlog_samples = int(_env("BOSSMAN_BACKLOG_SAMPLES", 6))
        self.dlq_growth = int(_env("BOSSMAN_DLQ_GROWTH", 5))
        self.cooldown_s = _env("BOSSMAN_SIGNAL_COOLDOWN_S", 300)
        self._baseline: dict[str, float] = {}
        self._pending_hist: deque[tuple[float, int, int]] = deque(maxlen=60)
        self._dlq_hist: deque[tuple[float, int]] = deque(maxlen=120)
        self._fired: dict[tuple[str, str], float] = {}
        self.anomalies: deque[dict[str, Any]] = deque(maxlen=200)

    def _cool(self, signal: str, subject: str) -> bool:
        now = time.monotonic()
        last = self._fired.get((signal, subject))
        if last is not None and now - last < self.cooldown_s:
            return False
        self._fired[(signal, subject)] = now
        return True

    async def _anomaly(self, signal: str, subject: str, *, failure: FailureType, strategy: RecoveryStrategy,
                       message: str, agent_id: str | None = None, namespace: str | None = None,
                       telemetry: dict[str, Any]) -> None:
        if not self._cool(signal, subject):
            return
        record = {"signal": signal, "subject": subject, "at": time.time(), "message": message, **telemetry}
        self.anomalies.appendleft(record)
        await self._bus.publish(BossmanEvent.create(
            EventType.SIGNAL_ANOMALY, agent_id=agent_id, namespace=namespace, message=message,
            payload={"signal": signal, "failure_type": failure.value,
                     "recommended_strategy": strategy.value,
                     "telemetry": {"signal": signal, **telemetry}}))

    async def tick(self) -> None:
        agents = await self._registry.all()
        by_role: dict[tuple[str, str], int] = defaultdict(int)
        for a in agents:
            if not a.is_alive:
                continue
            by_role[(a.info.namespace, a.info.role)] += 1
            tpm = a.tokens_per_minute
            base = self._baseline.get(a.agent_id)
            if tpm > 0:
                if base is not None and base > 0 and tpm > self.token_burn_factor * base and tpm > 1000:
                    await self._anomaly("token_burn", a.agent_id, failure=FailureType.TOKEN_BURN_ANOMALY,
                                        strategy=RecoveryStrategy.ROLLBACK_CHECKPOINT, agent_id=a.agent_id,
                                        namespace=a.info.namespace,
                                        message=f"{a.info.name} burning {tpm:,.0f} tokens/min "
                                                f"({tpm / base:.1f}× its baseline) — possible loop",
                                        telemetry={"tokens_per_minute": round(tpm), "baseline_per_minute": round(base)})
                elif tpm > self.token_burn_ceiling:
                    await self._anomaly("token_burn", a.agent_id, failure=FailureType.TOKEN_BURN_ANOMALY,
                                        strategy=RecoveryStrategy.ROLLBACK_CHECKPOINT, agent_id=a.agent_id,
                                        namespace=a.info.namespace,
                                        message=f"{a.info.name} burning {tpm:,.0f} tokens/min (ceiling "
                                                f"{self.token_burn_ceiling:,.0f})",
                                        telemetry={"tokens_per_minute": round(tpm),
                                                   "baseline_per_minute": round(base or 0)})
                # baseline only learns from normal-looking samples
                if base is None:
                    self._baseline[a.agent_id] = tpm
                elif tpm <= self.token_burn_factor * base:
                    self._baseline[a.agent_id] = 0.9 * base + 0.1 * tpm
            cpm = a.resource_usage.api_calls_per_minute
            if cpm > self.api_calls_ceiling and a.status != AgentStatus.TERMINATED:
                await self._anomaly("api_rate", a.agent_id, failure=FailureType.RETRY_STORM,
                                    strategy=RecoveryStrategy.CIRCUIT_BREAKER, agent_id=a.agent_id,
                                    namespace=a.info.namespace,
                                    message=f"{a.info.name} making {cpm:.0f} API calls/min — retry loop suspected",
                                    telemetry={"api_calls_per_minute": cpm, "failure_count_in_window": round(cpm),
                                               "window_seconds": 60})

        if self._role_limits is not None:
            for (ns, role), n in by_role.items():
                cap = self._role_limits(ns, role)
                if cap is not None and n > cap:
                    await self._anomaly("spawn_storm", f"{ns}/{role}", failure=FailureType.DUPLICATE_SPAWN,
                                        strategy=RecoveryStrategy.ESCALATE_HUMAN, namespace=ns,
                                        message=f"{n} live '{role}' agents in {ns} (cap {cap})",
                                        telemetry={"live_count": n, "role": role, "expected_max": cap})

        stats = await self._tasks.stats()
        now = time.monotonic()
        self._pending_hist.append((now, stats.get("PENDING", 0), stats.get("COMPLETED", 0)))
        recent = list(self._pending_hist)[-self.backlog_samples:]
        if len(recent) == self.backlog_samples and recent[-1][1] >= self.backlog_min_pending:
            growing = all(b[1] > a[1] for a, b in zip(recent, recent[1:]))
            stalled = recent[-1][2] == recent[0][2]
            if growing and stalled:
                await self._anomaly("queue_backlog", "global", failure=FailureType.QUEUE_BACKLOG,
                                    strategy=RecoveryStrategy.ESCALATE_HUMAN,
                                    message=f"Queue backlog: {recent[-1][1]} pending, rising, nothing completing",
                                    telemetry={"value": recent[-1][1], "threshold": self.backlog_min_pending})
        dlq = stats.get("DLQ_depth", 0)
        self._dlq_hist.append((now, dlq))
        window = [d for t, d in self._dlq_hist if now - t <= 300]
        if window and dlq - min(window) >= self.dlq_growth:
            await self._anomaly("dlq_growth", "global", failure=FailureType.DLQ_GROWTH,
                                strategy=RecoveryStrategy.ESCALATE_HUMAN,
                                message=f"DLQ grew by {dlq - min(window)} in 5 minutes (depth {dlq})",
                                telemetry={"value": dlq, "threshold": self.dlq_growth})
