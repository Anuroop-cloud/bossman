"""
evaluation.health_checker
──────────────────────────
L1-L4 Health Checker — structured four-level health assessment for BOSSman agents.

Level design (zylos.md §Part4 — Degradation Hierarchy):
  "Level 1 — Full capability: Primary model, all tools, real-time data"
  "Level 2 — Reduced model: Fallback to smaller model when primary unavailable"
  "Level 3 — Cached responses: Serve similar cached results when unreachable"
  "Level 4 — Static fallback: Return pre-defined error with actionable guidance"

BOSSman L1-L4 mapping:
  L1 — LIVENESS      Is the agent alive? Heartbeat within threshold? Not STUCK/FAILED?
  L2 — PROGRESS      Is the agent making forward progress? Failure rate acceptable?
  L3 — RESOURCES     Are resource levels healthy? No deadlock? Budget not exhausted?
  L4 — QUALITY       Silent degradation signals? Quality metadata flags?

The HealthChecker is PURE — it reads snapshots and returns results. It never
mutates state, never publishes events. Those are the Evaluator's job.

HealthCheckStatus values:
  PASS  — level is healthy, no action needed
  WARN  — level is degraded but functional, consider preventive action
  FAIL  — level is unhealthy, recovery needed
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any

from contracts.agent_state import AgentId, AgentState, AgentStatus


class HealthCheckStatus(str, Enum):
    PASS = "PASS"
    WARN = "WARN"
    FAIL = "FAIL"


class HealthLevel(str, Enum):
    L1_LIVENESS = "L1_LIVENESS"
    L2_PROGRESS = "L2_PROGRESS"
    L3_RESOURCES = "L3_RESOURCES"
    L4_QUALITY = "L4_QUALITY"


# Priority ordering: FAIL > WARN > PASS
_STATUS_RANK = {HealthCheckStatus.PASS: 0, HealthCheckStatus.WARN: 1, HealthCheckStatus.FAIL: 2}


@dataclass
class HealthCheckResult:
    level: HealthLevel
    status: HealthCheckStatus
    detail: str
    metrics: dict[str, Any] = field(default_factory=dict)


@dataclass
class AgentHealthReport:
    """
    Full health report for a single agent — one result per level.

    overall_status is the worst of all four levels.
    """
    agent_id: AgentId
    checked_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    results: list[HealthCheckResult] = field(default_factory=list)

    @property
    def overall_status(self) -> HealthCheckStatus:
        if not self.results:
            return HealthCheckStatus.FAIL
        return max(self.results, key=lambda r: _STATUS_RANK[r.status]).status

    @property
    def passed(self) -> bool:
        return self.overall_status == HealthCheckStatus.PASS

    @property
    def failed(self) -> bool:
        return self.overall_status == HealthCheckStatus.FAIL

    def by_level(self, level: HealthLevel) -> HealthCheckResult | None:
        return next((r for r in self.results if r.level == level), None)

    def as_dict(self) -> dict[str, Any]:
        return {
            "agent_id": self.agent_id,
            "overall_status": self.overall_status.value,
            "checked_at": self.checked_at.isoformat(),
            "levels": {r.level.value: {"status": r.status.value, "detail": r.detail, "metrics": r.metrics} for r in self.results},
        }


# ── Thresholds (configurable) ─────────────────────────────────────────────────

DEFAULT_HEARTBEAT_TIMEOUT_SECONDS: float = 30.0
DEFAULT_HEARTBEAT_WARN_SECONDS: float = 20.0
DEFAULT_MAX_CONSECUTIVE_FAILURES: int = 3
DEFAULT_CONTEXT_WARN_THRESHOLD: float = 0.75
DEFAULT_CONTEXT_FAIL_THRESHOLD: float = 0.95
DEFAULT_TOKEN_WARN_FRACTION: float = 0.85
DEFAULT_TOKEN_FAIL_FRACTION: float = 0.95


class HealthChecker:
    """
    Runs L1-L4 health checks against an AgentState snapshot.

    Pure reader — never mutates, never publishes.

    Usage
    ─────
        checker = HealthChecker()
        report = checker.check(agent_state)
        if report.failed:
            ...
    """

    def __init__(
        self,
        heartbeat_timeout_seconds: float = DEFAULT_HEARTBEAT_TIMEOUT_SECONDS,
        heartbeat_warn_seconds: float = DEFAULT_HEARTBEAT_WARN_SECONDS,
        max_consecutive_failures: int = DEFAULT_MAX_CONSECUTIVE_FAILURES,
        context_warn_threshold: float = DEFAULT_CONTEXT_WARN_THRESHOLD,
        context_fail_threshold: float = DEFAULT_CONTEXT_FAIL_THRESHOLD,
        token_warn_fraction: float = DEFAULT_TOKEN_WARN_FRACTION,
        token_fail_fraction: float = DEFAULT_TOKEN_FAIL_FRACTION,
    ) -> None:
        self._hb_timeout = heartbeat_timeout_seconds
        self._hb_warn = heartbeat_warn_seconds
        self._max_consec = max_consecutive_failures
        self._ctx_warn = context_warn_threshold
        self._ctx_fail = context_fail_threshold
        self._tok_warn = token_warn_fraction
        self._tok_fail = token_fail_fraction

    def check(self, state: AgentState) -> AgentHealthReport:
        """Run all four levels and return a full health report."""
        report = AgentHealthReport(agent_id=state.agent_id)
        report.results = [
            self._check_l1_liveness(state),
            self._check_l2_progress(state),
            self._check_l3_resources(state),
            self._check_l4_quality(state),
        ]
        return report

    def check_many(self, states: list[AgentState]) -> list[AgentHealthReport]:
        return [self.check(s) for s in states]

    # ── L1: Liveness ──────────────────────────────────────────────────────────

    def _check_l1_liveness(self, state: AgentState) -> HealthCheckResult:
        """
        Is the agent alive and heartbeating?

        FAIL: STUCK, FAILED, TERMINATED, or heartbeat age > timeout
        WARN: heartbeat age approaching timeout
        PASS: healthy status and recent heartbeat
        """
        # Terminal/unhealthy status → immediate FAIL
        if state.status == AgentStatus.TERMINATED:
            return HealthCheckResult(
                level=HealthLevel.L1_LIVENESS,
                status=HealthCheckStatus.FAIL,
                detail=f"Agent TERMINATED",
                metrics={"status": state.status.value},
            )

        if state.status in {AgentStatus.STUCK, AgentStatus.FAILED}:
            return HealthCheckResult(
                level=HealthLevel.L1_LIVENESS,
                status=HealthCheckStatus.FAIL,
                detail=f"Agent status is {state.status.value} — not healthy",
                metrics={"status": state.status.value, "last_error": state.last_error},
            )

        hb_age = state.heartbeat_age_seconds

        if hb_age > self._hb_timeout:
            return HealthCheckResult(
                level=HealthLevel.L1_LIVENESS,
                status=HealthCheckStatus.FAIL,
                detail=f"Heartbeat {hb_age:.1f}s old (timeout={self._hb_timeout:.0f}s)",
                metrics={"heartbeat_age_seconds": hb_age, "timeout": self._hb_timeout},
            )

        if hb_age > self._hb_warn:
            return HealthCheckResult(
                level=HealthLevel.L1_LIVENESS,
                status=HealthCheckStatus.WARN,
                detail=f"Heartbeat {hb_age:.1f}s old — approaching timeout",
                metrics={"heartbeat_age_seconds": hb_age, "warn_threshold": self._hb_warn},
            )

        return HealthCheckResult(
            level=HealthLevel.L1_LIVENESS,
            status=HealthCheckStatus.PASS,
            detail=f"Agent {state.status.value}, heartbeat {hb_age:.1f}s ago",
            metrics={"heartbeat_age_seconds": hb_age, "status": state.status.value},
        )

    # ── L2: Progress ──────────────────────────────────────────────────────────

    def _check_l2_progress(self, state: AgentState) -> HealthCheckResult:
        """
        Is the agent making forward progress?

        FAIL: consecutive_failures >= max threshold
        WARN: consecutive_failures > 1 (trending toward failure)
        PASS: low/zero failure rate
        """
        consec = state.consecutive_failures
        total_done = state.total_tasks_completed + state.total_tasks_failed

        if consec >= self._max_consec:
            return HealthCheckResult(
                level=HealthLevel.L2_PROGRESS,
                status=HealthCheckStatus.FAIL,
                detail=(
                    f"{consec} consecutive failures (threshold={self._max_consec}). "
                    f"Agent is not making progress."
                ),
                metrics={
                    "consecutive_failures": consec,
                    "threshold": self._max_consec,
                    "total_completed": state.total_tasks_completed,
                    "total_failed": state.total_tasks_failed,
                },
            )

        if consec > 1:
            return HealthCheckResult(
                level=HealthLevel.L2_PROGRESS,
                status=HealthCheckStatus.WARN,
                detail=f"{consec} consecutive failures — trending toward threshold",
                metrics={"consecutive_failures": consec, "threshold": self._max_consec},
            )

        # Check overall failure rate if enough history exists
        if total_done >= 5:
            failure_rate = state.total_tasks_failed / total_done
            if failure_rate > 0.5:
                return HealthCheckResult(
                    level=HealthLevel.L2_PROGRESS,
                    status=HealthCheckStatus.WARN,
                    detail=f"High failure rate {failure_rate:.0%} ({state.total_tasks_failed}/{total_done})",
                    metrics={"failure_rate": failure_rate, "total_tasks": total_done},
                )

        return HealthCheckResult(
            level=HealthLevel.L2_PROGRESS,
            status=HealthCheckStatus.PASS,
            detail=(
                f"Progress OK: {consec} consecutive failures, "
                f"{state.total_tasks_completed} completed"
            ),
            metrics={
                "consecutive_failures": consec,
                "total_completed": state.total_tasks_completed,
            },
        )

    # ── L3: Resources ─────────────────────────────────────────────────────────

    def _check_l3_resources(self, state: AgentState) -> HealthCheckResult:
        """
        Are resource levels healthy?

        FAIL: context ≥ 95%, OR token budget ≥ 95%, OR deadlock suspect
        WARN: context ≥ 75%, OR token budget ≥ 85%
        PASS: all resources within bounds
        """
        usage = state.resource_usage

        # Deadlock suspect: holds AND waits
        if state.is_deadlock_suspect:
            return HealthCheckResult(
                level=HealthLevel.L3_RESOURCES,
                status=HealthCheckStatus.FAIL,
                detail=(
                    f"Deadlock suspect: holds {usage.resources_held}, "
                    f"waiting {usage.resources_waiting}"
                ),
                metrics={
                    "resources_held": usage.resources_held,
                    "resources_waiting": usage.resources_waiting,
                },
            )

        # Context window
        ctx = usage.context_utilisation
        if ctx >= self._ctx_fail:
            return HealthCheckResult(
                level=HealthLevel.L3_RESOURCES,
                status=HealthCheckStatus.FAIL,
                detail=f"Context at {ctx:.0%} — overflow imminent (threshold={self._ctx_fail:.0%})",
                metrics={"context_utilisation": ctx, "fail_threshold": self._ctx_fail},
            )

        if ctx >= self._ctx_warn:
            return HealthCheckResult(
                level=HealthLevel.L3_RESOURCES,
                status=HealthCheckStatus.WARN,
                detail=f"Context at {ctx:.0%} — compaction recommended (threshold={self._ctx_warn:.0%})",
                metrics={"context_utilisation": ctx, "warn_threshold": self._ctx_warn},
            )

        # Token budget
        tok = usage.token_budget_fraction
        if tok >= self._tok_fail:
            return HealthCheckResult(
                level=HealthLevel.L3_RESOURCES,
                status=HealthCheckStatus.FAIL,
                detail=f"Token budget at {tok:.0%} ({usage.tokens_used}/{usage.tokens_budget})",
                metrics={"token_fraction": tok, "tokens_used": usage.tokens_used, "budget": usage.tokens_budget},
            )

        if tok >= self._tok_warn:
            return HealthCheckResult(
                level=HealthLevel.L3_RESOURCES,
                status=HealthCheckStatus.WARN,
                detail=f"Token budget at {tok:.0%} — approaching limit",
                metrics={"token_fraction": tok},
            )

        return HealthCheckResult(
            level=HealthLevel.L3_RESOURCES,
            status=HealthCheckStatus.PASS,
            detail=(
                f"Resources OK: ctx={ctx:.0%}, tokens={tok:.0%}, "
                f"held={usage.resources_held}"
            ),
            metrics={
                "context_utilisation": ctx,
                "token_fraction": tok,
                "resources_held": usage.resources_held,
            },
        )

    # ── L4: Quality ───────────────────────────────────────────────────────────

    def _check_l4_quality(self, state: AgentState) -> HealthCheckResult:
        """
        Silent degradation signals — quality flags in agent metadata.

        zylos.md §Part1: "Perhaps the most insidious failure: the agent
        continues operating but produces progressively lower-quality outputs
        without raising any errors."

        FAIL: quality_degraded flag set in metadata
        WARN: degradation_note present but not flagged as degraded
        PASS: no quality signals
        """
        meta = state.info.metadata

        if meta.get("quality_degraded"):
            return HealthCheckResult(
                level=HealthLevel.L4_QUALITY,
                status=HealthCheckStatus.FAIL,
                detail=(
                    f"Quality degradation flagged: "
                    f"{meta.get('degradation_note', 'no note')}"
                ),
                metrics={
                    "quality_degraded": True,
                    "degradation_note": meta.get("degradation_note"),
                },
            )

        # Directive-based quality signals (set by RecoveryEngine)
        directive = meta.get("directive")
        if directive in {"fallback_model", "compact_context"}:
            return HealthCheckResult(
                level=HealthLevel.L4_QUALITY,
                status=HealthCheckStatus.WARN,
                detail=f"Active recovery directive: {directive!r}",
                metrics={"directive": directive},
            )

        return HealthCheckResult(
            level=HealthLevel.L4_QUALITY,
            status=HealthCheckStatus.PASS,
            detail="No quality degradation signals",
            metrics={"quality_degraded": False, "directive": directive},
        )
