"""
contracts.recovery_ledger
──────────────────────────
The Recovery Ledger — BOSSman's audit trail of every failure and recovery.

Inspired by the ALAS framework (zylos.md §Part6):
  "ALAS maintains a recovery ledger of failed operations for post-session
   auditing."

Every entry in the ledger answers:
  - What failed?          (agent_id, task_id, failure_type, failure_detail)
  - What did we try?      (strategy, attempt_number)
  - What happened?        (outcome, verified_by_evaluator)
  - When?                 (timestamps throughout)

The ledger is:
  - Persisted to Postgres (recovery_ledger table) — never mutated, append-only.
  - Used by the Evaluator to confirm recovery actually worked.
  - Used by the LLM Diagnostician as structured telemetry.
  - Surfaced on the dashboard's Recovery Timeline view.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field

from contracts.agent_state import AgentId, TaskId


class RecoveryStrategy(str, Enum):
    """
    The recovery actions BOSSman can take.
    Ordered roughly from least to most disruptive.
    """

    WAIT_AND_RETRY = "WAIT_AND_RETRY"            # Exponential backoff retry
    RESTART_AGENT = "RESTART_AGENT"              # Kill + restart agent process
    REASSIGN_TASK = "REASSIGN_TASK"              # Give task to a different agent
    ROLLBACK_CHECKPOINT = "ROLLBACK_CHECKPOINT"  # Resume from last checkpoint
    CIRCUIT_BREAKER = "CIRCUIT_BREAKER"          # Open circuit, use fallback
    FALLBACK_MODEL = "FALLBACK_MODEL"            # Switch to degraded LLM model
    CONTEXT_COMPACTION = "CONTEXT_COMPACTION"    # Compress context window
    RELEASE_RESOURCES = "RELEASE_RESOURCES"      # Force-release held locks
    QUEUE_FOR_LATER = "QUEUE_FOR_LATER"          # DLQ — defer until capacity
    ESCALATE_HUMAN = "ESCALATE_HUMAN"            # Requires human intervention


class RecoveryOutcome(str, Enum):
    SUCCESS = "SUCCESS"            # Recovery verified by Evaluator
    FAILURE = "FAILURE"            # Recovery attempted but system still broken
    PARTIAL = "PARTIAL"            # Partially recovered, degraded operation
    ESCALATED = "ESCALATED"        # Handed off to next strategy or human
    IN_PROGRESS = "IN_PROGRESS"    # Recovery underway, not yet evaluated


class FailureType(str, Enum):
    """
    Mirrors the six core failure categories from zylos.md §Part1.
    """

    HEARTBEAT_TIMEOUT = "HEARTBEAT_TIMEOUT"
    TASK_TIMEOUT = "TASK_TIMEOUT"
    RETRY_STORM = "RETRY_STORM"
    DEADLOCK = "DEADLOCK"
    RESOURCE_STARVATION = "RESOURCE_STARVATION"
    CONTEXT_OVERFLOW = "CONTEXT_OVERFLOW"
    CASCADING_FAILURE = "CASCADING_FAILURE"
    SILENT_DEGRADATION = "SILENT_DEGRADATION"
    EXTERNAL_SERVICE_FAILURE = "EXTERNAL_SERVICE_FAILURE"
    PERMISSION_VIOLATION = "PERMISSION_VIOLATION"
    DUPLICATE_SPAWN = "DUPLICATE_SPAWN"
    UNKNOWN = "UNKNOWN"


# ── Core ledger entry ─────────────────────────────────────────────────────────


class RecoveryAttempt(BaseModel):
    """
    A single recovery attempt record.

    One failure may produce multiple RecoveryAttempts if the first strategy
    fails and BOSSman escalates to the next. Each attempt is a separate row
    in the recovery_ledger table.
    """

    # Identity
    attempt_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    incident_id: str = Field(
        ...,
        description="Groups all attempts for the same failure incident.",
    )

    # What failed
    agent_id: AgentId
    task_id: TaskId | None = None
    failure_type: FailureType
    failure_detail: str = Field(..., description="Human-readable failure description")
    failure_detected_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    # What we're trying
    strategy: RecoveryStrategy
    attempt_number: int = Field(default=1, ge=1)
    recovery_started_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    # What happened
    outcome: RecoveryOutcome = RecoveryOutcome.IN_PROGRESS
    recovery_completed_at: datetime | None = None
    verified_by_evaluator: bool = False
    evaluator_notes: str | None = None

    # Structured evidence consumed by the LLM Diagnostician
    telemetry_snapshot: dict[str, Any] = Field(
        default_factory=dict,
        description="Raw telemetry at time of failure: API call rates, "
                    "heartbeat age, resource graph, retry counts, etc.",
    )

    # Arbitrary notes from the Recovery Engine
    recovery_notes: str | None = None

    model_config = {"frozen": False}  # Mutable — outcome is updated post-evaluation

    # ── Helpers ───────────────────────────────────────────────────────────────

    def mark_complete(
        self,
        outcome: RecoveryOutcome,
        notes: str | None = None,
        verified: bool = False,
    ) -> None:
        self.outcome = outcome
        self.recovery_completed_at = datetime.now(timezone.utc)
        self.recovery_notes = notes
        self.verified_by_evaluator = verified

    @property
    def duration_seconds(self) -> float | None:
        if self.recovery_completed_at is None:
            return None
        return (self.recovery_completed_at - self.recovery_started_at).total_seconds()

    @property
    def is_terminal(self) -> bool:
        return self.outcome in {
            RecoveryOutcome.SUCCESS,
            RecoveryOutcome.FAILURE,
            RecoveryOutcome.ESCALATED,
        }


def new_incident_id() -> str:
    """Generate a stable incident ID that groups all attempts for one failure."""
    return f"inc-{uuid.uuid4().hex[:12]}"
