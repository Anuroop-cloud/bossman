"""
contracts.events
────────────────
The complete event vocabulary for BOSSman.

Every event — agent lifecycle, task transitions, resource ops, failure signals,
recovery steps — is a typed Pydantic model. Events are:

  1. Persisted to Postgres (events table) for audit and replay.
  2. Published over WebSocket to the dashboard in real-time.
  3. Consumed by Failure Detector, Diagnostician, and Evaluator.

Events are immutable facts. BOSSman never mutates a stored event — only appends.

Taxonomy
────────
AGENT_*      → Agent lifecycle
TASK_*       → Task assignment and progression
RESOURCE_*   → Resource acquisition, release, contention
TOOL_*       → Tool calls and permission gates
HEALTH_*     → Heartbeat and quality signals
FAILURE_*    → Detected failure conditions
RECOVERY_*   → Recovery planning and execution
EVALUATION_* → Post-recovery verification
FAULT_*      → Fault injection (testing)
SYSTEM_*     → BOSSman internal events
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field

from bossman_server.contracts.agent_state import AgentId, TaskId


class EventType(str, Enum):
    # Agent lifecycle
    AGENT_REGISTERED = "AGENT_REGISTERED"
    AGENT_STARTED = "AGENT_STARTED"
    AGENT_IDLE = "AGENT_IDLE"
    AGENT_HEARTBEAT = "AGENT_HEARTBEAT"
    AGENT_TIMEOUT = "AGENT_TIMEOUT"
    AGENT_STUCK = "AGENT_STUCK"
    AGENT_TERMINATED = "AGENT_TERMINATED"
    AGENT_RESTARTED = "AGENT_RESTARTED"
    AGENT_PAUSED = "AGENT_PAUSED"
    AGENT_RESUMED = "AGENT_RESUMED"
    AGENT_FENCED = "AGENT_FENCED"            # stale epoch rejected after takeover
    CONTEXT_COMPACTED = "CONTEXT_COMPACTED"
    AGENT_LOG = "AGENT_LOG"                  # agent-reported log line / custom event

    # Directives (BOSSman → agent control channel)
    DIRECTIVE_ISSUED = "DIRECTIVE_ISSUED"
    DIRECTIVE_ACKED = "DIRECTIVE_ACKED"
    DIRECTIVE_EXPIRED = "DIRECTIVE_EXPIRED"

    # Task lifecycle
    TASK_CREATED = "TASK_CREATED"
    TASK_ASSIGNED = "TASK_ASSIGNED"
    TASK_STARTED = "TASK_STARTED"
    TASK_PROGRESS = "TASK_PROGRESS"
    TASK_COMPLETED = "TASK_COMPLETED"
    TASK_FAILED = "TASK_FAILED"
    TASK_REASSIGNED = "TASK_REASSIGNED"
    TASK_CANCELLED = "TASK_CANCELLED"
    TASK_QUEUED_DLQ = "TASK_QUEUED_DLQ"      # exhausted retries → dead-letter queue
    TASK_RETRY_SCHEDULED = "TASK_RETRY_SCHEDULED"
    TASK_LEASE_EXPIRED = "TASK_LEASE_EXPIRED"
    TASK_BLOCKED = "TASK_BLOCKED"            # waiting on upstream dependencies
    TASK_UNBLOCKED = "TASK_UNBLOCKED"
    TASK_QUARANTINED = "TASK_QUARANTINED"    # upstream failed — never run on poisoned input
    TASK_OUTPUT_INVALID = "TASK_OUTPUT_INVALID"
    TASK_CHECKPOINTED = "TASK_CHECKPOINTED"

    # Resource operations
    RESOURCE_ACQUIRED = "RESOURCE_ACQUIRED"
    RESOURCE_RELEASED = "RESOURCE_RELEASED"
    RESOURCE_CONTENTION = "RESOURCE_CONTENTION"
    RESOURCE_TIMEOUT = "RESOURCE_TIMEOUT"
    RESOURCE_STARVATION = "RESOURCE_STARVATION"
    RESOURCE_REVOKED = "RESOURCE_REVOKED"    # lease force-released by BOSSman

    # Tool calls
    TOOL_CALLED = "TOOL_CALLED"
    TOOL_SUCCEEDED = "TOOL_SUCCEEDED"
    TOOL_FAILED = "TOOL_FAILED"
    TOOL_GATED = "TOOL_GATED"               # blocked by permission tier

    # Health signals
    HEALTH_OK = "HEALTH_OK"
    HEALTH_DEGRADED = "HEALTH_DEGRADED"
    HEALTH_UNHEALTHY = "HEALTH_UNHEALTHY"
    CONTEXT_COMPACTION_TRIGGERED = "CONTEXT_COMPACTION_TRIGGERED"
    TOKEN_BUDGET_WARNING = "TOKEN_BUDGET_WARNING"

    # Failure detection
    FAILURE_DETECTED = "FAILURE_DETECTED"
    DEADLOCK_SUSPECTED = "DEADLOCK_SUSPECTED"
    DEADLOCK_CONFIRMED = "DEADLOCK_CONFIRMED"
    RETRY_STORM_DETECTED = "RETRY_STORM_DETECTED"
    CIRCUIT_BREAKER_OPENED = "CIRCUIT_BREAKER_OPENED"
    CIRCUIT_BREAKER_CLOSED = "CIRCUIT_BREAKER_CLOSED"
    CIRCUIT_BREAKER_HALF_OPEN = "CIRCUIT_BREAKER_HALF_OPEN"
    DUPLICATE_SPAWN_BLOCKED = "DUPLICATE_SPAWN_BLOCKED"  # idempotency guard fired

    # Recovery
    RECOVERY_STARTED = "RECOVERY_STARTED"
    RECOVERY_STRATEGY_CHOSEN = "RECOVERY_STRATEGY_CHOSEN"
    RECOVERY_STEP = "RECOVERY_STEP"
    RECOVERY_COMPLETED = "RECOVERY_COMPLETED"
    RECOVERY_FAILED = "RECOVERY_FAILED"
    RECOVERY_ESCALATED = "RECOVERY_ESCALATED"

    # Evaluation
    EVALUATION_STARTED = "EVALUATION_STARTED"
    EVALUATION_PASSED = "EVALUATION_PASSED"
    EVALUATION_FAILED = "EVALUATION_FAILED"

    # Permission / governance
    PERMISSION_DENIED = "PERMISSION_DENIED"
    HUMAN_GATE_REQUIRED = "HUMAN_GATE_REQUIRED"
    HUMAN_GATE_APPROVED = "HUMAN_GATE_APPROVED"
    HUMAN_GATE_REJECTED = "HUMAN_GATE_REJECTED"
    HUMAN_GATE_EXPIRED = "HUMAN_GATE_EXPIRED"
    POLICY_DECISION = "POLICY_DECISION"
    CREDENTIAL_RELEASED = "CREDENTIAL_RELEASED"
    PROXY_REQUEST = "PROXY_REQUEST"

    # Quality / degradation (Phase 15)
    QUALITY_REPORTED = "QUALITY_REPORTED"
    QUALITY_DEGRADED = "QUALITY_DEGRADED"
    PROBE_PASSED = "PROBE_PASSED"
    PROBE_FAILED = "PROBE_FAILED"
    DEGRADATION_CHANGED = "DEGRADATION_CHANGED"
    SIGNAL_ANOMALY = "SIGNAL_ANOMALY"        # leading indicator crossed a threshold
    COMPENSATION_DECIDED = "COMPENSATION_DECIDED"

    # Managed processes / supervision trees (Phase 14)
    MANAGED_STARTED = "MANAGED_STARTED"
    MANAGED_EXITED = "MANAGED_EXITED"
    MANAGED_STOPPED = "MANAGED_STOPPED"
    MANAGED_CRASHLOOP = "MANAGED_CRASHLOOP"
    SUPERVISOR_RESTART = "SUPERVISOR_RESTART"
    SUPERVISOR_ESCALATED = "SUPERVISOR_ESCALATED"

    # Fault injection (testing infrastructure)
    FAULT_INJECTED = "FAULT_INJECTED"

    # Configuration / operator audit
    FLAG_CHANGED = "FLAG_CHANGED"
    POLICY_UPDATED = "POLICY_UPDATED"
    KEY_CREATED = "KEY_CREATED"
    KEY_REVOKED = "KEY_REVOKED"
    SECRET_UPDATED = "SECRET_UPDATED"
    OPERATOR_ACTION = "OPERATOR_ACTION"
    INCIDENT_RESOLVED = "INCIDENT_RESOLVED"

    # System
    BOSSMAN_STARTED = "BOSSMAN_STARTED"
    BOSSMAN_SHUTDOWN = "BOSSMAN_SHUTDOWN"
    SYSTEM_DEGRADED = "SYSTEM_DEGRADED"
    SYSTEM_RECOVERED = "SYSTEM_RECOVERED"


class EventSeverity(str, Enum):
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


# Default severity per event type — used by dashboard for color-coding
_SEVERITY_MAP: dict[EventType, EventSeverity] = {
    EventType.AGENT_TIMEOUT:            EventSeverity.WARNING,
    EventType.AGENT_STUCK:              EventSeverity.WARNING,
    EventType.TASK_FAILED:              EventSeverity.ERROR,
    EventType.TASK_QUEUED_DLQ:          EventSeverity.ERROR,
    EventType.RESOURCE_CONTENTION:      EventSeverity.WARNING,
    EventType.RESOURCE_TIMEOUT:         EventSeverity.ERROR,
    EventType.RESOURCE_STARVATION:      EventSeverity.ERROR,
    EventType.FAILURE_DETECTED:         EventSeverity.ERROR,
    EventType.DEADLOCK_SUSPECTED:       EventSeverity.WARNING,
    EventType.DEADLOCK_CONFIRMED:       EventSeverity.CRITICAL,
    EventType.RETRY_STORM_DETECTED:     EventSeverity.ERROR,
    EventType.CIRCUIT_BREAKER_OPENED:   EventSeverity.WARNING,
    EventType.RECOVERY_FAILED:          EventSeverity.ERROR,
    EventType.RECOVERY_ESCALATED:       EventSeverity.CRITICAL,
    EventType.EVALUATION_FAILED:        EventSeverity.ERROR,
    EventType.PERMISSION_DENIED:        EventSeverity.WARNING,
    EventType.HUMAN_GATE_REQUIRED:      EventSeverity.WARNING,
    EventType.FAULT_INJECTED:           EventSeverity.WARNING,
    EventType.SYSTEM_DEGRADED:          EventSeverity.CRITICAL,
    EventType.AGENT_FENCED:             EventSeverity.WARNING,
    EventType.TASK_LEASE_EXPIRED:       EventSeverity.WARNING,
    EventType.TASK_QUARANTINED:         EventSeverity.ERROR,
    EventType.TASK_OUTPUT_INVALID:      EventSeverity.ERROR,
    EventType.RESOURCE_REVOKED:         EventSeverity.WARNING,
    EventType.HUMAN_GATE_EXPIRED:       EventSeverity.WARNING,
    EventType.QUALITY_DEGRADED:         EventSeverity.ERROR,
    EventType.PROBE_FAILED:             EventSeverity.ERROR,
    EventType.SIGNAL_ANOMALY:           EventSeverity.WARNING,
    EventType.MANAGED_EXITED:           EventSeverity.WARNING,
    EventType.MANAGED_CRASHLOOP:        EventSeverity.CRITICAL,
    EventType.SUPERVISOR_ESCALATED:     EventSeverity.ERROR,
    EventType.DIRECTIVE_EXPIRED:        EventSeverity.WARNING,
}


def default_severity(event_type: EventType) -> EventSeverity:
    return _SEVERITY_MAP.get(event_type, EventSeverity.INFO)


class BossmanEvent(BaseModel):
    """
    Immutable event record. Every observable thing that happens in BOSSman
    is a BossmanEvent — persisted, streamed to dashboard, consumed by engines.
    """

    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    event_type: EventType
    severity: EventSeverity = EventSeverity.INFO
    occurred_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    agent_id: AgentId | None = None
    task_id: TaskId | None = None
    resource_name: str | None = None
    namespace: str | None = None

    # W3C trace context: links every event of a multi-agent pipeline causally
    # (zylos.md §Part7 — "causal trace linkage across steps").
    trace_id: str | None = None
    span_id: str | None = None

    # Human-readable message for the dashboard live-event feed
    message: str = ""

    # Structured payload — keep small and JSON-serialisable
    payload: dict[str, Any] = Field(default_factory=dict)

    model_config = {"frozen": True}

    @classmethod
    def create(
        cls,
        event_type: EventType,
        message: str = "",
        agent_id: AgentId | None = None,
        task_id: TaskId | None = None,
        resource_name: str | None = None,
        payload: dict[str, Any] | None = None,
        severity: EventSeverity | None = None,
        namespace: str | None = None,
        trace_id: str | None = None,
        span_id: str | None = None,
    ) -> BossmanEvent:
        """Factory — auto-fills severity from the default map."""
        return cls(
            event_type=event_type,
            severity=severity or default_severity(event_type),
            message=message,
            agent_id=agent_id,
            task_id=task_id,
            resource_name=resource_name,
            payload=payload or {},
            namespace=namespace,
            trace_id=trace_id,
            span_id=span_id,
        )
