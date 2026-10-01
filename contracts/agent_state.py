"""
contracts.agent_state
─────────────────────
The core data model for every agent in the BOSSman workforce.

Every agent MUST maintain an AgentState and report it on request.
BOSSman uses AgentState as its single source of truth about the workforce.

Design notes
────────────
- AgentStatus is a strict enum — no freeform strings in state machines.
- PermissionTier enforces the Kiro-incident lesson: no agent is "omnipotent"
  by default. Destructive actions (DROP, DELETE, DEPLOY) require PRIVILEGED.
- ResourceUsage is reported by the agent; BOSSman cross-validates against
  the Resource Manager's own accounting.
- AgentState is intentionally flat and JSON-serialisable — it travels over
  WebSockets, gets written to Postgres, and is read by the LLM diagnostician.
  Keep it that way.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from enum import Enum
from typing import Any, NewType

from pydantic import BaseModel, Field, field_validator

# ── Branded types ─────────────────────────────────────────────────────────────

AgentId = NewType("AgentId", str)
TaskId = NewType("TaskId", str)


def new_agent_id() -> AgentId:
    return AgentId(f"agent-{uuid.uuid4().hex[:8]}")


def new_task_id() -> TaskId:
    return TaskId(f"task-{uuid.uuid4().hex[:8]}")


# ── Enums ─────────────────────────────────────────────────────────────────────


class AgentStatus(str, Enum):
    """
    Lifecycle states for a BOSSman-managed agent.

    Transitions (valid only):
        INITIALIZING → IDLE
        IDLE         → RUNNING | TERMINATED
        RUNNING      → IDLE | STUCK | FAILED | RECOVERING | TERMINATED
        STUCK        → RECOVERING | TERMINATED
        FAILED       → RECOVERING | TERMINATED
        RECOVERING   → IDLE | FAILED | TERMINATED
        TERMINATED   → (terminal, no transitions out)
    """

    INITIALIZING = "INITIALIZING"  # Agent process starting up
    IDLE = "IDLE"                  # Alive, no active task
    RUNNING = "RUNNING"            # Actively working on a task
    STUCK = "STUCK"                # Alive but no progress (watchdog detected)
    FAILED = "FAILED"              # Task or agent errored
    RECOVERING = "RECOVERING"      # BOSSman is executing a recovery strategy
    TERMINATED = "TERMINATED"      # Agent is dead (clean or forced)

    @classmethod
    def active(cls) -> frozenset[AgentStatus]:
        return frozenset({cls.RUNNING, cls.STUCK, cls.RECOVERING})

    @classmethod
    def unhealthy(cls) -> frozenset[AgentStatus]:
        return frozenset({cls.STUCK, cls.FAILED, cls.RECOVERING})

    @classmethod
    def terminal(cls) -> frozenset[AgentStatus]:
        return frozenset({cls.TERMINATED})


class PermissionTier(str, Enum):
    """
    Formal trust tiers enforced at the BOSSman infrastructure layer.

    Lesson from Amazon Kiro (Dec 2025): permission constraints expressed only
    in prompts are advisory. These tiers are enforced by the Resource Manager
    and Task Manager — an agent CANNOT bypass them through reasoning.

    READ_ONLY   → Can query data, call read-only tools, report findings.
    WRITE       → Can create/update data, call stateful tools, write files.
    PRIVILEGED  → Can delete data, modify system config. Requires BOSSman gate.
    DESTRUCTIVE → Can DELETE production resources. Requires human-in-the-loop.
    """

    READ_ONLY = "READ_ONLY"
    WRITE = "WRITE"
    PRIVILEGED = "PRIVILEGED"
    DESTRUCTIVE = "DESTRUCTIVE"

    def can_write(self) -> bool:
        return self in {self.WRITE, self.PRIVILEGED, self.DESTRUCTIVE}

    def can_destroy(self) -> bool:
        return self in {self.PRIVILEGED, self.DESTRUCTIVE}

    def requires_human_gate(self) -> bool:
        return self == self.DESTRUCTIVE


# ── Sub-models ────────────────────────────────────────────────────────────────


class ResourceUsage(BaseModel):
    """
    Agent's self-reported resource consumption.
    BOSSman cross-validates this against the Resource Manager's accounting.
    """

    tokens_used: int = Field(default=0, ge=0)
    tokens_budget: int = Field(default=50_000, gt=0)
    api_calls_total: int = Field(default=0, ge=0)
    api_calls_per_minute: float = Field(default=0.0, ge=0.0)
    context_utilisation: float = Field(
        default=0.0,
        ge=0.0,
        le=1.0,
        description="Fraction of context window in use (0.0–1.0). "
                    "Compaction triggers at 0.75 per zylos.md §Part4.",
    )
    resources_held: list[str] = Field(default_factory=list)
    resources_waiting: list[str] = Field(
        default_factory=list,
        description="Non-empty sets raise deadlock suspicion in BOSSman.",
    )

    @property
    def token_budget_fraction(self) -> float:
        return self.tokens_used / self.tokens_budget if self.tokens_budget else 0.0

    @property
    def near_context_limit(self) -> bool:
        """True when compaction should be triggered (≥75% context used)."""
        return self.context_utilisation >= 0.75

    @property
    def near_token_budget(self) -> bool:
        """True when >90% of token budget consumed."""
        return self.token_budget_fraction >= 0.90


class AgentInfo(BaseModel):
    """
    Static registration info — set once when agent registers with BOSSman.
    Never mutated during agent lifetime.
    """

    agent_id: AgentId = Field(default_factory=new_agent_id)
    name: str = Field(..., min_length=1, max_length=64)
    role: str = Field(..., description="e.g. 'researcher', 'coder', 'reviewer'")
    permission_tier: PermissionTier = Field(
        default=PermissionTier.READ_ONLY,
        description="Enforced permission boundary. Defaults to READ_ONLY.",
    )
    registered_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    metadata: dict[str, Any] = Field(default_factory=dict)


# ── Core AgentState ───────────────────────────────────────────────────────────


class AgentState(BaseModel):
    """
    The complete runtime state of a single agent.

    This is the struct BOSSman reads to decide whether an agent is healthy,
    what it's doing, and what resources it holds. Consumed by the watchdog,
    failure detector, diagnostician, and evaluator.
    """

    # Identity
    info: AgentInfo

    # Lifecycle
    status: AgentStatus = AgentStatus.INITIALIZING
    current_task_id: TaskId | None = None
    current_step: str | None = None

    # Health signals
    last_heartbeat: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    consecutive_failures: int = Field(default=0, ge=0)
    total_tasks_completed: int = Field(default=0, ge=0)
    total_tasks_failed: int = Field(default=0, ge=0)

    # Resource accounting
    resource_usage: ResourceUsage = Field(default_factory=ResourceUsage)

    # Diagnostics
    last_error: str | None = None
    last_error_at: datetime | None = None

    # Timestamps
    started_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    last_activity: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    # ── Derived properties ────────────────────────────────────────────────────

    @property
    def agent_id(self) -> AgentId:
        return self.info.agent_id

    @property
    def heartbeat_age_seconds(self) -> float:
        """Seconds since last heartbeat. BOSSman watchdog uses this."""
        return (datetime.now(timezone.utc) - self.last_heartbeat).total_seconds()

    @property
    def is_healthy(self) -> bool:
        return self.status not in AgentStatus.unhealthy()

    @property
    def is_alive(self) -> bool:
        return self.status not in AgentStatus.terminal()

    @property
    def is_deadlock_suspect(self) -> bool:
        """
        True when this agent holds ≥1 resource AND waits on ≥1 resource.
        Necessary (not sufficient) condition for deadlock participation.
        """
        return (
            len(self.resource_usage.resources_held) > 0
            and len(self.resource_usage.resources_waiting) > 0
        )

    # ── Mutation helpers ──────────────────────────────────────────────────────

    def touch(self) -> None:
        self.last_activity = datetime.now(timezone.utc)

    def record_heartbeat(self) -> None:
        """Agent signals liveness. Resets watchdog timer."""
        self.last_heartbeat = datetime.now(timezone.utc)
        self.touch()

    def transition_to(self, new_status: AgentStatus) -> None:
        """
        Attempt a status transition. Raises ValueError for illegal transitions.
        This is the ONLY authorised way to change agent status.
        """
        _VALID: dict[AgentStatus, frozenset[AgentStatus]] = {
            AgentStatus.INITIALIZING: frozenset({AgentStatus.IDLE}),
            AgentStatus.IDLE:         frozenset({AgentStatus.RUNNING, AgentStatus.TERMINATED}),
            AgentStatus.RUNNING:      frozenset({
                AgentStatus.IDLE, AgentStatus.STUCK,
                AgentStatus.FAILED, AgentStatus.RECOVERING, AgentStatus.TERMINATED,
            }),
            AgentStatus.STUCK:        frozenset({AgentStatus.RECOVERING, AgentStatus.TERMINATED}),
            AgentStatus.FAILED:       frozenset({AgentStatus.RECOVERING, AgentStatus.TERMINATED}),
            AgentStatus.RECOVERING:   frozenset({
                AgentStatus.IDLE, AgentStatus.FAILED, AgentStatus.TERMINATED,
            }),
            AgentStatus.TERMINATED:   frozenset(),
        }
        allowed = _VALID.get(self.status, frozenset())
        if new_status not in allowed:
            raise ValueError(
                f"Illegal transition: {self.status} → {new_status} "
                f"(agent={self.agent_id}). Allowed: {[s.value for s in allowed]}"
            )
        self.status = new_status
        self.touch()

    def record_failure(self, error: str) -> None:
        self.last_error = error
        self.last_error_at = datetime.now(timezone.utc)
        self.consecutive_failures += 1
        self.total_tasks_failed += 1
        self.touch()

    def record_success(self) -> None:
        self.consecutive_failures = 0
        self.total_tasks_completed += 1
        self.current_task_id = None
        self.current_step = None
        self.touch()

    @field_validator("status", mode="before")
    @classmethod
    def coerce_status(cls, v: Any) -> AgentStatus:
        if isinstance(v, str):
            return AgentStatus(v)
        return v
