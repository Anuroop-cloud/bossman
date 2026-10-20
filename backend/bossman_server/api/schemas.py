"""
api.schemas
───────────
Request bodies for the agent protocol (/v1) and the operator API (/api).
Strict bounds everywhere: the API is the trust boundary.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field

from bossman_server.contracts.agent_state import NAMESPACE_PATTERN, AgentStatus, PermissionTier
from bossman_server.contracts.directives import DirectiveAck, DirectiveType
from bossman_server.core.task_manager import TaskPriority

NAME = r"^[A-Za-z0-9][A-Za-z0-9 ._:/-]{0,63}$"
ROLE = r"^[a-z0-9][a-z0-9._-]{0,63}$"


class ActionBody(BaseModel):
    reason: str = Field(default="", max_length=200)


# ── Agents ───────────────────────────────────────────────────────────────────


class RegisterBody(BaseModel):
    name: str = Field(..., pattern=NAME)
    role: str = Field(..., pattern=ROLE)
    namespace: str | None = Field(default=None, pattern=NAMESPACE_PATTERN)
    permission_tier: PermissionTier = PermissionTier.READ_ONLY
    metadata: dict[str, Any] = Field(default_factory=dict)
    capabilities: list[str] = Field(default_factory=list, max_length=200)
    labels: dict[str, str] = Field(default_factory=dict)
    singleton_key: str | None = Field(default=None, max_length=128)
    takeover: bool = False
    supervisor: str | None = Field(default=None, pattern=ROLE)
    start_order: int = Field(default=0, ge=0, le=10_000)
    managed_id: str | None = Field(default=None, max_length=64)
    sdk: str | None = Field(default=None, max_length=64)
    tokens_budget: int | None = Field(default=None, gt=0)


class HeartbeatBody(BaseModel):
    epoch: int | None = None
    status: AgentStatus = AgentStatus.IDLE
    current_task_id: str | None = Field(default=None, max_length=64)
    current_step: str | None = Field(default=None, max_length=200)
    tokens_used: int = Field(default=0, ge=0)
    context_utilisation: float = Field(default=0.0, ge=0.0, le=1.0)
    api_calls_since_last_heartbeat: int = Field(default=0, ge=0)
    self_reported_healthy: bool = True
    self_reported_note: str | None = Field(default=None, max_length=500)
    degradation_level: int | None = Field(default=None, ge=0, le=4)
    model: str | None = Field(default=None, max_length=128)
    paused: bool | None = None
    acked_directives: list[DirectiveAck] = Field(default_factory=list, max_length=100)
    flags_version: int | None = None


class AckBody(BaseModel):
    ok: bool = True
    result: dict[str, Any] | None = None


class AgentEvent(BaseModel):
    type: Literal["tool_call", "log", "llm_call", "custom"] = "custom"
    name: str = Field(default="", max_length=128)        # tool / model name
    ok: bool = True
    duration_ms: float | None = Field(default=None, ge=0)
    status_code: int | None = None
    error: str | None = Field(default=None, max_length=2000)
    error_class: str | None = Field(default=None, max_length=64)
    transient: bool | None = None
    dependency: str | None = Field(default=None, max_length=128)
    task_id: str | None = Field(default=None, max_length=64)
    trace_id: str | None = Field(default=None, max_length=64)
    span_id: str | None = Field(default=None, max_length=32)
    tokens: int | None = Field(default=None, ge=0)
    message: str = Field(default="", max_length=1000)
    data: dict[str, Any] = Field(default_factory=dict)


class EventsBody(BaseModel):
    events: list[AgentEvent] = Field(..., max_length=500)


class QualityBody(BaseModel):
    score: float = Field(..., ge=0.0, le=1.0)
    task_id: str | None = Field(default=None, max_length=64)
    evaluator: str = Field(default="agent", max_length=64)
    note: str = Field(default="", max_length=500)


class ProbeBody(BaseModel):
    name: str = Field(..., max_length=64)
    input: Any = None
    expect: dict[str, Any]
    interval_s: float = Field(default=300.0, ge=10, le=86_400)
    timeout_s: float = Field(default=60.0, ge=1, le=3600)


class ProbesBody(BaseModel):
    probes: list[ProbeBody] = Field(..., max_length=50)


class ProbeResultBody(BaseModel):
    output: Any = None
    error: str | None = Field(default=None, max_length=2000)


class DirectiveBody(BaseModel):
    type: DirectiveType
    params: dict[str, Any] = Field(default_factory=dict)
    reason: str = Field(default="", max_length=500)
    ttl_s: float = Field(default=600.0, ge=5, le=86_400)


# ── Tasks ────────────────────────────────────────────────────────────────────


class SubmitTaskBody(BaseModel):
    title: str = Field(..., min_length=1, max_length=200)
    description: str = Field(default="", max_length=20_000)
    input: dict[str, Any] = Field(default_factory=dict)
    required_role: str | None = Field(default=None, pattern=ROLE)
    priority: TaskPriority | str = TaskPriority.NORMAL
    max_attempts: int = Field(default=3, ge=1, le=50)
    deadline: datetime | None = None
    idempotency_key: str | None = Field(default=None, max_length=128)
    depends_on: list[str] = Field(default_factory=list, max_length=200)
    output_schema: dict[str, Any] | None = None
    parent_task_id: str | None = Field(default=None, max_length=64)
    traceparent: str | None = Field(default=None, max_length=64)
    namespace: str | None = Field(default=None, pattern=NAMESPACE_PATTERN)
    labels: dict[str, str] = Field(default_factory=dict)

    def priority_enum(self) -> TaskPriority:
        if isinstance(self.priority, TaskPriority):
            return self.priority
        return TaskPriority[str(self.priority).upper()]


class ClaimBody(BaseModel):
    wait_s: float = Field(default=0.0, ge=0, le=60)
    lease_s: float = Field(default=60.0, ge=5, le=3600)
    roles: list[str] = Field(default_factory=list, max_length=20)


class TaskHeartbeatBody(BaseModel):
    lease_id: str
    lease_s: float = Field(default=60.0, ge=5, le=3600)
    step: str | None = Field(default=None, max_length=200)


class CompleteBody(BaseModel):
    lease_id: str | None = None
    output: dict[str, Any] = Field(default_factory=dict)


class FailBody(BaseModel):
    lease_id: str | None = None
    error: str = Field(..., max_length=4000)
    error_class: str | None = Field(default=None, max_length=64)
    status_code: int | None = None
    retryable: bool | None = None


class CheckpointBody(BaseModel):
    lease_id: str | None = None
    phase: str = Field(..., min_length=1, max_length=120)
    state: dict[str, Any] = Field(default_factory=dict)


# ── Resources ────────────────────────────────────────────────────────────────


class LockBody(BaseModel):
    agent_id: str
    resources: list[str] = Field(..., min_length=1, max_length=32)
    ttl_s: float = Field(default=30.0, ge=1, le=3600)
    wait_s: float = Field(default=10.0, ge=0, le=300)
    task_id: str | None = None


class RenewBody(BaseModel):
    ttl_s: float | None = Field(default=None, ge=1, le=3600)


class PermitBody(BaseModel):
    agent_id: str
    cost: float = Field(default=1.0, gt=0, le=1_000_000)
    wait_s: float = Field(default=30.0, ge=0, le=300)


class CircuitReportBody(BaseModel):
    ok: bool
    error: str | None = Field(default=None, max_length=500)
    agent_id: str | None = None


# ── Governance ───────────────────────────────────────────────────────────────


class AuthorizeBody(BaseModel):
    action: str = Field(..., pattern=r"^[a-z0-9][a-z0-9._-]{0,63}$")
    tool: str | None = Field(default=None, max_length=128)
    resource: str | None = Field(default=None, max_length=500)
    irreversible: bool | None = None
    args: Any = None
    justification: str | None = Field(default=None, max_length=2000)
    labels: dict[str, str] = Field(default_factory=dict)
    credentials: list[str] = Field(default_factory=list, max_length=10)
    task_id: str | None = Field(default=None, max_length=64)


class ProxyBody(BaseModel):
    grant_id: str
    credential: str
    method: str = Field(default="GET", max_length=8)
    url: str = Field(..., max_length=4000)
    headers: dict[str, str] = Field(default_factory=dict)
    body: Any = None
    timeout_s: float = Field(default=30.0, gt=0, le=300)


class CompensateBody(BaseModel):
    tool: str = Field(..., max_length=128)
    error_class: str | None = Field(default=None, max_length=64)
    status_code: int | None = None
    error: str = Field(default="", max_length=2000)
    params: dict[str, Any] = Field(default_factory=dict)
    attempt: int = Field(default=1, ge=1, le=100)
    alternatives: list[str] = Field(default_factory=list, max_length=10)
    retry_after_s: float | None = Field(default=None, ge=0, le=3600)
    dependency: str | None = Field(default=None, max_length=128)


class IdempotencyBeginBody(BaseModel):
    agent_id: str
    ttl_s: float = Field(default=300.0, ge=1, le=86_400)


class IdempotencyCompleteBody(BaseModel):
    agent_id: str
    result: Any = None


class DecisionBody(BaseModel):
    reason: str = Field(default="", max_length=500)


class KeyBody(BaseModel):
    name: str = Field(..., pattern=NAME)
    role: Literal["viewer", "agent", "operator", "admin"]
    namespaces: list[str] = Field(default_factory=list, max_length=50)
    max_tier: PermissionTier = PermissionTier.WRITE
    ttl_days: float | None = Field(default=None, gt=0, le=3650)


class FlagBody(BaseModel):
    enabled: bool
    namespace: str | None = Field(default=None, pattern=NAMESPACE_PATTERN)
    role: str | None = Field(default=None, pattern=ROLE)
    agent_id: str | None = None
    rollout_percent: int = Field(default=100, ge=0, le=100)
    note: str = Field(default="", max_length=300)


class SecretBody(BaseModel):
    value: str = Field(..., min_length=1, max_length=20_000)
    namespace: str = "*"
    allowed_hosts: list[str] = Field(default_factory=list, max_length=50)
    allowed_tools: list[str] = Field(default_factory=list, max_length=50)
    allowed_actions: list[str] = Field(default_factory=list, max_length=50)
    min_tier: PermissionTier = PermissionTier.READ_ONLY
    release: Literal["proxy", "direct"] = "proxy"
    header: str = Field(default="Authorization", max_length=64)
    header_format: str = Field(default="Bearer {value}", max_length=128)
    description: str = Field(default="", max_length=300)


class SupervisorBody(BaseModel):
    strategy: Literal["one_for_one", "one_for_all", "rest_for_one"] = "one_for_one"
    max_restarts: int = Field(default=3, ge=0, le=1000)
    window_s: float = Field(default=60.0, gt=0, le=86_400)
    parent: str | None = Field(default=None, pattern=ROLE)
    description: str = Field(default="", max_length=300)


class ManagedBody(BaseModel):
    name: str = Field(..., pattern=ROLE)
    runtime: Literal["process", "docker"] = "process"
    command: list[str] = Field(default_factory=list, max_length=100)
    image: str | None = Field(default=None, max_length=300)
    args: list[str] = Field(default_factory=list, max_length=100)
    env: dict[str, str] = Field(default_factory=dict)
    cwd: str | None = Field(default=None, max_length=1000)
    namespace: str = Field(default="default", pattern=NAMESPACE_PATTERN)
    role: str = Field(default="worker", pattern=ROLE)
    supervisor: str | None = Field(default=None, pattern=ROLE)
    restart_policy: Literal["always", "on-failure", "never"] = "on-failure"
    max_restarts: int = Field(default=5, ge=0, le=1000)
    window_s: float = Field(default=300.0, gt=0)
    graceful_timeout_s: float = Field(default=10.0, gt=0, le=600)
    desired_state: Literal["running", "stopped"] = "running"


class LimitBody(BaseModel):
    max_concurrent: int | None = Field(default=None, ge=1, le=1_000_000)
    rate_per_s: float | None = Field(default=None, gt=0)
    burst: float | None = Field(default=None, gt=0)
    permit_ttl_s: float = Field(default=120.0, ge=1, le=86_400)
    description: str = Field(default="", max_length=300)


class RoleLimitBody(BaseModel):
    max_instances: int | None = Field(default=None, ge=0, le=100_000)


class PolicyBody(BaseModel):
    policy: dict[str, Any] | None = None
    yaml: str | None = Field(default=None, max_length=200_000)
