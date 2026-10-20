"""
contracts.directives
────────────────────
Directives are BOSSman's control channel *into* an agent.

An agent outside the BOSSman process cannot be reached by mutating its
in-memory state — the only way to change what it does is to tell it. Every
recovery action that needs the agent's cooperation (compact, roll back, fall
back to another model, pause, restart, shut down…) becomes a Directive that
is delivered on the next heartbeat ack (or immediately to a long-polling
agent) and must be acknowledged.

Delivery is at-least-once: an un-acked directive is re-sent, and agents
de-duplicate by `directive_id`.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field

from bossman_server.contracts.agent_state import AgentId

DEFAULT_DIRECTIVE_TTL_S = 600.0


class DirectiveType(str, Enum):
    COMPACT_CONTEXT = "COMPACT_CONTEXT"          # summarise history now
    PAUSE = "PAUSE"                              # hold at the next safe point
    RESUME = "RESUME"
    ABORT_TASK = "ABORT_TASK"                    # cancel the current task (it is requeued)
    ROLLBACK_CHECKPOINT = "ROLLBACK_CHECKPOINT"  # resume from the previous checkpoint
    FALLBACK_MODEL = "FALLBACK_MODEL"            # degrade to the fallback model
    RESTORE_MODEL = "RESTORE_MODEL"              # back to the primary model
    SET_DEGRADATION = "SET_DEGRADATION"          # params.level 0..4 (degradation ladder)
    RELEASE_RESOURCES = "RELEASE_RESOURCES"      # drop every lock the agent holds
    RESTART = "RESTART"                          # reset in-memory state, keep the session
    SHUTDOWN = "SHUTDOWN"                        # graceful exit (managed: before SIGTERM)
    RUN_PROBE = "RUN_PROBE"                      # run a registered known-answer probe
    REFRESH_FLAGS = "REFRESH_FLAGS"


class DirectiveStatus(str, Enum):
    PENDING = "PENDING"
    DELIVERED = "DELIVERED"
    ACKED = "ACKED"
    FAILED = "FAILED"        # agent acked with ok=false
    EXPIRED = "EXPIRED"


def _now() -> datetime:
    return datetime.now(timezone.utc)


class Directive(BaseModel):
    directive_id: str = Field(default_factory=lambda: f"dir-{uuid.uuid4().hex[:12]}")
    agent_id: AgentId
    type: DirectiveType
    params: dict[str, Any] = Field(default_factory=dict)
    reason: str = ""
    incident_id: str | None = None
    issued_by: str = "bossman"
    created_at: datetime = Field(default_factory=_now)
    expires_at: datetime = Field(default_factory=lambda: _now() + timedelta(seconds=DEFAULT_DIRECTIVE_TTL_S))
    status: DirectiveStatus = DirectiveStatus.PENDING
    delivered_at: datetime | None = None
    acked_at: datetime | None = None
    deliveries: int = 0
    result: dict[str, Any] | None = None

    @property
    def expired(self) -> bool:
        return _now() >= self.expires_at


class DirectiveAck(BaseModel):
    directive_id: str
    ok: bool = True
    result: dict[str, Any] | None = None
