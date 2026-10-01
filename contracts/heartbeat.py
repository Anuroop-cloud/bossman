"""
contracts.heartbeat
────────────────────
Heartbeat protocol for the BOSSman agent workforce.

Every running agent MUST emit a heartbeat at a fixed interval (default: 10s).
BOSSman's watchdog declares an agent STUCK if no heartbeat arrives within the
timeout window (default: 30s = 3 missed heartbeats).

Reference: zylos.md §Part2/Watchdog
  "A watchdog timer resets a system if a heartbeat signal is not received
   within a defined interval. For AI agents, watchdog timers provide a backstop
   against infinite loops and hangs the agent itself cannot detect."

Heartbeats are bidirectional:
  Agent → BOSSman : HeartbeatPayload  (liveness + lightweight progress)
  BOSSman → Agent : HeartbeatAck      (optional directives: compact, pause, abort)
"""

from __future__ import annotations

from datetime import datetime, timezone

from pydantic import BaseModel, Field

from contracts.agent_state import AgentId, AgentStatus, TaskId


class HeartbeatPayload(BaseModel):
    """
    Sent by agents → BOSSman on every heartbeat interval.

    Route: POST /agents/{agent_id}/heartbeat
    or WebSocket message with type="heartbeat"

    BOSSman updates AgentState.last_heartbeat and emits AGENT_HEARTBEAT event.
    """

    agent_id: AgentId
    status: AgentStatus
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    # Progress signal — if current_step never changes across consecutive
    # heartbeats, the watchdog flags it as "no progress"
    current_task_id: TaskId | None = None
    current_step: str | None = None

    # Lightweight resource snapshot (full accounting is in AgentState)
    tokens_used: int = 0
    context_utilisation: float = 0.0          # 0.0 – 1.0
    api_calls_since_last_heartbeat: int = 0

    # Agent's own health self-assessment
    self_reported_healthy: bool = True
    self_reported_note: str | None = None

    model_config = {"frozen": True}


class HeartbeatAck(BaseModel):
    """
    BOSSman → agent response to a heartbeat.
    Carries urgent directives — agents MUST honour these.
    """

    received_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    agent_id: AgentId

    # Directives
    compact_context: bool = False             # trigger context compaction immediately
    pause_task: bool = False                  # hold execution until further notice
    abort_task: bool = False                  # cancel current task gracefully
    token_budget_remaining: int | None = None # updated budget signal

    note: str | None = None

    model_config = {"frozen": True}
