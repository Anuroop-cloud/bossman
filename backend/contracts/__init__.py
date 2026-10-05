"""
BOSSman contracts package — Phase 0.

Everything here is the shared language between BOSSman and every agent.
If it isn't in contracts/, it isn't part of the agent contract.
"""

from contracts.agent_state import (
    AgentId,
    AgentInfo,
    AgentState,
    AgentStatus,
    PermissionTier,
    ResourceUsage,
    TaskId,
)
from contracts.events import BossmanEvent, EventType
from contracts.heartbeat import HeartbeatAck, HeartbeatPayload
from contracts.recovery_ledger import RecoveryAttempt, RecoveryOutcome, RecoveryStrategy

__all__ = [
    # agent_state
    "AgentId",
    "AgentInfo",
    "AgentState",
    "AgentStatus",
    "PermissionTier",
    "ResourceUsage",
    "TaskId",
    # events
    "BossmanEvent",
    "EventType",
    # heartbeat
    "HeartbeatAck",
    "HeartbeatPayload",
    # recovery_ledger
    "RecoveryAttempt",
    "RecoveryOutcome",
    "RecoveryStrategy",
]
