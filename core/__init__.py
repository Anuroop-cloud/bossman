"""
core — BOSSman supervisor core (Phase 1).

Exports the five subsystems that form the supervisor runtime:
  - EventBus        : internal pub/sub for all BossmanEvents
  - AgentRegistry   : register, deregister, query the agent workforce
  - TaskManager     : create, assign, and track tasks
  - SubagentManager : idempotency guard (zylos §Part2)
  - Watchdog        : heartbeat monitor + stuck-agent detector
"""

from core.event_bus import EventBus
from core.registry import AgentRegistry
from core.subagent_manager import SubagentManager
from core.task_manager import Task, TaskManager, TaskPriority, TaskStatus
from core.watchdog import Watchdog

__all__ = [
    "EventBus",
    "AgentRegistry",
    "SubagentManager",
    "Task",
    "TaskManager",
    "TaskPriority",
    "TaskStatus",
    "Watchdog",
]
