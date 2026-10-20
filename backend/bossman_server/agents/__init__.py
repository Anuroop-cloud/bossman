"""
agents
──────
BOSSman-managed LangGraph agents.

Each agent in this package:
  - Is a LangGraph StateGraph with SqliteSaver checkpointing
  - Reports heartbeats to BOSSman's registry
  - Honours HeartbeatAck directives (compact, pause, abort)
  - Has a PermissionTier baked into AgentInfo at construction time
  - Performs context compaction when context_utilisation ≥ 0.75
"""
