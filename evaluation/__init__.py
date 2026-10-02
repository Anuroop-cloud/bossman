"""
evaluation
───────────
Phase 6: Evaluator — L1-L4 health checks + recovery verification loop.

Components:
  HealthChecker    — runs L1-L4 health checks against any AgentState snapshot
  Evaluator        — subscribes to RECOVERY_STARTED events, waits, re-checks,
                     updates the RecoveryLedger outcome
"""
