"""
recovery
─────────
Phase 5: Recovery Engine.

Components:
  CircuitBreaker    — three-state machine (CLOSED/OPEN/HALF_OPEN) per agent/resource
  RecoveryEngine    — orchestrates strategy execution, ledger recording, event publishing
"""
