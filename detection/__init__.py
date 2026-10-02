"""
detection
─────────
Phase 4: Failure Detection + Rule-Based Diagnostician.

Two components:

  FailureDetector  — subscribes to the EventBus, monitors the workforce,
                     and fires FAILURE_DETECTED events when patterns match.

  Diagnostician    — receives a FAILURE_DETECTED event + telemetry snapshot
                     and applies hard-coded rules to return:
                       - confirmed FailureType
                       - recommended RecoveryStrategy (for Phase 5)
                       - structured diagnosis dict for the LLM Diagnostician (Phase 7)

Design principle (zylos.md §Part8 — Detection Before Cascades):
  "These metrics should trigger alerts before thresholds are reached —
   not after a hang has been in progress for 35 minutes."
"""
