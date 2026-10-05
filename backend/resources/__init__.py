"""
resources
─────────
Phase 3: BOSSman Resource Manager.

Provides three complementary layers of resource control, drawn directly
from zylos.md §Part3 (Concurrency Control):

  LLMSemaphore   — caps the number of *concurrent* LLM calls across all agents
  TokenBucket    — caps the *rate* of LLM calls (tokens per second)
  ResourceMediator — brokers named locks with timeout-based deadlock prevention

Together these solve the two orthogonal dimensions of resource contention:
  "how many things are happening simultaneously, and how fast they happen."
  — zylos.md §Part3

All three publish BossmanEvents to the shared EventBus so the dashboard
and failure detector can observe contention in real time.
"""
