"""
BOSSman API layer (Phase 8A).

A thin read/adapter layer over the existing Phase 0-7 components. It owns NO
business logic: every value served is read from AgentRegistry, TaskManager,
ResourceManager, RecoveryEngine.ledger, FailureDetector, Evaluator, HealthChecker
or the EventBus. The only mutations exposed are the ones the backend already
supports (DLQ replay / discard) and they are authorised server-side.
"""
