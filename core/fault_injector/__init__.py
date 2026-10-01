"""
fault_injector — Phase 1 testing infrastructure.

The FaultInjector is BOSSman's controlled chaos toolkit. It is built in
Phase 1 (not Phase 10) because designing every subsystem to be testable
under failure is more important than the subsystems themselves.

Usage
─────
Every fault scenario in the final demo starts with a FaultInjector call:

    injector = FaultInjector(registry, task_manager, bus)
    await injector.inject(FaultType.AGENT_HANG, agent_id=agent_id)

Available fault types mirror the six zylos.md failure categories + extras
useful for demo scenarios.
"""

from core.fault_injector.injector import FaultInjector, FaultType, FaultConfig

__all__ = ["FaultInjector", "FaultType", "FaultConfig"]
