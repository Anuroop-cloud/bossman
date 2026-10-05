"""
api.demo
────────
DEVELOPMENT-ONLY seeding (BOSSMAN_ENV=development and BOSSMAN_DEMO_SEED=1).

Populates the *real* runtime through the real registry / task manager /
FaultInjector so every dashboard view has live data to exercise. It adds no
API behaviour; remove the env flag and nothing here runs.
"""

from __future__ import annotations

import asyncio
import logging
import random

from contracts.agent_state import AgentInfo, AgentStatus, PermissionTier
from contracts.heartbeat import HeartbeatPayload
from core.fault_injector.injector import FaultConfig, FaultInjector, FaultType
from core.task_manager import TaskPriority

from api.runtime import BossmanRuntime

log = logging.getLogger(__name__)

_ROSTER = [
    ("Atlas", "researcher", PermissionTier.READ_ONLY),
    ("Nova", "researcher", PermissionTier.READ_ONLY),
    ("Forge", "coder", PermissionTier.WRITE),
    ("Sentinel", "reviewer", PermissionTier.READ_ONLY),
    ("Warden", "operator", PermissionTier.PRIVILEGED),
]


async def run_demo(rt: BossmanRuntime) -> None:
    try:
        ids = []
        for name, role, tier in _ROSTER:
            st = await rt.registry.register(AgentInfo(name=name, role=role, permission_tier=tier))
            await rt.registry.mark_ready(st.agent_id)
            ids.append(st.agent_id)

        titles = ["Summarise Q3 filings", "Refactor auth module", "Review PR #482", "Index knowledge base",
                  "Draft incident report", "Scrape pricing pages", "Audit permissions"]
        task_ids = []
        for i, title in enumerate(titles):
            t = await rt.tasks.create(title, priority=list(TaskPriority)[i % 4],
                                      max_attempts=1 if i == 2 else 3)
            task_ids.append(t.task_id)
        for aid, tid in zip(ids[:4], task_ids[:4]):
            await rt.tasks.assign(tid, aid)
            await rt.tasks.mark_running(tid)
            st = await rt.registry.get(aid)
            st.transition_to(AgentStatus.RUNNING)
            st.current_task_id = tid

        injector = FaultInjector(rt.registry, rt.tasks, rt.bus)
        hung = ids[3]
        flags = {"hang": False}

        async def heartbeats() -> None:
            tokens = {a: random.randint(2_000, 12_000) for a in ids}
            while True:
                for aid in ids:
                    if aid == hung and flags["hang"]:
                        continue
                    st = await rt.registry.get(aid)
                    if st is None:
                        continue
                    tokens[aid] += random.randint(50, 900)
                    await rt.registry.record_heartbeat(HeartbeatPayload(
                        agent_id=aid, status=st.status, current_task_id=st.current_task_id,
                        current_step=f"step-{random.randint(1, 9)}" if st.current_task_id else None,
                        tokens_used=tokens[aid],
                        context_utilisation=min(0.95, 0.2 + (tokens[aid] % 40_000) / 60_000),
                        api_calls_since_last_heartbeat=random.randint(0, 4),
                    ))
                await asyncio.sleep(5)

        hb = asyncio.create_task(heartbeats())
        await asyncio.sleep(15)
        await injector.inject(FaultConfig(fault_type=FaultType.RESOURCE_DEADLOCK, agent_id=ids[0],
                                          target_agent_id=ids[1], note="demo seed"))
        await asyncio.sleep(10)
        await rt.detector.detect_now(
            resource_snapshot=rt.resources.snapshot(),
            registry_snapshot=await rt.registry.snapshot(),
        )
        await injector.inject(FaultConfig(fault_type=FaultType.TASK_FAILURE, agent_id=ids[2], task_id=task_ids[2],
                                          params={"error": "Simulated external API failure"}, note="demo seed"))
        flags["hang"] = True
        await injector.inject(FaultConfig(fault_type=FaultType.AGENT_HANG, agent_id=hung, note="demo seed"))
        await hb
    except asyncio.CancelledError:
        raise
    except Exception:
        log.exception("demo seed failed")
