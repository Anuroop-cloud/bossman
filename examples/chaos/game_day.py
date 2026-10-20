"""
Game day: break things on purpose and watch BOSSman heal them.

Server must run with BOSSMAN_ENABLE_FAULTS=1; use an admin token:
    BOSSMAN_TOKEN=<admin> python examples/chaos/game_day.py

Scenarios (each prints the incident BOSSman opened and how it recovered):
  1. hang      — an agent stops heartbeating mid-task → RESTART + task requeued
  2. deadlock  — two agents grab locks in opposite order → the cycle is refused
  3. overflow  — context at 98% → COMPACT_CONTEXT directive
  4. degraded  — quality flag → SILENT_DEGRADATION incident (human review)
"""

import asyncio
import os

from bossman import Bossman, errors


async def watch_incidents(ops: Bossman, seen: set[str]) -> None:
    for inc in (await ops.get("/api/incidents"))["incidents"]:
        if inc["incident_id"] not in seen:
            seen.add(inc["incident_id"])
            print(f"  ⚑ incident {inc['incident_id']}: {inc['failure_type']} → {inc['recovery_strategy']} "
                  f"[{inc['state']}]")


async def main():
    ops = Bossman(api_key=os.environ["BOSSMAN_TOKEN"])
    agents_key = os.getenv("BOSSMAN_API_KEY")
    bm = Bossman(api_key=agents_key)
    seen: set[str] = set()
    async with bm.agent(name="victim-a", role="chaos", heartbeat_interval=2) as a, \
               bm.agent(name="victim-b", role="chaos", heartbeat_interval=2) as b:
        print("1. deadlock: A holds x and wants y; B holds y and wants x")
        async with a.lock("x"), b.lock("y"):
            waiter = asyncio.create_task(_try(a.lock("y", wait_s=5)))
            await asyncio.sleep(0.5)
            try:
                async with b.lock("x", wait_s=5):
                    pass
            except errors.Deadlock as exc:
                print(f"  ✓ refused: {exc}")
            waiter.cancel()
        await asyncio.sleep(1)
        await watch_incidents(ops, seen)

        print("2. context overflow on A")
        await ops.post("/api/faults", {"fault_type": "CONTEXT_OVERFLOW", "agent_id": a.agent_id,
                                       "params": {"context_utilisation": 0.98}})
        await ops.post(f"/api/agents/{a.agent_id}/directives", {"type": "COMPACT_CONTEXT", "reason": "game day"})
        await asyncio.sleep(3)
        print(f"  ✓ A compact_requested={a.compact_requested}")

        print("3. silent degradation on B")
        await ops.post("/api/faults", {"fault_type": "SILENT_DEGRADATION", "agent_id": b.agent_id})
        for s in (0.3, 0.2, 0.1):
            await b.report_quality(s, evaluator="game-day")
        await asyncio.sleep(1)
        await watch_incidents(ops, seen)

        print("4. hang: B claims a task and goes silent")
        await bm.submit("chaos job", required_role="chaos")
        task = await b.claim(wait_s=5)
        for t in b._loops:
            t.cancel()  # stop heartbeating without deregistering
        print("  (waiting for the watchdog — HEARTBEAT_TIMEOUT_SECONDS)")
        for _ in range(60):
            await asyncio.sleep(2)
            await watch_incidents(ops, seen)
            cur = await ops.get(f"/api/tasks/{task.task_id}")
            if cur["task"]["status"] != "RUNNING":
                print(f"  ✓ task requeued: {cur['task']['status']} ({cur['task']['last_error']})")
                break
    await ops.close()
    await bm.close()


async def _try(cm):
    try:
        async with cm:
            await asyncio.sleep(0.1)
    except Exception:  # noqa: BLE001
        pass


if __name__ == "__main__":
    asyncio.run(main())
