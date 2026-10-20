"""
A worker meant to be launched *by* BOSSman (managed mode). It needs no
configuration: BOSSman passes BOSSMAN_URL, BOSSMAN_API_KEY, BOSSMAN_MANAGED_ID,
BOSSMAN_AGENT_NAME, BOSSMAN_ROLE and BOSSMAN_NAMESPACE in its environment.

    bossman managed create -f examples/managed/spec.yaml
    bossman managed                 # state, pid, restarts
    bossman managed logs <id>
    bossman managed restart <id>    # graceful: SHUTDOWN directive → SIGTERM → SIGKILL

Set CRASH_EVERY=3 to watch BOSSman restart it with backoff and, if it keeps
crashing, stop and raise a human gate (crash-loop protection).
"""

import asyncio
import os

from bossman import Bossman

done = 0


async def handle(task):
    global done
    await asyncio.sleep(0.5)
    done += 1
    crash_every = int(os.getenv("CRASH_EVERY", "0"))
    if crash_every and done % crash_every == 0:
        print("simulated crash", flush=True)
        os._exit(1)
    return {"processed": task.input, "by": os.getenv("BOSSMAN_AGENT_NAME")}


async def main():
    async with Bossman() as bm, bm.agent() as agent:   # identity comes from the environment
        print(f"managed worker {agent.name} ({agent.agent_id}, epoch {agent.epoch}) up", flush=True)
        await agent.serve(handle)
        print("shutdown requested — exiting cleanly", flush=True)


if __name__ == "__main__":
    asyncio.run(main())
