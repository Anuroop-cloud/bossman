"""
Quickstart: a supervised research agent.

    export BOSSMAN_URL=http://localhost:7070
    export BOSSMAN_API_KEY=<agent token from .env: BOSSMAN_AGENT_TOKEN>
    python examples/quickstart/agent.py        # run 2–3 copies to see work spread out
    python examples/quickstart/submit.py       # in another terminal

What BOSSman does for this ~40-line agent:
  * hands it tasks through leases (a crash → the task is retried elsewhere)
  * watches its heartbeats and step progress (hang → restart directive)
  * gates every LLM call through the fleet-wide "llm" limit
  * applies PAUSE / RESUME / COMPACT_CONTEXT / FALLBACK_MODEL directives
  * answers known-answer probes so silent degradation is caught
"""

import asyncio
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from bossman import Bossman  # noqa: E402
from common import llm  # noqa: E402

agent = None


async def handle(task):
    question = task.input.get("question", task.title)
    if task.resumed:
        await agent.log(f"resuming {task.task_id} from checkpoint {task.last_checkpoint['phase']}")
    await task.progress("thinking")
    async with agent.limit("llm"):                       # fleet-wide LLM concurrency + rate gate
        answer, tokens = await llm(f"Answer concisely: {question}")
    agent.report(tokens=tokens, context=min(1.0, tokens / 200_000))
    await task.checkpoint("answered", {"chars": len(answer)})
    if agent.compact_requested:                          # BOSSman asked us to free context
        agent.compact_requested = False
        agent.report(context=0.05)
    return {"answer": answer}


async def main():
    global agent
    async with Bossman() as bm:
        async with bm.agent(name=os.getenv("AGENT_NAME", f"researcher-{os.getpid()}"), role="researcher",
                            primary_model="claude-opus-5-5", fallback_model="claude-haiku-5-5",
                            capabilities=["web_search"]) as agent:

            @agent.probe("capital", input="What is the capital of France? One word.",
                         expect={"contains": "paris"}, interval_s=120)
            async def capital_probe(q):
                if not os.getenv("ANTHROPIC_API_KEY"):
                    return "Paris"  # stub mode
                text, _ = await llm(q)
                return text

            print(f"{agent.name} registered as {agent.agent_id} — waiting for tasks (Ctrl-C to stop)")
            await agent.serve(handle, concurrency=2)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
