"""
Planner → workers → reviewer pipeline with dependencies, output schemas,
one shared trace, and cascade containment.

    python examples/pipeline/pipeline.py          # runs all three roles + submits a job

The reviewer task depends on every worker task. If a worker's output fails
its schema (or the worker dead-letters), the reviewer is QUARANTINED instead
of reviewing poisoned input — replay the worker from the dashboard/CLI and
the quarantine lifts automatically.
"""

import asyncio
import os
import random
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from bossman import Bossman  # noqa: E402
from common import llm  # noqa: E402

SECTION_SCHEMA = {"type": "object", "required": ["section", "text"],
                  "properties": {"section": {"type": "string"}, "text": {"type": "string", "minLength": 1}}}


async def planner(bm):
    async with bm.agent(name="planner", role="planner") as agent:
        async def plan(task):
            topic = task.input["topic"]
            sections = ["background", "risks", "recommendations"]
            workers = [await task.spawn(f"write: {s}", required_role="writer", input={"topic": topic, "section": s},
                                        output_schema=SECTION_SCHEMA) for s in sections]
            review = await task.spawn("review report", required_role="reviewer",
                                      depends_on=[w["task_id"] for w in workers],
                                      input={"parts": [w["task_id"] for w in workers]})
            return {"review_task": review["task_id"], "trace_id": task.trace_id}
        await agent.serve(plan)


async def writer(bm, n):
    async with bm.agent(name=f"writer-{n}", role="writer") as agent:
        async def write(task):
            async with agent.limit("llm"):
                text, tokens = await llm(f"Write the {task.input['section']} section about {task.input['topic']}")
            agent.report(tokens=tokens)
            if os.getenv("PIPELINE_CHAOS") and random.random() < 0.3:
                return {"section": task.input["section"], "text": ""}  # violates minLength → rejected
            return {"section": task.input["section"], "text": text}
        await agent.serve(write)


async def reviewer(bm):
    async with bm.agent(name="reviewer", role="reviewer") as agent:
        async def review(task):
            parts = [await bm.task(tid) for tid in task.input["parts"]]
            body = "\n".join(p["output_data"]["text"] for p in parts)
            verdict, _ = await llm(f"Review this report for accuracy:\n{body}")
            await agent.report_quality(0.9, task_id=task.task_id, evaluator="reviewer")
            return {"verdict": verdict, "sections": len(parts)}
        await agent.serve(review)


async def main():
    async with Bossman() as bm:
        roles = [asyncio.create_task(planner(bm)), asyncio.create_task(reviewer(bm))]
        roles += [asyncio.create_task(writer(bm, i)) for i in range(2)]
        await asyncio.sleep(1)
        job = await bm.submit("report: agent reliability", required_role="planner",
                              input={"topic": "AI agent reliability"})
        planned = await bm.wait(job["task_id"], timeout=60)
        print("planned:", planned["output_data"])
        review = await bm.wait(planned["output_data"]["review_task"], timeout=180)
        print(f"review {review['status']}: {review['output_data'] or review['quarantine_reason']}")
        print(f"trace: bossman trace {planned['output_data']['trace_id']}")
        for r in roles:
            r.cancel()


if __name__ == "__main__":
    asyncio.run(main())
