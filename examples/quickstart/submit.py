"""Submit a few research tasks and wait for the answers."""

import asyncio

from bossman import Bossman

QUESTIONS = [
    "Why do multi-agent pipelines need idempotency guards?",
    "What is a circuit breaker?",
    "Explain exponential backoff with full jitter.",
]


async def main():
    async with Bossman() as bm:
        tasks = [await bm.submit(q[:60], input={"question": q}, required_role="researcher",
                                 output_schema={"type": "object", "required": ["answer"]})
                 for q in QUESTIONS]
        for t in tasks:
            done = await bm.wait(t["task_id"], timeout=120)
            print(f"[{done['status']}] {t['title']}\n  → {done['output_data'].get('answer', done['last_error'])}\n")


if __name__ == "__main__":
    asyncio.run(main())
