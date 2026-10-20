"""
A LangGraph agent supervised by BOSSman (pip install langgraph).

Each node transition becomes the agent's reported step (feeding the
no-progress watchdog), is recorded as a task checkpoint, honours PAUSE at
node boundaries, and LLM calls go through the fleet-wide limit.
"""

import asyncio
import os
import sys
from typing import TypedDict

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from langgraph.graph import END, StateGraph  # noqa: E402

from bossman import Bossman  # noqa: E402
from bossman.integrations.langgraph import run_graph  # noqa: E402
from common import llm  # noqa: E402

agent = None


class State(TypedDict, total=False):
    question: str
    plan: str
    answer: str


async def plan(state: State) -> State:
    async with agent.limit("llm"):
        text, tokens = await llm(f"Plan how to answer: {state['question']}")
    agent.report(tokens=tokens)
    return {"plan": text}


async def answer(state: State) -> State:
    async with agent.limit("llm"):
        text, tokens = await llm(f"Using this plan:\n{state['plan']}\nAnswer: {state['question']}")
    agent.report(tokens=tokens)
    return {"answer": text}


graph = StateGraph(State)
graph.add_node("plan", plan)
graph.add_node("answer", answer)
graph.set_entry_point("plan")
graph.add_edge("plan", "answer")
graph.add_edge("answer", END)
app = graph.compile()


async def handle(task):
    final = await run_graph(agent, app, {"question": task.input["question"]},
                            config={"configurable": {"thread_id": task.task_id}})
    return {"answer": final["answer"]}


async def main():
    global agent
    async with Bossman() as bm, bm.agent(name="lg-researcher", role="researcher") as agent:
        await agent.serve(handle)


if __name__ == "__main__":
    asyncio.run(main())
