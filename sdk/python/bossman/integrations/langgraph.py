"""
LangGraph integration — supervise a compiled graph run.

    from bossman.integrations.langgraph import run_graph

    async def handle(task):
        state = await run_graph(agent, app, {"question": task["question"]},
                                config={"configurable": {"thread_id": task.task_id}})
        return {"answer": state["answer"]}

Every node transition is reported as the agent's current step (feeding the
watchdog's no-progress detector), the run pauses at node boundaries while
BOSSman has the agent paused, a ROLLBACK/ABORT stops it, and each node is
recorded as a task checkpoint (phase = node name) so a retried task can see
how far the previous attempt got. LangGraph's own checkpointer (use
`thread_id = task.task_id`) still owns full graph-state resumption.
"""

from __future__ import annotations

from typing import Any

from bossman.agent import Agent


async def run_graph(agent: Agent, app: Any, inputs: Any, *, config: dict[str, Any] | None = None,
                    checkpoint_nodes: bool = True) -> Any:
    final: Any = None
    task = agent._current_task
    async for mode, chunk in app.astream(inputs, config=config, stream_mode=["updates", "values"]):
        if mode == "values":
            final = chunk
            continue
        for node in (chunk or {}):
            agent.report(step=f"node:{node}")
            if task is not None:
                await task.progress(f"node:{node}")
                if checkpoint_nodes:
                    await task.checkpoint(f"node:{node}", {"node": node})
        await agent.wait_if_paused()
        if agent.stopping.is_set():
            raise RuntimeError("agent is shutting down")
        if agent.compact_requested:
            agent.report(step="compaction requested")
    return final
