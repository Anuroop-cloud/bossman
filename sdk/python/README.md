# bossman-sdk (Python)

Connect any agent — LangGraph, CrewAI, the OpenAI Agents SDK, the Claude Agent
SDK, or plain Python — to a BOSSman control plane.

```bash
pip install bossman-sdk          # or: pip install -e sdk/python
export BOSSMAN_URL=http://localhost:7070
export BOSSMAN_API_KEY=bm_...    # an agent key (bossman keys create --role agent)
```

```python
import asyncio
from bossman import Bossman

async def handle(task):
    async with agent.limit("llm"):               # fleet-wide LLM concurrency/rate gate
        answer = await my_llm(task.input["question"])
    agent.report(tokens=1200, context=0.31)       # telemetry → watchdog, token-burn detector
    return {"answer": answer}                     # completes the task (validated against output_schema)

async def main():
    global agent
    async with Bossman() as bm, bm.agent(name="researcher-1", role="researcher") as agent:
        await agent.serve(handle)

asyncio.run(main())
```

Synchronous code: `bossman.run(handle_fn, name="researcher-1", role="researcher")`.

| Capability | API |
|---|---|
| Supervised session (heartbeats, directives, fencing, auto re-register) | `bm.agent(...)`, `agent.serve(handler)` |
| Leased tasks, retries, DLQ, checkpoints, child tasks | `task.progress()`, `task.checkpoint()`, `task.rollback()`, `task.spawn()` |
| Exclusive leased locks with deadlock detection | `async with agent.lock("a", "b")` |
| Fleet-wide rate/concurrency limits | `async with agent.limit("llm", cost=1)` |
| Shared circuit breakers | `await agent.guarded("openai", call, fallback=...)` |
| Compensating tool calls (retry with new params / substitute) | `await agent.call_tool("search", fn, params, alternatives={...})` |
| Policy, human approvals, credential gateway | `auth = await agent.authorize("deploy", ...)`; `await agent.proxy(auth, credential="gh", url=...)` |
| Exactly-once side effects | `await agent.once("charge-order-42", charge)` |
| Degradation ladder + semantic cache | `DegradationLadder(primary=..., fallback=..., cache=SemanticCache(), agent=agent)` |
| Known-answer probes, quality scores | `@agent.probe(...)`, `await agent.report_quality(0.9)` |
| Directive hooks | `@agent.on("COMPACT_CONTEXT")` |
| LangGraph / MCP | `bossman.integrations.langgraph.run_graph`, `bossman.integrations.mcp.GuardedSession` |

The `bossman` CLI ships with this package: `bossman status`, `bossman agents`,
`bossman approvals`, `bossman events -f`, … (`bossman --help`).
