"""
Run an agent from plain (synchronous) code:

    import bossman

    def handle(task):                      # sync function — runs in a worker thread
        return {"answer": solve(task.input["question"])}

    bossman.run(handle, name="solver-1", role="solver")

Blocks until the agent is told to shut down (SHUTDOWN directive, Ctrl-C).
"""

from __future__ import annotations

import asyncio
import inspect
import logging
import signal
from typing import Any, Callable

from bossman.client import Bossman


def run(handler: Callable[..., Any], *, url: str | None = None, api_key: str | None = None,
        concurrency: int = 1, setup: Callable[[Any], Any] | None = None, **agent_kwargs: Any) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    async def main() -> None:
        async with Bossman(url, api_key=api_key) as bm:
            async with bm.agent(**agent_kwargs) as agent:
                if setup is not None:
                    out = setup(agent)
                    if inspect.isawaitable(out):
                        await out
                loop = asyncio.get_running_loop()
                for sig in (signal.SIGINT, signal.SIGTERM):
                    try:
                        loop.add_signal_handler(sig, agent.stopping.set)
                    except (NotImplementedError, RuntimeError):  # Windows
                        pass

                async def call(task: Any) -> Any:
                    if inspect.iscoroutinefunction(handler):
                        return await handler(task)
                    return await asyncio.to_thread(handler, task)

                await agent.serve(call, concurrency=concurrency)

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
