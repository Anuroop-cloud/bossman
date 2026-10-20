"""
bossman — connect any AI agent to a BOSSman control plane.

    from bossman import Bossman

    async def handle(task):
        return {"answer": await solve(task.input["question"])}

    async def main():
        async with Bossman() as bm:                       # BOSSMAN_URL, BOSSMAN_API_KEY
            async with bm.agent(name="solver-1", role="solver") as agent:
                await agent.serve(handle)
"""

from bossman import errors
from bossman._version import __version__
from bossman.agent import Agent, Authorization
from bossman.client import Bossman
from bossman.resilience import (
    DegradationLadder,
    LadderResult,
    QueuedForLater,
    SemanticCache,
    full_jitter,
    is_transient,
    retry_with_backoff,
)
from bossman.sync import run
from bossman.task import Task

__all__ = [
    "Agent",
    "Authorization",
    "Bossman",
    "DegradationLadder",
    "LadderResult",
    "QueuedForLater",
    "SemanticCache",
    "Task",
    "__version__",
    "errors",
    "full_jitter",
    "is_transient",
    "retry_with_backoff",
    "run",
]
