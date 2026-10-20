"""
MCP integration — put BOSSman policy in front of every MCP tool call.

    from bossman.integrations.mcp import GuardedSession

    guarded = GuardedSession(session, agent, action_for={"delete_file": "delete", "write_file": "write"})
    result = await guarded.call_tool("write_file", {"path": "...", "content": "..."})

Each call is authorised first (deny → PolicyDenied; irreversible → waits for
human approval), runs through the fleet-wide circuit for the server, and is
reported as a tool call. Works with any object exposing
`call_tool(name, arguments)` (the official MCP ClientSession does).
"""

from __future__ import annotations

from typing import Any

from bossman.agent import Agent


class GuardedSession:
    def __init__(self, session: Any, agent: Agent, *, server: str = "mcp",
                 action_for: dict[str, str] | None = None, default_action: str = "execute",
                 irreversible: set[str] | None = None) -> None:
        self._session = session
        self._agent = agent
        self._server = server
        self._action_for = action_for or {}
        self._default = default_action
        self._irreversible = irreversible or set()

    def __getattr__(self, name: str) -> Any:
        return getattr(self._session, name)

    async def call_tool(self, name: str, arguments: dict[str, Any] | None = None, *,
                        justification: str | None = None, **kw: Any) -> Any:
        action = self._action_for.get(name, self._default)
        await self._agent.authorize(action, tool=name, args=arguments, justification=justification,
                                    irreversible=True if name in self._irreversible else None)
        return await self._agent.guarded(f"{self._server}.{name}",
                                         lambda: self._session.call_tool(name, arguments or {}, **kw))
