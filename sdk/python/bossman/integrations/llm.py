"""
LLM usage helpers: report token usage and context fill from provider responses.

    from bossman.integrations.llm import track_usage

    async with agent.limit("llm"):
        resp = await client.messages.create(model=agent.model, ...)
    track_usage(agent, resp, context_window=1_000_000)

Works with Anthropic (`usage.input_tokens` / `output_tokens`) and
OpenAI-style (`usage.prompt_tokens` / `completion_tokens`) responses, or dicts.
"""

from __future__ import annotations

from typing import Any


def _get(obj: Any, name: str) -> Any:
    if isinstance(obj, dict):
        return obj.get(name)
    return getattr(obj, name, None)


def usage_of(response: Any) -> tuple[int, int]:
    usage = _get(response, "usage") or {}
    inp = _get(usage, "input_tokens") or _get(usage, "prompt_tokens") or 0
    out = _get(usage, "output_tokens") or _get(usage, "completion_tokens") or 0
    cached = _get(usage, "cache_read_input_tokens") or 0
    return int(inp) + int(cached), int(out)


def track_usage(agent: Any, response: Any, *, context_window: int | None = None) -> tuple[int, int]:
    inp, out = usage_of(response)
    kwargs: dict[str, Any] = {"tokens": inp + out, "api_calls": 1}
    if context_window:
        kwargs["context"] = min(1.0, inp / context_window)
    agent.report(**kwargs)
    return inp, out
