"""
control.llm
───────────
The one place BOSSman calls an LLM: the LLM Diagnostician (novel failures)
and the optional quality judge. Both take an injected `llm_call(prompt)`
coroutine, so this module is only the production wiring.

Configured when an Anthropic credential is available (ANTHROPIC_API_KEY or
any credential source the SDK resolves) and BOSSMAN_LLM != "off".
Model: BOSSMAN_LLM_MODEL (default claude-opus-5-5); effort: BOSSMAN_LLM_EFFORT
(default low — diagnostics are short structured answers).

Server-side refusal fallback is enabled by default (`fallbacks="default"`);
set BOSSMAN_LLM_FALLBACKS=off to disable it (required on Bedrock/Vertex/Foundry).
"""

from __future__ import annotations

import logging
import os
from typing import Awaitable, Callable

log = logging.getLogger(__name__)

DEFAULT_MODEL = "claude-opus-5-5"
LLMCall = Callable[[str], Awaitable[str]]


class LLMUnavailable(RuntimeError):
    pass


def llm_enabled() -> bool:
    if os.getenv("BOSSMAN_LLM", "auto").lower() == "off":
        return False
    key = os.getenv("ANTHROPIC_API_KEY", "")
    return bool(key) and not key.startswith("your_")


def model_name() -> str:
    return os.getenv("BOSSMAN_LLM_MODEL", DEFAULT_MODEL)


def make_anthropic_call(max_tokens: int = 2048) -> LLMCall:
    """Async `prompt -> text` using the official Anthropic SDK."""
    try:
        import anthropic
    except ImportError as exc:  # pragma: no cover - optional dependency
        raise LLMUnavailable("pip install 'bossman-server[llm]' to enable LLM diagnosis") from exc

    client = anthropic.AsyncAnthropic(max_retries=2, timeout=30.0)
    model = model_name()
    effort = os.getenv("BOSSMAN_LLM_EFFORT", "low")
    use_fallbacks = os.getenv("BOSSMAN_LLM_FALLBACKS", "default").lower() != "off"

    async def call(prompt: str) -> str:
        kwargs = dict(
            model=model,
            max_tokens=max_tokens,
            output_config={"effort": effort},
            messages=[{"role": "user", "content": prompt}],
        )
        if use_fallbacks:
            response = await client.beta.messages.create(
                betas=["server-side-fallback-2026-07-01"], fallbacks="default", **kwargs)
        else:
            response = await client.messages.create(**kwargs)
        if response.stop_reason == "refusal":
            category = getattr(getattr(response, "stop_details", None), "category", None)
            raise LLMUnavailable(f"model declined the request (category={category})")
        text = "".join(b.text for b in response.content if b.type == "text").strip()
        if not text:
            raise LLMUnavailable(f"empty response (stop_reason={response.stop_reason})")
        return text

    return call
