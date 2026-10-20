"""
Shared helper for the examples: `llm(prompt)` calls Claude when
ANTHROPIC_API_KEY is set, otherwise returns a deterministic stub so every
example runs with no credentials.
"""

from __future__ import annotations

import asyncio
import hashlib
import os

MODEL = os.getenv("EXAMPLE_MODEL", "claude-opus-5-5")
_client = None


async def llm(prompt: str, *, max_tokens: int = 1024) -> tuple[str, int]:
    """Returns (text, tokens_used)."""
    global _client
    if os.getenv("ANTHROPIC_API_KEY"):
        import anthropic

        _client = _client or anthropic.AsyncAnthropic()
        resp = await _client.messages.create(
            model=MODEL, max_tokens=max_tokens, output_config={"effort": "low"},
            messages=[{"role": "user", "content": prompt}])
        text = "".join(b.text for b in resp.content if b.type == "text")
        return text, resp.usage.input_tokens + resp.usage.output_tokens
    await asyncio.sleep(0.3)  # pretend to think
    digest = hashlib.sha256(prompt.encode()).hexdigest()[:8]
    return f"[stub answer {digest}] {prompt[:80]}", len(prompt) // 4 + 50
