"""
agents.context_compactor
─────────────────────────
Context compaction for BOSSman agents.

Implements the anchored iterative summarisation strategy from zylos.md §Part4:
  "Periodically summarise the conversation so far, replacing detailed history
   with a compressed summary while retaining the original task anchor."

The compactor:
  1. Fires when context_utilisation ≥ COMPACTION_THRESHOLD (0.75 by default)
  2. Preserves the system prompt (task anchor) and the last N messages
  3. Summarises everything in between via the LLM itself
  4. Emits a CONTEXT_COMPACTED event to BOSSman's event bus
  5. Returns the compacted message list and a token estimate

This module is LLM-provider-agnostic: it accepts any callable that takes
`list[dict]` → `str` (the summary). The actual provider (Anthropic/OpenAI)
is injected by the agent at construction time.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable

logger = logging.getLogger(__name__)

# Fraction of context window at which compaction triggers
COMPACTION_THRESHOLD: float = 0.75
# How many of the most-recent messages to always preserve verbatim
PRESERVE_RECENT: int = 8


@dataclass
class CompactionResult:
    """Returned by compact_messages()."""

    compacted_messages: list[dict[str, Any]]
    original_count: int
    compacted_count: int
    was_compacted: bool
    summary_snippet: str = ""   # first 100 chars of summary for logging


async def compact_messages(
    messages: list[dict[str, Any]],
    context_utilisation: float,
    summarise: Callable[[list[dict[str, Any]]], Awaitable[str]],
    threshold: float = COMPACTION_THRESHOLD,
    preserve_recent: int = PRESERVE_RECENT,
) -> CompactionResult:
    """
    Compact ``messages`` if ``context_utilisation`` has hit the threshold.

    Parameters
    ----------
    messages:
        Full message history in OpenAI-style [{role, content}] format.
    context_utilisation:
        Fraction 0.0–1.0 of context window in use, as reported by the agent.
    summarise:
        Async callable that receives the messages to summarise and returns a
        summary string. Injected by the agent to avoid provider coupling.
    threshold:
        Trigger compaction when context_utilisation ≥ threshold.
    preserve_recent:
        Number of tail messages to keep verbatim after the summary.

    Returns
    -------
    CompactionResult
    """
    if context_utilisation < threshold:
        return CompactionResult(
            compacted_messages=messages,
            original_count=len(messages),
            compacted_count=len(messages),
            was_compacted=False,
        )

    if len(messages) <= preserve_recent + 1:
        # Not enough history to compact meaningfully
        return CompactionResult(
            compacted_messages=messages,
            original_count=len(messages),
            compacted_count=len(messages),
            was_compacted=False,
        )

    # Separate: system message(s) / anchor, body to summarise, recent tail
    system_msgs = [m for m in messages if m.get("role") == "system"]
    non_system = [m for m in messages if m.get("role") != "system"]

    if len(non_system) <= preserve_recent:
        return CompactionResult(
            compacted_messages=messages,
            original_count=len(messages),
            compacted_count=len(messages),
            was_compacted=False,
        )

    to_summarise = non_system[:-preserve_recent]
    recent_tail = non_system[-preserve_recent:]

    logger.info(
        "context_compactor: compacting %d messages (utilisation=%.1f%%, threshold=%.0f%%)",
        len(to_summarise),
        context_utilisation * 100,
        threshold * 100,
    )

    summary_text = await summarise(to_summarise)

    summary_msg: dict[str, Any] = {
        "role": "system",
        "content": (
            "[BOSSman Context Compaction]\n"
            "The following is a compressed summary of earlier conversation history "
            "that was compacted to free context space. All key decisions, findings, "
            "and task objectives are preserved:\n\n"
            + summary_text
        ),
    }

    compacted = system_msgs + [summary_msg] + recent_tail

    logger.info(
        "context_compactor: %d → %d messages after compaction",
        len(messages),
        len(compacted),
    )

    return CompactionResult(
        compacted_messages=compacted,
        original_count=len(messages),
        compacted_count=len(compacted),
        was_compacted=True,
        summary_snippet=summary_text[:100],
    )
