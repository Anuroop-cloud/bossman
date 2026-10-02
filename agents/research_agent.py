"""
agents.research_agent
──────────────────────
BOSSman's first real LangGraph agent: a ResearchAgent.

Role: READ_ONLY researcher that answers questions by reasoning over
provided context. It does NOT call external tools — it reasons with
the LLM alone. This makes it safe to run without API keys in tests
(the LLM call can be mocked).

Graph topology (Plan → Reflect → Answer):

    [START] → plan → reflect → answer → [END]
                ↑       |
                └───────┘  (if reflect decides more work is needed)

Nodes
─────
plan:    Decompose the task into sub-questions.
reflect: Evaluate whether the plan has been sufficiently answered.
         If not, loop back to plan.
answer:  Synthesise the final answer from the plan's work.

Checkpointing
─────────────
Every node transition is persisted by SqliteSaver. If the process
crashes at `reflect`, the next run resumes from there with full state.

Context Compaction
──────────────────
After each LLM call the agent checks context_utilisation. If it hits
the 75% threshold (or receives a compact_context directive), messages
are compacted before the next call.

Permission Tier
───────────────
READ_ONLY (default). The agent cannot write, delete, or modify anything.
Attempting to call _check_permission(WRITE) raises PermissionGateError.
"""

from __future__ import annotations

import logging
import os
from typing import Annotated, Any

from langgraph.graph import StateGraph, END
from langgraph.graph.message import add_messages

from agents.base_agent import BaseAgent
from contracts.agent_state import AgentStatus, PermissionTier, TaskId

logger = logging.getLogger(__name__)

# Maximum reflect → plan iterations before forcing an answer
MAX_REFLECT_LOOPS: int = int(os.getenv("RESEARCH_AGENT_MAX_LOOPS", "3"))


# ── LangGraph state schema ────────────────────────────────────────────────────

class ResearchState(dict):
    """
    LangGraph graph state for ResearchAgent.

    Uses TypedDict-style but as a plain dict for compatibility.
    Fields:
        messages:       Full message history (managed by add_messages reducer)
        task:           Original task string
        plan:           Decomposed sub-questions as a string
        reflect_count:  Number of reflect → plan loops so far
        answer:         Final synthesised answer
        done:           Whether the graph should terminate
    """


# ── Agent class ───────────────────────────────────────────────────────────────

class ResearchAgent(BaseAgent):
    """
    A READ_ONLY LangGraph agent that researches and answers questions.

    Parameters
    ----------
    llm_call:
        Async callable (messages: list[dict]) → str.
        Injected at construction — decouples the agent from the LLM provider,
        making it trivially testable without real API calls.
    name, role, event_bus, registry:
        Passed through to BaseAgent.
    """

    def __init__(
        self,
        *,
        llm_call: Any,  # Callable[[list[dict[str, Any]]], Awaitable[str]]
        name: str = "research-agent",
        role: str = "researcher",
        permission_tier: PermissionTier = PermissionTier.READ_ONLY,
        event_bus: Any | None = None,
        registry: Any | None = None,
        resource_manager: Any | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(
            name=name,
            role=role,
            permission_tier=permission_tier,
            event_bus=event_bus,
            registry=registry,
            resource_manager=resource_manager,
            **kwargs,
        )
        self._llm_call = llm_call

    # ── Graph construction ────────────────────────────────────────────────────

    def _build_graph(self) -> StateGraph:
        """
        Build the Plan → Reflect → Answer LangGraph.
        BaseAgent.start() will compile it with the checkpointer.
        """
        graph = StateGraph(dict)  # state is a plain dict

        graph.add_node("plan", self._node_plan)
        graph.add_node("reflect", self._node_reflect)
        graph.add_node("answer", self._node_answer)

        graph.set_entry_point("plan")
        graph.add_edge("plan", "reflect")
        graph.add_conditional_edges(
            "reflect",
            self._should_continue,
            {"continue": "plan", "answer": "answer"},
        )
        graph.add_edge("answer", END)

        return graph

    # ── Nodes ─────────────────────────────────────────────────────────────────

    async def _node_plan(self, state: dict) -> dict:
        """
        Decompose the task into sub-questions using the LLM.
        Respects pause/abort directives before calling the LLM.
        """
        await self._wait_if_paused()
        if self._abort_requested:
            return {**state, "done": True}

        task = state.get("task", "")
        reflect_count = state.get("reflect_count", 0)
        messages = state.get("messages", [])

        # Build prompt
        if reflect_count == 0:
            user_msg = (
                f"You are a research assistant. Break down this task into "
                f"2-3 specific sub-questions to research:\n\nTask: {task}"
            )
        else:
            prior_plan = state.get("plan", "")
            user_msg = (
                f"Revise your research plan. Prior plan:\n{prior_plan}\n\n"
                f"Go deeper on the most important aspects."
            )

        messages = await self._maybe_compact(messages)
        messages.append({"role": "user", "content": user_msg})

        plan = await self._call_llm(messages)
        messages.append({"role": "assistant", "content": plan})

        self._state.current_step = "plan"
        return {**state, "messages": messages, "plan": plan}

    async def _node_reflect(self, state: dict) -> dict:
        """
        Evaluate the plan. Decide if we have enough to answer or need more work.
        """
        await self._wait_if_paused()
        if self._abort_requested:
            return {**state, "done": True}

        plan = state.get("plan", "")
        messages = state.get("messages", [])
        reflect_count = state.get("reflect_count", 0)

        messages = await self._maybe_compact(messages)
        messages.append({
            "role": "user",
            "content": (
                f"Reflect on this research plan:\n{plan}\n\n"
                f"Reply with exactly one word: 'SUFFICIENT' if we have enough "
                f"to write a complete answer, or 'CONTINUE' if more research is needed."
            ),
        })

        verdict = await self._call_llm(messages)
        messages.append({"role": "assistant", "content": verdict})

        self._state.current_step = "reflect"
        return {
            **state,
            "messages": messages,
            "reflect_verdict": verdict.strip().upper(),
            "reflect_count": reflect_count + 1,
        }

    async def _node_answer(self, state: dict) -> dict:
        """Synthesise the final answer from the accumulated plan and research."""
        await self._wait_if_paused()
        if self._abort_requested:
            return {**state, "done": True, "answer": "[ABORTED]"}

        task = state.get("task", "")
        plan = state.get("plan", "")
        messages = state.get("messages", [])

        messages = await self._maybe_compact(messages)
        messages.append({
            "role": "user",
            "content": (
                f"Based on your research plan:\n{plan}\n\n"
                f"Write a comprehensive, well-structured answer to:\n{task}"
            ),
        })

        answer = await self._call_llm(messages)
        messages.append({"role": "assistant", "content": answer})

        self._state.current_step = "answer"
        self._state.record_success()
        return {**state, "messages": messages, "answer": answer, "done": True}

    # ── Routing ───────────────────────────────────────────────────────────────

    def _should_continue(self, state: dict) -> str:
        """
        Conditional edge: reflect → plan  OR  reflect → answer.
        Forces answer after MAX_REFLECT_LOOPS to prevent infinite loops.
        """
        if state.get("done"):
            return "answer"
        if state.get("reflect_count", 0) >= MAX_REFLECT_LOOPS:
            logger.info(
                "ResearchAgent[%s]: max reflect loops (%d) reached, forcing answer",
                self.agent_id, MAX_REFLECT_LOOPS,
            )
            return "answer"
        verdict = state.get("reflect_verdict", "CONTINUE")
        return "answer" if "SUFFICIENT" in verdict else "continue"

    # ── Public entry point ────────────────────────────────────────────────────

    async def run(self, task: str, **kwargs: Any) -> dict[str, Any]:
        """
        Run the research agent on a task.

        Resumes from the last LangGraph checkpoint if the process was
        interrupted mid-run (the thread_id is stable across restarts).

        Returns the final graph state dict with `answer` populated.
        """
        if self._app is None:
            raise RuntimeError(
                f"ResearchAgent[{self.agent_id}] has not been started. Call await agent.start() first."
            )

        # Transition to RUNNING
        self._state.transition_to(AgentStatus.RUNNING)
        task_id = TaskId(f"research-{hash(task) & 0xFFFFFF:06x}")
        self._state.current_task_id = task_id

        logger.info("ResearchAgent[%s]: starting task=%s", self.agent_id, task_id)

        initial_state: dict[str, Any] = {
            "task": task,
            "messages": [
                {
                    "role": "system",
                    "content": (
                        "You are a careful, methodical research assistant operating under "
                        "BOSSman supervision. You have READ_ONLY permissions. "
                        "You cannot modify, delete, or write to any system. "
                        "Be concise, accurate, and cite your reasoning."
                    ),
                }
            ],
            "plan": "",
            "reflect_count": 0,
            "reflect_verdict": "",
            "answer": "",
            "done": False,
        }

        try:
            result = await self._app.ainvoke(
                initial_state,
                config=self._thread_config,
            )
            self._state.transition_to(AgentStatus.IDLE)
            logger.info("ResearchAgent[%s]: completed task=%s", self.agent_id, task_id)
            return result

        except asyncio.CancelledError:
            logger.warning("ResearchAgent[%s]: task cancelled", self.agent_id)
            self._state.transition_to(AgentStatus.FAILED)
            raise

        except Exception as exc:
            logger.exception("ResearchAgent[%s]: task failed: %s", self.agent_id, exc)
            self._state.record_failure(str(exc))
            self._state.transition_to(AgentStatus.FAILED)
            raise

    # ── LLM call wrapper ──────────────────────────────────────────────────────

    async def _call_llm(self, messages: list[dict[str, Any]]) -> str:
        """
        Call the injected LLM and update context tracking.
        Updates self._tokens_used and self._context_utilisation for heartbeats.
        """
        response = await self._llm_call(messages)

        # Update resource tracking (rough estimate — real providers return usage)
        total_chars = sum(len(str(m.get("content", ""))) for m in messages)
        self._tokens_used += len(response) // 4  # ~4 chars/token
        # Estimate context utilisation (200k char ≈ 50k tokens ≈ typical context window)
        self._context_utilisation = min(total_chars / 200_000, 1.0)
        self._state.resource_usage.context_utilisation = self._context_utilisation
        self._state.resource_usage.tokens_used = self._tokens_used

        return response

    async def _summarise(self, messages: list[dict[str, Any]]) -> str:
        """
        LLM-powered summarisation for context compaction.
        Falls back to the base class text-join if the LLM call fails.
        """
        summary_prompt = [
            {
                "role": "user",
                "content": (
                    "Summarise the following conversation concisely, "
                    "preserving all key research findings, decisions, and pending tasks:\n\n"
                    + "\n".join(
                        f"{m.get('role', '?')}: {str(m.get('content', ''))[:300]}"
                        for m in messages
                    )
                ),
            }
        ]
        try:
            return await self._call_llm(summary_prompt)
        except Exception as exc:
            logger.warning("ResearchAgent[%s]: LLM summarisation failed, using fallback: %s", self.agent_id, exc)
            return await super()._summarise(messages)
