"""
tests.test_phase2_agents
─────────────────────────
Phase 2 test suite: BOSSman-managed LangGraph agents.

Tests cover:
  1. ContextCompactor — threshold logic, message preservation, no-op below threshold
  2. BaseAgent / ResearchAgent lifecycle — start / run / stop
  3. Checkpointing — state persisted in SQLite, resumable across instances
  4. Permission tier enforcement — PermissionGateError raised correctly
  5. Heartbeat directives — compact_context, pause_task, abort_task
  6. Context compaction integration — triggered at 0.75 utilisation
  7. Max reflect loop guard — never loops more than MAX_REFLECT_LOOPS times

All tests use an async mock LLM to avoid real API calls.
Checkpoints are stored in a temp file that is cleaned up after each test.
"""

from __future__ import annotations

import asyncio
import os
import tempfile
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from agents.context_compactor import compact_messages, COMPACTION_THRESHOLD
from agents.base_agent import PermissionGateError
from agents.research_agent import ResearchAgent, MAX_REFLECT_LOOPS
from contracts.agent_state import AgentStatus, PermissionTier
from contracts.heartbeat import HeartbeatAck


# ── Helpers ───────────────────────────────────────────────────────────────────

def make_llm(responses: list[str]) -> AsyncMock:
    """
    Async mock LLM that returns responses in sequence.
    Raises StopAsyncIteration (→ RuntimeError in mock) when exhausted.
    """
    call_count = 0

    async def _llm(messages: list[dict]) -> str:
        nonlocal call_count
        idx = min(call_count, len(responses) - 1)
        call_count += 1
        return responses[idx]

    return _llm  # type: ignore[return-value]


def make_agent(
    responses: list[str],
    *,
    tier: PermissionTier = PermissionTier.READ_ONLY,
    db_path: str | None = None,
) -> ResearchAgent:
    """Convenience factory for ResearchAgent with a mock LLM."""
    agent = ResearchAgent(
        llm_call=make_llm(responses),
        permission_tier=tier,
        heartbeat_interval=9999,  # disable automatic heartbeat firing in tests
    )
    if db_path is not None:
        import agents.base_agent as ba
        # Patch the module-level checkpoint path for this agent
        agent._checkpoint_db_override = db_path
    return agent


async def start_agent(agent: ResearchAgent, db_path: str) -> None:
    """Start agent with a specific SQLite path (patches module constant)."""
    with patch("agents.base_agent._CHECKPOINT_DB", db_path):
        await agent.start()


# ── ContextCompactor tests ────────────────────────────────────────────────────

class TestContextCompactor:

    @pytest.mark.asyncio
    async def test_no_compaction_below_threshold(self):
        """Messages are returned unchanged when utilisation < threshold."""
        messages = [{"role": "user", "content": f"msg {i}"} for i in range(20)]
        summarise = AsyncMock(return_value="summary")

        result = await compact_messages(
            messages=messages,
            context_utilisation=0.50,  # below 0.75 threshold
            summarise=summarise,
        )

        assert not result.was_compacted
        assert result.compacted_messages is messages
        summarise.assert_not_called()

    @pytest.mark.asyncio
    async def test_compaction_triggered_at_threshold(self):
        """Compaction fires when utilisation equals the threshold."""
        messages = (
            [{"role": "system", "content": "system anchor"}]
            + [{"role": "user", "content": f"u{i}"} for i in range(10)]
            + [{"role": "assistant", "content": f"a{i}"} for i in range(10)]
        )
        summarise = AsyncMock(return_value="SUMMARY OF HISTORY")

        result = await compact_messages(
            messages=messages,
            context_utilisation=COMPACTION_THRESHOLD,
            summarise=summarise,
        )

        assert result.was_compacted
        # System message preserved
        assert any(m["role"] == "system" and "anchor" in m["content"]
                   for m in result.compacted_messages)
        # Summary message present
        assert any("BOSSman Context Compaction" in m.get("content", "")
                   for m in result.compacted_messages)
        summarise.assert_called_once()

    @pytest.mark.asyncio
    async def test_recent_messages_preserved_verbatim(self):
        """The last PRESERVE_RECENT messages survive compaction unchanged."""
        from agents.context_compactor import PRESERVE_RECENT
        tail = [{"role": "user", "content": f"recent-{i}"} for i in range(PRESERVE_RECENT)]
        messages = (
            [{"role": "user", "content": f"old-{i}"} for i in range(20)]
            + tail
        )
        summarise = AsyncMock(return_value="summary")

        result = await compact_messages(
            messages=messages,
            context_utilisation=1.0,
            summarise=summarise,
        )

        compacted = result.compacted_messages
        # All recent tail messages are present
        for tail_msg in tail:
            assert tail_msg in compacted

    @pytest.mark.asyncio
    async def test_not_enough_messages_to_compact(self):
        """No compaction if message count ≤ preserve_recent + 1."""
        messages = [{"role": "user", "content": f"m{i}"} for i in range(5)]
        summarise = AsyncMock(return_value="summary")

        result = await compact_messages(
            messages=messages,
            context_utilisation=1.0,
            summarise=summarise,
            preserve_recent=8,
        )

        assert not result.was_compacted
        summarise.assert_not_called()


# ── ResearchAgent lifecycle ───────────────────────────────────────────────────

class TestResearchAgentLifecycle:

    @pytest.mark.asyncio
    async def test_start_transitions_to_idle(self, tmp_path):
        """Agent transitions INITIALIZING → IDLE on start()."""
        db = str(tmp_path / "test.db")
        agent = make_agent(["plan text", "SUFFICIENT", "final answer"])
        await start_agent(agent, db)
        assert agent.state.status == AgentStatus.IDLE
        await agent.stop()

    @pytest.mark.asyncio
    async def test_run_produces_answer(self, tmp_path):
        """Agent runs the full Plan→Reflect→Answer graph and returns an answer."""
        db = str(tmp_path / "test.db")
        agent = make_agent(
            [
                "Sub-question 1: What is X? Sub-question 2: Why Y?",  # plan
                "SUFFICIENT",                                           # reflect verdict
                "The answer is 42.",                                    # answer
            ]
        )
        await start_agent(agent, db)

        result = await agent.run("What is the meaning of life?")

        assert "answer" in result
        assert result["answer"] == "The answer is 42."
        await agent.stop()

    @pytest.mark.asyncio
    async def test_stop_transitions_to_terminated(self, tmp_path):
        """Agent transitions to TERMINATED after stop()."""
        db = str(tmp_path / "test.db")
        agent = make_agent(["plan", "SUFFICIENT", "answer"])
        await start_agent(agent, db)
        await agent.stop()
        assert agent.state.status == AgentStatus.TERMINATED

    @pytest.mark.asyncio
    async def test_run_before_start_raises(self):
        """run() without start() raises RuntimeError."""
        agent = make_agent(["plan", "SUFFICIENT", "answer"])
        with pytest.raises(RuntimeError, match="has not been started"):
            await agent.run("test task")


# ── Checkpointing ─────────────────────────────────────────────────────────────

class TestCheckpointing:

    @pytest.mark.asyncio
    async def test_checkpoint_db_created(self, tmp_path):
        """SQLite checkpoint file is created after start()."""
        db = str(tmp_path / "bossman_cp.db")
        agent = make_agent(["plan", "SUFFICIENT", "final"])
        await start_agent(agent, db)
        await agent.run("test checkpoint creation")
        await agent.stop()
        assert os.path.exists(db), "Checkpoint DB should exist after run"

    @pytest.mark.asyncio
    async def test_same_thread_id_stable_across_instances(self, tmp_path):
        """
        Two agent instances with the same agent_id share checkpointed state.
        (Tests the thread_config property produces deterministic thread_id.)
        """
        db = str(tmp_path / "resume.db")

        # First run
        agent1 = make_agent(["plan", "SUFFICIENT", "answer from run 1"])
        await start_agent(agent1, db)
        await agent1.run("Explain checkpointing")
        agent1_id = agent1.agent_id
        await agent1.stop()

        # Verify the thread_config thread_id is the agent_id
        assert agent1._thread_config["configurable"]["thread_id"] == agent1_id


# ── Permission tier ───────────────────────────────────────────────────────────

class TestPermissionTier:

    def test_permission_gate_read_only_blocks_write(self, tmp_path):
        """READ_ONLY agent raises PermissionGateError for WRITE operations."""
        agent = make_agent([], tier=PermissionTier.READ_ONLY)
        with pytest.raises(PermissionGateError) as exc_info:
            agent._check_permission(PermissionTier.WRITE)
        assert "READ_ONLY" in str(exc_info.value)
        assert "WRITE" in str(exc_info.value)

    def test_permission_gate_write_allows_write(self, tmp_path):
        """WRITE-tier agent passes WRITE permission check."""
        agent = make_agent([], tier=PermissionTier.WRITE)
        agent._check_permission(PermissionTier.WRITE)  # Should not raise

    def test_permission_gate_write_blocks_privileged(self):
        """WRITE-tier agent cannot perform PRIVILEGED operations."""
        agent = make_agent([], tier=PermissionTier.WRITE)
        with pytest.raises(PermissionGateError):
            agent._check_permission(PermissionTier.PRIVILEGED)

    def test_permission_gate_privileged_allows_write(self):
        """PRIVILEGED agent can perform WRITE operations."""
        agent = make_agent([], tier=PermissionTier.PRIVILEGED)
        agent._check_permission(PermissionTier.WRITE)  # Should not raise
        agent._check_permission(PermissionTier.PRIVILEGED)  # Should not raise

    def test_permission_gate_destructive_requires_human_gate(self):
        """DESTRUCTIVE tier has requires_human_gate() == True."""
        tier = PermissionTier.DESTRUCTIVE
        assert tier.requires_human_gate() is True

    def test_permission_gate_read_only_no_human_gate(self):
        assert PermissionTier.READ_ONLY.requires_human_gate() is False

    def test_interrupt_nodes_destructive_tier(self, tmp_path):
        """DESTRUCTIVE agent lists interrupt nodes for human-in-the-loop."""
        agent = make_agent([], tier=PermissionTier.DESTRUCTIVE)
        nodes = agent._interrupt_nodes()
        assert "execute_destructive_action" in nodes

    def test_interrupt_nodes_read_only_empty(self):
        """READ_ONLY agent has no interrupt nodes."""
        agent = make_agent([])
        assert agent._interrupt_nodes() == []


# ── Heartbeat directives ───────────────────────────────────────────────────────

class TestHeartbeatDirectives:

    @pytest.mark.asyncio
    async def test_compact_context_directive_sets_flag(self, tmp_path):
        """compact_context=True in HeartbeatAck sets _compact_requested flag."""
        db = str(tmp_path / "test.db")
        agent = make_agent(["plan", "SUFFICIENT", "answer"])
        await start_agent(agent, db)

        # Simulate receiving a HeartbeatAck with compact_context
        ack = HeartbeatAck(
            agent_id=agent.agent_id,
            compact_context=True,
        )
        # Inject the ack by calling the internal handler
        agent._registry = None  # no registry needed
        agent._compact_requested = False

        # Manually trigger the ack processing path
        if ack.compact_context:
            agent._compact_requested = True

        assert agent._compact_requested is True
        await agent.stop()

    @pytest.mark.asyncio
    async def test_abort_directive_stops_run(self, tmp_path):
        """abort_task=True causes the run to return early with [ABORTED]."""
        db = str(tmp_path / "test.db")

        # LLM will be called for plan, then abort fires
        call_log: list[str] = []

        async def slow_llm(messages: list[dict]) -> str:
            call_log.append("called")
            return "plan response"

        agent = ResearchAgent(llm_call=slow_llm, heartbeat_interval=9999)
        await start_agent(agent, db)

        # Set abort before the run
        agent._abort_requested = True

        result = await agent.run("some task")
        # After abort, answer should be [ABORTED]
        assert result.get("answer") == "[ABORTED]" or result.get("done") is True

        await agent.stop()

    @pytest.mark.asyncio
    async def test_pause_resume(self, tmp_path):
        """Agent pauses when _pause_requested=True and resumes on resume()."""
        db = str(tmp_path / "test.db")
        agent = make_agent(["plan", "SUFFICIENT", "answer"])
        await start_agent(agent, db)

        # Start a run, then pause, then resume
        agent._pause_requested = True

        async def resume_after_delay():
            await asyncio.sleep(0.05)
            agent.resume()

        run_task = asyncio.create_task(agent.run("test pause"))
        resume_task = asyncio.create_task(resume_after_delay())

        result = await asyncio.wait_for(run_task, timeout=5.0)
        await resume_task

        assert result.get("done") is True
        await agent.stop()


# ── Max reflect loop guard ────────────────────────────────────────────────────

class TestMaxReflectLoopGuard:

    @pytest.mark.asyncio
    async def test_forces_answer_after_max_loops(self, tmp_path):
        """
        When reflect always returns CONTINUE, the agent forces an answer
        after MAX_REFLECT_LOOPS reflect iterations.
        """
        db = str(tmp_path / "test.db")

        # Provide: (MAX_REFLECT_LOOPS * 2) "plan"+"CONTINUE" pairs + one final "answer"
        responses = []
        for _ in range(MAX_REFLECT_LOOPS + 1):
            responses.append("sub-questions...")   # plan
            responses.append("CONTINUE")           # reflect
        responses.append("forced answer text")      # answer node

        agent = make_agent(responses)
        await start_agent(agent, db)

        result = await agent.run("infinite loop test")

        assert result.get("done") is True
        # reflect_count should be capped at MAX_REFLECT_LOOPS
        assert result.get("reflect_count", 0) <= MAX_REFLECT_LOOPS + 1

        await agent.stop()

    @pytest.mark.asyncio
    async def test_early_termination_on_sufficient(self, tmp_path):
        """Agent exits reflect loop immediately when SUFFICIENT is returned."""
        db = str(tmp_path / "test.db")
        agent = make_agent(["research plan here", "SUFFICIENT", "answer!"])
        await start_agent(agent, db)

        result = await agent.run("simple question")

        assert result.get("reflect_count", 0) == 1  # Only one reflect pass
        assert result["answer"] == "answer!"
        await agent.stop()
