"""
agents.base_agent
──────────────────
Base class for all BOSSman-managed LangGraph agents.

Provides the lifecycle scaffold that every BOSSman agent inherits:

  1. LangGraph StateGraph with SqliteSaver checkpointing
     - State is persisted at every node transition
     - On crash/restart the graph resumes from the last checkpoint
     - thread_id = agent_id (one persisted thread per agent instance)

  2. Permission tier enforcement
     - Each agent is constructed with a PermissionTier
     - _check_permission() gate raises PermissionError before any
       WRITE / PRIVILEGED / DESTRUCTIVE operation
     - Human-in-the-loop interrupt registered for DESTRUCTIVE actions

  3. Heartbeat reporting
     - Agent calls _heartbeat() at the end of each node execution
     - Sends HeartbeatPayload → BOSSman AgentRegistry
     - Processes HeartbeatAck directives: compact, pause, abort

  4. Context compaction
     - Agent tracks token / context_utilisation in its LangGraph state
     - Before each LLM call, _maybe_compact() checks the threshold
     - If triggered, messages are summarised and the compacted list is
       used for the next LLM call

Design notes
────────────
- The graph is defined by subclasses via _build_graph().
  BaseAgent wires in the checkpointer and human-interrupt config.
- All BOSSman events are published through the shared EventBus.
- Agents do NOT import from core.* — they receive injected deps
  (event_bus, registry) at construction time to keep them testable.
"""

from __future__ import annotations

import asyncio
import logging
import os
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Any

from langgraph.graph import StateGraph, END  # noqa: F401
from langgraph.checkpoint.sqlite.aio import AsyncSqliteSaver

from contracts.agent_state import (
    AgentId,
    AgentInfo,
    AgentState,
    AgentStatus,
    PermissionTier,
    ResourceUsage,
    new_agent_id,
)
from contracts.events import BossmanEvent, EventSeverity, EventType
from contracts.heartbeat import HeartbeatAck, HeartbeatPayload
from agents.context_compactor import compact_messages, COMPACTION_THRESHOLD

# Optional — only imported when resource_manager is provided to avoid circular deps
# from resources.resource_manager import ResourceManager  # type: ignore[import]

logger = logging.getLogger(__name__)

# Path to the SQLite checkpoint database
_CHECKPOINT_DB: str = os.getenv("CHECKPOINT_DB_PATH", "./bossman_checkpoints.db")


class PermissionGateError(PermissionError):
    """Raised when an agent attempts an operation above its permission tier."""

    def __init__(self, agent_id: AgentId, required: PermissionTier, held: PermissionTier) -> None:
        super().__init__(
            f"Agent {agent_id} attempted a {required.value} operation "
            f"but holds only {held.value} tier. "
            f"Escalate through BOSSman or use a higher-tier agent."
        )
        self.agent_id = agent_id
        self.required = required
        self.held = held


class BaseAgent(ABC):
    """
    Abstract base for every BOSSman-managed LangGraph agent.

    Subclasses must implement:
      - _build_graph() → StateGraph  — define nodes and edges
      - run(task: str) → dict        — entry point called by BOSSman

    Optionally override:
      - _summarise(messages) → str   — custom summarisation logic
        (default: join role:content pairs, no LLM needed for tests)
    """

    def __init__(
        self,
        *,
        name: str,
        role: str,
        permission_tier: PermissionTier = PermissionTier.READ_ONLY,
        event_bus: Any | None = None,     # core.event_bus.EventBus
        registry: Any | None = None,      # core.registry.AgentRegistry
        resource_manager: Any | None = None,  # resources.resource_manager.ResourceManager
        heartbeat_interval: float = float(os.getenv("HEARTBEAT_INTERVAL_SECONDS", "10")),
        compaction_threshold: float = COMPACTION_THRESHOLD,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        # Build AgentInfo (immutable identity)
        self._info = AgentInfo(
            agent_id=new_agent_id(),
            name=name,
            role=role,
            permission_tier=permission_tier,
            metadata=metadata or {},
        )

        # Mutable runtime state
        self._state = AgentState(info=self._info)

        # Infrastructure dependencies (optional — allow None for standalone tests)
        self._event_bus = event_bus
        self._registry = registry
        self._resource_manager = resource_manager  # ResourceManager | None

        # Config
        self._heartbeat_interval = heartbeat_interval
        self._compaction_threshold = compaction_threshold

        # Runtime flags set by HeartbeatAck directives
        self._pause_requested: bool = False
        self._abort_requested: bool = False
        self._compact_requested: bool = False

        # Context tracking — updated by subclass after each LLM call
        self._context_utilisation: float = 0.0
        self._tokens_used: int = 0

        # LangGraph compiled app — built lazily in start()
        self._app: Any | None = None
        self._checkpointer: AsyncSqliteSaver | None = None

        # Heartbeat task handle
        self._heartbeat_task: asyncio.Task | None = None  # type: ignore[type-arg]

        logger.info(
            "BaseAgent: created agent=%s role=%s tier=%s",
            self._info.agent_id, role, permission_tier.value,
        )

    # ── Identity ──────────────────────────────────────────────────────────────

    @property
    def agent_id(self) -> AgentId:
        return self._info.agent_id

    @property
    def info(self) -> AgentInfo:
        return self._info

    @property
    def state(self) -> AgentState:
        return self._state

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def start(self) -> None:
        """
        Initialise the agent:
          1. Open the SQLite checkpoint connection
          2. Compile the LangGraph graph with checkpointing + interrupt hooks
          3. Transition state INITIALIZING → IDLE
          4. Register with BOSSman registry (if available)
          5. Start background heartbeat loop
        """
        # Open async SQLite checkpointer (from_conn_string is an async ctx manager)
        self._checkpointer_cm = AsyncSqliteSaver.from_conn_string(_CHECKPOINT_DB)
        self._checkpointer = await self._checkpointer_cm.__aenter__()

        # Build and compile the graph
        graph = self._build_graph()
        interrupt_before = self._interrupt_nodes()
        self._app = graph.compile(
            checkpointer=self._checkpointer,
            interrupt_before=interrupt_before if interrupt_before else None,
        )

        # Transition to IDLE
        self._state.transition_to(AgentStatus.IDLE)
        logger.info("BaseAgent[%s]: started (tier=%s)", self.agent_id, self._info.permission_tier.value)

        # Register with BOSSman (non-blocking — don't fail if registry unavailable)
        if self._registry is not None:
            try:
                await self._registry.register(self._state)
            except Exception as exc:
                logger.warning("BaseAgent[%s]: registry registration failed: %s", self.agent_id, exc)

        # Publish AGENT_STARTED event
        await self._publish(EventType.AGENT_STARTED, {"tier": self._info.permission_tier.value})

        # Start heartbeat background loop
        self._heartbeat_task = asyncio.create_task(
            self._heartbeat_loop(), name=f"heartbeat-{self.agent_id}"
        )

    async def stop(self, reason: str = "normal") -> None:
        """Graceful shutdown: cancel heartbeat, transition to TERMINATED."""
        if self._heartbeat_task and not self._heartbeat_task.done():
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass

        if self._state.status not in AgentStatus.terminal():
            try:
                self._state.transition_to(AgentStatus.TERMINATED)
            except ValueError:
                self._state.status = AgentStatus.TERMINATED

        await self._publish(EventType.AGENT_TERMINATED, {"reason": reason})

        if self._registry is not None:
            try:
                await self._registry.deregister(self.agent_id)
            except Exception:
                pass

        # Close SQLite checkpointer
        cm = getattr(self, "_checkpointer_cm", None)
        if cm is not None:
            try:
                await cm.__aexit__(None, None, None)
            except Exception:
                pass

        logger.info("BaseAgent[%s]: stopped (reason=%s)", self.agent_id, reason)

    # ── Abstract interface ────────────────────────────────────────────────────

    @abstractmethod
    def _build_graph(self) -> StateGraph:
        """
        Build and return the LangGraph StateGraph for this agent.
        Do NOT call .compile() here — BaseAgent does that in start().
        """
        ...

    @abstractmethod
    async def run(self, task: str, **kwargs: Any) -> dict[str, Any]:
        """
        Run the agent on a task string. Called by BOSSman's TaskManager.
        Must honour self._abort_requested and self._pause_requested.
        """
        ...

    def _interrupt_nodes(self) -> list[str]:
        """
        Return node names that require human-in-the-loop interruption.
        DESTRUCTIVE-tier agents should include destructive action nodes here.
        Override in subclasses.
        """
        if self._info.permission_tier == PermissionTier.DESTRUCTIVE:
            return ["execute_destructive_action"]
        return []

    # ── Permission gate ───────────────────────────────────────────────────────

    def _check_permission(self, required: PermissionTier) -> None:
        """
        Raise PermissionGateError if this agent's tier is below required.

        Usage in subclass nodes:
            self._check_permission(PermissionTier.WRITE)
        """
        tier_order = {
            PermissionTier.READ_ONLY: 0,
            PermissionTier.WRITE: 1,
            PermissionTier.PRIVILEGED: 2,
            PermissionTier.DESTRUCTIVE: 3,
        }
        if tier_order[self._info.permission_tier] < tier_order[required]:
            raise PermissionGateError(self.agent_id, required, self._info.permission_tier)

    # ── Context compaction ────────────────────────────────────────────────────

    async def _maybe_compact(
        self, messages: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        """
        Compact ``messages`` if context utilisation is at/above the threshold
        OR if a compact_context directive was received in the last HeartbeatAck.

        Returns the (possibly compacted) message list.
        Emits CONTEXT_COMPACTED event if compaction occurred.
        """
        trigger = self._compact_requested or (self._context_utilisation >= self._compaction_threshold)
        if not trigger:
            return messages

        self._compact_requested = False  # consume the directive

        result = await compact_messages(
            messages=messages,
            context_utilisation=self._context_utilisation,
            summarise=self._summarise,
            threshold=self._compaction_threshold,
        )

        if result.was_compacted:
            await self._publish(
                EventType.CONTEXT_COMPACTED,
                {
                    "original_count": result.original_count,
                    "compacted_count": result.compacted_count,
                    "utilisation": self._context_utilisation,
                    "summary_snippet": result.summary_snippet,
                },
            )
            logger.info(
                "BaseAgent[%s]: context compacted %d → %d messages",
                self.agent_id,
                result.original_count,
                result.compacted_count,
            )

        return result.compacted_messages

    async def _summarise(self, messages: list[dict[str, Any]]) -> str:
        """
        Default summariser — joins role: content pairs.
        Subclasses SHOULD override this to use the LLM for proper summarisation.
        """
        parts = []
        for m in messages:
            role = m.get("role", "unknown")
            content = m.get("content", "")
            if isinstance(content, list):
                # Handle multi-part content (e.g. tool results)
                content = " ".join(
                    c.get("text", "") if isinstance(c, dict) else str(c)
                    for c in content
                )
            parts.append(f"{role}: {content[:200]}")
        return "\n".join(parts)

    # ── Heartbeat ─────────────────────────────────────────────────────────────

    async def _heartbeat_loop(self) -> None:
        """Background task: emit heartbeat every _heartbeat_interval seconds."""
        while True:
            try:
                await asyncio.sleep(self._heartbeat_interval)
                await self._send_heartbeat()
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.warning("BaseAgent[%s]: heartbeat error: %s", self.agent_id, exc)

    async def _send_heartbeat(self) -> None:
        """Build HeartbeatPayload, send to registry, process HeartbeatAck."""
        payload = HeartbeatPayload(
            agent_id=self.agent_id,
            status=self._state.status,
            current_task_id=self._state.current_task_id,
            current_step=self._state.current_step,
            tokens_used=self._tokens_used,
            context_utilisation=self._context_utilisation,
            self_reported_healthy=self._state.is_healthy,
        )

        ack: HeartbeatAck | None = None
        if self._registry is not None:
            try:
                ack = await self._registry.process_heartbeat(payload)
            except Exception as exc:
                logger.warning("BaseAgent[%s]: heartbeat registry error: %s", self.agent_id, exc)

        # Update local state's last_heartbeat
        self._state.record_heartbeat()

        if ack is None:
            return

        # --- Process directives ---
        if ack.compact_context:
            logger.info("BaseAgent[%s]: received compact_context directive", self.agent_id)
            self._compact_requested = True

        if ack.pause_task and not self._pause_requested:
            logger.info("BaseAgent[%s]: received pause_task directive", self.agent_id)
            self._pause_requested = True
            await self._publish(EventType.AGENT_PAUSED, {"source": "heartbeat_ack"})

        if ack.abort_task and not self._abort_requested:
            logger.info("BaseAgent[%s]: received abort_task directive", self.agent_id)
            self._abort_requested = True
            await self._publish(EventType.TASK_CANCELLED, {"source": "heartbeat_ack"})

        if ack.token_budget_remaining is not None:
            self._state.resource_usage.tokens_budget = ack.token_budget_remaining

    # ── Pause / resume helpers ────────────────────────────────────────────────

    async def _wait_if_paused(self, poll_interval: float = 1.0) -> None:
        """
        Busy-wait while pause is requested.
        Raises asyncio.CancelledError if abort is set while paused.
        """
        if not self._pause_requested:
            return
        logger.info("BaseAgent[%s]: paused — waiting for resume", self.agent_id)
        while self._pause_requested:
            if self._abort_requested:
                raise asyncio.CancelledError("abort_task directive received while paused")
            await asyncio.sleep(poll_interval)
        logger.info("BaseAgent[%s]: resumed", self.agent_id)

    def resume(self) -> None:
        """Externally resume a paused agent."""
        self._pause_requested = False

    # ── Event helpers ─────────────────────────────────────────────────────────

    async def _publish(self, event_type: EventType, payload: dict[str, Any] | None = None) -> None:
        """Publish a BOSSman event. No-ops if event_bus is None."""
        if self._event_bus is None:
            return
        event = BossmanEvent(
            event_type=event_type,
            source_agent_id=self.agent_id,
            payload=payload or {},
        )
        try:
            await self._event_bus.publish(event)
        except Exception as exc:
            logger.warning("BaseAgent[%s]: event publish error: %s", self.agent_id, exc)

    # ── LangGraph thread config ───────────────────────────────────────────────

    @property
    def _thread_config(self) -> dict[str, Any]:
        """LangGraph config dict for this agent's persistent thread."""
        return {"configurable": {"thread_id": self.agent_id}}
