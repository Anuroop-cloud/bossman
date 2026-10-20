"""
control.agents
──────────────
AgentService — how a remote agent joins and leaves the fleet.

Registration enforces, at the infrastructure layer:
  * namespace isolation   — a key only registers agents in its namespaces
  * tier ceiling          — an agent can never claim more than its key's max_tier
                            (no agent self-promotes to DESTRUCTIVE)
  * idempotent spawning   — one live agent per singleton_key (zylos.md §Part2:
                            "never spawn without checking"); a second process
                            is refused unless it explicitly takes over
  * spawn caps            — at most N live agents per (namespace, role)
  * fencing               — takeover bumps the epoch; the old process is
                            rejected on its next call
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Any

from bossman_server.contracts.agent_state import AgentInfo, AgentState, AgentStatus, PermissionTier
from bossman_server.contracts.events import BossmanEvent, EventType
from bossman_server.core.documents import DocumentStore

ROLE_LIMITS = "role_limits"


class RegistrationError(Exception):
    status = 400
    code = "invalid_request"


class DuplicateAgent(RegistrationError):
    status = 409
    code = "duplicate_agent"

    def __init__(self, existing: AgentState) -> None:
        super().__init__(
            f"An agent with singleton_key {existing.info.singleton_key!r} is already live "
            f"({existing.agent_id}, heartbeat {existing.heartbeat_age_seconds:.0f}s ago). "
            "Pass takeover=true to replace it."
        )
        self.existing = existing


class SpawnLimit(RegistrationError):
    status = 429
    code = "spawn_limit"


class TierCeiling(RegistrationError):
    status = 403
    code = "forbidden"


@dataclass
class RegisterRequest:
    name: str
    role: str
    namespace: str | None = None
    permission_tier: PermissionTier = PermissionTier.READ_ONLY
    metadata: dict[str, Any] = field(default_factory=dict)
    capabilities: list[str] = field(default_factory=list)
    labels: dict[str, str] = field(default_factory=dict)
    singleton_key: str | None = None
    takeover: bool = False
    supervisor: str | None = None
    start_order: int = 0
    managed_id: str | None = None
    sdk: str | None = None
    tokens_budget: int | None = None


class RoleLimits:
    """Expected maximum concurrency per (namespace, role)."""

    def __init__(self, docs: DocumentStore) -> None:
        self._docs = docs
        self._limits: dict[str, int] = {}

    async def load(self) -> None:
        self._limits = {}
        for item in os.getenv("BOSSMAN_ROLE_LIMITS", "").split(","):
            if "=" in item:
                k, v = item.split("=", 1)
                key = k.strip() if "/" in k else f"*/{k.strip()}"
                self._limits[key] = int(v)
        for d in await self._docs.list(ROLE_LIMITS, limit=10_000):
            self._limits[d["key"]] = int(d["max_instances"])

    async def set(self, namespace: str, role: str, max_instances: int | None) -> None:
        key = f"{namespace}/{role}"
        if max_instances is None:
            await self._docs.delete(ROLE_LIMITS, key)
            self._limits.pop(key, None)
        else:
            await self._docs.put(ROLE_LIMITS, key, {"key": key, "max_instances": int(max_instances)})
            self._limits[key] = int(max_instances)

    def get(self, namespace: str, role: str) -> int | None:
        return self._limits.get(f"{namespace}/{role}", self._limits.get(f"*/{role}"))

    def all(self) -> dict[str, int]:
        return dict(self._limits)


class AgentService:
    def __init__(self, rt: Any) -> None:
        self.rt = rt

    def _stale(self, a: AgentState) -> bool:
        return (
            not a.is_alive
            or a.status in (AgentStatus.STUCK, AgentStatus.FAILED)
            or a.heartbeat_age_seconds > self.rt.heartbeat_timeout_s
        )

    async def register(self, principal: Any, req: RegisterRequest) -> tuple[AgentState, bool]:
        """Returns (state, took_over)."""
        rt = self.rt
        ns = req.namespace or principal.default_namespace
        if not principal.in_namespace(ns):
            raise TierCeiling(f"key may not register agents in namespace {ns!r}")
        if not principal.max_tier.allows(req.permission_tier):
            raise TierCeiling(
                f"requested tier {req.permission_tier.value} exceeds this key's ceiling {principal.max_tier.value}"
            )
        info = AgentInfo(
            name=req.name, role=req.role, namespace=ns, permission_tier=req.permission_tier,
            metadata=dict(req.metadata), capabilities=list(req.capabilities), labels=dict(req.labels),
            singleton_key=req.singleton_key, supervisor=req.supervisor, start_order=req.start_order,
            managed_id=req.managed_id, transport="remote", sdk=req.sdk,
        )

        # Same logical agent coming back (managed restart or singleton) → takeover.
        existing: AgentState | None = None
        if req.managed_id:
            existing = next((a for a in await rt.registry.find_managed(req.managed_id)), None)
        if existing is None and req.singleton_key:
            existing = await rt.registry.find_singleton(ns, req.singleton_key)
            if existing is not None and not (req.takeover or self._stale(existing)):
                await rt.bus.publish(BossmanEvent.create(
                    EventType.DUPLICATE_SPAWN_BLOCKED, agent_id=existing.agent_id, namespace=ns,
                    message=f"Duplicate registration of {req.singleton_key!r} refused (live: {existing.agent_id})",
                    payload={"singleton_key": req.singleton_key, "existing": existing.agent_id,
                             "attempted_by": principal.actor}))
                raise DuplicateAgent(existing)
        if existing is not None:
            await rt.locks.revoke_agent(existing.agent_id, reason="agent taken over")
            await rt.tasks.release_agent_tasks(existing.agent_id, "agent process replaced (takeover)")
            state = await rt.registry.takeover(existing.agent_id, info)
            await rt.registry.mark_ready(state.agent_id)
            return state, True

        cap = rt.role_limits.get(ns, req.role)
        if cap is not None:
            live = [a for a in await rt.registry.by_role(req.role, ns) if a.is_alive]
            if len(live) >= cap:
                await rt.bus.publish(BossmanEvent.create(
                    EventType.DUPLICATE_SPAWN_BLOCKED, namespace=ns,
                    message=f"Spawn of '{req.role}' refused: {len(live)} live (cap {cap})",
                    payload={"role": req.role, "live": len(live), "cap": cap}))
                raise SpawnLimit(f"role {req.role!r} in {ns!r} is at its cap of {cap} live agents")

        state = await rt.registry.register(info)
        if req.tokens_budget:
            state.resource_usage.tokens_budget = req.tokens_budget
        await rt.registry.mark_ready(state.agent_id)
        return state, False

    async def deregister(self, agent_id: str, reason: str = "deregistered") -> None:
        rt = self.rt
        await rt.locks.revoke_agent(agent_id, reason=f"agent left: {reason}")
        await rt.tasks.release_agent_tasks(agent_id, f"agent left: {reason}")
        await rt.registry.deregister(agent_id, reason)
