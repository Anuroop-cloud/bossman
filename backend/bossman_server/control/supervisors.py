"""
control.supervisors
───────────────────
Erlang/OTP-style supervision trees (zylos.md §Part2).

A supervisor groups agents (AgentInfo.supervisor = name), ordered by
`start_order`. When a child fails:

  one_for_one   restart only that child
  one_for_all   restart every child of the group
  rest_for_one  restart the child and every child started after it

Each supervisor has a budget (max_restarts within window_s). When it is
exhausted the failure propagates to the parent supervisor, which restarts
the *whole* failed group under its own strategy and budget. A root
supervisor that runs out of budget escalates to a human.
"""

from __future__ import annotations

import re
import time
from collections import defaultdict, deque
from dataclasses import asdict, dataclass, field
from typing import Any

from bossman_server.contracts.events import BossmanEvent, EventType
from bossman_server.core.documents import DocumentStore
from bossman_server.core.event_bus import EventBus

COLLECTION = "supervisors"
STRATEGIES = {"one_for_one", "one_for_all", "rest_for_one"}
NAME_RE = re.compile(r"^[a-z0-9][a-z0-9._-]{0,63}$")


@dataclass
class SupervisorSpec:
    name: str
    strategy: str = "one_for_one"
    max_restarts: int = 3
    window_s: float = 60.0
    parent: str | None = None
    description: str = ""

    def validate(self) -> None:
        if not NAME_RE.match(self.name):
            raise ValueError(f"invalid supervisor name {self.name!r}")
        if self.strategy not in STRATEGIES:
            raise ValueError(f"strategy must be one of {sorted(STRATEGIES)}")
        if self.max_restarts < 0 or self.window_s <= 0:
            raise ValueError("max_restarts must be ≥ 0 and window_s > 0")
        if self.parent == self.name:
            raise ValueError("a supervisor cannot be its own parent")


@dataclass
class RestartPlan:
    failed: str
    restart: list[str] = field(default_factory=list)
    path: list[str] = field(default_factory=list)   # supervisors consulted, child → root
    escalate: bool = False
    reason: str = ""

    def describe(self) -> str:
        if self.escalate:
            return f"Supervisor budget exhausted along {' → '.join(self.path)}: escalating to human"
        return (f"Supervisor {self.path[-1]!r} restarting {len(self.restart)} agent(s) "
                f"[{', '.join(self.restart)}] ({self.reason})")


class SupervisorTree:
    def __init__(self, docs: DocumentStore, bus: EventBus, registry: Any) -> None:
        self._docs = docs
        self._bus = bus
        self._registry = registry
        self._specs: dict[str, SupervisorSpec] = {}
        self._history: dict[str, deque[float]] = defaultdict(deque)

    async def load(self) -> None:
        self._specs = {}
        for d in await self._docs.list(COLLECTION, limit=10_000):
            spec = SupervisorSpec(**{k: v for k, v in d.items() if not k.startswith("_")})
            self._specs[spec.name] = spec

    async def put(self, spec: SupervisorSpec) -> SupervisorSpec:
        spec.validate()
        # reject cycles in the parent chain
        seen, p = {spec.name}, spec.parent
        while p:
            if p in seen:
                raise ValueError(f"supervisor parent cycle via {p!r}")
            seen.add(p)
            p = self._specs[p].parent if p in self._specs else None
        self._specs[spec.name] = spec
        await self._docs.put(COLLECTION, spec.name, asdict(spec))
        return spec

    async def delete(self, name: str) -> bool:
        self._specs.pop(name, None)
        return await self._docs.delete(COLLECTION, name)

    def spec(self, name: str) -> SupervisorSpec:
        # Agents may name a supervisor nobody configured: default one_for_one.
        return self._specs.get(name) or SupervisorSpec(name=name)

    def list(self) -> list[dict[str, Any]]:
        return [asdict(s) for s in sorted(self._specs.values(), key=lambda s: s.name)]

    def _budget_ok(self, name: str) -> bool:
        spec = self.spec(name)
        now = time.monotonic()
        h = self._history[name]
        while h and now - h[0] > spec.window_s:
            h.popleft()
        if len(h) >= spec.max_restarts:
            return False
        h.append(now)
        return True

    async def members(self, name: str) -> list[str]:
        """Agents supervised by `name`, including those of child supervisors (in order)."""
        out = [a.agent_id for a in await self._registry.by_supervisor(name)]
        for child in [s.name for s in self._specs.values() if s.parent == name]:
            out.extend(await self.members(child))
        return out

    async def on_child_failure(self, agent_id: str, reason: str) -> RestartPlan:
        state = await self._registry.get(agent_id)
        plan = RestartPlan(failed=agent_id, reason=reason[:200])
        if state is None or not state.info.supervisor:
            plan.restart = [agent_id]
            return plan

        group = state.info.supervisor
        failed_unit: list[str] = [agent_id]  # what the current level must restart
        while True:
            plan.path.append(group)
            spec = self.spec(group)
            if self._budget_ok(group):
                siblings = [a.agent_id for a in await self._registry.by_supervisor(group)]
                if spec.strategy == "one_for_all":
                    plan.restart = await self.members(group)
                elif spec.strategy == "rest_for_one" and failed_unit[0] in siblings:
                    plan.restart = siblings[siblings.index(failed_unit[0]):]
                else:
                    plan.restart = failed_unit
                plan.reason = f"{spec.strategy}: {reason[:160]}"
                await self._bus.publish(BossmanEvent.create(
                    EventType.SUPERVISOR_RESTART, agent_id=agent_id,
                    message=f"Supervisor {group!r} ({spec.strategy}) restarting {plan.restart}",
                    payload={"supervisor": group, "strategy": spec.strategy, "restart": plan.restart,
                             "path": plan.path},
                ))
                return plan
            await self._bus.publish(BossmanEvent.create(
                EventType.SUPERVISOR_ESCALATED, agent_id=agent_id,
                message=f"Supervisor {group!r} exceeded {spec.max_restarts} restarts in {spec.window_s:.0f}s",
                payload={"supervisor": group, "parent": spec.parent, "path": plan.path},
            ))
            if not spec.parent:
                plan.escalate = True
                return plan
            # The whole failed group becomes the failing "child" of the parent.
            failed_unit = await self.members(group)
            group = spec.parent

    def status(self) -> list[dict[str, Any]]:
        now = time.monotonic()
        out = []
        for name in sorted(set(self._specs) | set(self._history)):
            spec = self.spec(name)
            recent = [t for t in self._history.get(name, ()) if now - t <= spec.window_s]
            out.append({**asdict(spec), "restarts_in_window": len(recent),
                        "budget_remaining": max(0, spec.max_restarts - len(recent))})
        return out
