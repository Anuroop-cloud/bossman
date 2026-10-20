"""
control.flags
─────────────
Feature flags / kill switches (zylos.md §Part4 — "Feature Flags for
Graceful Degradation").

A flag rule disables (or re-enables) a capability for a scope:
    global  ⊂  namespace  ⊂  role  ⊂  agent      (most specific wins)
with an optional rollout percentage for gradual restore.

Conventions:  tool.<name>   action.<name>   model.<name>   any custom key
Unknown keys default to enabled — flags are kill switches, not allow-lists.

Agents receive their evaluated flag map on heartbeat whenever `version`
changes; the policy engine refuses actions whose flag is off.
"""

from __future__ import annotations

import hashlib
import re
import time
from typing import Any

from bossman_server.contracts.agent_state import AgentState
from bossman_server.contracts.events import BossmanEvent, EventType
from bossman_server.core.documents import DocumentStore
from bossman_server.core.event_bus import EventBus

COLLECTION = "flags"
KEY_RE = re.compile(r"^[a-z0-9][a-z0-9._:-]{0,127}$")


def _rule_id(key: str, namespace: str | None, role: str | None, agent_id: str | None) -> str:
    return f"{key}|{namespace or '*'}|{role or '*'}|{agent_id or '*'}"


def _specificity(rule: dict[str, Any]) -> int:
    return (8 if rule.get("agent_id") else 0) + (4 if rule.get("role") else 0) + (2 if rule.get("namespace") else 0)


def _bucket(agent_id: str, key: str) -> int:
    return int(hashlib.sha256(f"{agent_id}:{key}".encode()).hexdigest()[:8], 16) % 100


class FlagService:
    def __init__(self, docs: DocumentStore, bus: EventBus) -> None:
        self._docs = docs
        self._bus = bus
        self._rules: list[dict[str, Any]] = []
        self.version = 0
        bus.subscribe(self._on_event, EventType.FLAG_CHANGED)

    async def load(self) -> None:
        self._rules = [{k: v for k, v in r.items() if not k.startswith("_")}
                       for r in await self._docs.list(COLLECTION, limit=100_000)]
        # Content hash: identical on every node that sees the same rules.
        sig = "|".join(sorted(f"{r['rule_id']}:{r['enabled']}:{r.get('rollout_percent', 100)}:{r['updated_at']}"
                              for r in self._rules))
        self.version = int(hashlib.sha256(sig.encode()).hexdigest()[:7], 16) if self._rules else 0

    async def _on_event(self, event: BossmanEvent) -> None:
        # Another node changed a flag: reload so every node serves the same map.
        if event.payload.get("version") != self.version:
            await self.load()

    async def set(self, key: str, enabled: bool, *, namespace: str | None = None, role: str | None = None,
                  agent_id: str | None = None, rollout_percent: int = 100, note: str = "",
                  by: str = "system") -> dict[str, Any]:
        if not KEY_RE.match(key):
            raise ValueError(f"invalid flag key {key!r}")
        if not 0 <= rollout_percent <= 100:
            raise ValueError("rollout_percent must be 0..100")
        rule = {
            "rule_id": _rule_id(key, namespace, role, agent_id), "key": key, "enabled": bool(enabled),
            "namespace": namespace, "role": role, "agent_id": agent_id, "rollout_percent": rollout_percent,
            "note": note[:300], "updated_by": by, "updated_at": time.time(),
        }
        await self._docs.put(COLLECTION, rule["rule_id"], rule, namespace=namespace,
                             status="on" if enabled else "off")
        await self._changed(f"Flag {key} → {'ON' if enabled else 'OFF'} ({rule['rule_id']}) by {by}", rule)
        return rule

    async def delete(self, rule_id: str, *, by: str = "system") -> bool:
        ok = await self._docs.delete(COLLECTION, rule_id)
        if ok:
            await self._changed(f"Flag rule {rule_id} removed by {by}", {"rule_id": rule_id})
        return ok

    async def _changed(self, message: str, payload: dict[str, Any]) -> None:
        await self.load()
        await self._bus.publish(BossmanEvent.create(
            EventType.FLAG_CHANGED, message=message, payload={**payload, "version": self.version},
        ))

    def list(self) -> list[dict[str, Any]]:
        return sorted(self._rules, key=lambda r: (r["key"], _specificity(r)))

    def evaluate(self, key: str, *, agent_id: str | None = None, namespace: str | None = None,
                 role: str | None = None) -> bool:
        candidates = [
            r for r in self._rules
            if r["key"] == key
            and (r.get("agent_id") in (None, agent_id))
            and (r.get("role") in (None, role))
            and (r.get("namespace") in (None, namespace))
        ]
        for rule in sorted(candidates, key=_specificity, reverse=True):
            if rule.get("rollout_percent", 100) >= 100 or (
                agent_id is not None and _bucket(agent_id, key) < rule["rollout_percent"]
            ):
                return bool(rule["enabled"])
        return True

    def evaluate_for(self, key: str, state: AgentState | None) -> bool:
        if state is None:
            return self.evaluate(key)
        return self.evaluate(key, agent_id=state.agent_id, namespace=state.info.namespace, role=state.info.role)

    async def snapshot_for(self, state: AgentState) -> dict[str, bool]:
        return {key: self.evaluate_for(key, state) for key in sorted({r["key"] for r in self._rules})}
