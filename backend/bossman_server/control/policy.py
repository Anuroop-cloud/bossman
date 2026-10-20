"""
control.policy
──────────────
PolicyEngine — infrastructure-enforced permission boundaries.

zylos.md §Part9: "Trust Infrastructure Over Prompts for Safety … Prompt
constraints are advisory; permission boundaries are mandatory." A remote
agent's own tier check runs in a process BOSSman does not control, so the
binding decision is made here: agents ask before acting, and the credentials
needed to act (control.secrets) are only released — or only *used*, via the
tool gateway — after an allow or a human approval.
"""

from __future__ import annotations

import copy
import fnmatch
import json
import re
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from bossman_server.contracts.agent_state import AgentState, PermissionTier
from bossman_server.contracts.events import BossmanEvent, EventType
from bossman_server.core.documents import DocumentStore
from bossman_server.core.event_bus import EventBus

COLLECTION = "policies"
DEFAULT_POLICY_PATH = Path(__file__).resolve().parent.parent / "policies" / "default.yaml"
DECISIONS = {"allow", "deny", "require_approval"}


class PolicyError(ValueError):
    pass


@dataclass
class ActionRequest:
    action: str
    tool: str | None = None
    resource: str | None = None
    irreversible: bool | None = None
    args: Any = None
    justification: str | None = None
    labels: dict[str, str] = field(default_factory=dict)
    credentials: list[str] = field(default_factory=list)
    task_id: str | None = None

    def args_text(self) -> str:
        if self.args is None:
            return ""
        return self.args if isinstance(self.args, str) else json.dumps(self.args, default=str)[:20_000]

    def to_dict(self) -> dict[str, Any]:
        return {
            "action": self.action, "tool": self.tool, "resource": self.resource,
            "irreversible": self.irreversible, "args": self.args_text()[:2000],
            "justification": self.justification, "labels": self.labels,
            "credentials": self.credentials, "task_id": self.task_id,
        }


@dataclass
class PolicyDecision:
    decision: str
    reason: str
    rule: str | None = None
    required_tier: str | None = None
    approvals_required: int = 0
    irreversible: bool = False
    decision_id: str = field(default_factory=lambda: f"pd-{uuid.uuid4().hex[:12]}")
    at: float = field(default_factory=time.time)

    def to_dict(self) -> dict[str, Any]:
        return {
            "decision_id": self.decision_id, "decision": self.decision, "reason": self.reason,
            "rule": self.rule, "required_tier": self.required_tier,
            "approvals_required": self.approvals_required, "irreversible": self.irreversible, "at": self.at,
        }


def load_default_policy() -> dict[str, Any]:
    return yaml.safe_load(DEFAULT_POLICY_PATH.read_text(encoding="utf-8"))


def validate_policy(doc: Any) -> dict[str, Any]:
    if not isinstance(doc, dict):
        raise PolicyError("policy must be a mapping")
    doc = copy.deepcopy(doc)
    doc.setdefault("version", 1)
    settings = doc.setdefault("settings", {})
    if not isinstance(settings, dict):
        raise PolicyError("settings must be a mapping")
    classes = doc.setdefault("action_classes", {})
    for action, tier in classes.items():
        try:
            classes[action] = PermissionTier(str(tier).upper()).value
        except ValueError:
            raise PolicyError(f"action_classes.{action}: unknown tier {tier!r}") from None
    if "unknown_action_tier" in settings:
        settings["unknown_action_tier"] = PermissionTier(str(settings["unknown_action_tier"]).upper()).value
    if not isinstance(doc.setdefault("irreversible_actions", []), list):
        raise PolicyError("irreversible_actions must be a list")
    rules = doc.setdefault("rules", [])
    if not isinstance(rules, list):
        raise PolicyError("rules must be a list")
    for i, rule in enumerate(rules):
        if not isinstance(rule, dict) or "name" not in rule:
            raise PolicyError(f"rules[{i}] needs a name")
        if rule.get("decision") not in DECISIONS:
            raise PolicyError(f"rules[{i}].decision must be one of {sorted(DECISIONS)}")
        match = rule.setdefault("match", {})
        if not isinstance(match, dict):
            raise PolicyError(f"rules[{i}].match must be a mapping")
        if "args_regex" in match:
            try:
                re.compile(match["args_regex"])
            except re.error as exc:
                raise PolicyError(f"rules[{i}].match.args_regex: {exc}") from None
    return doc


class PolicyEngine:
    def __init__(self, docs: DocumentStore, bus: EventBus, flags: Any | None = None) -> None:
        self._docs = docs
        self._bus = bus
        self._flags = flags
        self.policy: dict[str, Any] = validate_policy(load_default_policy())
        self.revision = 0
        self.stats = {"allow": 0, "deny": 0, "require_approval": 0}
        bus.subscribe(self._on_event, EventType.POLICY_UPDATED)

    async def load(self) -> None:
        stored = await self._docs.get(COLLECTION, "active")
        if stored:
            self.policy = validate_policy(stored["policy"])
            self.revision = stored.get("revision", 0)

    async def _on_event(self, event: BossmanEvent) -> None:
        if event.payload.get("revision") != self.revision:
            await self.load()

    async def apply(self, policy: dict[str, Any], *, by: str) -> int:
        validated = validate_policy(policy)
        revision = self.revision + 1
        await self._docs.put(COLLECTION, f"rev-{revision}", {"policy": validated, "revision": revision, "by": by})
        await self._docs.put(COLLECTION, "active", {"policy": validated, "revision": revision, "by": by,
                                                     "applied_at": time.time()})
        self.policy, self.revision = validated, revision
        await self._bus.publish(BossmanEvent.create(
            EventType.POLICY_UPDATED, message=f"Policy revision {revision} applied by {by}",
            payload={"revision": revision, "by": by, "rules": len(validated["rules"])},
        ))
        return revision

    @property
    def settings(self) -> dict[str, Any]:
        return self.policy.get("settings", {})

    def required_tier(self, action: str) -> PermissionTier:
        classes = self.policy.get("action_classes", {})
        tier = classes.get(action)
        if tier is None:
            # "deploy_service" → class "deploy"
            tier = next((t for a, t in classes.items() if action.startswith(a + "_") or action.startswith(a + ".")),
                        None)
        return PermissionTier(tier or self.settings.get("unknown_action_tier", "WRITE"))

    def is_irreversible(self, req: ActionRequest) -> bool:
        if req.irreversible is not None:
            return bool(req.irreversible)
        listed = self.policy.get("irreversible_actions", [])
        return req.action in listed or any(req.action.startswith(a + "_") for a in listed)

    # ── Evaluation ───────────────────────────────────────────────────────────

    def evaluate(self, agent: AgentState, req: ActionRequest) -> PolicyDecision:
        required = self.required_tier(req.action)
        irreversible = self.is_irreversible(req)
        s = self.settings

        def done(decision: str, reason: str, rule: str | None = None, approvals: int = 0) -> PolicyDecision:
            self.stats[decision] += 1
            return PolicyDecision(decision, reason, rule, required.value, approvals, irreversible)

        # 1. kill switches
        if self._flags is not None:
            if not self._flags.evaluate_for(f"action.{req.action}", agent):
                return done("deny", f"action {req.action!r} is disabled by a kill switch", "flag")
            if req.tool and not self._flags.evaluate_for(f"tool.{req.tool}", agent):
                return done("deny", f"tool {req.tool!r} is disabled by a kill switch", "flag")

        # 2. least privilege: declared capabilities
        caps = agent.info.capabilities
        if s.get("enforce_capabilities", True) and caps and req.tool and not any(
            fnmatch.fnmatchcase(req.tool, c) for c in caps
        ):
            return done("deny", f"tool {req.tool!r} is not among the agent's declared capabilities {caps}",
                        "capabilities")

        # 3. tier
        tier = agent.info.permission_tier
        if not tier.allows(required):
            return done("deny", f"action {req.action!r} requires {required.value}; agent holds {tier.value}",
                        "tier")

        # 4. explicit rules
        for rule in self.policy.get("rules", []):
            if self._matches(rule.get("match", {}), agent, req, irreversible):
                approvals = int(rule.get("approvals_required", s.get("default_approvals_required", 1)))
                return done(rule["decision"], rule.get("reason") or f"matched rule {rule['name']!r}",
                            rule["name"], approvals if rule["decision"] == "require_approval" else 0)

        # 5. irreversible
        if irreversible:
            if s.get("require_justification_for_irreversible", True) and not (req.justification or "").strip():
                return done("deny", "irreversible actions require a justification", "justification")
            if s.get("irreversible_requires_approval", True):
                approvals = int(s.get("destructive_approvals_required", 2)
                                if required == PermissionTier.DESTRUCTIVE
                                else s.get("default_approvals_required", 1))
                return done("require_approval", f"{req.action!r} is irreversible", "irreversible", approvals)

        # 6. destructive is always a human gate
        if required == PermissionTier.DESTRUCTIVE:
            return done("require_approval", "DESTRUCTIVE actions always need a human",
                        "destructive", int(s.get("destructive_approvals_required", 2)))

        return done("allow", "within policy")

    @staticmethod
    def _matches(m: dict[str, Any], agent: AgentState, req: ActionRequest, irreversible: bool) -> bool:
        def glob(value: str | None, pattern: str) -> bool:
            return value is not None and fnmatch.fnmatchcase(value, pattern)

        if "action" in m and not glob(req.action, m["action"]):
            return False
        if "action_in" in m and req.action not in m["action_in"]:
            return False
        if "tool" in m and not glob(req.tool, m["tool"]):
            return False
        if "resource" in m and not glob(req.resource, m["resource"]):
            return False
        if "namespace" in m and agent.info.namespace != m["namespace"]:
            return False
        if "role" in m and not glob(agent.info.role, m["role"]):
            return False
        if "agent" in m and not glob(agent.info.name, m["agent"]):
            return False
        if "irreversible" in m and bool(m["irreversible"]) != irreversible:
            return False
        if "labels" in m:
            merged = {**agent.info.labels, **req.labels}
            if any(merged.get(k) != str(v) for k, v in m["labels"].items()):
                return False
        if "args_contains" in m and m["args_contains"] not in req.args_text():
            return False
        if "args_regex" in m and not re.search(m["args_regex"], req.args_text()):
            return False
        return True
