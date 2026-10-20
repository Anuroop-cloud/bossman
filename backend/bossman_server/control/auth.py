"""
control.auth
────────────
Principals and API keys.

Roles
  viewer    read the dashboard API
  agent     use the agent protocol (/v1) inside its namespaces; may only
            register agents up to its key's `max_tier`
  operator  viewer + operate: submit/replay tasks, approve actions, send
            directives, restart managed agents, toggle flags
  admin     everything, including keys, secrets, policies, discarding work

Credentials
  * Bootstrap tokens from the environment (never stored):
      BOSSMAN_ADMIN_TOKEN, BOSSMAN_OPERATOR_TOKEN, BOSSMAN_VIEWER_TOKEN,
      BOSSMAN_AGENT_TOKEN (+ BOSSMAN_AGENT_TOKEN_NAMESPACES, BOSSMAN_AGENT_MAX_TIER)
  * API keys `bm_<key_id>_<secret>` created at runtime; only a SHA-256 hash
    is stored, the full key is shown once.

Tokens are compared in constant time. Keys can expire and be revoked.
"""

from __future__ import annotations

import hashlib
import hmac
import os
import re
import secrets
import time
import uuid
from dataclasses import dataclass
from enum import Enum
from typing import Any

from bossman_server.contracts.agent_state import NAMESPACE_PATTERN, PermissionTier
from bossman_server.contracts.events import BossmanEvent, EventType
from bossman_server.core.documents import DocumentStore
from bossman_server.core.event_bus import EventBus

COLLECTION = "api_keys"
KEY_RE = re.compile(r"^bm_([a-z0-9]{6,32})_([A-Za-z0-9_-]{20,})$")
_NS_RE = re.compile(NAMESPACE_PATTERN)
CACHE_TTL_S = 5.0


class Role(str, Enum):
    VIEWER = "viewer"
    AGENT = "agent"
    OPERATOR = "operator"
    ADMIN = "admin"


@dataclass(frozen=True)
class Principal:
    key_id: str
    name: str
    role: Role
    namespaces: tuple[str, ...] = ("*",)
    max_tier: PermissionTier = PermissionTier.READ_ONLY
    source: str = "key"  # env | key | anonymous

    def in_namespace(self, ns: str) -> bool:
        return "*" in self.namespaces or ns in self.namespaces

    @property
    def default_namespace(self) -> str:
        return next((n for n in self.namespaces if n != "*"), "default")

    @property
    def can_read(self) -> bool:
        return self.role in (Role.VIEWER, Role.OPERATOR, Role.ADMIN)

    @property
    def can_operate(self) -> bool:
        return self.role in (Role.OPERATOR, Role.ADMIN)

    @property
    def is_admin(self) -> bool:
        return self.role == Role.ADMIN

    @property
    def can_use_agent_api(self) -> bool:
        return self.role in (Role.AGENT, Role.ADMIN)

    @property
    def tier(self) -> PermissionTier:
        """Dashboard action tier (kept for the Phase 8 session contract)."""
        if self.role == Role.ADMIN:
            return PermissionTier.PRIVILEGED
        if self.role == Role.OPERATOR:
            return PermissionTier.WRITE
        return PermissionTier.READ_ONLY

    @property
    def actor(self) -> str:
        return f"{self.role.value}:{self.name}"


ANONYMOUS = Principal(key_id="anonymous", name="anonymous", role=Role.VIEWER, source="anonymous")


def _hash(token: str) -> str:
    return hashlib.sha256(token.encode()).hexdigest()


def _env_principals() -> list[tuple[str, Principal]]:
    out: list[tuple[str, Principal]] = []
    admin = os.getenv("BOSSMAN_ADMIN_TOKEN", "")
    operator = os.getenv("BOSSMAN_OPERATOR_TOKEN", "")
    viewer = os.getenv("BOSSMAN_VIEWER_TOKEN", "")
    agent = os.getenv("BOSSMAN_AGENT_TOKEN", "")
    if admin:
        out.append((admin, Principal("env-admin", "admin", Role.ADMIN, ("*",), PermissionTier.DESTRUCTIVE, "env")))
    if operator:
        out.append((operator, Principal("env-operator", "operator", Role.OPERATOR, ("*",),
                                        PermissionTier.PRIVILEGED, "env")))
    if viewer:
        out.append((viewer, Principal("env-viewer", "viewer", Role.VIEWER, ("*",), PermissionTier.READ_ONLY, "env")))
    if agent:
        ns = tuple(n.strip() for n in os.getenv("BOSSMAN_AGENT_TOKEN_NAMESPACES", "*").split(",") if n.strip())
        tier = PermissionTier(os.getenv("BOSSMAN_AGENT_MAX_TIER", "WRITE").upper())
        out.append((agent, Principal("env-agent", "agent", Role.AGENT, ns or ("*",), tier, "env")))
    return out


def env_actions_enabled() -> bool:
    return bool(os.getenv("BOSSMAN_ADMIN_TOKEN") or os.getenv("BOSSMAN_OPERATOR_TOKEN"))


class KeyService:
    def __init__(self, docs: DocumentStore, bus: EventBus) -> None:
        self._docs = docs
        self._bus = bus
        self._cache: dict[str, tuple[dict[str, Any] | None, float]] = {}
        self._privileged_keys = 0
        self._last_used_write: dict[str, float] = {}

    async def load(self) -> None:
        keys = await self._docs.list(COLLECTION, limit=100_000)
        self._privileged_keys = sum(
            1 for k in keys if k["role"] in (Role.OPERATOR.value, Role.ADMIN.value) and not k.get("revoked")
        )

    @property
    def actions_enabled(self) -> bool:
        return env_actions_enabled() or self._privileged_keys > 0

    # ── Create / revoke / list ───────────────────────────────────────────────

    async def create(
        self,
        *,
        name: str,
        role: Role,
        namespaces: list[str] | None = None,
        max_tier: PermissionTier = PermissionTier.WRITE,
        created_by: str = "system",
        ttl_days: float | None = None,
    ) -> tuple[str, dict[str, Any]]:
        namespaces = namespaces or (["*"] if role != Role.AGENT else ["default"])
        for ns in namespaces:
            if ns != "*" and not _NS_RE.match(ns):
                raise ValueError(f"invalid namespace {ns!r}")
        key_id = uuid.uuid4().hex[:12]
        token = f"bm_{key_id}_{secrets.token_urlsafe(32)}"
        record = {
            "key_id": key_id,
            "name": name[:64],
            "role": role.value,
            "namespaces": namespaces,
            "max_tier": max_tier.value,
            "hash": _hash(token),
            "prefix": token[:16],
            "created_by": created_by,
            "created_at": time.time(),
            "expires_at": time.time() + ttl_days * 86400 if ttl_days else None,
            "revoked": False,
            "last_used_at": None,
        }
        await self._docs.put(COLLECTION, key_id, record, status="active")
        if role in (Role.OPERATOR, Role.ADMIN):
            self._privileged_keys += 1
        await self._bus.publish(BossmanEvent.create(
            EventType.KEY_CREATED, message=f"API key {name!r} ({role.value}) created by {created_by}",
            payload={"key_id": key_id, "role": role.value, "namespaces": namespaces, "by": created_by},
        ))
        return token, self.public(record)

    async def revoke(self, key_id: str, *, by: str) -> bool:
        rec = await self._docs.get(COLLECTION, key_id)
        if rec is None or rec.get("revoked"):
            return False
        rec["revoked"] = True
        rec["revoked_by"] = by
        rec["revoked_at"] = time.time()
        await self._docs.put(COLLECTION, key_id, rec, status="revoked")
        self._cache.pop(key_id, None)
        if rec["role"] in (Role.OPERATOR.value, Role.ADMIN.value):
            self._privileged_keys = max(0, self._privileged_keys - 1)
        await self._bus.publish(BossmanEvent.create(
            EventType.KEY_REVOKED, message=f"API key {rec['name']!r} revoked by {by}",
            payload={"key_id": key_id, "by": by},
        ))
        return True

    async def list(self) -> list[dict[str, Any]]:
        return [self.public(r) for r in await self._docs.list(COLLECTION, limit=10_000)]

    @staticmethod
    def public(rec: dict[str, Any]) -> dict[str, Any]:
        return {k: v for k, v in rec.items() if k != "hash" and not k.startswith("_")}

    # ── Authenticate ─────────────────────────────────────────────────────────

    async def authenticate(self, token: str | None) -> Principal | None:
        if not token:
            return None
        token = token.strip()
        for secret, principal in _env_principals():
            if hmac.compare_digest(secret.encode(), token.encode()):
                return principal
        m = KEY_RE.match(token)
        if not m:
            return None
        key_id = m.group(1)
        rec = await self._cached(key_id)
        if rec is None or rec.get("revoked"):
            return None
        if not hmac.compare_digest(rec["hash"], _hash(token)):
            return None
        if rec.get("expires_at") and rec["expires_at"] < time.time():
            return None
        await self._touch(key_id, rec)
        return Principal(
            key_id=key_id,
            name=rec["name"],
            role=Role(rec["role"]),
            namespaces=tuple(rec["namespaces"]),
            max_tier=PermissionTier(rec["max_tier"]),
        )

    async def _cached(self, key_id: str) -> dict[str, Any] | None:
        hit = self._cache.get(key_id)
        if hit and time.monotonic() - hit[1] < CACHE_TTL_S:
            return hit[0]
        rec = await self._docs.get(COLLECTION, key_id)
        self._cache[key_id] = (rec, time.monotonic())
        return rec

    async def _touch(self, key_id: str, rec: dict[str, Any]) -> None:
        now = time.time()
        if now - self._last_used_write.get(key_id, 0) < 60:
            return
        self._last_used_write[key_id] = now
        rec = dict(rec)
        rec["last_used_at"] = now
        try:
            await self._docs.put(COLLECTION, key_id, rec, status="active")
        except Exception:  # noqa: BLE001 - best effort
            pass
