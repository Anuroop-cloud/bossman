"""
api.auth
────────
Server-side authorisation for mutating endpoints. The UI never decides
permissions — it only *displays* what GET /api/session reports.

Tokens live in server environment variables only:

    BOSSMAN_OPERATOR_TOKEN → PermissionTier.WRITE      (may replay DLQ tasks)
    BOSSMAN_ADMIN_TOKEN    → PermissionTier.PRIVILEGED (may also discard tasks)

If neither is configured every mutating endpoint is disabled (403).
The token is sent in the `X-BOSSman-Token` header and compared in constant time.
"""

from __future__ import annotations

import hmac
import os

from fastapi import Header, HTTPException

from contracts.agent_state import PermissionTier

TOKEN_HEADER = "X-BOSSman-Token"


def _configured() -> list[tuple[str, PermissionTier]]:
    pairs: list[tuple[str, PermissionTier]] = []
    admin = os.getenv("BOSSMAN_ADMIN_TOKEN", "")
    operator = os.getenv("BOSSMAN_OPERATOR_TOKEN", "")
    if admin:
        pairs.append((admin, PermissionTier.PRIVILEGED))
    if operator:
        pairs.append((operator, PermissionTier.WRITE))
    return pairs


def actions_enabled() -> bool:
    return bool(_configured())


def resolve_tier(token: str | None) -> PermissionTier:
    """Map a presented token to a tier. No/invalid token → READ_ONLY."""
    if not token:
        return PermissionTier.READ_ONLY
    for secret, tier in _configured():
        if hmac.compare_digest(secret.encode(), token.encode()):
            return tier
    return PermissionTier.READ_ONLY


def caller_tier(x_bossman_token: str | None = Header(default=None)) -> PermissionTier:
    return resolve_tier(x_bossman_token)


def require_write(x_bossman_token: str | None = Header(default=None)) -> PermissionTier:
    return _require(x_bossman_token, lambda t: t.can_write(), "WRITE")


def require_privileged(x_bossman_token: str | None = Header(default=None)) -> PermissionTier:
    return _require(x_bossman_token, lambda t: t.can_destroy(), "PRIVILEGED")


def _require(token: str | None, ok, needed: str) -> PermissionTier:
    if not actions_enabled():
        raise HTTPException(status_code=403, detail="Operator actions are disabled on this server.")
    tier = resolve_tier(token)
    if tier == PermissionTier.READ_ONLY:
        raise HTTPException(status_code=401, detail="Operator token required.")
    if not ok(tier):
        raise HTTPException(
            status_code=403, detail=f"Permission denied: requires {needed} tier."
        )
    return tier
