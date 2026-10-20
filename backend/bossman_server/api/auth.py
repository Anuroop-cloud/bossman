"""
api.auth
────────
Request authentication / authorisation for the HTTP API. The UI never
decides permissions — it only *displays* what GET /api/session reports.

Credentials (see control.auth):
    Authorization: Bearer <token>      (preferred; SDKs and CLI)
    X-BOSSman-Token: <token>           (dashboard, Phase 8 compatible)
    ?token=<token>                     (WebSocket only — browsers cannot set headers)

Reads of the dashboard API are public unless BOSSMAN_PUBLIC_READS=false
(the default when BOSSMAN_ENV=production). Every mutation is authorised.
Operator actions are disabled (403) until an operator/admin credential exists.
"""

from __future__ import annotations

import os

from fastapi import HTTPException, Request

from bossman_server.contracts.agent_state import PermissionTier
from bossman_server.control.auth import ANONYMOUS, Principal, env_actions_enabled

TOKEN_HEADER = "X-BOSSman-Token"


def public_reads() -> bool:
    raw = os.getenv("BOSSMAN_PUBLIC_READS")
    if raw is None:
        return os.getenv("BOSSMAN_ENV", "development") != "production"
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def token_from(request: Request, allow_query: bool = False) -> str | None:
    auth = request.headers.get("authorization", "")
    if auth.lower().startswith("bearer "):
        return auth[7:].strip() or None
    tok = request.headers.get(TOKEN_HEADER.lower())
    if tok:
        return tok.strip()
    if allow_query:
        return request.query_params.get("token")
    return None


async def resolve(request: Request, allow_query: bool = False) -> tuple[Principal | None, bool]:
    """(principal, token_was_presented)."""
    token = token_from(request, allow_query)
    if not token:
        return None, False
    return await request.app.state.rt.keys.authenticate(token), True


def actions_enabled(request: Request | None = None) -> bool:
    if request is not None:
        return request.app.state.rt.keys.actions_enabled
    return env_actions_enabled()


# ── Dependencies ─────────────────────────────────────────────────────────────


async def optional_principal(request: Request) -> Principal | None:
    return (await resolve(request))[0]


async def viewer(request: Request) -> Principal:
    p, presented = await resolve(request)
    if p is not None:
        if not p.can_read:
            raise HTTPException(403, "This credential cannot read the dashboard API.")
        return p
    if presented:
        raise HTTPException(401, "Invalid or revoked token.")
    if public_reads():
        return ANONYMOUS
    raise HTTPException(401, "Authentication required.")


async def agent_principal(request: Request) -> Principal:
    p, presented = await resolve(request)
    if p is None:
        raise HTTPException(401, "Invalid or revoked token." if presented else "Agent API key required.")
    if not p.can_use_agent_api:
        raise HTTPException(403, f"A {p.role.value} credential cannot use the agent API.")
    return p


async def _privileged(request: Request, need_admin: bool) -> Principal:
    if not actions_enabled(request):
        raise HTTPException(403, "Operator actions are disabled on this server.")
    p, presented = await resolve(request)
    if p is None:
        raise HTTPException(401, "Invalid or revoked token." if presented else "Operator token required.")
    if not p.can_operate:
        raise HTTPException(403, f"Permission denied: a {p.role.value} credential cannot do this.")
    if need_admin and not p.is_admin:
        raise HTTPException(403, "Permission denied: requires PRIVILEGED tier.")
    return p


async def operator(request: Request) -> Principal:
    return await _privileged(request, need_admin=False)


async def admin(request: Request) -> Principal:
    return await _privileged(request, need_admin=True)


async def caller_tier(request: Request) -> PermissionTier:
    p = await optional_principal(request)
    return p.tier if p else PermissionTier.READ_ONLY
