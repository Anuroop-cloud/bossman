"""
control.secrets
───────────────
Credential broker + tool gateway.

Production credentials do not live in the agent process. They are stored
here encrypted (Fernet, key from BOSSMAN_SECRET_KEY) and used only after a
policy allow or a human approval:

  release="proxy"   (default) the value never leaves BOSSman — the agent
                    sends the HTTP request to the gateway, which injects the
                    credential and forwards it to an allow-listed host.
  release="direct"  the value is returned in the authorize response for
                    tools that cannot be proxied (still policy-gated, logged).

This is the "permission boundary, not a prompt constraint" from zylos.md:
an agent that was never authorised never holds the key.
"""

from __future__ import annotations

import base64
import hashlib
import logging
import os
import re
import time
from typing import Any
from urllib.parse import urlparse

import httpx

from bossman_server.contracts.agent_state import AgentState, PermissionTier
from bossman_server.contracts.events import BossmanEvent, EventType
from bossman_server.core.documents import DocumentStore
from bossman_server.core.event_bus import EventBus

log = logging.getLogger(__name__)

COLLECTION = "secrets"
NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")
MAX_PROXY_BODY = 1_048_576
HOP_HEADERS = {"host", "content-length", "connection", "transfer-encoding", "authorization", "cookie"}


class SecretError(Exception):
    code = "forbidden"


def _fernet():  # noqa: ANN202
    from cryptography.fernet import Fernet

    raw = os.getenv("BOSSMAN_SECRET_KEY", "")
    if raw:
        try:
            return Fernet(raw.encode())
        except ValueError:
            # Accept any passphrase: derive a Fernet key from it.
            return Fernet(base64.urlsafe_b64encode(hashlib.sha256(raw.encode()).digest()))
    if not hasattr(_fernet, "_ephemeral"):
        log.warning("BOSSMAN_SECRET_KEY not set — using an ephemeral key; stored secrets will not "
                    "survive a restart. Set BOSSMAN_SECRET_KEY in production.")
        _fernet._ephemeral = Fernet(Fernet.generate_key())  # type: ignore[attr-defined]
    return _fernet._ephemeral  # type: ignore[attr-defined]


class SecretService:
    def __init__(self, docs: DocumentStore, bus: EventBus, audit: Any) -> None:
        self._docs = docs
        self._bus = bus
        self._audit = audit
        self._fernet = _fernet()

    async def put(self, name: str, value: str, *, by: str, namespace: str = "*",
                  allowed_hosts: list[str] | None = None, allowed_tools: list[str] | None = None,
                  allowed_actions: list[str] | None = None, min_tier: str = "READ_ONLY",
                  release: str = "proxy", header: str = "Authorization",
                  header_format: str = "Bearer {value}", description: str = "") -> dict[str, Any]:
        if not NAME_RE.match(name):
            raise ValueError(f"invalid secret name {name!r}")
        if release not in ("proxy", "direct"):
            raise ValueError("release must be 'proxy' or 'direct'")
        PermissionTier(min_tier)
        if "{value}" not in header_format:
            raise ValueError("header_format must contain {value}")
        doc = {
            "name": name, "namespace": namespace, "ciphertext": self._fernet.encrypt(value.encode()).decode(),
            "allowed_hosts": [h.lower() for h in (allowed_hosts or [])],
            "allowed_tools": allowed_tools or [], "allowed_actions": allowed_actions or [],
            "min_tier": min_tier, "release": release, "header": header, "header_format": header_format,
            "description": description[:300], "updated_by": by, "updated_at": time.time(),
        }
        await self._docs.put(COLLECTION, name, doc, namespace=None if namespace == "*" else namespace)
        await self._audit.record("secret_put", by, data={"name": name, "namespace": namespace, "release": release})
        await self._bus.publish(BossmanEvent.create(
            EventType.SECRET_UPDATED, message=f"Secret {name!r} updated by {by}",
            payload={"name": name, "release": release}))
        return self.public(doc)

    async def delete(self, name: str, *, by: str) -> bool:
        ok = await self._docs.delete(COLLECTION, name)
        if ok:
            await self._audit.record("secret_delete", by, data={"name": name})
        return ok

    @staticmethod
    def public(doc: dict[str, Any]) -> dict[str, Any]:
        return {k: v for k, v in doc.items() if k != "ciphertext" and not k.startswith("_")}

    async def list(self) -> list[dict[str, Any]]:
        return [self.public(d) for d in await self._docs.list(COLLECTION, limit=10_000)]

    async def _usable(self, name: str, agent: AgentState, *, tool: str | None,
                      action: str | None) -> dict[str, Any]:
        doc = await self._docs.get(COLLECTION, name)
        if doc is None:
            raise SecretError(f"unknown credential {name!r}")
        if doc["namespace"] not in ("*", agent.info.namespace):
            raise SecretError(f"credential {name!r} is not available in namespace {agent.info.namespace!r}")
        if not agent.info.permission_tier.allows(PermissionTier(doc["min_tier"])):
            raise SecretError(f"credential {name!r} requires tier {doc['min_tier']}")
        if doc["allowed_tools"] and tool not in doc["allowed_tools"]:
            raise SecretError(f"credential {name!r} is not allowed for tool {tool!r}")
        if doc["allowed_actions"] and action not in doc["allowed_actions"]:
            raise SecretError(f"credential {name!r} is not allowed for action {action!r}")
        return doc

    async def release_for(self, names: list[str], agent: AgentState, *, tool: str | None,
                          action: str | None, grant_id: str | None) -> dict[str, Any]:
        """Called only after an allow/approval. Returns direct values or proxy markers."""
        out: dict[str, Any] = {}
        for name in names:
            doc = await self._usable(name, agent, tool=tool, action=action)
            if doc["release"] == "direct":
                out[name] = {"mode": "direct", "value": self._fernet.decrypt(doc["ciphertext"].encode()).decode()}
            else:
                out[name] = {"mode": "proxy", "hosts": doc["allowed_hosts"]}
        if names:
            await self._audit.record("credential_release", f"agent:{agent.agent_id}", namespace=agent.info.namespace,
                                     agent_id=agent.agent_id,
                                     data={"credentials": names, "tool": tool, "action": action, "grant_id": grant_id,
                                           "modes": {n: v["mode"] for n, v in out.items()}})
            await self._bus.publish(BossmanEvent.create(
                EventType.CREDENTIAL_RELEASED, agent_id=agent.agent_id, namespace=agent.info.namespace,
                message=f"Credentials {names} released to {agent.info.name} for {action}",
                payload={"credentials": names, "grant_id": grant_id}))
        return out

    # ── Tool gateway ─────────────────────────────────────────────────────────

    async def proxy(self, agent: AgentState, grant: dict[str, Any], *, credential: str, method: str,
                    url: str, headers: dict[str, str] | None, body: Any, timeout_s: float = 30.0,
                    transport: httpx.AsyncBaseTransport | None = None) -> dict[str, Any]:
        req = grant["request"]
        if req.get("credentials") and credential not in req["credentials"]:
            raise SecretError(f"grant does not cover credential {credential!r}")
        doc = await self._usable(credential, agent, tool=req.get("tool"), action=req.get("action"))
        parsed = urlparse(url)
        host = (parsed.hostname or "").lower()
        if parsed.scheme not in ("https", "http"):
            raise SecretError("only http(s) URLs can be proxied")
        if parsed.scheme == "http" and not os.getenv("BOSSMAN_PROXY_ALLOW_HTTP"):
            raise SecretError("plain http is refused (set BOSSMAN_PROXY_ALLOW_HTTP=1 for local testing)")
        if not doc["allowed_hosts"] or not any(host == h or host.endswith("." + h) for h in doc["allowed_hosts"]):
            raise SecretError(f"host {host!r} is not allow-listed for credential {credential!r}")
        if method.upper() not in {"GET", "POST", "PUT", "PATCH", "DELETE", "HEAD"}:
            raise SecretError(f"method {method!r} not allowed")
        value = self._fernet.decrypt(doc["ciphertext"].encode()).decode()
        out_headers = {k: v for k, v in (headers or {}).items() if k.lower() not in HOP_HEADERS}
        out_headers[doc["header"]] = doc["header_format"].replace("{value}", value)
        kwargs: dict[str, Any] = {}
        if isinstance(body, (dict, list)):
            kwargs["json"] = body
        elif isinstance(body, str):
            kwargs["content"] = body.encode()
        started = time.time()
        async with httpx.AsyncClient(timeout=timeout_s, follow_redirects=False, transport=transport) as client:
            resp = await client.request(method.upper(), url, headers=out_headers, **kwargs)
        content = resp.content[:MAX_PROXY_BODY]
        try:
            text: str | None = content.decode()
            b64 = None
        except UnicodeDecodeError:
            text, b64 = None, base64.b64encode(content).decode()
        await self._audit.record("proxy_request", f"agent:{agent.agent_id}", namespace=agent.info.namespace,
                                 agent_id=agent.agent_id,
                                 data={"grant_id": grant["grant_id"], "credential": credential, "method": method,
                                       "host": host, "path": parsed.path[:200], "status": resp.status_code,
                                       "ms": round((time.time() - started) * 1000)})
        await self._bus.publish(BossmanEvent.create(
            EventType.PROXY_REQUEST, agent_id=agent.agent_id, namespace=agent.info.namespace,
            message=f"{agent.info.name} → {method.upper()} {host}{parsed.path[:80]} ({resp.status_code})",
            payload={"host": host, "status": resp.status_code, "credential": credential}))
        safe_headers = {k: v for k, v in resp.headers.items()
                        if k.lower() in {"content-type", "retry-after", "x-request-id", "etag", "location"}}
        return {"status": resp.status_code, "headers": safe_headers, "text": text, "body_base64": b64,
                "truncated": len(resp.content) > MAX_PROXY_BODY}
