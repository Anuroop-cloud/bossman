"""Shared fixtures: an in-process BOSSman server + SDK clients wired over ASGI."""

from __future__ import annotations

import asyncio
import contextlib
from typing import Any, AsyncIterator

import httpx

from bossman_server.api.app import create_app
from bossman_server.api.runtime import BossmanRuntime
from bossman_server.infra.settings import InfraSettings

ADMIN = "test-admin-token"
OPERATOR = "test-operator-token"
OPERATOR2 = "test-operator2-token"   # second human for two-person approvals (created as a key)
AGENT = "test-agent-token"


def set_env(monkeypatch: Any, **extra: str) -> None:
    monkeypatch.setenv("BOSSMAN_ADMIN_TOKEN", ADMIN)
    monkeypatch.setenv("BOSSMAN_OPERATOR_TOKEN", OPERATOR)
    monkeypatch.setenv("BOSSMAN_AGENT_TOKEN", AGENT)
    monkeypatch.setenv("BOSSMAN_AGENT_MAX_TIER", "PRIVILEGED")
    monkeypatch.delenv("BOSSMAN_DEMO_SEED", raising=False)
    for k, v in extra.items():
        monkeypatch.setenv(k, v)


@contextlib.asynccontextmanager
async def server(settings: InfraSettings | None = None, **rt_kwargs: Any) -> AsyncIterator[tuple[Any, BossmanRuntime]]:
    rt = BossmanRuntime(settings or InfraSettings(heartbeat_interval_s=0.2, heartbeat_timeout_s=30), **rt_kwargs)
    app = create_app(rt)
    async with app.router.lifespan_context(app):
        yield app, rt


def http(app: Any, token: str | None) -> httpx.AsyncClient:
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://bossman", headers=headers)


def sdk(app: Any, token: str = AGENT, **kw: Any):  # noqa: ANN201
    from bossman import Bossman

    return Bossman("http://bossman", api_key=token, transport=httpx.ASGITransport(app=app), **kw)


async def until(cond: Any, timeout: float = 5.0, interval: float = 0.02) -> bool:
    loop = asyncio.get_running_loop()
    end = loop.time() + timeout
    while loop.time() < end:
        v = cond()
        if asyncio.iscoroutine(v):
            v = await v
        if v:
            return True
        await asyncio.sleep(interval)
    return False
