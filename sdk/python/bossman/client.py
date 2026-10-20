"""
Bossman — the async client.

    bm = Bossman()                         # BOSSMAN_URL / BOSSMAN_API_KEY from the environment
    bm = Bossman("http://localhost:7070", api_key="bm_…")

    async with bm.agent(name="researcher-1", role="researcher") as agent:
        await agent.serve(handle_task)

    task = await bm.submit("Summarise Q3", input={...}, required_role="researcher")
    done = await bm.wait(task["task_id"], timeout=300)
"""

from __future__ import annotations

import asyncio
import os
from typing import Any

import httpx

from bossman import errors
from bossman._version import __version__
from bossman.resilience import full_jitter

DEFAULT_URL = "http://127.0.0.1:7070"
RETRY_STATUS = {502, 503, 504}


class Bossman:
    def __init__(
        self,
        url: str | None = None,
        *,
        api_key: str | None = None,
        namespace: str | None = None,
        timeout: float = 30.0,
        max_retries: int = 3,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self.url = (url or os.getenv("BOSSMAN_URL") or DEFAULT_URL).rstrip("/")
        self.api_key = api_key or os.getenv("BOSSMAN_API_KEY") or os.getenv("BOSSMAN_TOKEN")
        self.namespace = namespace or os.getenv("BOSSMAN_NAMESPACE")
        self.max_retries = max_retries
        headers = {"User-Agent": f"bossman-sdk-python/{__version__}", "Accept": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        self._http = httpx.AsyncClient(base_url=self.url, headers=headers, timeout=timeout, transport=transport)

    async def close(self) -> None:
        await self._http.aclose()

    async def __aenter__(self) -> Bossman:
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.close()

    # ── Transport ────────────────────────────────────────────────────────────

    async def request(self, method: str, path: str, *, json: Any = None, params: dict[str, Any] | None = None,
                      timeout: float | None = None, retries: int | None = None) -> Any:
        """One API call with transport-level retries (full jitter). Returns parsed JSON or None (204)."""
        attempts = self.max_retries if retries is None else retries
        params = {k: v for k, v in (params or {}).items() if v is not None}
        kwargs: dict[str, Any] = {"json": json, "params": params}
        if timeout is not None:
            kwargs["timeout"] = timeout
        for attempt in range(attempts + 1):
            try:
                resp = await self._http.request(method, path, **kwargs)
            except httpx.TransportError as exc:
                if attempt >= attempts:
                    raise errors.Unavailable(f"BOSSman unreachable at {self.url}: {exc}") from exc
                await asyncio.sleep(full_jitter(attempt, 0.5, 10))
                continue
            if resp.status_code in RETRY_STATUS and attempt < attempts:
                await asyncio.sleep(full_jitter(attempt, 0.5, 10))
                continue
            if resp.status_code == 204:
                return None
            try:
                body = resp.json()
            except ValueError:
                body = {"error": {"code": "bad_response", "message": resp.text[:300]}}
            if resp.status_code >= 400:
                raise errors.from_response(resp.status_code, body)
            return body
        raise errors.Unavailable("retries exhausted")

    async def get(self, path: str, **kw: Any) -> Any:
        return await self.request("GET", path, **kw)

    async def post(self, path: str, json: Any = None, **kw: Any) -> Any:
        return await self.request("POST", path, json=json if json is not None else {}, **kw)

    async def put(self, path: str, json: Any = None, **kw: Any) -> Any:
        return await self.request("PUT", path, json=json if json is not None else {}, **kw)

    async def delete(self, path: str, **kw: Any) -> Any:
        return await self.request("DELETE", path, **kw)

    # ── Agents ───────────────────────────────────────────────────────────────

    def agent(self, *, name: str | None = None, role: str | None = None, **kw: Any):  # noqa: ANN201
        """Create an Agent session (use as `async with`)."""
        from bossman.agent import Agent

        return Agent(self, name=name, role=role, **kw)

    # ── Tasks (submitter side) ───────────────────────────────────────────────

    async def submit(
        self,
        title: str,
        *,
        input: dict[str, Any] | None = None,  # noqa: A002
        description: str = "",
        required_role: str | None = None,
        priority: str = "NORMAL",
        max_attempts: int = 3,
        idempotency_key: str | None = None,
        depends_on: list[str] | None = None,
        output_schema: dict[str, Any] | None = None,
        parent_task_id: str | None = None,
        traceparent: str | None = None,
        deadline: str | None = None,
        labels: dict[str, str] | None = None,
        namespace: str | None = None,
    ) -> dict[str, Any]:
        body = {
            "title": title, "description": description, "input": input or {}, "required_role": required_role,
            "priority": priority, "max_attempts": max_attempts, "idempotency_key": idempotency_key,
            "depends_on": depends_on or [], "output_schema": output_schema, "parent_task_id": parent_task_id,
            "traceparent": traceparent, "deadline": deadline, "labels": labels or {},
            "namespace": namespace or self.namespace,
        }
        return await self.post("/v1/tasks", {k: v for k, v in body.items() if v is not None})

    async def task(self, task_id: str) -> dict[str, Any]:
        return await self.get(f"/v1/tasks/{task_id}")

    async def wait(self, task_id: str, timeout: float = 300.0) -> dict[str, Any]:
        """Block until the task is terminal (COMPLETED / DEAD_LETTER / QUARANTINED / CANCELLED)."""
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        while True:
            remaining = deadline - loop.time()
            t = await self.get(f"/v1/tasks/{task_id}", params={"wait": max(0.0, min(remaining, 25.0))},
                               timeout=40.0)
            if t["is_terminal"] or remaining <= 0:
                return t

    async def tasks(self, status: str | None = None, limit: int = 100) -> list[dict[str, Any]]:
        return (await self.get("/v1/tasks", params={"status": status, "limit": limit,
                                                     "namespace": self.namespace}))["tasks"]

    async def cancel(self, task_id: str) -> dict[str, Any]:
        return await self.post(f"/v1/tasks/{task_id}/cancel")

    async def agents(self, role: str | None = None) -> list[dict[str, Any]]:
        return (await self.get("/v1/agents", params={"role": role, "namespace": self.namespace}))["agents"]
