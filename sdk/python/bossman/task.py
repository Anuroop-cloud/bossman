"""Task — a leased unit of work held by an Agent."""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, Any

from bossman import errors

if TYPE_CHECKING:  # pragma: no cover
    from bossman.agent import Agent

log = logging.getLogger("bossman.task")


class Task:
    def __init__(self, agent: Agent, data: dict[str, Any], *, lease_s: float = 60.0) -> None:
        self._agent = agent
        self.data = data
        self.task_id: str = data["task_id"]
        self.title: str = data["title"]
        self.description: str = data.get("description", "")
        self.input: dict[str, Any] = data.get("input_data") or {}
        self.lease_id: str | None = data.get("lease_id")
        self.trace_id: str | None = data.get("trace_id")
        self.traceparent: str | None = data.get("traceparent")
        self.attempt: int = data.get("attempt_count", 1)
        self.max_attempts: int = data.get("max_attempts", 1)
        self.output_schema: dict[str, Any] | None = data.get("output_schema")
        self.checkpoint_data: dict[str, Any] | None = data.get("checkpoint")
        self.lease_s = lease_s
        self.settled = False
        self.cancel_requested = False
        self._runner: asyncio.Task | None = None

    def __repr__(self) -> str:
        return f"Task({self.task_id!r}, {self.title!r}, attempt={self.attempt}/{self.max_attempts})"

    @property
    def resumed(self) -> bool:
        """True when a previous attempt left a checkpoint to resume from."""
        return self.checkpoint_data is not None

    @property
    def last_checkpoint(self) -> dict[str, Any] | None:
        return self.checkpoint_data

    async def progress(self, step: str) -> None:
        """Report progress (also renews the lease and feeds the watchdog's stagnation check)."""
        self._agent.report(step=step)
        await self._renew(step)

    async def _renew(self, step: str | None = None) -> None:
        res = await self._agent.client.post(
            f"/v1/tasks/{self.task_id}/heartbeat",
            {"lease_id": self.lease_id, "lease_s": self.lease_s, "step": step}, retries=1)
        if res.get("cancel_requested"):
            self.cancel_requested = True

    async def _keep_alive(self) -> None:
        while not self.settled:
            await asyncio.sleep(max(1.0, self.lease_s / 3))
            try:
                await self._renew()
            except errors.LeaseConflict:
                log.warning("lease on %s lost", self.task_id)
                return
            except Exception:  # noqa: BLE001 - transient; the next renewal will retry
                continue
            if self.cancel_requested:
                if self._runner is not None and not self._runner.done():
                    self._runner.cancel()  # task was cancelled upstream: stop working on it
                return

    async def checkpoint(self, phase: str, state: dict[str, Any] | None = None) -> dict[str, Any]:
        """Record a resumable phase marker. A retry of this task starts with `task.last_checkpoint`."""
        cp = await self._agent.client.put(f"/v1/tasks/{self.task_id}/checkpoint",
                                          {"lease_id": self.lease_id, "phase": phase, "state": state or {}})
        self.checkpoint_data = cp
        return cp

    async def load_checkpoint(self, *, before_seq: int | None = None) -> dict[str, Any] | None:
        """Latest checkpoint (or the one before `before_seq` — used for ROLLBACK_CHECKPOINT)."""
        res = await self._agent.client.get(f"/v1/tasks/{self.task_id}/checkpoint",
                                           params={"before_seq": before_seq})
        return res.get("checkpoint")

    async def rollback(self) -> dict[str, Any] | None:
        """Step back one checkpoint (what BOSSman means by ROLLBACK_CHECKPOINT)."""
        current = self.checkpoint_data["seq"] if self.checkpoint_data else None
        cp = await self.load_checkpoint(before_seq=current) if current else None
        self.checkpoint_data = cp
        self._agent.rollback_requested = False
        return cp

    async def complete(self, output: dict[str, Any] | None = None) -> dict[str, Any]:
        self.settled = True
        return await self._agent.client.post(f"/v1/tasks/{self.task_id}/complete",
                                             {"lease_id": self.lease_id, "output": output or {}}, retries=2)

    async def fail(self, error: str, *, error_class: str | None = None, status_code: int | None = None,
                   retryable: bool | None = None) -> dict[str, Any]:
        self.settled = True
        return await self._agent.client.post(f"/v1/tasks/{self.task_id}/fail", {
            "lease_id": self.lease_id, "error": error[:4000], "error_class": error_class,
            "status_code": status_code, "retryable": retryable}, retries=2)

    async def spawn(self, title: str, **kw: Any) -> dict[str, Any]:
        """Submit a child task in the same trace (planner → worker)."""
        kw.setdefault("parent_task_id", self.task_id)
        kw.setdefault("traceparent", self.traceparent)
        kw.setdefault("namespace", self._agent.namespace)
        return await self._agent.client.submit(title, **kw)

    def __getitem__(self, key: str) -> Any:
        return self.input[key]
