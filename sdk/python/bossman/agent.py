"""
Agent — one supervised agent process.

    async with Bossman().agent(name="researcher-1", role="researcher",
                               capabilities=["web_search"]) as agent:
        await agent.serve(handle)          # claim → run → complete/fail, forever

    async def handle(task):
        async with agent.lock("kb-write"):           # leased, deadlock-checked lock
            ...
        async with agent.limit("llm", cost=1):       # fleet-wide LLM gate
            reply = await llm(...)
        agent.report(tokens_used=usage, context=0.4) # telemetry for the watchdog
        await task.checkpoint("drafted", {"draft": reply})
        return {"answer": reply}

Background loops keep the agent alive and controllable:
  * heartbeat every N seconds (liveness + telemetry + directive acks)
  * directive long-poll (PAUSE / ABORT / RESTART … arrive within ~a second)
Directives are applied by built-in handlers and then by your hooks
(`@agent.on("COMPACT_CONTEXT")`), and acknowledged.
"""

from __future__ import annotations

import asyncio
import contextlib
import inspect
import logging
import os
import time
import uuid
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Awaitable, Callable

from bossman import errors
from bossman._version import __version__
from bossman.resilience import error_class_of, full_jitter, is_transient, status_of
from bossman.task import Task

log = logging.getLogger("bossman.agent")

Handler = Callable[[Task], Awaitable[Any] | Any]


class Authorization:
    """Result of an allowed (or approved) action."""

    def __init__(self, data: dict[str, Any]) -> None:
        self.data = data
        self.decision: str = data.get("decision", "allow")
        self.grant_id: str | None = data.get("grant_id")
        self.approval_id: str | None = data.get("approval_id")
        self.credentials: dict[str, Any] = data.get("credentials") or {}

    def secret(self, name: str) -> str:
        """Value of a credential released in `direct` mode."""
        c = self.credentials.get(name)
        if not c or c.get("mode") != "direct":
            raise errors.Forbidden(f"credential {name!r} is proxy-only; use agent.proxy(...)")
        return c["value"]


class Agent:
    def __init__(
        self,
        client: Any,
        *,
        name: str | None = None,
        role: str | None = None,
        namespace: str | None = None,
        tier: str = "READ_ONLY",
        capabilities: list[str] | None = None,
        labels: dict[str, str] | None = None,
        metadata: dict[str, Any] | None = None,
        singleton_key: str | None = None,
        takeover: bool = False,
        supervisor: str | None = None,
        start_order: int = 0,
        managed_id: str | None = None,
        heartbeat_interval: float | None = None,
        primary_model: str | None = None,
        fallback_model: str | None = None,
        tokens_budget: int | None = None,
        claim_lease_s: float = 60.0,
    ) -> None:
        self.client = client
        self.name = name or os.getenv("BOSSMAN_AGENT_NAME") or f"agent-{uuid.uuid4().hex[:6]}"
        self.role = role or os.getenv("BOSSMAN_ROLE") or "worker"
        self.namespace = namespace or client.namespace or os.getenv("BOSSMAN_NAMESPACE")
        self.tier = tier.upper()
        self.capabilities = list(capabilities or [])
        self.labels = dict(labels or {})
        # Kubernetes downward API → lets BOSSman restart this pod.
        if os.getenv("POD_NAME") and os.getenv("POD_NAMESPACE"):
            self.labels.setdefault("k8s.pod", os.environ["POD_NAME"])
            self.labels.setdefault("k8s.namespace", os.environ["POD_NAMESPACE"])
        self.metadata = dict(metadata or {})
        self.singleton_key = singleton_key
        self.takeover = takeover
        self.supervisor = supervisor or os.getenv("BOSSMAN_SUPERVISOR")
        self.start_order = start_order
        self.managed_id = managed_id or os.getenv("BOSSMAN_MANAGED_ID")
        self.primary_model = primary_model
        self.fallback_model = fallback_model
        self.model = primary_model
        self.tokens_budget = tokens_budget
        self.claim_lease_s = claim_lease_s
        self._hb_interval_override = heartbeat_interval

        # Session state
        self.agent_id: str | None = None
        self.epoch: int | None = None
        self.heartbeat_interval = heartbeat_interval or 10.0
        self.flags: dict[str, bool] = {}
        self.flags_version = 0
        self.degradation_level = 0          # floor requested by BOSSman
        self._reported_degradation = 0
        self.compact_requested = False
        self.rollback_requested = False
        self.fenced = False
        self.stopping = asyncio.Event()
        self._running = asyncio.Event()
        self._running.set()

        # Telemetry sent on the next heartbeat
        self._tokens_used = 0
        self._context = 0.0
        self._api_calls = 0
        self._step: str | None = None
        self._current_task: Task | None = None
        self._status = "IDLE"

        self._hooks: dict[str, list[Callable[..., Any]]] = {}
        self._probes: dict[str, tuple[dict[str, Any], Callable[[Any], Any]]] = {}
        self._seen: set[str] = set()
        self._acks: list[dict[str, Any]] = []
        self._held_leases: dict[str, Any] = {}
        self._loops: list[asyncio.Task] = []
        self._task_runners: set[asyncio.Task] = set()

    # ── Lifecycle ────────────────────────────────────────────────────────────

    async def __aenter__(self) -> Agent:
        await self.start()
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        await self.stop("error" if exc_type else "shutdown")

    async def _register(self) -> None:
        body = {
            "name": self.name, "role": self.role, "namespace": self.namespace, "permission_tier": self.tier,
            "capabilities": self.capabilities, "labels": self.labels, "metadata": self.metadata,
            "singleton_key": self.singleton_key, "takeover": self.takeover, "supervisor": self.supervisor,
            "start_order": self.start_order, "managed_id": self.managed_id,
            "sdk": f"python/{__version__}", "tokens_budget": self.tokens_budget,
        }
        data = await self.client.post("/v1/agents", {k: v for k, v in body.items() if v is not None})
        self.agent_id = data["agent_id"]
        self.epoch = data["epoch"]
        self.namespace = data["namespace"]
        if self._hb_interval_override is None:
            self.heartbeat_interval = float(data.get("heartbeat_interval_s") or 10.0)
        self.flags = data.get("flags") or {}
        self.flags_version = data.get("flags_version", 0)
        self.fenced = False
        log.info("registered %s as %s (epoch %s)", self.name, self.agent_id, self.epoch)

    async def start(self) -> None:
        await self._register()
        if self._probes:
            await self._register_probes()
        self._loops = [
            asyncio.create_task(self._heartbeat_loop(), name=f"bossman-hb-{self.name}"),
            asyncio.create_task(self._directive_loop(), name=f"bossman-dir-{self.name}"),
        ]

    async def stop(self, reason: str = "shutdown") -> None:
        self.stopping.set()
        for t in list(self._task_runners):
            t.cancel()
        for lease_id in list(self._held_leases):
            with contextlib.suppress(Exception):
                await self.client.post(f"/v1/locks/{lease_id}/release")
        self._held_leases.clear()
        for t in self._loops:
            t.cancel()
        for t in self._loops:
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await t
        self._loops = []
        if self.agent_id and not self.fenced:
            with contextlib.suppress(Exception):
                await self.client.delete(f"/v1/agents/{self.agent_id}",
                                         params={"reason": reason, "epoch": self.epoch}, retries=1)

    # ── Telemetry ────────────────────────────────────────────────────────────

    def report(self, *, tokens_used: int | None = None, tokens: int | None = None, context: float | None = None,
               step: str | None = None, api_calls: int = 0, degradation_level: int | None = None,
               model: str | None = None) -> None:
        """Update telemetry; sent with the next heartbeat. `tokens` adds, `tokens_used` sets the total."""
        if tokens_used is not None:
            self._tokens_used = tokens_used
        if tokens:
            self._tokens_used += tokens
        if context is not None:
            self._context = max(0.0, min(1.0, context))
        if step is not None:
            self._step = step[:200]
        self._api_calls += api_calls
        if degradation_level is not None:
            self._reported_degradation = degradation_level
        if model is not None:
            self.model = model

    async def heartbeat(self) -> dict[str, Any]:
        acks, self._acks = self._acks, []
        body = {
            "epoch": self.epoch, "status": self._status,
            "current_task_id": self._current_task.task_id if self._current_task else None,
            "current_step": self._step, "tokens_used": self._tokens_used, "context_utilisation": self._context,
            "api_calls_since_last_heartbeat": self._api_calls,
            "degradation_level": max(self.degradation_level, self._reported_degradation),
            "model": self.model, "paused": not self._running.is_set(), "acked_directives": acks,
            "flags_version": self.flags_version,
        }
        self._api_calls = 0
        try:
            ack = await self.client.post(f"/v1/agents/{self.agent_id}/heartbeat", body, retries=1)
        except Exception:
            self._acks = acks + self._acks  # resend next time
            raise
        if ack.get("flags") is not None:
            self.flags = ack["flags"]
            self.flags_version = ack.get("flags_version", self.flags_version)
        for d in ack.get("directives", []):
            await self._handle_directive(d)
        return ack

    async def _heartbeat_loop(self) -> None:
        failures = 0
        while not self.stopping.is_set():
            try:
                await asyncio.sleep(self.heartbeat_interval if failures == 0
                                    else min(self.heartbeat_interval, full_jitter(failures, 0.5, 10)))
                await self.heartbeat()
                failures = 0
            except asyncio.CancelledError:
                raise
            except errors.Fenced:
                await self._on_fenced()
                return
            except errors.NotFound:
                # Control plane lost us (e.g. restarted without persistence): re-register.
                log.warning("%s unknown to BOSSman — re-registering", self.name)
                with contextlib.suppress(Exception):
                    self.takeover = True
                    await self._register()
                    if self._probes:
                        await self._register_probes()
            except Exception as exc:  # noqa: BLE001 - keep beating through outages
                failures += 1
                log.warning("heartbeat failed (%s): %s", failures, exc)

    async def _directive_loop(self) -> None:
        failures = 0
        while not self.stopping.is_set():
            try:
                res = await self.client.get(f"/v1/agents/{self.agent_id}/directives",
                                            params={"wait": 25, "epoch": self.epoch}, timeout=40.0, retries=0)
                for d in res.get("directives", []):
                    await self._handle_directive(d)
                failures = 0
            except asyncio.CancelledError:
                raise
            except errors.Fenced:
                await self._on_fenced()
                return
            except Exception:  # noqa: BLE001
                failures += 1
                await asyncio.sleep(full_jitter(failures, 0.5, 15))

    async def _on_fenced(self) -> None:
        log.error("%s was taken over by another process (fenced) — stopping work", self.name)
        self.fenced = True
        self.stopping.set()
        for t in list(self._task_runners):
            t.cancel()

    # ── Directives ───────────────────────────────────────────────────────────

    def on(self, directive_type: str) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        """Register a hook: `@agent.on("COMPACT_CONTEXT") async def compact(directive): ...`"""
        def deco(fn: Callable[..., Any]) -> Callable[..., Any]:
            self._hooks.setdefault(directive_type.upper(), []).append(fn)
            return fn
        return deco

    async def _handle_directive(self, d: dict[str, Any]) -> None:
        did = d["directive_id"]
        if did in self._seen:
            return
        self._seen.add(did)
        dtype = d["type"]
        params = d.get("params") or {}
        ok, result = True, {"applied": dtype}
        try:
            builtin = await self._builtin(dtype, params)
            if builtin:
                result.update(builtin)
            for hook in self._hooks.get(dtype, []):
                out = hook(d)
                if inspect.isawaitable(out):
                    out = await out
                if isinstance(out, dict):
                    result.update(out)
        except Exception as exc:  # noqa: BLE001
            ok, result = False, {"error": f"{type(exc).__name__}: {exc}"[:300]}
        with contextlib.suppress(Exception):
            await self.client.post(f"/v1/agents/{self.agent_id}/directives/{did}/ack",
                                   {"ok": ok, "result": result}, retries=2)
            return
        self._acks.append({"directive_id": did, "ok": ok, "result": result})  # piggy-back on heartbeat

    async def _builtin(self, dtype: str, params: dict[str, Any]) -> dict[str, Any] | None:
        if dtype == "PAUSE":
            self._running.clear()
        elif dtype == "RESUME":
            self._running.set()
        elif dtype == "ABORT_TASK":
            for t in list(self._task_runners):
                t.cancel()
        elif dtype == "COMPACT_CONTEXT":
            self.compact_requested = True
        elif dtype == "ROLLBACK_CHECKPOINT":
            self.rollback_requested = True
        elif dtype == "FALLBACK_MODEL":
            self.degradation_level = max(self.degradation_level, 1)
            if self.fallback_model:
                self.model = self.fallback_model
            return {"model": self.model}
        elif dtype == "RESTORE_MODEL":
            self.degradation_level = 0
            self.model = self.primary_model
            return {"model": self.model}
        elif dtype == "SET_DEGRADATION":
            self.degradation_level = int(params.get("level", 0))
            if self.degradation_level == 0:
                self.model = self.primary_model
            elif self.fallback_model:
                self.model = self.fallback_model
        elif dtype == "RELEASE_RESOURCES":
            self._held_leases.clear()  # server already revoked them; drop local handles
        elif dtype == "RESTART":
            for t in list(self._task_runners):
                t.cancel()
            self._held_leases.clear()
            self.compact_requested = False
            self.rollback_requested = False
            self._context = 0.0
            self._step = None
            self._running.set()
        elif dtype == "SHUTDOWN":
            self.stopping.set()
            for t in list(self._task_runners):
                t.cancel()
        elif dtype == "RUN_PROBE":
            asyncio.ensure_future(self._run_probe(params.get("probe", ""), params.get("input")))
        elif dtype == "REFRESH_FLAGS":
            self.flags_version = -1
        return None

    async def wait_if_paused(self) -> None:
        """Call at safe points in long work: blocks while BOSSman has paused this agent."""
        if not self._running.is_set():
            self._step = "paused"
            await self._running.wait()

    def flag(self, key: str, default: bool = True) -> bool:
        return self.flags.get(key, default)

    # ── Tasks ────────────────────────────────────────────────────────────────

    async def claim(self, *, wait_s: float = 20.0, lease_s: float | None = None,
                    roles: list[str] | None = None) -> Task | None:
        await self.wait_if_paused()
        data = await self.client.post(
            f"/v1/agents/{self.agent_id}/tasks/claim",
            {"wait_s": wait_s, "lease_s": lease_s or self.claim_lease_s, "roles": roles or []},
            params={"epoch": self.epoch}, timeout=wait_s + 15, retries=1)
        if data is None:
            return None
        return Task(self, data, lease_s=lease_s or self.claim_lease_s)

    async def serve(self, handler: Handler, *, concurrency: int = 1, wait_s: float = 20.0,
                    stop_when_idle: bool = False) -> None:
        """
        Claim tasks and run `handler(task)` until stopped (SHUTDOWN directive,
        fencing, or `agent.stopping.set()`). The handler's return value (a
        dict) completes the task; an exception fails it — transient errors are
        retried by BOSSman with backoff, others go to the DLQ.
        """
        sem = asyncio.Semaphore(concurrency)
        while not self.stopping.is_set():
            await sem.acquire()
            if self.stopping.is_set():
                sem.release()
                break
            try:
                task = await self.claim(wait_s=wait_s)
            except (errors.Fenced, asyncio.CancelledError):
                sem.release()
                break
            except Exception as exc:  # noqa: BLE001
                sem.release()
                log.warning("claim failed: %s", exc)
                await asyncio.sleep(full_jitter(2, 0.5, 5))
                continue
            if task is None:
                sem.release()
                if stop_when_idle:
                    break
                continue
            runner = asyncio.create_task(self._run(task, handler))
            self._task_runners.add(runner)
            runner.add_done_callback(lambda t: (self._task_runners.discard(t), sem.release()))
        if self._task_runners:
            await asyncio.gather(*self._task_runners, return_exceptions=True)

    async def _run(self, task: Task, handler: Handler) -> None:
        self._current_task = task
        self._status = "RUNNING"
        self._step = "started"
        task._runner = asyncio.current_task()
        renew = asyncio.create_task(task._keep_alive())
        try:
            out = handler(task)
            if inspect.isawaitable(out):
                out = await out
            if not task.settled:
                await task.complete(out if isinstance(out, dict) else ({"result": out} if out is not None else {}))
        except asyncio.CancelledError:
            if not task.settled:
                with contextlib.suppress(Exception):
                    await task.fail("aborted by BOSSman directive or shutdown", error_class="aborted", retryable=True)
        except errors.OutputInvalid as exc:
            log.warning("task %s output rejected: %s", task.task_id, exc)
        except errors.LeaseConflict:
            log.warning("task %s lease lost — another worker owns it now", task.task_id)
        except Exception as exc:  # noqa: BLE001
            if not task.settled:
                with contextlib.suppress(Exception):
                    await task.fail(f"{type(exc).__name__}: {exc}", error_class=error_class_of(exc),
                                    status_code=status_of(exc), retryable=is_transient(exc))
        finally:
            renew.cancel()
            self._current_task = None
            self._status = "IDLE"
            self._step = None

    async def submit(self, title: str, **kw: Any) -> dict[str, Any]:
        """Submit a task; inside a task handler, it becomes a child in the same trace."""
        if self._current_task is not None:
            kw.setdefault("parent_task_id", self._current_task.task_id)
            kw.setdefault("traceparent", self._current_task.traceparent)
        kw.setdefault("namespace", self.namespace)
        return await self.client.submit(title, **kw)

    # ── Locks / limits / circuits ────────────────────────────────────────────

    @asynccontextmanager
    async def lock(self, *resources: str, ttl_s: float = 30.0, wait_s: float = 10.0) -> AsyncIterator[dict[str, Any]]:
        """
        Exclusive, leased lock on one or more named resources (acquired
        atomically in a global order). Renewed in the background; raises
        errors.Deadlock if waiting would close a cycle, errors.LockTimeout on timeout.
        """
        lease = await self.client.post("/v1/locks/acquire", {
            "agent_id": self.agent_id, "resources": list(resources), "ttl_s": ttl_s, "wait_s": wait_s,
            "task_id": self._current_task.task_id if self._current_task else None,
        }, timeout=wait_s + 15, retries=0)
        lease_id = lease["lease_id"]
        self._held_leases[lease_id] = lease

        async def renew() -> None:
            while True:
                await asyncio.sleep(max(0.5, ttl_s / 3))
                try:
                    await self.client.post(f"/v1/locks/{lease_id}/renew", {"ttl_s": ttl_s}, retries=1)
                except (errors.Conflict, errors.NotFound):
                    log.error("lock lease %s lost (revoked or expired)", lease_id)
                    return
                except Exception:  # noqa: BLE001
                    continue

        renewer = asyncio.create_task(renew())
        try:
            yield lease
        finally:
            renewer.cancel()
            self._held_leases.pop(lease_id, None)
            with contextlib.suppress(Exception):
                await self.client.post(f"/v1/locks/{lease_id}/release", retries=2)

    @asynccontextmanager
    async def limit(self, name: str = "llm", *, cost: float = 1.0, wait_s: float = 30.0) -> AsyncIterator[dict[str, Any]]:
        """Fleet-wide concurrency + rate gate (e.g. all LLM calls across every agent)."""
        permit = await self.client.post(f"/v1/limits/{name}/acquire",
                                        {"agent_id": self.agent_id, "cost": cost, "wait_s": wait_s},
                                        timeout=wait_s + 15, retries=0)
        try:
            yield permit
        finally:
            with contextlib.suppress(Exception):
                await self.client.post(f"/v1/limits/permits/{permit['permit_id']}/release", retries=2)

    async def circuit_allows(self, key: str) -> bool:
        res = await self.client.post(f"/v1/circuits/{key}/allow")
        return bool(res.get("allowed"))

    async def guarded(self, dependency: str, fn: Callable[[], Awaitable[Any]], *,
                      fallback: Callable[[], Awaitable[Any]] | Any = None) -> Any:
        """
        Call a dependency through the fleet-wide circuit breaker: fail fast (or
        use `fallback`) while it is open, and report every outcome.
        """
        allow = await self.client.post(f"/v1/circuits/{dependency}/allow")
        if not allow.get("allowed"):
            if fallback is not None:
                out = fallback() if callable(fallback) else fallback
                return await out if inspect.isawaitable(out) else out
            raise errors.CircuitOpen(dependency, allow.get("retry_after_s", 0.0))
        started = time.monotonic()
        try:
            result = await fn()
        except Exception as exc:
            await self._report_outcome(dependency, False, exc, started)
            raise
        await self._report_outcome(dependency, True, None, started)
        return result

    async def _report_outcome(self, dependency: str, ok: bool, exc: BaseException | None, started: float) -> None:
        with contextlib.suppress(Exception):
            await self.client.post(f"/v1/circuits/{dependency}/report",
                                   {"ok": ok, "error": str(exc)[:300] if exc else None, "agent_id": self.agent_id},
                                   retries=1)
        await self.event("tool_call", dependency, ok=ok, duration_ms=(time.monotonic() - started) * 1000,
                         status_code=status_of(exc) if exc else None,
                         error_class=error_class_of(exc) if exc else None,
                         transient=is_transient(exc) if exc else None,
                         error=f"{type(exc).__name__}: {exc}"[:500] if exc else None)

    async def call_tool(
        self,
        name: str,
        fn: Callable[[dict[str, Any]], Awaitable[Any]],
        params: dict[str, Any] | None = None,
        *,
        alternatives: dict[str, Callable[[dict[str, Any]], Awaitable[Any]]] | None = None,
        max_attempts: int = 4,
    ) -> Any:
        """
        Run a tool with BOSSman-directed compensation: on failure the server
        decides retry (possibly with modified params), substitute an
        alternative, or give up. Every attempt is reported.
        """
        params = dict(params or {})
        alternatives = dict(alternatives or {})
        current_name, current_fn = name, fn
        attempt = 0
        budget = max_attempts + len(alternatives)  # a substitution earns its own attempt
        while attempt < budget:
            attempt += 1
            started = time.monotonic()
            try:
                result = await current_fn(params)
                await self.event("tool_call", current_name, ok=True, duration_ms=(time.monotonic() - started) * 1000)
                return result
            except Exception as exc:  # noqa: BLE001
                await self.event("tool_call", current_name, ok=False, duration_ms=(time.monotonic() - started) * 1000,
                                 status_code=status_of(exc), error_class=error_class_of(exc),
                                 transient=is_transient(exc), error=f"{type(exc).__name__}: {exc}"[:500])
                try:
                    decision = await self.client.post(f"/v1/agents/{self.agent_id}/compensate", {
                        "tool": current_name, "error_class": error_class_of(exc), "status_code": status_of(exc),
                        "error": str(exc)[:1000], "params": params, "attempt": attempt,
                        "alternatives": list(alternatives),
                        "retry_after_s": getattr(exc, "retry_after_s", None),
                    }, retries=1)
                except Exception:  # noqa: BLE001 - control plane down: local policy
                    decision = {"action": "retry" if is_transient(exc) and attempt < max_attempts else "give_up",
                                "delay_s": full_jitter(attempt), "params": params}
                substitute = decision["action"] == "substitute" and decision.get("substitute") in alternatives
                if decision["action"] == "give_up" or (attempt >= max_attempts and not substitute):
                    raise
                if substitute:
                    current_name = decision["substitute"]
                    current_fn = alternatives.pop(current_name)
                params = decision.get("params") or params
                await asyncio.sleep(float(decision.get("delay_s") or 0))
        raise RuntimeError("unreachable")

    # ── Governance ───────────────────────────────────────────────────────────

    async def authorize(
        self,
        action: str,
        *,
        tool: str | None = None,
        resource: str | None = None,
        irreversible: bool | None = None,
        args: Any = None,
        justification: str | None = None,
        labels: dict[str, str] | None = None,
        credentials: list[str] | None = None,
        wait_for_approval_s: float = 600.0,
    ) -> Authorization:
        """
        Ask BOSSman before acting. Returns an Authorization on allow (waiting
        for human approval when required); raises PolicyDenied otherwise.
        """
        body = {"action": action, "tool": tool, "resource": resource, "irreversible": irreversible,
                "args": args, "justification": justification, "labels": labels or {},
                "credentials": credentials or [],
                "task_id": self._current_task.task_id if self._current_task else None}
        decision = await self.client.post(f"/v1/agents/{self.agent_id}/authorize", body,
                                          params={"epoch": self.epoch})
        if decision["decision"] == "allow":
            return Authorization(decision)
        if decision["decision"] == "deny":
            raise errors.PolicyDenied(f"{action} denied: {decision['reason']}", decision)
        approval_id = decision["approval_id"]
        self._step = f"awaiting approval {approval_id}"
        loop = asyncio.get_running_loop()
        deadline = loop.time() + wait_for_approval_s
        while True:
            remaining = deadline - loop.time()
            if remaining <= 0:
                raise errors.ApprovalTimeout(f"approval {approval_id} not decided within {wait_for_approval_s}s",
                                             body={"approval_id": approval_id})
            a = await self.client.get(f"/v1/approvals/{approval_id}", params={"wait": min(25.0, remaining)},
                                      timeout=40.0)
            if a["status"] == "approved":
                return Authorization({**decision, **a, "decision": "allow"})
            if a["status"] in ("denied", "expired"):
                raise errors.PolicyDenied(f"{action} {a['status']} by humans",
                                          {**decision, "status": a["status"], "denials": a.get("denials")})

    async def proxy(self, auth: Authorization, *, credential: str, method: str = "GET", url: str,
                    headers: dict[str, str] | None = None, body: Any = None, timeout_s: float = 30.0) -> dict[str, Any]:
        """Perform an authorised HTTP call through the gateway; BOSSman injects the credential."""
        if not auth.grant_id:
            raise errors.Forbidden("authorization has no grant")
        return await self.client.post(f"/v1/agents/{self.agent_id}/proxy", {
            "grant_id": auth.grant_id, "credential": credential, "method": method, "url": url,
            "headers": headers or {}, "body": body, "timeout_s": timeout_s}, timeout=timeout_s + 15, retries=0)

    async def once(self, key: str, fn: Callable[[], Awaitable[Any]], *, ttl_s: float = 300.0,
                   wait_s: float = 60.0) -> Any:
        """
        Run a side effect at most once per key across the fleet; replays (after
        a crash or reassignment) get the stored result instead of a second effect.
        """
        loop = asyncio.get_running_loop()
        deadline = loop.time() + wait_s
        while True:
            state = await self.client.post(f"/v1/idempotency/{key}/begin", {"agent_id": self.agent_id, "ttl_s": ttl_s})
            if state["state"] == "completed":
                return state.get("result")
            if state["state"] == "new":
                break
            if loop.time() >= deadline:
                raise errors.Conflict(f"idempotency key {key!r} is held by {state.get('owner')}")
            await asyncio.sleep(min(float(state.get("retry_after_s") or 1.0), 2.0))
        try:
            result = await fn()
        except BaseException:
            with contextlib.suppress(Exception):
                await self.client.post(f"/v1/idempotency/{key}/abort", {"agent_id": self.agent_id})
            raise
        await self.client.post(f"/v1/idempotency/{key}/complete", {"agent_id": self.agent_id, "result": result})
        return result

    # ── Quality / probes / events ────────────────────────────────────────────

    async def report_quality(self, score: float, *, task_id: str | None = None, evaluator: str = "agent",
                             note: str = "") -> dict[str, Any]:
        return await self.client.post(f"/v1/agents/{self.agent_id}/quality",
                                      {"score": score, "task_id": task_id, "evaluator": evaluator, "note": note})

    def probe(self, name: str, *, input: Any, expect: dict[str, Any], interval_s: float = 300.0,  # noqa: A002
              timeout_s: float = 60.0) -> Callable[[Callable[[Any], Any]], Callable[[Any], Any]]:
        """
        Register a known-answer probe (L3 functional health):

            @agent.probe("arithmetic", input="What is 2+2?", expect={"contains": "4"})
            async def p(question): return await answer(question)
        """
        def deco(fn: Callable[[Any], Any]) -> Callable[[Any], Any]:
            self._probes[name] = ({"name": name, "input": input, "expect": expect, "interval_s": interval_s,
                                   "timeout_s": timeout_s}, fn)
            if self.agent_id:
                asyncio.ensure_future(self._register_probes())
            return fn
        return deco

    async def _register_probes(self) -> None:
        await self.client.put(f"/v1/agents/{self.agent_id}/probes",
                              {"probes": [spec for spec, _ in self._probes.values()]})

    async def _run_probe(self, name: str, value: Any) -> None:
        entry = self._probes.get(name)
        if entry is None:
            return
        try:
            out = entry[1](value)
            if inspect.isawaitable(out):
                out = await asyncio.wait_for(out, timeout=entry[0]["timeout_s"])
            body: dict[str, Any] = {"output": out}
        except Exception as exc:  # noqa: BLE001
            body = {"error": f"{type(exc).__name__}: {exc}"[:1000]}
        with contextlib.suppress(Exception):
            await self.client.post(f"/v1/agents/{self.agent_id}/probes/{name}/result", body)

    async def event(self, type: str, name: str = "", **fields: Any) -> None:  # noqa: A002
        """Report a tool/LLM call or log line (feeds BOSSman's detectors and traces)."""
        if type in ("tool_call", "llm_call"):
            self._api_calls += 1
        task = self._current_task
        payload = {"type": type, "name": name, "task_id": task.task_id if task else None,
                   "trace_id": task.trace_id if task else None,
                   **{k: v for k, v in fields.items() if v is not None}}
        with contextlib.suppress(Exception):
            await self.client.post(f"/v1/agents/{self.agent_id}/events", {"events": [payload]}, retries=1)

    async def log(self, message: str, **data: Any) -> None:
        await self.event("log", message=message[:1000], data=data)
