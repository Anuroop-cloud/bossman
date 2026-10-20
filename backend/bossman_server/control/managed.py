"""
control.managed
───────────────
Managed mode: BOSSman owns the agent process, so a restart is a real restart.

  runtime=process   asyncio subprocess (any command: python agent.py, node …)
  runtime=docker    `docker run --rm` of an image (stopped with `docker stop`)

The launched process gets BOSSMAN_URL / BOSSMAN_API_KEY / BOSSMAN_MANAGED_ID /
BOSSMAN_AGENT_NAME / BOSSMAN_NAMESPACE / BOSSMAN_ROLE in its environment; the
SDK uses them to register with `managed_id`, so the agent identity survives
process restarts (registration takes over the old identity, bumping the epoch).

Graceful stop maps zylos.md's SIGUSR1 → SIGTERM sequence:
  SHUTDOWN directive (soft) → wait → terminate (SIGTERM) → wait → kill.

Crash handling: restart_policy (always | on-failure | never) with exponential
backoff + full jitter and a restart budget; an exhausted budget marks the
spec CRASHLOOP and raises a human gate instead of restarting forever.

Only the leader runs processes; desired state lives in the document store
so a new leader starts what the old one ran.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import random
import re
import shutil
import signal
import sys
import time
import uuid
from collections import deque
from dataclasses import asdict, dataclass, field
from typing import Any

import httpx

from bossman_server.contracts.agent_state import NAMESPACE_PATTERN, AgentState
from bossman_server.contracts.directives import DirectiveType
from bossman_server.contracts.events import BossmanEvent, EventSeverity, EventType
from bossman_server.core.documents import DocumentStore
from bossman_server.core.event_bus import EventBus

log = logging.getLogger(__name__)

COLLECTION = "managed"
NAME_RE = re.compile(r"^[a-z0-9][a-z0-9._-]{0,63}$")
LOG_LINES = 300


@dataclass
class ManagedSpec:
    name: str
    command: list[str] = field(default_factory=list)
    runtime: str = "process"                 # process | docker
    image: str | None = None                 # docker runtime
    args: list[str] = field(default_factory=list)
    env: dict[str, str] = field(default_factory=dict)
    cwd: str | None = None
    namespace: str = "default"
    role: str = "worker"
    supervisor: str | None = None
    restart_policy: str = "on-failure"       # always | on-failure | never
    max_restarts: int = 5
    window_s: float = 300.0
    graceful_timeout_s: float = 10.0
    desired_state: str = "running"           # running | stopped
    managed_id: str = field(default_factory=lambda: f"mg-{uuid.uuid4().hex[:10]}")
    created_by: str = "system"

    def validate(self) -> None:
        if not NAME_RE.match(self.name):
            raise ValueError(f"invalid managed agent name {self.name!r}")
        if not re.match(NAMESPACE_PATTERN, self.namespace):
            raise ValueError(f"invalid namespace {self.namespace!r}")
        if self.runtime not in ("process", "docker"):
            raise ValueError("runtime must be 'process' or 'docker'")
        if self.runtime == "process" and not self.command:
            raise ValueError("process runtime needs a command")
        if self.runtime == "docker" and not self.image:
            raise ValueError("docker runtime needs an image")
        if self.restart_policy not in ("always", "on-failure", "never"):
            raise ValueError("restart_policy must be always | on-failure | never")
        if self.desired_state not in ("running", "stopped"):
            raise ValueError("desired_state must be running | stopped")
        for k in self.env:
            if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", k):
                raise ValueError(f"invalid env var name {k!r}")


@dataclass
class ProcState:
    proc: asyncio.subprocess.Process | None = None
    pid: int | None = None
    started_at: float | None = None
    exit_code: int | None = None
    state: str = "stopped"            # starting | running | stopping | stopped | backoff | crashloop
    restarts: deque = field(default_factory=deque)
    total_restarts: int = 0
    next_start_at: float = 0.0
    logs: deque = field(default_factory=lambda: deque(maxlen=LOG_LINES))
    stopping: bool = False
    watcher: asyncio.Task | None = None


class ProcessManager:
    def __init__(self, docs: DocumentStore, bus: EventBus, *, directives: Any | None = None,
                 registry: Any | None = None, base_url: str | None = None,
                 agent_token: str | None = None) -> None:
        self._docs = docs
        self._bus = bus
        self._directives = directives
        self._registry = registry
        self._base_url = base_url or os.getenv("BOSSMAN_PUBLIC_URL") or "http://127.0.0.1:7070"
        self._agent_token = agent_token
        self._specs: dict[str, ManagedSpec] = {}
        self._procs: dict[str, ProcState] = {}
        self._task: asyncio.Task | None = None
        self._lock = asyncio.Lock()
        self.active = False

    # ── Specs ────────────────────────────────────────────────────────────────

    async def load(self) -> None:
        self._specs = {}
        for d in await self._docs.list(COLLECTION, limit=10_000):
            spec = ManagedSpec(**{k: v for k, v in d.items() if not k.startswith("_")})
            self._specs[spec.managed_id] = spec

    async def put(self, spec: ManagedSpec) -> ManagedSpec:
        spec.validate()
        if any(s.name == spec.name and s.managed_id != spec.managed_id for s in self._specs.values()):
            raise ValueError(f"managed agent {spec.name!r} already exists")
        self._specs[spec.managed_id] = spec
        await self._docs.put(COLLECTION, spec.managed_id, asdict(spec), namespace=spec.namespace,
                             status=spec.desired_state)
        return spec

    async def delete(self, managed_id: str) -> bool:
        spec = self._specs.get(managed_id)
        if spec is None:
            return False
        spec.desired_state = "stopped"
        await self._stop(managed_id, reason="deleted")
        self._specs.pop(managed_id, None)
        self._procs.pop(managed_id, None)
        return await self._docs.delete(COLLECTION, managed_id)

    async def set_desired(self, managed_id: str, desired: str) -> ManagedSpec:
        spec = self._specs[managed_id]
        spec.desired_state = desired
        if desired == "running":
            st = self._procs.get(managed_id)
            if st and st.state == "crashloop":
                st.state = "stopped"
                st.restarts.clear()
        await self.put(spec)
        if desired == "stopped":
            await self._stop(managed_id, reason="stopped by operator")
        return spec

    def get_spec(self, managed_id: str) -> ManagedSpec | None:
        return self._specs.get(managed_id)

    # ── Leader lifecycle ─────────────────────────────────────────────────────

    async def start(self) -> None:
        if self._task is None:
            await self.load()
            self.active = True
            self._task = asyncio.create_task(self._loop(), name="bossman:managed")

    async def stop(self) -> None:
        self.active = False
        if self._task:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._task = None
        await asyncio.gather(*(self._stop(mid, reason="supervisor stepping down") for mid in list(self._procs)),
                             return_exceptions=True)

    async def _loop(self) -> None:
        while True:
            try:
                await self.reconcile()
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001
                log.exception("managed reconcile failed")
            await asyncio.sleep(1.0)

    async def reconcile(self) -> None:
        async with self._lock:
            now = time.time()
            for mid, spec in list(self._specs.items()):
                st = self._procs.setdefault(mid, ProcState())
                if spec.desired_state != "running" or st.state in ("running", "starting", "stopping", "crashloop"):
                    continue
                if now < st.next_start_at:
                    st.state = "backoff"
                    continue
                await self._launch(spec, st)

    # ── Process control ──────────────────────────────────────────────────────

    def _command(self, spec: ManagedSpec, env: dict[str, str]) -> list[str]:
        if spec.runtime == "docker":
            docker = shutil.which("docker") or "docker"
            cmd = [docker, "run", "--rm", "--name", f"bossman-{spec.managed_id}"]
            for k, v in env.items():
                if k.startswith("BOSSMAN_") or k in spec.env:
                    cmd += ["-e", f"{k}={v}"]
            return cmd + [spec.image or ""] + list(spec.args)
        cmd = list(spec.command) + list(spec.args)
        if cmd and cmd[0] in ("python", "python3"):
            cmd[0] = sys.executable  # same interpreter as the server, predictable
        return cmd

    async def _launch(self, spec: ManagedSpec, st: ProcState) -> None:
        env = {**os.environ, **spec.env,
               "BOSSMAN_URL": self._base_url,
               "BOSSMAN_MANAGED_ID": spec.managed_id,
               "BOSSMAN_AGENT_NAME": spec.name,
               "BOSSMAN_NAMESPACE": spec.namespace,
               "BOSSMAN_ROLE": spec.role,
               "PYTHONUNBUFFERED": "1"}
        if spec.supervisor:
            env["BOSSMAN_SUPERVISOR"] = spec.supervisor
        token = self._agent_token or os.getenv("BOSSMAN_AGENT_TOKEN")
        if token:
            env["BOSSMAN_API_KEY"] = token
        for secret_var in ("BOSSMAN_ADMIN_TOKEN", "BOSSMAN_OPERATOR_TOKEN", "BOSSMAN_SECRET_KEY", "DATABASE_URL"):
            if secret_var not in spec.env:
                env.pop(secret_var, None)  # never leak control-plane secrets into agents
        st.state = "starting"
        st.stopping = False
        st.exit_code = None
        try:
            kwargs: dict[str, Any] = {}
            if os.name == "nt":
                kwargs["creationflags"] = 0x00000200  # CREATE_NEW_PROCESS_GROUP
            else:
                kwargs["start_new_session"] = True
            st.proc = await asyncio.create_subprocess_exec(
                *self._command(spec, env), cwd=spec.cwd or None, env=env,
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT, **kwargs)
        except (OSError, ValueError) as exc:
            st.state = "backoff"
            st.logs.append(f"[bossman] failed to start: {exc}")
            await self._after_exit(spec, st, code=-1, reason=f"spawn failed: {exc}")
            return
        st.pid = st.proc.pid
        st.started_at = time.time()
        st.state = "running"
        st.watcher = asyncio.create_task(self._watch(spec, st, st.proc), name=f"managed:{spec.name}")
        await self._emit(EventType.MANAGED_STARTED, spec, f"Started {spec.name} (pid {st.pid})",
                         {"pid": st.pid, "runtime": spec.runtime})

    async def _watch(self, spec: ManagedSpec, st: ProcState, proc: asyncio.subprocess.Process) -> None:
        assert proc.stdout is not None
        try:
            async for raw in proc.stdout:
                st.logs.append(raw.decode(errors="replace").rstrip()[:2000])
        except Exception:  # noqa: BLE001
            pass
        code = await proc.wait()
        if st.proc is proc:
            st.exit_code = code
            st.proc = None
            st.pid = None
            if st.stopping:
                st.state = "stopped"
                await self._emit(EventType.MANAGED_STOPPED, spec, f"{spec.name} stopped (exit {code})",
                                 {"exit_code": code})
            else:
                await self._after_exit(spec, st, code=code, reason=f"exited with code {code}")

    async def _after_exit(self, spec: ManagedSpec, st: ProcState, *, code: int, reason: str) -> None:
        await self._emit(EventType.MANAGED_EXITED, spec, f"{spec.name} {reason}", {"exit_code": code})
        if spec.desired_state != "running" or spec.restart_policy == "never" or (
            spec.restart_policy == "on-failure" and code == 0
        ):
            st.state = "stopped"
            return
        now = time.monotonic()
        while st.restarts and now - st.restarts[0] > spec.window_s:
            st.restarts.popleft()
        if len(st.restarts) >= spec.max_restarts:
            st.state = "crashloop"
            await self._emit(EventType.MANAGED_CRASHLOOP, spec,
                             f"{spec.name} crashed {len(st.restarts)}× in {spec.window_s:.0f}s — not restarting",
                             {"restarts": len(st.restarts)}, severity=EventSeverity.CRITICAL)
            await self._bus.publish(BossmanEvent.create(
                EventType.HUMAN_GATE_REQUIRED, namespace=spec.namespace, severity=EventSeverity.CRITICAL,
                message=f"Managed agent {spec.name} is crash-looping; human intervention required",
                payload={"managed_id": spec.managed_id, "last_logs": list(st.logs)[-20:]}))
            return
        st.restarts.append(now)
        st.total_restarts += 1
        delay = random.uniform(0, min(60.0, 1.0 * 2 ** len(st.restarts)))
        st.next_start_at = time.time() + delay
        st.state = "backoff"
        st.logs.append(f"[bossman] restarting in {delay:.1f}s (restart {len(st.restarts)}/{spec.max_restarts})")

    async def _stop(self, managed_id: str, *, reason: str) -> None:
        st = self._procs.get(managed_id)
        spec = self._specs.get(managed_id)
        if st is None or st.proc is None or spec is None:
            return
        proc = st.proc
        st.stopping = True
        st.state = "stopping"
        grace = max(0.5, spec.graceful_timeout_s)
        # 1. soft: ask the agent to shut down (≈ SIGUSR1)
        if self._directives is not None and self._registry is not None:
            for agent in await self._registry.find_managed(managed_id):
                if agent.is_alive:
                    await self._directives.issue(agent.agent_id, DirectiveType.SHUTDOWN, {"reason": reason},
                                                 reason=reason, namespace=spec.namespace)
            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(proc.wait(), timeout=grace / 2)
        # 2. SIGTERM / docker stop
        if proc.returncode is None:
            if spec.runtime == "docker":
                with contextlib.suppress(Exception):
                    p = await asyncio.create_subprocess_exec(
                        shutil.which("docker") or "docker", "stop", "-t", str(int(grace)),
                        f"bossman-{managed_id}", stdout=asyncio.subprocess.DEVNULL,
                        stderr=asyncio.subprocess.DEVNULL)
                    await asyncio.wait_for(p.wait(), timeout=grace + 5)
            else:
                with contextlib.suppress(ProcessLookupError):
                    if os.name == "nt":
                        proc.terminate()
                    else:
                        os.killpg(proc.pid, signal.SIGTERM)
            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(proc.wait(), timeout=grace / 2)
        # 3. SIGKILL
        if proc.returncode is None:
            with contextlib.suppress(ProcessLookupError):
                if os.name == "nt":
                    proc.kill()
                else:
                    os.killpg(proc.pid, signal.SIGKILL)
            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(proc.wait(), timeout=5)
        if st.watcher:
            with contextlib.suppress(Exception):
                await asyncio.wait_for(st.watcher, timeout=5)

    async def restart(self, managed_id: str, reason: str = "restart requested") -> bool:
        spec = self._specs.get(managed_id)
        if spec is None:
            return False
        st = self._procs.setdefault(managed_id, ProcState())
        await self._stop(managed_id, reason=reason)
        if st.state == "crashloop":
            st.restarts.clear()
        st.state = "stopped"
        st.next_start_at = 0.0
        spec.desired_state = "running"
        if self.active:
            await self.reconcile()
        return True

    async def restart_agent(self, agent: AgentState, reason: str) -> bool:
        """Restarter hook for RecoveryEngine: True if this agent is managed here."""
        mid = agent.info.managed_id
        if not mid or mid not in self._specs or not self.active:
            return False
        return await self.restart(mid, reason)

    # ── Views ────────────────────────────────────────────────────────────────

    def status(self) -> list[dict[str, Any]]:
        out = []
        for mid, spec in sorted(self._specs.items(), key=lambda kv: kv[1].name):
            st = self._procs.get(mid) or ProcState()
            out.append({
                **asdict(spec), "env": {k: "…" for k in spec.env},
                "state": st.state if self.active else "inactive (not leader)",
                "pid": st.pid, "started_at": st.started_at, "exit_code": st.exit_code,
                "restarts_in_window": len(st.restarts), "total_restarts": st.total_restarts,
            })
        return out

    def logs(self, managed_id: str, limit: int = 200) -> list[str]:
        st = self._procs.get(managed_id)
        return list(st.logs)[-limit:] if st else []

    async def _emit(self, et: EventType, spec: ManagedSpec, message: str, payload: dict[str, Any],
                    severity: EventSeverity | None = None) -> None:
        await self._bus.publish(BossmanEvent.create(
            et, namespace=spec.namespace, message=message, severity=severity,
            payload={"managed_id": spec.managed_id, "name": spec.name, **payload}))


class KubernetesRestarter:
    """
    Restart agents that run as Kubernetes pods by deleting the pod (its
    Deployment recreates it). The SDK labels the agent with k8s.pod /
    k8s.namespace from the downward API (POD_NAME / POD_NAMESPACE).
    Enabled with BOSSMAN_K8S_RESTART=1 inside a cluster.
    """

    TOKEN = "/var/run/secrets/kubernetes.io/serviceaccount/token"
    CA = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"

    def __init__(self, transport: httpx.AsyncBaseTransport | None = None) -> None:
        self._transport = transport
        host = os.getenv("KUBERNETES_SERVICE_HOST")
        port = os.getenv("KUBERNETES_SERVICE_PORT", "443")
        self.base = f"https://{host}:{port}" if host else None

    @property
    def enabled(self) -> bool:
        return bool(os.getenv("BOSSMAN_K8S_RESTART") == "1" and (self.base or self._transport))

    async def restart_agent(self, agent: AgentState, reason: str) -> bool:
        pod = agent.info.labels.get("k8s.pod")
        ns = agent.info.labels.get("k8s.namespace")
        if not (self.enabled and pod and ns):
            return False
        headers = {}
        if os.path.exists(self.TOKEN):
            with open(self.TOKEN, encoding="utf-8") as fh:
                headers["Authorization"] = f"Bearer {fh.read().strip()}"
        verify: Any = self.CA if os.path.exists(self.CA) else True
        async with httpx.AsyncClient(base_url=self.base or "https://kubernetes.default.svc", headers=headers,
                                     verify=verify, timeout=10, transport=self._transport) as c:
            r = await c.delete(f"/api/v1/namespaces/{ns}/pods/{pod}",
                               params={"gracePeriodSeconds": 10})
        if r.status_code in (200, 202, 404):
            log.warning("deleted pod %s/%s to restart agent %s (%s)", ns, pod, agent.agent_id, reason)
            return True
        log.error("k8s pod restart failed: %s %s", r.status_code, r.text[:200])
        return False
