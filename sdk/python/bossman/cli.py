"""
bossman — command-line client for a BOSSman control plane.

Connection: --url / BOSSMAN_URL (default http://127.0.0.1:7070)
Credential: --token / BOSSMAN_TOKEN / BOSSMAN_API_KEY

  bossman status                                   cluster + fleet summary
  bossman doctor                                   connectivity / auth check
  bossman up | down                                docker compose up/down (in BOSSMAN_HOME or .)
  bossman agents [--namespace ns]                  list agents
  bossman agent show|pause|resume|compact|abort|restart|shutdown|release AGENT
  bossman tasks [--status S]                       list tasks
  bossman task show|cancel ID
  bossman task submit TITLE [--role R] [--input JSON] [--wait] [--schema FILE]
  bossman dlq [replay|discard ID]
  bossman incidents | incident show|resolve ID
  bossman approvals [--all] | approve ID | deny ID [--reason …]
  bossman flags | flag set KEY on|off [--namespace --role --agent --rollout N] | flag rm RULE_ID
  bossman keys | keys create --role R [--namespace ns]… [--max-tier T] [--name N] | keys revoke ID
  bossman secrets | secret set NAME (--value V | --from-env VAR) --host H… | secret rm NAME
  bossman policy show | policy apply -f FILE | policy test AGENT ACTION [--tool T] [--irreversible]
  bossman managed | managed create -f SPEC.yaml | managed start|stop|restart|logs|rm ID
  bossman supervisors | supervisor set NAME --strategy S [--max-restarts N] [--window S] [--parent P]
  bossman limits | limit set NAME [--concurrency N] [--rate R] [--burst B]
  bossman circuits | circuit open|close KEY
  bossman events [--follow] [--agent ID] [--type T]
  bossman trace TRACE_ID
  bossman audit [--kind K]
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

from bossman import errors
from bossman._version import __version__
from bossman.client import Bossman


def _table(rows: list[dict[str, Any]], cols: list[str]) -> str:
    if not rows:
        return "(none)"
    cells = [[str(r.get(c, "") if r.get(c) is not None else "—")[:48] for c in cols] for r in rows]
    widths = [max(len(c), *(len(row[i]) for row in cells)) for i, c in enumerate(cols)]
    line = "  ".join(c.upper().ljust(w) for c, w in zip(cols, widths))
    return "\n".join([line] + ["  ".join(v.ljust(w) for v, w in zip(row, widths)) for row in cells])


def _load(path: str) -> Any:
    text = Path(path).read_text(encoding="utf-8")
    if path.endswith((".yaml", ".yml")):
        try:
            import yaml
        except ImportError:
            raise SystemExit("pip install pyyaml to read YAML files") from None
        return yaml.safe_load(text)
    return json.loads(text)


class CLI:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.bm = Bossman(args.url, api_key=args.token)

    def out(self, data: Any, cols: list[str] | None = None, key: str | None = None) -> None:
        if self.args.json or cols is None:
            print(json.dumps(data, indent=2, default=str))
            return
        rows = data[key] if key else data
        print(_table(rows, cols))

    # ── commands ─────────────────────────────────────────────────────────────

    async def status(self) -> None:
        health = await self.bm.get("/api/health")
        cluster = await self.bm.get("/api/cluster")
        ov = await self.bm.get("/api/overview")
        if self.args.json:
            self.out({"health": health, "cluster": cluster, "overview": ov})
            return
        print(f"BOSSman {ov['status']}  ·  runtime {health['runtime']}  ·  mode {cluster['mode']}"
              f"  ·  node {cluster['node_id']}{' (leader)' if cluster['is_leader'] else ''}"
              f"  ·  persistence {cluster.get('persistence')}")
        a, t, i = ov["agents"], ov["tasks"], ov["incidents"]
        print(f"agents     {a['total']} total  {a['by_status']}")
        print(f"tasks      running {t['running']}  queued {t['queued']}  dlq {t['dlq']}  completed {t['completed']}")
        print(f"incidents  active {i['active']}  critical {i['critical']}  resolved {i['resolved']}")
        g = ov.get("governance", {})
        print(f"approvals  pending {g.get('approvals_pending', 0)}  ·  circuits open {g.get('circuits_open', 0)}"
              f"  ·  managed {g.get('managed', 0)}  ·  leases {g.get('leases', 0)}")

    async def doctor(self) -> None:
        print(f"bossman CLI {__version__}  →  {self.bm.url}")
        try:
            h = await self.bm.get("/api/health", retries=0)
            print(f"  reachable          ok ({h['runtime']})")
        except errors.BossmanError as exc:
            print(f"  reachable          FAILED: {exc}")
            raise SystemExit(1) from None
        s = await self.bm.get("/api/session")
        print(f"  credential         {s['role']}" + (f" ({s['name']})" if s.get("name") else "")
              + ("" if self.bm.api_key else "  — no token set (BOSSMAN_TOKEN)"))
        print(f"  operator actions   {'enabled' if s['actions_enabled'] else 'disabled (no operator/admin credential configured)'}")
        sysv = await self.bm.get("/api/system")
        db = sysv["database"]
        print(f"  persistence        {'sql' if db['configured'] else 'memory'}"
              + (f" ({'connected' if db['connected'] else 'DISCONNECTED'})" if db['configured'] else ""))
        print(f"  llm diagnostics    {'on' if sysv['llm'].get('enabled') else 'off'}")

    async def up(self, down: bool = False) -> None:
        home = Path(os.getenv("BOSSMAN_HOME", "."))
        if not (home / "docker-compose.yml").exists():
            raise SystemExit(f"no docker-compose.yml in {home.resolve()} (set BOSSMAN_HOME to your bossman checkout)")
        docker = shutil.which("docker")
        if not docker:
            raise SystemExit("docker is not installed")
        cmd = [docker, "compose", "down"] if down else [docker, "compose", "up", "-d", "--wait"]
        raise SystemExit(subprocess.call(cmd, cwd=home))

    async def agents(self) -> None:
        data = await self.bm.get("/api/agents", params={"namespace": self.args.namespace})
        self.out(data, ["agent_id", "name", "role", "status", "permission_tier", "heartbeat_age_s",
                        "current_task_id", "context_utilisation"], "agents")

    async def agent(self) -> None:
        a, aid = self.args.action, self.args.id
        if a == "show":
            return self.out(await self.bm.get(f"/api/agents/{aid}"))
        if a == "restart":
            return self.out(await self.bm.post(f"/api/agents/{aid}/restart", {"reason": self.args.reason}))
        if a == "release":
            return self.out(await self.bm.post(f"/api/agents/{aid}/release-resources", {"reason": self.args.reason}))
        dtype = {"pause": "PAUSE", "resume": "RESUME", "compact": "COMPACT_CONTEXT", "abort": "ABORT_TASK",
                 "shutdown": "SHUTDOWN", "fallback": "FALLBACK_MODEL", "restore": "RESTORE_MODEL"}[a]
        d = await self.bm.post(f"/api/agents/{aid}/directives", {"type": dtype, "reason": self.args.reason})
        print(f"{dtype} → {aid} ({d['directive_id']})")

    async def tasks(self) -> None:
        data = await self.bm.get("/api/tasks")
        rows = [t for t in data["tasks"] if not self.args.status or t["status"] == self.args.status.upper()]
        self.out(rows, ["task_id", "title", "status", "priority", "assigned_to", "attempt_count", "required_role"])

    async def task(self) -> None:
        a = self.args.action
        if a == "show":
            return self.out(await self.bm.get(f"/api/tasks/{self.args.target}"))
        if a == "cancel":
            return self.out(await self.bm.post(f"/api/tasks/{self.args.target}/cancel", {"reason": self.args.reason}))
        body: dict[str, Any] = {"title": self.args.target, "required_role": self.args.role,
                                "input": json.loads(self.args.input) if self.args.input else {},
                                "priority": self.args.priority, "max_attempts": self.args.max_attempts,
                                "namespace": self.args.namespace}
        if self.args.schema:
            body["output_schema"] = _load(self.args.schema)
        t = await self.bm.post("/api/tasks", {k: v for k, v in body.items() if v is not None})
        print(f"submitted {t['task_id']} (trace {t['trace_id']})", file=sys.stderr)
        if self.args.wait:
            # poll via the dashboard API (operator credentials need not be agent keys)
            while True:
                cur = (await self.bm.get(f"/api/tasks/{t['task_id']}"))["task"]
                if cur["status"] in ("COMPLETED", "DEAD_LETTER", "QUARANTINED", "CANCELLED"):
                    return self.out(cur)
                await asyncio.sleep(1.0)
        self.out(t)

    async def dlq(self) -> None:
        if self.args.action == "replay":
            return self.out(await self.bm.post(f"/api/tasks/{self.args.id}/replay", {"reason": self.args.reason}))
        if self.args.action == "discard":
            return self.out(await self.bm.post(f"/api/tasks/{self.args.id}/discard", {"reason": self.args.reason}))
        self.out(await self.bm.get("/api/tasks"), ["task_id", "title", "status", "dlq_reason", "attempt_count"], "dlq")

    async def incidents(self) -> None:
        self.out(await self.bm.get("/api/incidents"),
                 ["incident_id", "failure_type", "agent_id", "state", "severity", "recovery_strategy"], "incidents")

    async def incident(self) -> None:
        if self.args.action == "resolve":
            return self.out(await self.bm.post(f"/api/incidents/{self.args.id}/resolve", {"reason": self.args.reason}))
        self.out(await self.bm.get(f"/api/incidents/{self.args.id}"))

    async def approvals(self) -> None:
        data = await self.bm.get("/api/approvals", params={"status": None if self.args.all else "pending"})
        rows = [{**a, "action": a["request"].get("action"), "tool": a["request"].get("tool"),
                 "votes": f"{len(a['approvals'])}/{a['required']}"} for a in data["approvals"]]
        self.out(rows, ["approval_id", "agent_name", "action", "tool", "status", "votes"])

    async def decide(self, approve: bool) -> None:
        verb = "approve" if approve else "deny"
        self.out(await self.bm.post(f"/api/approvals/{self.args.id}/{verb}", {"reason": self.args.reason}))

    async def flags(self) -> None:
        self.out(await self.bm.get("/api/flags"),
                 ["rule_id", "key", "enabled", "namespace", "role", "agent_id", "rollout_percent"], "rules")

    async def flag(self) -> None:
        if self.args.action == "rm":
            return self.out(await self.bm.delete(f"/api/flags/rules/{self.args.key}"))
        body = {"enabled": self.args.state == "on", "namespace": self.args.namespace, "role": self.args.role,
                "agent_id": self.args.agent, "rollout_percent": self.args.rollout, "note": self.args.note or ""}
        self.out(await self.bm.put(f"/api/flags/{self.args.key}", {k: v for k, v in body.items() if v is not None}))

    async def keys(self) -> None:
        a = self.args.action
        if a == "create":
            res = await self.bm.post("/api/keys", {"name": self.args.name or f"{self.args.role}-key",
                                                    "role": self.args.role, "namespaces": self.args.namespace or [],
                                                    "max_tier": self.args.max_tier.upper(),
                                                    "ttl_days": self.args.ttl_days})
            print(res["key"])
            print("Store this key now — it is never shown again.", file=sys.stderr)
            return None
        if a == "revoke":
            return self.out(await self.bm.post(f"/api/keys/{self.args.id}/revoke"))
        self.out(await self.bm.get("/api/keys"),
                 ["key_id", "name", "role", "namespaces", "max_tier", "revoked", "prefix"], "keys")

    async def secrets(self) -> None:
        a = self.args.action
        if a == "set":
            value = self.args.value or (os.getenv(self.args.from_env) if self.args.from_env else None)
            if not value:
                raise SystemExit("provide --value or --from-env VAR")
            body = {"value": value, "allowed_hosts": self.args.host or [], "allowed_tools": self.args.tool or [],
                    "namespace": self.args.namespace or "*", "release": self.args.release,
                    "min_tier": self.args.min_tier.upper()}
            return self.out(await self.bm.put(f"/api/secrets/{self.args.name}", body))
        if a == "rm":
            return self.out(await self.bm.delete(f"/api/secrets/{self.args.name}"))
        self.out(await self.bm.get("/api/secrets"),
                 ["name", "namespace", "release", "allowed_hosts", "allowed_tools", "min_tier"], "secrets")

    async def policy(self) -> None:
        a = self.args.action
        if a == "apply":
            text = Path(self.args.file).read_text(encoding="utf-8")
            return self.out(await self.bm.put("/api/policies", {"yaml": text}))
        if a == "test":
            if not (self.args.agent and self.args.target_action):
                raise SystemExit("usage: bossman policy test AGENT_ID ACTION [--tool T] [--irreversible]")
            req = {"action": self.args.target_action, "tool": self.args.tool,
                   "irreversible": True if self.args.irreversible else None,
                   "justification": self.args.justification}
            return self.out(await self.bm.post("/api/policies/evaluate",
                                               {"agent_id": self.args.agent, "request": req}))
        data = await self.bm.get("/api/policies")
        if self.args.json:
            return self.out(data)
        print(f"# revision {data['revision']}\n{data['yaml']}")

    async def managed(self) -> None:
        a = self.args.action
        if a == "create":
            return self.out(await self.bm.post("/api/managed", _load(self.args.file)))
        if a in ("start", "stop", "restart"):
            return self.out(await self.bm.post(f"/api/managed/{self.args.id}/{a}", {"reason": self.args.reason}))
        if a == "logs":
            print("\n".join((await self.bm.get(f"/api/managed/{self.args.id}/logs"))["lines"]))
            return None
        if a == "rm":
            return self.out(await self.bm.delete(f"/api/managed/{self.args.id}"))
        self.out(await self.bm.get("/api/managed"),
                 ["managed_id", "name", "runtime", "state", "pid", "restarts_in_window", "desired_state"], "managed")

    async def supervisors(self) -> None:
        if self.args.action == "set":
            body = {"strategy": self.args.strategy, "max_restarts": self.args.max_restarts,
                    "window_s": self.args.window, "parent": self.args.parent}
            return self.out(await self.bm.put(f"/api/supervisors/{self.args.name}",
                                              {k: v for k, v in body.items() if v is not None}))
        self.out(await self.bm.get("/api/supervisors"),
                 ["name", "strategy", "max_restarts", "window_s", "parent", "restarts_in_window", "members"],
                 "supervisors")

    async def limits(self) -> None:
        if self.args.action == "set":
            body = {"max_concurrent": self.args.concurrency, "rate_per_s": self.args.rate, "burst": self.args.burst}
            return self.out(await self.bm.put(f"/api/limits/{self.args.name}",
                                              {k: v for k, v in body.items() if v is not None}))
        self.out(await self.bm.get("/api/limits"),
                 ["name", "max_concurrent", "rate_per_s", "burst", "in_use", "tokens_available"], "limits")

    async def circuits(self) -> None:
        if self.args.action in ("open", "close"):
            return self.out(await self.bm.post(f"/api/circuits/{self.args.key}/{self.args.action}",
                                               {"reason": self.args.reason} if self.args.action == "open" else None))
        self.out(await self.bm.get("/api/circuits"), ["key", "state", "failures", "retry_after_s", "reason"], "shared")

    async def events(self) -> None:
        since = 0
        first = True
        while True:
            data = await self.bm.get("/api/events", params={"since_seq": since, "limit": 200,
                                                            "agent_id": self.args.agent,
                                                            "event_type": self.args.type})
            for e in reversed(data["events"]):
                if self.args.json:
                    print(json.dumps(e, default=str))
                else:
                    print(f"{e['occurred_at'][11:19]}  {e['severity'][:4]:4}  {e['event_type']:26}  {e['message']}")
            since = max(since, data["last_seq"]) if not first or self.args.follow else since
            first = False
            if not self.args.follow:
                return
            await asyncio.sleep(1.0)

    async def trace(self) -> None:
        data = await self.bm.get(f"/api/traces/{self.args.id}")
        if self.args.json:
            return self.out(data)
        print(_table(data["tasks"], ["task_id", "title", "status", "assigned_to", "parent_task_id"]))
        print()
        for e in data["events"]:
            print(f"{e['occurred_at'][11:23]}  {e['event_type']:24}  {e['message']}")
        return None

    async def audit(self) -> None:
        self.out(await self.bm.get("/api/audit", params={"kind": self.args.kind}),
                 ["audit_id", "kind", "actor", "agent_id", "data"], "entries")


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="bossman", description="BOSSman control-plane CLI")
    p.add_argument("--url", default=None)
    p.add_argument("--token", default=os.getenv("BOSSMAN_TOKEN"))
    p.add_argument("--json", action="store_true", help="JSON output")
    p.add_argument("--version", action="version", version=f"bossman {__version__}")
    sub = p.add_subparsers(dest="cmd", required=True)

    sub.add_parser("status")
    sub.add_parser("doctor")
    sub.add_parser("up")
    sub.add_parser("down")
    s = sub.add_parser("agents")
    s.add_argument("--namespace")
    s = sub.add_parser("agent")
    s.add_argument("action", choices=["show", "pause", "resume", "compact", "abort", "restart", "shutdown",
                                      "release", "fallback", "restore"])
    s.add_argument("id")
    s.add_argument("--reason", default="")
    s = sub.add_parser("tasks")
    s.add_argument("--status")
    s = sub.add_parser("task")
    s.add_argument("action", choices=["show", "cancel", "submit"])
    s.add_argument("target", help="task id (show/cancel) or title (submit)")
    s.add_argument("--role")
    s.add_argument("--input")
    s.add_argument("--schema")
    s.add_argument("--priority", default="NORMAL")
    s.add_argument("--max-attempts", type=int, default=3)
    s.add_argument("--namespace")
    s.add_argument("--wait", action="store_true")
    s.add_argument("--reason", default="")
    s = sub.add_parser("dlq")
    s.add_argument("action", nargs="?", choices=["ls", "replay", "discard"], default="ls")
    s.add_argument("id", nargs="?")
    s.add_argument("--reason", default="")
    sub.add_parser("incidents")
    s = sub.add_parser("incident")
    s.add_argument("action", choices=["show", "resolve"])
    s.add_argument("id")
    s.add_argument("--reason", default="")
    s = sub.add_parser("approvals")
    s.add_argument("--all", action="store_true")
    for verb in ("approve", "deny"):
        s = sub.add_parser(verb)
        s.add_argument("id")
        s.add_argument("--reason", default="")
    sub.add_parser("flags")
    s = sub.add_parser("flag")
    s.add_argument("action", choices=["set", "rm"])
    s.add_argument("key")
    s.add_argument("state", nargs="?", choices=["on", "off"], default="off")
    s.add_argument("--namespace")
    s.add_argument("--role")
    s.add_argument("--agent")
    s.add_argument("--rollout", type=int, default=100)
    s.add_argument("--note")
    s = sub.add_parser("keys")
    s.add_argument("action", nargs="?", choices=["ls", "create", "revoke"], default="ls")
    s.add_argument("id", nargs="?")
    s.add_argument("--role", default="agent")
    s.add_argument("--namespace", action="append")
    s.add_argument("--max-tier", default="WRITE")
    s.add_argument("--name")
    s.add_argument("--ttl-days", type=float)
    s = sub.add_parser("secrets")
    s.add_argument("action", nargs="?", choices=["ls", "set", "rm"], default="ls")
    s.add_argument("name", nargs="?")
    s.add_argument("--value")
    s.add_argument("--from-env")
    s.add_argument("--host", action="append")
    s.add_argument("--tool", action="append")
    s.add_argument("--namespace")
    s.add_argument("--release", choices=["proxy", "direct"], default="proxy")
    s.add_argument("--min-tier", default="READ_ONLY")
    s = sub.add_parser("policy")
    s.add_argument("action", nargs="?", choices=["show", "apply", "test"], default="show")
    s.add_argument("agent", nargs="?")
    s.add_argument("target_action", nargs="?")
    s.add_argument("-f", "--file")
    s.add_argument("--tool")
    s.add_argument("--irreversible", action="store_true")
    s.add_argument("--justification")
    s = sub.add_parser("managed")
    s.add_argument("action", nargs="?", choices=["ls", "create", "start", "stop", "restart", "logs", "rm"],
                   default="ls")
    s.add_argument("id", nargs="?")
    s.add_argument("-f", "--file")
    s.add_argument("--reason", default="")
    s = sub.add_parser("supervisors")
    s.add_argument("action", nargs="?", choices=["ls", "set"], default="ls")
    s.add_argument("name", nargs="?")
    s.add_argument("--strategy", choices=["one_for_one", "one_for_all", "rest_for_one"])
    s.add_argument("--max-restarts", type=int)
    s.add_argument("--window", type=float)
    s.add_argument("--parent")
    s = sub.add_parser("limits")
    s.add_argument("action", nargs="?", choices=["ls", "set"], default="ls")
    s.add_argument("name", nargs="?")
    s.add_argument("--concurrency", type=int)
    s.add_argument("--rate", type=float)
    s.add_argument("--burst", type=float)
    s = sub.add_parser("circuits")
    s.add_argument("action", nargs="?", choices=["ls", "open", "close"], default="ls")
    s.add_argument("key", nargs="?")
    s.add_argument("--reason", default="")
    s = sub.add_parser("events")
    s.add_argument("--follow", "-f", action="store_true")
    s.add_argument("--agent")
    s.add_argument("--type")
    s = sub.add_parser("trace")
    s.add_argument("id")
    s = sub.add_parser("audit")
    s.add_argument("--kind")
    return p


async def _dispatch(args: argparse.Namespace) -> None:
    cli = CLI(args)
    try:
        c = args.cmd
        if c in ("up", "down"):
            await cli.up(down=c == "down")
        elif c in ("approve", "deny"):
            await cli.decide(c == "approve")
        elif c == "secrets":
            await cli.secrets()
        else:
            await getattr(cli, c)()
    finally:
        await cli.bm.close()


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        asyncio.run(_dispatch(args))
    except errors.BossmanError as exc:
        print(f"error ({exc.status or 'network'} {exc.code}): {exc}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        return 130
    return 0


if __name__ == "__main__":
    sys.exit(main())
