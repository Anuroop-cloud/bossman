"""
bossmand — the BOSSman daemon.

    bossmand serve [--host 0.0.0.0] [--port 7070] [--workers 1]
    bossmand migrate                      apply database migrations (DATABASE_URL)
    bossmand keys create --role agent --namespace default [--max-tier WRITE] [--name ci]
    bossmand keys list | keys revoke <key_id>
    bossmand check                        validate configuration and connectivity
    bossmand version

Key management talks to the database directly (for bootstrapping before any
admin key exists); everything else is configured through the API.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys

from bossman_server import __version__


def _serve(args: argparse.Namespace) -> int:
    import uvicorn

    uvicorn.run(
        "bossman_server.api.app:app",
        host=args.host,
        port=args.port,
        workers=1,  # one process per node; scale out with more nodes (leader election)
        log_level=args.log_level,
        proxy_headers=True,
        forwarded_allow_ips=os.getenv("FORWARDED_ALLOW_IPS", "127.0.0.1"),
    )
    return 0


def _migrate(_: argparse.Namespace) -> int:
    from bossman_server.infra.migrate import current_async, upgrade

    url = os.getenv("DATABASE_URL")
    if not url:
        print("DATABASE_URL is not set", file=sys.stderr)
        return 2
    upgrade(url)
    print(f"database at revision {asyncio.run(current_async(url))}")
    return 0


async def _keys(args: argparse.Namespace) -> int:
    from bossman_server.contracts.agent_state import PermissionTier
    from bossman_server.control.auth import KeyService, Role
    from bossman_server.core.event_bus import EventBus
    from bossman_server.infra.db import Database
    from bossman_server.infra.sql_stores import SqlDocumentStore

    url = os.getenv("DATABASE_URL")
    if not url or os.getenv("BOSSMAN_PERSISTENCE", "sql" if os.getenv("BOSSMAN_MODE") == "distributed"
                                                   else "memory") != "sql":
        print("Key management needs persistent storage: set BOSSMAN_PERSISTENCE=sql and DATABASE_URL.\n"
              "(With in-memory persistence, use BOSSMAN_*_TOKEN environment variables or POST /api/keys.)",
              file=sys.stderr)
        return 2
    db = Database(url)
    try:
        await db.migrate()
        svc = KeyService(SqlDocumentStore(db), EventBus())
        if args.keys_cmd == "create":
            token, rec = await svc.create(
                name=args.name or f"{args.role}-key", role=Role(args.role),
                namespaces=args.namespace or None, max_tier=PermissionTier(args.max_tier.upper()),
                created_by="bossmand-cli", ttl_days=args.ttl_days)
            print(json.dumps({"key": token, "record": rec}, indent=2, default=str))
            print("\nStore this key now — it is never shown again.", file=sys.stderr)
        elif args.keys_cmd == "list":
            print(json.dumps(await svc.list(), indent=2, default=str))
        elif args.keys_cmd == "revoke":
            ok = await svc.revoke(args.key_id, by="bossmand-cli")
            print("revoked" if ok else "not found / already revoked")
            return 0 if ok else 1
        return 0
    finally:
        await db.close()


async def _check(_: argparse.Namespace) -> int:
    from bossman_server.infra.settings import InfraSettings

    s = InfraSettings.from_env()
    report: dict[str, object] = {"mode": s.mode, "persistence": s.persistence, "node_id": s.node_id}
    ok = True
    if s.sql:
        from bossman_server.infra.db import Database

        db = Database(s.database_url)
        report["database"] = await db.ping()
        ok &= bool(report["database"])
        await db.close()
    if s.distributed:
        from bossman_server.infra.backend import RedisBackend

        be = RedisBackend(s.redis_url)
        report["redis"] = await be.ping()
        ok &= bool(report["redis"])
        await be.close()
    report["auth"] = {
        "admin_token": bool(os.getenv("BOSSMAN_ADMIN_TOKEN")),
        "agent_token": bool(os.getenv("BOSSMAN_AGENT_TOKEN")),
        "secret_key": bool(os.getenv("BOSSMAN_SECRET_KEY")),
    }
    if os.getenv("BOSSMAN_ENV") == "production" and not os.getenv("BOSSMAN_SECRET_KEY"):
        report["warning"] = "BOSSMAN_SECRET_KEY is unset: stored secrets will not survive restarts"
    print(json.dumps(report, indent=2))
    return 0 if ok else 1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="bossmand", description="BOSSman control-plane daemon")
    sub = parser.add_subparsers(dest="cmd", required=True)

    sp = sub.add_parser("serve", help="run the API server")
    sp.add_argument("--host", default=os.getenv("API_HOST_BIND", "127.0.0.1"))
    sp.add_argument("--port", type=int, default=int(os.getenv("API_PORT", "7070")))
    sp.add_argument("--log-level", default=os.getenv("BOSSMAN_LOG_LEVEL", "info").lower())

    sub.add_parser("migrate", help="apply database migrations")
    sub.add_parser("check", help="validate configuration and connectivity")
    sub.add_parser("version", help="print the version")

    kp = sub.add_parser("keys", help="manage API keys (direct database access)")
    ksub = kp.add_subparsers(dest="keys_cmd", required=True)
    kc = ksub.add_parser("create")
    kc.add_argument("--role", required=True, choices=["viewer", "agent", "operator", "admin"])
    kc.add_argument("--namespace", action="append", help="repeatable; '*' for all")
    kc.add_argument("--max-tier", default="WRITE")
    kc.add_argument("--name")
    kc.add_argument("--ttl-days", type=float)
    ksub.add_parser("list")
    kr = ksub.add_parser("revoke")
    kr.add_argument("key_id")

    args = parser.parse_args(argv)
    if args.cmd == "serve":
        return _serve(args)
    if args.cmd == "migrate":
        return _migrate(args)
    if args.cmd == "keys":
        return asyncio.run(_keys(args))
    if args.cmd == "check":
        return asyncio.run(_check(args))
    print(__version__)
    return 0


if __name__ == "__main__":
    sys.exit(main())
