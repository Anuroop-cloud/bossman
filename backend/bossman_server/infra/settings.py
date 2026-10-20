"""
infra.settings
──────────────
Deployment mode, resolved from the environment.

  BOSSMAN_MODE=standalone   (default) single process; coordination in memory.
  BOSSMAN_MODE=distributed  Redis (coordination/events/registry/locks) +
                            PostgreSQL (tasks, ledger, documents, events) +
                            leader election. Run as many nodes as you like.

  BOSSMAN_PERSISTENCE=memory | sql
                            Where tasks/keys/approvals/policies/ledger live.
                            Default: memory in standalone, sql in distributed.
                            `sql` with DATABASE_URL=sqlite+aiosqlite:///bossman.db
                            gives a durable single node with no extra services.

Modes are always explicit so a stray REDIS_URL or DATABASE_URL in a dev .env
can never silently change behaviour (or break the test suite).
"""

from __future__ import annotations

import os
import socket
import uuid
from dataclasses import dataclass


def _bool(name: str, default: bool) -> bool:
    v = os.getenv(name)
    return default if v is None else v.strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class InfraSettings:
    mode: str = "standalone"
    redis_url: str = "redis://localhost:6379/0"
    database_url: str = "postgresql+asyncpg://bossman:bossman@localhost:5432/bossman"
    node_id: str = ""
    lease_ms: int = 3000
    ledger_sync_s: float = 2.0
    persistence: str = "memory"
    auto_migrate: bool = True
    heartbeat_interval_s: float = 10.0
    heartbeat_timeout_s: float = 30.0
    public_url: str = "http://127.0.0.1:7070"

    @property
    def distributed(self) -> bool:
        return self.mode == "distributed"

    @property
    def sql(self) -> bool:
        return self.persistence == "sql"

    @classmethod
    def from_env(cls) -> "InfraSettings":
        mode = os.getenv("BOSSMAN_MODE", "standalone").strip().lower()
        if mode not in {"standalone", "distributed"}:
            raise ValueError(f"BOSSMAN_MODE must be 'standalone' or 'distributed', got {mode!r}")
        persistence = os.getenv("BOSSMAN_PERSISTENCE", "sql" if mode == "distributed" else "memory").strip().lower()
        if persistence not in {"memory", "sql"}:
            raise ValueError(f"BOSSMAN_PERSISTENCE must be 'memory' or 'sql', got {persistence!r}")
        default = cls()
        port = os.getenv("API_PORT", "7070")
        return cls(
            mode=mode,
            redis_url=os.getenv("REDIS_URL", default.redis_url),
            database_url=os.getenv("DATABASE_URL", default.database_url),
            node_id=os.getenv("BOSSMAN_NODE_ID")
            or f"{socket.gethostname()}-{uuid.uuid4().hex[:6]}",
            lease_ms=int(os.getenv("BOSSMAN_LEASE_MS", default.lease_ms)),
            ledger_sync_s=float(os.getenv("BOSSMAN_LEDGER_SYNC_S", default.ledger_sync_s)),
            persistence=persistence,
            auto_migrate=_bool("BOSSMAN_AUTO_MIGRATE", True),
            heartbeat_interval_s=float(os.getenv("HEARTBEAT_INTERVAL_SECONDS", "10")),
            heartbeat_timeout_s=float(os.getenv("HEARTBEAT_TIMEOUT_SECONDS", "30")),
            public_url=os.getenv("BOSSMAN_PUBLIC_URL", f"http://127.0.0.1:{port}"),
        )
