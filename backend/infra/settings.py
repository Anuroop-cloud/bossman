"""
infra.settings
──────────────
Deployment mode, resolved from the environment.

  BOSSMAN_MODE=standalone   (default) single process, everything in memory.
  BOSSMAN_MODE=distributed  Redis (coordination/events/registry) +
                            PostgreSQL (recovery ledger) + leader election.

Distributed mode is always explicit so a stray REDIS_URL in a dev .env can
never silently change behaviour (or break the test suite).
"""

from __future__ import annotations

import os
import socket
import uuid
from dataclasses import dataclass


@dataclass(frozen=True)
class InfraSettings:
    mode: str = "standalone"
    redis_url: str = "redis://localhost:6379/0"
    database_url: str = "postgresql+asyncpg://bossman:bossman@localhost:5432/bossman"
    node_id: str = ""
    lease_ms: int = 3000
    ledger_sync_s: float = 2.0

    @property
    def distributed(self) -> bool:
        return self.mode == "distributed"

    @classmethod
    def from_env(cls) -> "InfraSettings":
        mode = os.getenv("BOSSMAN_MODE", "standalone").strip().lower()
        if mode not in {"standalone", "distributed"}:
            raise ValueError(f"BOSSMAN_MODE must be 'standalone' or 'distributed', got {mode!r}")
        default = cls()
        return cls(
            mode=mode,
            redis_url=os.getenv("REDIS_URL", default.redis_url),
            database_url=os.getenv("DATABASE_URL", default.database_url),
            node_id=os.getenv("BOSSMAN_NODE_ID")
            or f"{socket.gethostname()}-{uuid.uuid4().hex[:6]}",
            lease_ms=int(os.getenv("BOSSMAN_LEASE_MS", default.lease_ms)),
            ledger_sync_s=float(os.getenv("BOSSMAN_LEDGER_SYNC_S", default.ledger_sync_s)),
        )
