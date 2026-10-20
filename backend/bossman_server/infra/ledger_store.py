"""
infra.ledger_store
──────────────────
Durable storage for RecoveryAttempt rows.

  InMemoryLedgerStore  default / tests
  SqlLedgerStore       SQLAlchemy async — PostgreSQL (asyncpg) in prod,
                       SQLite (aiosqlite) for local tests.

Rows are keyed by attempt_id. `append` inserts; `update_outcome` only
touches outcome fields (the attempt's identity/evidence never changes).
"""

from __future__ import annotations

from typing import Protocol

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine

from bossman_server.contracts.recovery_ledger import RecoveryAttempt
from bossman_server.infra.db import Base as _Base
from bossman_server.infra.db import Database, LedgerRow


class LedgerStore(Protocol):
    async def append(self, attempt: RecoveryAttempt) -> None: ...
    async def update_outcome(self, attempt: RecoveryAttempt) -> None: ...
    async def get(self, attempt_id: str) -> RecoveryAttempt | None: ...
    async def by_incident(self, incident_id: str) -> list[RecoveryAttempt]: ...
    async def by_agent(self, agent_id: str, limit: int = 100) -> list[RecoveryAttempt]: ...
    async def upsert(self, attempt: RecoveryAttempt) -> None: ...
    async def recent(self, limit: int = 1000) -> list[RecoveryAttempt]: ...


class InMemoryLedgerStore:
    def __init__(self) -> None:
        self._rows: dict[str, RecoveryAttempt] = {}

    async def append(self, attempt: RecoveryAttempt) -> None:
        if attempt.attempt_id in self._rows:
            raise ValueError(f"duplicate attempt {attempt.attempt_id}")
        self._rows[attempt.attempt_id] = attempt.model_copy(deep=True)

    async def update_outcome(self, attempt: RecoveryAttempt) -> None:
        if attempt.attempt_id not in self._rows:
            raise KeyError(attempt.attempt_id)
        self._rows[attempt.attempt_id] = attempt.model_copy(deep=True)

    async def get(self, attempt_id: str) -> RecoveryAttempt | None:
        row = self._rows.get(attempt_id)
        return row.model_copy(deep=True) if row else None

    async def by_incident(self, incident_id: str) -> list[RecoveryAttempt]:
        rows = [r for r in self._rows.values() if r.incident_id == incident_id]
        return sorted(rows, key=lambda r: r.attempt_number)

    async def by_agent(self, agent_id: str, limit: int = 100) -> list[RecoveryAttempt]:
        rows = [r for r in self._rows.values() if r.agent_id == agent_id]
        rows.sort(key=lambda r: r.failure_detected_at, reverse=True)
        return rows[:limit]

    async def upsert(self, attempt: RecoveryAttempt) -> None:
        self._rows[attempt.attempt_id] = attempt.model_copy(deep=True)

    async def recent(self, limit: int = 1000) -> list[RecoveryAttempt]:
        rows = sorted(self._rows.values(), key=lambda r: r.failure_detected_at)
        return [r.model_copy(deep=True) for r in rows[-limit:]]


class SqlLedgerStore:
    """Pass a URL (owns its engine) or a shared infra.db.Database."""

    def __init__(self, url: str | None = None, *, db: Database | None = None, **engine_kwargs) -> None:  # noqa: ANN003
        if db is None:
            if url is None:
                raise ValueError("SqlLedgerStore needs a url or a Database")
            db = Database(url, **engine_kwargs)
            self._owns_db = True
        else:
            self._owns_db = False
        self.db = db
        self.engine: AsyncEngine = db.engine
        self._sessions = db.sessions

    async def init_schema(self) -> None:
        async with self.engine.begin() as conn:
            await conn.run_sync(_Base.metadata.create_all)

    async def close(self) -> None:
        if self._owns_db:
            await self.db.close()

    @staticmethod
    def _to_row(a: RecoveryAttempt) -> LedgerRow:
        return LedgerRow(
            attempt_id=a.attempt_id,
            incident_id=a.incident_id,
            agent_id=a.agent_id,
            attempt_number=a.attempt_number,
            detected_at=a.failure_detected_at.isoformat(),
            outcome=a.outcome.value,
            verified=a.verified_by_evaluator,
            doc=a.model_dump_json(),
        )

    async def append(self, attempt: RecoveryAttempt) -> None:
        async with self._sessions() as s, s.begin():
            s.add(self._to_row(attempt))

    async def update_outcome(self, attempt: RecoveryAttempt) -> None:
        async with self._sessions() as s, s.begin():
            row = await s.get(LedgerRow, attempt.attempt_id)
            if row is None:
                raise KeyError(attempt.attempt_id)
            row.outcome = attempt.outcome.value
            row.verified = attempt.verified_by_evaluator
            row.doc = attempt.model_dump_json()

    async def get(self, attempt_id: str) -> RecoveryAttempt | None:
        async with self._sessions() as s:
            row = await s.get(LedgerRow, attempt_id)
            return RecoveryAttempt.model_validate_json(row.doc) if row else None

    async def by_incident(self, incident_id: str) -> list[RecoveryAttempt]:
        async with self._sessions() as s:
            q = (
                select(LedgerRow)
                .where(LedgerRow.incident_id == incident_id)
                .order_by(LedgerRow.attempt_number)
            )
            return [RecoveryAttempt.model_validate_json(r.doc) for r in (await s.scalars(q))]

    async def by_agent(self, agent_id: str, limit: int = 100) -> list[RecoveryAttempt]:
        async with self._sessions() as s:
            q = (
                select(LedgerRow)
                .where(LedgerRow.agent_id == agent_id)
                .order_by(LedgerRow.detected_at.desc())
                .limit(limit)
            )
            return [RecoveryAttempt.model_validate_json(r.doc) for r in (await s.scalars(q))]

    async def upsert(self, attempt: RecoveryAttempt) -> None:
        async with self._sessions() as s, s.begin():
            row = await s.get(LedgerRow, attempt.attempt_id)
            if row is None:
                s.add(self._to_row(attempt))
            else:
                row.outcome = attempt.outcome.value
                row.verified = attempt.verified_by_evaluator
                row.doc = attempt.model_dump_json()

    async def recent(self, limit: int = 1000) -> list[RecoveryAttempt]:
        async with self._sessions() as s:
            q = select(LedgerRow).order_by(LedgerRow.detected_at.desc()).limit(limit)
            rows = [RecoveryAttempt.model_validate_json(r.doc) for r in (await s.scalars(q))]
            return list(reversed(rows))
