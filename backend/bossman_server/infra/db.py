"""
infra.db
────────
One SQLAlchemy metadata for every durable table, and a small Database
wrapper (engine + sessions) shared by the SQL stores.

Tables
  recovery_ledger   RecoveryAttempt rows (Phase 9)
  tasks             Task rows; claims use optimistic concurrency on `version`
  documents         generic JSON documents (keys, approvals, policies, flags,
                    secrets, managed specs, checkpoints, idempotency, audit …)
  events            persisted BossmanEvents (audit / trace queries)

Times that are compared in SQL are stored as float epoch seconds so the same
queries work on PostgreSQL and SQLite.
"""

from __future__ import annotations

from sqlalchemy import Boolean, Float, Index, Integer, String, Text, UniqueConstraint, text
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class LedgerRow(Base):
    __tablename__ = "recovery_ledger"

    attempt_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    incident_id: Mapped[str] = mapped_column(String(64), index=True)
    agent_id: Mapped[str] = mapped_column(String(64), index=True)
    attempt_number: Mapped[int] = mapped_column(Integer)
    detected_at: Mapped[str] = mapped_column(String(40), index=True)  # ISO-8601 UTC
    outcome: Mapped[str] = mapped_column(String(24))
    verified: Mapped[bool] = mapped_column(Boolean, default=False)
    # Full pydantic document — lossless round-trip, schema-evolution friendly.
    doc: Mapped[str] = mapped_column(Text)


class TaskRow(Base):
    __tablename__ = "tasks"
    __table_args__ = (
        UniqueConstraint("namespace", "idempotency_key", name="uq_tasks_idempotency"),
        Index("ix_tasks_claim", "namespace", "status", "priority", "created_ts"),
        Index("ix_tasks_lease", "status", "lease_expires_ts"),
    )

    task_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    namespace: Mapped[str] = mapped_column(String(64))
    status: Mapped[str] = mapped_column(String(16))
    priority: Mapped[int] = mapped_column(Integer)
    required_role: Mapped[str | None] = mapped_column(String(64), nullable=True)
    assigned_to: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    created_ts: Mapped[float] = mapped_column(Float)
    not_before_ts: Mapped[float | None] = mapped_column(Float, nullable=True)
    lease_expires_ts: Mapped[float | None] = mapped_column(Float, nullable=True)
    idempotency_key: Mapped[str | None] = mapped_column(String(128), nullable=True)
    parent_task_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    trace_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    version: Mapped[int] = mapped_column(Integer, default=0)
    doc: Mapped[str] = mapped_column(Text)


class DocumentRow(Base):
    __tablename__ = "documents"
    __table_args__ = (Index("ix_documents_lookup", "collection", "namespace", "status"),)

    collection: Mapped[str] = mapped_column(String(48), primary_key=True)
    doc_id: Mapped[str] = mapped_column(String(160), primary_key=True)
    namespace: Mapped[str | None] = mapped_column(String(64), nullable=True)
    status: Mapped[str | None] = mapped_column(String(32), nullable=True)
    created_ts: Mapped[float] = mapped_column(Float)
    updated_ts: Mapped[float] = mapped_column(Float)
    version: Mapped[int] = mapped_column(Integer, default=1)
    doc: Mapped[str] = mapped_column(Text)


class EventRow(Base):
    __tablename__ = "events"
    __table_args__ = (Index("ix_events_time", "occurred_ts"),)

    event_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    occurred_ts: Mapped[float] = mapped_column(Float)
    event_type: Mapped[str] = mapped_column(String(48), index=True)
    severity: Mapped[str] = mapped_column(String(16))
    agent_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    task_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    namespace: Mapped[str | None] = mapped_column(String(64), nullable=True)
    trace_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    doc: Mapped[str] = mapped_column(Text)


def make_engine(url: str, **kwargs) -> AsyncEngine:  # noqa: ANN003
    if url.startswith("sqlite"):
        kwargs.setdefault("connect_args", {"timeout": 30})
    else:
        kwargs.setdefault("pool_pre_ping", True)
    return create_async_engine(url, **kwargs)


class Database:
    def __init__(self, url: str, **engine_kwargs) -> None:  # noqa: ANN003
        self.url = url
        self.engine: AsyncEngine = make_engine(url, **engine_kwargs)
        self.sessions = async_sessionmaker(self.engine, expire_on_commit=False)

    async def init_schema(self) -> None:
        """create_all (dev/tests). Production uses migrate()."""
        async with self.engine.begin() as conn:
            if self.url.startswith("sqlite"):
                await conn.execute(text("PRAGMA journal_mode=WAL"))
            await conn.run_sync(Base.metadata.create_all)

    async def migrate(self) -> None:
        from bossman_server.infra.migrate import upgrade_async

        await upgrade_async(self.url)

    async def ping(self) -> bool:
        try:
            async with self.engine.connect() as conn:
                await conn.execute(text("SELECT 1"))
            return True
        except Exception:  # noqa: BLE001
            return False

    async def close(self) -> None:
        await self.engine.dispose()
