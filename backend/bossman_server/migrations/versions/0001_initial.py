"""Initial schema: recovery_ledger, tasks, documents, events.

Idempotent: deployments that ran Phase 9 already have `recovery_ledger`
(created by create_all), so every table is created only if missing.

Revision ID: 0001
Revises:
Create Date: 2026-10-20
"""

import sqlalchemy as sa
from alembic import op

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


def _has(name: str) -> bool:
    return sa.inspect(op.get_bind()).has_table(name)


def upgrade() -> None:
    if not _has("recovery_ledger"):
        op.create_table(
            "recovery_ledger",
            sa.Column("attempt_id", sa.String(64), primary_key=True),
            sa.Column("incident_id", sa.String(64), nullable=False),
            sa.Column("agent_id", sa.String(64), nullable=False),
            sa.Column("attempt_number", sa.Integer, nullable=False),
            sa.Column("detected_at", sa.String(40), nullable=False),
            sa.Column("outcome", sa.String(24), nullable=False),
            sa.Column("verified", sa.Boolean, nullable=False, server_default=sa.false()),
            sa.Column("doc", sa.Text, nullable=False),
        )
        op.create_index("ix_recovery_ledger_incident_id", "recovery_ledger", ["incident_id"])
        op.create_index("ix_recovery_ledger_agent_id", "recovery_ledger", ["agent_id"])
        op.create_index("ix_recovery_ledger_detected_at", "recovery_ledger", ["detected_at"])

    if not _has("tasks"):
        op.create_table(
            "tasks",
            sa.Column("task_id", sa.String(64), primary_key=True),
            sa.Column("namespace", sa.String(64), nullable=False),
            sa.Column("status", sa.String(16), nullable=False),
            sa.Column("priority", sa.Integer, nullable=False),
            sa.Column("required_role", sa.String(64)),
            sa.Column("assigned_to", sa.String(64)),
            sa.Column("created_ts", sa.Float, nullable=False),
            sa.Column("not_before_ts", sa.Float),
            sa.Column("lease_expires_ts", sa.Float),
            sa.Column("idempotency_key", sa.String(128)),
            sa.Column("parent_task_id", sa.String(64)),
            sa.Column("trace_id", sa.String(64)),
            sa.Column("version", sa.Integer, nullable=False, server_default="0"),
            sa.Column("doc", sa.Text, nullable=False),
            sa.UniqueConstraint("namespace", "idempotency_key", name="uq_tasks_idempotency"),
        )
        op.create_index("ix_tasks_claim", "tasks", ["namespace", "status", "priority", "created_ts"])
        op.create_index("ix_tasks_lease", "tasks", ["status", "lease_expires_ts"])
        op.create_index("ix_tasks_assigned_to", "tasks", ["assigned_to"])
        op.create_index("ix_tasks_parent_task_id", "tasks", ["parent_task_id"])
        op.create_index("ix_tasks_trace_id", "tasks", ["trace_id"])

    if not _has("documents"):
        op.create_table(
            "documents",
            sa.Column("collection", sa.String(48), primary_key=True),
            sa.Column("doc_id", sa.String(160), primary_key=True),
            sa.Column("namespace", sa.String(64)),
            sa.Column("status", sa.String(32)),
            sa.Column("created_ts", sa.Float, nullable=False),
            sa.Column("updated_ts", sa.Float, nullable=False),
            sa.Column("version", sa.Integer, nullable=False, server_default="1"),
            sa.Column("doc", sa.Text, nullable=False),
        )
        op.create_index("ix_documents_lookup", "documents", ["collection", "namespace", "status"])

    if not _has("events"):
        op.create_table(
            "events",
            sa.Column("event_id", sa.String(64), primary_key=True),
            sa.Column("occurred_ts", sa.Float, nullable=False),
            sa.Column("event_type", sa.String(48), nullable=False),
            sa.Column("severity", sa.String(16), nullable=False),
            sa.Column("agent_id", sa.String(64)),
            sa.Column("task_id", sa.String(64)),
            sa.Column("namespace", sa.String(64)),
            sa.Column("trace_id", sa.String(64)),
            sa.Column("doc", sa.Text, nullable=False),
        )
        op.create_index("ix_events_time", "events", ["occurred_ts"])
        op.create_index("ix_events_event_type", "events", ["event_type"])
        op.create_index("ix_events_agent_id", "events", ["agent_id"])
        op.create_index("ix_events_task_id", "events", ["task_id"])
        op.create_index("ix_events_trace_id", "events", ["trace_id"])


def downgrade() -> None:
    for name in ("events", "documents", "tasks"):
        if _has(name):
            op.drop_table(name)
