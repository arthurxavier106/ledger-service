"""Transactional outbox e endpoints de webhook.

Revision ID: 0003_outbox
Revises: 0002_idempotency_keys
"""

from __future__ import annotations

from alembic import op

from ledger.db import read_sql, split_sql_statements

revision = "0003_outbox"
down_revision = "0002_idempotency_keys"
branch_labels = None
depends_on = None


def upgrade() -> None:
    for statement in split_sql_statements(read_sql("0003_up.sql")):
        op.execute(statement)


def downgrade() -> None:
    for statement in split_sql_statements(read_sql("0003_down.sql")):
        op.execute(statement)
