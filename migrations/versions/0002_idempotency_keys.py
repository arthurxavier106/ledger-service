"""Tabela de idempotencia (fonte de verdade; Redis e apenas cache).

Revision ID: 0002_idempotency_keys
Revises: 0001_initial_ledger
"""

from __future__ import annotations

from alembic import op

from ledger.db import read_sql, split_sql_statements

revision = "0002_idempotency_keys"
down_revision = "0001_initial_ledger"
branch_labels = None
depends_on = None


def upgrade() -> None:
    for statement in split_sql_statements(read_sql("0002_up.sql")):
        op.execute(statement)


def downgrade() -> None:
    for statement in split_sql_statements(read_sql("0002_down.sql")):
        op.execute(statement)
