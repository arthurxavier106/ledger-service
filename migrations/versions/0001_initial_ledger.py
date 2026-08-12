"""Schema inicial do ledger, com as invariantes I1-I4 como constraints.

O DDL vive em src/ledger/sql/*.sql e e lido daqui. Fonte unica: a suite de testes
aplica exatamente o mesmo arquivo, entao nao existe drift entre o schema testado e
o schema migrado -- que e a forma mais comum de uma migration passar no CI e
quebrar em producao.

Revision ID: 0001_initial_ledger
Revises:
"""

from __future__ import annotations

from alembic import op

from ledger.db import read_sql, split_sql_statements

revision = "0001_initial_ledger"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    for statement in split_sql_statements(read_sql("0001_up.sql")):
        op.execute(statement)


def downgrade() -> None:
    for statement in split_sql_statements(read_sql("0001_down.sql")):
        op.execute(statement)
