"""Job de reconciliacao: prova que o saldo materializado nao divergiu.

Materializar `accounts.balance` e uma escolha de performance -- leitura O(1) em vez
de somar a vida inteira da conta. O preco e a possibilidade de divergencia. Este
job e o que torna a escolha defensavel: se `ledger_reconciliation_drift` sair de
zero uma vez, o design de saldo materializado esta errado e o README precisa dizer.

Reconciliacao incremental: em vez de somar todos os lancamentos desde sempre, soma
a partir do ultimo snapshot de cada conta.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from ledger import metrics

logger = logging.getLogger(__name__)

_DRIFT_SQL = """
WITH latest AS (
    SELECT DISTINCT ON (account_id) account_id, as_of_entry_id, balance
      FROM balance_snapshots
     ORDER BY account_id, as_of_entry_id DESC
),
computed AS (
    SELECT a.id AS account_id,
           a.balance AS materialized,
           COALESCE(l.balance, 0) + COALESCE((
               SELECT SUM(CASE WHEN e.direction = 'credit' THEN e.amount ELSE -e.amount END)
                 FROM entries e
                WHERE e.account_id = a.id
                  AND e.id > COALESCE(l.as_of_entry_id, 0)
           ), 0) AS derived
      FROM accounts a
      LEFT JOIN latest l ON l.account_id = a.id
)
SELECT account_id, materialized, derived, materialized - derived AS drift
  FROM computed
 WHERE materialized <> derived
"""


@dataclass(slots=True)
class DriftRow:
    account_id: str
    materialized: int
    derived: int
    drift: int


@dataclass(slots=True)
class ReconciliationReport:
    accounts_checked: int
    drifted: list[DriftRow]
    snapshots_written: int

    @property
    def is_clean(self) -> bool:
        return not self.drifted


async def find_drift(session: AsyncSession) -> list[DriftRow]:
    result = await session.execute(text(_DRIFT_SQL))
    return [
        DriftRow(str(r.account_id), r.materialized, r.derived, r.drift)
        for r in result
    ]


async def write_snapshots(session: AsyncSession) -> int:
    """Grava um snapshot por conta no ultimo lancamento conhecido.

    Torna a proxima reconciliacao barata: ela so precisa somar dai para frente.
    Idempotente pela PK (account_id, as_of_entry_id).
    """
    result = await session.execute(text("""
        INSERT INTO balance_snapshots (account_id, as_of_entry_id, balance)
        SELECT a.id, COALESCE(MAX(e.id), 0), a.balance
          FROM accounts a
          LEFT JOIN entries e ON e.account_id = a.id
         GROUP BY a.id, a.balance
        ON CONFLICT (account_id, as_of_entry_id) DO NOTHING
        RETURNING account_id
    """))
    return len(result.fetchall())


async def reconcile(
    session_factory: async_sessionmaker[AsyncSession], *, snapshot: bool = True
) -> ReconciliationReport:
    async with session_factory() as session, session.begin():
        total = await session.scalar(text("SELECT count(*) FROM accounts")) or 0
        drifted = await find_drift(session)
        written = await write_snapshots(session) if snapshot and not drifted else 0

    metrics.reconciliation_drift.set(len(drifted))
    if drifted:
        # Divergencia aqui e incidente, nao aviso: alguem tem que olhar.
        logger.error("reconciliacao encontrou %s conta(s) divergentes: %s",
                     len(drifted), [d.account_id for d in drifted])
    return ReconciliationReport(total, drifted, written)
