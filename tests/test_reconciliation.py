"""Reconciliacao: prova que o saldo materializado nao divergiu dos lancamentos."""

from __future__ import annotations

import pytest
from sqlalchemy import text

from ledger.metrics import REGISTRY
from ledger.reconciliation import find_drift, reconcile, write_snapshots
from ledger.service import TransferRequest, execute_transfer

pytestmark = pytest.mark.asyncio(loop_scope="session")


async def test_healthy_ledger_has_no_drift(session_factory, account_factory):
    src = await account_factory(balance=1_000)
    dst = await account_factory()
    await execute_transfer(session_factory, TransferRequest(src.id, dst.id, 250, "BRL"))

    report = await reconcile(session_factory)

    assert report.is_clean
    assert report.accounts_checked >= 3  # inclui a conta de sistema do aporte
    assert REGISTRY.get_sample_value("ledger_reconciliation_drift") == 0


async def test_injected_drift_is_detected(session_factory, account_factory):
    """Corrompe o saldo por fora do servico. O job tem que enxergar."""
    src = await account_factory(balance=1_000)
    dst = await account_factory()
    await execute_transfer(session_factory, TransferRequest(src.id, dst.id, 100, "BRL"))

    async with session_factory() as s, s.begin():
        await s.execute(text("UPDATE accounts SET balance = balance + 500 WHERE id = :i"),
                        {"i": dst.id})

    report = await reconcile(session_factory)

    assert not report.is_clean
    assert len(report.drifted) == 1
    row = report.drifted[0]
    assert row.account_id == str(dst.id)
    assert row.drift == 500, "o job precisa dizer o tamanho da divergencia"
    assert REGISTRY.get_sample_value("ledger_reconciliation_drift") == 1


async def test_snapshots_make_reconciliation_incremental(session_factory, account_factory):
    """Depois do snapshot, a checagem soma so do snapshot para frente -- e continua
    dando o mesmo resultado."""
    src = await account_factory(balance=10_000)
    dst = await account_factory()
    for _ in range(5):
        await execute_transfer(session_factory, TransferRequest(src.id, dst.id, 100, "BRL"))

    first = await reconcile(session_factory)
    assert first.is_clean
    assert first.snapshots_written > 0

    async with session_factory() as s:
        max_snapshot = await s.scalar(
            text("SELECT MAX(as_of_entry_id) FROM balance_snapshots WHERE account_id=:i"),
            {"i": dst.id})
        max_entry = await s.scalar(
            text("SELECT MAX(id) FROM entries WHERE account_id=:i"), {"i": dst.id})
    assert max_snapshot == max_entry

    # lancamentos novos depois do snapshot continuam sendo conferidos
    await execute_transfer(session_factory, TransferRequest(src.id, dst.id, 700, "BRL"))
    assert (await reconcile(session_factory)).is_clean

    async with session_factory() as s, s.begin():
        await s.execute(text("UPDATE accounts SET balance = balance - 1 WHERE id=:i"),
                        {"i": dst.id})
    after = await reconcile(session_factory)
    assert not after.is_clean, "drift posterior ao snapshot tambem tem que aparecer"
    assert after.drifted[0].drift == -1


async def test_snapshot_write_is_idempotent(session_factory, account_factory):
    src = await account_factory(balance=1_000)
    dst = await account_factory()
    await execute_transfer(session_factory, TransferRequest(src.id, dst.id, 100, "BRL"))

    async with session_factory() as s, s.begin():
        first = await write_snapshots(s)
    async with session_factory() as s, s.begin():
        second = await write_snapshots(s)

    assert first > 0
    assert second == 0, "rodar de novo sem lancamentos novos nao cria snapshot duplicado"


async def test_find_drift_returns_empty_on_clean_ledger(session_factory, account_factory):
    await account_factory(balance=500)
    async with session_factory() as s:
        assert await find_drift(s) == []
