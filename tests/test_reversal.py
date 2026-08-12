"""Estorno: nova transacao espelhada, nunca mutacao da original."""

from __future__ import annotations

import asyncio
from collections import Counter

import pytest
from sqlalchemy import text

from ledger.config import LockStrategy, settings
from ledger.errors import (
    AlreadyReversedError,
    InsufficientFundsError,
    InvalidReversalError,
    TransactionNotFoundError,
)
from ledger.service import (
    ReversalRequest,
    TransferRequest,
    execute_reversal,
    execute_transfer,
)

pytestmark = pytest.mark.asyncio(loop_scope="session")


@pytest.fixture(params=[LockStrategy.ROW_LOCK, LockStrategy.SERIALIZABLE], ids=str)
def strategy(request, monkeypatch):
    monkeypatch.setattr(settings, "lock_strategy", request.param)
    monkeypatch.setattr(settings, "serializable_max_retries", 40)
    monkeypatch.setattr(settings, "serializable_base_backoff_s", 0.002)
    return request.param


async def _balance(session_factory, account_id) -> int:
    async with session_factory() as s:
        return await s.scalar(text("SELECT balance FROM accounts WHERE id=:i"),
                              {"i": account_id})


async def test_reversal_restores_balances(strategy, session_factory, account_factory):
    src = await account_factory(balance=1_000)
    dst = await account_factory()

    txn = await execute_transfer(session_factory, TransferRequest(src.id, dst.id, 300, "BRL"))
    assert await _balance(session_factory, src.id) == 700

    reversal = await execute_reversal(session_factory, ReversalRequest(txn.id))

    assert reversal.id != txn.id
    assert reversal.reverses_transaction_id == txn.id
    assert await _balance(session_factory, src.id) == 1_000
    assert await _balance(session_factory, dst.id) == 0


async def test_reversal_mirrors_entries_without_mutating_original(
    strategy, session_factory, account_factory
):
    """A original continua intacta: entries e append-only e a historia tem que
    permanecer auditavel. So o status da transacao original muda."""
    src = await account_factory(balance=1_000)
    dst = await account_factory()
    txn = await execute_transfer(session_factory, TransferRequest(src.id, dst.id, 250, "BRL"))
    await execute_reversal(session_factory, ReversalRequest(txn.id))

    async with session_factory() as s:
        original = (await s.execute(
            text("SELECT account_id, direction, amount FROM entries"
                 " WHERE transaction_id=:t ORDER BY id"), {"t": txn.id})).all()
        mirrored = (await s.execute(
            text("SELECT account_id, direction, amount FROM entries e"
                 " JOIN transactions r ON r.id = e.transaction_id"
                 " WHERE r.reverses_transaction_id=:t ORDER BY e.id"), {"t": txn.id})).all()
        status = await s.scalar(text("SELECT status FROM transactions WHERE id=:t"),
                                {"t": txn.id})

    assert len(original) == len(mirrored) == 2
    assert status == "reversed"
    for before, after in zip(original, mirrored, strict=True):
        assert before.account_id == after.account_id
        assert before.amount == after.amount
        assert before.direction != after.direction, "o lancamento tem que ser espelhado"


async def test_cannot_reverse_twice(strategy, session_factory, account_factory):
    src = await account_factory(balance=1_000)
    dst = await account_factory()
    txn = await execute_transfer(session_factory, TransferRequest(src.id, dst.id, 100, "BRL"))
    await execute_reversal(session_factory, ReversalRequest(txn.id))

    with pytest.raises(AlreadyReversedError):
        await execute_reversal(session_factory, ReversalRequest(txn.id))


async def test_cannot_reverse_a_reversal(strategy, session_factory, account_factory):
    src = await account_factory(balance=1_000)
    dst = await account_factory()
    txn = await execute_transfer(session_factory, TransferRequest(src.id, dst.id, 100, "BRL"))
    reversal = await execute_reversal(session_factory, ReversalRequest(txn.id))

    with pytest.raises(InvalidReversalError):
        await execute_reversal(session_factory, ReversalRequest(reversal.id))


async def test_unknown_transaction(strategy, session_factory):
    from ledger.ids import uuid7

    with pytest.raises(TransactionNotFoundError):
        await execute_reversal(session_factory, ReversalRequest(uuid7()))


async def test_reversal_fails_if_funds_already_spent(strategy, session_factory,
                                                     account_factory):
    """Limitacao conhecida, documentada no README: se o destino ja gastou o dinheiro,
    o estorno falha em vez de deixar a conta negativa. Sistemas reais fazem clawback
    com saldo negativo controlado."""
    src = await account_factory(balance=1_000)
    merchant = await account_factory()
    elsewhere = await account_factory()

    txn = await execute_transfer(session_factory, TransferRequest(src.id, merchant.id, 500, "BRL"))
    await execute_transfer(session_factory, TransferRequest(merchant.id, elsewhere.id, 500, "BRL"))

    with pytest.raises(InsufficientFundsError):
        await execute_reversal(session_factory, ReversalRequest(txn.id))

    # a tentativa falha inteira: nada de estorno parcial
    assert await _balance(session_factory, src.id) == 500
    assert await _balance(session_factory, merchant.id) == 0


# --- C4 --------------------------------------------------------------------
async def test_c4_concurrent_reversals_exactly_one_succeeds(strategy, session_factory,
                                                            account_factory):
    """10 estornos simultaneos da mesma transacao.

    A checagem de status na aplicacao e so fast path -- varias requisicoes passam
    por ela. Quem arbitra e o indice unico parcial transactions_single_reversal_uniq
    (I4), no banco.
    """
    src = await account_factory(balance=10_000)
    dst = await account_factory()
    txn = await execute_transfer(session_factory, TransferRequest(src.id, dst.id, 1_000, "BRL"))

    async def attempt() -> str:
        try:
            await execute_reversal(session_factory, ReversalRequest(txn.id))
            return "reversed"
        except AlreadyReversedError:
            return "already_reversed"
        except Exception as exc:
            return type(exc).__name__

    outcomes = Counter(await asyncio.gather(*(attempt() for _ in range(10))))

    assert outcomes["reversed"] == 1, f"resultados: {outcomes}"
    assert outcomes["already_reversed"] == 9, f"resultados: {outcomes}"
    assert await _balance(session_factory, src.id) == 10_000
    assert await _balance(session_factory, dst.id) == 0

    async with session_factory() as s:
        reversals = await s.scalar(
            text("SELECT count(*) FROM transactions WHERE reverses_transaction_id=:t"),
            {"t": txn.id})
        net = await s.scalar(
            text("SELECT COALESCE(SUM(CASE WHEN direction='debit' THEN amount"
                 " ELSE -amount END),0) FROM entries"))
    assert reversals == 1
    assert net == 0, "o ledger tem que continuar fechando em zero"
