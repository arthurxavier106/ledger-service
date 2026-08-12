"""Cada teste aqui TENTA violar uma invariante direto no banco, por fora da
aplicacao, e exige que o Postgres recuse. Se algum destes passar sem erro, a
garantia esta so no codigo Python -- que e exatamente o que o design evita."""

from __future__ import annotations

import uuid

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError, IntegrityError

from ledger.ids import uuid7

pytestmark = pytest.mark.asyncio(loop_scope="session")


async def _new_txn(session, kind: str = "transfer") -> uuid.UUID:
    txn_id = uuid7()
    await session.execute(
        text("INSERT INTO transactions (id, kind) VALUES (:id, CAST(:k AS transaction_kind))"),
        {"id": txn_id, "k": kind},
    )
    return txn_id


async def _insert_entry(session, txn_id, account, direction, amount) -> None:
    await session.execute(
        text(
            "INSERT INTO entries (transaction_id, account_id, direction, amount,"
            " currency, balance_after) VALUES (:t, :a, CAST(:d AS entry_direction),"
            " :amt, :cur, 0)"
        ),
        {"t": txn_id, "a": account.id, "d": direction, "amt": amount, "cur": account.currency},
    )


# --- I1: debito == credito -------------------------------------------------
async def test_i1_rejects_single_sided_entry(session, account_factory):
    """Um lancamento so de debito nao pode ser commitado."""
    acct = await account_factory(balance=1000)
    txn_id = await _new_txn(session)
    await _insert_entry(session, txn_id, acct, "debit", 100)

    # O INSERT passa; o COMMIT e que falha -- constraint trigger DEFERRED.
    with pytest.raises(DBAPIError, match="minimum is 2"):
        await session.commit()


async def test_i1_rejects_unbalanced_entries(session, account_factory):
    """Debito 100 contra credito 90 nao fecha."""
    src = await account_factory(balance=1000)
    dst = await account_factory()
    txn_id = await _new_txn(session)
    await _insert_entry(session, txn_id, src, "debit", 100)
    await _insert_entry(session, txn_id, dst, "credit", 90)

    with pytest.raises(DBAPIError, match="unbalanced by 10"):
        await session.commit()


async def test_i1_accepts_balanced_entries(session, account_factory):
    src = await account_factory(balance=1000)
    dst = await account_factory()
    txn_id = await _new_txn(session)
    await _insert_entry(session, txn_id, src, "debit", 100)
    await _insert_entry(session, txn_id, dst, "credit", 100)
    await session.commit()  # nao levanta


# --- I2: saldo nao-negativo ------------------------------------------------
async def test_i2_rejects_negative_balance(session, account_factory):
    """UPDATE direto, por fora do servico. O banco recusa mesmo assim."""
    acct = await account_factory(balance=100)
    with pytest.raises(IntegrityError, match="accounts_balance_non_negative"):
        await session.execute(
            text("UPDATE accounts SET balance = -1 WHERE id = :id"), {"id": acct.id}
        )
        await session.commit()


async def test_i2_allows_negative_when_flagged(session, account_factory):
    """Conta de sistema (liquidacao, contrapartida de deposito) pode ficar negativa."""
    acct = await account_factory(balance=0, allow_negative=True)
    await session.execute(
        text("UPDATE accounts SET balance = -5000 WHERE id = :id"), {"id": acct.id}
    )
    await session.commit()
    balance = await session.scalar(
        text("SELECT balance FROM accounts WHERE id = :id"), {"id": acct.id}
    )
    assert balance == -5000


# --- I3: append-only -------------------------------------------------------
@pytest.mark.parametrize(
    "statement",
    [
        "UPDATE entries SET amount = 1 WHERE transaction_id = :t",
        "DELETE FROM entries WHERE transaction_id = :t",
    ],
)
async def test_i3_entries_are_immutable(session, account_factory, statement):
    src = await account_factory(balance=1000)
    dst = await account_factory()
    txn_id = await _new_txn(session)
    await _insert_entry(session, txn_id, src, "debit", 100)
    await _insert_entry(session, txn_id, dst, "credit", 100)
    await session.commit()

    with pytest.raises(DBAPIError, match="append-only"):
        await session.execute(text(statement), {"t": txn_id})
        await session.commit()


# --- I4: estorno unico -----------------------------------------------------
async def test_i4_single_reversal_per_transaction(session, account_factory):
    original = await _new_txn(session)
    await session.commit()

    # Diferente de I1, o indice unico NAO e deferred: falha ja no segundo INSERT,
    # nao no COMMIT. Vale saber, porque muda onde o erro aparece no servico.
    with pytest.raises(IntegrityError, match="transactions_single_reversal_uniq"):
        for _ in range(2):
            await session.execute(
                text(
                    "INSERT INTO transactions (id, kind, reverses_transaction_id)"
                    " VALUES (:id, 'reversal', :orig)"
                ),
                {"id": uuid7(), "orig": original},
            )
        await session.commit()


async def test_reversal_shape_constraint(session):
    """kind='reversal' e reverses_transaction_id andam juntos ou nao andam."""
    with pytest.raises(IntegrityError, match="reversal_shape"):
        await session.execute(
            text("INSERT INTO transactions (id, kind) VALUES (:id, 'reversal')"),
            {"id": uuid7()},
        )
        await session.commit()
