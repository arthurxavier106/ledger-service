"""Idempotencia: claim, replay seguro e reuso de chave."""

from __future__ import annotations

import asyncio
import datetime as dt
from collections import Counter

import pytest
from sqlalchemy import text

from ledger.config import LockStrategy, settings
from ledger.errors import IdempotencyKeyReuseError, InsufficientFundsError
from ledger.idempotency import execute_idempotent
from ledger.service import TransferRequest, post_transfer

pytestmark = pytest.mark.asyncio(loop_scope="session")

SCOPE = "POST /v1/transfers"


@pytest.fixture(params=[LockStrategy.ROW_LOCK, LockStrategy.SERIALIZABLE], ids=str)
def strategy(request, monkeypatch):
    monkeypatch.setattr(settings, "lock_strategy", request.param)
    monkeypatch.setattr(settings, "serializable_max_retries", 40)
    monkeypatch.setattr(settings, "serializable_base_backoff_s", 0.002)
    return request.param


def _transfer_op(req: TransferRequest):
    async def operation(session):
        txn = await post_transfer(session, req)
        return 201, {"transaction_id": str(txn.id), "amount": req.amount}, txn.id

    return operation


async def _count_transactions(session_factory) -> int:
    async with session_factory() as s:
        return await s.scalar(text("SELECT count(*) FROM transactions WHERE kind='transfer'"))


# --- replay ----------------------------------------------------------------
async def test_replay_returns_stored_response(strategy, session_factory, cache,
                                              account_factory):
    src = await account_factory(balance=1_000)
    dst = await account_factory()
    req = TransferRequest(src.id, dst.id, 400, "BRL")
    payload = {"from": str(src.id), "to": str(dst.id), "amount": 400}

    first = await execute_idempotent(session_factory, cache, scope=SCOPE, key="k-1",
                                     payload=payload, operation=_transfer_op(req))
    second = await execute_idempotent(session_factory, cache, scope=SCOPE, key="k-1",
                                      payload=payload, operation=_transfer_op(req))

    assert first.replayed is False
    assert second.replayed is True
    assert second.body == first.body
    assert await _count_transactions(session_factory) == 1

    async with session_factory() as s:
        balance = await s.scalar(text("SELECT balance FROM accounts WHERE id=:i"),
                                 {"i": src.id})
    assert balance == 600, "o replay nao pode debitar de novo"


async def test_replay_works_without_redis(strategy, session_factory, no_cache,
                                          account_factory):
    """Redis e cache. Com ele fora do ar a idempotencia tem que continuar correta --
    so mais lenta, porque todo replay bate no Postgres."""
    src = await account_factory(balance=1_000)
    dst = await account_factory()
    req = TransferRequest(src.id, dst.id, 250, "BRL")
    payload = {"amount": 250}

    first = await execute_idempotent(session_factory, no_cache, scope=SCOPE, key="k-2",
                                     payload=payload, operation=_transfer_op(req))
    second = await execute_idempotent(session_factory, no_cache, scope=SCOPE, key="k-2",
                                      payload=payload, operation=_transfer_op(req))

    assert (first.replayed, second.replayed) == (False, True)
    assert second.body == first.body
    assert await _count_transactions(session_factory) == 1


async def test_key_reuse_with_different_payload_is_rejected(strategy, session_factory,
                                                            cache, account_factory):
    """Devolver a resposta antiga para um corpo novo e pior do que falhar."""
    src = await account_factory(balance=1_000)
    dst = await account_factory()

    await execute_idempotent(session_factory, cache, scope=SCOPE, key="k-3",
                             payload={"amount": 100},
                             operation=_transfer_op(TransferRequest(src.id, dst.id, 100, "BRL")))

    with pytest.raises(IdempotencyKeyReuseError):
        await execute_idempotent(
            session_factory, cache, scope=SCOPE, key="k-3", payload={"amount": 900},
            operation=_transfer_op(TransferRequest(src.id, dst.id, 900, "BRL")))

    assert await _count_transactions(session_factory) == 1


async def test_scope_isolates_keys(strategy, session_factory, cache, account_factory):
    """A mesma chave em endpoints diferentes nao colide -- por isso scope entra na PK."""
    src = await account_factory(balance=1_000)
    dst = await account_factory()
    req = TransferRequest(src.id, dst.id, 100, "BRL")

    a = await execute_idempotent(session_factory, cache, scope="POST /v1/transfers",
                                 key="same", payload={"x": 1}, operation=_transfer_op(req))
    b = await execute_idempotent(session_factory, cache, scope="POST /v1/other",
                                 key="same", payload={"x": 1}, operation=_transfer_op(req))

    assert a.replayed is False and b.replayed is False
    assert a.body["transaction_id"] != b.body["transaction_id"]
    assert await _count_transactions(session_factory) == 2


async def test_failed_operation_does_not_persist_claim(strategy, session_factory, cache,
                                                       account_factory):
    """Se a operacao falha, a transacao inteira reverte -- inclusive o claim. A chave
    fica livre para uma tentativa legitima depois."""
    src = await account_factory(balance=100)
    dst = await account_factory()

    with pytest.raises(InsufficientFundsError):
        await execute_idempotent(
            session_factory, cache, scope=SCOPE, key="k-4", payload={"amount": 5_000},
            operation=_transfer_op(TransferRequest(src.id, dst.id, 5_000, "BRL")))

    async with session_factory() as s:
        claims = await s.scalar(text("SELECT count(*) FROM idempotency_keys"))
    assert claims == 0, "claim de operacao abortada nao pode sobreviver ao rollback"

    ok = await execute_idempotent(
        session_factory, cache, scope=SCOPE, key="k-4", payload={"amount": 50},
        operation=_transfer_op(TransferRequest(src.id, dst.id, 50, "BRL")))
    assert ok.replayed is False


async def test_expired_key_is_a_new_request(strategy, session_factory, no_cache,
                                            account_factory):
    """Chave expirada nao e replay: e requisicao nova."""
    src = await account_factory(balance=1_000)
    dst = await account_factory()
    req = TransferRequest(src.id, dst.id, 100, "BRL")

    await execute_idempotent(session_factory, no_cache, scope=SCOPE, key="k-5",
                             payload={"a": 1}, operation=_transfer_op(req),
                             ttl=dt.timedelta(seconds=0))
    async with session_factory() as s, s.begin():
        await s.execute(text("UPDATE idempotency_keys SET expires_at = now() - interval '1 hour'"))

    again = await execute_idempotent(session_factory, no_cache, scope=SCOPE, key="k-5",
                                     payload={"a": 1}, operation=_transfer_op(req))
    assert again.replayed is False
    assert await _count_transactions(session_factory) == 2


# --- C3: concorrencia ------------------------------------------------------
async def test_c3_same_key_fired_concurrently_creates_one_transaction(
    strategy, session_factory, cache, account_factory
):
    """20 requisicoes com a MESMA Idempotency-Key ao mesmo tempo.

    O claim e um INSERT ... ON CONFLICT DO NOTHING, entao a unicidade da PK decide o
    vencedor: os 19 perdedores bloqueiam ate o commit e depois releem a resposta
    gravada. Exatamente uma transacao e criada.
    """
    src = await account_factory(balance=10_000)
    dst = await account_factory()
    req = TransferRequest(src.id, dst.id, 700, "BRL")
    payload = {"amount": 700}

    async def attempt():
        try:
            result = await execute_idempotent(session_factory, cache, scope=SCOPE,
                                              key="burst", payload=payload,
                                              operation=_transfer_op(req))
            return "replay" if result.replayed else "created"
        except Exception as exc:
            return type(exc).__name__

    outcomes = Counter(await asyncio.gather(*(attempt() for _ in range(20))))

    assert outcomes["created"] == 1, f"resultados: {outcomes}"
    assert outcomes["replay"] == 19, f"resultados: {outcomes}"
    assert await _count_transactions(session_factory) == 1

    async with session_factory() as s:
        balance = await s.scalar(text("SELECT balance FROM accounts WHERE id=:i"),
                                 {"i": src.id})
    assert balance == 10_000 - 700, "debitou mais de uma vez"


async def test_c3_distinct_keys_all_execute(strategy, session_factory, cache,
                                            account_factory):
    """Controle do teste anterior: chaves distintas nao devem ser deduplicadas."""
    src = await account_factory(balance=10_000)
    dst = await account_factory()

    async def attempt(n: int):
        req = TransferRequest(src.id, dst.id, 100, "BRL")
        return await execute_idempotent(session_factory, cache, scope=SCOPE,
                                        key=f"key-{n}", payload={"n": n},
                                        operation=_transfer_op(req))

    results = await asyncio.gather(*(attempt(n) for n in range(10)))
    assert all(r.replayed is False for r in results)
    assert await _count_transactions(session_factory) == 10
    assert len({r.body["transaction_id"] for r in results}) == 10
