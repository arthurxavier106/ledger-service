"""Provas de concorrencia contra Postgres real.

Regra da suite: cada task concorrente usa a PROPRIA sessao/conexao. asyncio.gather
sobre uma sessao compartilhada nao testa concorrencia -- testa a serializacao
acidental do pool, passa verde e nao prova nada.
"""

from __future__ import annotations

import asyncio

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from ledger.config import LockStrategy, settings
from ledger.errors import InsufficientFundsError, SerializationExhaustedError
from ledger.service import (
    SQLSTATE_DEADLOCK_DETECTED,
    TransferRequest,
    execute_transfer,
    sqlstate,
)

pytestmark = pytest.mark.asyncio(loop_scope="session")


@pytest.fixture(params=[LockStrategy.ROW_LOCK, LockStrategy.SERIALIZABLE], ids=str)
def strategy(request, monkeypatch):
    """As duas estrategias passam na MESMA suite. Isso e o que torna a escolha
    entre elas uma decisao de performance, e nao de correcao."""
    monkeypatch.setattr(settings, "lock_strategy", request.param)
    monkeypatch.setattr(settings, "serializable_max_retries", 30)
    monkeypatch.setattr(settings, "serializable_base_backoff_s", 0.002)
    return request.param


async def _attempt(session_factory, req: TransferRequest) -> str:
    try:
        await execute_transfer(session_factory, req)
        return "ok"
    except InsufficientFundsError:
        return "insufficient_funds"
    except SerializationExhaustedError:
        return "serialization_exhausted"
    except DBAPIError as exc:
        state = sqlstate(exc)
        return "deadlock" if state == SQLSTATE_DEADLOCK_DETECTED else f"dberror:{state}"


async def _balance(session_factory, account_id) -> int:
    async with session_factory() as s:
        return await s.scalar(
            text("SELECT balance FROM accounts WHERE id = :id"), {"id": account_id}
        )


async def _assert_ledger_is_consistent(session_factory) -> None:
    """I5 e I6: o saldo materializado bate com a soma dos lancamentos, e o
    ledger inteiro fecha em zero."""
    async with session_factory() as s:
        net = await s.scalar(
            text("SELECT COALESCE(SUM(CASE WHEN direction='debit' THEN amount"
                 " ELSE -amount END), 0) FROM entries")
        )
        assert net == 0, f"ledger nao fecha: debitos - creditos = {net}"

        drift = await s.scalar(
            text("""
            SELECT COUNT(*) FROM accounts a
             WHERE a.balance <> COALESCE((
                 SELECT SUM(CASE WHEN e.direction='credit' THEN e.amount ELSE -e.amount END)
                   FROM entries e WHERE e.account_id = a.id), 0)
               AND a.balance <> 0
            """)
        )
        assert drift == 0, f"{drift} conta(s) com saldo divergente dos lancamentos"


# =============================================================================
# C1 -- saldo nunca fica negativo sob transferencias simultaneas
# =============================================================================
async def test_c1_concurrent_transfers_never_overdraw(strategy, session_factory,
                                                      account_factory):
    """50 transferencias simultaneas de 100 numa conta com 1000.

    Exatamente 10 podem passar. Sem o lock de linha, varias leriam o mesmo saldo
    inicial, todas validariam com sucesso e o sistema perderia dinheiro (lost update).
    """
    src = await account_factory(balance=1_000)
    dst = await account_factory(balance=0)

    reqs = [
        TransferRequest(src.id, dst.id, amount=100, currency="BRL")
        for _ in range(50)
    ]
    results = await asyncio.gather(*(_attempt(session_factory, r) for r in reqs))

    assert results.count("deadlock") == 0, "deadlock detectado"
    assert results.count("ok") == 10, f"esperado 10 sucessos, veio {results.count('ok')}"
    assert results.count("insufficient_funds") == 40

    assert await _balance(session_factory, src.id) == 0
    assert await _balance(session_factory, dst.id) == 1_000
    await _assert_ledger_is_consistent(session_factory)


async def test_c1_never_goes_negative_with_odd_amounts(strategy, session_factory,
                                                       account_factory):
    """Valores que nao dividem o saldo exatamente: sobra tem que ficar na conta,
    nunca virar saldo negativo."""
    src = await account_factory(balance=1_000)
    dst = await account_factory(balance=0)

    results = await asyncio.gather(*(
        _attempt(session_factory, TransferRequest(src.id, dst.id, 300, "BRL"))
        for _ in range(20)
    ))

    assert results.count("ok") == 3          # 3 x 300 = 900, sobra 100
    assert await _balance(session_factory, src.id) == 100
    assert await _balance(session_factory, src.id) >= 0
    await _assert_ledger_is_consistent(session_factory)


# =============================================================================
# C2 -- transferencias bidirecionais nao causam deadlock
# =============================================================================
async def test_c2_bidirectional_transfers_do_not_deadlock(strategy, session_factory,
                                                          account_factory):
    """A->B e B->A ao mesmo tempo. E o cenario classico de deadlock:

        T1 trava A, quer B
        T2 trava B, quer A     -> ciclo -> SQLSTATE 40P01

    A ordenacao total dos ids em _lock_accounts elimina a espera circular, entao o
    deadlock nao acontece -- nao e retentado, simplesmente nao existe.
    """
    a = await account_factory(balance=100_000)
    b = await account_factory(balance=100_000)
    total_before = 200_000

    reqs = [TransferRequest(a.id, b.id, 1, "BRL") for _ in range(50)]
    reqs += [TransferRequest(b.id, a.id, 1, "BRL") for _ in range(50)]

    results = await asyncio.gather(*(_attempt(session_factory, r) for r in reqs))

    assert results.count("deadlock") == 0, "ordenacao de lock falhou: houve deadlock"
    assert not [r for r in results if r.startswith("dberror")], f"erros: {set(results)}"

    total_after = (await _balance(session_factory, a.id)
                   + await _balance(session_factory, b.id))
    assert total_after == total_before, "dinheiro foi criado ou destruido"

    if strategy is LockStrategy.ROW_LOCK:
        assert results.count("ok") == 100

    await _assert_ledger_is_consistent(session_factory)


# =============================================================================
# C5 -- conta quente: N remetentes para um unico destino
# =============================================================================
async def test_c5_hot_destination_account(strategy, session_factory, account_factory):
    """Caso real de pagamentos: um merchant recebendo de muitos pagadores ao mesmo
    tempo. Toda a contencao cai numa linha so."""
    senders = [await account_factory(balance=500) for _ in range(30)]
    merchant = await account_factory(balance=0)

    results = await asyncio.gather(*(
        _attempt(session_factory, TransferRequest(s.id, merchant.id, 500, "BRL"))
        for s in senders
    ))

    assert results.count("ok") == 30, f"resultados: {set(results)}"
    assert await _balance(session_factory, merchant.id) == 30 * 500
    await _assert_ledger_is_consistent(session_factory)


# =============================================================================
# Demonstracao do bug que o design previne
# =============================================================================
async def test_naive_read_modify_write_loses_money(session_factory, account_factory):
    """Este teste NAO usa o servico. Faz o read-modify-write ingenuo de proposito
    para documentar o bug que o FOR UPDATE existe para evitar.

    Sob READ COMMITTED, `SELECT balance` seguido de `UPDATE SET balance = <lido> - x`
    perde escritas: varias transacoes leem 1000, todas escrevem 900, e 9 debitos
    somem sem erro nenhum. Nenhuma excecao, nenhum log -- so dinheiro faltando na
    conciliacao do dia seguinte.
    """
    acct = await account_factory(balance=1_000)
    debits = 10
    amount = 100

    async def naive_debit() -> None:
        async with session_factory() as s, s.begin():
            balance = await s.scalar(
                text("SELECT balance FROM accounts WHERE id = :id"), {"id": acct.id}
            )
            await asyncio.sleep(0.05)  # janela de corrida, deterministica
            await s.execute(
                text("UPDATE accounts SET balance = :b WHERE id = :id"),
                {"b": balance - amount, "id": acct.id},
            )

    await asyncio.gather(*(naive_debit() for _ in range(debits)))

    final = await _balance(session_factory, acct.id)
    assert final != 1_000 - debits * amount, (
        "o lost update nao ocorreu -- se este assert falhar, revisar a premissa"
    )
    assert final == 900, "todas as transacoes leram o mesmo saldo inicial"
