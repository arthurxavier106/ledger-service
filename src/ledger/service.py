"""Write path do ledger.

Este arquivo e o projeto. Tudo mais e infraestrutura em volta dele.
Ver DESIGN.md secao 4 para o racional completo.
"""

from __future__ import annotations

import asyncio
import random
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field

from sqlalchemy import select, update
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from ledger import metrics
from ledger.config import LockStrategy, settings
from ledger.errors import (
    AccountNotActiveError,
    AccountNotFoundError,
    AlreadyReversedError,
    CurrencyMismatchError,
    IdempotencyInFlightError,
    InsufficientFundsError,
    InvalidReversalError,
    InvalidTransferError,
    LedgerError,
    LedgerInvariantError,
    SerializationExhaustedError,
    TransactionNotFoundError,
)
from ledger.ids import uuid7
from ledger.models import (
    Account,
    AccountStatus,
    Entry,
    EntryDirection,
    Transaction,
    TransactionKind,
    TransactionStatus,
)
from ledger.outbox import enqueue

SQLSTATE_SERIALIZATION_FAILURE = "40001"
SQLSTATE_DEADLOCK_DETECTED = "40P01"


@dataclass(slots=True)
class TransferRequest:
    from_account_id: uuid.UUID
    to_account_id: uuid.UUID
    amount: int
    currency: str
    external_ref: str | None = None
    metadata: dict = field(default_factory=dict)
    # Aporte externo debitando conta de sistema e 'deposit', nao 'transfer'.
    # Mesma mecanica, semantica contabil diferente.
    kind: TransactionKind = TransactionKind.TRANSFER


def sqlstate(exc: BaseException) -> str | None:
    orig = getattr(exc, "orig", None)
    return getattr(orig, "sqlstate", None) or getattr(orig, "pgcode", None)


# -----------------------------------------------------------------------------
# Lock ordering
# -----------------------------------------------------------------------------
async def _lock_accounts(
    session: AsyncSession, account_ids: list[uuid.UUID]
) -> dict[uuid.UUID, Account]:
    """Trava as contas SEMPRE na mesma ordem global.

    Este e o ponto que elimina deadlock. Sem ordenacao:

        T1 (A->B): trava A, espera B
        T2 (B->A): trava B, espera A     -> ciclo -> SQLSTATE 40P01

    Com uma ordem total sobre o UUID, toda transacao do sistema adquire os locks
    na mesma sequencia. A condicao de espera circular de Coffman deixa de ser
    possivel *por construcao*, entao nao ha deadlock para retentar.

    Travamos em SELECTs sequenciais, um por conta, em vez de um unico
    `WHERE id = ANY(...) ORDER BY id FOR UPDATE`: no segundo, a ordem de aquisicao
    depende do plano de execucao, e sob READ COMMITTED um re-fetch de EvalPlanQual
    pode em tese reordenar. Custa um round-trip a mais por transferencia -- custo
    que aparece medido no load test -- e em troca a ordem nao depende do planner.
    """
    locked: dict[uuid.UUID, Account] = {}
    strategy = settings.lock_strategy.value
    with metrics.observe(metrics.lock_wait, strategy=strategy):
        return await _acquire(session, account_ids, locked)


async def _acquire(
    session: AsyncSession, account_ids: list[uuid.UUID], locked: dict[uuid.UUID, Account]
) -> dict[uuid.UUID, Account]:
    for account_id in sorted(account_ids):  # ordem total, deterministica
        stmt = select(Account).where(Account.id == account_id)
        if settings.lock_strategy is LockStrategy.ROW_LOCK:
            stmt = stmt.with_for_update()
        # Em SERIALIZABLE nao travamos: o SSI do Postgres detecta o conflito de
        # dependencia e aborta com 40001, e o retry acontece em _with_retry.
        account = (await session.execute(stmt)).scalar_one_or_none()
        if account is None:
            raise AccountNotFoundError(account_id)
        locked[account_id] = account
    return locked


async def _lock_transaction(
    session: AsyncSession, transaction_id: uuid.UUID
) -> Transaction | None:
    """Trava a linha da transacao original antes de qualquer decisao sobre ela.

    Mesmo racional de _lock_accounts: sob SERIALIZABLE nao travamos, porque o SSI
    detecta o conflito e aborta com 40001 -- e o retry reexecuta a checagem inteira.
    O indice unico parcial (I4) continua sendo o arbitro final nas duas estrategias.
    """
    stmt = select(Transaction).where(Transaction.id == transaction_id)
    if settings.lock_strategy is LockStrategy.ROW_LOCK:
        stmt = stmt.with_for_update()
    return (await session.execute(stmt)).scalar_one_or_none()


# -----------------------------------------------------------------------------
# Core
# -----------------------------------------------------------------------------
async def post_transfer(session: AsyncSession, req: TransferRequest) -> Transaction:
    """Executa uma transferencia dentro da transacao ja aberta pelo chamador."""
    if req.amount <= 0:
        raise InvalidTransferError(f"Amount must be positive, got {req.amount}")
    if req.from_account_id == req.to_account_id:
        raise InvalidTransferError("Cannot transfer to the same account")

    accounts = await _lock_accounts(session, [req.from_account_id, req.to_account_id])
    src = accounts[req.from_account_id]
    dst = accounts[req.to_account_id]

    for account in (src, dst):
        if account.status is not AccountStatus.ACTIVE:
            raise AccountNotActiveError(f"Account {account.id} is {account.status.value}")
        if account.currency != req.currency:
            raise CurrencyMismatchError(
                f"Account {account.id} is {account.currency}, transfer is {req.currency}"
            )

    if not src.allow_negative and src.balance < req.amount:
        raise InsufficientFundsError(src.id, src.balance, req.amount, src.currency)

    txn = Transaction(
        id=uuid7(),
        kind=req.kind,
        external_ref=req.external_ref,
        meta=req.metadata,
    )
    session.add(txn)
    await session.flush()

    # Os UPDATEs vao na MESMA ordem total dos locks, nao na ordem origem->destino.
    # Sob ROW_LOCK isso e redundante (ja seguramos as duas linhas desde
    # _lock_accounts). Sob SERIALIZABLE nao ha lock explicito, entao a ordem dos
    # UPDATEs *e* a ordem de aquisicao de lock: A->B e B->A concorrentes formavam
    # espera circular e deadlockavam. Ordenar aqui fecha o buraco para as duas
    # estrategias -- a regra e "toda escrita em accounts respeita a ordem global".
    deltas = {src.id: -req.amount, dst.id: +req.amount}
    balances = {
        account_id: await _apply_delta(session, account_id, deltas[account_id])
        for account_id in sorted(deltas)
    }
    debit_balance, credit_balance = balances[src.id], balances[dst.id]

    session.add_all([
        Entry(transaction_id=txn.id, account_id=src.id, direction=EntryDirection.DEBIT,
              amount=req.amount, currency=req.currency, balance_after=debit_balance),
        Entry(transaction_id=txn.id, account_id=dst.id, direction=EntryDirection.CREDIT,
              amount=req.amount, currency=req.currency, balance_after=credit_balance),
    ])
    await session.flush()

    # Evento na MESMA transacao: se a transferencia commitou, o evento existe;
    # se nao commitou, o evento nao existe. Nunca ha divergencia entre os dois.
    await enqueue(
        session,
        event_type="transaction.posted",
        aggregate_id=txn.id,
        payload={
            "transaction_id": str(txn.id),
            "kind": txn.kind.value,
            "amount": req.amount,
            "currency": req.currency,
            "from_account_id": str(src.id),
            "to_account_id": str(dst.id),
            "external_ref": req.external_ref,
        },
    )
    return txn


async def _apply_delta(session: AsyncSession, account_id: uuid.UUID, delta: int) -> int:
    """UPDATE relativo (`balance = balance + delta`), nunca `balance = <valor lido>`.

    Escrever um valor calculado na aplicacao reintroduz o lost update caso alguem
    remova o FOR UPDATE. O UPDATE relativo continua correto mesmo sem lock; o lock
    esta ali para validar a suficiencia de saldo *antes* de escrever. Defesa em
    profundidade -- as duas coisas teriam que quebrar juntas para perder dinheiro.
    """
    stmt = (
        update(Account)
        .where(Account.id == account_id)
        .values(balance=Account.balance + delta, version=Account.version + 1)
        .returning(Account.balance)
        .execution_options(synchronize_session=False)
    )
    return (await session.execute(stmt)).scalar_one()


# -----------------------------------------------------------------------------
# Estorno
# -----------------------------------------------------------------------------
@dataclass(slots=True)
class ReversalRequest:
    transaction_id: uuid.UUID
    external_ref: str | None = None
    metadata: dict = field(default_factory=dict)


async def post_reversal(session: AsyncSession, req: ReversalRequest) -> Transaction:
    """Estorno cria uma NOVA transacao com os lancamentos espelhados.

    A transacao original nunca e mutada nem apagada: `entries` e append-only e a
    historia precisa continuar auditavel. O unico campo que muda na original e o
    `status`, que passa a 'reversed'.
    """
    # Ordem de lock entre tabelas: transactions ANTES de accounts, sempre.
    # Travar a original primeiro e o que torna a checagem de status autoritativa:
    # sob READ COMMITTED o perdedor bloqueia aqui e, ao destravar, o Postgres
    # re-busca a versao mais recente da linha (EvalPlanQual) -- ou seja, ja ve
    # status='reversed'. Sem esse lock os perdedores liam 'posted' obsoleto,
    # seguiam adiante e falhavam la na frente com "saldo insuficiente" -- erro
    # tecnicamente verdadeiro e semanticamente errado para o cliente.
    original = await _lock_transaction(session, req.transaction_id)
    if original is None:
        raise TransactionNotFoundError(req.transaction_id)
    if original.kind is TransactionKind.REVERSAL:
        raise InvalidReversalError("Cannot reverse a reversal")
    if original.status is TransactionStatus.REVERSED:
        raise AlreadyReversedError(f"Transaction {original.id} was already reversed")

    entries = (
        await session.execute(
            select(Entry).where(Entry.transaction_id == original.id).order_by(Entry.id)
        )
    ).scalars().all()
    if not entries:
        raise InvalidReversalError(f"Transaction {original.id} has no entries")

    accounts = await _lock_accounts(session, [e.account_id for e in entries])

    # Espelha: o que foi debitado volta como credito, e vice-versa.
    deltas: dict[uuid.UUID, int] = {}
    for entry in entries:
        signed = entry.amount if entry.direction is EntryDirection.DEBIT else -entry.amount
        deltas[entry.account_id] = deltas.get(entry.account_id, 0) + signed

    for account_id, delta in deltas.items():
        account = accounts[account_id]
        if delta < 0 and not account.allow_negative and account.balance + delta < 0:
            # O destino ja gastou o dinheiro. Ver README: limitacao conhecida --
            # sistemas reais permitem clawback com saldo negativo controlado.
            raise InsufficientFundsError(account.id, account.balance, -delta,
                                         account.currency)

    reversal = Transaction(
        id=uuid7(),
        kind=TransactionKind.REVERSAL,
        reverses_transaction_id=original.id,
        external_ref=req.external_ref,
        meta=req.metadata,
    )
    session.add(reversal)
    await session.flush()  # I4 dispara aqui: o indice unico nao e deferred

    balances = {
        account_id: await _apply_delta(session, account_id, deltas[account_id])
        for account_id in sorted(deltas)
    }

    session.add_all([
        Entry(
            transaction_id=reversal.id,
            account_id=entry.account_id,
            direction=(
                EntryDirection.CREDIT
                if entry.direction is EntryDirection.DEBIT
                else EntryDirection.DEBIT
            ),
            amount=entry.amount,
            currency=entry.currency,
            balance_after=balances[entry.account_id],
        )
        for entry in entries
    ])
    original.status = TransactionStatus.REVERSED
    await session.flush()

    await enqueue(
        session,
        event_type="transaction.reversed",
        aggregate_id=reversal.id,
        payload={
            "transaction_id": str(reversal.id),
            "reverses_transaction_id": str(original.id),
            "kind": reversal.kind.value,
            "external_ref": req.external_ref,
        },
    )
    return reversal


# -----------------------------------------------------------------------------
# Unit of work: transacao + estrategia de lock
# -----------------------------------------------------------------------------
type UnitOfWork[T] = Callable[[AsyncSession], Awaitable[T]]

# Nome da constraint -> erro de dominio. A constraint do banco e a ultima linha de
# defesa; traduzir aqui e o que evita 500 quando ela dispara.
_CONSTRAINT_ERRORS: dict[str, Callable[[str], LedgerError]] = {
    "transactions_single_reversal_uniq": lambda _: AlreadyReversedError(
        "Transaction was already reversed by a concurrent request"
    ),
    "idempotency_keys_pkey": lambda _: IdempotencyInFlightError(
        "A concurrent request claimed this Idempotency-Key"
    ),
}


async def run_in_transaction[T](
    session_factory: async_sessionmaker[AsyncSession], work: UnitOfWork[T]
) -> T:
    """Executa `work` numa transacao, aplicando a estrategia de lock configurada.

    O claim da Idempotency-Key e o lancamento que ele protege precisam commitar
    JUNTOS -- nao pode existir estado onde a transferencia foi confirmada mas o
    replay nao funciona. Por isso a gestao de transacao vive aqui, generica, em vez
    de dentro de cada operacao.
    """
    strategy = settings.lock_strategy.value
    if settings.lock_strategy is LockStrategy.SERIALIZABLE:
        return await _run_serializable(session_factory, work)

    with metrics.observe(metrics.transaction_duration, kind="unit", strategy=strategy):
        async with session_factory() as session:
            try:
                async with session.begin():
                    return await work(session)
            except IntegrityError as exc:
                raise _translate_integrity_error(exc) from exc
            except DBAPIError as exc:
                if sqlstate(exc) == SQLSTATE_DEADLOCK_DETECTED:
                    metrics.deadlocks.inc()
                raise


async def _run_serializable[T](
    session_factory: async_sessionmaker[AsyncSession], work: UnitOfWork[T]
) -> T:
    """SERIALIZABLE + retry em 40001.

    O SSI do Postgres rastreia dependencias de leitura/escrita e aborta uma das
    transacoes quando detecta um ciclo perigoso. Sem lock explicito o codigo de
    dominio fica mais limpo -- mas sob conta quente a taxa de aborto sobe com a
    contencao e o throughput efetivo cai. E esse trade-off que o load test mede.

    O retry so e seguro porque a unidade de trabalho inclui o claim de
    idempotencia: a tentativa nova reusa a mesma chave e nao duplica lancamento.
    """
    last: BaseException | None = None

    for attempt in range(settings.serializable_max_retries):
        session = session_factory()
        try:
            await session.connection(execution_options={"isolation_level": "SERIALIZABLE"})
            with metrics.observe(metrics.transaction_duration, kind="unit",
                                 strategy=LockStrategy.SERIALIZABLE.value):
                result = await work(session)
                await session.commit()
            metrics.serialization_retries.observe(attempt)
            return result
        except (DBAPIError, IntegrityError) as exc:
            await session.rollback()
            state = sqlstate(exc)
            if state == SQLSTATE_DEADLOCK_DETECTED:
                metrics.deadlocks.inc()
            if state == SQLSTATE_SERIALIZATION_FAILURE:
                metrics.serialization_failures.inc()
                last = exc
                # Backoff exponencial COM jitter. Sem jitter as transacoes abortadas
                # voltam todas no mesmo instante e colidem de novo -- o retry vira
                # amplificador de contencao em vez de mitigacao.
                delay = settings.serializable_base_backoff_s * (2**attempt)
                await asyncio.sleep(delay * random.uniform(0.5, 1.5))  # noqa: S311
                continue
            if isinstance(exc, IntegrityError):
                raise _translate_integrity_error(exc) from exc
            raise
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()

    raise SerializationExhaustedError(
        f"aborted after {settings.serializable_max_retries} serialization failures"
    ) from last


def _translate_integrity_error(exc: IntegrityError) -> Exception:
    constraint = getattr(getattr(exc, "orig", None), "constraint_name", None) or ""
    message = str(exc)
    for name, factory in _CONSTRAINT_ERRORS.items():
        if name in constraint or name in message:
            return factory(name)
    if "accounts_balance_non_negative" in constraint or "accounts_balance_non_negative" in message:
        return LedgerInvariantError("accounts_balance_non_negative")
    if "entries_balanced" in message or "unbalanced" in message:
        return LedgerInvariantError("entries_balanced_trg")
    return exc


# -----------------------------------------------------------------------------
# Atalhos
# -----------------------------------------------------------------------------
async def execute_transfer(
    session_factory: async_sessionmaker[AsyncSession], req: TransferRequest
) -> Transaction:
    return await run_in_transaction(session_factory, lambda s: post_transfer(s, req))


async def execute_reversal(
    session_factory: async_sessionmaker[AsyncSession], req: ReversalRequest
) -> Transaction:
    return await run_in_transaction(session_factory, lambda s: post_reversal(s, req))
