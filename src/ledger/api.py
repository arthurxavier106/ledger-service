"""Borda HTTP.

Escopo desta fatia vertical: contas, transferencia e extrato. Idempotencia,
outbox e /metrics entram na proxima fatia -- ver README, secao Roadmap.
"""

from __future__ import annotations

import base64
import uuid
from collections.abc import AsyncIterator
from typing import Annotated

from fastapi import APIRouter, Depends, Header, Query, Request, Response
from fastapi.responses import JSONResponse
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from ledger.errors import AccountNotFoundError, LedgerError, TransactionNotFoundError
from ledger.idempotency import IdempotencyCache, execute_idempotent
from ledger.ids import uuid7
from ledger.models import Account, Entry, Transaction
from ledger.schemas import (
    AccountResponse,
    CreateAccountRequest,
    EntryResponse,
    Page,
    ReversalRequestBody,
    TransactionResponse,
    TransferRequestBody,
)
from ledger.service import ReversalRequest, TransferRequest, post_reversal, post_transfer

router = APIRouter(prefix="/v1")


def get_session_factory(request: Request) -> async_sessionmaker[AsyncSession]:
    """A factory vem de app.state, nao de um global de modulo: e o que permite a
    suite apontar a API para um banco de teste sem monkeypatch."""
    return request.app.state.session_factory


FactoryDep = Annotated[async_sessionmaker[AsyncSession], Depends(get_session_factory)]


def get_cache(request: Request) -> IdempotencyCache:
    return request.app.state.idempotency_cache


CacheDep = Annotated[IdempotencyCache, Depends(get_cache)]

IdempotencyKeyHeader = Annotated[
    str | None,
    Header(
        alias="Idempotency-Key",
        max_length=255,
        description=(
            "Chave de idempotencia. Repetir a mesma chave com o MESMO corpo devolve a "
            "resposta original (header `Idempotency-Replayed: true`). Com corpo "
            "diferente devolve 422. Validade: 24h."
        ),
    ),
]


async def get_session(
    factory: FactoryDep,
) -> AsyncIterator[AsyncSession]:
    async with factory() as session:
        yield session


SessionDep = Annotated[AsyncSession, Depends(get_session)]


def _encode_cursor(entry_id: int) -> str:
    return base64.urlsafe_b64encode(f"entry:{entry_id}".encode()).decode()


def _decode_cursor(cursor: str | None) -> int | None:
    if not cursor:
        return None
    try:
        return int(base64.urlsafe_b64decode(cursor.encode()).decode().split(":", 1)[1])
    except (ValueError, IndexError, UnicodeDecodeError) as exc:
        raise LedgerError(f"Malformed cursor: {cursor}") from exc


async def ledger_error_handler(_: Request, exc: LedgerError) -> JSONResponse:
    """RFC 9457 (application/problem+json). O campo `type` e estavel: e o que o
    cliente programa em cima, nao a mensagem."""
    return JSONResponse(
        status_code=exc.status_code,
        content=exc.problem(),
        media_type="application/problem+json",
    )


@router.post("/accounts", response_model=AccountResponse, status_code=201)
async def create_account(body: CreateAccountRequest, session: SessionDep) -> Account:
    account = Account(
        id=uuid7(),
        external_id=body.external_id,
        owner_id=body.owner_id,
        currency=body.currency,
        type=body.type,
        allow_negative=body.allow_negative,
        balance=0,
    )
    session.add(account)
    await session.commit()
    return account


@router.get("/accounts/{account_id}", response_model=AccountResponse)
async def get_account(account_id: uuid.UUID, session: SessionDep) -> Account:
    account = await session.get(Account, account_id)
    if account is None:
        raise AccountNotFoundError(account_id)
    return account


@router.get("/accounts/{account_id}/entries", response_model=Page)
async def list_entries(
    account_id: uuid.UUID,
    session: SessionDep,
    limit: Annotated[int, Query(ge=1, le=200)] = 50,
    cursor: str | None = None,
) -> Page:
    """Extrato por keyset. Servido inteiro pelo indice (account_id, id DESC),
    entao a pagina 1 e a pagina 10.000 custam o mesmo."""
    if await session.get(Account, account_id) is None:
        raise AccountNotFoundError(account_id)

    stmt = select(Entry).where(Entry.account_id == account_id)
    after = _decode_cursor(cursor)
    if after is not None:
        stmt = stmt.where(Entry.id < after)

    stmt = stmt.order_by(Entry.id.desc()).limit(limit + 1)
    rows = (await session.execute(stmt)).scalars().all()
    has_more = len(rows) > limit
    page = rows[:limit]
    return Page(
        data=[EntryResponse.model_validate(e) for e in page],
        next_cursor=_encode_cursor(page[-1].id) if page and has_more else None,
        has_more=has_more,
    )


async def _serialize(session: AsyncSession, txn: Transaction) -> dict:
    entries = (
        await session.execute(
            select(Entry).where(Entry.transaction_id == txn.id).order_by(Entry.id)
        )
    ).scalars().all()
    return TransactionResponse(
        id=txn.id, kind=txn.kind, status=txn.status,
        reverses_transaction_id=txn.reverses_transaction_id,
        external_ref=txn.external_ref, created_at=txn.created_at,
        entries=[EntryResponse.model_validate(e) for e in entries],
    ).model_dump(mode="json")


@router.post("/transfers", response_model=TransactionResponse, status_code=201)
async def create_transfer(
    body: TransferRequestBody,
    factory: FactoryDep,
    cache: CacheDep,
    response: Response,
    idempotency_key: IdempotencyKeyHeader = None,
) -> TransactionResponse:
    async def operation(session: AsyncSession):
        txn = await post_transfer(session, TransferRequest(
            from_account_id=body.from_account_id,
            to_account_id=body.to_account_id,
            amount=body.amount,
            currency=body.currency,
            external_ref=body.external_ref,
            metadata=body.metadata,
        ))
        return 201, await _serialize(session, txn), txn.id

    result = await execute_idempotent(
        factory, cache,
        scope="POST /v1/transfers",
        key=idempotency_key,
        payload=body.model_dump(mode="json"),
        operation=operation,
    )
    if result.replayed:
        response.headers["Idempotency-Replayed"] = "true"
    return TransactionResponse.model_validate(result.body)


@router.post("/transactions/{transaction_id}/reversal",
             response_model=TransactionResponse, status_code=201)
async def create_reversal(
    transaction_id: uuid.UUID,
    factory: FactoryDep,
    cache: CacheDep,
    response: Response,
    body: ReversalRequestBody | None = None,
    idempotency_key: IdempotencyKeyHeader = None,
) -> TransactionResponse:
    """Estorno.

    Cria uma nova transacao com os lancamentos espelhados; a original nunca e
    mutada. Alem da Idempotency-Key, o indice unico parcial (I4) garante no banco
    que uma transacao e estornada no maximo uma vez, mesmo sem chave.
    """
    payload = (body or ReversalRequestBody()).model_dump(mode="json")

    async def operation(session: AsyncSession):
        txn = await post_reversal(session, ReversalRequest(
            transaction_id=transaction_id,
            external_ref=payload.get("external_ref"),
            metadata=payload.get("metadata") or {},
        ))
        return 201, await _serialize(session, txn), txn.id

    result = await execute_idempotent(
        factory, cache,
        scope=f"POST /v1/transactions/{transaction_id}/reversal",
        key=idempotency_key,
        payload=payload,
        operation=operation,
    )
    if result.replayed:
        response.headers["Idempotency-Replayed"] = "true"
    return TransactionResponse.model_validate(result.body)


@router.get("/transactions/{transaction_id}", response_model=TransactionResponse)
async def get_transaction(transaction_id: uuid.UUID, session: SessionDep) -> TransactionResponse:
    txn = await session.get(Transaction, transaction_id)
    if txn is None:
        raise TransactionNotFoundError(transaction_id)
    return TransactionResponse.model_validate(await _serialize(session, txn))
