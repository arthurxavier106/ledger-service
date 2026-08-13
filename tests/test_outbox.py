"""Transactional outbox: atomicidade, SKIP LOCKED, backoff e assinatura."""

from __future__ import annotations

import asyncio
import datetime as dt
import json

import httpx
import pytest
from sqlalchemy import text

from ledger.config import settings
from ledger.errors import InsufficientFundsError
from ledger.outbox import (
    ClaimedEvent,
    claim_batch,
    deliver,
    mark_delivered,
    process_batch,
    schedule_retry,
    verify,
)
from ledger.service import TransferRequest, execute_transfer

pytestmark = pytest.mark.asyncio(loop_scope="session")

# Nota de ordem nos testes: um endpoint so recebe eventos emitidos DEPOIS do seu
# registro. Como account_factory abre saldo com uma transferencia real -- que ja
# emite evento --, as contas sao criadas antes do endpoint para que cada teste
# observe apenas os eventos que ele mesmo produziu.


async def _events(session_factory, event_type: str | None = None) -> list:
    async with session_factory() as s:
        rows = await s.execute(
            text("SELECT id, status, attempts, event_type, payload, next_attempt_at,"
                 " last_error FROM outbox"
                 " WHERE (CAST(:event_type AS text) IS NULL"
                 "      OR event_type = CAST(:event_type AS text)) ORDER BY id"),
            {"event_type": event_type})
        return list(rows)


# --- atomicidade -----------------------------------------------------------
async def test_event_is_written_in_the_same_transaction(session_factory,
                                                        account_factory, webhook_factory):
    src = await account_factory(balance=1_000)
    dst = await account_factory()
    await webhook_factory()

    txn = await execute_transfer(session_factory, TransferRequest(src.id, dst.id, 300, "BRL"))

    events = await _events(session_factory, "transaction.posted")
    assert len(events) == 1
    assert events[0].payload["transaction_id"] == str(txn.id)
    assert events[0].status == "pending"


async def test_rolled_back_transfer_leaves_no_event(session_factory, account_factory,
                                                    webhook_factory):
    """O ponto inteiro do outbox: nao existe evento sem transacao confirmada."""
    src = await account_factory(balance=100)
    dst = await account_factory()
    await webhook_factory()

    with pytest.raises(InsufficientFundsError):
        await execute_transfer(session_factory, TransferRequest(src.id, dst.id, 9_999, "BRL"))

    assert await _events(session_factory) == []


async def test_reversal_emits_its_own_event(session_factory, account_factory,
                                            webhook_factory):
    src = await account_factory(balance=1_000)
    dst = await account_factory()
    await webhook_factory()
    txn = await execute_transfer(session_factory, TransferRequest(src.id, dst.id, 200, "BRL"))

    from ledger.service import ReversalRequest, execute_reversal

    await execute_reversal(session_factory, ReversalRequest(txn.id))

    kinds = {e.event_type for e in await _events(session_factory)}
    assert kinds == {"transaction.posted", "transaction.reversed"}


async def test_only_subscribed_and_active_endpoints_receive(session_factory,
                                                            account_factory, webhook_factory):
    src = await account_factory(balance=1_000)
    dst = await account_factory()

    await webhook_factory(event_types=["transaction.posted"])          # recebe
    await webhook_factory(event_types=[])                              # recebe (todos)
    await webhook_factory(event_types=["transaction.reversed"])        # nao recebe
    await webhook_factory(event_types=[], active=False)                # nao recebe
    await execute_transfer(session_factory, TransferRequest(src.id, dst.id, 100, "BRL"))

    assert len(await _events(session_factory, "transaction.posted")) == 2


# --- SKIP LOCKED -----------------------------------------------------------
async def test_concurrent_workers_claim_disjoint_sets(session_factory, account_factory,
                                                      webhook_factory):
    """FOR UPDATE SKIP LOCKED: dois workers em paralelo nao pegam o mesmo evento
    e nenhum fica bloqueado esperando o outro."""
    src = await account_factory(balance=100_000)
    dst = await account_factory()
    await webhook_factory()
    for _ in range(10):
        await execute_transfer(session_factory, TransferRequest(src.id, dst.id, 10, "BRL"))

    claimed: list[list[int]] = []

    async def worker():
        async with session_factory() as s, s.begin():
            events = await claim_batch(s, limit=5)
            claimed.append([e.id for e in events])
            await asyncio.sleep(0.3)  # segura o lock enquanto o outro tenta

    started = asyncio.get_event_loop().time()
    await asyncio.gather(worker(), worker())
    elapsed = asyncio.get_event_loop().time() - started

    a, b = claimed
    assert len(a) == len(b) == 5
    assert set(a).isdisjoint(set(b)), "dois workers pegaram o mesmo evento"
    assert elapsed < 0.6, "o segundo worker bloqueou em vez de pular as linhas travadas"


async def test_claim_sets_lease_so_dead_worker_does_not_spin(session_factory,
                                                             account_factory, webhook_factory):
    """Ao reivindicar, next_attempt_at vai para frente. Se o worker morrer, o evento
    reaparece depois do lease em vez de voltar num loop apertado."""
    src = await account_factory(balance=1_000)
    dst = await account_factory()
    await webhook_factory()
    await execute_transfer(session_factory, TransferRequest(src.id, dst.id, 50, "BRL"))

    async with session_factory() as s, s.begin():
        first = await claim_batch(s, limit=10)
    assert len(first) == 1

    async with session_factory() as s, s.begin():
        again = await claim_batch(s, limit=10)
    assert again == [], "evento reivindicado nao pode reaparecer antes do lease"

    events = await _events(session_factory)
    assert events[0].status == "delivering"
    assert events[0].attempts == 1


async def test_retry_then_dead_after_max_attempts(session_factory, account_factory,
                                                  webhook_factory):
    src = await account_factory(balance=1_000)
    dst = await account_factory()
    endpoint, _ = await webhook_factory()
    await execute_transfer(session_factory, TransferRequest(src.id, dst.id, 50, "BRL"))

    event_id = (await _events(session_factory))[0].id

    for attempt in range(1, settings.outbox_max_attempts + 1):
        event = ClaimedEvent(id=event_id, endpoint_id=endpoint.id, url=endpoint.url,
                             secret=b"x", event_type="transaction.posted",
                             aggregate_id=src.id, payload={}, attempts=attempt)
        async with session_factory() as s, s.begin():
            will_retry = await schedule_retry(s, event, "HTTP 500")
        expected = attempt < settings.outbox_max_attempts
        assert will_retry is expected, f"tentativa {attempt}"

    final = (await _events(session_factory))[0]
    assert final.status == "dead"
    assert "HTTP 500" in final.last_error


async def test_mark_delivered_clears_error(session_factory, account_factory, webhook_factory):
    src = await account_factory(balance=1_000)
    dst = await account_factory()
    await webhook_factory()
    await execute_transfer(session_factory, TransferRequest(src.id, dst.id, 50, "BRL"))
    event_id = (await _events(session_factory))[0].id

    async with session_factory() as s, s.begin():
        await mark_delivered(s, event_id)

    async with session_factory() as s:
        row = (await s.execute(text(
            "SELECT status, delivered_at, last_error FROM outbox WHERE id=:i"),
            {"i": event_id})).one()
    assert row.status == "delivered"
    assert row.delivered_at is not None
    assert row.last_error is None


# --- entrega ---------------------------------------------------------------
async def test_delivery_sends_signed_request():
    received: dict = {}

    def handler(request: httpx.Request) -> httpx.Response:
        received["headers"] = dict(request.headers)
        received["body"] = request.content.decode()
        return httpx.Response(200)

    secret = b"k" * 32
    event = ClaimedEvent(id=42, endpoint_id=None, url="https://merchant.example/hook",
                         secret=secret, event_type="transaction.posted",
                         aggregate_id=None, payload={"amount": 100}, attempts=1)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        response = await deliver(client, event)

    assert response.status_code == 200
    assert received["headers"]["x-ledger-event-id"] == "42"
    assert received["headers"]["x-ledger-event-type"] == "transaction.posted"
    assert verify(secret, received["headers"]["x-ledger-signature"], received["body"])
    assert json.loads(received["body"])["data"] == {"amount": 100}


async def test_process_batch_delivers_and_marks(session_factory, account_factory,
                                                webhook_factory):
    src = await account_factory(balance=1_000)
    dst = await account_factory()
    await webhook_factory()
    await execute_transfer(session_factory, TransferRequest(src.id, dst.id, 50, "BRL"))

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _r: httpx.Response(200))
    ) as client:
        processed = await process_batch(session_factory, client)

    assert processed == 1
    assert (await _events(session_factory))[0].status == "delivered"


async def test_process_batch_reschedules_on_server_error(session_factory, account_factory,
                                                         webhook_factory):
    src = await account_factory(balance=1_000)
    dst = await account_factory()
    await webhook_factory()
    await execute_transfer(session_factory, TransferRequest(src.id, dst.id, 50, "BRL"))

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _r: httpx.Response(503, text="indisponivel"))
    ) as client:
        await process_batch(session_factory, client)

    event = (await _events(session_factory))[0]
    assert event.status == "pending"
    assert event.attempts == 1
    assert event.next_attempt_at > dt.datetime.now(dt.UTC)


async def test_event_id_is_stable_across_retries(session_factory, account_factory,
                                                 webhook_factory):
    """Contrato at-least-once: o receptor deduplica por X-Ledger-Event-Id, entao
    ele nao pode mudar entre tentativas."""
    src = await account_factory(balance=1_000)
    dst = await account_factory()
    await webhook_factory()
    await execute_transfer(session_factory, TransferRequest(src.id, dst.id, 50, "BRL"))

    seen: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request.headers["x-ledger-event-id"])
        return httpx.Response(500)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        await process_batch(session_factory, client)
        async with session_factory() as s, s.begin():
            await s.execute(text("UPDATE outbox SET next_attempt_at = now()"))
        await process_batch(session_factory, client)

    assert len(seen) == 2
    assert seen[0] == seen[1]


# --- worker ----------------------------------------------------------------
async def test_worker_stops_when_asked(session_factory):
    """O worker precisa sair no sinal de parada: sem isso o container fica pendurado
    no shutdown e o orquestrador acaba matando ele no meio de uma entrega."""
    from ledger.outbox import run_worker

    stop = asyncio.Event()
    stop.set()

    await asyncio.wait_for(run_worker(session_factory, stop=stop), timeout=5)


async def test_worker_drains_the_queue_then_idles(session_factory, account_factory,
                                                  webhook_factory):
    import ledger.outbox as outbox_module

    src = await account_factory(balance=10_000)
    dst = await account_factory()
    await webhook_factory()
    for _ in range(3):
        await execute_transfer(session_factory, TransferRequest(src.id, dst.id, 10, "BRL"))

    stop = asyncio.Event()
    delivered: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        delivered.append(request.headers["x-ledger-event-id"])
        if len(delivered) == 3:
            stop.set()
        return httpx.Response(200)

    original = httpx.AsyncClient

    def patched(*args, **kwargs):
        return original(*args, transport=httpx.MockTransport(handler), **kwargs)

    outbox_module.httpx.AsyncClient = patched
    try:
        await asyncio.wait_for(outbox_module.run_worker(session_factory, stop=stop), timeout=15)
    finally:
        outbox_module.httpx.AsyncClient = original

    assert len(delivered) == 3
    assert all(e.status == "delivered" for e in await _events(session_factory))
