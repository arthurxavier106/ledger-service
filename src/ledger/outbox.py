"""Transactional outbox e worker de entrega de webhooks.

O evento e gravado na MESMA transacao do lancamento (ver enqueue). A entrega
acontece depois, num processo separado, lendo a fila com FOR UPDATE SKIP LOCKED.

Contrato com o cliente: entrega **at-least-once**. Exactly-once nao existe em rede
-- prometer isso seria mentira. O receptor deve deduplicar por X-Ledger-Event-Id,
que e estavel entre as tentativas.
"""

from __future__ import annotations

import asyncio
import datetime as dt
import hashlib
import hmac
import json
import logging
import random
import uuid
from dataclasses import dataclass

import httpx
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from ledger import metrics
from ledger.config import settings

logger = logging.getLogger(__name__)

SIGNATURE_HEADER = "X-Ledger-Signature"
EVENT_ID_HEADER = "X-Ledger-Event-Id"
EVENT_TYPE_HEADER = "X-Ledger-Event-Type"


# -----------------------------------------------------------------------------
# Assinatura
# -----------------------------------------------------------------------------
def sign(secret: bytes, timestamp: int, body: str) -> str:
    """Assinatura no estilo Stripe: t=<unix>,v1=<hmac_sha256(secret, "t.body")>.

    O timestamp entra DENTRO do que e assinado. Sem isso, quem interceptasse uma
    entrega poderia reenvia-la indefinidamente com a assinatura ainda valida --
    o receptor rejeita eventos com t muito antigo.
    """
    signed_payload = f"{timestamp}.{body}".encode()
    digest = hmac.new(secret, signed_payload, hashlib.sha256).hexdigest()
    return f"t={timestamp},v1={digest}"


def verify(secret: bytes, header: str, body: str, tolerance_s: int = 300) -> bool:
    """Verificacao do lado do receptor. Existe aqui para o teste provar que a
    assinatura fecha, e serve de referencia para quem for consumir o webhook."""
    try:
        parts = dict(piece.split("=", 1) for piece in header.split(","))
        timestamp = int(parts["t"])
        received = parts["v1"]
    except (ValueError, KeyError):
        return False

    if abs(int(dt.datetime.now(dt.UTC).timestamp()) - timestamp) > tolerance_s:
        return False

    expected = hmac.new(secret, f"{timestamp}.{body}".encode(), hashlib.sha256).hexdigest()
    # compare_digest: comparacao em tempo constante, senao o tempo de resposta
    # vaza quantos bytes iniciais da assinatura estavam certos.
    return hmac.compare_digest(expected, received)


# -----------------------------------------------------------------------------
# Producao: roda DENTRO da transacao do lancamento
# -----------------------------------------------------------------------------
async def enqueue(
    session: AsyncSession, *, event_type: str, aggregate_id: uuid.UUID, payload: dict
) -> int:
    """Enfileira o evento para todos os endpoints ativos inscritos no tipo.

    Precisa ser chamado dentro da transacao que produziu o lancamento -- e isso
    que garante que nunca existe transacao confirmada sem evento, nem evento sem
    transacao. Retorna quantas linhas foram criadas.
    """
    result = await session.execute(
        text("""
        INSERT INTO outbox (endpoint_id, event_type, aggregate_id, payload)
        SELECT e.id, :event_type, :aggregate_id, CAST(:payload AS jsonb)
          FROM webhook_endpoints e
         WHERE e.active
           AND (cardinality(e.event_types) = 0 OR :event_type = ANY(e.event_types))
        RETURNING id
        """),
        {"event_type": event_type, "aggregate_id": aggregate_id,
         "payload": json.dumps(payload)},
    )
    return len(result.fetchall())


# -----------------------------------------------------------------------------
# Consumo
# -----------------------------------------------------------------------------
@dataclass(slots=True)
class ClaimedEvent:
    id: int
    endpoint_id: uuid.UUID
    url: str
    secret: bytes
    event_type: str
    aggregate_id: uuid.UUID
    payload: dict
    attempts: int


async def claim_batch(session: AsyncSession, limit: int | None = None) -> list[ClaimedEvent]:
    """Reivindica ate `limit` eventos vencidos.

    FOR UPDATE SKIP LOCKED e o que permite rodar N workers em paralelo sem
    coordenacao externa: cada um PULA as linhas ja travadas por outro em vez de
    esperar por elas. E o padrao canonico de fila em Postgres, e evita puxar
    Kafka ou RabbitMQ para dentro do projeto so para entregar webhook.

    O claim tambem empurra next_attempt_at para frente (lease). Se o worker morrer
    no meio da entrega, o evento nao fica preso em 'delivering' para sempre nem
    volta imediatamente num loop apertado -- ele reaparece apos o lease.
    """
    limit = limit or settings.outbox_batch_size
    result = await session.execute(
        text("""
        WITH claimed AS (
            SELECT id FROM outbox
             WHERE status IN ('pending','delivering')
               AND next_attempt_at <= now()
             ORDER BY next_attempt_at, id
             LIMIT :limit
             FOR UPDATE SKIP LOCKED
        )
        UPDATE outbox o
           SET status = 'delivering',
               attempts = o.attempts + 1,
               next_attempt_at = now() + make_interval(secs => :lease)
          FROM claimed c, webhook_endpoints e
         WHERE o.id = c.id AND e.id = o.endpoint_id
        RETURNING o.id, o.endpoint_id, e.url, e.secret, o.event_type,
                  o.aggregate_id, o.payload, o.attempts
        """),
        {"limit": limit, "lease": settings.outbox_lease_seconds},
    )
    return [
        ClaimedEvent(
            id=row.id, endpoint_id=row.endpoint_id, url=row.url,
            secret=bytes(row.secret), event_type=row.event_type,
            aggregate_id=row.aggregate_id, payload=row.payload, attempts=row.attempts,
        )
        for row in result
    ]


def backoff_delay(attempts: int) -> float:
    """Backoff exponencial com jitter, teto de 1 hora.

    O jitter nao e enfeite: sem ele, uma queda do endpoint do cliente faz TODOS os
    eventos pendentes retentarem no mesmo instante e derrubarem o cliente de novo
    assim que ele volta. O retry vira amplificador da falha em vez de mitigacao.
    """
    base = min(2**attempts, 3600)
    return base * random.uniform(0.5, 1.5)  # noqa: S311 - jitter, nao criptografia


async def mark_delivered(session: AsyncSession, event_id: int) -> None:
    await session.execute(
        text("UPDATE outbox SET status='delivered', delivered_at=now(), last_error=NULL"
             " WHERE id = :id"),
        {"id": event_id},
    )


async def schedule_retry(session: AsyncSession, event: ClaimedEvent, error: str) -> bool:
    """Reagenda ou marca como dead. Retorna True se ainda havera nova tentativa."""
    if event.attempts >= settings.outbox_max_attempts:
        await session.execute(
            text("UPDATE outbox SET status='dead', last_error=:err WHERE id=:id"),
            {"id": event.id, "err": error[:1000]},
        )
        logger.error("outbox event %s dead after %s attempts", event.id, event.attempts)
        return False

    await session.execute(
        text("UPDATE outbox SET status='pending', last_error=:err,"
             " next_attempt_at = now() + make_interval(secs => :delay) WHERE id=:id"),
        {"id": event.id, "err": error[:1000], "delay": backoff_delay(event.attempts)},
    )
    return True


async def deliver(client: httpx.AsyncClient, event: ClaimedEvent) -> httpx.Response:
    body = json.dumps(
        {
            "id": event.id,
            "type": event.event_type,
            "aggregate_id": str(event.aggregate_id),
            "data": event.payload,
        },
        separators=(",", ":"),
    )
    timestamp = int(dt.datetime.now(dt.UTC).timestamp())
    return await client.post(
        event.url,
        content=body,
        headers={
            "Content-Type": "application/json",
            SIGNATURE_HEADER: sign(event.secret, timestamp, body),
            EVENT_ID_HEADER: str(event.id),
            EVENT_TYPE_HEADER: event.event_type,
        },
        timeout=settings.webhook_timeout_seconds,
    )


async def process_batch(
    session_factory: async_sessionmaker[AsyncSession], client: httpx.AsyncClient
) -> int:
    """Uma rodada do worker. Retorna quantos eventos foram processados."""
    async with session_factory() as session, session.begin():
        events = await claim_batch(session)

    if not events:
        return 0

    for event in events:
        outcome = "delivered"
        error: str | None = None
        try:
            response = await deliver(client, event)
            if response.status_code >= 400:
                error = f"HTTP {response.status_code}: {response.text[:200]}"
        except Exception as exc:
            error = f"{type(exc).__name__}: {exc}"

        async with session_factory() as session, session.begin():
            if error is None:
                await mark_delivered(session, event.id)
            else:
                outcome = "retry" if await schedule_retry(session, event, error) else "dead"
        metrics.webhook_deliveries.labels(outcome=outcome).inc()

    return len(events)


async def run_worker(
    session_factory: async_sessionmaker[AsyncSession], *, stop: asyncio.Event | None = None
) -> None:
    async with httpx.AsyncClient() as client:
        while stop is None or not stop.is_set():
            processed = await process_batch(session_factory, client)
            if processed == 0:
                await asyncio.sleep(settings.outbox_poll_interval_seconds)
