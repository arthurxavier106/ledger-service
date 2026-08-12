"""Idempotencia em duas camadas.

A regra que nao se quebra:

    Redis e CACHE. Postgres e a VERDADE.

Um miss no Redis cai no Postgres; um Redis vazio (flush, failover, cold start) pode
deixar a API mais lenta, nunca pode causar transferencia duplicada. Todo acesso ao
cache e best-effort e falha em silencio -- com log e metrica, sem derrubar a
requisicao.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import logging
import time
import uuid
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from typing import Any

from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from ledger.errors import IdempotencyInFlightError, IdempotencyKeyReuseError
from ledger.models import IdempotencyKey, IdempotencyStatus
from ledger.service import run_in_transaction

logger = logging.getLogger(__name__)

# (status_code, corpo da resposta, id da transacao criada)
type Operation = Callable[[AsyncSession], Awaitable[tuple[int, dict, uuid.UUID | None]]]


def fingerprint(payload: Mapping[str, Any]) -> str:
    """sha256 do payload canonicalizado.

    Serve para detectar reuso de chave com corpo diferente. Sem isso, um cliente que
    reaproveita a chave por engano recebe silenciosamente a resposta da requisicao
    anterior -- acha que mandou R$ 500 e leva o 201 do R$ 50.
    """
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(canonical.encode()).hexdigest()


@dataclass(slots=True)
class IdempotentResult:
    status_code: int
    body: dict
    replayed: bool


# -----------------------------------------------------------------------------
# Cache (nao-autoritativo)
# -----------------------------------------------------------------------------
class IdempotencyCache:
    """Cache de idempotencia com circuit breaker.

    Degradar para "so Postgres" quando o Redis cai e correto, mas ingenuidade custa
    caro: sem breaker, TODA requisicao paga um timeout de conexao antes de desistir.
    Medido no load test, isso levou o p50 de ~30ms para ~920ms -- o servico ficava
    de pe e inutil ao mesmo tempo, que e a pior falha possivel.

    Com breaker: apos `failure_threshold` falhas seguidas o cache se desliga por
    `cooldown_seconds` e responde None instantaneamente. Correcao nao muda (Postgres
    e a verdade); so o custo do caminho degradado.
    """

    def __init__(self, redis: Any | None = None, ttl_seconds: int = 24 * 3600,
                 failure_threshold: int = 3, cooldown_seconds: float = 30.0) -> None:
        self._redis = redis
        self._ttl = ttl_seconds
        self._failure_threshold = failure_threshold
        self._cooldown = cooldown_seconds
        self._consecutive_failures = 0
        self._open_until = 0.0

    @property
    def circuit_open(self) -> bool:
        return time.monotonic() < self._open_until

    def _usable(self) -> bool:
        return self._redis is not None and not self.circuit_open

    def _on_failure(self) -> None:
        self._consecutive_failures += 1
        if self._consecutive_failures >= self._failure_threshold and not self.circuit_open:
            self._open_until = time.monotonic() + self._cooldown
            logger.warning(
                "idempotency cache circuit opened for %.0fs after %d failures",
                self._cooldown, self._consecutive_failures,
            )

    def _on_success(self) -> None:
        self._consecutive_failures = 0

    @staticmethod
    def _redis_key(scope: str, key: str) -> str:
        return f"idem:{scope}:{key}"

    async def get(self, scope: str, key: str) -> dict | None:
        if not self._usable():
            return None
        try:
            raw = await self._redis.get(self._redis_key(scope, key))
        except Exception:
            logger.warning("idempotency cache read failed", exc_info=True)
            self._on_failure()
            return None
        self._on_success()
        return json.loads(raw) if raw else None

    async def set(self, scope: str, key: str, request_hash: str,
                  status_code: int, body: dict) -> None:
        if not self._usable():
            return
        payload = json.dumps({"request_hash": request_hash,
                              "status_code": status_code, "body": body})
        try:
            await self._redis.set(self._redis_key(scope, key), payload, ex=self._ttl)
        except Exception:
            logger.warning("idempotency cache write failed", exc_info=True)
            self._on_failure()
            return
        self._on_success()

    async def close(self) -> None:
        if self._redis is not None:
            await self._redis.aclose()


# -----------------------------------------------------------------------------
# Claim / replay
# -----------------------------------------------------------------------------
@dataclass(slots=True)
class _Claim:
    acquired: bool
    replay: tuple[int, dict] | None = None


async def _claim(
    session: AsyncSession, *, scope: str, key: str, request_hash: str, ttl: dt.timedelta
) -> _Claim:
    """Reivindica a chave com INSERT ... ON CONFLICT DO NOTHING.

    Nao e SELECT-depois-INSERT: aquele tem janela de corrida entre as duas queries.
    Aqui a unicidade da PK decide o vencedor e o BANCO arbitra a corrida, nao a
    aplicacao. O perdedor bloqueia ate o vencedor commitar e entao ve o resultado.
    """
    now = dt.datetime.now(dt.UTC)

    claim_stmt = (
        pg_insert(IdempotencyKey)
        .values(scope=scope, key=key, request_hash=request_hash,
                status=IdempotencyStatus.IN_FLIGHT, expires_at=now + ttl)
        .on_conflict_do_nothing(index_elements=["scope", "key"])
        .returning(IdempotencyKey.key)
    )
    if (await session.execute(claim_stmt)).scalar_one_or_none() is not None:
        return _Claim(acquired=True)

    # Perdemos a corrida. Sob READ COMMITTED este SELECT tira snapshot novo e ja ve
    # a linha commitada pelo vencedor. Sob SERIALIZABLE o snapshot e o do inicio da
    # transacao, entao aqui costuma vir 40001 -- e o retry de _run_serializable
    # reexecuta o claim inteiro, que e por que ele e seguro.
    existing = (
        await session.execute(
            select(IdempotencyKey).where(
                IdempotencyKey.scope == scope, IdempotencyKey.key == key
            )
        )
    ).scalar_one_or_none()

    if existing is None:
        raise IdempotencyInFlightError(f"Idempotency-Key {key} is being claimed concurrently")

    if existing.expires_at <= now:
        # Chave expirada nao e replay: e requisicao nova. Reivindica com WHERE
        # guardado em expires_at para que so um concorrente consiga reciclar.
        reclaimed = (
            await session.execute(
                update(IdempotencyKey)
                .where(IdempotencyKey.scope == scope, IdempotencyKey.key == key,
                       IdempotencyKey.expires_at <= now)
                .values(request_hash=request_hash, status=IdempotencyStatus.IN_FLIGHT,
                        response_status=None, response_body=None, transaction_id=None,
                        created_at=now, expires_at=now + ttl)
                .returning(IdempotencyKey.key)
                .execution_options(synchronize_session=False)
            )
        ).scalar_one_or_none()
        if reclaimed is not None:
            return _Claim(acquired=True)
        raise IdempotencyInFlightError(f"Idempotency-Key {key} was reclaimed concurrently")

    if existing.request_hash != request_hash:
        raise IdempotencyKeyReuseError(
            f"Idempotency-Key {key} was already used with a different request body"
        )

    if existing.status is IdempotencyStatus.COMPLETED:
        return _Claim(acquired=False,
                      replay=(existing.response_status or 200, existing.response_body or {}))

    raise IdempotencyInFlightError(f"Idempotency-Key {key} is still in flight")


async def _complete(session: AsyncSession, *, scope: str, key: str, status_code: int,
                    body: dict, transaction_id: uuid.UUID | None) -> None:
    await session.execute(
        update(IdempotencyKey)
        .where(IdempotencyKey.scope == scope, IdempotencyKey.key == key)
        .values(status=IdempotencyStatus.COMPLETED, response_status=status_code,
                response_body=body, transaction_id=transaction_id)
        .execution_options(synchronize_session=False)
    )


async def execute_idempotent(
    session_factory: async_sessionmaker[AsyncSession],
    cache: IdempotencyCache,
    *,
    scope: str,
    key: str | None,
    payload: Mapping[str, Any],
    operation: Operation,
    ttl: dt.timedelta = dt.timedelta(hours=24),
) -> IdempotentResult:
    """Executa `operation` no maximo uma vez por (scope, key).

    O claim e a operacao rodam na MESMA transacao: nao existe estado onde o
    lancamento foi commitado mas o replay nao funciona.
    """
    if key is None:
        status_code, body, _ = await run_in_transaction(session_factory, operation)
        return IdempotentResult(status_code, body, replayed=False)

    request_hash = fingerprint(payload)

    cached = await cache.get(scope, key)
    if cached is not None:
        if cached.get("request_hash") != request_hash:
            raise IdempotencyKeyReuseError(
                f"Idempotency-Key {key} was already used with a different request body"
            )
        return IdempotentResult(cached["status_code"], cached["body"], replayed=True)

    async def unit(session: AsyncSession) -> tuple[int, dict, bool]:
        claim = await _claim(session, scope=scope, key=key,
                             request_hash=request_hash, ttl=ttl)
        if claim.replay is not None:
            return (*claim.replay, True)
        status_code, body, transaction_id = await operation(session)
        await _complete(session, scope=scope, key=key, status_code=status_code,
                        body=body, transaction_id=transaction_id)
        return status_code, body, False

    status_code, body, replayed = await run_in_transaction(session_factory, unit)
    if not replayed:
        await cache.set(scope, key, request_hash, status_code, body)
    return IdempotentResult(status_code, body, replayed)
