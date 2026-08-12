from __future__ import annotations

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from sqlalchemy import text

from ledger.api import ledger_error_handler, router
from ledger.config import settings
from ledger.db import SessionFactory, engine
from ledger.errors import LedgerError
from ledger.idempotency import IdempotencyCache
from ledger.metrics import REGISTRY


def _build_cache() -> IdempotencyCache:
    """Redis e opcional por design: sem ele a API fica mais lenta (todo replay bate
    no Postgres), nunca incorreta."""
    try:
        import redis.asyncio as aioredis

        return IdempotencyCache(aioredis.from_url(settings.redis_url, decode_responses=True))
    except Exception:
        logging.getLogger(__name__).warning(
            "Redis indisponivel; idempotencia segue so no Postgres", exc_info=True
        )
        return IdempotencyCache(None)


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.session_factory = SessionFactory
    app.state.idempotency_cache = _build_cache()
    yield
    await app.state.idempotency_cache.close()
    await engine.dispose()


app = FastAPI(
    title="Ledger Service",
    version="0.1.0",
    description=(
        "Double-entry ledger com garantias de consistencia verificadas por teste "
        "de concorrencia contra Postgres real. Ver DESIGN.md."
    ),
    lifespan=lifespan,
)
app.include_router(router)
app.add_exception_handler(LedgerError, ledger_error_handler)


@app.get("/health/live", tags=["health"])
async def live() -> dict:
    return {"status": "ok", "lock_strategy": settings.lock_strategy.value}


@app.get("/health/ready", tags=["health"])
async def ready() -> dict:
    async with engine.connect() as conn:
        await conn.execute(text("SELECT 1"))
    return {"status": "ready"}


@app.get("/metrics", tags=["ops"], include_in_schema=False)
async def metrics() -> Response:
    return Response(generate_latest(REGISTRY), media_type=CONTENT_TYPE_LATEST)
