"""Circuit breaker do cache de idempotencia.

Sem ele, um Redis fora do ar faz cada requisicao pagar um timeout de conexao.
O servico continua correto e fica inutilizavel -- a pior combinacao.
"""

from __future__ import annotations

import pytest

from ledger.idempotency import IdempotencyCache


class BrokenRedis:
    def __init__(self) -> None:
        self.calls = 0

    async def get(self, _key):
        self.calls += 1
        raise ConnectionError("redis down")

    async def set(self, *_args, **_kwargs):
        self.calls += 1
        raise ConnectionError("redis down")

    async def eval(self, *_args, **_kwargs):
        self.calls += 1
        raise ConnectionError("redis down")

    async def aclose(self):
        pass


class FlakyRedis(BrokenRedis):
    def __init__(self, fail_times: int) -> None:
        super().__init__()
        self.remaining = fail_times
        self.store: dict = {}

    async def get(self, key):
        self.calls += 1
        if self.remaining > 0:
            self.remaining -= 1
            raise ConnectionError("redis down")
        return self.store.get(key)


@pytest.mark.asyncio
async def test_breaker_stops_hammering_a_dead_redis():
    redis = BrokenRedis()
    cache = IdempotencyCache(redis, failure_threshold=3, cooldown_seconds=60)

    for _ in range(20):
        assert await cache.get("scope", "key") is None

    assert cache.circuit_open is True
    assert redis.calls == 3, f"deveria parar apos 3 falhas, tentou {redis.calls}x"


@pytest.mark.asyncio
async def test_cache_failure_never_raises():
    """Falha de cache nao pode virar erro de API: o Postgres ainda e a verdade."""
    cache = IdempotencyCache(BrokenRedis(), failure_threshold=99)
    assert await cache.get("s", "k") is None
    await cache.set("s", "k", "hash", 201, {"ok": True})  # nao levanta


@pytest.mark.asyncio
async def test_breaker_closes_again_after_cooldown():
    redis = FlakyRedis(fail_times=3)
    cache = IdempotencyCache(redis, failure_threshold=3, cooldown_seconds=0.0)

    for _ in range(3):
        await cache.get("s", "k")
    assert cache.circuit_open is False, "cooldown zero deve reabrir imediatamente"

    redis.store["idem:s:k"] = '{"request_hash": "h", "status_code": 201, "body": {}}'
    assert await cache.get("s", "k") == {"request_hash": "h", "status_code": 201, "body": {}}


@pytest.mark.asyncio
async def test_disabled_cache_is_a_noop():
    cache = IdempotencyCache(None)
    assert await cache.get("s", "k") is None
    await cache.set("s", "k", "h", 201, {})
    await cache.close()


# --- o mesmo breaker protege o rate limiter -------------------------------
@pytest.mark.asyncio
async def test_rate_limiter_also_stops_hammering_a_dead_redis():
    """Regressao: o rate limiter roda em TODA escrita. Sem breaker, um Redis fora
    do ar custaria um timeout de conexao por requisicao -- o mesmo bug que o load
    test pegou no cache de idempotencia."""
    from ledger.ratelimit import RateLimiter

    redis = BrokenRedis()
    limiter = RateLimiter(redis, limit=5, window_seconds=60)
    limiter._breaker._failure_threshold = 3

    for _ in range(20):
        assert (await limiter.check("scope")).allowed, "falha do limiter deve liberar"

    assert limiter.circuit_open is True
    assert redis.calls == 3, f"deveria parar apos 3 falhas, tentou {redis.calls}x"


@pytest.mark.asyncio
async def test_breaker_reports_consecutive_failures():
    from ledger.circuit import CircuitBreaker

    breaker = CircuitBreaker("t", failure_threshold=2, cooldown_seconds=60)
    breaker.record_failure()
    assert breaker.consecutive_failures == 1
    assert breaker.is_open is False
    breaker.record_failure()
    assert breaker.is_open is True
    breaker.record_success()
    assert breaker.consecutive_failures == 0
