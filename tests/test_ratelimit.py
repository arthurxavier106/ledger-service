"""Rate limiting: janela deslizante atomica em Redis."""

from __future__ import annotations

import asyncio

import pytest

from ledger.ratelimit import RateLimiter


@pytest.fixture
def redis():
    from fakeredis.aioredis import FakeRedis

    return FakeRedis(decode_responses=True)


@pytest.mark.asyncio
async def test_allows_up_to_the_limit_then_blocks(redis):
    limiter = RateLimiter(redis, limit=5, window_seconds=60)

    results = [await limiter.check("cliente-a") for _ in range(7)]

    assert [r.allowed for r in results] == [True] * 5 + [False] * 2
    assert results[4].remaining == 0
    assert results[5].retry_after_seconds >= 1


@pytest.mark.asyncio
async def test_scopes_are_independent(redis):
    limiter = RateLimiter(redis, limit=2, window_seconds=60)

    assert (await limiter.check("a")).allowed
    assert (await limiter.check("a")).allowed
    assert not (await limiter.check("a")).allowed
    assert (await limiter.check("b")).allowed, "um cliente nao pode consumir a cota do outro"


@pytest.mark.asyncio
async def test_window_slides(redis):
    """Janela fixa deixaria passar o dobro do limite na virada; a deslizante nao."""
    limiter = RateLimiter(redis, limit=2, window_seconds=1)

    assert (await limiter.check("c")).allowed
    assert (await limiter.check("c")).allowed
    assert not (await limiter.check("c")).allowed

    await asyncio.sleep(1.1)
    assert (await limiter.check("c")).allowed, "a janela deveria ter deslizado"


@pytest.mark.asyncio
async def test_concurrent_checks_never_exceed_the_limit(redis):
    """A prova de que o script Lua e atomico.

    Com INCR e EXPIRE soltos, a contagem e a poda correriam entre si e mais
    requisicoes passariam do que o limite permite.
    """
    limiter = RateLimiter(redis, limit=10, window_seconds=60)

    results = await asyncio.gather(*(limiter.check("hot") for _ in range(50)))

    assert sum(r.allowed for r in results) == 10, "o limite vazou sob concorrencia"


@pytest.mark.asyncio
async def test_fails_open_when_redis_is_down():
    """Rate limit protege disponibilidade. Transforma-lo em ponto unico de falha
    inverteria o proposito: com o Redis fora, as requisicoes passam."""

    class DeadRedis:
        async def eval(self, *_args, **_kwargs):
            raise ConnectionError("redis down")

    limiter = RateLimiter(DeadRedis(), limit=1, window_seconds=60)

    results = [await limiter.check("x") for _ in range(5)]
    assert all(r.allowed for r in results)


@pytest.mark.asyncio
async def test_disabled_limiter_is_permissive():
    limiter = RateLimiter(None, limit=1, window_seconds=60)
    results = [await limiter.check("x") for _ in range(10)]
    assert all(r.allowed for r in results)


@pytest.mark.asyncio
async def test_lua_scripting_is_actually_available(redis):
    """Guarda-chuva contra 'funciona na minha maquina'.

    Sem o extra fakeredis[lua], o EVAL levanta, o limiter falha aberto e todos os
    testes acima passariam sem exercitar o script. Este falha alto e explica.
    """
    result = await redis.eval("return 1 + 1", 0)
    assert int(result) == 2, "fakeredis sem suporte a Lua: instale fakeredis[lua]"
