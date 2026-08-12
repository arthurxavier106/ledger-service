"""Rate limiting com janela deslizante em Redis.

Duas decisoes que valem explicar:

1. **Janela deslizante, nao fixa.** Janela fixa permite o dobro do limite na
   virada: 100 requisicoes no ultimo segundo da janela e 100 no primeiro da
   seguinte. A deslizante conta o que aconteceu nos ultimos N segundos de fato.

2. **Script Lua, nao INCR + EXPIRE soltos.** Feitos separados, o EXPIRE pode se
   perder entre os dois comandos e a chave vaza para sempre; e sob concorrencia a
   contagem e a poda correm entre si. O Lua roda inteiro dentro do Redis, sem
   interleaving.

Falha ABERTA: se o Redis cair, as requisicoes passam. Rate limit protege
disponibilidade -- transforma-lo em ponto unico de falha inverteria o proposito.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Any

from ledger.circuit import CircuitBreaker
from ledger.config import settings

logger = logging.getLogger(__name__)

# KEYS[1] = chave da janela
# ARGV[1] = agora (ms)  ARGV[2] = janela (ms)  ARGV[3] = limite  ARGV[4] = id unico
_SLIDING_WINDOW_LUA = """
local key    = KEYS[1]
local now    = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local limit  = tonumber(ARGV[3])
local member = ARGV[4]

-- descarta o que saiu da janela
redis.call('ZREMRANGEBYSCORE', key, 0, now - window)

local used = redis.call('ZCARD', key)
if used >= limit then
    local oldest = redis.call('ZRANGE', key, 0, 0, 'WITHSCORES')
    local reset_in = window
    if oldest[2] then
        reset_in = (tonumber(oldest[2]) + window) - now
    end
    return {0, used, reset_in}
end

redis.call('ZADD', key, now, member)
redis.call('PEXPIRE', key, window)
return {1, used + 1, 0}
"""


@dataclass(slots=True)
class RateLimitResult:
    allowed: bool
    used: int
    limit: int
    retry_after_seconds: int

    @property
    def remaining(self) -> int:
        return max(self.limit - self.used, 0)


class RateLimiter:
    def __init__(self, redis: Any | None = None, *, limit: int | None = None,
                 window_seconds: int | None = None) -> None:
        self._redis = redis
        self._limit = limit or settings.rate_limit_requests
        self._window_ms = (window_seconds or settings.rate_limit_window_seconds) * 1000
        # Mesmo breaker do cache de idempotencia: sem ele, um Redis fora do ar faz
        # cada escrita pagar um timeout de conexao antes de liberar.
        self._breaker = CircuitBreaker("rate-limiter")

    @property
    def circuit_open(self) -> bool:
        return self._breaker.is_open

    async def check(self, scope: str) -> RateLimitResult:
        if self._redis is None or not settings.rate_limit_enabled or self._breaker.is_open:
            return RateLimitResult(True, 0, self._limit, 0)

        now_ms = int(time.time() * 1000)
        member = f"{now_ms}:{time.perf_counter_ns()}"
        try:
            allowed, used, reset_ms = await self._redis.eval(
                _SLIDING_WINDOW_LUA, 1, f"ratelimit:{scope}",
                now_ms, self._window_ms, self._limit, member,
            )
        except Exception:
            logger.warning("rate limiter indisponivel; liberando requisicao", exc_info=True)
            self._breaker.record_failure()
            return RateLimitResult(True, 0, self._limit, 0)

        self._breaker.record_success()
        return RateLimitResult(
            allowed=bool(int(allowed)),
            used=int(used),
            limit=self._limit,
            retry_after_seconds=max(1, int(int(reset_ms) / 1000)) if not int(allowed) else 0,
        )
