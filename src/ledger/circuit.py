"""Circuit breaker para dependencias opcionais (Redis).

Degradar quando uma dependencia cai e correto. Fazer isso sem breaker e uma
armadilha: TODA requisicao passa a pagar o timeout de conexao antes de desistir.
Medido no load test deste projeto, isso levou o p50 de ~30ms para ~920ms -- o
servico ficava de pe e inutil ao mesmo tempo, que e a pior falha possivel.

Depois de `failure_threshold` falhas seguidas o circuito abre por
`cooldown_seconds` e as chamadas retornam na hora, sem tocar na rede.
"""

from __future__ import annotations

import logging
import time

logger = logging.getLogger(__name__)


class CircuitBreaker:
    def __init__(self, name: str, *, failure_threshold: int = 3,
                 cooldown_seconds: float = 30.0) -> None:
        self.name = name
        self._failure_threshold = failure_threshold
        self._cooldown = cooldown_seconds
        self._consecutive_failures = 0
        self._open_until = 0.0

    @property
    def is_open(self) -> bool:
        return time.monotonic() < self._open_until

    @property
    def consecutive_failures(self) -> int:
        return self._consecutive_failures

    def record_failure(self) -> None:
        self._consecutive_failures += 1
        if self._consecutive_failures >= self._failure_threshold and not self.is_open:
            self._open_until = time.monotonic() + self._cooldown
            logger.warning("circuito '%s' aberto por %.0fs apos %d falhas",
                           self.name, self._cooldown, self._consecutive_failures)

    def record_success(self) -> None:
        self._consecutive_failures = 0
