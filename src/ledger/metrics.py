"""Metricas Prometheus.

O criterio para uma metrica entrar aqui: ela precisa responder a uma pergunta que
alguem faria as 3 da manha. `ledger_lock_wait_seconds` e a mais importante -- e ela
que mostra a contencao acontecendo, e e o grafico que conta a historia do projeto
melhor do que qualquer paragrafo do README.
"""

from __future__ import annotations

import time
from collections.abc import Iterator
from contextlib import contextmanager

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram

REGISTRY = CollectorRegistry(auto_describe=True)

# Buckets em escala de milissegundos: o write path saudavel vive abaixo de 50ms;
# os buckets altos existem para enxergar a cauda sob contencao, nao o caso normal.
_LATENCY_BUCKETS = (0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0)

transactions_total = Counter(
    "ledger_transactions_total",
    "Lancamentos processados",
    ["kind", "outcome", "strategy"],
    registry=REGISTRY,
)

transaction_duration = Histogram(
    "ledger_transaction_duration_seconds",
    "Duracao do write path completo, do BEGIN ao COMMIT",
    ["kind", "strategy"],
    buckets=_LATENCY_BUCKETS,
    registry=REGISTRY,
)

lock_wait = Histogram(
    "ledger_lock_wait_seconds",
    "Tempo esperando os locks de linha das contas",
    ["strategy"],
    buckets=_LATENCY_BUCKETS,
    registry=REGISTRY,
)

serialization_failures = Counter(
    "ledger_serialization_failures_total",
    "Abortos por SQLSTATE 40001 (SSI do Postgres)",
    registry=REGISTRY,
)

deadlocks = Counter(
    "ledger_deadlocks_total",
    "SQLSTATE 40P01. Com a ordenacao de lock isto deve ficar sempre em zero;"
    " se sair de zero, a ordenacao quebrou.",
    registry=REGISTRY,
)

serialization_retries = Histogram(
    "ledger_serialization_retries",
    "Tentativas ate o commit sob SERIALIZABLE",
    buckets=(0, 1, 2, 3, 5, 8, 13, 21),
    registry=REGISTRY,
)

idempotency_total = Counter(
    "ledger_idempotency_total",
    "Resultado do protocolo de idempotencia",
    ["result", "source"],  # result: created|replayed|reuse  source: redis|postgres
    registry=REGISTRY,
)

reconciliation_drift = Gauge(
    "ledger_reconciliation_drift",
    "Contas onde balance != SUM(entries). Tem que ser zero.",
    registry=REGISTRY,
)


@contextmanager
def observe(histogram: Histogram, **labels: str) -> Iterator[None]:
    started = time.perf_counter()
    try:
        yield
    finally:
        target = histogram.labels(**labels) if labels else histogram
        target.observe(time.perf_counter() - started)
