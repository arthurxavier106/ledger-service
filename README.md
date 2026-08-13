# Ledger Service

[![CI](https://github.com/arthurxavier106/ledger-service/actions/workflows/ci.yml/badge.svg)](https://github.com/arthurxavier106/ledger-service/actions/workflows/ci.yml)
[![Cobertura](https://raw.githubusercontent.com/arthurxavier106/ledger-service/badges/coverage.svg)](https://github.com/arthurxavier106/ledger-service/actions/workflows/ci.yml)
![Python](https://img.shields.io/badge/python-3.12-blue)
![PostgreSQL](https://img.shields.io/badge/postgresql-17-blue)

API de lançamentos financeiros com double-entry bookkeeping e garantias de
consistência **verificadas por teste de concorrência contra Postgres real**.

As decisões de arquitetura estão em [`DESIGN.md`](DESIGN.md) — schema, estratégia de
isolamento, protocolo de idempotência e limitações conhecidas.

**122 testes** e **90 % de cobertura** contra PostgreSQL real (sem mock, sem
SQLite), com os testes de
concorrência parametrizados nas duas estratégias de lock.

---

## O ponto do projeto

A regra que organiza o código:

> **Invariante de dinheiro mora no banco, não na aplicação.**

Se a única coisa impedindo saldo negativo é um `if` em Python, o sistema está a um
race condition de distância de perder dinheiro. Cada invariante crítica tem uma
constraint de Postgres correspondente que a aplicação **não consegue** violar:

| # | Invariante | Garantida por | Teste |
|---|---|---|---|
| I1 | Todo lançamento tem débito = crédito | `CONSTRAINT TRIGGER` deferred | `test_i1_*` |
| I2 | Conta de usuário nunca fica negativa | `CHECK (allow_negative OR balance >= 0)` | `test_i2_*` |
| I3 | `entries` é append-only | trigger `BEFORE UPDATE OR DELETE` + `REVOKE` | `test_i3_*` |
| I4 | Estorno no máximo uma vez por transação | índice único parcial | `test_i4_*`, `test_c4_*` |
| I5 | `balance` == `SUM(entries)` | job de reconciliação | `test_reconciliation.py` |
| I6 | `SUM(débitos)` == `SUM(créditos)` | job de reconciliação | idem |

Os testes de I1–I4 **tentam violar a invariante por fora da aplicação**, com SQL cru,
e exigem que o Postgres recuse.

## Concorrência

O coração é `service.py::_lock_accounts`: contas são travadas sempre na mesma ordem
global, o que elimina deadlock por construção em vez de resolvê-lo por retry. A mesma
ordem vale para os `UPDATE` de saldo — não só para o lock. No estorno, a ordem entre
tabelas também é fixa: `transactions` antes de `accounts`.

Duas estratégias, selecionáveis por `LEDGER_LOCK_STRATEGY`, que passam **na mesma
suíte de testes**. A escolha entre elas é de performance, não de correção.

`test_naive_read_modify_write_loses_money` faz o read-modify-write ingênuo de
propósito e prova que 9 de 10 débitos somem sem erro nenhum. Documenta o bug que o
`FOR UPDATE` existe para evitar.

## Idempotência

Duas camadas, com uma regra que não se quebra:

> **Redis é cache. Postgres é a verdade.** Um Redis vazio deixa a API mais lenta,
> nunca causa transferência duplicada.

O claim é `INSERT ... ON CONFLICT DO NOTHING` — atômico, sem a janela de corrida do
`SELECT`-depois-`INSERT` — e roda na **mesma transação** do lançamento que protege.
`request_hash` divergente devolve `422`, não um replay silencioso da resposta errada.

## Webhooks — transactional outbox

`COMMIT` da transferência e `POST` para o cliente não podem ser atômicos entre si.
Qualquer ordem ingênua perde: envia antes do commit e o cliente recebe evento de algo
que não aconteceu; envia depois e um crash deixa a transação sem evento.

O evento é gravado na **mesma transação** do lançamento. Se a transferência commitou,
o evento existe. Se não commitou, o evento não existe. `test_rolled_back_transfer_leaves_no_event`
é a prova.

A entrega roda num processo separado (`ledger.worker`), lendo a fila com
`FOR UPDATE SKIP LOCKED` — cada worker pula as linhas já travadas por outro em vez de
esperar. É o padrão canônico de fila em Postgres e evita puxar Kafka ou RabbitMQ para
dentro do projeto só para entregar webhook.

- **Backoff exponencial com jitter**, teto de 1h, 8 tentativas → `dead`. O jitter não
  é enfeite: sem ele, uma queda do endpoint faz todos os eventos retentarem no mesmo
  instante e derrubarem o cliente de novo assim que ele volta.
- **Lease no claim** — `next_attempt_at` vai para frente ao reivindicar. Worker que
  morre no meio não deixa o evento preso nem o faz girar em loop apertado.
- **Assinatura estilo Stripe**: `X-Ledger-Signature: t=<unix>,v1=<hmac_sha256>`. O
  timestamp entra no que é assinado, o que impede replay da entrega por terceiro.
- **Entrega at-least-once**, documentada como contrato e não escondida. O receptor
  deduplica por `X-Ledger-Event-Id`, estável entre tentativas.

## Rate limiting

Janela deslizante em Redis via **script Lua**. Janela fixa permitiria o dobro do
limite na virada; `INCR` + `EXPIRE` soltos deixariam a chave vazar se o segundo
comando se perdesse, e a contagem correria com a poda sob concorrência. O Lua roda
inteiro dentro do Redis, sem interleaving —
`test_concurrent_checks_never_exceed_the_limit` dispara 50 chamadas simultâneas contra
um limite de 10 e exige exatamente 10 liberadas.

**Falha aberta**: com o Redis fora, as requisições passam. Rate limit protege
disponibilidade; transformá-lo em ponto único de falha inverteria o propósito.

## Circuit breaker

Degradar quando o Redis cai é correto. Fazer isso **sem breaker** é uma armadilha, e
esse projeto tem a medição: na primeira execução do load test o p50 estava em **920 ms**
porque cada requisição pagava o timeout de conexão antes de desistir. O serviço ficava
de pé e inutilizável ao mesmo tempo — a pior combinação.

`ledger.circuit.CircuitBreaker` (3 falhas → 30 s desligado) é usado tanto pelo cache de
idempotência quanto pelo rate limiter, que roda em toda escrita.

## Reconciliação

Materializar `accounts.balance` é escolha de performance; o preço é a possibilidade de
divergência. O job de reconciliação é o que torna a escolha defensável: compara o saldo
materializado com a soma dos lançamentos (incremental, a partir do último snapshot) e
alimenta `ledger_reconciliation_drift`. Se essa métrica sair de zero, o design de saldo
materializado está errado e o README precisa dizer.

---

## Rodando

```bash
docker compose up -d --build     # postgres + redis + migrations + api + outbox-worker
curl localhost:8000/health/ready
open http://localhost:8000/docs  # OpenAPI com exemplos
```

Testes:

```bash
docker compose exec api pytest -q
docker compose exec api pytest tests/test_concurrency.py -v
LEDGER_LOCK_STRATEGY=serializable docker compose exec api pytest -q
```

---

## Load test

### Ambiente

Sem isso a tabela abaixo não significa nada:

| | |
|---|---|
| CPU / RAM | **2 vCPU**, 3.9 GB (Linux 6.8, x86_64) |
| PostgreSQL | 16.2 |
| Python | 3.12.13, uvicorn 1 worker, asyncpg |
| Pool | `pool_size=40`, `max_overflow=20` |
| Gerador de carga | **Locust, no mesmo host da API e do banco** |
| Duração | 30 s por cenário, ramp instantâneo, 50 VUs |
| Redis | ausente — o breaker abre após 3 falhas e o caminho degradado não custa nada |

**O gerador de carga divide 2 vCPUs com a API e o Postgres.** O tempo de serviço com 1
usuário é de 14 ms; o teto de throughput é da máquina, não do design. Os números
absolutos são um piso e variam entre execuções conforme a carga do host — o que é
válido aqui são as **comparações relativas na mesma sessão de medição** e o formato das
curvas. A tabela abaixo foi medida inteira de uma vez.

### Resultados

| Cenário | Estratégia | req/s | p50 | p95 | p99 | erro % | deadlocks | 40001 |
|---|---|---|---|---|---|---|---|---|
| Baseline (pares aleatórios) | `row_lock` | **124.7** | 380 ms | 530 ms | 900 ms | 0 % | **0** | 0 |
| Baseline | `serializable` | 69.1 | 560 ms | 1600 ms | 2800 ms | 3.2 % | **0** | 1473 |
| Conta quente (90% → 1 conta) | `row_lock` | **108.2** | 210 ms | 1600 ms | 2400 ms | 0 % | **0** | 0 |
| Conta quente | `serializable` (5 retries) | 47.6 | 1100 ms | 1400 ms | 1900 ms | **77.6 %** | **0** | 6029 |
| Conta quente | `serializable` (25 retries) | 60.8 | 40 ms | 1600 ms | **8300 ms** | 0 % | **0** | 3785 |
| Replay idempotente (30 %) | `row_lock` | **136.2** | 360 ms | 520 ms | 880 ms | 0 % | **0** | 0 |

### O que os números dizem

**1. `row_lock` entrega 1.8x o throughput do `serializable` sob conta quente**
(108.2 vs 60.8 req/s), com erro zero e p99 3.5x menor. É o cenário real de pagamentos —
um merchant recebendo de muitos pagadores — e é onde as duas estratégias divergem.

**2. Zero deadlocks em todas as execuções.** Inclusive no `serializable`, que não usa
lock explícito. A ordenação total dos ids se sustenta sob carga real, não só no teste.

**3. Aumentar o orçamento de retry converte erro em latência, e só.** Com 5 tentativas,
77.6 % das requisições morrem em `503`. Com 25, o erro vai a zero — mas o p99 vai a
**8.3 segundos**, porque as requisições azaradas retentam vinte e poucas vezes. O p50 de
40 ms engana: é a mediana das que passaram de primeira. A conclusão não é "retentar
mais": é que SSI numa linha quente não tem configuração boa.

**4. O caminho de replay é o mais barato** — 136.2 req/s com 30 % de replays contra
124.7 sem nenhum. O replay não abre transação de escrita.

**5. O outbox no write path não custou throughput mensurável.** Com nenhum endpoint
registrado, o `INSERT ... SELECT` não produz linhas; com endpoints, o custo é uma
inserção por destino dentro de uma transação que já estava aberta.

### Reproduzindo

```bash
docker compose up -d --build
python loadtest/seed.py
LOADTEST_SCENARIO=hot_account locust -f loadtest/locustfile.py --headless \
  -u 50 -r 50 -t 30s -H http://127.0.0.1:8000 --csv=results
curl -s localhost:8000/metrics | grep -E "ledger_(deadlocks|serialization_failures)"
```

---

## Métricas

`GET /metrics` no formato Prometheus. A que mais importa é `ledger_lock_wait_seconds` —
é ela que mostra a contenção acontecendo.

```
ledger_transactions_total{kind,outcome,strategy}
ledger_transaction_duration_seconds{kind,strategy}
ledger_lock_wait_seconds{strategy}
ledger_serialization_failures_total
ledger_deadlocks_total              # tem que ser 0
ledger_serialization_retries
ledger_idempotency_total{result,source}
ledger_webhook_deliveries_total{outcome}
ledger_outbox_backlog
ledger_rate_limit_rejections_total{scope}
ledger_reconciliation_drift         # tem que ser 0
```

## Roadmap

- [x] Schema + invariantes I1–I4 no banco
- [x] Write path com lock ordering determinístico
- [x] Testes de concorrência contra Postgres real (C1–C5)
- [x] API de contas, transferência e extrato paginado por keyset
- [x] Idempotência por `Idempotency-Key` (Redis cache + Postgres autoritativo)
- [x] Reversão/estorno
- [x] `/metrics` Prometheus
- [x] GitHub Actions: ruff, pytest nas duas estratégias, round-trip de migration, build
- [x] Load test com números reais
- [x] Webhooks via transactional outbox + `FOR UPDATE SKIP LOCKED`
- [x] Rate limiting em Redis com script Lua
- [x] Job de reconciliação alimentando `ledger_reconciliation_drift`
- [x] Badge de cobertura — gerado pelo próprio CI, sem serviço de terceiro

## Limitações conhecidas

Detalhadas no [`DESIGN.md` §9](DESIGN.md). Em resumo:

1. **Sem hold/pré-autorização** — não existe saldo disponível ≠ saldo contábil.
2. **Sem FX** — lançamento cross-currency é rejeitado por constraint.
3. **Escala vertical** — todo o dinheiro numa instância Postgres.
4. **Estorno falha se o destino já gastou o dinheiro**, em vez de deixar a conta
   negativa. Sistemas reais fazem clawback com saldo negativo controlado.
5. **Reconciliação em batch**, não contínua — drift é detectado no job, não no write.
6. **Webhook at-least-once**; o receptor precisa deduplicar por `X-Ledger-Event-Id`.
7. **Endpoints de webhook são globais por tipo de evento**, sem roteamento por tenant.
8. **Load test co-localizado** — números são um piso, não o teto do design.
