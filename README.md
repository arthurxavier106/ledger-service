# Ledger Service

API de lançamentos financeiros com double-entry bookkeeping e garantias de
consistência **verificadas por teste de concorrência contra Postgres real**.

As decisões de arquitetura estão em [`DESIGN.md`](DESIGN.md) — schema, estratégia de
isolamento, protocolo de idempotência e limitações conhecidas.

**73 testes passando** contra PostgreSQL real (sem mock, sem SQLite), com os testes
de concorrência parametrizados nas duas estratégias de lock.

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
| I5 | `balance` == `SUM(entries)` | job de reconciliação | `_assert_ledger_is_consistent` |
| I6 | `SUM(débitos)` == `SUM(créditos)` | job de reconciliação | idem |

Os testes de I1–I4 **tentam violar a invariante por fora da aplicação**, com SQL cru,
e exigem que o Postgres recuse.

## Concorrência

O coração é `service.py::_lock_accounts`: contas são travadas sempre na mesma ordem
global, o que elimina deadlock por construção em vez de resolvê-lo por retry. A mesma
ordem vale para os `UPDATE` de saldo — não só para o lock. No estorno, a ordem entre
tabelas também é fixa: `transactions` antes de `accounts`.

Duas estratégias, selecionáveis por `LEDGER_LOCK_STRATEGY`, que passam **na mesma
suíte de testes**. A escolha entre elas é de performance, não de correção — e os
números abaixo mostram o tamanho da diferença.

`test_naive_read_modify_write_loses_money` faz o read-modify-write ingênuo de
propósito e prova que 9 de 10 débitos somem sem erro nenhum. Documenta o bug que o
`FOR UPDATE` existe para evitar.

## Idempotência

Duas camadas, com uma regra que não se quebra:

> **Redis é cache. Postgres é a verdade.** Um Redis vazio deixa a API mais lenta,
> nunca causa transferência duplicada.

O claim é `INSERT ... ON CONFLICT DO NOTHING` — atômico, sem a janela de corrida do
`SELECT`-depois-`INSERT` — e roda na **mesma transação** do lançamento que protege.
Não existe estado onde a transferência foi confirmada mas o replay não funciona.
`request_hash` divergente devolve `422`, não um replay silencioso da resposta errada.

## Rodando

```bash
docker compose up -d --build     # postgres + redis + migrations + api
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
| Duração | 30s por cenário, ramp instantâneo |

**O gerador de carga divide 2 vCPUs com a API e o Postgres.** O throughput satura em
~90 req/s independente da concorrência, enquanto o tempo de serviço com 1 usuário é de
14 ms. Ou seja: o teto é da máquina, não do design. Os números absolutos são um piso;
o que é válido aqui são as **comparações relativas na mesma concorrência** e o formato
das curvas.

### Resultados (50 VUs, 30s)

| Cenário | Estratégia | req/s | p50 | p95 | p99 | erro % | deadlocks | 40001 |
|---|---|---|---|---|---|---|---|---|
| Baseline (pares aleatórios) | `row_lock` | **88.6** | 520 ms | 780 ms | 1500 ms | 0 % | **0** | 0 |
| Baseline | `serializable` | 54.5 | 690 ms | 2200 ms | 3200 ms | 4.1 % | **0** | 1358 |
| Conta quente (90% → 1 conta) | `row_lock` | **79.8** | 320 ms | 2100 ms | 3200 ms | 0 % | **0** | 0 |
| Conta quente | `serializable` (5 retries) | 32.8 | 1600 ms | 2100 ms | 2900 ms | **76.1 %** | **0** | 4169 |
| Conta quente | `serializable` (25 retries) | 35.7 | 80 ms | 2700 ms | **12000 ms** | 0 % | **0** | 2912 |
| Replay idempotente (30%) | `row_lock` | **104.8** | 460 ms | 670 ms | 1300 ms | 0 % | **0** | 0 |

Curva de concorrência (baseline, `row_lock`):

| VUs | req/s | p50 | p95 | p99 |
|---|---|---|---|---|
| 1 | 67.1 | **14 ms** | 18 ms | 21 ms |
| 10 | 90.5 | 110 ms | 140 ms | 190 ms |
| 25 | 81.6 | 270 ms | 560 ms | 820 ms |
| 50 | 88.6 | 520 ms | 780 ms | 1500 ms |

### O que os números dizem

**1. `row_lock` entrega 2.2x o throughput do `serializable` sob conta quente**
(79.8 vs 35.7 req/s), com erro zero. É o cenário real de pagamentos — um merchant
recebendo de muitos pagadores — e é onde as duas estratégias divergem de verdade. No
baseline, sem contenção, a diferença cai para 1.6x.

**2. Zero deadlocks em todas as execuções.** Incluindo `serializable`, que não usa lock
explícito. A ordenação total dos ids se sustenta sob carga real, não só no teste.

**3. Aumentar o orçamento de retry converte erro em latência, e só.** Com 5 tentativas,
76% das requisições morrem em `503`. Com 25, o erro vai a zero — mas o p99 vai a **12
segundos**, porque as requisições azaradas retentam vinte e poucas vezes. O p50 de 80ms
engana: é a mediana das que passaram de primeira. A conclusão não é "retentar mais": é
que SSI numa linha quente não tem configuração boa.

**4. O caminho de replay é mais barato que o de escrita** — 104.8 req/s com 30% de
replays contra 88.6 sem nenhum. O replay não abre transação de escrita.

**5. Achado do próprio load test: graceful degradation sem circuit breaker é uma
armadilha.** Na primeira execução o p50 estava em **920 ms** com o Redis fora do ar.
Causa: cada requisição pagava o timeout de conexão antes de desistir e cair no
Postgres. Correto e inutilizável ao mesmo tempo — a pior combinação. Com o breaker
(`IdempotencyCache`, 3 falhas → 30 s desligado) o p50 caiu para 520 ms na mesma
configuração. Coberto por `tests/test_cache_breaker.py`.

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

`GET /metrics` no formato Prometheus. A que mais importa é
`ledger_lock_wait_seconds` — é ela que mostra a contenção acontecendo, e conta a
história do projeto melhor que qualquer parágrafo.

```
ledger_transactions_total{kind,outcome,strategy}
ledger_transaction_duration_seconds{kind,strategy}
ledger_lock_wait_seconds{strategy}
ledger_serialization_failures_total
ledger_deadlocks_total          # tem que ser 0
ledger_serialization_retries
ledger_idempotency_total{result,source}
ledger_reconciliation_drift     # tem que ser 0
```

## Roadmap

- [x] Schema + invariantes I1–I4 no banco
- [x] Write path com lock ordering determinístico
- [x] Testes de concorrência contra Postgres real (C1, C2, C3, C4, C5)
- [x] API de contas, transferência e extrato paginado por keyset
- [x] Idempotência por `Idempotency-Key` (Redis cache + Postgres autoritativo)
- [x] Reversão/estorno
- [x] `/metrics` Prometheus
- [x] GitHub Actions: ruff, pytest nas duas estratégias, round-trip de migration, build da imagem
- [x] Load test com números reais
- [ ] Webhooks via transactional outbox + `FOR UPDATE SKIP LOCKED`
- [ ] Rate limiting em Redis (script Lua)
- [ ] Job de reconciliação alimentando `ledger_reconciliation_drift`
- [ ] Badge de cobertura (depende do repo estar no GitHub)

## Limitações conhecidas

Detalhadas no [`DESIGN.md` §9](DESIGN.md). Em resumo:

1. **Sem hold/pré-autorização** — não existe saldo disponível ≠ saldo contábil.
2. **Sem FX** — lançamento cross-currency é rejeitado por constraint.
3. **Escala vertical** — todo o dinheiro numa instância Postgres.
4. **Estorno falha se o destino já gastou o dinheiro** — em vez de deixar a conta
   negativa. Sistemas reais fazem clawback com saldo negativo controlado.
   Coberto por `test_reversal_fails_if_funds_already_spent`.
5. **Reconciliação em batch**, não contínua.
6. **Load test co-localizado** — números são um piso, não o teto do design.
