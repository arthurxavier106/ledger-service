from __future__ import annotations

import os
import uuid

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from ledger.db import read_sql, schema_scripts, split_sql_statements
from ledger.idempotency import IdempotencyCache
from ledger.ids import uuid7
from ledger.models import Account, AccountType, TransactionKind
from ledger.service import TransferRequest, execute_transfer

TEST_DATABASE_URL = os.environ.get(
    "LEDGER_TEST_DATABASE_URL",
    "postgresql+asyncpg://ledger:ledger@localhost:5432/ledger_test",
)

# Os testes de concorrencia disparam ate 50 tasks simultaneas. Se o pool for menor
# que isso as tasks enfileiram no pool e o teste vira uma execucao serializada que
# passa sem provar nada. O pool precisa comportar a concorrencia toda.
POOL_SIZE = 60


async def _run_script(engine, sql: str) -> None:
    """Aplica um script statement a statement, exatamente como a migration faz.

    Usar o mesmo caminho da migration e proposital: se o splitter quebrar algum
    statement, os testes quebram junto -- em vez de o CI passar verde e o
    `alembic upgrade` estourar em producao.
    """
    async with engine.begin() as conn:
        for statement in split_sql_statements(sql):
            await conn.execute(text(statement))


@pytest_asyncio.fixture(scope="session")
async def engine():
    eng = create_async_engine(TEST_DATABASE_URL, pool_size=POOL_SIZE, max_overflow=0)
    await _run_script(eng, "DROP SCHEMA IF EXISTS public CASCADE; CREATE SCHEMA public;")
    # Mesmo arquivo .sql que a migration aplica -> o teste exercita o schema real.
    for script in schema_scripts():
        await _run_script(eng, read_sql(script))
    yield eng
    await eng.dispose()


@pytest_asyncio.fixture
async def session_factory(engine):
    factory = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)
    async with engine.begin() as conn:
        # TRUNCATE nao dispara triggers de linha, entao passa pela protecao
        # append-only de entries (que so bloqueia UPDATE/DELETE).
        await conn.execute(
            text(
                "TRUNCATE idempotency_keys, entries, transactions, accounts"
                " RESTART IDENTITY CASCADE"
            )
        )
    return factory


@pytest_asyncio.fixture
async def session(session_factory):
    async with session_factory() as s:
        yield s


async def _new_account(
    session_factory,
    *,
    currency: str = "BRL",
    allow_negative: bool = False,
    account_type: AccountType = AccountType.LIABILITY,
) -> Account:
    account = Account(
        id=uuid7(),
        external_id=f"acct-{uuid.uuid4()}",
        owner_id=uuid7(),
        currency=currency,
        type=account_type,
        balance=0,
        allow_negative=allow_negative,
    )
    async with session_factory() as s:
        s.add(account)
        await s.commit()
    return account


@pytest.fixture
def account_factory(session_factory):
    """Cria contas com saldo de abertura via LANCAMENTO real.

    Escrever accounts.balance direto seria mais rapido, mas quebraria a invariante
    I5 (balance == SUM(entries)) de forma legitima e tornaria a checagem de drift
    inutil. Num ledger de verdade dinheiro nao aparece do nada: ele e debitado de
    uma conta de sistema (allow_negative=True), que e a contrapartida contabil do
    deposito externo.
    """
    funding: dict[str, Account] = {}

    async def _funding_account(currency: str) -> Account:
        if currency not in funding:
            funding[currency] = await _new_account(
                session_factory,
                currency=currency,
                allow_negative=True,
                account_type=AccountType.EQUITY,
            )
        return funding[currency]

    async def _factory(
        *,
        balance: int = 0,
        currency: str = "BRL",
        allow_negative: bool = False,
        account_type: AccountType = AccountType.LIABILITY,
    ) -> Account:
        account = await _new_account(
            session_factory,
            currency=currency,
            allow_negative=allow_negative,
            account_type=account_type,
        )
        if balance:
            source = await _funding_account(currency)
            await execute_transfer(
                session_factory,
                TransferRequest(source.id, account.id, balance, currency,
                                kind=TransactionKind.DEPOSIT),
            )
            account.balance = balance
        return account

    return _factory


@pytest.fixture
def cache():
    """Redis falso: exercita o caminho com cache sem exigir infra na suite."""
    from fakeredis.aioredis import FakeRedis

    return IdempotencyCache(FakeRedis(decode_responses=True))


@pytest.fixture
def no_cache():
    """Cache desligado: prova que a idempotencia continua correta so com Postgres."""
    return IdempotencyCache(None)


@pytest_asyncio.fixture
async def client(session_factory, cache):
    """Cliente ASGI apontando para o Postgres de teste e o cache falso."""
    from httpx import ASGITransport, AsyncClient

    from ledger.main import app

    app.state.session_factory = session_factory
    app.state.idempotency_cache = cache
    async with AsyncClient(transport=ASGITransport(app=app),
                           base_url="http://test") as http_client:
        yield http_client
