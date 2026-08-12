from __future__ import annotations

import re
from collections.abc import AsyncIterator
from pathlib import Path

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from ledger.config import settings

SQL_DIR = Path(__file__).parent / "sql"

engine = create_async_engine(
    settings.database_url,
    echo=settings.db_echo,
    pool_size=settings.db_pool_size,
    max_overflow=settings.db_max_overflow,
    pool_pre_ping=True,
)

SessionFactory = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)


async def get_session() -> AsyncIterator[AsyncSession]:
    async with SessionFactory() as session:
        yield session


def schema_scripts() -> list[str]:
    """Todos os scripts de upgrade, em ordem. A suite aplica exatamente esta lista,
    entao adicionar uma migration nova sem os testes verem e impossivel."""
    return sorted(path.name for path in SQL_DIR.glob("*_up.sql"))


def read_sql(name: str) -> str:
    """Fonte unica do DDL: a migration e os testes leem o MESMO arquivo,
    entao nao existe drift entre o schema testado e o schema migrado."""
    return (SQL_DIR / name).read_text(encoding="utf-8")


_DOLLAR_TAG = re.compile(r"\$[A-Za-z_][A-Za-z0-9_]*\$|\$\$")


def split_sql_statements(script: str) -> list[str]:
    """Divide um script SQL em statements individuais.

    Existe porque o driver asyncpg usa prepared statements, que aceitam um comando
    por vez -- mandar o arquivo inteiro de uma vez levanta
    "cannot insert multiple commands into a prepared statement".

    Split ingenuo em ";" nao serve: os corpos plpgsql estao entre $fn$...$fn$ e tem
    ponto-e-virgula dentro. Este parser respeita dollar-quoting (com tag),
    string literal com aspas simples (incluindo o escape '') e comentario de linha.
    """
    statements: list[str] = []
    buf: list[str] = []
    i, n = 0, len(script)
    open_tag: str | None = None

    while i < n:
        if open_tag:
            if script.startswith(open_tag, i):
                buf.append(open_tag)
                i += len(open_tag)
                open_tag = None
            else:
                buf.append(script[i])
                i += 1
            continue

        char = script[i]

        if script.startswith("--", i):
            end = script.find("\n", i)
            end = n if end == -1 else end
            buf.append(script[i:end])
            i = end
            continue

        if char == "'":
            j = i + 1
            while j < n:
                if script[j] == "'":
                    if j + 1 < n and script[j + 1] == "'":
                        j += 2
                        continue
                    break
                j += 1
            buf.append(script[i : j + 1])
            i = j + 1
            continue

        if char == "$":
            match = _DOLLAR_TAG.match(script, i)
            if match:
                open_tag = match.group(0)
                buf.append(open_tag)
                i += len(open_tag)
                continue

        if char == ";":
            statement = "".join(buf).strip()
            if statement:
                statements.append(statement)
            buf = []
            i += 1
            continue

        buf.append(char)
        i += 1

    tail = "".join(buf).strip()
    if tail:
        statements.append(tail)
    return statements
