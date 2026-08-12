from __future__ import annotations

import datetime as dt
import uuid
from enum import StrEnum

from sqlalchemy import (
    CHAR,
    BigInteger,
    Boolean,
    DateTime,
    ForeignKey,
    Identity,
    Integer,
    LargeBinary,
    SmallInteger,
    Text,
    func,
)
from sqlalchemy.dialects import postgresql
from sqlalchemy.dialects.postgresql import ENUM, JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class AccountType(StrEnum):
    ASSET = "asset"
    LIABILITY = "liability"
    EQUITY = "equity"
    REVENUE = "revenue"
    EXPENSE = "expense"


class AccountStatus(StrEnum):
    ACTIVE = "active"
    FROZEN = "frozen"
    CLOSED = "closed"


class TransactionKind(StrEnum):
    TRANSFER = "transfer"
    DEPOSIT = "deposit"
    WITHDRAWAL = "withdrawal"
    REVERSAL = "reversal"


class TransactionStatus(StrEnum):
    POSTED = "posted"
    REVERSED = "reversed"


class EntryDirection(StrEnum):
    DEBIT = "debit"
    CREDIT = "credit"


# create_type=False: os tipos sao criados pela migration, nao pelo ORM.
def _pg_enum(py_enum: type[StrEnum], name: str) -> ENUM:
    return ENUM(
        py_enum, name=name, create_type=False,
        values_callable=lambda e: [m.value for m in e],
    )


class Currency(Base):
    """Tabela pequena e estatica, mas transforma "moeda" de string magica em FK.
    Custo proximo de zero; elimina currency='BRLL' chegando em producao."""

    __tablename__ = "currencies"

    code: Mapped[str] = mapped_column(CHAR(3), primary_key=True)
    exponent: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    name: Mapped[str] = mapped_column(Text, nullable=False)


class Account(Base):
    __tablename__ = "accounts"

    id: Mapped[uuid.UUID] = mapped_column(postgresql.UUID(as_uuid=True), primary_key=True)
    external_id: Mapped[str] = mapped_column(Text, nullable=False, unique=True)
    owner_id: Mapped[uuid.UUID] = mapped_column(postgresql.UUID(as_uuid=True), nullable=False)
    currency: Mapped[str] = mapped_column(CHAR(3), ForeignKey("currencies.code"), nullable=False)
    type: Mapped[AccountType] = mapped_column(_pg_enum(AccountType, "account_type"), nullable=False)
    status: Mapped[AccountStatus] = mapped_column(
        _pg_enum(AccountStatus, "account_status"), nullable=False, default=AccountStatus.ACTIVE
    )
    balance: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    allow_negative: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    version: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    created_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    updated_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )


class Transaction(Base):
    __tablename__ = "transactions"

    id: Mapped[uuid.UUID] = mapped_column(postgresql.UUID(as_uuid=True), primary_key=True)
    kind: Mapped[TransactionKind] = mapped_column(
        _pg_enum(TransactionKind, "transaction_kind"), nullable=False
    )
    status: Mapped[TransactionStatus] = mapped_column(
        _pg_enum(TransactionStatus, "transaction_status"), nullable=False,
        default=TransactionStatus.POSTED,
    )
    reverses_transaction_id: Mapped[uuid.UUID | None] = mapped_column(
        postgresql.UUID(as_uuid=True), ForeignKey("transactions.id"), nullable=True
    )
    external_ref: Mapped[str | None] = mapped_column(Text, nullable=True)
    # 'metadata' e atributo reservado em DeclarativeBase -> atributo Python renomeado,
    # coluna no banco continua sendo "metadata".
    meta: Mapped[dict] = mapped_column("metadata", JSONB, nullable=False, default=dict)
    created_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )


class Entry(Base):
    __tablename__ = "entries"

    id: Mapped[int] = mapped_column(BigInteger, Identity(always=True), primary_key=True)
    transaction_id: Mapped[uuid.UUID] = mapped_column(
        postgresql.UUID(as_uuid=True), ForeignKey("transactions.id"), nullable=False
    )
    account_id: Mapped[uuid.UUID] = mapped_column(
        postgresql.UUID(as_uuid=True), ForeignKey("accounts.id"), nullable=False
    )
    direction: Mapped[EntryDirection] = mapped_column(
        _pg_enum(EntryDirection, "entry_direction"), nullable=False
    )
    amount: Mapped[int] = mapped_column(BigInteger, nullable=False)
    currency: Mapped[str] = mapped_column(CHAR(3), ForeignKey("currencies.code"), nullable=False)
    balance_after: Mapped[int] = mapped_column(BigInteger, nullable=False)
    created_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )


class IdempotencyStatus(StrEnum):
    IN_FLIGHT = "in_flight"
    COMPLETED = "completed"
    FAILED = "failed"


class IdempotencyKey(Base):
    """Postgres e a verdade da idempotencia. Redis e so cache."""

    __tablename__ = "idempotency_keys"

    scope: Mapped[str] = mapped_column(Text, primary_key=True)
    key: Mapped[str] = mapped_column(Text, primary_key=True)
    request_hash: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[IdempotencyStatus] = mapped_column(
        _pg_enum(IdempotencyStatus, "idempotency_status"), nullable=False
    )
    response_status: Mapped[int | None] = mapped_column(Integer, nullable=True)
    response_body: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    transaction_id: Mapped[uuid.UUID | None] = mapped_column(
        postgresql.UUID(as_uuid=True), ForeignKey("transactions.id"), nullable=True
    )
    created_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    expires_at: Mapped[dt.datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class OutboxStatus(StrEnum):
    PENDING = "pending"
    DELIVERING = "delivering"
    DELIVERED = "delivered"
    DEAD = "dead"


class WebhookEndpoint(Base):
    __tablename__ = "webhook_endpoints"

    id: Mapped[uuid.UUID] = mapped_column(postgresql.UUID(as_uuid=True), primary_key=True)
    owner_id: Mapped[uuid.UUID] = mapped_column(postgresql.UUID(as_uuid=True), nullable=False)
    url: Mapped[str] = mapped_column(Text, nullable=False)
    secret: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)
    event_types: Mapped[list[str]] = mapped_column(
        postgresql.ARRAY(Text), nullable=False, default=list
    )
    active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    created_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )


class OutboxEvent(Base):
    """Evento gravado na mesma transacao do lancamento que o originou."""

    __tablename__ = "outbox"

    id: Mapped[int] = mapped_column(BigInteger, Identity(always=True), primary_key=True)
    endpoint_id: Mapped[uuid.UUID] = mapped_column(
        postgresql.UUID(as_uuid=True), ForeignKey("webhook_endpoints.id"), nullable=False
    )
    event_type: Mapped[str] = mapped_column(Text, nullable=False)
    aggregate_id: Mapped[uuid.UUID] = mapped_column(
        postgresql.UUID(as_uuid=True), nullable=False
    )
    payload: Mapped[dict] = mapped_column(JSONB, nullable=False)
    status: Mapped[OutboxStatus] = mapped_column(
        _pg_enum(OutboxStatus, "outbox_status"), nullable=False, default=OutboxStatus.PENDING
    )
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    next_attempt_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    delivered_at: Mapped[dt.datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
