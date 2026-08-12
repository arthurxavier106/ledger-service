from __future__ import annotations

import datetime as dt
import uuid
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field

# Dinheiro trafega como inteiro em unidade menor (centavos). Nunca float, nunca
# string decimal: float perde precisao e string decimal empurra o arredondamento
# para o cliente. O expoente por moeda vem de GET /v1/currencies.
Amount = Annotated[int, Field(gt=0, description="Valor em unidade menor (ex.: centavos)")]
CurrencyCode = Annotated[str, Field(min_length=3, max_length=3, pattern=r"^[A-Z]{3}$")]


class CreateAccountRequest(BaseModel):
    model_config = ConfigDict(json_schema_extra={
        "examples": [{"external_id": "merchant-4711", "owner_id":
                      "0190f8c2-1b3a-7c4d-8e5f-6a7b8c9d0e1f",
                      "currency": "BRL", "type": "liability"}]
    })

    external_id: str = Field(max_length=255)
    owner_id: uuid.UUID
    currency: CurrencyCode = "BRL"
    type: Literal["asset", "liability", "equity", "revenue", "expense"] = "liability"
    allow_negative: bool = False


class AccountResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    external_id: str
    owner_id: uuid.UUID
    currency: str
    type: str
    status: str
    balance: int
    created_at: dt.datetime


class TransferRequestBody(BaseModel):
    model_config = ConfigDict(json_schema_extra={
        "examples": [{"from_account_id": "0190f8c2-1b3a-7c4d-8e5f-6a7b8c9d0e1f",
                      "to_account_id": "0190f8c2-2c4b-7d5e-9f60-7b8c9d0e1f2a",
                      "amount": 12345, "currency": "BRL",
                      "external_ref": "pix-e2e-20260811-0001"}]
    })

    from_account_id: uuid.UUID
    to_account_id: uuid.UUID
    amount: Amount
    currency: CurrencyCode
    external_ref: str | None = Field(default=None, max_length=255)
    metadata: dict = Field(default_factory=dict)


class ReversalRequestBody(BaseModel):
    model_config = ConfigDict(json_schema_extra={
        "examples": [{"external_ref": "chargeback-4711",
                      "metadata": {"reason": "customer_dispute"}}]
    })

    external_ref: str | None = Field(default=None, max_length=255)
    metadata: dict = Field(default_factory=dict)


class EntryResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    account_id: uuid.UUID
    direction: str
    amount: int
    currency: str
    balance_after: int
    created_at: dt.datetime


class TransactionResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    kind: str
    status: str
    reverses_transaction_id: uuid.UUID | None = None
    external_ref: str | None
    created_at: dt.datetime
    entries: list[EntryResponse] = Field(default_factory=list)


class Page(BaseModel):
    """Paginacao por keyset, nao OFFSET: num ledger as paginas antigas existem
    para sempre, e OFFSET fica mais lento quanto mais fundo se navega."""

    data: list[EntryResponse]
    next_cursor: str | None = None
    has_more: bool = False


class CreateWebhookEndpointRequest(BaseModel):
    model_config = ConfigDict(json_schema_extra={
        "examples": [{"owner_id": "0190f8c2-1b3a-7c4d-8e5f-6a7b8c9d0e1f",
                      "url": "https://merchant.example.com/hooks/ledger",
                      "event_types": ["transaction.posted", "transaction.reversed"]}]
    })

    owner_id: uuid.UUID
    url: Annotated[str, Field(pattern=r"^https?://", max_length=2000)]
    event_types: list[str] = Field(default_factory=list,
                                   description="Vazio = recebe todos os tipos")


class WebhookEndpointResponse(BaseModel):
    # sem from_attributes de proposito: o segredo e bytes no modelo e nao pode
    # vazar por mapeamento automatico. Ver api._endpoint_response.

    id: uuid.UUID
    owner_id: uuid.UUID
    url: str
    event_types: list[str]
    active: bool
    created_at: dt.datetime
    secret: str | None = Field(
        default=None,
        description="Devolvido APENAS na criacao. Guarde: nao e possivel recuperar depois.",
    )
