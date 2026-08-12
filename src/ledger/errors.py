"""Erros de dominio, mapeados para RFC 9457 na borda HTTP."""

from __future__ import annotations

import uuid


class LedgerError(Exception):
    status_code = 400
    error_type = "https://ledger.dev/errors/ledger-error"
    title = "Ledger error"

    def problem(self) -> dict:
        return {"type": self.error_type, "title": self.title,
                "status": self.status_code, "detail": str(self)}


class AccountNotFoundError(LedgerError):
    status_code = 404
    error_type = "https://ledger.dev/errors/account-not-found"
    title = "Account not found"

    def __init__(self, account_id: uuid.UUID) -> None:
        self.account_id = account_id
        super().__init__(f"Account {account_id} does not exist")

    def problem(self) -> dict:
        return super().problem() | {"account_id": str(self.account_id)}


class InsufficientFundsError(LedgerError):
    status_code = 422
    error_type = "https://ledger.dev/errors/insufficient-funds"
    title = "Insufficient funds"

    def __init__(self, account_id: uuid.UUID, available: int, requested: int,
                 currency: str) -> None:
        self.account_id, self.available = account_id, available
        self.requested, self.currency = requested, currency
        super().__init__(
            f"Account has {available} {currency} available, requested {requested} {currency}"
        )

    def problem(self) -> dict:
        return super().problem() | {"account_id": str(self.account_id),
                                    "available": self.available,
                                    "requested": self.requested,
                                    "currency": self.currency}


class CurrencyMismatchError(LedgerError):
    status_code = 422
    error_type = "https://ledger.dev/errors/currency-mismatch"
    title = "Currency mismatch"


class AccountNotActiveError(LedgerError):
    status_code = 422
    error_type = "https://ledger.dev/errors/account-not-active"
    title = "Account not active"


class InvalidTransferError(LedgerError):
    status_code = 422
    error_type = "https://ledger.dev/errors/invalid-transfer"
    title = "Invalid transfer"


class SerializationExhaustedError(LedgerError):
    status_code = 503
    error_type = "https://ledger.dev/errors/serialization-exhausted"
    title = "Too much contention"


class TransactionNotFoundError(LedgerError):
    status_code = 404
    error_type = "https://ledger.dev/errors/transaction-not-found"
    title = "Transaction not found"


class InvalidReversalError(LedgerError):
    status_code = 422
    error_type = "https://ledger.dev/errors/invalid-reversal"
    title = "Invalid reversal"


class AlreadyReversedError(LedgerError):
    status_code = 409
    error_type = "https://ledger.dev/errors/already-reversed"
    title = "Transaction already reversed"


class IdempotencyKeyReuseError(LedgerError):
    """Mesma chave, payload diferente.

    Devolver a resposta antiga aqui seria pior do que falhar: o cliente acha que
    mandou R$ 500 e recebe o 201 do R$ 50 de antes.
    """

    status_code = 422
    error_type = "https://ledger.dev/errors/idempotency-key-reuse"
    title = "Idempotency-Key reused with a different payload"


class IdempotencyInFlightError(LedgerError):
    status_code = 409
    error_type = "https://ledger.dev/errors/idempotency-in-flight"
    title = "A request with this Idempotency-Key is still in flight"


class LedgerInvariantError(LedgerError):
    """Uma constraint do banco recusou o que a aplicacao deixou passar.

    Com o lock de linha este caminho deveria ser inalcancavel. Se ele disparar em
    producao existe um bug no write path -- e caso de alerta, nao de retry.
    """

    status_code = 409
    error_type = "https://ledger.dev/errors/invariant-violation"
    title = "Ledger invariant violated"

    def __init__(self, constraint: str) -> None:
        self.constraint = constraint
        super().__init__(f"Database constraint {constraint} rejected the write")

    def problem(self) -> dict:
        return super().problem() | {"constraint": self.constraint}


class RateLimitExceededError(LedgerError):
    status_code = 429
    error_type = "https://ledger.dev/errors/rate-limit-exceeded"
    title = "Too many requests"

    def __init__(self, limit: int, retry_after_seconds: int) -> None:
        self.limit = limit
        self.retry_after_seconds = retry_after_seconds
        super().__init__(f"Rate limit of {limit} requests exceeded")

    def problem(self) -> dict:
        return super().problem() | {"limit": self.limit,
                                    "retry_after": self.retry_after_seconds}
