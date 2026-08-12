from __future__ import annotations

from enum import StrEnum

from pydantic_settings import BaseSettings, SettingsConfigDict


class LockStrategy(StrEnum):
    """Ver DESIGN.md secao 4. As duas passam na mesma suite de testes;
    a escolha entre elas e de performance, nao de correcao."""

    ROW_LOCK = "row_lock"        # READ COMMITTED + SELECT FOR UPDATE ordenado
    SERIALIZABLE = "serializable"  # SERIALIZABLE + retry em SQLSTATE 40001


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="LEDGER_", env_file=".env",
                                      extra="ignore")

    database_url: str = "postgresql+asyncpg://ledger:ledger@localhost:5432/ledger"
    redis_url: str = "redis://localhost:6379/0"

    lock_strategy: LockStrategy = LockStrategy.ROW_LOCK
    serializable_max_retries: int = 5
    serializable_base_backoff_s: float = 0.005

    # outbox
    outbox_batch_size: int = 100
    outbox_max_attempts: int = 8
    outbox_lease_seconds: int = 60
    outbox_poll_interval_seconds: float = 1.0
    webhook_timeout_seconds: float = 10.0

    # rate limiting
    rate_limit_enabled: bool = True
    rate_limit_requests: int = 600
    rate_limit_window_seconds: int = 60

    db_pool_size: int = 20
    db_max_overflow: int = 40
    db_echo: bool = False


settings = Settings()
