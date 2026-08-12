-- =============================================================================
-- Transactional outbox
--
-- COMMIT da transferencia e POST para o cliente nao podem ser atomicos entre si.
-- Qualquer ordem ingenua perde:
--   envia antes do commit -> commit falha, cliente recebeu evento de algo que nao
--                            aconteceu
--   envia depois do commit -> processo morre no meio, transacao existe sem evento
--
-- Solucao: o evento e gravado na MESMA transacao do lancamento. Se a transferencia
-- commitou, o evento existe. Se nao commitou, o evento nao existe. Nunca diverge.
-- =============================================================================

CREATE TABLE webhook_endpoints (
    id          UUID        PRIMARY KEY,
    owner_id    UUID        NOT NULL,
    url         TEXT        NOT NULL,
    secret      BYTEA       NOT NULL,          -- chave HMAC-SHA256
    event_types TEXT[]      NOT NULL DEFAULT '{}',  -- vazio = todos
    active      BOOLEAN     NOT NULL DEFAULT true,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT webhook_url_is_http CHECK (url ~ '^https?://')
);

CREATE INDEX webhook_endpoints_active_idx ON webhook_endpoints (owner_id)
    WHERE active;

CREATE TYPE outbox_status AS ENUM ('pending','delivering','delivered','dead');

CREATE TABLE outbox (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    endpoint_id     UUID          NOT NULL REFERENCES webhook_endpoints(id),
    event_type      TEXT          NOT NULL,   -- transaction.posted | transaction.reversed
    aggregate_id    UUID          NOT NULL,   -- transaction_id
    payload         JSONB         NOT NULL,
    status          outbox_status NOT NULL DEFAULT 'pending',
    attempts        INT           NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ   NOT NULL DEFAULT now(),
    last_error      TEXT,
    created_at      TIMESTAMPTZ   NOT NULL DEFAULT now(),
    delivered_at    TIMESTAMPTZ,

    CONSTRAINT outbox_attempts_non_negative CHECK (attempts >= 0)
);

-- Indice PARCIAL: linhas ja entregues saem do indice, entao ele fica do tamanho do
-- backlog e nao do historico. E o que mantem a fila rapida com 50M de eventos
-- entregues acumulados.
CREATE INDEX outbox_due_idx ON outbox (next_attempt_at, id)
    WHERE status IN ('pending','delivering');

CREATE INDEX outbox_aggregate_idx ON outbox (aggregate_id);
