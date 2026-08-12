CREATE TYPE idempotency_status AS ENUM ('in_flight','completed','failed');

CREATE TABLE idempotency_keys (
    scope           TEXT               NOT NULL,   -- ex.: 'POST /v1/transfers'
    key             TEXT               NOT NULL,   -- header Idempotency-Key
    request_hash    TEXT               NOT NULL,   -- sha256 do payload canonicalizado
    status          idempotency_status NOT NULL DEFAULT 'in_flight',
    response_status INT,
    response_body   JSONB,
    transaction_id  UUID REFERENCES transactions(id),
    created_at      TIMESTAMPTZ        NOT NULL DEFAULT now(),
    expires_at      TIMESTAMPTZ        NOT NULL,

    -- scope na PK: a mesma chave em endpoints diferentes nao pode colidir.
    PRIMARY KEY (scope, key)
);

-- suporta o job de expurgo por expires_at
CREATE INDEX idempotency_expires_idx ON idempotency_keys (expires_at);
