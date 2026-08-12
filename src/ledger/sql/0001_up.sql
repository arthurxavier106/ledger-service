-- =============================================================================
-- Ledger schema v1
-- As invariantes de dinheiro (I1-I4) sao garantidas AQUI, nao na aplicacao.
-- Ver DESIGN.md secao 1.
-- =============================================================================

CREATE TYPE account_type       AS ENUM ('asset','liability','equity','revenue','expense');
CREATE TYPE account_status     AS ENUM ('active','frozen','closed');
CREATE TYPE transaction_kind   AS ENUM ('transfer','deposit','withdrawal','reversal');
CREATE TYPE transaction_status AS ENUM ('posted','reversed');
CREATE TYPE entry_direction    AS ENUM ('debit','credit');

-- -----------------------------------------------------------------------------
CREATE TABLE currencies (
    code     CHAR(3)  PRIMARY KEY,
    exponent SMALLINT NOT NULL CHECK (exponent BETWEEN 0 AND 4),
    name     TEXT     NOT NULL
);

INSERT INTO currencies (code, exponent, name) VALUES
    ('BRL', 2, 'Brazilian Real'),
    ('USD', 2, 'US Dollar'),
    ('EUR', 2, 'Euro'),
    ('JPY', 0, 'Japanese Yen');

-- -----------------------------------------------------------------------------
CREATE TABLE accounts (
    id             UUID           PRIMARY KEY,
    external_id    TEXT           NOT NULL,
    owner_id       UUID           NOT NULL,
    currency       CHAR(3)        NOT NULL REFERENCES currencies(code),
    type           account_type   NOT NULL,
    status         account_status NOT NULL DEFAULT 'active',
    balance        BIGINT         NOT NULL DEFAULT 0,
    allow_negative BOOLEAN        NOT NULL DEFAULT false,
    version        BIGINT         NOT NULL DEFAULT 0,
    created_at     TIMESTAMPTZ    NOT NULL DEFAULT now(),
    updated_at     TIMESTAMPTZ    NOT NULL DEFAULT now(),

    CONSTRAINT accounts_external_id_uniq UNIQUE (external_id),
    -- I2: rede de seguranca final. Nem a aplicacao, nem psql, nem migration
    -- conseguem deixar uma conta de usuario negativa.
    CONSTRAINT accounts_balance_non_negative CHECK (allow_negative OR balance >= 0)
);

CREATE INDEX accounts_owner_idx ON accounts (owner_id);

-- -----------------------------------------------------------------------------
CREATE TABLE transactions (
    id                      UUID               PRIMARY KEY,
    kind                    transaction_kind   NOT NULL,
    status                  transaction_status NOT NULL DEFAULT 'posted',
    reverses_transaction_id UUID REFERENCES transactions(id),
    external_ref            TEXT,
    metadata                JSONB              NOT NULL DEFAULT '{}',
    created_at              TIMESTAMPTZ        NOT NULL DEFAULT now(),

    CONSTRAINT reversal_shape CHECK (
        (kind = 'reversal') = (reverses_transaction_id IS NOT NULL)
    )
);

-- I4: uma transacao e estornada no maximo uma vez.
CREATE UNIQUE INDEX transactions_single_reversal_uniq
    ON transactions (reverses_transaction_id)
    WHERE reverses_transaction_id IS NOT NULL;

CREATE INDEX transactions_created_at_idx ON transactions (created_at DESC);

-- -----------------------------------------------------------------------------
CREATE TABLE entries (
    id             BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    transaction_id UUID            NOT NULL REFERENCES transactions(id),
    account_id     UUID            NOT NULL REFERENCES accounts(id),
    direction      entry_direction NOT NULL,
    amount         BIGINT          NOT NULL CHECK (amount > 0),
    currency       CHAR(3)         NOT NULL REFERENCES currencies(code),
    balance_after  BIGINT          NOT NULL,
    created_at     TIMESTAMPTZ     NOT NULL DEFAULT now()
);

-- extrato por keyset: cobre WHERE account_id = ? ORDER BY id DESC
CREATE INDEX entries_account_id_idx  ON entries (account_id, id DESC);
CREATE INDEX entries_transaction_idx ON entries (transaction_id);

-- I1: debito == credito, verificado no COMMIT.
CREATE FUNCTION assert_transaction_balanced() RETURNS TRIGGER AS $fn$
DECLARE
    net BIGINT;
    n   INT;
BEGIN
    SELECT COALESCE(SUM(CASE WHEN direction = 'debit' THEN amount ELSE -amount END), 0),
           COUNT(*)
      INTO net, n
      FROM entries
     WHERE transaction_id = NEW.transaction_id;

    IF n < 2 THEN
        RAISE EXCEPTION 'transaction % has % entries, minimum is 2', NEW.transaction_id, n
            USING ERRCODE = 'check_violation';
    END IF;

    IF net <> 0 THEN
        RAISE EXCEPTION 'transaction % is unbalanced by %', NEW.transaction_id, net
            USING ERRCODE = 'check_violation';
    END IF;

    RETURN NULL;
END;
$fn$ LANGUAGE plpgsql;

-- DEFERRABLE INITIALLY DEFERRED: roda no COMMIT, nao no INSERT.
-- Sem isso o primeiro INSERT (o debito sozinho) ja falharia.
CREATE CONSTRAINT TRIGGER entries_balanced_trg
    AFTER INSERT ON entries
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION assert_transaction_balanced();

-- I3: append-only.
CREATE FUNCTION entries_are_immutable() RETURNS TRIGGER AS $fn$
BEGIN
    RAISE EXCEPTION 'entries are append-only (attempted %)', TG_OP
        USING ERRCODE = 'integrity_constraint_violation';
END;
$fn$ LANGUAGE plpgsql;

CREATE TRIGGER entries_immutable_trg
    BEFORE UPDATE OR DELETE ON entries
    FOR EACH ROW EXECUTE FUNCTION entries_are_immutable();

-- O trigger acima da a mensagem legivel; o REVOKE e a barreira real.
DO $do$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'ledger_app') THEN
        REVOKE UPDATE, DELETE ON entries FROM ledger_app;
    END IF;
END;
$do$;

-- -----------------------------------------------------------------------------
CREATE TABLE balance_snapshots (
    account_id     UUID        NOT NULL REFERENCES accounts(id),
    as_of_entry_id BIGINT      NOT NULL,
    balance        BIGINT      NOT NULL,
    taken_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (account_id, as_of_entry_id)
);
