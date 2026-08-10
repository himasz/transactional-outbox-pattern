-- Transactional Outbox schema.
--
-- Applied by the service that OWNS this database (see Schema.java). The relay
-- validates the schema on startup but never migrates it: the owning service and
-- the relay are deployed independently, so migration ownership must sit with
-- exactly one of them.

CREATE TABLE IF NOT EXISTS outbox_message (
    id         BIGSERIAL   PRIMARY KEY,

    -- The 64-bit transaction id of the INSERT. This is the load-bearing column
    -- for FIFO: `id` is handed out when the row is inserted, NOT when the
    -- transaction commits, so two concurrent writers can commit out of id order
    -- and leave a temporary hole. The relay uses tx_id against the snapshot
    -- xmin to skip any batch that could still develop a hole. Requires PG 13+.
    tx_id      xid8        NOT NULL DEFAULT pg_current_xact_id(),
    subject    TEXT        NOT NULL,
    -- Sent as Nats-Msg-Id so JetStream can collapse the duplicates that
    -- at-least-once delivery inevitably produces.
    message_id UUID        NOT NULL,
    headers    JSONB       NOT NULL DEFAULT '{}'::jsonb,
    payload    BYTEA       NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- A single watermark row rather than a per-row `sent` flag: FIFO means the relay
-- advances one cursor, so there is nothing to update per message and retention
-- is a single range delete behind the cursor.
CREATE TABLE IF NOT EXISTS outbox_cursor (
    name          TEXT        PRIMARY KEY,
    last_id       BIGINT      NOT NULL DEFAULT 0,
    -- Monotonic token from the leader elector. Every cursor write is conditional
    -- on this, which is what stops a stale leader corrupting the watermark.
    fencing_token BIGINT      NOT NULL DEFAULT 0,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Used only by PostgresLeaseElector. Present here so the example is genuinely
-- distributed; the brief permits mocking election, and MockLeaderElector does
-- not touch this table.
CREATE TABLE IF NOT EXISTS outbox_lease (
    name          TEXT        PRIMARY KEY,
    owner         TEXT        NOT NULL,
    fencing_token BIGINT      NOT NULL DEFAULT 0,
    expires_at    TIMESTAMPTZ NOT NULL
);

INSERT INTO outbox_cursor (name) VALUES ('default') ON CONFLICT (name) DO NOTHING;
