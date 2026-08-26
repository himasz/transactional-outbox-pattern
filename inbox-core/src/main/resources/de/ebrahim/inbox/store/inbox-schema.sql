-- Transactional Inbox schema — the consumer-side counterpart to the outbox.
--
-- The outbox guarantees at-least-once delivery: the relay can crash between
-- publishing and advancing its cursor, so the same message legitimately arrives
-- more than once. This table is what turns that into exactly-once *effects*.
--
-- Applied by the service that OWNS this database, exactly like schema.sql on the
-- producing side. A consumer's inbox lives in the consumer's own database — it
-- must be the same database the handler writes its business rows to, or the
-- whole point (one local transaction, no 2PC) is lost.

CREATE TABLE IF NOT EXISTS inbox_message (
    -- Which logical consumer processed this. Part of the key because two
    -- independent handlers in the same service must each get their own chance
    -- at the same message; deduplicating on message_id alone would let the
    -- first handler silently swallow the message for the second.
    consumer     TEXT        NOT NULL,

    -- The producer's own message id, arriving as the Nats-Msg-Id header. It is
    -- generated once by OutboxMessage and is stable across every redelivery,
    -- which is precisely what makes it usable as a deduplication key. The
    -- JetStream sequence number is NOT usable: a republish after a relay
    -- failover gets a new sequence for the same logical message.
    message_id   UUID        NOT NULL,

    -- Arrival order, used only by the staging processor. BIGSERIAL, so it is
    -- assigned at INSERT time and carries the same commit-order hazard the
    -- outbox documents at length; see tx_id below.
    seq          BIGSERIAL   NOT NULL,

    -- Same load-bearing column as outbox_message.tx_id, for the same reason. If
    -- more than one receiver replica stages messages concurrently, seq can
    -- develop a temporary hole, and a processor reading ORDER BY seq would step
    -- over the hole and process out of order. The processor's fetch compares
    -- this against the snapshot xmin. Requires PG 13+.
    tx_id        xid8        NOT NULL DEFAULT pg_current_xact_id(),

    subject      TEXT        NOT NULL,
    headers      JSONB       NOT NULL DEFAULT '{}'::jsonb,

    -- NULL in inline mode. Inline processing never needs to replay the message
    -- from the database, because the handler already ran against it before the
    -- row committed, so storing a second copy of every payload would be pure
    -- cost. Staging mode stores it, because the whole point there is that the
    -- broker is acknowledged before the work happens.
    payload      BYTEA,

    --   PENDING  staged and not yet handled, or handled and failed
    --   DONE     the handler committed; the effects exist exactly once
    --   DEAD     parked after maxAttempts; will never be retried automatically
    --
    -- Note that DONE and DEAD both suppress reprocessing, and PENDING does not.
    -- That is the entire state machine, and every claim query keys off it.
    status       TEXT        NOT NULL DEFAULT 'PENDING',

    attempts     INT         NOT NULL DEFAULT 0,
    last_error   TEXT,
    received_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    processed_at TIMESTAMPTZ,

    PRIMARY KEY (consumer, message_id),
    CONSTRAINT inbox_message_status_valid CHECK (status IN ('PENDING', 'DONE', 'DEAD'))
);

-- Partial index: the processor only ever scans the head of the PENDING queue,
-- and PENDING is a vanishing fraction of the table in steady state.
CREATE INDEX IF NOT EXISTS inbox_message_pending
    ON inbox_message (consumer, seq) WHERE status = 'PENDING';

-- Supports the retention sweep without scanning live rows.
CREATE INDEX IF NOT EXISTS inbox_message_done
    ON inbox_message (consumer, processed_at) WHERE status = 'DONE';
