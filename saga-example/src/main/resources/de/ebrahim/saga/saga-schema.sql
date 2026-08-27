-- Business tables for the choreographed order saga.
--
-- Everything here is ORDINARY BUSINESS DATA. There is no saga_state table, no
-- step log, no coordinator bookkeeping — that absence is the point. In
-- choreography the saga's position is implied by the state of the domain rows
-- and the events in flight; nothing owns the workflow, so nothing has to store
-- it.
--
-- All five tables would live in five separate databases in a real deployment.
-- They share one here so the demo needs one container and so verify-saga.sql
-- can check invariants that span services. Nothing in the code joins across
-- them: each service touches only its own tables, plus its own inbox and outbox.
--
-- Every table carries a saga_ prefix. That is not decoration: sharing a database
-- means sharing a namespace, and unprefixed names like `payment` and `shipment`
-- are exactly the ones another module's demo or test will also want. The prefix
-- removes the collision as a class rather than waiting to hit an instance of it.

-- ---------------------------------------------------------------- order svc --
CREATE TABLE IF NOT EXISTS saga_order (
    ref           TEXT        PRIMARY KEY,
    amount_cents  BIGINT      NOT NULL,
    sku           TEXT        NOT NULL,
    quantity      INT         NOT NULL,
    destination   TEXT        NOT NULL,

    --   PENDING    created, awaiting payment
    --   PAID       payment authorized
    --   RESERVED   stock reserved
    --   COMPLETED  terminal, happy path
    --   CANCELLED  terminal, after compensation completed
    status        TEXT        NOT NULL DEFAULT 'PENDING',
    cancel_reason TEXT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT saga_order_status CHECK (status IN
        ('PENDING', 'PAID', 'RESERVED', 'COMPLETED', 'CANCELLED'))
);

-- -------------------------------------------------------------- payment svc --
CREATE TABLE IF NOT EXISTS saga_payment (
    ref          TEXT        PRIMARY KEY,
    amount_cents BIGINT      NOT NULL,
    --   AUTHORIZED  funds held
    --   DECLINED    never held
    --   REFUNDED    held, then released by a compensating step
    status       TEXT        NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    refunded_at  TIMESTAMPTZ,
    CONSTRAINT saga_payment_status CHECK (status IN ('AUTHORIZED', 'DECLINED', 'REFUNDED'))
);

-- ------------------------------------------------------------ inventory svc --
CREATE TABLE IF NOT EXISTS saga_stock (
    sku       TEXT   PRIMARY KEY,
    available INT    NOT NULL,
    -- Never changes after seeding. Its only job is to make the conservation
    -- invariant checkable: available + outstanding reservations must always
    -- equal this, and it will not if a compensating release is ever missed.
    initial   INT    NOT NULL,
    CONSTRAINT saga_stock_not_negative CHECK (available >= 0)
);

CREATE TABLE IF NOT EXISTS saga_stock_reservation (
    ref         TEXT        PRIMARY KEY,
    sku         TEXT        NOT NULL,
    quantity    INT         NOT NULL,
    released    BOOLEAN     NOT NULL DEFAULT false,
    reserved_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    released_at TIMESTAMPTZ
);

-- ------------------------------------------------------------- shipping svc --
CREATE TABLE IF NOT EXISTS saga_shipment (
    ref         TEXT        PRIMARY KEY,
    destination TEXT        NOT NULL,
    status      TEXT        NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT saga_shipment_status CHECK (status IN ('DISPATCHED', 'REFUSED'))
);

-- Seed stock. SKU-SCARCE is deliberately tiny: it runs out partway through the
-- demo, which is what drives the compensating path. A modulus-based "every Nth
-- order fails" would be easier, and would prove less — this failure is real
-- contention over a real resource, and the conditional decrement that detects
-- it is the same one a production service would write.
INSERT INTO saga_stock (sku, available, initial) VALUES
    ('SKU-COMMON',  1000000, 1000000),
    ('SKU-REGULAR', 1000000, 1000000),
    ('SKU-SCARCE',       12,      12)
ON CONFLICT (sku) DO NOTHING;
