# End to end: `make example`, start to finish

Think of it like a restaurant with a bulletin board instead of shouting orders across the
kitchen. Nobody talks to NATS directly — they pin a note to the board *as part of* writing the
order ticket, and one runner keeps checking the board and delivering notes in order. Here's that
story traced through the actual code.

## 0. Cold start — `make example`

`docker-compose.yml` brings everything up in dependency order:

```
postgres, nats  (must be healthy first)
      │
      ├─► relay-1, relay-2      (two copies of RelayMain — only one will lead)
      ├─► producer-1/2/3        (three copies of ProducerMain)
      └─► consumer              (ConsumerMain)
```

Nothing coordinates who does what yet — that gets figured out live, which is the whole point of
the demo.

## 1. A relay boots but can't do anything yet

`RelayMain.main()` wires up one `RelayEngine` per relay instance: a `PostgresLeaseElector`, an
`OutboxStore`, a `NatsPublisher`. It calls `engine.start()`, which spins up a virtual thread that
blocks waiting for a wake-up nudge. **Both relay-1 and relay-2 are now just sitting there,
parked.**

## 2. A producer writes an order — this is the one moment the whole pattern exists for

`ProducerMain.placeOrder()` calls `outbox.inTransaction((tx, writer) -> ...)`. Inside that one
transaction, on the one connection:

```sql
INSERT INTO orders (ref) VALUES (?)              -- the real business row
INSERT INTO outbox_message (subject, ...) ...    -- writer.enqueue(...)
```

Both inserts live or die together — that's `OutboxWriter.enqueue` in
`outbox-core/.../store/OutboxWriter.java`, and it's why no 2PC is needed: Postgres is the only
participant. If `rollback=true` (every 10th order, on purpose), `Outbox.inTransaction` rolls both
inserts back and **stops** — no nudge is ever sent, because the nudge only fires after `commit()`
*returns* (`Outbox.java:66`).

## 3. Commit succeeds → the nudge

The instant `tx.commit()` returns, `Outbox` calls `wakeup.signal()` → `NatsWakeupSignal` fires an
empty message on `outbox.wakeup`. This isn't the actual order data — it's just a doorbell.
Sub-millisecond, no polling involved.

## 4. Whichever relay is leader wakes up and drains the outbox

Both relays are blocked on that same doorbell subject, so both wake. But only one of them
actually gets to publish — `RelayEngine.tick()` first calls `elector.tryAcquire()`. The
`PostgresLeaseElector` uses a row in `outbox_lease` as the lock; one relay gets a `Lease` with a
fencing token, the other gets `Optional.empty()` and goes straight back to sleep.

The leader then, in `RelayEngine.drain()`:

```
cursor = store.readCursor()
batch  = store.fetchBatch(cursor, ...)   ← the tx_id/xmin trick, so no sequence hole is skipped
for each row: publisher.publish(row)     ← NatsPublisher, waits for JetStream ack
advance cursor past the last published row, fenced by the lease token
```

If relay-1 dies mid-stream, relay-2's next `tryAcquire()` succeeds, gets a higher fencing token,
and the cursor update guard (`fencing_token <= ?`) makes any zombie relay-1 write silently fail
instead of corrupting the watermark.

## 5. If the leader dies mid-stream

`docker compose kill relay-1` (`make demo-failover`) is the live version of this — relay-2 takes
the lease within about ten seconds and keeps going. `FencingTest.successorResumesAfterCrash` is
the automated version, and it pins down something less obvious than "relay-2 keeps going":
publishing to the broker and advancing the cursor are two separate steps, not one atomic move.

If relay-1 crashes between them — id 3 already reached NATS, but the cursor is still stamped at
2 — relay-2 has no way to tell that happened, so it legitimately **republishes** id 3. Ids already
past the cursor (1, 2) are never touched again; only the unconfirmed tail replays. Across the
handover, the id sequence a subscriber sees can be `1, 2, 3, 3, 4, 5` — not strictly increasing —
which is exactly why `ConsumerMain`'s FIFO check only flags an id going *backwards*, and why
`recordDelivery` is an idempotent upsert rather than a plain insert. Same at-least-once contract
as step 4, just with a relay handover as the reason instead of a broker retry.

## 6. NATS JetStream durably holds the message

This is the actual handoff to a different system — everything before this point was one Postgres
transaction plus an in-process nudge.

## 7. The consumer reads it back and checks itself

`ConsumerMain` subscribes to `orders.>` and for every message: checks that `Outbox-Id` never goes
backwards (the live FIFO check), and upserts the ref into `delivered` (idempotent — duplicates
increment a counter instead of erroring, because delivery is at-least-once by design).

## 8. `make verify` closes the loop with SQL, not vibes

Two queries that must return zero rows:

```sql
-- rolled-back orders that leaked out anyway
SELECT a.ref FROM rollback_audit a JOIN delivered d ON d.ref = a.ref;

-- committed orders that never arrived
SELECT o.ref FROM orders o LEFT JOIN delivered d ON d.ref = o.ref WHERE d.ref IS NULL;
```

**The one-sentence version:** the write is a boring local commit; a doorbell tells a single
elected relay to look; the relay reads in commit order (not insert order — that's the
`tx_id`/xmin trick) and republishes until acked; the consumer trusts nothing it can't re-derive
from what actually arrived.
