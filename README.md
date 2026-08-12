# Transactional Outbox

A library implementation of the Transactional Outbox pattern for PostgreSQL and NATS JetStream,
built for services running as multiple replicas without distributed transactions.

## Quick start

```bash
make build      # compile and package
make test       # test suite (needs a Docker socket for Testcontainers)
make example    # stand up the full distributed demo and follow the consumer
make verify     # counts summary and relay lag
make down       # tear everything down
```

Prerequisites: JDK 21, Maven 3.9+, Docker with Compose v2.

`make example` starts PostgreSQL, NATS with JetStream, **two** relay instances, **three**
producer replicas writing concurrently, and a consumer that verifies ordering end to end. The
consumer log is the demo: every line is a message that survived the round trip, and any FIFO
violation is printed as an error.

Two things worth watching:

- Every tenth producer transaction is rolled back on purpose. Run `make verify` to *prove* those
  orders were never published — see **Verifying the rollback guarantee** below.
- `make demo-failover` kills the active relay. The standby acquires the lease within about ten
  seconds and the stream continues, in order, from where the cursor left off.

## Verifying the rollback guarantee

"The transaction rolled back so nothing was published" cannot be checked by reading logs. A
missing message id looks **identical** whether the transaction was rolled back deliberately or
the relay silently skipped it — so a log line saying "gap expected" proves nothing, and would go
on printing happily while the pattern was broken.

The demo therefore records intent and outcome as data:

- when a producer rolls back, it writes the ref to `rollback_audit` in a **separate, committed**
  transaction;
- the consumer writes every ref it receives to `delivered`, upserting so redeliveries are counted
  rather than hidden.

`make verify` then reduces both guarantees to SQL that must return zero rows:

```sql
-- nothing rolled back may ever have been delivered
SELECT a.ref FROM rollback_audit a JOIN delivered d ON d.ref = a.ref;

-- nothing committed may be missing from the delivered set
SELECT o.ref FROM orders o LEFT JOIN delivered d ON d.ref = o.ref WHERE d.ref IS NULL;
```

The second query is the one that catches a broken gap-free read: a lost message shows up as a
committed order with no delivery. `duplicates > 0` in the summary is expected and correct —
delivery is at-least-once.

`rollback_audit` and `delivered` are **demo instrumentation only**. They are created by the
example, not by `schema.sql`; the library has no opinion about how you observe your own
rollbacks.

## The problem, and why this shape

Two systems must agree: a row must exist in PostgreSQL **and** a message must reach NATS, or
neither. Classic two-phase commit would do it, but NATS is not an XA participant, so 2PC is not
available even in principle — which is exactly why the pattern exists.

The outbox collapses the distributed problem into a local one. The message is written into the
same database, in the same transaction, as the business data. That commit is an ordinary
single-resource commit: atomic for free. A separate relay then reads committed rows and publishes
them. The publish is deliberately *not* part of the transaction, which is where at-least-once
delivery comes from and why consumers must be idempotent.

```
        ┌──────────── one local transaction ────────────┐
        │  INSERT INTO orders ...                       │   atomic, no 2PC
        │  INSERT INTO outbox_message ...               │
        └───────────────────────────────────────────────┘
                          │ commit returns
                          ▼
                  nudge over NATS  ──────►  relay wakes  ──►  publish  ──►  advance cursor
```

## Schema

| Table            | Purpose |
|------------------|---------|
| `outbox_message` | Pending messages. `tx_id` is the column that makes FIFO safe. |
| `outbox_cursor`  | A single watermark row plus the fencing token that guards it. |
| `outbox_lease`   | Used only by `PostgresLeaseElector`; the mock does not touch it. |

A cursor rather than a per-row `sent` flag: FIFO means the relay advances one watermark, so
there is nothing to update per message and retention becomes a single range delete.

## How each requirement is met

### No distributed transactions

`OutboxWriter.enqueue(Connection, OutboxMessage)` takes **the caller's** connection. There is
only ever one resource manager in the transaction. `enqueue` throws if the connection is in
auto-commit mode, because that silently defeats the entire pattern.

**Two ways a caller can still break this**, both covered by tests in `RollbackTest` and neither
detectable by the library:

- **Enqueueing on a second connection.** If the message is written through a different connection
  than the business writes, it is a different transaction and commits independently. Rolling back
  the business transaction then leaves an orphaned event for an order that does not exist. The
  auto-commit guard in `enqueue` catches the careless version of this; it cannot catch a second
  connection that has auto-commit switched off, because JDBC offers no portable way to ask whether
  two connections share a logical transaction.
- **Rolling back to a savepoint after enqueueing.** The outbox row is undone while the business
  rows survive, producing committed data with no event. Never enqueue inside a savepoint you might
  roll back.

### Suitable for multiple replicas

Every replica may write. Exactly one relay publishes at a time, enforced by the leader elector.
Followers run the same loop but `tryAcquire()` returns empty and they idle on a virtual thread.

### No LISTEN/NOTIFY, minimal delay

After `commit()` **returns**, the library publishes an empty message to `outbox.wakeup` on core
NATS. The relay is blocked on that subscription, so it wakes in well under a millisecond. A
250 ms idle tick is the safety net for a dropped nudge — it bounds worst-case latency, not
typical latency.

Because the nudge is a network message rather than an in-process channel, the relay can move out
of the writing service into its own deployment without a single code change. That is why
`outbox-runner` exists and why it is only fifty lines.

Sending the nudge *after* commit returns, and deliberately not in a `finally`, is the whole of
the "must not publish on rollback" requirement: on rollback the row is gone, so there is nothing
to publish and no compensation to run.

### FIFO — the part that is easy to get wrong

The naive relay query is `WHERE id > cursor ORDER BY id`. It loses messages under concurrency.

`BIGSERIAL` assigns ids at **INSERT** time, not **COMMIT** time. So:

```
Tx A  ├─ takes id 42 ──────────────────────────────┤ commits
Tx B        ├─ takes id 43 ───────┤ commits
                                       ▲
                                       relay polls here: sees 43, never 42
```

The relay publishes 43, advances the cursor past it, and 42 becomes invisible forever.

The fix is in `OutboxStore.FETCH_BATCH_SQL`:

```sql
WHERE id > ?
  AND tx_id < pg_snapshot_xmin(pg_current_snapshot())
ORDER BY id
```

`pg_snapshot_xmin` is the oldest transaction still running. A row whose `tx_id` is strictly older
was inserted by a transaction that has already finished, so no lower id can subsequently appear.
In the diagram above the relay simply does not see 43 yet; it waits for A to resolve and then
reads 42 and 43 together, in order.

`FifoOrderingTest.sequenceHoleIsNotSkipped` reproduces this precisely. Delete the `tx_id`
predicate and the test fails.

**Known trade-off:** one long-running transaction anywhere in the database stalls the outbox.
This is why *cursor lag* is the metric to alert on, not relay uptime.

Ordering also requires publishing strictly one at a time and awaiting each JetStream ack. The
async API is faster but completes acks out of order, which both breaks FIFO and leaves the relay
unable to tell where the stream actually stopped after a partial failure. Throughput is traded
for ordering on purpose; see *Extending* below for how to buy it back.

### At-least-once

The cursor advances only after a message is acknowledged by JetStream. A crash between publish
and cursor advance replays the batch — by design. `Nats-Msg-Id` lets JetStream's duplicate window
absorb most replays, but that is an optimisation, not a guarantee. **Consumers must be
idempotent.**

## Leader election

The brief permits mocking this, and `MockLeaderElector` is what tests use. Two things are worth
saying explicitly.

**Election is load-bearing, not decoration.** The fencing token stops a stale leader corrupting
the *watermark*, but it cannot un-publish. If two relays are live at once they interleave writes
into the broker and ordering is already broken before any cursor update happens. Exactly one
active publisher is a hard requirement for FIFO.

**The interface shape is the design decision.** A `boolean isLeader()` would imply leadership is
stable enough to check once. Every real implementation is lease-based and leases expire mid-work
during a partition or a long GC pause, so `LeaderElector` hands out a revocable `Lease` with:

- `isValid()` — re-checked between individual publishes, so a relay that loses leadership
  mid-batch stops immediately rather than at the end of the batch;
- `fencingToken()` — monotonic across handovers, and every cursor write is conditional on it:

```sql
UPDATE outbox_cursor SET last_id = ?, fencing_token = ?
 WHERE name = 'default' AND fencing_token <= ?
```

Zero rows updated means a newer leader exists, and this process steps down rather than trusting
its own view of leadership. So a brief split-brain degrades to **duplicates**, never to loss or
gaps.

`PostgresLeaseElector` is included so the example is genuinely distributed rather than relying on
a mock that says yes to everybody. It is also the architectural argument: PostgreSQL is already a
linearizable, durable, highly available store, so standing up a separate consensus cluster purely
to elect this leader would be redundant consensus with a real operational bill — peer discovery, a
persistent log, snapshots, quorum-aware rolling deploys. In production I would use a Kubernetes
`Lease` object, which is etcd, and therefore Raft, without operating Raft yourself.

## Embedded or standalone

`RelayEngine` does not know which mode it is in. `outbox-runner` is a thin `main()` around the
same class.

Standalone is what I would run in production: the relay is upgraded without redeploying every
service that writes to the outbox, and its resource profile is separate from the request path.

**One relay deployment per database, never one shared across services.** A relay reading several
services' outbox tables needs credentials to every database and couples itself to every schema.
Reuse belongs at the image level, not the instance level.

When the relay is standalone, the owning service applies `schema.sql` and the relay treats it as
a read contract — otherwise two independently released artifacts race on migrations.

## Tests

Run against real PostgreSQL via Testcontainers, because the guarantees rest on PostgreSQL
snapshot visibility and pre-commit id assignment. Neither can be faked, and a fake would hide the
exact bug these tests exist to catch. The broker is faked, since none of the logic under test is
NATS-specific.

| Test | What it pins down |
|------|-------------------|
| `RollbackTest` | Rollback publishes nothing (both deliberate and constraint-driven); commit publishes exactly once; auto-commit is rejected; the second-connection and savepoint anti-patterns are pinned as known limitations |
| `FifoOrderingTest` | The sequence hole is not skipped; concurrent writers yield a strictly increasing stream |
| `FencingTest` | Followers idle; a stale token is rejected; a lease lost mid-batch stops publishing; the Postgres lease is exclusive |
| `WakeupLatencyTest` | A commit wakes the relay without waiting for the poll interval |

## Known limitations and extensions

- **Global FIFO caps throughput** at one sequential publisher. The intended extension is a
  `partition_key` column with one cursor row per partition, giving FIFO *within* a key and
  parallelism across keys. Ordering guarantees would then need stating per key rather than
  globally.
- **Head-of-line blocking is by design.** A message that repeatedly fails to publish stalls the
  queue, because skipping it would violate FIFO. Production needs a bounded retry with backoff
  and then an explicit decision: park the message and continue, or halt and alert. The trade-off
  is real either way; what matters is choosing deliberately.
- **A long-running transaction delays the outbox**, per the xmin watermark above.
- **`xid8` requires PostgreSQL 13+.** On older versions, `txid_current()` and
  `txid_snapshot_xmin()` are the 32-bit equivalents, with wraparound to think about.
- **Cursor lag is the health signal.** A relay that is alive but wedged behind a poison message
  looks perfectly healthy to a liveness probe.