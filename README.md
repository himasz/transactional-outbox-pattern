# Transactional Outbox

A library implementation of the Transactional Outbox pattern for PostgreSQL and NATS JetStream —
for services that run as multiple replicas and can't use a distributed transaction to keep their
database and their message broker in sync.

**The idea in one sentence:** instead of writing to your database *and* publishing to NATS as two
separate steps that can fail independently, write the outgoing message as an ordinary row in the
same transaction as your business data. A separate process then reads that row back and publishes
it for real. One system to be atomic about, not two.

See [FLOW.md](FLOW.md) for a start-to-finish trace of a message through the whole demo.

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
producer replicas writing concurrently, and a consumer that checks ordering as messages arrive.
The consumer log *is* the demo: every line is a message that survived the round trip, and any
FIFO violation is printed as an error.

Two things worth watching while it runs:

- Every tenth producer transaction is rolled back on purpose. `make verify` *proves* those orders
  were never published — see below, since "check the logs" isn't good enough for that claim.
- `make demo-failover` kills the active relay. The standby takes over within about ten seconds and
  the stream continues, in order, from where the cursor left off.

## Proving the rollback guarantee, not just claiming it

"A rolled-back transaction publishes nothing" can't be checked by reading logs — a missing message
id looks identical whether the rollback was deliberate or the relay just silently dropped it. So
the demo turns both outcomes into data instead of log lines:

- when a producer rolls back, it writes the ref to `rollback_audit` on a **separate, committed**
  connection;
- the consumer writes every ref it receives to `delivered`, upserting so redeliveries are counted
  instead of hidden.

`make verify` then reduces the whole guarantee to two SQL queries that must return zero rows:

```sql
-- nothing rolled back may ever have been delivered
SELECT a.ref FROM rollback_audit a JOIN delivered d ON d.ref = a.ref;

-- nothing committed may be missing from the delivered set
SELECT o.ref FROM orders o LEFT JOIN delivered d ON d.ref = o.ref WHERE d.ref IS NULL;
```

The second query is the one that would catch a broken relay: a lost message shows up as a
committed order with no delivery. `duplicates > 0` in the summary is fine — delivery is
at-least-once by design (more on that below).

`rollback_audit` and `delivered` are demo instrumentation only, created by the example — not by
`schema.sql`. The library itself has no opinion about how you audit your own rollbacks.

## The idea, in one picture

A two-phase commit across PostgreSQL and NATS isn't on the table — NATS isn't a transaction
participant, so there's no protocol to speak to it in. The outbox sidesteps the whole problem: the
message never needs a separate atomic hand-off to NATS, because it's committed as an ordinary row
first, in the transaction you already have.

```
        ┌──────────── one local transaction ────────────┐
        │  INSERT INTO orders ...                       │   atomic, no 2PC —
        │  INSERT INTO outbox_message ...               │   just an ordinary commit
        └───────────────────────────────────────────────┘
                          │ commit returns
                          ▼
                  nudge over NATS  ──────►  relay wakes  ──►  publish  ──►  advance cursor
```

Publishing is deliberately **not** part of that transaction — it can't be, NATS isn't a resource
manager. That's the origin of everything interesting below: at-least-once delivery, why consumers
must be idempotent, and why a separate relay process exists at all.

## Guarantees

| Guarantee | How |
|---|---|
| No distributed transaction | The message is a row, inserted through the caller's own connection in the caller's own transaction. One resource manager, one ordinary commit. |
| Any number of replicas can write | Every replica writes freely. Exactly one relay *publishes* at a time — see **Leader election**. |
| Low latency, no polling loop | A commit fires an empty NATS message as a "wake up" nudge; the relay is blocked waiting on it, not polling. A slow 250ms tick is just the safety net for a dropped nudge. |
| Never publishes a rolled-back write | The nudge fires only after `commit()` *returns*. Roll back, and there's no row and nothing to publish — no compensating logic needed. |
| Strict FIFO order | The part that's easy to get wrong — see below. |
| At-least-once delivery | The cursor only advances after the broker acknowledges. A crash between publish and advance replays the batch. **Consumers must be idempotent.** |

Every message carries a `Nats-Msg-Id`, so JetStream's duplicate window collapses most replays
before a consumer ever sees them — that's an optimization, though, not a guarantee to depend on.

**Two ways a caller can still defeat the "no distributed transaction" guarantee**, both covered by
`RollbackTest` and neither one the library can detect:

- **Enqueueing on a second connection.** That's a second transaction, committing independently —
  roll back the business write and the event survives it, orphaned. The auto-commit guard in
  `enqueue` catches the careless version; it can't catch a second connection with auto-commit
  already off, because JDBC has no portable way to ask "do these two connections share a
  transaction?"
- **Rolling back to a savepoint after enqueueing.** The outbox row is undone while the business
  rows survive — committed data with no event. Never enqueue inside a savepoint you might roll
  back past.

### FIFO — the part that's easy to get wrong

The obvious relay query is `WHERE id > cursor ORDER BY id`. It quietly loses messages under
concurrency, because `BIGSERIAL` hands out ids at **INSERT** time, not at **COMMIT** time:

```
Tx A  ├─ takes id 42 ──────────────────────────────┤ commits
Tx B        ├─ takes id 43 ───────┤ commits
                                       ▲
                                       relay polls here: sees 43, never 42
```

The relay publishes 43, advances the cursor past it, and 42 is now invisible forever — its
transaction hadn't committed yet when the relay looked.

The fix, in `OutboxStore.FETCH_BATCH_SQL`:

```sql
WHERE id > ?
  AND tx_id < pg_snapshot_xmin(pg_current_snapshot())
ORDER BY id
```

`pg_snapshot_xmin` is the oldest transaction still running. A row whose `tx_id` is older than that
came from a transaction that has already finished, so no lower id can still show up later. In the
diagram above, the relay simply doesn't see 43 yet — it waits for A to finish, then reads 42 and
43 together, in order. `FifoOrderingTest.sequenceHoleIsNotSkipped` reproduces the bug precisely;
delete the `tx_id` predicate and the test fails.

**Trade-off:** one long-running transaction anywhere in the database stalls the whole outbox. This
is why *cursor lag*, not relay uptime, is the metric worth alerting on.

Ordering also means publishing strictly one message at a time and waiting for each JetStream ack —
the faster async API completes acks out of order, which breaks FIFO and leaves the relay unable to
tell where the stream actually stopped after a partial failure. Throughput is traded for ordering
on purpose (see *Known limitations* for how to buy it back).

## Leader election

Exactly one relay may publish at a time. Two running concurrently would interleave writes into
NATS, and ordering would already be broken before either one touched the cursor — so this isn't
an optimization, it's load-bearing.

`LeaderElector` hands out a `Lease` rather than a plain `isLeader(): boolean`, because real leases
expire mid-work — a network partition, a long GC pause. `Lease.isValid()` is re-checked between
*every* publish, not once per batch, and every cursor write carries the lease's `fencingToken` so
a stale leader's write is silently rejected the moment someone newer has taken over:

```sql
UPDATE outbox_cursor SET last_id = ?, fencing_token = ?
 WHERE name = 'default' AND fencing_token <= ?
```

Zero rows updated means a newer leader exists, and the stale process steps down rather than
trusting its own view of the world. A brief split-brain degrades to **duplicates**, never to loss
or reordering.

Two implementations ship: `MockLeaderElector`, driveable directly in tests (`FencingTest`), and
`PostgresLeaseElector`, a real lease row in `outbox_lease` used by the demo. Postgres is already a
linearizable, durable store, so it's a lease backend for free — no separate consensus cluster to
run. (In production I'd reach for a Kubernetes `Lease` object for the same reason: it's etcd, and
therefore Raft, without operating Raft yourself.)

**What actually happens when a leader dies mid-batch:** no gaps, ever — but the message it had
already handed to the broker without yet advancing the cursor *will* be replayed by whoever takes
over. See `FLOW.md` step 5 and `FencingTest.successorResumesAfterCrash` for the deterministic,
no-Docker version of this proof.

## Layout

`de.ebrahim.outbox` holds only the public entry points; everything else is one subpackage per
responsibility, each swappable behind its own interface.

```
de.ebrahim.outbox
├── Outbox, RelayEngine, RelayConfig, OutboxMessage    ← public API
├── store/       OutboxWriter, OutboxStore, Schema
├── transport/   WakeupSignal, NatsWakeupSignal, MessagePublisher, NatsPublisher
└── election/    LeaderElector, MockLeaderElector, PostgresLeaseElector
```

Who calls whom:

```
Outbox ──writes via──► store.OutboxWriter
       ──nudges via──► transport.WakeupSignal

RelayEngine ──reads/advances cursor via──► store.OutboxStore
            ──publishes via─────────────► transport.MessagePublisher
            ──elects leader via─────────► election.LeaderElector
            ──wakes on──────────────────► transport.WakeupSignal
```

`RelayEngine` is the only class that depends on all three subpackages — it exists to wire
election, storage and transport into the relay loop. `store`, `transport` and `election` never
depend on each other, so each is testable and replaceable on its own.

## Schema

| Table | Purpose |
|---|---|
| `outbox_message` | Pending messages. `tx_id` is the column that makes FIFO safe. |
| `outbox_cursor` | A single watermark row plus the fencing token that guards it. |
| `outbox_lease` | Used only by `PostgresLeaseElector`; the mock elector never touches it. |

A cursor rather than a per-row `sent` flag: since delivery is strictly in order, one watermark is
enough, there's nothing to update per message, and retention becomes a single range delete.

## Embedded or standalone

`RelayEngine` doesn't know which mode it's in. `Outbox` embeds it in your service; `outbox-runner`
is a thin `main()` wrapping the exact same class as its own deployment.

Standalone is what I'd run in production: the relay upgrades independently of every service that
writes to the outbox, with its own resource profile, off the request path. **One relay per
database, never one shared across services** — a shared relay would need credentials to every
database and couple itself to every schema; reuse belongs at the container-image level, not the
running-instance level.

When the relay runs standalone, the owning service applies `schema.sql` and the relay treats it as
a read-only contract — otherwise two independently released artifacts race on migrations.

## Tests

Run against a real PostgreSQL via Testcontainers, because the guarantees rest on PostgreSQL
snapshot visibility and pre-commit id assignment — neither can be faked without hiding the exact
bug the test exists to catch. The broker is faked, since none of the logic under test is
NATS-specific.

| Test | What it pins down |
|---|---|
| `RollbackTest` | Rollback publishes nothing (deliberate or constraint-driven); commit publishes exactly once; auto-commit is rejected; the second-connection and savepoint anti-patterns (see **Guarantees**) are pinned as known limitations |
| `FifoOrderingTest` | The sequence hole above is not skipped; concurrent writers yield a strictly increasing stream |
| `FencingTest` | Followers idle; a stale token is rejected; a lease lost mid-batch stops publishing; the Postgres lease is exclusive; a successor resumes from the cursor after a crash, replaying only the un-advanced tail |
| `WakeupLatencyTest` | A commit wakes the relay without waiting for the poll interval |

## Known limitations and extensions

- **Global FIFO caps throughput** at one sequential publisher. The intended extension is a
  `partition_key` column with one cursor row per partition — FIFO *within* a key, parallelism
  across keys, and ordering guarantees stated per key rather than globally.
- **Head-of-line blocking is by design.** A message that repeatedly fails to publish stalls the
  whole queue, because skipping it would violate FIFO. Production needs a bounded retry with
  backoff and then a deliberate choice: park the message and continue, or halt and alert.
- **A long-running transaction delays the outbox** — the `xmin` trade-off above.
- **`xid8` requires PostgreSQL 13+.** Older versions need the 32-bit equivalents
  (`txid_current()`, `txid_snapshot_xmin()`), with wraparound to account for.
- **Cursor lag is the health signal.** A relay that's alive but wedged behind a poison message
  looks perfectly healthy to a liveness probe.
