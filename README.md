# Transactional Patterns

Three transactional messaging patterns for PostgreSQL and NATS JetStream — the **outbox**, the
**inbox**, and a **choreographed saga** that is just the two of them composed — for services that
run as multiple replicas and can't use a distributed transaction to keep their database and their
message broker in sync.

**The outbox, in one sentence:** instead of writing to your database *and* publishing to NATS as two
separate steps that can fail independently, write the outgoing message as an ordinary row in the
same transaction as your business data. A separate process then reads that row back and publishes
it for real. One system to be atomic about, not two.

**And its other half.** Reading the row back and publishing it can't be atomic either, so delivery
is at-least-once by construction — the same message legitimately arrives twice. That's only a safe
design if something on the far side absorbs the repeat, so `inbox-core` is that something: the same
trick run backwards, turning at-least-once *delivery* into exactly-once *effects*. See
[The inbox](#the-inbox-the-other-half).

**And what the two compose into.** An inbox that claims the input and an outbox that emits the
next event, both on one connection, are all a choreographed saga needs — no coordinator, no state
machine. `saga-example` is that, across four services. See [Choreographed saga](#choreographed-saga).

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

The saga demo is separate — it runs its own Postgres, NATS and relay on their own ports via
`saga-example/docker-compose.saga.yml`. See [Choreographed saga](#choreographed-saga).

`make example` starts PostgreSQL, NATS with JetStream, **two** relay instances, **three**
producer replicas writing concurrently, a consumer that checks ordering as messages arrive and
applies them through an inbox, and a fulfilment service that consumes one event and produces
another in a single transaction. The consumer log *is* the demo: every line is a message that
survived the round trip, and any FIFO violation is printed as an error.

Three things worth watching while it runs:

- Every tenth producer transaction is rolled back on purpose. `make verify` *proves* those orders
  were never published — see below, since "check the logs" isn't good enough for that claim.
- Every fiftieth message is committed and then deliberately **not acknowledged** — precisely a
  crash in the window between commit and ack. JetStream redelivers it, and the inbox has to absorb
  it. `make demo-duplicates` shows it happening.
- `make demo-failover` kills the active relay. The standby takes over within about ten seconds and
  the stream continues, in order, from where the cursor left off.

## Proving the guarantees, not just claiming them

Neither side can be checked by reading logs, and for the same reason: healthy output and broken
output look identical.

"A rolled-back transaction publishes nothing" — a missing message id looks the same whether the
rollback was deliberate or the relay just silently dropped it.

"No duplicate was ever applied twice" — equally true of a working inbox and of a stream that
happened never to repeat itself. Nothing in the output tells you which one you're looking at.

So the demo turns intent into data and then holds itself to it:

- when a producer rolls back, it writes the ref to `rollback_audit` on a **separate, committed**
  connection;
- the consumer counts every arrival in `delivered` **outside** the inbox, so the duplicates the
  inbox is about to swallow stay visible;
- it applies the business effect in `order_projection` **inside** the inbox handler — a table with
  deliberately no unique constraint, so a double application shows up as a row you can find rather
  than an exception that destroys the evidence;
- and when it drops an ack on purpose, it records the ref in `ack_skipped`, so verification can
  insist that each one really did come back.

`make verify` then reduces the whole thing to SQL that must return zero rows:

```sql
-- nothing rolled back may ever have been delivered
SELECT a.ref FROM rollback_audit a JOIN delivered d ON d.ref = a.ref;

-- nothing committed may be missing from the delivered set
SELECT o.ref FROM orders o LEFT JOIN delivered d ON d.ref = o.ref WHERE d.ref IS NULL;

-- every ack dropped on purpose must actually have produced a redelivery,
-- or the two checks below are testing nothing at all
SELECT s.ref FROM ack_skipped s JOIN delivered d ON d.ref = s.ref WHERE d.deliveries < 2;

-- and no effect may have been applied twice, here or one hop downstream
SELECT ref FROM order_projection GROUP BY ref HAVING count(*) > 1;
SELECT ref FROM shipment         GROUP BY ref HAVING count(*) > 1;
```

The second query is the one that would catch a broken relay: a lost message shows up as a
committed order with no delivery. `duplicates > 0` in the summary is fine — delivery is
at-least-once by design (more on that below).

The summary makes the point in two numbers:

```
total_deliveries > distinct_delivered    the broker really did repeat itself
effects_applied  = distinct_delivered    and it changed nothing
```

`rollback_audit`, `delivered`, `order_projection`, `ack_skipped` and `shipment` are demo
instrumentation only, created by the example — not by `schema.sql` or `inbox-schema.sql`. The
libraries have no opinion about how you audit your own behaviour.

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
| At-least-once delivery | The cursor only advances after the broker acknowledges. A crash between publish and advance replays the batch. **Consumers must be idempotent** — see [The inbox](#the-inbox-the-other-half), which is the library that makes them so. |

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

## The inbox: the other half

Everything above is at-least-once **on purpose**. The cursor only advances after JetStream
acknowledges, so a crash in between replays the batch — which is the right trade, because the
alternative is losing messages. It's only a *safe* trade if something downstream absorbs the
replay, and "consumers must be idempotent" is easy to write in a README and easy to get wrong in
practice. `inbox-core` is that something.

The inbox is this same library run backwards. The outbox writes the message in the same
transaction as the business data, so a message can never exist for data that got rolled back. The
inbox writes the record of *having handled* a message in the same transaction as its effects, so
those effects can never happen twice.

```
        ┌──────────── one local transaction ────────────┐
        │  INSERT INTO inbox_message ...  (the claim)   │   atomic, no 2PC —
        │  INSERT INTO order_projection ...  (effects)  │   just an ordinary commit
        └───────────────────────────────────────────────┘
                          │ commit returns
                          ▼
                    acknowledge the broker
```

Crash before the commit and both disappear: the broker never got an ack, redelivers, and the work
happens once. Crash *after* the commit but before the ack — much the likeliest failure in the whole
system, and one no amount of care inside the handler can close — and the redelivery finds the claim
already sitting there and does nothing. There's no third case, because there's no instant at which
the effects exist without the claim, or the claim without the effects.

Worth stating plainly what this is not: exactly-once **effects**, not exactly-once **execution**.
The handler body may well run more than once. Anything it does that a rollback can't undo — sending
mail, charging a card, writing to a second datastore — is outside the transaction and outside the
guarantee. Put those in an outbox on the same connection.

### The claim is one statement

`store.InboxGuard.claim` is a single upsert doing three jobs, and it's the only place the guarantee
actually lives:

```sql
INSERT INTO inbox_message (consumer, message_id, subject, headers, status, attempts, processed_at)
VALUES (?, ?, ?, CAST(? AS jsonb), 'DONE', 1, now())
ON CONFLICT (consumer, message_id) DO UPDATE
   SET status = 'DONE', attempts = inbox_message.attempts + 1, processed_at = now()
 WHERE inbox_message.status = 'PENDING'
RETURNING attempts
```

**Deduplication.** A message already recorded conflicts on the primary key, and the `WHERE` guard
means a row that's `DONE` (handled) or `DEAD` (parked) updates nothing. Zero rows back is the signal
to skip the handler.

**Mutual exclusion between replicas.** Two replicas handed the same message race into this
statement. PostgreSQL makes the loser *wait* on the winner's uncommitted tuple rather than raising a
unique violation — and when the winner commits, the loser re-evaluates the `WHERE` clause against
the row that's now visible, sees `DONE`, and skips. If the winner rolls back instead, the row was
never there and the loser carries on with the insert. Both outcomes correct, no explicit locking, no
election. That behaviour is exactly why the inbox tests run against real PostgreSQL and not a fake:
a fake would make `racingReplicasApplyTheEffectOnce` pass for the wrong reason.

**Retry.** A message whose handler threw is left `PENDING`, so the guard matches next time round and
the attempt counter carries over.

Two smaller decisions hiding in the same statement. The key is `(consumer, message_id)` and not
`message_id` alone, because two independent handlers in one service each need their own shot at a
message — dedupe on the id by itself and the first one silently swallows it for the second. And the
row goes straight to `DONE` rather than `PENDING`-then-`DONE`, because the handler runs inside this
same transaction: the row only becomes visible to anyone else if the handler already succeeded, so a
two-phase status would be a lie about what the row means *and* cost a round trip to tell it.

### The attempt counter has to live somewhere else

A retry count incremented inside the failing transaction gets rolled back along with everything
else, so it's permanently zero and the message retries forever. `store.InboxStore.recordFailure`
writes it on a **separate committed connection** — the same trick the demo's `rollback_audit` uses on
the producing side, for exactly the same reason. After `maxAttempts` the message is parked as
`DEAD`, which both stops the retries and unblocks whatever queued up behind it.

### No leader election, and that's not an oversight

The relay needs an elector, a fencing token and a guarded cursor write. `InboxProcessor` needs none
of them, and the difference is worth a paragraph because it isn't accidental.

The relay advances **one shared watermark**, and a leader that hasn't noticed it was replaced can
corrupt it. The inbox has no watermark at all — progress is recorded per row. So a second processor
is redundant rather than dangerous: in ordered mode it blocks on a `FOR UPDATE` lock on the head of
the queue, and when the holder commits it finds that row is no longer `PENDING` and moves to the
next one. Order is preserved by the lock, not by an election.

Shared mutable position is what forces consensus. Per-row state doesn't need it.

### Two modes

| | inline (`Inbox.process`) | staging (`Inbox.stage` + `InboxProcessor`) |
|---|---|---|
| Broker acked | after the work is committed | as soon as the message is durable |
| An ack means | handled | received |
| Retries driven by | the broker, by redelivery | the database, by re-reading the row |
| Costs you | slow handlers hold the ack-wait open | a second hop, and a queue to watch |

**Inline is the default and the one to reach for**: fewer moving parts, no second queue to monitor,
and an acknowledgement that means what everyone assumes it means. Reach for staging when the handler
is slow or failure-prone enough that holding the broker's ack-wait open is a problem, or when you
want the redelivery policy living in your own database rather than in a broker consumer config.

Staging re-imposes FIFO from the table, and needs the **same** `tx_id < pg_snapshot_xmin(...)`
predicate the relay does, for the same reason: `seq` is assigned at INSERT, so with more than one
receiver a lower seq can still be in flight when a higher one commits. Delete the predicate and
`StagingProcessorTest.stagedHoleIsNotSkipped` fails.

### Ordering doesn't survive the broker by itself

Easy to assume, and wrong. Two replicas sharing a durable consumer pull different messages and
handle them concurrently, so the order the relay worked so hard for is gone by the time the handlers
run. Deduplication is unaffected — that's per-message — but ordering isn't.

So `transport.NatsMessageSource` sets `max_ack_pending = 1` whenever `InboxConfig.ordered()` is set:
JetStream won't deliver message N+1 to *anyone* until N is acknowledged. That serialises the entire
consumer group, at the cost of its throughput — the same trade the relay makes by awaiting each
publish ack. If that ceiling is too low, staging mode buys it back, because there the ordering
constraint costs a row lock instead of a network round trip.

### Retention is a correctness setting

`InboxConfig.retention()` looks like housekeeping and isn't. Delete a `DONE` row while the broker
could still redeliver its message and there's nothing left to deduplicate against, so the effects get
applied a second time — silently, and long after the deploy that shortened the setting. It has to
comfortably exceed the broker's maximum redelivery age: for JetStream that's `max_deliver × ack_wait`,
**not** the stream's duplicate window. `DEAD` rows are never swept at all; they're the dead-letter
record, and they're the one thing nobody wants garbage-collected.

### Consume one event, produce another

This is where the two patterns stop being two tools. A service that reacts to a message by emitting
another one has three things to make durable — that it handled the input, the business change, and
the outgoing event — and all three go down the same connection:

```
   ┌──────────────── one local transaction ────────────────┐
   │  INSERT INTO inbox_message ...   (claim the input)    │
   │  INSERT INTO shipment ...        (the business fact)  │
   │  INSERT INTO outbox_message ...  (the output event)   │
   └───────────────────────────────────────────────────────┘
```

`FulfilmentMain` in the example does exactly this and `ChainingTest` pins it down. Suppressing the
duplicate matters more here than it first looks: without it, an at-least-once redelivery doesn't just
repeat local work, it emits a second downstream event — and that duplicate gets amplified at every
hop behind this service.

Note that **`inbox-core` does not depend on `outbox-core`**. The inbox hands the handler an open
transaction and then stops having opinions about what else goes on it. The composition happens in
application code, which is the only layer that knows about both. `outbox-core` appears in
`inbox-core` at *test* scope only, and only to prove this works.

## Choreographed saga

`saga-example` is where the two libraries compose, and it adds **no messaging machinery of its
own**. A
multi-service order saga — payment, then inventory, then shipping, with all three compensating
paths including a two-service unwind — falls out of `inbox.process` + `outbox.enqueue` on one
connection, looped:

```java
inbox.process(message, (tx, event) -> {
    for (OutboxMessage out : step.apply(tx, event))   // change state, say what follows next
        outbox.enqueue(tx, out);
});
```

That handler is the entire implementation. It is the *Consume one event, produce another* section
above, run in a loop across four services — no orchestrator, no state machine, no saga log.

Participants run **unordered** on purpose. Choreography doesn't need message ordering, because
causality is already enforced by the data: `inventory.rejected` can't overtake `order.created`
when it doesn't exist until a step that consumed `order.created` has committed. Paying for
`max_ack_pending = 1` here would buy a guarantee the workload already has for free.

`verify-saga.sql` checks the outcome as invariants over ordinary business data, since there's no
coordinator to ask whether it worked. The sharp one is stock conservation: every effect in the
saga is naturally idempotent except `available = available - 1`, a read-modify-write a broken
inbox would apply twice with nothing else noticing.

The saga tests (`SagaStepTest`, `OrderSagaTest`) drive the wiring against real PostgreSQL with an
in-memory bus — no broker, no sleeping — including every message delivered twice and the real
`RelayEngine` on the same setup.

See [saga-example/README.md](saga-example/README.md) for the full event graph, the compensation
paths, and what's deliberately simplified.

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

`inbox-core` mirrors the shape, minus the one subpackage it doesn't need:

```
de.ebrahim.inbox
├── Inbox, InboxProcessor, InboxConsumer, InboxConfig,  ← public API
│   InboxMessage, InboxHandler, InboxResult
├── store/       InboxGuard, InboxStore, InboxSchema
└── transport/   MessageSource, NatsMessageSource
```

The missing `election/` is the point, not an omission — see **No leader election** above.

`saga-example` sits on top of both and introduces one type worth naming: `SagaParticipant`, the
`inbox.process` + `outbox.enqueue` loop, plus a `SagaStep` per event it reacts to. Everything
else in the module — the four services, the event envelope, the schema — is application code, not
library.

## Schema

| Table | Applied by | Purpose |
|---|---|---|
| `outbox_message` | `schema.sql` | Pending messages. `tx_id` is the column that makes FIFO safe. |
| `outbox_cursor` | `schema.sql` | A single watermark row plus the fencing token that guards it. |
| `outbox_lease` | `schema.sql` | Used only by `PostgresLeaseElector`; the mock elector never touches it. |
| `inbox_message` | `inbox-schema.sql` | One row per message a consumer has seen. Keyed `(consumer, message_id)`; `status` is the whole state machine. |

A cursor rather than a per-row `sent` flag: since delivery is strictly in order, one watermark is
enough, there's nothing to update per message, and retention becomes a single range delete.

The inbox goes the other way — per-row state and *no* cursor — because a consumer has to remember
each message individually to recognise it again, and because per-row progress is what lets it skip
leader election entirely.

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

**outbox-core**

| Test | What it pins down |
|---|---|
| `RollbackTest` | Rollback publishes nothing (deliberate or constraint-driven); commit publishes exactly once; auto-commit is rejected; the second-connection and savepoint anti-patterns (see **Guarantees**) are pinned as known limitations |
| `FifoOrderingTest` | The sequence hole above is not skipped; concurrent writers yield a strictly increasing stream |
| `FencingTest` | Followers idle; a stale token is rejected; a lease lost mid-batch stops publishing; the Postgres lease is exclusive; a successor resumes from the cursor after a crash, replaying only the un-advanced tail |
| `WakeupLatencyTest` | A commit wakes the relay without waiting for the poll interval |

**inbox-core**

| Test | What it pins down |
|---|---|
| `IdempotencyTest` | A redelivery applies the effect once; the crash-between-commit-and-ack window costs nothing; two replicas racing on one message yield one effect and a `DUPLICATE`, not a unique violation; consumer names are independent; auto-commit is rejected; the claim rolls back with the handler's writes |
| `FailureHandlingTest` | A failing handler commits nothing; the attempt counter survives the rollback that caused it; a poison message parks after `maxAttempts` and *stays* parked; a dead letter keeps its payload and error; retention sweeps `DONE` but never `DEAD` — and a purged row **is** reprocessed, which is why retention is a correctness setting |
| `StagingProcessorTest` | Staged messages are handled in order, exactly once each; staging is idempotent; the seq hole is not skipped; a second processor neither reorders nor repeats work *with no leader election anywhere*; a poison message unblocks the queue by parking; unordered mode keeps exactly-once while giving up FIFO |
| `ChainingTest` | Claim, business write and outbox enqueue commit as one unit; a failure after enqueueing leaves no event behind; a redelivered input does not emit the output event twice; the chained event reaches the real relay and is published once |

Two of these depend on behaviour no fake reproduces — how `ON CONFLICT DO UPDATE` re-evaluates its
`WHERE` after waiting on a concurrent inserter, and how `BIGSERIAL` interacts with snapshot
visibility. Both predicates were checked by deleting them: dropping the `tx_id` guard fails
`stagedHoleIsNotSkipped`, and dropping the `status` guard from the claim fails six tests.

**saga** — the two libraries composed, with `InMemoryBus` (a pub/sub broker in twenty lines) and
an explicit `settle()` loop instead of threads and sleeps.

| Test | What it pins down |
|---|---|
| `SagaStepTest` | State change and outgoing event commit together; a failing step leaves neither behind and stays retryable; a duplicated input emits its event once; a step returning no events ends its branch; re-*creating* an event instead of replaying it defeats dedup and produces exactly the stock drift `verify-saga.sql` CHECK 7 reports |
| `OrderSagaTest` | The happy path; all three compensating paths including the two-service cascade; every message delivered twice changes no outcome; stock and money invariants hold under contention; the real `RelayEngine` drives the same wiring |

## Known limitations and extensions

### Outbox

- **Global FIFO caps throughput** at one sequential publisher. The intended extension is a
  `partition_key` column with one cursor row per partition — FIFO *within* a key, parallelism
  across keys, and ordering guarantees stated per key rather than globally.
- **Head-of-line blocking is by design.** A message that repeatedly fails to publish stalls the
  whole queue, because skipping it would violate FIFO. Production needs a bounded retry with
  backoff and then a deliberate choice: park the message and continue, or halt and alert.
  `InboxProcessor` makes that choice explicitly — bounded retries, then park — and is the shape the
  relay wants too.
- **A long-running transaction delays the outbox** — the `xmin` trade-off above.
- **`xid8` requires PostgreSQL 13+.** Older versions need the 32-bit equivalents
  (`txid_current()`, `txid_snapshot_xmin()`), with wraparound to account for.
- **Cursor lag is the health signal.** A relay that's alive but wedged behind a poison message
  looks perfectly healthy to a liveness probe. `Inbox.pendingDepth()` is the consuming side's
  equivalent, and it rises for the same reason.

### Inbox

- **Ordered inline consumption caps throughput** at one in-flight message per consumer group — that
  is what `max_ack_pending = 1` buys you. Staging mode, or a `partition_key` extension mirroring the
  outbox's, is how to get it back.
- **A slow handler blocks a competing replica**, which sits inside `claim` waiting on the row lock
  rather than failing fast. Correct, but it makes handler latency everyone's problem: keep handlers
  short and push slow work behind an outbox.
- **Retention is a correctness setting, not housekeeping.** Purging a `DONE` row inside the broker's
  redelivery horizon quietly reintroduces the double application.
- **Exactly-once effects, not exactly-once execution.** Non-transactional side effects inside a
  handler are outside the guarantee and always will be.
- **`Nats-Msg-Id` is mandatory.** A message without it can't be deduplicated, and
  `NatsMessageSource` terminates it rather than guessing. The JetStream stream sequence is not a
  substitute: a relay failover republishes the same logical message under a new one.
- **A `DEAD` row needs a human.** Nothing retries it and nothing sweeps it. Alert on
  `count(*) FILTER (WHERE status = 'DEAD')`.

### Saga (`saga-example`)

- **No timeouts.** A saga that stalls because a participant is down stays stalled until it comes
  back. Production wants a deadline per step and an escalation path — which is the point where
  choreography starts wanting an orchestrator.
- **One database, one relay for four services**, and one event envelope for nine event types.
  Real deployment is a database and a relay each, with per-service event contracts. No code joins
  across service tables, but the isolation is a convention here, not a boundary — hence the
  `saga_` prefix on every table.
- **`available = available - 1` is the one non-idempotent effect.** The saga leans on the inbox
  to make it safe; `verify-saga.sql` CHECK 7 (stock conservation) is the invariant that would
  catch it if the inbox ever failed.
