# Choreographed saga

An order saga built on `outbox-core` and `inbox-core`. It adds **no messaging
machinery of its own** — that is the claim this module exists to make.

Self-contained: its own schema, its own compose file, its own verification SQL.
Nothing outside this directory changes except one `<module>` line in the root
`pom.xml`.

## The whole framework

```java
inbox.process(message, (tx, msg) -> {
    List<OutboxMessage> next = step.apply(tx, event);   // change state
    for (OutboxMessage out : next) outbox.enqueue(tx, out);   // say what follows
});
```

That is `SagaParticipant.handle`, and it is the entire saga implementation. Once
you have an inbox and an outbox, choreography needs a loop and nothing else —
no coordinator, no state machine, no workflow definition, no saga log. If you
came here expecting a framework, its absence is the result.

The single load-bearing detail is that the handler's writes and its outgoing
events go through **the same connection the inbox used to claim the input**.
Everything below follows from that.

| Without one transaction per step | With it |
|---|---|
| State changed, next event lost → the saga stalls forever, silently | Impossible |
| Event emitted, state change rolled back → downstream acts on a fiction | Impossible |
| Redelivery re-runs the step → double charge, double shipment | Suppressed by the claim |

## The saga

```
FORWARD
  order.created ──► payment ──► payment.authorized ──► inventory
  inventory ──► inventory.reserved ──► shipping
  shipping ──► order.shipped ──► order                          [COMPLETED]

COMPENSATION — three entry points, converging
  payment declines
    └─► payment.declined ───────────────────────────► order     [CANCELLED]

  stock runs out (payment already held)
    └─► inventory.rejected ──► payment refunds
           └─► payment.refunded ──────────────────► order       [CANCELLED]

  shipping refuses (payment held AND stock reserved)
    └─► shipping.failed ──► inventory releases stock
           └─► inventory.released ──► payment refunds
                  └─► payment.refunded ───────────► order       [CANCELLED]
```

The third path is the one worth having. A saga whose compensations are all
single-step never has to unwind two services in the right order, and that
cascade is where real ones leak.

Note that `payment.refunded` is reached from two different failures and
`order.cancelled` from three. Steps therefore have to be written as reactions to
a **state**, not as positions in a script — which is the practical difference
between choreography and orchestration. `PaymentService.refund` has no idea which
failure sent it there and does not need to.

## Three things worth reading the code for

**No FIFO, on purpose.** Participants run with `ordered = false`. A choreographed
saga does not need message ordering: causality is enforced by the data.
`inventory.rejected` cannot overtake `order.created`, because it does not exist
until a step that consumed `order.created` has committed. Paying for global
ordering here — `max_ack_pending = 1` across the whole consumer group — would buy
a guarantee the workload already has for free.

**Event-carried state transfer.** `payment.authorized` forwards the sku, quantity
and destination it received, even though payment cares about none of them. The
alternative is inventory and shipping reading the order service's table, which is
how a set of services quietly becomes a distributed monolith.

**The failures are real.** `SKU-SCARCE` is seeded with twelve units and runs out;
the rejection comes from a conditional `UPDATE ... WHERE available >= ?` that
stops matching. A modulus-based "every Nth order fails" would prove the
compensating path runs, but not that it runs under the conditions that cause it.

## Proving it

`verify-saga.sql` reduces the saga to invariants over ordinary business data —
which is the only thing to check against, since there is no coordinator to ask
whether it worked. Every CHECK must return zero rows.

The sharpest one is **CHECK 7, stock conservation**, and it is worth
understanding why:

> Every effect in this saga is naturally idempotent — an upsert, or a status
> update — with exactly one exception. `available = available - 1` is a
> read-modify-write. Apply it twice and two units leave the shelf against one
> reservation, and no constraint anywhere notices. Conservation is the only thing
> that does.

So CHECK 7 is where a broken inbox would surface, and it is also where a
compensating step that emitted its event but skipped its work would surface. Both
failures are invisible to everything else: nobody complains about stock that was
never released, they complain weeks later about an out-of-stock nobody can
explain.

`CHECKS 3–5` (nothing charged or shipped twice) cannot fire while
`saga_payment.ref` and `saga_shipment.ref` are primary keys — correct domain modelling, since a payment
service should refuse a double charge on its own account. They are kept as
belt-and-braces, and the file says so rather than implying they are doing work.

There is also a **non-vacuity** block. Every invariant above passes trivially on
a system where nothing ever failed, so the declined / out-of-stock / refused
counts must all be greater than zero, or the run proved only that the happy path
works.

## Running it

```bash
docker compose -f saga-example/docker-compose.saga.yml up -d --build
docker compose -f saga-example/docker-compose.saga.yml logs -f order payment inventory shipping
docker compose -f saga-example/docker-compose.saga.yml exec -T postgres \
    psql -U outbox -d outbox -f - < saga-example/verify-saga.sql
docker compose -f saga-example/docker-compose.saga.yml down -v
```

It runs its own PostgreSQL, NATS and relay on separate ports, so it does not
collide with the outbox demo. Four containers, one per participant — nothing
connects them but the event stream. `PARTICIPANT=all` runs the four in one JVM
for local work; that is a packaging choice, not a different architecture, since
they still share nothing but the broker.

## Tests

Thirteen, against real PostgreSQL, with no broker and no sleeping —
`InMemoryBus` is a pub/sub broker in twenty lines and `Choreography.settle()`
drives every round explicitly. A saga test that starts four threads and sleeps is
a test that fails once a fortnight on somebody else's laptop.

| Test | What it pins down |
|---|---|
| `SagaStepTest` | State change and outgoing event commit together; a failing step leaves neither behind and stays retryable; a duplicated input emits the outgoing event once; a step returning no events ends its branch |
| `SagaStepTest` (contrast pair) | Replaying the same message id costs nothing, but **re-creating** an event instead of replaying it defeats deduplication and produces exactly the stock drift CHECK 7 reports — the one duplicate an inbox cannot catch |
| `OrderSagaTest` | The happy path; all three compensating paths including the two-service cascade; every message delivered twice changes no outcome; stock and money invariants hold under contention; the real `RelayEngine` drives the same wiring |

`InMemoryBus.withDuplicates()` delivers **every** message twice to **every**
participant. At-least-once is what the relay guarantees, and turning it up to
"always" is the cheapest way to show the saga does not care — a broker that
happened to behave during a test run would prove nothing about the day it does
not.

## Deliberately simplified

Named rather than hidden, since a demo that pretends to be production teaches the
wrong things.

- **One database for five services.** Real deployment means five databases and
  five relays, one each. No code here joins across service tables — the schema
  comment says which tables belong to whom — but the isolation is a convention
  here, not a boundary. Every table is `saga_`-prefixed for the same reason:
  sharing a database means sharing a namespace, and `payment` and `shipment` are
  precisely the names another module will also want.
- **One event envelope for nine event types.** Real events are separate contracts
  with separate schemas; a shared envelope couples every service to every field.
- **One wildcard subscription per service**, dispatched by subject in-process.
  Per-subject filters or a consumer per subject would be the production shape;
  the wildcard costs an acknowledge for events a service does not handle.
- **No timeouts.** A saga that stalls because a participant is down stays stalled
  until it returns. Production wants a deadline per step and a way to escalate —
  which is where choreography starts wanting an orchestrator, and the honest
  place to draw the line between the two.
