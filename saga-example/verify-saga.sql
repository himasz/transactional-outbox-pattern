-- Invariants for the choreographed order saga.
--
--   docker compose -f saga-example/docker-compose.saga.yml exec -T postgres \
--       psql -U outbox -d outbox -f - < saga-example/verify-saga.sql
--
-- Every CHECK must return ZERO rows. Give the demo a minute first: an order
-- still mid-saga is not a violation, so the time-sensitive checks exclude
-- anything touched in the last 30 seconds.
--
-- A saga has no coordinator to ask "did that work?", which is exactly why its
-- correctness has to be expressed as invariants over ordinary business data.
-- These queries are the closest thing to a specification this system has.

\echo '=============================================================='
\echo 'LIVENESS  every saga must finish'
\echo '=============================================================='

\echo ''
\echo 'CHECK 1  no saga may be stuck between states'
\echo 'expect: 0 rows'
\echo 'this is the failure mode with no alert: a step committed, its event was'
\echo 'lost, and the order sits half-done forever. Nothing times it out, because'
\echo 'in choreography nothing is watching the workflow as a whole.'
SELECT ref, status, cancel_reason, updated_at
  FROM saga_order
 WHERE status NOT IN ('COMPLETED', 'CANCELLED')
   AND updated_at < now() - interval '30 seconds'
 ORDER BY updated_at
 LIMIT 20;

\echo ''
\echo 'CHECK 2  no message may be parked as a dead letter'
\echo 'expect: 0 rows  (a parked event means a saga branch simply stopped)'
SELECT consumer, message_id, attempts, left(last_error, 80) AS last_error
  FROM inbox_message
 WHERE status = 'DEAD';

\echo ''
\echo '=============================================================='
\echo 'SAFETY  nothing may happen twice'
\echo '=============================================================='
\echo ''
\echo 'Worth knowing where the real detector is. payment.ref, shipment.ref and'
\echo 'stock_reservation.ref are PRIMARY KEYS -- correct domain modelling, since a'
\echo 'payment service should refuse to charge twice on its own account. So'
\echo 'CHECKS 3-5 cannot fire while those constraints stand; they are kept as'
\echo 'belt-and-braces against a future migration that relaxes one.'
\echo ''
\echo 'The check that actually catches a broken inbox is CHECK 7. Every effect in'
\echo 'this saga is naturally idempotent -- an upsert, or a status update -- with'
\echo 'exactly ONE exception: the stock decrement. "available = available - 1" is'
\echo 'a read-modify-write, and applying it twice takes two units while recording'
\echo 'one reservation. That drift is invisible to every constraint here and shows'
\echo 'up only in conservation. If the deduplication ever stops working, CHECK 7'
\echo 'is where you will find out.'

\echo ''
\echo 'CHECK 3  no customer may be charged twice'
\echo 'expect: 0 rows'
SELECT ref, count(*) AS charges FROM saga_payment GROUP BY ref HAVING count(*) > 1;

\echo ''
\echo 'CHECK 4  no parcel may be shipped twice'
\echo 'expect: 0 rows'
SELECT ref, count(*) AS shipments FROM saga_shipment GROUP BY ref HAVING count(*) > 1;

\echo ''
\echo 'CHECK 5  no stock may be reserved twice for one order'
\echo 'expect: 0 rows'
SELECT ref, count(*) AS reservations FROM saga_stock_reservation GROUP BY ref HAVING count(*) > 1;

\echo ''
\echo '=============================================================='
\echo 'COMPENSATION  every failure must unwind completely'
\echo '=============================================================='

\echo ''
\echo 'CHECK 6  money must never be held for a cancelled order,'
\echo '         and must always be held for a completed one'
\echo 'expect: 0 rows  (the visible half of compensation)'
SELECT p.ref, o.status AS order_status, p.status AS payment_status
  FROM saga_payment p
  JOIN saga_order o ON o.ref = p.ref
 WHERE (o.status = 'CANCELLED' AND p.status = 'AUTHORIZED')
    OR (o.status = 'COMPLETED' AND p.status <> 'AUTHORIZED');

\echo ''
\echo 'CHECK 7  stock must be conserved'
\echo 'expect: 0 rows'
\echo 'the INVISIBLE half, and the single most load-bearing query in this file.'
\echo 'It catches BOTH failure modes that nothing else here can see:'
\echo '  - a compensating step that emitted its event but skipped its work'
\echo '    (an unreleased reservation upsets nobody today, it just quietly'
\echo '     shrinks the shelf until an out-of-stock nobody can explain);'
\echo '  - a duplicate that got past the inbox, because the stock decrement is'
\echo '    the one non-idempotent effect in the saga.'
SELECT s.sku, s.initial, s.available,
       COALESCE((SELECT sum(r.quantity) FROM saga_stock_reservation r
                  WHERE r.sku = s.sku AND r.released = false), 0) AS held,
       s.available + COALESCE((SELECT sum(r.quantity) FROM saga_stock_reservation r
                  WHERE r.sku = s.sku AND r.released = false), 0) - s.initial AS drift
  FROM saga_stock s
 WHERE s.available + COALESCE((SELECT sum(r.quantity) FROM saga_stock_reservation r
                  WHERE r.sku = s.sku AND r.released = false), 0) <> s.initial;

\echo ''
\echo 'CHECK 8  a refused shipment must have released its stock'
\echo 'expect: 0 rows  (the two-service unwind, checked at its weakest link)'
SELECT s.ref
  FROM saga_shipment s
  JOIN saga_stock_reservation r ON r.ref = s.ref
 WHERE s.status = 'REFUSED'
   AND r.released = false
   AND s.created_at < now() - interval '30 seconds';

\echo ''
\echo 'CHECK 9  a cancelled order must never have shipped'
\echo 'expect: 0 rows'
SELECT o.ref, o.cancel_reason
  FROM saga_order o
  JOIN saga_shipment s ON s.ref = o.ref
 WHERE o.status = 'CANCELLED' AND s.status = 'DISPATCHED';

\echo ''
\echo 'CHECK 10  a completed order must actually have shipped'
\echo 'expect: 0 rows'
SELECT o.ref
  FROM saga_order o
  LEFT JOIN saga_shipment s ON s.ref = o.ref AND s.status = 'DISPATCHED'
 WHERE o.status = 'COMPLETED' AND s.ref IS NULL;

\echo ''
\echo '=============================================================='
\echo 'NON-VACUITY  the failure paths must actually have run'
\echo '=============================================================='
\echo ''
\echo 'Every check above passes trivially on a system where nothing ever failed.'
\echo 'These three counts must all be > 0 after a minute or so, or the demo has'
\echo 'proved only that the happy path works.'
SELECT
    (SELECT count(*) FROM saga_payment WHERE status = 'DECLINED')          AS payments_declined,
    (SELECT count(*) FROM saga_order
      WHERE cancel_reason LIKE 'out of stock%')                       AS out_of_stock_compensations,
    (SELECT count(*) FROM saga_shipment WHERE status = 'REFUSED')          AS shipping_refusals,
    (SELECT count(*) FROM saga_stock_reservation WHERE released)           AS stock_releases;

\echo ''
\echo '=============================================================='
\echo 'SUMMARY'
\echo '=============================================================='
SELECT status, count(*) FROM saga_order GROUP BY status ORDER BY status;

SELECT coalesce(cancel_reason, '(completed or in flight)') AS outcome, count(*)
  FROM saga_order GROUP BY 1 ORDER BY 2 DESC;

SELECT sku, initial, available,
       COALESCE((SELECT sum(r.quantity) FROM saga_stock_reservation r
                  WHERE r.sku = s.sku AND r.released = false), 0) AS currently_held
  FROM saga_stock s ORDER BY sku;

SELECT consumer,
       count(*) FILTER (WHERE status = 'DONE')    AS handled,
       count(*) FILTER (WHERE status = 'PENDING') AS pending,
       count(*) FILTER (WHERE status = 'DEAD')    AS dead
  FROM inbox_message GROUP BY consumer ORDER BY consumer;

\echo ''
\echo 'Note on duplicates: this demo does not force redeliveries, so a clean run'
\echo 'may see none. Immunity to them is pinned deterministically instead, by the'
\echo 'test suite -- InMemoryBus.withDuplicates() delivers EVERY message twice to'
\echo 'EVERY participant, and the same invariants above are asserted after.'
