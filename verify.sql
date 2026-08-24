-- End-to-end verification of the guarantees on both sides of the broker.
--
-- Run with `make verify` after the demo has been up for a minute or so. Every
-- CHECK below must return ZERO rows. If one does not, the query names which
-- guarantee failed.
--
-- This file exists because log inspection cannot prove any of it. A missing
-- message id looks exactly the same whether a transaction was rolled back on
-- purpose or the relay silently skipped it, and "I saw no duplicate effects"
-- looks exactly the same whether the inbox worked or no duplicate ever arrived.
-- Only a positive check against recorded intent can tell those apart, which is
-- why the demo writes down what it MEANT to do -- rollback_audit on the
-- producing side, ack_skipped on the consuming side -- and then holds itself to
-- it here.

\echo '=============================================================='
\echo 'PRODUCING SIDE -- the outbox'
\echo '=============================================================='

\echo ''
\echo 'CHECK 1  a rolled-back transaction must NEVER publish'
\echo 'expect: 0 rows'
SELECT a.ref AS rolled_back_but_delivered
  FROM rollback_audit a
  JOIN delivered d ON d.ref = a.ref;

\echo ''
\echo 'CHECK 2  a rolled-back order must not exist as business data'
\echo 'expect: 0 rows  (proves the rollback was real, not just unpublished)'
SELECT a.ref AS rolled_back_but_persisted
  FROM rollback_audit a
  JOIN orders o ON o.ref = a.ref;

\echo ''
\echo 'CHECK 3  every committed order must have been delivered'
\echo 'expect: 0 rows  (a non-empty result means a LOST message)'
SELECT o.ref AS committed_but_never_delivered
  FROM orders o
  LEFT JOIN delivered d ON d.ref = o.ref
 WHERE d.ref IS NULL
   AND o.created_at < now() - interval '10 seconds';

\echo ''
\echo '=============================================================='
\echo 'CONSUMING SIDE -- the inbox'
\echo '=============================================================='

\echo ''
\echo 'CHECK 4  the duplicate path was actually exercised'
\echo 'expect: 0 rows  (every deliberately unacked message WAS redelivered)'
\echo 'this check protects the three below: without a real duplicate in the'
\echo 'stream, "nothing was applied twice" would be a vacuous result'
SELECT s.ref AS ack_dropped_but_never_redelivered
  FROM ack_skipped s
  JOIN delivered d ON d.ref = s.ref
 WHERE d.deliveries < 2
   AND s.at < now() - interval '20 seconds';

\echo ''
\echo 'CHECK 5  no order effect may be applied twice'
\echo 'expect: 0 rows  (this is the inbox doing its job)'
\echo 'order_projection deliberately has NO unique constraint, so a broken'
\echo 'inbox shows up here as data rather than as an exception somewhere else'
SELECT ref, count(*) AS times_applied
  FROM order_projection
 GROUP BY ref
HAVING count(*) > 1;

\echo ''
\echo 'CHECK 6  every delivered order must have been applied exactly once'
\echo 'expect: 0 rows  (a non-empty result means a message was DROPPED, which'
\echo 'is the failure mode a too-eager deduplicator produces)'
SELECT d.ref AS delivered_but_never_applied
  FROM delivered d
  LEFT JOIN order_projection p ON p.ref = d.ref
 WHERE p.ref IS NULL
   AND d.first_seen < now() - interval '20 seconds';

\echo ''
\echo 'CHECK 7  no message may be parked as a dead letter'
\echo 'expect: 0 rows  (nothing in this demo should ever exhaust its retries)'
SELECT consumer, message_id, attempts, left(last_error, 80) AS last_error
  FROM inbox_message
 WHERE status = 'DEAD';

\echo ''
\echo '=============================================================='
\echo 'CHAINING -- consume one event, produce another, atomically'
\echo '=============================================================='

\echo ''
\echo 'CHECK 8  no shipment may be created twice'
\echo 'expect: 0 rows  (a duplicate input must not become a duplicate output)'
SELECT ref, count(*) AS times_shipped
  FROM shipment
 GROUP BY ref
HAVING count(*) > 1;

\echo ''
\echo 'CHECK 9  every shipment must have a matching outbound event'
\echo 'expect: 0 rows  (the business row and the event share one transaction,'
\echo 'so one existing without the other means the atomicity broke)'
SELECT s.ref AS shipped_but_no_event
  FROM shipment s
  LEFT JOIN outbox_message m
         ON m.subject = 'shipments.requested'
        AND m.headers ->> 'Ref' = s.ref
 WHERE m.id IS NULL
   AND s.ordered_at < now() - interval '10 seconds'
   -- Rows behind the relay cursor are purged after the retention window, so an
   -- old shipment legitimately has no surviving outbox row to match.
   AND s.ordered_at > now() - interval '30 minutes';

\echo ''
\echo 'CHECK 10  no rolled-back order may have been shipped'
\echo 'expect: 0 rows  (the guarantee has to survive the extra hop)'
SELECT a.ref AS rolled_back_but_shipped
  FROM rollback_audit a
  JOIN shipment s ON s.ref = a.ref;

\echo ''
\echo '=============================================================='
\echo 'SUMMARY'
\echo '=============================================================='
SELECT
    (SELECT count(*) FROM orders)                              AS committed_orders,
    (SELECT count(*) FROM rollback_audit)                       AS rolled_back,
    (SELECT count(*) FROM delivered)                            AS distinct_delivered,
    (SELECT coalesce(sum(deliveries), 0) FROM delivered)        AS total_deliveries,
    (SELECT coalesce(sum(deliveries - 1), 0) FROM delivered)    AS duplicate_arrivals,
    (SELECT count(*) FROM ack_skipped)                          AS acks_dropped_on_purpose,
    (SELECT count(*) FROM order_projection)                     AS effects_applied,
    (SELECT count(*) FROM shipment)                             AS shipments_created;

SELECT
    consumer,
    count(*) FILTER (WHERE status = 'DONE')    AS handled,
    count(*) FILTER (WHERE status = 'PENDING') AS pending,
    count(*) FILTER (WHERE status = 'DEAD')    AS dead,
    max(attempts)                              AS max_attempts_seen
  FROM inbox_message
 GROUP BY consumer
 ORDER BY consumer;

SELECT
    (SELECT last_id FROM outbox_cursor WHERE name = 'default') AS cursor_at,
    (SELECT coalesce(max(id), 0) FROM outbox_message)          AS highest_enqueued;

\echo ''
\echo 'The two numbers that tell the story:'
\echo ''
\echo '  total_deliveries > distinct_delivered   the broker really did repeat itself'
\echo '  effects_applied  = distinct_delivered   and it changed nothing'
\echo ''
\echo 'duplicate_arrivals > 0 is EXPECTED and correct: delivery is at-least-once.'
\echo 'Applying those duplicates would NOT be. That gap is the inbox.'
\echo ''
\echo 'acks_dropped_on_purpose should be > 0 after a minute or so. If it is 0,'
\echo 'the consumer has not yet reached its 50th committed message and CHECKS 5'
\echo 'and 8 have not been given anything to catch.'
