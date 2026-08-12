-- End-to-end verification of the two guarantees that matter.
--
-- Run with `make verify` after the demo has been up for a minute or so. Every
-- query below must return ZERO rows. If one does not, the pattern is broken and
-- the query names which guarantee failed.
--
-- This exists because log inspection cannot prove the rollback requirement: a
-- missing message id looks exactly the same whether a transaction was rolled
-- back on purpose or the relay silently skipped it. Only a positive check
-- against recorded intent can tell the two apart.

\echo '=============================================================='
\echo 'CHECK 1  a rolled-back transaction must NEVER publish'
\echo 'expect: 0 rows'
\echo '=============================================================='
SELECT a.ref AS rolled_back_but_delivered
  FROM rollback_audit a
  JOIN delivered d ON d.ref = a.ref;

\echo '=============================================================='
\echo 'CHECK 2  a rolled-back order must not exist as business data'
\echo 'expect: 0 rows  (proves the rollback was real, not just unpublished)'
\echo '=============================================================='
SELECT a.ref AS rolled_back_but_persisted
  FROM rollback_audit a
  JOIN orders o ON o.ref = a.ref;

\echo '=============================================================='
\echo 'CHECK 3  every committed order must have been delivered'
\echo 'expect: 0 rows  (a non-empty result means a LOST message)'
\echo 'note: allow a few seconds of relay lag before trusting this'
\echo '=============================================================='
SELECT o.ref AS committed_but_never_delivered
  FROM orders o
  LEFT JOIN delivered d ON d.ref = o.ref
 WHERE d.ref IS NULL
   AND o.created_at < now() - interval '10 seconds';

\echo '=============================================================='
\echo 'SUMMARY  counts and relay lag'
\echo '=============================================================='
SELECT
    (SELECT count(*) FROM orders)                              AS committed_orders,
    (SELECT count(*) FROM rollback_audit)                      AS rolled_back,
    (SELECT count(*) FROM delivered)                           AS distinct_delivered,
    (SELECT coalesce(sum(deliveries), 0) FROM delivered)       AS total_deliveries,
    (SELECT coalesce(sum(deliveries - 1), 0) FROM delivered)   AS duplicates,
    (SELECT last_id FROM outbox_cursor WHERE name = 'default') AS cursor_at,
    (SELECT coalesce(max(id), 0) FROM outbox_message)          AS highest_enqueued;

\echo ''
\echo 'duplicates > 0 is EXPECTED and correct: delivery is at-least-once.'
\echo 'committed_orders should equal distinct_delivered once the relay catches up.'