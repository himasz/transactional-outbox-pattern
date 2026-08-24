package de.ebrahim.inbox.store;

import de.ebrahim.inbox.InboxMessage;
import de.ebrahim.inbox.InboxResult;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * Every inbox query that is not the claim itself. Read {@link InboxGuard} first;
 * the claim is where the guarantee lives, and this class is the supporting cast.
 */
public final class InboxStore {

    /** A staged message waiting to be handled. Staging mode only. */
    public record Staged(long seq, UUID messageId, String subject,
                         Map<String, String> headers, byte[] payload, int attempts) {

        public InboxMessage toMessage() {
            return new InboxMessage(messageId, subject, headers, payload == null ? new byte[0] : payload);
        }
    }

    /**
     * Store-and-forward arrival. Committed on its own, before the broker is
     * acknowledged, so the message survives a consumer crash — that durability
     * is the entire justification for staging mode, and it is also the reason
     * the payload is stored here but not on the inline path.
     *
     * <p>{@code DO NOTHING} rather than {@code DO UPDATE}: a message that is
     * already staged, already handled, or parked needs nothing done to it.
     */
    private static final String STAGE_SQL = """
            INSERT INTO inbox_message (consumer, message_id, subject, headers, payload, status)
            VALUES (?, ?, ?, CAST(? AS jsonb), ?, 'PENDING')
            ON CONFLICT (consumer, message_id) DO NOTHING
            RETURNING seq
            """;

    /**
     * Takes the head of the queue, and holds it for the duration of the caller's
     * transaction.
     *
     * <p>Three things are load-bearing:
     *
     * <p><b>{@code LIMIT 1}.</b> One message per transaction, so each message's
     * effects commit with its own completion marker. Batching would be faster
     * and would mean a failure halfway through re-ran the handlers that had
     * already succeeded. The outbox makes the same trade for the same reason.
     *
     * <p><b>{@code FOR UPDATE} without {@code SKIP LOCKED} in ordered mode.</b>
     * This is how the inbox gets FIFO across replicas <em>without</em> leader
     * election, and it is the sharpest difference from the relay. The relay
     * needs an elected leader because it advances a single shared watermark that
     * a zombie could corrupt. The inbox has no watermark: progress is recorded
     * per row. So a second processor is not dangerous, merely redundant — it
     * blocks on the head of the queue, and when the holder commits, PostgreSQL
     * re-checks the row against the query's qualification, finds it is no longer
     * {@code PENDING}, and returns nothing. The processor loops and picks up the
     * next message. Order is preserved by the lock, not by an election.
     *
     * <p><b>The {@code tx_id} predicate.</b> Identical to the relay's gap-free
     * read, and needed for identical reasons whenever more than one receiver
     * stages messages: {@code seq} is assigned at INSERT, so a receiver holding
     * seq 5 can still be in flight while seq 6 commits. Without this a processor
     * would take 6, then 5, and deliver them to the handler in the wrong order —
     * having just been handed them in the right one by a relay that went to
     * considerable trouble.
     */
    private static final String NEXT_PENDING_SQL = """
            SELECT seq, message_id, subject, headers::text AS headers, payload, attempts
              FROM inbox_message
             WHERE consumer = ?
               AND status = 'PENDING'
               AND tx_id < pg_snapshot_xmin(pg_current_snapshot())
             ORDER BY seq
             LIMIT 1
             FOR UPDATE
            """;

    /**
     * The unordered variant. {@code SKIP LOCKED} steps over a row another
     * processor is holding, so N processors work on N different messages — real
     * parallelism, and no ordering guarantee whatsoever. Correct only when the
     * handler's effects genuinely commute; use it, and per-message idempotency
     * is still intact but "the order the producer sent them in" is gone.
     */
    private static final String NEXT_PENDING_UNORDERED_SQL =
            NEXT_PENDING_SQL.replace("FOR UPDATE", "FOR UPDATE SKIP LOCKED");

    private static final String MARK_DONE_SQL = """
            UPDATE inbox_message
               SET status = 'DONE', attempts = attempts + 1,
                   processed_at = now(), last_error = NULL
             WHERE consumer = ? AND message_id = ?
            """;

    /**
     * Records a failed attempt on its own committed transaction.
     *
     * <p>It has to be a separate transaction, and the reason is the same one the
     * demo's {@code rollback_audit} table exists for: the attempt failed, so the
     * handler's transaction was rolled back, taking any counter incremented
     * inside it with it. A retry count written in the failing transaction is a
     * retry count that is always zero.
     *
     * <p>The payload is stored here even though the inline path does not store
     * it on success. A dead letter with no body is not much of a dead letter,
     * and you only pay for it on the path where something is already wrong.
     *
     * <p>The {@code WHERE status = 'PENDING'} guard keeps this from resurrecting
     * a row that another replica has meanwhile completed, and from re-counting
     * attempts against one already parked.
     */
    private static final String RECORD_FAILURE_SQL = """
            INSERT INTO inbox_message
                   (consumer, message_id, subject, headers, payload, status, attempts, last_error)
            VALUES (?, ?, ?, CAST(? AS jsonb), ?,
                    CASE WHEN 1 >= ? THEN 'DEAD' ELSE 'PENDING' END, 1, ?)
            ON CONFLICT (consumer, message_id) DO UPDATE
               SET attempts   = inbox_message.attempts + 1,
                   last_error = EXCLUDED.last_error,
                   payload    = COALESCE(inbox_message.payload, EXCLUDED.payload),
                   status     = CASE WHEN inbox_message.attempts + 1 >= ? THEN 'DEAD' ELSE 'PENDING' END
             WHERE inbox_message.status = 'PENDING'
            RETURNING status, attempts
            """;

    private static final String STATUS_SQL =
            "SELECT status FROM inbox_message WHERE consumer = ? AND message_id = ?";

    /**
     * Retention.
     *
     * <p><b>DONE only.</b> {@code DEAD} rows are the dead-letter queue and are
     * never swept automatically; deleting them would destroy the record of every
     * message the service failed to handle, which is the one thing nobody wants
     * garbage-collected.
     *
     * <p>The interval is a correctness parameter. Delete a DONE row while the
     * broker could still redeliver its message and there is nothing left to
     * deduplicate against, so the effects are applied twice — silently, and long
     * after the deploy that shortened the setting. See {@link InboxConfig#retention()}.
     */
    private static final String PURGE_SQL = """
            DELETE FROM inbox_message
             WHERE consumer = ?
               AND status = 'DONE'
               AND processed_at < now() - make_interval(secs => ?)
            """;

    /**
     * The health signal, and the inbox's equivalent of the relay's cursor lag. A
     * consumer that is alive but wedged behind a poison message looks perfectly
     * healthy to a liveness probe; a rising pending depth is what tells you
     * otherwise. In ordered mode it also rises whenever the head of the queue is
     * stuck, which is precisely when you want to know.
     */
    private static final String PENDING_DEPTH_SQL =
            "SELECT count(*) FROM inbox_message WHERE consumer = ? AND status = 'PENDING'";

    private static final TypeReference<Map<String, String>> HEADER_TYPE = new TypeReference<>() { };

    private final DataSource dataSource;
    private final String consumer;
    private final ObjectMapper json = new ObjectMapper();

    public InboxStore(DataSource dataSource, String consumer) {
        this.dataSource = dataSource;
        this.consumer = consumer;
    }

    /** @return false if this message is already staged, handled, or parked */
    public boolean stage(InboxMessage message) throws SQLException {
        try (Connection c = dataSource.getConnection();
             PreparedStatement ps = c.prepareStatement(STAGE_SQL)) {
            ps.setString(1, consumer);
            ps.setObject(2, message.messageId());
            ps.setString(3, message.subject());
            ps.setString(4, json.writeValueAsString(message.headers()));
            ps.setBytes(5, message.payload());
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
        } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
            throw new SQLException("could not serialise inbox headers", e);
        }
    }

    /**
     * Locks and returns the head of the pending queue.
     *
     * @param tx      an open transaction; the row stays locked until it commits
     * @param ordered false to use SKIP LOCKED and give up FIFO for parallelism
     */
    public Optional<Staged> nextPending(Connection tx, boolean ordered) throws SQLException {
        String sql = ordered ? NEXT_PENDING_SQL : NEXT_PENDING_UNORDERED_SQL;
        try (PreparedStatement ps = tx.prepareStatement(sql)) {
            ps.setString(1, consumer);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) return Optional.empty();
                return Optional.of(new Staged(
                        rs.getLong("seq"),
                        rs.getObject("message_id", UUID.class),
                        rs.getString("subject"),
                        readHeaders(rs.getString("headers")),
                        rs.getBytes("payload"),
                        rs.getInt("attempts")));
            }
        }
    }

    /** Completes a staged message on the caller's transaction, alongside its effects. */
    public void markDone(Connection tx, UUID messageId) throws SQLException {
        try (PreparedStatement ps = tx.prepareStatement(MARK_DONE_SQL)) {
            ps.setString(1, consumer);
            ps.setObject(2, messageId);
            ps.executeUpdate();
        }
    }

    /**
     * Counts a failed attempt and decides whether the message is retried or parked.
     *
     * @return {@link InboxResult#RETRY} while attempts remain, {@link InboxResult#PARKED} once spent
     */
    public InboxResult recordFailure(InboxMessage message, int maxAttempts, Throwable cause) throws SQLException {
        try (Connection c = dataSource.getConnection();
             PreparedStatement ps = c.prepareStatement(RECORD_FAILURE_SQL)) {
            ps.setString(1, consumer);
            ps.setObject(2, message.messageId());
            ps.setString(3, message.subject());
            ps.setString(4, json.writeValueAsString(message.headers()));
            ps.setBytes(5, message.payload());
            ps.setInt(6, maxAttempts);
            ps.setString(7, describe(cause));
            ps.setInt(8, maxAttempts);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    // The row is DONE or DEAD: another replica finished it, or it
                    // was already parked. Either way this attempt must not be
                    // retried, and there is nothing left to count.
                    return InboxResult.PARKED;
                }
                return "DEAD".equals(rs.getString("status")) ? InboxResult.PARKED : InboxResult.RETRY;
            }
        } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
            throw new SQLException("could not serialise inbox headers", e);
        }
    }

    /** The stored status, or empty if this consumer has never seen the message. */
    public Optional<String> statusOf(UUID messageId) throws SQLException {
        try (Connection c = dataSource.getConnection();
             PreparedStatement ps = c.prepareStatement(STATUS_SQL)) {
            ps.setString(1, consumer);
            ps.setObject(2, messageId);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? Optional.of(rs.getString(1)) : Optional.empty();
            }
        }
    }

    public int purgeProcessed(long retainSeconds) throws SQLException {
        try (Connection c = dataSource.getConnection();
             PreparedStatement ps = c.prepareStatement(PURGE_SQL)) {
            ps.setString(1, consumer);
            ps.setDouble(2, retainSeconds);
            return ps.executeUpdate();
        }
    }

    public long pendingDepth() throws SQLException {
        try (Connection c = dataSource.getConnection();
             PreparedStatement ps = c.prepareStatement(PENDING_DEPTH_SQL)) {
            ps.setString(1, consumer);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getLong(1) : 0L;
            }
        }
    }

    private Map<String, String> readHeaders(String raw) {
        try {
            return raw == null || raw.isBlank() ? Map.of() : json.readValue(raw, HEADER_TYPE);
        } catch (Exception e) {
            // A malformed header blob must not wedge the queue behind one row.
            return Map.of();
        }
    }

    /** Bounded, because last_error is a diagnostic column and not a log sink. */
    private static String describe(Throwable cause) {
        if (cause == null) return "unknown";
        String text = cause.getClass().getName() + ": " + cause.getMessage();
        return text.length() <= 1000 ? text : text.substring(0, 1000);
    }
}
