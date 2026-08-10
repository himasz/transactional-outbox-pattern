package de.ebrahim.outbox;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * All relay-side database access. Read this class first: the two queries below
 * are where the FIFO and at-least-once guarantees actually live.
 */
public final class OutboxStore {

    /** One unpublished message, as read by the relay. */
    public record Row(long id, String subject, UUID messageId, Map<String, String> headers, byte[] payload) { }

    /**
     * The gap-free read.
     *
     * <p>The naive version of this query is {@code WHERE id > cursor ORDER BY id},
     * and it silently loses messages under concurrency. {@code BIGSERIAL} hands
     * out ids at INSERT time, not at COMMIT time, so a transaction holding id 42
     * can still be in flight while a transaction holding id 43 commits. A relay
     * polling in that window sees 43, publishes it, advances the cursor past it,
     * and 42 becomes invisible forever.
     *
     * <p>{@code pg_snapshot_xmin(pg_current_snapshot())} is the id of the oldest
     * transaction still running. Any row whose {@code tx_id} is strictly older
     * than that watermark was inserted by a transaction that has already
     * finished, so no lower id can subsequently appear. In the example above the
     * relay simply does not see 43 yet; it waits until 42's transaction resolves
     * and then reads both, in order.
     *
     * <p>The cost is that one long-running transaction anywhere in the database
     * stalls the outbox. That is a real operational trade-off, and the reason
     * {@code cursor lag} is the metric worth alerting on.
     */
    private static final String FETCH_BATCH_SQL = """
            SELECT id, subject, message_id, headers::text AS headers, payload
              FROM outbox_message
             WHERE id > ?
               AND tx_id < pg_snapshot_xmin(pg_current_snapshot())
             ORDER BY id
             LIMIT ?
            """;

    private static final String READ_CURSOR_SQL =
            "SELECT last_id FROM outbox_cursor WHERE name = 'default'";

    /**
     * Stamps our fencing token on the cursor the moment we believe we are
     * leader, before publishing anything. This is what evicts a zombie: once a
     * higher token is stored, every subsequent write by the old leader fails its
     * own {@code fencing_token <= ?} guard.
     */
    private static final String CLAIM_CURSOR_SQL = """
            UPDATE outbox_cursor
               SET fencing_token = ?, updated_at = now()
             WHERE name = 'default' AND fencing_token <= ?
            """;

    /**
     * Advances the watermark, conditional on still holding the newest token.
     * Zero rows updated means a newer leader has taken over and this process
     * must stop immediately rather than trusting its own view of leadership.
     */
    private static final String ADVANCE_CURSOR_SQL = """
            UPDATE outbox_cursor
               SET last_id = ?, fencing_token = ?, updated_at = now()
             WHERE name = 'default' AND fencing_token <= ?
            """;

    /** Retention. Rows behind the cursor have been published and are dead weight. */
    private static final String PURGE_SQL = """
            DELETE FROM outbox_message
             WHERE id <= (SELECT last_id FROM outbox_cursor WHERE name = 'default')
               AND created_at < now() - make_interval(secs => ?)
            """;

    private static final TypeReference<Map<String, String>> HEADER_TYPE = new TypeReference<>() { };

    private final DataSource dataSource;
    private final ObjectMapper json = new ObjectMapper();

    public OutboxStore(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public long readCursor() throws SQLException {
        try (Connection c = dataSource.getConnection();
             PreparedStatement ps = c.prepareStatement(READ_CURSOR_SQL);
             ResultSet rs = ps.executeQuery()) {
            if (!rs.next()) {
                throw new IllegalStateException(
                        "outbox_cursor row 'default' is missing; has schema.sql been applied?");
            }
            return rs.getLong(1);
        }
    }

    public List<Row> fetchBatch(long afterId, int limit) throws SQLException {
        List<Row> batch = new ArrayList<>(limit);
        try (Connection c = dataSource.getConnection();
             PreparedStatement ps = c.prepareStatement(FETCH_BATCH_SQL)) {
            ps.setLong(1, afterId);
            ps.setInt(2, limit);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    batch.add(new Row(
                            rs.getLong("id"),
                            rs.getString("subject"),
                            (UUID) rs.getObject("message_id"),
                            readHeaders(rs.getString("headers")),
                            rs.getBytes("payload")));
                }
            }
        }
        return batch;
    }

    /** @return false if a newer leader already holds the cursor */
    public boolean claimCursor(long fencingToken) throws SQLException {
        return update(CLAIM_CURSOR_SQL, fencingToken, fencingToken) == 1;
    }

    /** @return false if fenced out by a newer leader; the caller must step down */
    public boolean advanceCursor(long newLastId, long fencingToken) throws SQLException {
        return update(ADVANCE_CURSOR_SQL, newLastId, fencingToken, fencingToken) == 1;
    }

    public int purgePublished(long retainSeconds) throws SQLException {
        try (Connection c = dataSource.getConnection();
             PreparedStatement ps = c.prepareStatement(PURGE_SQL)) {
            ps.setDouble(1, retainSeconds);
            return ps.executeUpdate();
        }
    }

    private int update(String sql, long... args) throws SQLException {
        try (Connection c = dataSource.getConnection();
             PreparedStatement ps = c.prepareStatement(sql)) {
            for (int i = 0; i < args.length; i++) {
                ps.setLong(i + 1, args[i]);
            }
            return ps.executeUpdate();
        }
    }

    private Map<String, String> readHeaders(String raw) {
        try {
            return raw == null || raw.isBlank() ? Map.of() : json.readValue(raw, HEADER_TYPE);
        } catch (Exception e) {
            // A malformed header blob must not stall the FIFO queue forever.
            return Map.of();
        }
    }
}
