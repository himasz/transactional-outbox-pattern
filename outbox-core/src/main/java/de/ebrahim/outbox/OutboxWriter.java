package de.ebrahim.outbox;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Writes outbox rows using a connection supplied by the caller.
 *
 * <p>This is the whole trick of the pattern, and the reason no distributed
 * transaction is needed: the message is inserted through the <em>same</em>
 * JDBC connection, inside the <em>same</em> transaction, as the business
 * writes. There is exactly one resource manager involved, so atomicity is an
 * ordinary local commit. If the transaction rolls back, the outbox row rolls
 * back with it and no message can ever be published.
 *
 * <p>Thread-safe and stateless; a single instance can be shared.
 */
public final class OutboxWriter {

    private static final String INSERT_SQL = """
            INSERT INTO outbox_message (subject, message_id, headers, payload)
            VALUES (?, ?, CAST(? AS jsonb), ?)
            """;

    private final ObjectMapper json = new ObjectMapper();

    /**
     * Enqueues a message on the caller's transaction.
     *
     * @param tx an open connection with auto-commit disabled; NOT closed or
     *           committed by this method — the caller owns its lifecycle
     * @throws IllegalStateException if the connection is in auto-commit mode,
     *         which would defeat the entire pattern by making the insert durable
     *         independently of the business writes
     */
    public void enqueue(Connection tx, OutboxMessage message) throws SQLException {
        if (tx.getAutoCommit()) {
            throw new IllegalStateException(
                    "enqueue() must run inside an explicit transaction. "
                    + "Call connection.setAutoCommit(false) first, or use Outbox.inTransaction().");
        }
        try (PreparedStatement ps = tx.prepareStatement(INSERT_SQL)) {
            ps.setString(1, message.subject());
            ps.setObject(2, message.messageId());
            ps.setString(3, json.writeValueAsString(message.headers()));
            ps.setBytes(4, message.payload());
            ps.executeUpdate();
        } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
            throw new SQLException("could not serialise outbox headers", e);
        }
    }
}
