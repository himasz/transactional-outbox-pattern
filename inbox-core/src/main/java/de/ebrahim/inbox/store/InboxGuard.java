package de.ebrahim.inbox.store;

import de.ebrahim.inbox.InboxMessage;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Claims a message on a connection supplied by the caller.
 *
 * <p>This is the whole trick of the inbox, and the exact mirror of
 * {@code OutboxWriter}: the record that a message was handled is written
 * through the <em>same</em> JDBC connection, inside the <em>same</em>
 * transaction, as the handler's business writes. One resource manager, one
 * ordinary local commit, no distributed transaction. Either the effects and the
 * "handled" marker both exist, or neither does — which is what turns the
 * outbox's at-least-once delivery into exactly-once effects.
 *
 * <p>Thread-safe and stateless; a single instance can be shared.
 */
public final class InboxGuard {

    /**
     * Deduplicate, mutually exclude, and claim, in one statement.
     *
     * <p>Reading it as three separate jobs is the clearest way in:
     *
     * <ol>
     *   <li><b>Deduplication.</b> The primary key is {@code (consumer,
     *       message_id)}, so a message already recorded conflicts. The
     *       {@code WHERE inbox_message.status = 'PENDING'} guard on the update
     *       means a row that is {@code DONE} (handled) or {@code DEAD} (parked
     *       after too many failures) updates nothing and returns nothing. Zero
     *       rows back is the caller's signal to skip the handler.</li>
     *
     *   <li><b>Mutual exclusion between replicas.</b> Two consumer replicas
     *       handed the same message race into this statement. PostgreSQL makes
     *       the loser <em>wait</em> on the winner's uncommitted tuple rather
     *       than raising a unique violation, and — this is the part worth
     *       knowing — when the winner commits, the loser re-evaluates the
     *       {@code WHERE} clause against the newly visible row. It sees
     *       {@code DONE}, updates nothing, and skips. If the winner instead
     *       rolls back, the row was never there and the loser proceeds with the
     *       insert. Both outcomes are correct with no explicit locking, and no
     *       leader election.</li>
     *
     *   <li><b>Retry of an earlier failure.</b> A message whose handler threw
     *       last time is left {@code PENDING} by
     *       {@link InboxStore#recordFailure}, so the guard matches and the
     *       update flips it to {@code DONE}. The attempt counter carries over,
     *       which is what eventually parks a poison message.</li>
     * </ol>
     *
     * <p>Written {@code DONE} immediately rather than {@code PENDING}-then-
     * {@code DONE}: the handler runs inside this same transaction, so the row
     * only ever becomes visible to anyone else if the handler already succeeded.
     * A two-phase status would be a lie about what the row means, and would cost
     * a second round trip to tell it.
     */
    private static final String CLAIM_SQL = """
            INSERT INTO inbox_message
                   (consumer, message_id, subject, headers, status, attempts, processed_at)
            VALUES (?, ?, ?, CAST(? AS jsonb), 'DONE', 1, now())
            ON CONFLICT (consumer, message_id) DO UPDATE
               SET status       = 'DONE',
                   attempts     = inbox_message.attempts + 1,
                   processed_at = now(),
                   last_error   = NULL
             WHERE inbox_message.status = 'PENDING'
            RETURNING attempts
            """;

    private final ObjectMapper json = new ObjectMapper();

    /**
     * Claims {@code message} for {@code consumer} on the caller's transaction.
     *
     * <p>A {@code true} return is permission to do the work, and an obligation
     * to do it on {@code tx}. If the caller commits, the effects and the claim
     * commit together; if the caller rolls back, both disappear and the message
     * is eligible again.
     *
     * @param tx an open connection with auto-commit disabled; NOT committed or
     *           closed by this method — the caller owns its lifecycle
     * @return true if this call won the claim and the handler should run; false
     *         if the message was already handled or is parked
     * @throws IllegalStateException if the connection is in auto-commit mode,
     *         which would make the claim durable independently of the handler's
     *         writes and so reintroduce exactly the double-application this
     *         class exists to prevent
     */
    public boolean claim(Connection tx, String consumer, InboxMessage message) throws SQLException {
        if (tx.getAutoCommit()) {
            throw new IllegalStateException(
                    "claim() must run inside an explicit transaction. "
                    + "Call connection.setAutoCommit(false) first, or use Inbox.process().");
        }
        try (PreparedStatement ps = tx.prepareStatement(CLAIM_SQL)) {
            ps.setString(1, consumer);
            ps.setObject(2, message.messageId());
            ps.setString(3, message.subject());
            ps.setString(4, json.writeValueAsString(message.headers()));
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
        } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
            throw new SQLException("could not serialise inbox headers", e);
        }
    }
}
