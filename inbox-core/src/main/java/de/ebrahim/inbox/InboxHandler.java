package de.ebrahim.inbox;

import java.sql.Connection;

/**
 * The application's message handler.
 *
 * <p>The signature is the whole design. The handler is given the <em>same</em>
 * open JDBC connection on which the inbox has just claimed the message, so
 * every business write it makes commits atomically with the record that the
 * message was handled. There is one resource manager in the transaction, so
 * there is no distributed transaction to fail — the identical argument the
 * outbox makes on the producing side, run backwards.
 *
 * <p>Three rules follow from that, and breaking any of them breaks the
 * guarantee:
 * <ul>
 *   <li><b>Use the supplied connection.</b> Writes made on a second connection
 *       are a second transaction and commit independently, so a later failure
 *       leaves effects behind with no record that the message was handled — and
 *       the redelivery then applies them twice.</li>
 *   <li><b>Do not commit, roll back, or close it.</b> The inbox owns the
 *       transaction boundary.</li>
 *   <li><b>Throw to reject.</b> An exception rolls back the claim along with
 *       every effect, and the message is retried. Returning normally is a
 *       promise that the work is done.</li>
 * </ul>
 *
 * <p>Non-transactional side effects — sending mail, calling a payment API,
 * writing to another datastore — cannot be rolled back and therefore cannot be
 * made exactly-once by this or any other mechanism. Enqueue them into an outbox
 * on the same connection instead; that is what
 * {@code ChainingTest} demonstrates and what makes the two patterns compose.
 */
@FunctionalInterface
public interface InboxHandler {

    /**
     * @param tx      the claiming transaction; write business rows here
     * @param message the message to apply, guaranteed to be handled at most once
     * @throws Exception to roll back the claim and every effect, and be retried
     */
    void handle(Connection tx, InboxMessage message) throws Exception;
}
