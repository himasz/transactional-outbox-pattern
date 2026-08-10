package de.ebrahim.outbox;

/**
 * Broker abstraction. Exists so the relay logic can be tested without a broker,
 * and so a different transport can be dropped in without touching the loop.
 */
public interface MessagePublisher extends AutoCloseable {

    /**
     * Publishes one message and returns only once the broker has durably
     * accepted it. Implementations MUST be synchronous: the relay treats a
     * successful return as permission to advance the cursor past this row.
     *
     * @throws Exception if the broker did not accept the message, in which case
     *         the relay must not advance past it
     */
    void publish(OutboxStore.Row row) throws Exception;

    @Override
    default void close() throws Exception { }
}
