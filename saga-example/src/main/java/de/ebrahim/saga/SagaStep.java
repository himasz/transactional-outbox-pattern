package de.ebrahim.saga;

import de.ebrahim.outbox.OutboxMessage;

import java.sql.Connection;
import java.util.List;

/**
 * One reaction: given an event, change some state and say what happens next.
 *
 * <p>The signature is the entire saga model. A step is a function from an
 * incoming event to (a) writes on the supplied transaction and (b) the events
 * that follow — and because the returned events are enqueued on that same
 * transaction, a step either happens completely or not at all. There is no
 * moment where the state changed but the next event was lost, and none where an
 * event was emitted for a change that got rolled back.
 *
 * <p>Returning {@link List#of()} ends this branch of the saga. That is how the
 * terminal steps work, and also how a step declines an event whose precondition
 * no longer holds.
 */
@FunctionalInterface
public interface SagaStep {

    /**
     * @param tx    the claiming transaction — write business rows here and only here
     * @param event the incoming event, already correlated by {@code ref}
     * @return the events to emit, enqueued on {@code tx} by the caller
     * @throws Exception to roll the whole step back and have the event redelivered
     */
    List<OutboxMessage> apply(Connection tx, SagaEvent event) throws Exception;
}
