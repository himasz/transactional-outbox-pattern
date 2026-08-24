package de.ebrahim.inbox.transport;

import de.ebrahim.inbox.InboxMessage;
import java.time.Duration;

/**
 * Broker abstraction for the consuming side, mirroring {@code MessagePublisher}
 * on the producing side. Exists so the inbox logic can be tested without a
 * broker, and so a different transport can be dropped in without touching the
 * loop.
 */
public interface MessageSource extends AutoCloseable {

    /**
     * Waits for the next message.
     *
     * @return the delivery, or {@code null} if nothing arrived within {@code timeout}
     */
    Delivery next(Duration timeout) throws Exception;

    @Override
    default void close() throws Exception { }

    /**
     * One received message plus the two things a consumer can tell the broker
     * about it.
     *
     * <p>There is no third option on purpose. "Acknowledge but remember it
     * failed" is what the {@code DEAD} status is for, and it belongs in the
     * database rather than in the broker's redelivery state, where it would be
     * invisible to the SQL that has to prove the system is behaving.
     */
    interface Delivery {

        InboxMessage message();

        /** Done with it — handled, duplicate, or parked. Do not send it again. */
        void acknowledge() throws Exception;

        /** Not handled. Send it again after a delay. */
        void retryLater() throws Exception;
    }
}
