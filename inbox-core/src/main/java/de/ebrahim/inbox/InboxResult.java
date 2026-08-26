package de.ebrahim.inbox;

/**
 * What the inbox did with a message. Returned rather than thrown, because a
 * duplicate is a completely ordinary outcome under at-least-once delivery and
 * treating it as an error would drown the logs of a healthy system.
 */
public enum InboxResult {

    /** The handler ran and its effects are committed. Acknowledge the broker. */
    PROCESSED,

    /** Already handled. Do nothing and acknowledge — this is the pattern working. */
    DUPLICATE,

    /** The handler failed and nothing was committed. Negatively acknowledge so the broker redelivers. */
    RETRY,

    /**
     * The handler has now failed {@code maxAttempts} times and the message is
     * parked as DEAD. Acknowledge, because redelivering forever would block the
     * queue behind a message that has already proven it cannot be handled.
     */
    PARKED;

    /**
     * Whether the broker should be told the message is finished with. True for
     * everything except {@link #RETRY} — including {@link #PARKED}, where the
     * message is finished as far as the broker is concerned and now lives in the
     * inbox as a dead letter for a human to deal with.
     */
    public boolean shouldAcknowledge() {
        return this != RETRY;
    }
}
