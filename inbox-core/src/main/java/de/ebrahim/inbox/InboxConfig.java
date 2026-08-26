package de.ebrahim.inbox;

import java.time.Duration;
import java.util.Objects;

/**
 * Inbox tuning.
 *
 * @param consumer     the logical consumer name, stored on every row and part of
 *                     the deduplication key. Name it after the <em>handler</em>,
 *                     not the process: two replicas of one service must share a
 *                     name so they deduplicate against each other, while two
 *                     different handlers in one service must not, or the first
 *                     to see a message silently swallows it for the second
 * @param maxAttempts  how many times a failing message is retried before it is
 *                     parked as DEAD. Retrying forever is not a safe default:
 *                     under FIFO a poison message blocks everything behind it
 * @param ordered      whether processing preserves the producer's FIFO order.
 *                     True takes a lock on the head of the queue, so replicas
 *                     serialise; false uses SKIP LOCKED and trades ordering for
 *                     throughput. Only meaningful in staging mode
 * @param pollInterval how long the staging processor waits before looking again
 *                     when the queue is empty
 * @param errorBackoff pause after an unexpected failure, so a database or broker
 *                     outage does not become a hot loop. In ordered staging mode
 *                     it is also the retry delay for the message at the head of
 *                     the queue, which nothing else can move past
 * @param purgeInterval how often the retention sweep runs
 * @param retention    how long DONE rows are kept before the retention sweep
 *                     deletes them. <b>This is a correctness setting, not a
 *                     housekeeping one.</b> A row deleted while the broker could
 *                     still redeliver its message leaves nothing to deduplicate
 *                     against, and the effects are applied a second time. It
 *                     must comfortably exceed the broker's maximum redelivery
 *                     age — for JetStream, the consumer's {@code max_deliver}
 *                     multiplied by its {@code ack_wait}, not the stream's
 *                     duplicate window
 */
public record InboxConfig(
        String consumer,
        int maxAttempts,
        boolean ordered,
        Duration pollInterval,
        Duration errorBackoff,
        Duration purgeInterval,
        Duration retention) {

    public InboxConfig {
        Objects.requireNonNull(consumer, "consumer");
        if (consumer.isBlank()) throw new IllegalArgumentException("consumer must not be blank");
        if (maxAttempts < 1) throw new IllegalArgumentException("maxAttempts must be at least 1");
    }

    /** Ordered, five attempts, seven days of deduplication history. */
    public static InboxConfig forConsumer(String consumer) {
        return new InboxConfig(
                consumer,
                5,
                true,
                Duration.ofMillis(250),
                Duration.ofSeconds(1),
                Duration.ofMinutes(10),
                Duration.ofDays(7));
    }

    public InboxConfig withMaxAttempts(int attempts) {
        return new InboxConfig(consumer, attempts, ordered, pollInterval, errorBackoff, purgeInterval, retention);
    }

    public InboxConfig withOrdered(boolean value) {
        return new InboxConfig(consumer, maxAttempts, value, pollInterval, errorBackoff, purgeInterval, retention);
    }

    public InboxConfig withPollInterval(Duration interval) {
        return new InboxConfig(consumer, maxAttempts, ordered, interval, errorBackoff, purgeInterval, retention);
    }

    public InboxConfig withErrorBackoff(Duration value) {
        return new InboxConfig(consumer, maxAttempts, ordered, pollInterval, value, purgeInterval, retention);
    }

    public InboxConfig withRetention(Duration value) {
        return new InboxConfig(consumer, maxAttempts, ordered, pollInterval, errorBackoff, purgeInterval, value);
    }
}
