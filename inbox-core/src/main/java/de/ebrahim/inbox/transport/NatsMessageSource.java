package de.ebrahim.inbox.transport;

import de.ebrahim.inbox.InboxConfig;
import de.ebrahim.inbox.InboxMessage;
import io.nats.client.JetStream;
import io.nats.client.JetStreamSubscription;
import io.nats.client.Message;
import io.nats.client.PullSubscribeOptions;
import io.nats.client.api.AckPolicy;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.impl.Headers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * JetStream source, reading the deduplication key from the {@code Nats-Msg-Id}
 * header the relay sets on every published message.
 *
 * <p>A <b>pull</b> consumer, not push. Pull gives the loop back-pressure for
 * free: nothing is delivered that the consumer has not asked for, so a slow
 * handler cannot accumulate an unbounded backlog of in-flight messages, and the
 * ack-wait timer is not already running on messages sitting in a client buffer.
 *
 * <h2>Ordering across replicas</h2>
 *
 * <p>Worth being precise about, because it is easy to assume the relay's FIFO
 * survives the trip and it does not do so automatically.
 *
 * <p>Two replicas sharing a durable consumer pull different messages and handle
 * them concurrently, so the order the relay went to such trouble to produce is
 * gone by the time the handlers run. Deduplication is unaffected — that is
 * per-message — but ordering is not.
 *
 * <p>When {@link InboxConfig#ordered()} is set, this class therefore configures
 * {@code max_ack_pending = 1} on the shared durable. JetStream then refuses to
 * deliver message N+1 to anyone until N has been acknowledged, which serialises
 * the whole consumer group at the cost of its throughput. That is the same trade
 * the relay makes when it awaits each publish ack, and for the same reason.
 *
 * <p>The alternative, if that ceiling is too low, is staging mode: acknowledge
 * fast and let {@link InboxProcessor} re-impose order from the table, where the
 * ordering constraint costs a row lock instead of a network round trip.
 */
public final class NatsMessageSource implements MessageSource {

    private static final Logger log = LoggerFactory.getLogger(NatsMessageSource.class);

    public static final String MSG_ID_HEADER = "Nats-Msg-Id";

    /**
     * How long a message may be in flight before JetStream assumes the consumer
     * died. It must exceed the slowest realistic handler, including the time a
     * competing replica can spend blocked on the claim.
     */
    private static final Duration DEFAULT_ACK_WAIT = Duration.ofSeconds(30);

    /** Delay applied on {@code retryLater}, so a failing message is not re-fetched instantly. */
    private final Duration retryDelay;
    private final JetStreamSubscription subscription;

    public NatsMessageSource(JetStream jetStream, String stream, String subject, InboxConfig config)
            throws Exception {
        this(jetStream, stream, subject, config, defaultConsumerConfiguration(config), Duration.ofSeconds(5));
    }

    /** Escape hatch for tuning the JetStream consumer directly. */
    public NatsMessageSource(JetStream jetStream, String stream, String subject, InboxConfig config,
                             ConsumerConfiguration consumerConfiguration, Duration retryDelay) throws Exception {
        this.retryDelay = retryDelay;
        this.subscription = jetStream.subscribe(subject, PullSubscribeOptions.builder()
                .stream(stream)
                .durable(config.consumer())
                .configuration(consumerConfiguration)
                .build());
        log.info("inbox consumer {} pulling {} from stream {} (ordered={})",
                config.consumer(), subject, stream, config.ordered());
    }

    private static ConsumerConfiguration defaultConsumerConfiguration(InboxConfig config) {
        ConsumerConfiguration.Builder builder = ConsumerConfiguration.builder()
                .durable(config.consumer())
                .ackPolicy(AckPolicy.Explicit)
                .ackWait(DEFAULT_ACK_WAIT)
                // Unlimited broker-side redelivery, because the inbox owns the
                // decision to give up: it counts attempts in the database, where
                // the evidence survives a broker restart and can be queried. A
                // JetStream max_deliver set below maxAttempts would silently
                // discard messages the inbox still intended to retry.
                .maxDeliver(-1);
        if (config.ordered()) {
            builder.maxAckPending(1);
        }
        return builder.build();
    }

    @Override
    public Delivery next(Duration timeout) {
        List<Message> batch = subscription.fetch(1, timeout);
        if (batch.isEmpty()) return null;
        Message msg = batch.get(0);

        UUID messageId = readMessageId(msg);
        if (messageId == null) {
            // Unrecoverable, and dangerous to guess about. Without the producer's
            // own id there is no stable key across redeliveries, so handling this
            // message would mean handling it again on every redelivery. The
            // stream sequence is NOT a substitute: a relay failover republishes
            // the same logical message under a new one.
            log.error("message on {} has no {} header and cannot be deduplicated; terminating it",
                    msg.getSubject(), MSG_ID_HEADER);
            msg.term();
            return null;
        }

        InboxMessage message = new InboxMessage(
                messageId, msg.getSubject(), readHeaders(msg.getHeaders()),
                msg.getData() == null ? new byte[0] : msg.getData());
        return new JetStreamDelivery(msg, message, retryDelay);
    }

    private static UUID readMessageId(Message msg) {
        Headers headers = msg.getHeaders();
        if (headers == null) return null;
        String raw = headers.getFirst(MSG_ID_HEADER);
        if (raw == null || raw.isBlank()) return null;
        try {
            return UUID.fromString(raw);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    private static Map<String, String> readHeaders(Headers headers) {
        if (headers == null) return Map.of();
        Map<String, String> flat = new HashMap<>();
        headers.forEach((name, values) -> {
            if (!values.isEmpty()) flat.put(name, values.get(0));
        });
        return flat;
    }

    @Override
    public void close() {
        subscription.unsubscribe();
    }

    private record JetStreamDelivery(Message raw, InboxMessage message, Duration retryDelay) implements Delivery {

        @Override
        public void acknowledge() {
            raw.ack();
        }

        @Override
        public void retryLater() {
            // nak with a delay rather than a bare nak: an immediate redelivery of
            // a message whose handler just failed usually fails again, and burns
            // an attempt for nothing.
            raw.nakWithDelay(retryDelay);
        }
    }
}
