package de.ebrahim.outbox.example;

import io.nats.client.*;
import io.nats.client.api.AckPolicy;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.DeliverPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.Duration;

/**
 * Verifies the guarantees from the outside.
 *
 * <p>Prints every received event and, crucially, asserts that the
 * {@code Outbox-Id} header never goes backwards. Because several producers are
 * committing concurrently, that assertion is the end-to-end FIFO proof: if the
 * relay's gap-free read were wrong, this consumer would either report an
 * out-of-order id or silently receive fewer messages than were committed.
 *
 * <p>A repeated id is reported as a duplicate rather than a failure. Delivery is
 * at-least-once by design, so duplicates are expected after a relay restart and
 * the consumer must be idempotent — which is what tracking the highest seen id
 * amounts to here.
 */
public final class ConsumerMain {

    private static final Logger log = LoggerFactory.getLogger(ConsumerMain.class);

    public static void main(String[] args) throws Exception {
        String natsUrl = env("NATS_URL", "nats://nats:4222");
        String stream  = env("STREAM_NAME", "ORDERS");
        String subject = env("SUBJECT", "orders.>");

        try (Connection nats = Nats.connect(new Options.Builder()
                .server(natsUrl).maxReconnects(-1).reconnectWait(Duration.ofSeconds(1)).build())) {

            waitForStream(nats, stream);

            JetStream js = nats.jetStream();
            PushSubscribeOptions options = PushSubscribeOptions.builder()
                    .stream(stream)
                    .configuration(ConsumerConfiguration.builder()
                            .durable("order-verifier")
                            .deliverPolicy(DeliverPolicy.All)
                            .ackPolicy(AckPolicy.Explicit)
                            .build())
                    .build();

            JetStreamSubscription sub = js.subscribe(subject, options);
            log.info("consumer listening on {}", subject);

            long highestSeen = 0;
            long received = 0;
            long duplicates = 0;

            while (true) {
                Message msg = sub.nextMessage(Duration.ofSeconds(5));
                if (msg == null) continue;

                String outboxId = msg.getHeaders() == null ? null : msg.getHeaders().getFirst("Outbox-Id");
                long id = outboxId == null ? -1 : Long.parseLong(outboxId);
                String body = new String(msg.getData(), StandardCharsets.UTF_8);
                received++;

                if (id <= highestSeen) {
                    if (id == highestSeen) {
                        duplicates++;
                        log.info("DUPLICATE  id={} {} (expected under at-least-once)", id, body);
                    } else {
                        // This is the failure the gap-free read exists to prevent.
                        log.error("OUT OF ORDER  id={} arrived after {} — FIFO violated", id, highestSeen);
                    }
                } else {
                    if (id > highestSeen + 1 && highestSeen > 0) {
                        // Not necessarily a bug: ids skip when a transaction rolls
                        // back, which the producer does deliberately every 10th time.
                        log.info("gap {} -> {} (expected: rolled-back transactions burn ids)", highestSeen, id);
                    }
                    highestSeen = id;
                    log.info("ok  id={} {}  [received={} duplicates={}]", id, body, received, duplicates);
                }
                msg.ack();
            }
        }
    }

    private static void waitForStream(Connection nats, String stream) throws InterruptedException {
        for (int attempt = 0; attempt < 60; attempt++) {
            try {
                nats.jetStreamManagement().getStreamInfo(stream);
                return;
            } catch (Exception e) {
                log.info("waiting for stream {} to be created by the relay...", stream);
                Thread.sleep(2000);
            }
        }
        throw new IllegalStateException("stream " + stream + " never appeared");
    }

    private static String env(String key, String fallback) {
        String value = System.getenv(key);
        return value == null || value.isBlank() ? fallback : value;
    }
}
