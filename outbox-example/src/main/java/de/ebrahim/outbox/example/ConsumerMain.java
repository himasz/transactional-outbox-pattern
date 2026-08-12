package de.ebrahim.outbox.example;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.nats.client.*;
import io.nats.client.api.AckPolicy;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.DeliverPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;

/**
 * Verifies the guarantees from the outside.
 *
 * <p>Two jobs. It asserts that the {@code Outbox-Id} header never goes
 * backwards, which is the live FIFO check, and it records every received ref in
 * the {@code delivered} table so verify.sql can assert the two things a log
 * cannot: that every committed order arrived, and that no rolled-back order
 * ever did.
 *
 * <p>The upsert in {@link #recordDelivery} is what "consumers must be
 * idempotent" looks like in practice. Duplicates are expected after a relay
 * restart and are counted, not treated as failures.
 */
public final class ConsumerMain {

    private static final Logger log = LoggerFactory.getLogger(ConsumerMain.class);

    public static void main(String[] args) throws Exception {
        String natsUrl = env("NATS_URL", "nats://nats:4222");
        String stream  = env("STREAM_NAME", "ORDERS");
        String subject = env("SUBJECT", "orders.>");

        HikariConfig hikari = new HikariConfig();
        hikari.setJdbcUrl(env("PG_URL", "jdbc:postgresql://postgres:5432/outbox"));
        hikari.setUsername(env("PG_USER", "outbox"));
        hikari.setPassword(env("PG_PASSWORD", "outbox"));
        hikari.setMaximumPoolSize(2);
        hikari.setPoolName("consumer");

        try (HikariDataSource dataSource = new HikariDataSource(hikari);
             Connection nats = Nats.connect(new Options.Builder()
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
                String ref = msg.getHeaders() == null ? null : msg.getHeaders().getFirst("Ref");
                long id = outboxId == null ? -1 : Long.parseLong(outboxId);
                String body = new String(msg.getData(), StandardCharsets.UTF_8);
                received++;

                // Persist what actually arrived. This is what makes the demo
                // falsifiable: verify.sql can then assert that every committed
                // order was delivered and that no rolled-back order ever was.
                // A log line claiming "expected" proves nothing.
                boolean firstDelivery = recordDelivery(dataSource, ref);
                if (!firstDelivery) duplicates++;

                if (id < highestSeen) {
                    // The failure the gap-free read exists to prevent. Note this is
                    // reported as an error unconditionally: a redelivery of an OLD
                    // message after a relay restart also lands here, and treating
                    // both as worth investigating is the safe default.
                    log.error("OUT OF ORDER  id={} arrived after {} — FIFO violated", id, highestSeen);
                } else {
                    // Deliberately NOT reporting id gaps as "expected". A gap is
                    // indistinguishable from a lost message by inspection, so the
                    // only honest check is the SQL one in verify.sql.
                    highestSeen = Math.max(highestSeen, id);
                    log.info("{}  id={} ref={} {}  [received={} duplicates={}]",
                            firstDelivery ? "ok       " : "duplicate", id, ref, body, received, duplicates);
                }
                msg.ack();
            }
        }
    }

    /**
     * Idempotent upsert, which is what "consumers must be idempotent" looks like
     * in practice. The counter also gives the demo a real duplicate count instead
     * of an assertion that duplicates are theoretically possible.
     *
     * @return true if this ref had never been delivered before
     */
    private static boolean recordDelivery(HikariDataSource ds, String ref) {
        if (ref == null) return true;
        try (java.sql.Connection c = ds.getConnection();
             PreparedStatement ps = c.prepareStatement("""
                     INSERT INTO delivered (ref) VALUES (?)
                     ON CONFLICT (ref) DO UPDATE SET deliveries = delivered.deliveries + 1
                     RETURNING deliveries
                     """)) {
            ps.setString(1, ref);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() && rs.getInt(1) == 1;
            }
        } catch (SQLException e) {
            log.warn("could not record delivery of {}", ref, e);
            return true;
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