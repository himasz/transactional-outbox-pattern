package de.ebrahim.outbox.example;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import de.ebrahim.inbox.Inbox;
import de.ebrahim.inbox.InboxConfig;
import de.ebrahim.inbox.InboxMessage;
import de.ebrahim.inbox.InboxResult;
import de.ebrahim.inbox.store.InboxSchema;
import de.ebrahim.inbox.transport.MessageSource;
import de.ebrahim.inbox.transport.NatsMessageSource;
import io.nats.client.Nats;
import io.nats.client.Options;
import io.nats.client.api.AckPolicy;
import io.nats.client.api.ConsumerConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;

/**
 * Verifies both halves of the contract from the outside.
 *
 * <p>The producing side is at-least-once <em>on purpose</em>. This consumer is
 * where that becomes exactly-once, and the demo is built so the difference is
 * visible in SQL rather than asserted in a log line:
 *
 * <ul>
 *   <li>every arrival is counted in {@code delivered}, <b>outside</b> the inbox.
 *       {@code total_deliveries > distinct_delivered} is the evidence that
 *       duplicates really do reach this process;</li>
 *   <li>the business effect is written in {@code order_projection}, <b>inside</b>
 *       the inbox handler. That table deliberately has no unique constraint, so
 *       a broken inbox produces a second row rather than an error — and
 *       {@code verify.sql} catches it. A constraint would turn the bug into a
 *       crash, which is easier to notice and therefore proves less.</li>
 * </ul>
 *
 * <h2>The deliberate ack drop</h2>
 *
 * <p>The producer rolls back every tenth transaction on purpose so the rollback
 * guarantee can be checked rather than assumed. This consumer does the mirror
 * image: every {@value #SKIP_ACK_EVERY}th message is handled and committed and
 * then <b>not acknowledged</b>, which is precisely a crash in the window between
 * commit and ack. JetStream redelivers it once {@code ack_wait} expires, and the
 * inbox must absorb it.
 *
 * <p>Without that, "no duplicate effects" would be an uninteresting result:
 * it would mean the inbox worked, or it would mean no duplicate ever arrived,
 * and nothing in the output would say which. The skipped refs are recorded in
 * {@code ack_skipped} so verify.sql can insist that each one really was
 * redelivered.
 */
public final class ConsumerMain {

    private static final Logger log = LoggerFactory.getLogger(ConsumerMain.class);

    /** Every Nth successfully committed message has its acknowledgement dropped on purpose. */
    private static final int SKIP_ACK_EVERY = 50;

    /**
     * Short for the demo. With {@code max_ack_pending = 1} the whole consumer
     * waits this long each time an ack is dropped, so a production value of
     * thirty seconds would stall the stream visibly.
     */
    private static final Duration ACK_WAIT = Duration.ofSeconds(2);

    public static void main(String[] args) throws Exception {
        String natsUrl  = env("NATS_URL", "nats://nats:4222");
        String stream   = env("STREAM_NAME", "ORDERS");
        String subject  = env("SUBJECT", "orders.>");
        String consumer = env("CONSUMER_NAME", "order-projector");

        HikariConfig hikari = new HikariConfig();
        hikari.setJdbcUrl(env("PG_URL", "jdbc:postgresql://postgres:5432/outbox"));
        hikari.setUsername(env("PG_USER", "outbox"));
        hikari.setPassword(env("PG_PASSWORD", "outbox"));
        hikari.setMaximumPoolSize(4);
        hikari.setPoolName("consumer");

        try (HikariDataSource dataSource = new HikariDataSource(hikari);
             io.nats.client.Connection nats = Nats.connect(new Options.Builder()
                     .server(natsUrl).maxReconnects(-1).reconnectWait(Duration.ofSeconds(1)).build())) {

            // The consuming service owns its own inbox table, exactly as the
            // producing service owns its outbox table.
            InboxSchema.apply(dataSource);
            createDemoTables(dataSource);
            waitForStream(nats, stream);

            InboxConfig config = InboxConfig.forConsumer(consumer).withMaxAttempts(5);
            Inbox inbox = new Inbox(dataSource, config);

            try (MessageSource source = new NatsMessageSource(nats.jetStream(), stream, subject, config,
                    ConsumerConfiguration.builder()
                            .durable(consumer)
                            .ackPolicy(AckPolicy.Explicit)
                            .ackWait(ACK_WAIT)
                            .maxDeliver(-1)
                            // Ordered: nothing is delivered until the previous
                            // message is acknowledged, which is what preserves the
                            // relay's FIFO all the way to the handler. It also
                            // means a dropped ack stalls the consumer for one
                            // ack_wait, which is why ACK_WAIT is short here.
                            .maxAckPending(1)
                            .build(),
                    Duration.ofSeconds(1))) {

                log.info("inbox consumer {} listening on {}", consumer, subject);
                run(dataSource, inbox, source);
            }
        }
    }

    private static void run(DataSource dataSource, Inbox inbox, MessageSource source) throws Exception {
        long highestSeen = 0;
        long received = 0;
        long duplicates = 0;
        long committed = 0;

        while (true) {
            MessageSource.Delivery delivery = source.next(Duration.ofSeconds(5));
            if (delivery == null) continue;

            InboxMessage message = delivery.message();
            String ref = message.header("Ref");
            String outboxId = message.header("Outbox-Id");
            long id = outboxId == null ? -1 : Long.parseLong(outboxId);
            received++;

            // Counted before the inbox sees it, so this number includes the
            // duplicates the inbox is about to suppress. That is the whole point
            // of recording it here.
            recordArrival(dataSource, ref);

            if (id < highestSeen) {
                // The failure the relay's gap-free read exists to prevent. A
                // redelivery of an OLD message after a relay failover also lands
                // here, and treating both as worth investigating is the safe
                // default.
                log.error("OUT OF ORDER  id={} arrived after {} — FIFO violated", id, highestSeen);
            }
            highestSeen = Math.max(highestSeen, id);

            InboxResult result = inbox.process(message, (tx, msg) -> {
                // The business effect. One row per order, forever, no matter how
                // many times the message arrives.
                try (PreparedStatement ps = tx.prepareStatement(
                        "INSERT INTO order_projection (ref, outbox_id) VALUES (?, ?)")) {
                    ps.setString(1, msg.header("Ref"));
                    ps.setString(2, msg.header("Outbox-Id"));
                    ps.executeUpdate();
                }
            });

            if (result == InboxResult.PROCESSED) committed++;
            if (result == InboxResult.DUPLICATE) duplicates++;

            if (result == InboxResult.PROCESSED && committed % SKIP_ACK_EVERY == 0) {
                // Simulate a crash in the window between commit and ack. The work
                // is durable; the broker does not know that yet, and will send the
                // message again. Recorded so verify.sql can prove the redelivery
                // actually happened rather than assuming it.
                recordSkippedAck(dataSource, ref);
                log.info("dropping ack for {} on purpose: the effect is committed, "
                        + "so the redelivery must change nothing", ref);
                continue;
            }

            if (result.shouldAcknowledge()) {
                delivery.acknowledge();
            } else {
                delivery.retryLater();
            }

            log.info("{}  id={} ref={} [received={} applied={} suppressed={}]",
                    label(result), id, ref, received, committed, duplicates);
        }
    }

    private static String label(InboxResult result) {
        return switch (result) {
            case PROCESSED -> "applied   ";
            case DUPLICATE -> "suppressed";
            case RETRY     -> "retrying  ";
            case PARKED    -> "PARKED    ";
        };
    }

    /**
     * Raw arrival count, deliberately not idempotent and deliberately not part of
     * the inbox transaction. It is the control group: without it, "one row per
     * order" would not distinguish a working inbox from a stream that happened
     * never to repeat itself.
     */
    private static void recordArrival(DataSource ds, String ref) {
        if (ref == null) return;
        try (java.sql.Connection c = ds.getConnection();
             PreparedStatement ps = c.prepareStatement("""
                     INSERT INTO delivered (ref) VALUES (?)
                     ON CONFLICT (ref) DO UPDATE SET deliveries = delivered.deliveries + 1
                     """)) {
            ps.setString(1, ref);
            ps.executeUpdate();
        } catch (SQLException e) {
            log.warn("could not record arrival of {}", ref, e);
        }
    }

    private static void recordSkippedAck(DataSource ds, String ref) {
        if (ref == null) return;
        try (java.sql.Connection c = ds.getConnection();
             PreparedStatement ps = c.prepareStatement(
                     "INSERT INTO ack_skipped (ref) VALUES (?) ON CONFLICT DO NOTHING")) {
            ps.setString(1, ref);
            ps.executeUpdate();
        } catch (SQLException e) {
            log.warn("could not record skipped ack for {}", ref, e);
        }
    }

    /**
     * Demo instrumentation, not library schema. Note the absence of a unique
     * constraint on {@code order_projection.ref}: a double application must show
     * up as data for verify.sql to find, not as an exception that hides the
     * evidence.
     */
    private static void createDemoTables(DataSource ds) throws SQLException {
        try (java.sql.Connection c = ds.getConnection(); Statement st = c.createStatement()) {
            st.execute("SELECT pg_advisory_xact_lock(hashtext('de.ebrahim.outbox.example.consumer'));"
                    + "CREATE TABLE IF NOT EXISTS delivered ("
                    + " ref TEXT PRIMARY KEY, deliveries INT NOT NULL DEFAULT 1,"
                    + " first_seen TIMESTAMPTZ DEFAULT now());"
                    + "CREATE TABLE IF NOT EXISTS order_projection ("
                    + " ref TEXT NOT NULL, outbox_id TEXT, applied_at TIMESTAMPTZ DEFAULT now());"
                    + "CREATE INDEX IF NOT EXISTS order_projection_ref ON order_projection (ref);"
                    + "CREATE TABLE IF NOT EXISTS ack_skipped ("
                    + " ref TEXT PRIMARY KEY, at TIMESTAMPTZ DEFAULT now())");
        }
    }

    private static void waitForStream(io.nats.client.Connection nats, String stream) throws InterruptedException {
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
