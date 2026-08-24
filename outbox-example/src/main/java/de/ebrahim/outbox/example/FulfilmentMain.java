package de.ebrahim.outbox.example;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import de.ebrahim.inbox.Inbox;
import de.ebrahim.inbox.InboxConfig;
import de.ebrahim.inbox.InboxConsumer;
import de.ebrahim.inbox.store.InboxSchema;
import de.ebrahim.inbox.transport.MessageSource;
import de.ebrahim.inbox.transport.NatsMessageSource;
import de.ebrahim.outbox.transport.NatsWakeupSignal;
import de.ebrahim.outbox.OutboxMessage;
import de.ebrahim.outbox.store.OutboxWriter;
import de.ebrahim.outbox.store.Schema;
import io.nats.client.Nats;
import io.nats.client.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;

/**
 * A service that consumes one event and produces another — the step that turns
 * two patterns into a pipeline.
 *
 * <p>It reacts to {@code orders.created} by recording a shipment and emitting
 * {@code shipments.requested}. Three facts have to become durable together: that
 * this service handled the input, that the shipment exists, and that the output
 * event will be sent. All three are written through one JDBC connection in one
 * transaction, so all three commit or none do.
 *
 * <pre>
 *   ┌──────────────── one local transaction ────────────────┐
 *   │  INSERT INTO inbox_message ...   (claim the input)    │   atomic, no 2PC
 *   │  INSERT INTO shipment ...        (the business fact)  │
 *   │  INSERT INTO outbox_message ...  (the output event)   │
 *   └───────────────────────────────────────────────────────┘
 * </pre>
 *
 * <p>Two failure modes disappear at once. A crash before the commit leaves
 * nothing: no shipment, no event, and the input is redelivered. A duplicate
 * input is absorbed by the claim, so the downstream event is emitted once —
 * which matters more than it first appears, because without it every duplicate
 * would be amplified at every hop of the chain.
 *
 * <p>Note that the two libraries have no dependency on each other. The inbox
 * hands the handler an open transaction and stops having opinions about what
 * else goes on it; the outbox takes whatever connection it is given. The
 * composition happens here, in application code, which is the only place that
 * knows both.
 */
public final class FulfilmentMain {

    private static final Logger log = LoggerFactory.getLogger(FulfilmentMain.class);

    public static void main(String[] args) throws Exception {
        String natsUrl  = env("NATS_URL", "nats://nats:4222");
        String stream   = env("STREAM_NAME", "ORDERS");
        String subject  = env("SUBJECT", "orders.>");
        String consumer = env("CONSUMER_NAME", "fulfilment");
        String wakeSubj = env("WAKEUP_SUBJECT", "outbox.wakeup");

        HikariConfig hikari = new HikariConfig();
        hikari.setJdbcUrl(env("PG_URL", "jdbc:postgresql://postgres:5432/outbox"));
        hikari.setUsername(env("PG_USER", "outbox"));
        hikari.setPassword(env("PG_PASSWORD", "outbox"));
        hikari.setMaximumPoolSize(4);
        hikari.setPoolName("fulfilment");

        try (HikariDataSource dataSource = new HikariDataSource(hikari);
             io.nats.client.Connection nats = Nats.connect(new Options.Builder()
                     .server(natsUrl).maxReconnects(-1).reconnectWait(Duration.ofSeconds(1)).build())) {

            Schema.apply(dataSource);        // this service also writes to an outbox
            InboxSchema.apply(dataSource);   // ...and reads from an inbox
            createBusinessTable(dataSource);
            waitForStream(nats, stream);

            OutboxWriter outbox = new OutboxWriter();
            InboxConfig config = InboxConfig.forConsumer(consumer);
            Inbox inbox = new Inbox(dataSource, config);

            try (NatsWakeupSignal wakeup = new NatsWakeupSignal(nats, wakeSubj);
                 MessageSource source = new NatsMessageSource(nats.jetStream(), stream, subject, config);
                 InboxConsumer loop = InboxConsumer.inline(source, inbox, (tx, message) -> {
                     String ref = message.header("Ref");

                     try (PreparedStatement ps = tx.prepareStatement(
                             "INSERT INTO shipment (ref, ordered_at) VALUES (?, now())")) {
                         ps.setString(1, ref);
                         ps.executeUpdate();
                     }

                     // The output event, on the same connection. It becomes
                     // publishable only if this transaction commits, so a
                     // shipment is never announced that does not exist.
                     outbox.enqueue(tx, OutboxMessage
                             .of("shipments.requested", "{\"ref\":\"" + ref + "\"}")
                             .withHeader("Content-Type", "application/json")
                             .withHeader("Ref", ref));

                     log.info("fulfilled {} and enqueued shipments.requested", ref);
                 })) {

                log.info("fulfilment service listening on {}", subject);
                loop.start();

                // The relay is nudged on a timer rather than per commit, because
                // InboxConsumer owns the transaction boundary and there is no
                // after-commit hook to hang a nudge on. Losing a nudge costs
                // latency, never correctness — the relay's idle tick picks the
                // rows up regardless — so this is a fair trade for the demo. A
                // production service would use its framework's after-commit
                // callback, as Outbox.nudge() documents.
                while (true) {
                    Thread.sleep(250);
                    wakeup.signal();
                }
            }
        }
    }

    /**
     * Demo instrumentation. As with {@code order_projection}, {@code shipment.ref}
     * carries no unique constraint on purpose: a double application has to be
     * findable as data, not maskable as an error.
     */
    private static void createBusinessTable(DataSource ds) throws SQLException {
        try (java.sql.Connection c = ds.getConnection(); Statement st = c.createStatement()) {
            st.execute("SELECT pg_advisory_xact_lock(hashtext('de.ebrahim.outbox.example.fulfilment'));"
                    + "CREATE TABLE IF NOT EXISTS shipment ("
                    + " ref TEXT NOT NULL, ordered_at TIMESTAMPTZ NOT NULL DEFAULT now());"
                    + "CREATE INDEX IF NOT EXISTS shipment_ref ON shipment (ref)");
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
