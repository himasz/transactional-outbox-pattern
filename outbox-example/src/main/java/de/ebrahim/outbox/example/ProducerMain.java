package de.ebrahim.outbox.example;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import de.ebrahim.outbox.*;
import io.nats.client.Nats;
import io.nats.client.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Statement;
import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;

/**
 * An example service replica that writes orders and enqueues events.
 *
 * <p>Several of these run concurrently in docker-compose, which is what makes
 * the demo a real test: concurrent writers on separate connections are exactly
 * the conditions that produce the sequence holes the relay has to survive.
 *
 * <p>Every tenth transaction is rolled back on purpose. Those orders never
 * appear in the consumer output, which is the visible proof of the
 * "rolled back means never published" requirement.
 */
public final class ProducerMain {

    private static final Logger log = LoggerFactory.getLogger(ProducerMain.class);

    public static void main(String[] args) throws Exception {
        String jdbcUrl  = env("PG_URL", "jdbc:postgresql://postgres:5432/outbox");
        String natsUrl  = env("NATS_URL", "nats://nats:4222");
        String instance = env("INSTANCE_ID", "producer");
        String wakeSubj = env("WAKEUP_SUBJECT", "outbox.wakeup");
        int intervalMs  = Integer.parseInt(env("INTERVAL_MS", "300"));

        HikariConfig hikari = new HikariConfig();
        hikari.setJdbcUrl(jdbcUrl);
        hikari.setUsername(env("PG_USER", "outbox"));
        hikari.setPassword(env("PG_PASSWORD", "outbox"));
        hikari.setMaximumPoolSize(5);
        hikari.setPoolName("producer-" + instance);

        try (HikariDataSource dataSource = new HikariDataSource(hikari);
             io.nats.client.Connection nats = Nats.connect(
                     new Options.Builder().server(natsUrl).maxReconnects(-1)
                             .reconnectWait(Duration.ofSeconds(1)).build())) {

            // Only one replica needs to apply the schema; the others no-op
            // thanks to IF NOT EXISTS.
            Schema.apply(dataSource);
            createBusinessTable(dataSource);

            try (Outbox outbox = new Outbox(dataSource, new NatsWakeupSignal(nats, wakeSubj))) {
                log.info("producer {} writing every {}ms", instance, intervalMs);
                for (int n = 1; ; n++) {
                    boolean rollback = n % 10 == 0;
                    try {
                        placeOrder(outbox, instance, n, rollback);
                    } catch (DeliberateRollback expected) {
                        log.info("{} order {} rolled back on purpose: no event will be published", instance, n);
                    } catch (Exception e) {
                        log.error("{} order {} failed", instance, n, e);
                    }
                    Thread.sleep(ThreadLocalRandom.current().nextInt(intervalMs));
                }
            }
        }
    }

    /**
     * The whole write path in one method: business row and event in a single
     * local transaction, and the relay nudged only after that transaction
     * commits.
     */
    private static void placeOrder(Outbox outbox, String instance, int n, boolean rollback) throws Exception {
        String ref = instance + "-order-" + n;
        outbox.inTransaction((tx, writer) -> {
            try (Statement st = tx.createStatement()) {
                st.execute("INSERT INTO orders (ref) VALUES ('" + ref + "')");
            }
            writer.enqueue(tx, OutboxMessage
                    .of("orders.created", "{\"ref\":\"" + ref + "\"}")
                    .withHeader("Content-Type", "application/json")
                    .withHeader("Producer", instance));
            if (rollback) {
                // Any exception rolls the transaction back, taking the outbox row
                // with it. No compensating action is needed or possible.
                throw new DeliberateRollback();
            }
            return null;
        });
        log.info("{} committed {}", instance, ref);
    }

    private static void createBusinessTable(javax.sql.DataSource ds) throws Exception {
        try (var c = ds.getConnection(); Statement st = c.createStatement()) {
            st.execute("CREATE TABLE IF NOT EXISTS orders ("
                    + "id BIGSERIAL PRIMARY KEY, ref TEXT NOT NULL, created_at TIMESTAMPTZ DEFAULT now())");
        }
    }

    private static final class DeliberateRollback extends RuntimeException { }

    private static String env(String key, String fallback) {
        String value = System.getenv(key);
        return value == null || value.isBlank() ? fallback : value;
    }
}
