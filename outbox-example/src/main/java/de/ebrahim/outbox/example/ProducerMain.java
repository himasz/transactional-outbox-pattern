package de.ebrahim.outbox.example;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import de.ebrahim.outbox.Outbox;
import de.ebrahim.outbox.OutboxMessage;
import de.ebrahim.outbox.store.Schema;
import de.ebrahim.outbox.transport.NatsWakeupSignal;
import io.nats.client.Nats;
import io.nats.client.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.PreparedStatement;
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
 * <p>Every tenth transaction is rolled back on purpose, and the rolled-back ref
 * is recorded in {@code rollback_audit} on a separate committed connection.
 * That audit row is what makes the requirement checkable: absence from the
 * consumer log proves nothing, because a message lost by a broken relay looks
 * exactly the same. See verify.sql.
 */
public final class ProducerMain {

    private static final Logger log = LoggerFactory.getLogger(ProducerMain.class);

    public static void main(String[] args) throws Exception {
        String jdbcUrl = env("PG_URL", "jdbc:postgresql://postgres:5432/outbox");
        String natsUrl = env("NATS_URL", "nats://nats:4222");
        String instance = env("INSTANCE_ID", "producer");
        String wakeSubj = env("WAKEUP_SUBJECT", "outbox.wakeup");
        int intervalMs = Integer.parseInt(env("INTERVAL_MS", "300"));
        String runId = env("RUN_ID", Long.toHexString(System.currentTimeMillis()).substring(6));

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
                    String ref = instance + "-" + runId + "-" + n;
                    try {
                        placeOrder(outbox, ref, rollback);
                        log.info("committed {}", ref);
                    } catch (DeliberateRollback expected) {
                        // Record the rolled-back ref in a SEPARATE, committed
                        // transaction. Without this the demo has no evidence: a
                        // missing id looks identical whether it was rolled back
                        // on purpose or lost by a broken relay. verify.sql turns
                        // this into a positive assertion.
                        recordRollback(dataSource, ref);
                        log.info("rolled back {} on purpose: no event may ever be published", ref);
                    } catch (Exception e) {
                        log.error("{} failed", ref, e);
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
    private static void placeOrder(Outbox outbox, String ref, boolean rollback) throws Exception {
        outbox.inTransaction((tx, writer) -> {
            try (PreparedStatement ps = tx.prepareStatement("INSERT INTO orders (ref) VALUES (?)")) {
                ps.setString(1, ref);
                ps.executeUpdate();
            }
            writer.enqueue(tx, OutboxMessage
                    .of("orders.created", "{\"ref\":\"" + ref + "\"}")
                    .withHeader("Content-Type", "application/json")
                    .withHeader("Ref", ref));
            if (rollback) {
                // Any exception rolls the transaction back, taking the outbox row
                // with it. No compensating action is needed or possible.
                throw new DeliberateRollback();
            }
            return null;
        });
    }

    /**
     * Writes the audit row on its own connection, after the business transaction
     * has already rolled back. It deliberately does NOT use the outbox: this is
     * demo instrumentation, not domain behaviour.
     */
    private static void recordRollback(javax.sql.DataSource ds, String ref) throws Exception {
        try (var c = ds.getConnection();
             PreparedStatement ps = c.prepareStatement(
                     "INSERT INTO rollback_audit (ref) VALUES (?) ON CONFLICT DO NOTHING")) {
            ps.setString(1, ref);
            ps.executeUpdate();
        }
    }

    private static void createBusinessTable(javax.sql.DataSource ds) throws Exception {
        try (var c = ds.getConnection(); Statement st = c.createStatement()) {
            // Same race as Schema.apply: three producer replicas boot at once and
            // IF NOT EXISTS alone doesn't serialize them. Advisory-lock the whole
            // batch (transaction-scoped, released when this statement's implicit
            // transaction ends).
            st.execute("SELECT pg_advisory_xact_lock(hashtext('de.ebrahim.outbox.example.schema'));"
                    + "CREATE TABLE IF NOT EXISTS orders ("
                    + "id BIGSERIAL PRIMARY KEY, ref TEXT UNIQUE NOT NULL, created_at TIMESTAMPTZ DEFAULT now());"
                    // Demo-only tables. They are NOT part of the library schema: the
                    // library has no opinion about how you observe your own rollbacks.
                    + "CREATE TABLE IF NOT EXISTS rollback_audit ("
                    + "ref TEXT PRIMARY KEY, recorded_at TIMESTAMPTZ DEFAULT now());"
                    + "CREATE TABLE IF NOT EXISTS delivered ("
                    + "ref TEXT PRIMARY KEY, deliveries INT NOT NULL DEFAULT 1,"
                    + " first_seen TIMESTAMPTZ DEFAULT now())");
        }
    }

    private static final class DeliberateRollback extends RuntimeException {
    }

    private static String env(String key, String fallback) {
        String value = System.getenv(key);
        return value == null || value.isBlank() ? fallback : value;
    }
}