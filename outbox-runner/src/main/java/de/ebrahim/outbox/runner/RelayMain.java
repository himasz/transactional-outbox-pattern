package de.ebrahim.outbox.runner;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import de.ebrahim.outbox.RelayConfig;
import de.ebrahim.outbox.RelayEngine;
import de.ebrahim.outbox.election.LeaderElector;
import de.ebrahim.outbox.election.PostgresLeaseElector;
import de.ebrahim.outbox.store.OutboxStore;
import de.ebrahim.outbox.transport.MessagePublisher;
import de.ebrahim.outbox.transport.NatsPublisher;
import de.ebrahim.outbox.transport.NatsWakeupSignal;
import de.ebrahim.outbox.transport.WakeupSignal;
import io.nats.client.Nats;
import io.nats.client.Options;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

/**
 * The standalone relay: a thin main() around the same {@link RelayEngine} the
 * embedded mode uses.
 *
 * <p>Embedded and standalone are the same code, packaged differently. Standalone
 * is what I would run in production — the relay can then be upgraded without
 * redeploying every service that writes to the outbox, and its resource profile
 * is separate from the request path.
 *
 * <p>One relay deployment per database, never one shared across services: a
 * relay reading several services' outbox tables would need credentials to every
 * database and would couple itself to every schema. Reuse belongs at the image
 * level, not the instance level.
 */
public final class RelayMain {

    private static final Logger log = LoggerFactory.getLogger(RelayMain.class);

    public static void main(String[] args) throws Exception {
        String jdbcUrl   = env("PG_URL", "jdbc:postgresql://postgres:5432/outbox");
        String jdbcUser  = env("PG_USER", "outbox");
        String jdbcPass  = env("PG_PASSWORD", "outbox");
        String natsUrl   = env("NATS_URL", "nats://nats:4222");
        String instance  = env("INSTANCE_ID", "relay-" + System.currentTimeMillis());
        String stream    = env("STREAM_NAME", "ORDERS");
        String subjects  = env("STREAM_SUBJECTS", "orders.>");
        String wakeSubj  = env("WAKEUP_SUBJECT", "outbox.wakeup");

        HikariConfig hikari = new HikariConfig();
        hikari.setJdbcUrl(jdbcUrl);
        hikari.setUsername(jdbcUser);
        hikari.setPassword(jdbcPass);
        // The relay is single-threaded by design (FIFO), so it needs very few
        // connections: one for the batch read, one for the cursor write.
        hikari.setMaximumPoolSize(4);
        hikari.setPoolName("outbox-relay");

        try (HikariDataSource dataSource = new HikariDataSource(hikari);
             io.nats.client.Connection nats = Nats.connect(natsOptions(natsUrl))) {

            ensureStream(nats, stream, subjects);

            OutboxStore store = new OutboxStore(dataSource);
            MessagePublisher publisher = new NatsPublisher(nats.jetStream());
            // A real elector, not the mock: the example is genuinely distributed,
            // and this demonstrates using the store we already run rather than
            // standing up a separate consensus cluster.
            LeaderElector elector = new PostgresLeaseElector(dataSource, instance);
            WakeupSignal wakeup = new NatsWakeupSignal(nats, wakeSubj);

            RelayEngine engine = new RelayEngine(store, publisher, elector, wakeup, RelayConfig.defaults());
            Runtime.getRuntime().addShutdownHook(new Thread(engine::close));

            log.info("relay {} starting: db={} nats={} stream={}", instance, jdbcUrl, natsUrl, stream);
            engine.start();

            // Park the main thread; the relay runs on its own virtual thread.
            Thread.currentThread().join();
        }
    }

    private static Options natsOptions(String url) {
        return new Options.Builder()
                .server(url)
                .maxReconnects(-1)
                .reconnectWait(Duration.ofSeconds(1))
                .connectionTimeout(Duration.ofSeconds(10))
                .build();
    }

    /** Idempotent stream setup so the example is runnable from a cold start. */
    private static void ensureStream(io.nats.client.Connection nats, String name, String subjects) {
        try {
            StreamConfiguration config = StreamConfiguration.builder()
                    .name(name)
                    .subjects(subjects.split(","))
                    .storageType(StorageType.File)
                    // Absorbs the duplicates that a crash between publish and
                    // cursor advance necessarily produces.
                    .duplicateWindow(Duration.ofMinutes(5))
                    .build();
            nats.jetStreamManagement().addStream(config);
            log.info("created JetStream stream {}", name);
        } catch (Exception e) {
            log.info("stream {} already exists or could not be created: {}", name, e.getMessage());
        }
    }

    private static String env(String key, String fallback) {
        String value = System.getenv(key);
        return value == null || value.isBlank() ? fallback : value;
    }
}
