package de.ebrahim.saga;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import de.ebrahim.inbox.InboxConfig;
import de.ebrahim.inbox.store.InboxSchema;
import de.ebrahim.inbox.transport.NatsMessageSource;
import de.ebrahim.outbox.Outbox;
import de.ebrahim.outbox.store.Schema;
import de.ebrahim.outbox.transport.NatsWakeupSignal;
import io.nats.client.Nats;
import io.nats.client.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * Runs one saga participant, or all four in one process.
 *
 * <p>{@code PARTICIPANT=all} is a convenience for running the demo on a laptop.
 * It is <em>not</em> a different architecture: the four participants share a JVM
 * but nothing else — separate inbox consumer names, separate tables, and no
 * method call between them. Everything one service knows about another still
 * arrives as an event through the broker. Set {@code PARTICIPANT=payment} and
 * the same binary is a single deployable, which is how
 * {@code docker-compose.saga.yml} runs it.
 */
public final class SagaMain {

    private static final Logger log = LoggerFactory.getLogger(SagaMain.class);

    public static void main(String[] args) throws Exception {
        String which    = env("PARTICIPANT", "all");
        String natsUrl  = env("NATS_URL", "nats://nats:4222");
        String stream   = env("STREAM_NAME", "SAGA");
        String wakeSubj = env("WAKEUP_SUBJECT", "outbox.wakeup");
        int intervalMs  = Integer.parseInt(env("ORDER_INTERVAL_MS", "500"));

        HikariConfig hikari = new HikariConfig();
        hikari.setJdbcUrl(env("PG_URL", "jdbc:postgresql://postgres:5432/outbox"));
        hikari.setUsername(env("PG_USER", "outbox"));
        hikari.setPassword(env("PG_PASSWORD", "outbox"));
        hikari.setMaximumPoolSize(8);
        hikari.setPoolName("saga-" + which);

        try (HikariDataSource dataSource = new HikariDataSource(hikari);
             io.nats.client.Connection nats = Nats.connect(new Options.Builder()
                     .server(natsUrl).maxReconnects(-1).reconnectWait(Duration.ofSeconds(1)).build())) {

            // Each service owns its own database in production; here they share
            // one, so whoever boots first applies all three schemas. The advisory
            // locks inside make that safe when they boot together.
            Schema.apply(dataSource);
            InboxSchema.apply(dataSource);
            SagaSchema.apply(dataSource);
            waitForStream(nats, stream);

            List<SagaParticipant> running = new ArrayList<>();
            try (NatsWakeupSignal wakeup = new NatsWakeupSignal(nats, wakeSubj)) {

                for (String name : which.equals("all")
                        ? List.of("order", "payment", "inventory", "shipping")
                        : List.of(which)) {
                    running.add(start(name, dataSource, nats, stream, wakeup));
                }

                if (which.equals("all") || which.equals("order")) {
                    // The order service is also where new sagas begin. Placing an
                    // order is an ordinary outbox write — the saga has no special
                    // "start" API, because there is nothing to start.
                    generateOrders(new Outbox(dataSource, wakeup), env("INSTANCE_ID", "gen"), intervalMs);
                } else {
                    Thread.currentThread().join();
                }
            } finally {
                for (SagaParticipant participant : running) {
                    participant.close();
                }
            }
        }
    }

    private static SagaParticipant start(String name, javax.sql.DataSource dataSource,
                                         io.nats.client.Connection nats, String stream,
                                         NatsWakeupSignal wakeup) throws Exception {
        // One wildcard subscription per service, dispatched by subject inside the
        // participant. A production deployment would use per-subject filters or a
        // consumer per subject; the wildcard keeps the demo's wiring readable and
        // costs only an acknowledge for events the service does not handle.
        InboxConfig config = InboxConfig.forConsumer(name).withOrdered(false);
        SagaParticipant participant = new SagaParticipant(name, dataSource,
                new NatsMessageSource(nats.jetStream(), stream, SagaSubjects.ALL, config), wakeup);

        switch (name) {
            case "order"     -> OrderService.register(participant);
            case "payment"   -> PaymentService.register(participant);
            case "inventory" -> InventoryService.register(participant);
            case "shipping"  -> ShippingService.register(participant);
            default -> throw new IllegalArgumentException(
                    "unknown participant '" + name + "'; expected order|payment|inventory|shipping|all");
        }
        participant.start();
        return participant;
    }

    /**
     * Places orders forever, on a fixed pattern so the three failure paths are
     * reproducible rather than occasional.
     *
     * <ul>
     *   <li>every 11th order is over the authorization ceiling → payment declines;</li>
     *   <li>every 3rd order wants {@code SKU-SCARCE}, whose twelve units run out
     *       early → inventory rejects, and payment has to refund;</li>
     *   <li>every 7th order ships to {@code EMBARGOED} → shipping refuses, and
     *       inventory <em>and</em> payment both have to unwind.</li>
     * </ul>
     *
     * <p>An order can qualify for more than one, in which case it fails at
     * whichever step comes first. That is deliberate: the interesting property is
     * that every saga reaches a terminal state, not that each failure gets its
     * own tidy lane.
     */
    private static void generateOrders(Outbox outbox, String instance, int intervalMs) throws Exception {
        log.info("placing an order every {}ms", intervalMs);
        for (int n = 1; ; n++) {
            String ref = instance + "-" + n;
            String sku = switch (n % 3) {
                case 0 -> "SKU-SCARCE";
                case 1 -> "SKU-COMMON";
                default -> "SKU-REGULAR";
            };
            long amount = n % 11 == 0 ? PaymentService.DECLINE_ABOVE_CENTS + 5_000 : 5_000;
            String destination = n % 7 == 0 ? ShippingService.EMBARGOED_DESTINATION : "BERLIN";

            try {
                OrderService.place(outbox, ref, amount, sku, 1, destination);
            } catch (Exception e) {
                log.error("could not place {}", ref, e);
            }
            Thread.sleep(intervalMs);
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
