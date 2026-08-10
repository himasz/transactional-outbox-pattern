package de.ebrahim.outbox;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The "as little delay as possible" requirement.
 *
 * <p>The idle timeout here is set absurdly high so the test can only pass via
 * the nudge: if the wake-up path were broken, the relay would sit blocked for
 * thirty seconds and the assertion would time out.
 */
class WakeupLatencyTest extends OutboxTestBase {

    @Test
    @DisplayName("a commit wakes the relay without waiting for the poll interval")
    void commitWakesRelayImmediately() throws Exception {
        WakeupSignal wakeup = new WakeupSignal.Local();
        CountDownLatch delivered = new CountDownLatch(1);

        MessagePublisher publisher = row -> delivered.countDown();

        RelayConfig config = RelayConfig.defaults().withIdleTimeout(Duration.ofSeconds(30));
        try (RelayEngine engine = new RelayEngine(
                store, publisher, MockLeaderElector.alwaysLeader(), wakeup, config)) {
            engine.start();

            long startedAt = System.nanoTime();
            try (Connection tx = tx()) {
                writer.enqueue(tx, OutboxMessage.of("orders.created", "fast"));
                tx.commit();
            }
            wakeup.signal();   // what Outbox.inTransaction() does after a successful commit

            assertTrue(delivered.await(5, TimeUnit.SECONDS),
                    "relay did not wake on the nudge within 5s");
            long elapsedMillis = (System.nanoTime() - startedAt) / 1_000_000;
            assertTrue(elapsedMillis < 2_000,
                    "publish took " + elapsedMillis + "ms; the nudge path is not working");
        }
    }
}
