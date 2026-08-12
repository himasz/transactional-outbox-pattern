package de.ebrahim.outbox;

import de.ebrahim.outbox.election.MockLeaderElector;
import de.ebrahim.outbox.store.OutboxStore;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The test that actually earns its keep.
 *
 * <p>{@code sequenceHoleIsNotSkipped} reproduces the failure mode that a naive
 * {@code WHERE id > cursor ORDER BY id} relay hits under concurrency: ids are
 * assigned at INSERT time, so a transaction holding a lower id can commit after
 * one holding a higher id. Without the {@code pg_snapshot_xmin} guard the relay
 * publishes the higher id, advances the cursor past the hole, and the lower id
 * is never sent. Removing the {@code tx_id <} predicate from
 * {@code OutboxStore.FETCH_BATCH_SQL} makes this test fail.
 */
class FifoOrderingTest extends OutboxTestBase {

    @Test
    @DisplayName("an id assigned earlier but committed later is not skipped")
    void sequenceHoleIsNotSkipped() throws Exception {
        RecordingPublisher publisher = new RecordingPublisher();
        RelayEngine engine = relay(publisher, MockLeaderElector.alwaysLeader());

        Connection slow = tx();     // takes the lower id, commits last
        Connection fast = tx();     // takes the higher id, commits first

        writer.enqueue(slow, OutboxMessage.of("orders.created", "low-id-late-commit"));
        writer.enqueue(fast, OutboxMessage.of("orders.created", "high-id-early-commit"));

        fast.commit();

        // The relay polls in the window where the higher id is visible and the
        // lower id is not. It must publish nothing rather than skip ahead.
        engine.tick();
        assertTrue(publisher.published.isEmpty(),
                "relay must not publish past an in-flight lower id");

        slow.commit();
        slow.close();
        fast.close();

        engine.tick();

        assertEquals(List.of(1L, 2L), publisher.ids(),
                "both messages must arrive, in id order");
    }

    @Test
    @DisplayName("concurrent writers still produce a strictly increasing stream")
    void concurrentWritersPreserveOrder() throws Exception {
        int writers = 8;
        int perWriter = 25;
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(writers);
        List<Thread> threads = new ArrayList<>();

        for (int w = 0; w < writers; w++) {
            final int id = w;
            threads.add(Thread.ofVirtual().start(() -> {
                try {
                    start.await();
                    for (int i = 0; i < perWriter; i++) {
                        try (Connection tx = tx()) {
                            writer.enqueue(tx, OutboxMessage.of("orders.created", "w" + id + "-" + i));
                            tx.commit();
                        }
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    done.countDown();
                }
            }));
        }

        start.countDown();
        assertTrue(done.await(60, TimeUnit.SECONDS), "writers did not finish in time");

        RecordingPublisher publisher = new RecordingPublisher();
        RelayEngine engine = relay(publisher, MockLeaderElector.alwaysLeader());
        // Drain repeatedly: the xmin watermark may hold rows back briefly while
        // other transactions are still settling.
        for (int i = 0; i < 20 && publisher.published.size() < writers * perWriter; i++) {
            engine.tick();
        }

        List<Long> ids = publisher.ids();
        assertEquals(writers * perWriter, ids.size(), "every committed message must be published");
        for (int i = 1; i < ids.size(); i++) {
            assertTrue(ids.get(i) > ids.get(i - 1),
                    "ids must be strictly increasing, saw " + ids.get(i - 1) + " then " + ids.get(i));
        }
    }
}
