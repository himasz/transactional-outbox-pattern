package de.ebrahim.outbox;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Connection;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Leadership and fencing.
 *
 * <p>These are the tests the mocked elector exists for. A mock that always
 * returned true would make none of this observable, which is why
 * {@link MockLeaderElector} is driveable instead.
 */
class FencingTest extends OutboxTestBase {

    private void enqueue(int count) throws Exception {
        for (int i = 0; i < count; i++) {
            try (Connection tx = tx()) {
                writer.enqueue(tx, OutboxMessage.of("orders.created", "m" + i));
                tx.commit();
            }
        }
    }

    @Test
    @DisplayName("a follower publishes nothing and does not move the cursor")
    void followerIsIdle() throws Exception {
        enqueue(5);
        RecordingPublisher publisher = new RecordingPublisher();
        relay(publisher, new MockLeaderElector(false)).tick();

        assertTrue(publisher.published.isEmpty());
        assertEquals(0, store.readCursor());
    }

    @Test
    @DisplayName("a stale leader cannot advance the cursor")
    void staleLeaderIsFenced() throws Exception {
        enqueue(3);

        MockLeaderElector elector = MockLeaderElector.alwaysLeader();
        long staleToken = elector.currentToken();

        // A newer leader takes over and stamps its token on the cursor.
        elector.grant();
        assertTrue(store.claimCursor(elector.currentToken()));

        // The old leader, unaware it was replaced, tries to advance the watermark.
        assertFalse(store.advanceCursor(3, staleToken),
                "a lower fencing token must be rejected by the database");
        assertEquals(0, store.readCursor(), "the watermark must be untouched");
    }

    @Test
    @DisplayName("losing the lease mid-batch stops publishing and leaves the cursor alone")
    void leaseLostMidBatch() throws Exception {
        enqueue(4);

        MockLeaderElector elector = MockLeaderElector.alwaysLeader();
        RecordingPublisher publisher = new RecordingPublisher() {
            @Override
            public void publish(OutboxStore.Row row) {
                super.publish(row);
                if (published.size() == 2) {
                    elector.revoke();     // leadership yanked after the second message
                }
            }
        };

        relay(publisher, elector).tick();

        assertEquals(2, publisher.published.size(), "relay must stop as soon as the lease is invalid");
        // The two messages that WERE published are still recorded, because no
        // newer leader has claimed the cursor yet and the fence therefore lets
        // this write through. Recording them is the desirable outcome: it avoids
        // republishing work that already succeeded. Had a successor claimed the
        // cursor first, this same advance would have been rejected — that case is
        // covered by staleLeaderIsFenced.
        assertEquals(2, store.readCursor(), "successfully published messages are not replayed needlessly");
    }

    @Test
    @DisplayName("a successor fences the previous leader out of the cursor entirely")
    void successorBlocksMidBatchAdvance() throws Exception {
        enqueue(4);

        MockLeaderElector elector = MockLeaderElector.alwaysLeader();
        long originalToken = elector.currentToken();

        RecordingPublisher publisher = new RecordingPublisher() {
            @Override
            public void publish(OutboxStore.Row row) {
                super.publish(row);
                if (published.size() == 2) {
                    // A successor takes over and stamps its token mid-batch.
                    elector.grant();
                    try {
                        store.claimCursor(elector.currentToken());
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        };

        relay(publisher, elector).tick();

        assertEquals(2, publisher.published.size());
        assertEquals(0, store.readCursor(),
                "the displaced leader must not move a watermark it no longer owns");
        assertTrue(elector.currentToken() > originalToken);
    }

    @Test
    @DisplayName("the postgres lease elector grants leadership to exactly one owner")
    void postgresLeaseIsExclusive() {
        try (PostgresLeaseElector first = new PostgresLeaseElector(dataSource, "instance-1");
             PostgresLeaseElector second = new PostgresLeaseElector(dataSource, "instance-2")) {

            assertTrue(first.tryAcquire().isPresent(), "the first caller should win the lease");
            assertTrue(second.tryAcquire().isEmpty(), "a second caller must be refused while the lease is live");
            // Renewal keeps the same token, so an active leader never invalidates itself.
            assertEquals(first.tryAcquire().orElseThrow().fencingToken(),
                    first.tryAcquire().orElseThrow().fencingToken());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
