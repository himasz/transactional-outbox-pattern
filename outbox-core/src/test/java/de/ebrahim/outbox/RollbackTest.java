package de.ebrahim.outbox;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The requirement that a rolled-back transaction must never publish.
 *
 * <p>There is nothing clever to verify here, and that is the point: the message
 * lives in the same transaction as the business data, so rollback removes it
 * with no compensation logic anywhere in the library.
 */
class RollbackTest extends OutboxTestBase {

    @Test
    @DisplayName("a rolled-back transaction publishes nothing")
    void rollbackPublishesNothing() throws Exception {
        try (Connection tx = tx()) {
            try (Statement st = tx.createStatement()) {
                st.execute("INSERT INTO orders (ref) VALUES ('will-be-rolled-back')");
            }
            writer.enqueue(tx, OutboxMessage.of("orders.created", "never sent"));
            tx.rollback();
        }

        RecordingPublisher publisher = new RecordingPublisher();
        relay(publisher, MockLeaderElector.alwaysLeader()).tick();

        assertTrue(publisher.published.isEmpty(), "no message may survive a rollback");
        assertEquals(0, store.readCursor());
    }

    @Test
    @DisplayName("a committed transaction publishes exactly once")
    void commitPublishes() throws Exception {
        try (Connection tx = tx()) {
            writer.enqueue(tx, OutboxMessage.of("orders.created", "hello"));
            tx.commit();
        }

        RecordingPublisher publisher = new RecordingPublisher();
        RelayEngine engine = relay(publisher, MockLeaderElector.alwaysLeader());
        engine.tick();
        engine.tick();   // a second pass must not republish

        assertEquals(1, publisher.published.size());
        assertEquals("orders.created", publisher.published.get(0).subject());
    }

    @Test
    @DisplayName("enqueueing on an auto-commit connection is rejected")
    void autoCommitIsRejected() throws Exception {
        try (Connection c = dataSource.getConnection()) {
            assertThrows(IllegalStateException.class,
                    () -> writer.enqueue(c, OutboxMessage.of("orders.created", "nope")));
        }
    }
}
