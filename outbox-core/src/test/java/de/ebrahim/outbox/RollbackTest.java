package de.ebrahim.outbox;

import de.ebrahim.outbox.election.MockLeaderElector;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.Savepoint;
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

    @Test
    @DisplayName("a rollback forced by the database publishes nothing")
    void databaseErrorRollbackPublishesNothing() throws Exception {
        // The realistic rollback is not a deliberate throw: it is a constraint
        // violation, a deadlock, or a serialization failure. The message must
        // vanish just the same, and the library needs no code path for it.
        try (Connection tx = tx()) {
            try (Statement st = tx.createStatement()) {
                st.execute("ALTER TABLE orders ADD CONSTRAINT ref_not_empty CHECK (ref <> '')");
            }
            tx.commit();
        }

        try (Connection tx = tx()) {
            writer.enqueue(tx, OutboxMessage.of("orders.created", "doomed"));
            assertThrows(Exception.class, () -> {
                try (Statement st = tx.createStatement()) {
                    st.execute("INSERT INTO orders (ref) VALUES ('')");   // violates the check
                }
            });
            tx.rollback();
        }

        RecordingPublisher publisher = new RecordingPublisher();
        relay(publisher, MockLeaderElector.alwaysLeader()).tick();

        assertTrue(publisher.published.isEmpty(), "a constraint violation must take the message with it");
    }

    @Test
    @DisplayName("ANTI-PATTERN: enqueueing on a second connection defeats the pattern")
    void separateConnectionBreaksAtomicity() throws Exception {
        // Documents the one way a caller can still break atomicity, so the
        // failure is a known limitation rather than a surprise. The library
        // cannot detect this: there is no portable way to ask a JDBC connection
        // whether it belongs to the same logical transaction as another.
        try (Connection businessTx = tx();
             Connection outboxTx = tx()) {          // WRONG: a different transaction

            try (Statement st = businessTx.createStatement()) {
                st.execute("INSERT INTO orders (ref) VALUES ('orphan')");
            }
            writer.enqueue(outboxTx, OutboxMessage.of("orders.created", "orphan"));

            outboxTx.commit();      // the message survives independently...
            businessTx.rollback();  // ...while the business data does not
        }

        RecordingPublisher publisher = new RecordingPublisher();
        relay(publisher, MockLeaderElector.alwaysLeader()).tick();

        assertEquals(1, publisher.published.size(),
                "this is the orphaned-event failure the pattern exists to prevent; "
                        + "always enqueue on the SAME connection as your business writes");
    }

    @Test
    @DisplayName("ANTI-PATTERN: rolling back to a savepoint silently drops the message")
    void savepointRollbackDropsMessage() throws Exception {
        // Partial rollback is the subtler hazard: the business row survives but
        // the outbox row does not, leaving committed data with no event. Worth
        // knowing about before someone reaches for savepoints in a retry loop.
        try (Connection tx = tx()) {
            try (Statement st = tx.createStatement()) {
                st.execute("INSERT INTO orders (ref) VALUES ('kept')");
            }
            Savepoint savepoint = tx.setSavepoint();
            writer.enqueue(tx, OutboxMessage.of("orders.created", "dropped"));
            tx.rollback(savepoint);     // undoes ONLY the enqueue
            tx.commit();                // the order is committed regardless
        }

        RecordingPublisher publisher = new RecordingPublisher();
        relay(publisher, MockLeaderElector.alwaysLeader()).tick();

        assertTrue(publisher.published.isEmpty(),
                "the message is gone while the order is committed: never enqueue inside "
                        + "a savepoint you might roll back");
    }
}