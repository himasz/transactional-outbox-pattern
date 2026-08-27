package de.ebrahim.inbox;

import de.ebrahim.inbox.store.InboxGuard;
import de.ebrahim.inbox.store.InboxSchema;
import de.ebrahim.inbox.store.InboxStore;
import de.ebrahim.inbox.transport.MessageSource;
import org.junit.jupiter.api.BeforeEach;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Shared fixture, mirroring {@code OutboxTestBase}.
 *
 * <p>Real PostgreSQL, for the same reason the outbox tests use it and one more.
 * The relay's correctness rests on snapshot visibility; the inbox's rests on how
 * {@code INSERT ... ON CONFLICT DO UPDATE} behaves when two transactions race on
 * the same key — that the loser <em>waits</em> rather than failing, and then
 * re-evaluates its {@code WHERE} clause against the winner's committed row. No
 * in-memory database reproduces that, and a fake would quietly turn the most
 * important test in this package green for the wrong reason.
 *
 * <p>The broker is faked. Nothing under test is NATS-specific, and
 * {@link RecordingSource} makes redelivery — the thing that actually needs
 * testing — something a test can simply choose to do.
 */
@Testcontainers
abstract class InboxTestBase {

    // PG 13+ is required for pg_current_xact_id()/pg_snapshot_xmin().
    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine");

    static final String CONSUMER = "order-projector";

    DataSource dataSource;
    InboxStore store;
    InboxGuard guard = new InboxGuard();

    @BeforeEach
    void resetDatabase() throws Exception {
        PGSimpleDataSource ds = new PGSimpleDataSource();
        ds.setUrl(POSTGRES.getJdbcUrl());
        ds.setUser(POSTGRES.getUsername());
        ds.setPassword(POSTGRES.getPassword());
        this.dataSource = ds;

        try (Connection c = ds.getConnection(); Statement st = c.createStatement()) {
            st.execute("DROP TABLE IF EXISTS inbox_message, outbox_message, outbox_cursor, outbox_lease, projection");
            // The business table. Its UNIQUE constraint is deliberate: several
            // tests assert that the handler ran exactly once, and a duplicate
            // insert should fail loudly rather than be counted.
            st.execute("CREATE TABLE projection (ref TEXT PRIMARY KEY, applied_at TIMESTAMPTZ DEFAULT now())");
        }
        InboxSchema.apply(ds);
        this.store = new InboxStore(ds, CONSUMER);
    }

    Inbox inbox() {
        return inbox(InboxConfig.forConsumer(CONSUMER));
    }

    Inbox inbox(InboxConfig config) {
        return new Inbox(dataSource, config);
    }

    /** Opens a connection with auto-commit off, as the library requires. */
    Connection tx() throws Exception {
        Connection c = dataSource.getConnection();
        c.setAutoCommit(false);
        return c;
    }

    static InboxMessage message(String ref) {
        return InboxMessage.of(UUID.randomUUID(), "orders.created", ref);
    }

    /**
     * A handler that writes one business row per message and counts its own
     * invocations. The count is what separates "the effects happened once" from
     * "the handler ran once"; the tests care about both, and they are not the
     * same claim.
     */
    static class CountingHandler implements InboxHandler {
        final AtomicInteger invocations = new AtomicInteger();
        final List<String> seen = Collections.synchronizedList(new ArrayList<>());

        @Override
        public void handle(Connection tx, InboxMessage message) throws Exception {
            invocations.incrementAndGet();
            String ref = message.payloadAsString();
            seen.add(ref);
            try (PreparedStatement ps = tx.prepareStatement("INSERT INTO projection (ref) VALUES (?)")) {
                ps.setString(1, ref);
                ps.executeUpdate();
            }
        }
    }

    /** A broker stand-in whose deliveries can be replayed on demand. */
    static final class RecordingSource implements MessageSource {
        final List<InboxMessage> acknowledged = Collections.synchronizedList(new ArrayList<>());
        final List<InboxMessage> retried = Collections.synchronizedList(new ArrayList<>());
        private final List<InboxMessage> queue = Collections.synchronizedList(new ArrayList<>());

        RecordingSource deliver(InboxMessage... messages) {
            Collections.addAll(queue, messages);
            return this;
        }

        @Override
        public Delivery next(java.time.Duration timeout) {
            synchronized (queue) {
                if (queue.isEmpty()) return null;
                InboxMessage message = queue.remove(0);
                return new Delivery() {
                    @Override public InboxMessage message() { return message; }
                    @Override public void acknowledge() { acknowledged.add(message); }
                    @Override public void retryLater() { retried.add(message); }
                };
            }
        }
    }

    long countProjection() throws SQLException {
        return scalar("SELECT count(*) FROM projection");
    }

    long countProjection(String ref) throws SQLException {
        try (Connection c = dataSource.getConnection();
             PreparedStatement ps = c.prepareStatement("SELECT count(*) FROM projection WHERE ref = ?")) {
            ps.setString(1, ref);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getLong(1);
            }
        }
    }

    String statusOf(UUID messageId) throws SQLException {
        return store.statusOf(messageId).orElse(null);
    }

    int attemptsOf(UUID messageId) throws SQLException {
        try (Connection c = dataSource.getConnection();
             PreparedStatement ps = c.prepareStatement(
                     "SELECT attempts FROM inbox_message WHERE consumer = ? AND message_id = ?")) {
            ps.setString(1, CONSUMER);
            ps.setObject(2, messageId);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getInt(1) : -1;
            }
        }
    }

    long scalar(String sql) throws SQLException {
        try (Connection c = dataSource.getConnection();
             Statement st = c.createStatement();
             ResultSet rs = st.executeQuery(sql)) {
            rs.next();
            return rs.getLong(1);
        }
    }
}
