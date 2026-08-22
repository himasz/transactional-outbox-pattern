package de.ebrahim.outbox;

import de.ebrahim.outbox.election.LeaderElector;
import de.ebrahim.outbox.store.OutboxStore;
import de.ebrahim.outbox.store.OutboxWriter;
import de.ebrahim.outbox.store.Schema;
import de.ebrahim.outbox.transport.MessagePublisher;
import de.ebrahim.outbox.transport.WakeupSignal;
import org.junit.jupiter.api.BeforeEach;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Shared fixture.
 *
 * <p>Tests run against a real PostgreSQL container rather than an in-memory
 * database, because the correctness of the relay rests on PostgreSQL snapshot
 * visibility ({@code pg_snapshot_xmin}) and on {@code BIGSERIAL} handing out ids
 * before commit. Neither behaviour can be faked, and a fake would hide exactly
 * the bug these tests exist to catch.
 *
 * <p>The broker is faked, though: {@link RecordingPublisher} keeps the tests
 * fast and deterministic, and none of the logic under test is NATS-specific.
 */
@Testcontainers
abstract class OutboxTestBase {

    // PG 13+ is required for pg_current_xact_id()/pg_snapshot_xmin().
    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine");

    DataSource dataSource;
    OutboxStore store;
    OutboxWriter writer = new OutboxWriter();

    @BeforeEach
    void resetDatabase() throws Exception {
        PGSimpleDataSource ds = new PGSimpleDataSource();
        ds.setUrl(POSTGRES.getJdbcUrl());
        ds.setUser(POSTGRES.getUsername());
        ds.setPassword(POSTGRES.getPassword());
        this.dataSource = ds;

        try (Connection c = ds.getConnection(); Statement st = c.createStatement()) {
            st.execute("DROP TABLE IF EXISTS outbox_message, outbox_cursor, outbox_lease, orders");
            st.execute("CREATE TABLE orders (id BIGSERIAL PRIMARY KEY, ref TEXT NOT NULL)");
        }
        Schema.apply(ds);
        this.store = new OutboxStore(ds);
    }

    /** Opens a connection with auto-commit off, as the library requires. */
    Connection tx() throws Exception {
        Connection c = dataSource.getConnection();
        c.setAutoCommit(false);
        return c;
    }

    /**
     * A broker stand-in that records exactly what the relay handed it, in order.
     * Non-final so tests can subclass it to inject behaviour mid-publish.
     */
    static class RecordingPublisher implements MessagePublisher {
        final List<OutboxStore.Row> published = Collections.synchronizedList(new ArrayList<>());

        @Override
        public void publish(OutboxStore.Row row) {
            published.add(row);
        }

        List<Long> ids() {
            synchronized (published) {
                return published.stream().map(OutboxStore.Row::id).toList();
            }
        }
    }

    RelayEngine relay(MessagePublisher publisher, LeaderElector elector) {
        return new RelayEngine(store, publisher, elector, new WakeupSignal.Noop(), RelayConfig.defaults());
    }
}
