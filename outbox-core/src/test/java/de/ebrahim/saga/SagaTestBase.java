package de.ebrahim.saga;

import de.ebrahim.inbox.store.InboxSchema;
import de.ebrahim.outbox.Outbox;
import de.ebrahim.outbox.store.OutboxStore;
import de.ebrahim.outbox.store.Schema;
import de.ebrahim.outbox.transport.WakeupSignal;
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
import java.util.List;

/**
 * Shared fixture for the saga tests.
 *
 * <p>Real PostgreSQL, for the reasons the other two modules give, plus one of
 * its own: the compensating paths turn on conditional updates —
 * {@code WHERE available >= ?}, {@code WHERE status = 'AUTHORIZED'} — whose
 * whole value is that they are atomic. A fake that ran them as read-then-write
 * would pass every test here and oversell in production.
 *
 * <p>The broker is faked ({@link InMemoryBus}) and so is the relay's scheduling
 * ({@link Choreography#settle}), which is what makes these tests deterministic
 * and sub-second rather than a set of sleeps. The relay's own correctness is
 * pinned by {@code outbox-core}'s suite, not re-litigated here — except in
 * {@code OrderSagaTest.realRelayDrivesTheWholeSaga}, which runs the actual
 * {@code RelayEngine} once to confirm the wiring is honest.
 */
@Testcontainers
abstract class SagaTestBase {

    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine");

    DataSource dataSource;
    OutboxStore outboxStore;
    WakeupSignal wakeup;
    Outbox outbox;

    @BeforeEach
    void resetDatabase() throws Exception {
        PGSimpleDataSource ds = new PGSimpleDataSource();
        ds.setUrl(POSTGRES.getJdbcUrl());
        ds.setUser(POSTGRES.getUsername());
        ds.setPassword(POSTGRES.getPassword());
        this.dataSource = ds;

        try (Connection c = ds.getConnection(); Statement st = c.createStatement()) {
            st.execute("""
                    DROP TABLE IF EXISTS
                        saga_order, saga_payment, saga_stock, saga_stock_reservation,
                        saga_shipment, inbox_message, outbox_message, outbox_cursor, outbox_lease
                    """);
        }
        Schema.apply(ds);
        InboxSchema.apply(ds);
        SagaSchema.apply(ds);

        this.outboxStore = new OutboxStore(ds);
        this.wakeup = new WakeupSignal.Local();
        this.outbox = new Outbox(ds, wakeup);
    }

    /** Wires all four participants onto one bus. */
    Choreography choreography(InMemoryBus bus) {
        List<SagaParticipant> participants = new ArrayList<>();
        participants.add(OrderService.register(participant("order", bus)));
        participants.add(PaymentService.register(participant("payment", bus)));
        participants.add(InventoryService.register(participant("inventory", bus)));
        participants.add(ShippingService.register(participant("shipping", bus)));
        return new Choreography(participants, outboxStore, bus);
    }

    private SagaParticipant participant(String name, InMemoryBus bus) {
        return new SagaParticipant(name, dataSource, bus.subscribe(name), wakeup);
    }

    void place(String ref, long amountCents, String sku, int quantity, String destination) throws Exception {
        OrderService.place(outbox, ref, amountCents, sku, quantity, destination);
    }

    /** Overrides seeded stock so a test can make a SKU run out on demand. */
    void setStock(String sku, int available) throws SQLException {
        try (Connection c = dataSource.getConnection();
             PreparedStatement ps = c.prepareStatement(
                     "UPDATE saga_stock SET available = ?, initial = ? WHERE sku = ?")) {
            ps.setInt(1, available);
            ps.setInt(2, available);
            ps.setString(3, sku);
            ps.executeUpdate();
        }
    }

    String orderStatus(String ref) throws SQLException {
        return text("SELECT status FROM saga_order WHERE ref = ?", ref);
    }

    String cancelReason(String ref) throws SQLException {
        return text("SELECT cancel_reason FROM saga_order WHERE ref = ?", ref);
    }

    String paymentStatus(String ref) throws SQLException {
        return text("SELECT status FROM saga_payment WHERE ref = ?", ref);
    }

    String shipmentStatus(String ref) throws SQLException {
        return text("SELECT status FROM saga_shipment WHERE ref = ?", ref);
    }

    int stockAvailable(String sku) throws SQLException {
        return (int) scalar("SELECT available FROM saga_stock WHERE sku = '" + sku + "'");
    }

    long scalar(String sql) throws SQLException {
        try (Connection c = dataSource.getConnection();
             Statement st = c.createStatement();
             ResultSet rs = st.executeQuery(sql)) {
            return rs.next() ? rs.getLong(1) : 0L;
        }
    }

    private String text(String sql, String arg) throws SQLException {
        try (Connection c = dataSource.getConnection();
             PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, arg);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getString(1) : null;
            }
        }
    }
}
