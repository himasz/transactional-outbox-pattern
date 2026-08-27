package de.ebrahim.saga;

import de.ebrahim.outbox.OutboxMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.List;

/**
 * Reserves and releases stock.
 *
 * <p>The failure here is real contention over a real resource rather than an
 * injected coin flip: {@code SKU-SCARCE} is seeded with twelve units, and once
 * they are gone the conditional decrement stops matching. That matters for the
 * demo's credibility — a modulus-based failure proves the compensating path
 * runs, but not that it runs under the conditions that actually cause it.
 */
public final class InventoryService {

    private static final Logger log = LoggerFactory.getLogger(InventoryService.class);

    private InventoryService() { }

    public static SagaParticipant register(SagaParticipant participant) {
        return participant
                .on(SagaSubjects.PAYMENT_AUTHORIZED, InventoryService::reserve)
                .on(SagaSubjects.SHIPPING_FAILED, InventoryService::release);
    }

    /**
     * The whole availability check is one conditional UPDATE.
     *
     * <p>{@code WHERE available >= ?} makes the check and the decrement a single
     * atomic step, so two concurrent reservations cannot both see enough stock
     * and both take it. Reading the balance and then updating it would be the
     * obvious version and would oversell under exactly the concurrency this demo
     * runs at.
     */
    private static List<OutboxMessage> reserve(Connection tx, SagaEvent event) throws Exception {
        String sku = event.sku();
        int quantity = event.quantity() == null ? 1 : event.quantity();

        int taken;
        try (PreparedStatement ps = tx.prepareStatement("""
                UPDATE saga_stock SET available = available - ?
                 WHERE sku = ? AND available >= ?
                """)) {
            ps.setInt(1, quantity);
            ps.setString(2, sku);
            ps.setInt(3, quantity);
            taken = ps.executeUpdate();
        }

        if (taken == 0) {
            log.info("inventory REJECTED {} — {} x{} unavailable", event.ref(), sku, quantity);
            return List.of(SagaEvent.of(event.ref())
                    .withReason("out of stock: " + sku)
                    .toMessage(SagaSubjects.INVENTORY_REJECTED));
        }

        try (PreparedStatement ps = tx.prepareStatement("""
                INSERT INTO saga_stock_reservation (ref, sku, quantity) VALUES (?, ?, ?)
                ON CONFLICT (ref) DO NOTHING
                """)) {
            ps.setString(1, event.ref());
            ps.setString(2, sku);
            ps.setInt(3, quantity);
            ps.executeUpdate();
        }

        log.info("inventory RESERVED {} — {} x{}", event.ref(), sku, quantity);
        // Carries the destination onward, so shipping never has to look up
        // another service's data.
        return List.of(event.toMessage(SagaSubjects.INVENTORY_RESERVED));
    }

    /**
     * The compensating step: put the stock back and tell payment to release the
     * money.
     *
     * <p>This is the link in the chain that gets forgotten in real systems. The
     * refund is visible — a customer complains if it does not happen — while
     * unreleased stock is invisible until the shelf is mysteriously empty weeks
     * later. {@code verify-saga.sql} checks conservation precisely because
     * nothing else would notice.
     *
     * <p>The outgoing event is emitted even when the reservation row was already
     * released, which cannot happen here but would elsewhere. Ending the branch
     * instead would strand the payment: the money would stay held with nothing
     * left in flight to release it. When in doubt a saga should keep unwinding —
     * downstream steps are guarded, so an extra event is absorbed, while a
     * missing one is a stall nobody is watching for.
     */
    private static List<OutboxMessage> release(Connection tx, SagaEvent event) throws Exception {
        String sku = null;
        int quantity = 0;
        try (PreparedStatement ps = tx.prepareStatement("""
                UPDATE saga_stock_reservation SET released = true, released_at = now()
                 WHERE ref = ? AND released = false
                RETURNING sku, quantity
                """)) {
            ps.setString(1, event.ref());
            try (var rs = ps.executeQuery()) {
                if (rs.next()) {
                    sku = rs.getString(1);
                    quantity = rs.getInt(2);
                }
            }
        }

        if (sku != null) {
            try (PreparedStatement ps = tx.prepareStatement(
                    "UPDATE saga_stock SET available = available + ? WHERE sku = ?")) {
                ps.setInt(1, quantity);
                ps.setString(2, sku);
                ps.executeUpdate();
            }
            log.info("inventory RELEASED {} — {} x{} back to stock", event.ref(), sku, quantity);
        } else {
            log.warn("release for {} found no active reservation; continuing the unwind anyway", event.ref());
        }

        String reason = event.reason() == null ? "shipping failed" : event.reason();
        return List.of(SagaEvent.of(event.ref()).withReason(reason)
                .toMessage(SagaSubjects.INVENTORY_RELEASED));
    }
}
