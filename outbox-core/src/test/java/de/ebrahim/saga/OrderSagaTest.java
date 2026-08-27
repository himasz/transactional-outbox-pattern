package de.ebrahim.saga;

import de.ebrahim.outbox.RelayConfig;
import de.ebrahim.outbox.RelayEngine;
import de.ebrahim.outbox.election.MockLeaderElector;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The whole choreography: one forward path and three ways to fall off it.
 *
 * <p>{@code shippingRefusalUnwindsBothServices} is the one that earns its keep.
 * A saga whose compensations are all single-step never has to unwind two
 * services in order, and that cascade is where real ones leak: the refund is
 * visible so somebody notices, the stock release is not, so nobody does until
 * the shelf is empty. {@code stockIsConservedUnderPressure} is the check that
 * would catch it.
 */
class OrderSagaTest extends SagaTestBase {

    @Test
    @DisplayName("happy path: authorized, reserved, shipped, completed")
    void happyPathCompletes() throws Exception {
        InMemoryBus bus = new InMemoryBus();
        try (Choreography saga = choreography(bus)) {
            place("ok-1", 5_000, "SKU-COMMON", 2, "BERLIN");
            saga.settle();

            assertEquals("COMPLETED", orderStatus("ok-1"));
            assertEquals("AUTHORIZED", paymentStatus("ok-1"));
            assertEquals("DISPATCHED", shipmentStatus("ok-1"));
            assertEquals(999_998, stockAvailable("SKU-COMMON"), "two units left the shelf");
            assertNull(cancelReason("ok-1"));
        }
    }

    @Test
    @DisplayName("declined payment cancels the order and never touches stock")
    void declinedPaymentCancels() throws Exception {
        InMemoryBus bus = new InMemoryBus();
        try (Choreography saga = choreography(bus)) {
            // Above the authorization ceiling, so payment refuses at step one.
            place("declined-1", PaymentService.DECLINE_ABOVE_CENTS + 1, "SKU-COMMON", 1, "BERLIN");
            saga.settle();

            assertEquals("CANCELLED", orderStatus("declined-1"));
            assertEquals("insufficient funds", cancelReason("declined-1"));
            assertEquals("DECLINED", paymentStatus("declined-1"));
            assertNull(shipmentStatus("declined-1"), "nothing may ship for an unpaid order");
            assertEquals(1_000_000, stockAvailable("SKU-COMMON"),
                    "the saga failed before inventory, so there is nothing to compensate");
        }
    }

    @Test
    @DisplayName("out of stock refunds the payment, then cancels")
    void outOfStockRefundsThenCancels() throws Exception {
        setStock("SKU-SCARCE", 0);

        InMemoryBus bus = new InMemoryBus();
        try (Choreography saga = choreography(bus)) {
            place("oos-1", 5_000, "SKU-SCARCE", 1, "BERLIN");
            saga.settle();

            // The money was held before the failure, so it has to come back.
            // That is the whole reason this is a saga and not a transaction.
            assertEquals("REFUNDED", paymentStatus("oos-1"));
            assertEquals("CANCELLED", orderStatus("oos-1"));
            assertEquals("out of stock: SKU-SCARCE", cancelReason("oos-1"));
            assertNull(shipmentStatus("oos-1"));
            assertEquals(0, stockAvailable("SKU-SCARCE"));
        }
    }

    @Test
    @DisplayName("shipping refusal unwinds inventory AND payment, in order")
    void shippingRefusalUnwindsBothServices() throws Exception {
        InMemoryBus bus = new InMemoryBus();
        try (Choreography saga = choreography(bus)) {
            place("embargo-1", 5_000, "SKU-COMMON", 3, ShippingService.EMBARGOED_DESTINATION);
            saga.settle();

            assertEquals("REFUSED", shipmentStatus("embargo-1"));

            // Two compensations had to run, in the right order, across two
            // services that never speak to each other directly.
            assertEquals(1_000_000, stockAvailable("SKU-COMMON"), "the three units went back");
            assertEquals(1, scalar("SELECT count(*) FROM saga_stock_reservation"
                    + " WHERE ref = 'embargo-1' AND released = true"));
            assertEquals("REFUNDED", paymentStatus("embargo-1"));
            assertEquals("CANCELLED", orderStatus("embargo-1"));
            assertNotNull(cancelReason("embargo-1"));
        }
    }

    @Test
    @DisplayName("every message delivered twice changes none of the outcomes")
    void duplicatesChangeNothing() throws Exception {
        setStock("SKU-SCARCE", 2);

        InMemoryBus bus = new InMemoryBus().withDuplicates();
        try (Choreography saga = choreography(bus)) {
            place("dup-ok", 5_000, "SKU-COMMON", 1, "BERLIN");
            place("dup-declined", PaymentService.DECLINE_ABOVE_CENTS + 1, "SKU-COMMON", 1, "BERLIN");
            place("dup-oos-a", 5_000, "SKU-SCARCE", 2, "BERLIN");
            place("dup-oos-b", 5_000, "SKU-SCARCE", 2, "BERLIN");
            place("dup-embargo", 5_000, "SKU-COMMON", 1, ShippingService.EMBARGOED_DESTINATION);
            saga.settle();

            assertEquals("COMPLETED", orderStatus("dup-ok"));
            assertEquals("CANCELLED", orderStatus("dup-declined"));
            assertEquals("CANCELLED", orderStatus("dup-embargo"));

            // One of the two scarce orders wins the last two units; the other is
            // compensated. Which one is not asserted — the point is that exactly
            // one wins, however the race resolves.
            assertEquals(1, scalar("SELECT count(*) FROM saga_order"
                    + " WHERE ref LIKE 'dup-oos-%' AND status = 'COMPLETED'"));
            assertEquals(1, scalar("SELECT count(*) FROM saga_order"
                    + " WHERE ref LIKE 'dup-oos-%' AND status = 'CANCELLED'"));

            assertNoDoubleEffects();
            assertStockConserved();
            assertMoneyBalances();
        }
    }

    @Test
    @DisplayName("stock is conserved when a scarce SKU forces repeated compensation")
    void stockIsConservedUnderPressure() throws Exception {
        setStock("SKU-SCARCE", 5);

        InMemoryBus bus = new InMemoryBus().withDuplicates();
        try (Choreography saga = choreography(bus)) {
            // Twelve orders chasing five units, some of them also embargoed, so
            // reservations are taken and given back repeatedly.
            for (int i = 1; i <= 12; i++) {
                String destination = i % 4 == 0 ? ShippingService.EMBARGOED_DESTINATION : "BERLIN";
                place("scarce-" + i, 5_000, "SKU-SCARCE", 1, destination);
            }
            saga.settle();

            assertEquals(0, scalar("SELECT count(*) FROM saga_order"
                            + " WHERE status NOT IN ('COMPLETED', 'CANCELLED')"),
                    "every saga must reach a terminal state — a stuck one is the failure "
                    + "mode nobody has an alert for");

            assertNoDoubleEffects();
            assertStockConserved();
            assertMoneyBalances();
        }
    }

    @Test
    @DisplayName("the real relay drives the whole saga end to end")
    void realRelayDrivesTheWholeSaga() throws Exception {
        InMemoryBus bus = new InMemoryBus();
        try (Choreography saga = choreography(bus);
             RelayEngine relay = new RelayEngine(outboxStore, bus,
                     MockLeaderElector.alwaysLeader(), wakeup, RelayConfig.defaults())) {

            // Everything above replaces the relay's scheduling with an explicit
            // loop, which is what makes those tests deterministic. This one runs
            // the genuine RelayEngine against the same wiring, so the shortcut
            // cannot quietly diverge from the thing that ships.
            relay.start();
            place("real-1", 5_000, "SKU-COMMON", 1, "BERLIN");
            place("real-2", 5_000, "SKU-COMMON", 1, ShippingService.EMBARGOED_DESTINATION);

            long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
            while (System.nanoTime() < deadline && terminalOrders() < 2) {
                saga.pumpOnce();
                Thread.sleep(5);
            }

            assertEquals("COMPLETED", orderStatus("real-1"));
            assertEquals("CANCELLED", orderStatus("real-2"));
            assertStockConserved();
            assertMoneyBalances();
        }
    }

    private long terminalOrders() throws Exception {
        return scalar("SELECT count(*) FROM saga_order WHERE status IN ('COMPLETED', 'CANCELLED')");
    }

    /** No effect may be applied twice, in any service. */
    private void assertNoDoubleEffects() throws Exception {
        assertEquals(0, scalar("SELECT count(*) FROM (SELECT ref FROM saga_payment"
                + " GROUP BY ref HAVING count(*) > 1) d"), "a customer charged twice");
        assertEquals(0, scalar("SELECT count(*) FROM (SELECT ref FROM saga_shipment"
                + " GROUP BY ref HAVING count(*) > 1) d"), "a parcel shipped twice");
        assertEquals(0, scalar("SELECT count(*) FROM (SELECT ref FROM saga_stock_reservation"
                + " GROUP BY ref HAVING count(*) > 1) d"), "stock reserved twice");
    }

    /**
     * The invisible invariant. Every unit is either on the shelf or held by an
     * unreleased reservation; a compensation that silently skipped its release
     * shows up here and nowhere else.
     */
    private void assertStockConserved() throws Exception {
        assertEquals(0, scalar("""
                SELECT count(*) FROM saga_stock s
                 WHERE s.available + COALESCE((
                           SELECT sum(r.quantity) FROM saga_stock_reservation r
                            WHERE r.sku = s.sku AND r.released = false), 0) <> s.initial
                """), "stock is not conserved: a reservation was never released");
    }

    /** Money held must correspond exactly to orders that actually completed. */
    private void assertMoneyBalances() throws Exception {
        assertEquals(0, scalar("""
                SELECT count(*) FROM saga_payment p
                  JOIN saga_order o ON o.ref = p.ref
                 WHERE (o.status = 'CANCELLED' AND p.status = 'AUTHORIZED')
                    OR (o.status = 'COMPLETED' AND p.status <> 'AUTHORIZED')
                """), "money is held for a cancelled order, or missing for a completed one");
    }
}
