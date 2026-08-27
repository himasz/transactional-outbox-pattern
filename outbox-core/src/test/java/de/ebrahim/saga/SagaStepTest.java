package de.ebrahim.saga;

import de.ebrahim.outbox.OutboxMessage;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Statement;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * One step, in isolation.
 *
 * <p>Everything a saga can get wrong at the step level is a partial commit:
 * state changed but the next event lost, or an event emitted for a change that
 * was rolled back. The first stalls the saga silently and forever; the second
 * has downstream services act on something that never happened. Both are
 * structurally impossible when the claim, the writes and the outgoing events
 * share one transaction, and these tests are what say so out loud.
 */
class SagaStepTest extends SagaTestBase {

    private static final String SUBJECT = SagaSubjects.ORDER_CREATED;

    @Test
    @DisplayName("state change and outgoing event commit together")
    void stateAndEventCommitTogether() throws Exception {
        InMemoryBus bus = new InMemoryBus();
        try (Choreography saga = new Choreography(
                List.of(PaymentService.register(participant("payment", bus))), outboxStore, bus)) {

            place("ref-1", 5_000, "SKU-COMMON", 1, "BERLIN");
            saga.deliverPending();
            saga.pumpOnce();

            assertEquals("AUTHORIZED", paymentStatus("ref-1"));
            assertEquals(1, countEvents(SagaSubjects.PAYMENT_AUTHORIZED),
                    "the event must exist because the state change did");
        }
    }

    @Test
    @DisplayName("a step that fails leaves no state and no event")
    void failureLeavesNothingBehind() throws Exception {
        InMemoryBus bus = new InMemoryBus();
        SagaParticipant participant = participant("payment", bus)
                .on(SUBJECT, (tx, event) -> {
                    try (Statement st = tx.createStatement()) {
                        st.execute("INSERT INTO saga_payment (ref, amount_cents, status) "
                                + "VALUES ('ref-2', 5000, 'AUTHORIZED')");
                    }
                    // Anything can fail after the write and before the commit: a
                    // constraint, a downstream lookup, the process itself.
                    throw new IllegalStateException("authorization gateway timed out");
                });

        try (Choreography saga = new Choreography(List.of(participant), outboxStore, bus)) {
            place("ref-2", 5_000, "SKU-COMMON", 1, "BERLIN");
            saga.deliverPending();
            saga.pumpOnce();

            assertNull(paymentStatus("ref-2"), "no state may survive the failed step");
            assertEquals(0, countEvents(SagaSubjects.PAYMENT_AUTHORIZED),
                    "and no event may be emitted for a step that did not happen");
            assertEquals("PENDING", inboxStatus("payment", "ref-2"),
                    "the input stays eligible, so a redelivery retries the step");
        }
    }

    @Test
    @DisplayName("a duplicated input does not emit the outgoing event twice")
    void duplicateInputEmitsOnce() throws Exception {
        // Every message delivered twice, which is what at-least-once actually
        // means rather than what it usually looks like during a test run.
        InMemoryBus bus = new InMemoryBus().withDuplicates();
        try (Choreography saga = new Choreography(
                List.of(PaymentService.register(participant("payment", bus))), outboxStore, bus)) {

            place("ref-3", 5_000, "SKU-COMMON", 1, "BERLIN");
            saga.settle();

            assertEquals(1, scalar("SELECT count(*) FROM saga_payment WHERE ref = 'ref-3'"),
                    "the customer must be charged once");
            assertEquals(1, countEvents(SagaSubjects.PAYMENT_AUTHORIZED),
                    "and the duplicate must stop here rather than fan out downstream");
        }
    }

    @Test
    @DisplayName("a step returning no events ends its branch")
    void terminalStepEmitsNothing() throws Exception {
        InMemoryBus bus = new InMemoryBus();
        SagaParticipant participant = participant("order", bus)
                .on(SUBJECT, (tx, event) -> List.<OutboxMessage>of());

        try (Choreography saga = new Choreography(List.of(participant), outboxStore, bus)) {
            place("ref-4", 5_000, "SKU-COMMON", 1, "BERLIN");
            int rounds = saga.settle();

            assertEquals(1, scalar("SELECT count(*) FROM outbox_message"),
                    "only the order.created row: the branch ended without emitting");
            org.junit.jupiter.api.Assertions.assertTrue(rounds < 10, "settled promptly, no event cycle");
        }
    }

    @Test
    @DisplayName("re-creating an event instead of replaying it defeats deduplication")
    void regeneratedMessageIdDefeatsTheInbox() throws Exception {
        InMemoryBus bus = new InMemoryBus();
        try (Choreography saga = new Choreography(
                List.of(InventoryService.register(participant("inventory", bus))), outboxStore, bus)) {

            SagaEvent event = new SagaEvent("ref-5", 5_000L, "SKU-COMMON", 2, "BERLIN", null);

            // Two messages, identical content, DIFFERENT ids — because
            // SagaEvent.toMessage generates a fresh id every call. This is what a
            // retry looks like when it rebuilds the event instead of replaying the
            // outbox row, and it is the one duplicate the inbox cannot catch: to
            // the consumer these are simply two different messages.
            OutboxMessage first = event.toMessage(SagaSubjects.PAYMENT_AUTHORIZED);
            OutboxMessage second = event.toMessage(SagaSubjects.PAYMENT_AUTHORIZED);
            assertNotEquals(first.messageId(), second.messageId());

            bus.publish(row(1, first));
            bus.publish(row(2, second));
            saga.pump();

            // Four units left the shelf against one reservation of two. Every other
            // effect in this saga is an upsert or a status write and would have
            // absorbed this silently; the stock decrement is a read-modify-write,
            // so it does not.
            assertEquals(999_996, stockAvailable("SKU-COMMON"), "the decrement ran twice");
            assertEquals(1, scalar("SELECT count(*) FROM saga_stock_reservation WHERE ref = 'ref-5'"));
            assertEquals(2, conservationDrift("SKU-COMMON"),
                    "this drift is exactly what verify-saga.sql CHECK 7 reports, "
                    + "and nothing else in the system would notice it");
        }
    }

    @Test
    @DisplayName("replaying the same message id costs nothing, which is the contrast")
    void replayedMessageIdIsAbsorbed() throws Exception {
        InMemoryBus bus = new InMemoryBus();
        try (Choreography saga = new Choreography(
                List.of(InventoryService.register(participant("inventory", bus))), outboxStore, bus)) {

            SagaEvent event = new SagaEvent("ref-6", 5_000L, "SKU-COMMON", 2, "BERLIN", null);
            OutboxMessage message = event.toMessage(SagaSubjects.PAYMENT_AUTHORIZED);

            // The same row replayed, which is what the relay actually does after a
            // crash between publish and cursor advance.
            bus.publish(row(1, message));
            bus.publish(row(1, message));
            saga.pump();

            assertEquals(999_998, stockAvailable("SKU-COMMON"), "the decrement ran once");
            assertEquals(0, conservationDrift("SKU-COMMON"));
        }
    }

    private static de.ebrahim.outbox.store.OutboxStore.Row row(long id, OutboxMessage message) {
        return new de.ebrahim.outbox.store.OutboxStore.Row(
                id, message.subject(), message.messageId(), message.headers(), message.payload());
    }

    private long conservationDrift(String sku) throws Exception {
        return scalar("""
                SELECT s.available + COALESCE((SELECT sum(r.quantity) FROM saga_stock_reservation r
                          WHERE r.sku = s.sku AND r.released = false), 0) - s.initial
                  FROM saga_stock s WHERE s.sku = '""" + sku + "'") * -1;
    }

    private SagaParticipant participant(String name, InMemoryBus bus) {
        return new SagaParticipant(name, dataSource, bus.subscribe(name), wakeup);
    }

    private long countEvents(String subject) throws Exception {
        return scalar("SELECT count(*) FROM outbox_message WHERE subject = '" + subject + "'");
    }

    private String inboxStatus(String consumer, String ref) throws Exception {
        try (var c = dataSource.getConnection();
             var ps = c.prepareStatement(
                     "SELECT status FROM inbox_message WHERE consumer = ? ORDER BY seq DESC LIMIT 1")) {
            ps.setString(1, consumer);
            try (var rs = ps.executeQuery()) {
                return rs.next() ? rs.getString(1) : null;
            }
        }
    }
}
