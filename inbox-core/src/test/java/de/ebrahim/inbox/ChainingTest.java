package de.ebrahim.inbox;

import de.ebrahim.outbox.transport.MessagePublisher;
import de.ebrahim.outbox.election.MockLeaderElector;
import de.ebrahim.outbox.OutboxMessage;
import de.ebrahim.outbox.store.OutboxStore;
import de.ebrahim.outbox.store.OutboxWriter;
import de.ebrahim.outbox.RelayConfig;
import de.ebrahim.outbox.RelayEngine;
import de.ebrahim.outbox.store.Schema;
import de.ebrahim.outbox.transport.WakeupSignal;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Consume one event, produce another, atomically.
 *
 * <p>This is where the two patterns stop being two tools and become a pipeline.
 * A service that reacts to a message by emitting another has three things to
 * make durable — the record that it handled the input, the business change, and
 * the output event — and all three go through the same connection, so all three
 * commit or none do. No distributed transaction, no compensating action, and no
 * window in which the downstream event exists for work that was rolled back.
 *
 * <p>Note what is <em>not</em> here: any dependency between the two libraries.
 * {@code inbox-core} does not know {@code outbox-core} exists; it hands the
 * handler an open transaction and stops having opinions. The outbox appears in
 * this module at test scope only, which is the point being made.
 *
 * <p>{@code redeliveryDoesNotEmitTheEventTwice} is the one worth reading. Without
 * an inbox, an at-least-once redelivery does not just repeat local work — it
 * emits a second downstream event, and the duplicate propagates through every
 * service behind this one, amplifying at each hop.
 */
class ChainingTest extends InboxTestBase {

    private final OutboxWriter outbox = new OutboxWriter();

    @BeforeEach
    void applyOutboxSchema() throws Exception {
        Schema.apply(dataSource);
        try (var c = dataSource.getConnection(); Statement st = c.createStatement()) {
            st.execute("CREATE TABLE IF NOT EXISTS shipment (ref TEXT PRIMARY KEY)");
            st.execute("TRUNCATE shipment");
        }
    }

    /**
     * The whole pipeline step: claim the input, write the business row, enqueue
     * the output. One connection, one transaction, one commit.
     */
    private InboxHandler fulfil() {
        return (tx, message) -> {
            String ref = message.payloadAsString();
            try (var ps = tx.prepareStatement("INSERT INTO shipment (ref) VALUES (?)")) {
                ps.setString(1, ref);
                ps.executeUpdate();
            }
            outbox.enqueue(tx, OutboxMessage
                    .of("shipments.requested", "{\"ref\":\"" + ref + "\"}")
                    .withHeader("Ref", ref));
        };
    }

    @Test
    @DisplayName("claim, business write and outbox enqueue commit as one unit")
    void allThreeCommitTogether() throws Exception {
        InboxMessage message = message("order-1");

        assertEquals(InboxResult.PROCESSED, inbox().process(message, fulfil()));

        assertEquals("DONE", statusOf(message.messageId()));
        assertEquals(1, scalar("SELECT count(*) FROM shipment WHERE ref = 'order-1'"));
        assertEquals(1, scalar("SELECT count(*) FROM outbox_message WHERE subject = 'shipments.requested'"));
    }

    @Test
    @DisplayName("a failure after enqueueing leaves no event behind")
    void failureLeavesNoOutboundEvent() throws Exception {
        InboxMessage message = message("order-2");

        InboxResult result = inbox().process(message, (tx, msg) -> {
            fulfil().handle(tx, msg);
            // Anything at all can fail after the enqueue — a constraint, a
            // downstream lookup, the process itself. The outbox row must go with
            // it, or a rolled-back shipment is announced to the whole system.
            throw new IllegalStateException("fulfilment refused");
        });

        assertEquals(InboxResult.RETRY, result);
        assertEquals(0, scalar("SELECT count(*) FROM shipment"));
        assertEquals(0, scalar("SELECT count(*) FROM outbox_message"),
                "no event may survive the transaction that produced it");
        assertEquals("PENDING", statusOf(message.messageId()));
    }

    @Test
    @DisplayName("a redelivered input does not emit the output event twice")
    void redeliveryDoesNotEmitTheEventTwice() throws Exception {
        InboxMessage message = message("order-3");
        Inbox inbox = inbox();

        assertEquals(InboxResult.PROCESSED, inbox.process(message, fulfil()));
        assertEquals(InboxResult.DUPLICATE, inbox.process(message, fulfil()));
        assertEquals(InboxResult.DUPLICATE, inbox.process(message, fulfil()));

        assertEquals(1, scalar("SELECT count(*) FROM shipment"));
        assertEquals(1, scalar("SELECT count(*) FROM outbox_message"),
                "the duplicate must be absorbed here, not amplified downstream");
    }

    @Test
    @DisplayName("the chained event reaches the relay and is published once")
    void chainedEventIsPublishedOnce() throws Exception {
        Inbox inbox = inbox();
        for (int i = 1; i <= 3; i++) {
            InboxMessage message = message("order-" + i);
            assertEquals(InboxResult.PROCESSED, inbox.process(message, fulfil()));
            // Every input arrives twice, as at-least-once delivery guarantees it
            // eventually will.
            assertEquals(InboxResult.DUPLICATE, inbox.process(message, fulfil()));
        }

        // Drive the real relay over its public API, exactly as a deployment
        // would: nothing here reaches into the outbox module to make the test
        // easier, because the claim being made is about the two libraries
        // composing as shipped.
        RecordingPublisher publisher = new RecordingPublisher();
        try (RelayEngine relay = new RelayEngine(new OutboxStore(dataSource), publisher,
                MockLeaderElector.alwaysLeader(), new WakeupSignal.Local(), RelayConfig.defaults())) {
            relay.start();
            long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
            while (publisher.subjects.size() < 3 && System.nanoTime() < deadline) {
                Thread.sleep(50);
            }
        }

        assertEquals(3, publisher.subjects.size(),
                "six deliveries in, three events out: the inbox is where the duplicates stopped");
        assertNull(statusOf(UUID.randomUUID()), "an id this consumer has never seen has no row");
    }

    private static final class RecordingPublisher implements MessagePublisher {
        final List<String> subjects = java.util.Collections.synchronizedList(new ArrayList<>());

        @Override
        public void publish(OutboxStore.Row row) {
            subjects.add(row.subject());
        }
    }
}
