package de.ebrahim.saga;

import de.ebrahim.outbox.Outbox;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.List;

/**
 * Starts sagas and records how they end.
 *
 * <p>It is <em>not</em> a coordinator, and the difference is worth being precise
 * about because the shape looks similar from a distance. It never tells another
 * service what to do; it emits one event and then listens, exactly like every
 * other participant. Its {@code status} column is a projection of what has
 * already happened, not a workflow position that drives anything — delete it and
 * the saga still completes, it just becomes harder to watch.
 */
public final class OrderService {

    private static final Logger log = LoggerFactory.getLogger(OrderService.class);

    private OrderService() { }

    public static SagaParticipant register(SagaParticipant participant) {
        return participant
                // Progress tracking. Two services consuming the same event is
                // ordinary in choreography, and it works because the inbox key
                // includes the consumer: payment claiming order.created does not
                // stop the order service from claiming it too.
                .on(SagaSubjects.PAYMENT_AUTHORIZED, advanceTo("PAID"))
                .on(SagaSubjects.INVENTORY_RESERVED, advanceTo("RESERVED"))

                // Terminal states. All three emit nothing, which is how a
                // choreographed saga ends: not by being told it is over, but by
                // producing no further event.
                .on(SagaSubjects.ORDER_SHIPPED, complete())
                .on(SagaSubjects.PAYMENT_DECLINED, cancel())
                .on(SagaSubjects.PAYMENT_REFUNDED, cancel());
    }

    /** Places an order and emits the first event, in one transaction. */
    public static void place(Outbox outbox, String ref, long amountCents,
                             String sku, int quantity, String destination) throws Exception {
        outbox.inTransaction((tx, writer) -> {
            try (PreparedStatement ps = tx.prepareStatement("""
                    INSERT INTO saga_order (ref, amount_cents, sku, quantity, destination)
                    VALUES (?, ?, ?, ?, ?)
                    """)) {
                ps.setString(1, ref);
                ps.setLong(2, amountCents);
                ps.setString(3, sku);
                ps.setInt(4, quantity);
                ps.setString(5, destination);
                ps.executeUpdate();
            }
            writer.enqueue(tx, new SagaEvent(ref, amountCents, sku, quantity, destination, null)
                    .toMessage(SagaSubjects.ORDER_CREATED));
            return null;
        });
        log.info("order PLACED {} — {} x{} to {} ({} cents)", ref, sku, quantity, destination, amountCents);
    }

    /**
     * Moves the order forward, but never past a terminal state.
     *
     * <p>The {@code NOT IN ('COMPLETED', 'CANCELLED')} guard is what stops a
     * late progress event from resurrecting a cancelled order. It cannot trigger
     * in this demo, since causality rules it out — but "cannot happen given the
     * current event graph" is a property of today's graph, and someone will add
     * an event to it.
     */
    private static SagaStep advanceTo(String status) {
        return (tx, event) -> {
            try (PreparedStatement ps = tx.prepareStatement("""
                    UPDATE saga_order SET status = ?, updated_at = now()
                     WHERE ref = ? AND status NOT IN ('COMPLETED', 'CANCELLED')
                    """)) {
                ps.setString(1, status);
                ps.setString(2, event.ref());
                ps.executeUpdate();
            }
            return List.of();
        };
    }

    private static SagaStep complete() {
        return (tx, event) -> {
            try (PreparedStatement ps = tx.prepareStatement("""
                    UPDATE saga_order SET status = 'COMPLETED', updated_at = now()
                     WHERE ref = ? AND status <> 'CANCELLED'
                    """)) {
                ps.setString(1, event.ref());
                ps.executeUpdate();
            }
            log.info("order COMPLETED {}", event.ref());
            return List.of();
        };
    }

    private static SagaStep cancel() {
        return (tx, event) -> {
            String reason = event.reason() == null ? "compensated" : event.reason();
            try (PreparedStatement ps = tx.prepareStatement("""
                    UPDATE saga_order SET status = 'CANCELLED', cancel_reason = ?, updated_at = now()
                     WHERE ref = ? AND status <> 'COMPLETED'
                    """)) {
                ps.setString(1, reason);
                ps.setString(2, event.ref());
                ps.executeUpdate();
            }
            log.info("order CANCELLED {} — {}", event.ref(), reason);
            return List.of();
        };
    }
}
