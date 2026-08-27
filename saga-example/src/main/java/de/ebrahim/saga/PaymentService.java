package de.ebrahim.saga;

import de.ebrahim.outbox.OutboxMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.List;

/**
 * Holds and releases funds. The service with the most to lose from a duplicate,
 * and therefore the one worth reading first.
 *
 * <p>It appears in the saga three times: once forward, and twice as the
 * compensating step for two different downstream failures. That is why its
 * refund is written as a guarded state transition rather than as "step 4 of the
 * rollback" — it has no idea which failure sent it here, and does not need to.
 */
public final class PaymentService {

    private static final Logger log = LoggerFactory.getLogger(PaymentService.class);

    /** Anything above this is declined as insufficient funds. Deterministic on purpose. */
    static final long DECLINE_ABOVE_CENTS = 100_000;

    private PaymentService() { }

    public static SagaParticipant register(SagaParticipant participant) {
        return participant
                .on(SagaSubjects.ORDER_CREATED, PaymentService::authorize)
                // Both compensating entry points land on the same step. A refund
                // is a refund; the saga does not branch on how it got here.
                .on(SagaSubjects.INVENTORY_REJECTED, PaymentService::refund)
                .on(SagaSubjects.INVENTORY_RELEASED, PaymentService::refund);
    }

    private static List<OutboxMessage> authorize(Connection tx, SagaEvent event) throws Exception {
        long amount = event.amountCents() == null ? 0 : event.amountCents();

        if (amount > DECLINE_ABOVE_CENTS) {
            record(tx, event.ref(), amount, "DECLINED");
            log.info("payment DECLINED for {} ({} cents)", event.ref(), amount);
            return List.of(SagaEvent.of(event.ref())
                    .withReason("insufficient funds")
                    .toMessage(SagaSubjects.PAYMENT_DECLINED));
        }

        record(tx, event.ref(), amount, "AUTHORIZED");
        log.info("payment AUTHORIZED for {} ({} cents)", event.ref(), amount);
        // Event-carried state transfer: the incoming payload is forwarded whole,
        // because inventory needs the sku and quantity and shipping needs the
        // destination. Emitting a bare {ref} here would force both of them to
        // read the order service's table, which is how a set of services quietly
        // becomes a distributed monolith.
        return List.of(event.toMessage(SagaSubjects.PAYMENT_AUTHORIZED));
    }

    /**
     * The compensating step.
     *
     * <p>Guarded on {@code status = 'AUTHORIZED'} rather than trusting that it is
     * only ever called once. The inbox already guarantees that, so this can never
     * fire in this demo — it is here because a compensating action that is only
     * safe when its caller behaves is not a compensating action, and the guard
     * costs one clause.
     *
     * <p>Note what happens when the guard does not match: the step returns no
     * events and the saga branch ends. For a compensation that is the right
     * choice, because the only way to get here with a non-authorized payment is
     * that the money is already released.
     */
    private static List<OutboxMessage> refund(Connection tx, SagaEvent event) throws Exception {
        int updated;
        try (PreparedStatement ps = tx.prepareStatement("""
                UPDATE saga_payment SET status = 'REFUNDED', refunded_at = now()
                 WHERE ref = ? AND status = 'AUTHORIZED'
                """)) {
            ps.setString(1, event.ref());
            updated = ps.executeUpdate();
        }

        if (updated == 0) {
            log.warn("refund for {} found no authorized payment; nothing to release", event.ref());
            return List.of();
        }

        String reason = event.reason() == null ? "compensated" : event.reason();
        log.info("payment REFUNDED for {} ({})", event.ref(), reason);
        return List.of(SagaEvent.of(event.ref()).withReason(reason)
                .toMessage(SagaSubjects.PAYMENT_REFUNDED));
    }

    private static void record(Connection tx, String ref, long amount, String status) throws Exception {
        try (PreparedStatement ps = tx.prepareStatement("""
                INSERT INTO saga_payment (ref, amount_cents, status) VALUES (?, ?, ?)
                ON CONFLICT (ref) DO NOTHING
                """)) {
            ps.setString(1, ref);
            ps.setLong(2, amount);
            ps.setString(3, status);
            ps.executeUpdate();
        }
    }
}
