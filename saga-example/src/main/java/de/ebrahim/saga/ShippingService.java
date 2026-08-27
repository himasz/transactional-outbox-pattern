package de.ebrahim.saga;

import de.ebrahim.outbox.OutboxMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.List;

/**
 * The last forward step, and the one whose failure is most expensive.
 *
 * <p>By the time an order reaches shipping, money is held and stock is
 * reserved. A refusal here therefore has to unwind two services, in order,
 * through {@link SagaSubjects#SHIPPING_FAILED}. That cascade is the reason this
 * service exists in the demo at all: a saga whose compensations are all
 * single-step never has to get the unwinding order right.
 */
public final class ShippingService {

    private static final Logger log = LoggerFactory.getLogger(ShippingService.class);

    /** Deterministic refusal, so the cascade is reproducible rather than occasional. */
    static final String EMBARGOED_DESTINATION = "EMBARGOED";

    private ShippingService() { }

    public static SagaParticipant register(SagaParticipant participant) {
        return participant.on(SagaSubjects.INVENTORY_RESERVED, ShippingService::dispatch);
    }

    private static List<OutboxMessage> dispatch(Connection tx, SagaEvent event) throws Exception {
        // Off the event, not out of another service's table. The destination has
        // been carried forward from order.created through payment and inventory
        // precisely so this service needs nothing but its own tables.
        String destination = event.destination() == null ? "UNKNOWN" : event.destination();

        if (EMBARGOED_DESTINATION.equals(destination)) {
            record(tx, event.ref(), destination, "REFUSED");
            log.info("shipping REFUSED {} — cannot deliver to {}", event.ref(), destination);
            return List.of(SagaEvent.of(event.ref())
                    .withReason("cannot ship to " + destination)
                    .toMessage(SagaSubjects.SHIPPING_FAILED));
        }

        record(tx, event.ref(), destination, "DISPATCHED");
        log.info("shipping DISPATCHED {} to {}", event.ref(), destination);
        return List.of(SagaEvent.of(event.ref()).toMessage(SagaSubjects.ORDER_SHIPPED));
    }

    private static void record(Connection tx, String ref, String destination, String status) throws Exception {
        try (PreparedStatement ps = tx.prepareStatement("""
                INSERT INTO saga_shipment (ref, destination, status) VALUES (?, ?, ?)
                ON CONFLICT (ref) DO NOTHING
                """)) {
            ps.setString(1, ref);
            ps.setString(2, destination);
            ps.setString(3, status);
            ps.executeUpdate();
        }
    }
}
