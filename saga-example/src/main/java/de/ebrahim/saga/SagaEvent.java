package de.ebrahim.saga;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.ebrahim.outbox.OutboxMessage;

/**
 * The payload every saga event carries.
 *
 * <p>One envelope for all nine event types rather than nine record classes.
 * That is a demo simplification and worth naming as one: in a real system each
 * event is its own contract with its own schema, and a shared envelope quietly
 * couples every service to every field. It is used here because the interesting
 * part of this module is the choreography, not the serialisation.
 *
 * <p>{@code ref} is the correlation id — the thing that makes a scattered set of
 * events recognisable as one saga. It also travels as the {@code Ref} header, so
 * it can be read without deserialising the body, which is what the existing
 * outbox demo already does.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record SagaEvent(
        String ref,
        Long amountCents,
        String sku,
        Integer quantity,
        String destination,
        String reason) {

    private static final ObjectMapper JSON = new ObjectMapper();

    public static SagaEvent of(String ref) {
        return new SagaEvent(ref, null, null, null, null, null);
    }

    public SagaEvent withReason(String value) {
        return new SagaEvent(ref, amountCents, sku, quantity, destination, value);
    }

    public SagaEvent withAmount(long cents) {
        return new SagaEvent(ref, cents, sku, quantity, destination, reason);
    }

    /**
     * Builds the outbox message for this event.
     *
     * <p>The message id is generated here and never regenerated. That matters
     * more than it looks: it is the key the receiving inbox deduplicates on, so
     * an event re-created on a retry — rather than re-read from the outbox —
     * would arrive as a genuinely new message and be applied a second time.
     * Events are constructed once, inside the transaction that decides they
     * happened.
     */
    public OutboxMessage toMessage(String subject) {
        try {
            return OutboxMessage.of(subject, JSON.writeValueAsString(this))
                    .withHeader("Content-Type", "application/json")
                    .withHeader("Ref", ref);
        } catch (Exception e) {
            throw new IllegalStateException("could not serialise " + subject + " for " + ref, e);
        }
    }

    public static SagaEvent fromJson(byte[] payload) {
        try {
            return JSON.readValue(payload, SagaEvent.class);
        } catch (Exception e) {
            throw new IllegalArgumentException("malformed saga event payload", e);
        }
    }
}
