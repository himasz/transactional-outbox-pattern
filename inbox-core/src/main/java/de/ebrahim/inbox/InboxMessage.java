package de.ebrahim.inbox;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/**
 * A message as it arrived from the broker.
 *
 * <p>The mirror image of {@code OutboxMessage}, and deliberately not the same
 * class: the producing and consuming sides are separate deployments with
 * separate release cycles, and sharing a type across them would couple every
 * consumer's build to the producer's library version.
 *
 * <p>{@code messageId} is the deduplication key and the only field the inbox
 * treats as load-bearing. It must be the id the <em>producer</em> generated —
 * stable across every redelivery of the same logical message — which is exactly
 * what {@code OutboxMessage.messageId()} is, and what the relay sends as
 * {@code Nats-Msg-Id}. Broker-assigned identifiers are not usable: JetStream
 * hands out a fresh stream sequence when the relay republishes after a failover,
 * so deduplicating on it would let the duplicate through.
 *
 * <p>{@code payload} is a byte array, so the generated {@code equals}/
 * {@code hashCode} compare it by identity. Compare payloads explicitly if you
 * need value semantics.
 */
public record InboxMessage(UUID messageId, String subject, Map<String, String> headers, byte[] payload) {

    public InboxMessage {
        Objects.requireNonNull(messageId, "messageId");
        Objects.requireNonNull(subject, "subject");
        Objects.requireNonNull(payload, "payload");
        if (subject.isBlank()) throw new IllegalArgumentException("subject must not be blank");
        headers = Map.copyOf(Objects.requireNonNull(headers, "headers"));
    }

    public static InboxMessage of(UUID messageId, String subject, byte[] payload) {
        return new InboxMessage(messageId, subject, Map.of(), payload);
    }

    public static InboxMessage of(UUID messageId, String subject, String payload) {
        return of(messageId, subject, payload.getBytes(StandardCharsets.UTF_8));
    }

    public String payloadAsString() {
        return new String(payload, StandardCharsets.UTF_8);
    }

    public String header(String name) {
        return headers.get(name);
    }
}
