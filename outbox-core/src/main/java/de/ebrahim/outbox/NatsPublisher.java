package de.ebrahim.outbox;

import io.nats.client.JetStream;
import io.nats.client.api.PublishAck;
import io.nats.client.impl.Headers;
import io.nats.client.impl.NatsMessage;

/**
 * JetStream publisher.
 *
 * <p>Publishes strictly one at a time and waits for each acknowledgement. The
 * async API would be faster but lets acks complete out of order, which destroys
 * both the FIFO guarantee and the relay's ability to know where the stream
 * actually stopped after a partial failure. Throughput is traded for ordering
 * deliberately; per-partition FIFO is the documented way to buy it back.
 *
 * <p>Every message carries {@code Nats-Msg-Id}, so redeliveries within
 * JetStream's duplicate window are collapsed by the broker. That is an
 * optimisation, not a guarantee: delivery remains at-least-once and consumers
 * must be idempotent.
 */
public final class NatsPublisher implements MessagePublisher {

    public static final String MSG_ID_HEADER = "Nats-Msg-Id";
    public static final String OUTBOX_ID_HEADER = "Outbox-Id";

    private final JetStream jetStream;

    public NatsPublisher(JetStream jetStream) {
        this.jetStream = jetStream;
    }

    @Override
    public void publish(OutboxStore.Row row) throws Exception {
        Headers headers = new Headers();
        headers.add(MSG_ID_HEADER, row.messageId().toString());
        // Exposed so consumers (and the example) can verify ordering end to end.
        headers.add(OUTBOX_ID_HEADER, Long.toString(row.id()));
        row.headers().forEach(headers::add);

        PublishAck ack = jetStream.publish(NatsMessage.builder()
                .subject(row.subject())
                .headers(headers)
                .data(row.payload())
                .build());

        if (ack.isDuplicate()) {
            // Expected after a crash between publish and cursor advance. The
            // broker already has this message; treat it as published.
            return;
        }
        if (ack.hasError()) {
            throw new IllegalStateException("JetStream rejected message " + row.id() + ": " + ack.getError());
        }
    }
}
