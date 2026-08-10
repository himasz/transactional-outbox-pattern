package de.ebrahim.outbox;

import io.nats.client.Connection;
import io.nats.client.Dispatcher;

import java.time.Duration;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Cross-process wake-up over a core NATS subject.
 *
 * <p>Deliberately core NATS and not JetStream: the nudge carries no information
 * beyond "something may be waiting", so persisting it would be pure cost. An
 * empty-payload publish is effectively free and reaches the relay in well under
 * a millisecond on a local cluster.
 */
public final class NatsWakeupSignal implements WakeupSignal {

    private static final byte[] EMPTY = new byte[0];

    private final Connection nats;
    private final String subject;
    private final BlockingQueue<Object> latch = new ArrayBlockingQueue<>(1);
    private final Dispatcher dispatcher;

    public NatsWakeupSignal(Connection nats, String subject) {
        this.nats = nats;
        this.subject = subject;
        // Every replica subscribes, including the writers. Followers simply never
        // call await(), so their subscription costs one idle callback per nudge.
        this.dispatcher = nats.createDispatcher(msg -> latch.offer(Boolean.TRUE));
        this.dispatcher.subscribe(subject);
    }

    @Override
    public void signal() {
        nats.publish(subject, EMPTY);
    }

    @Override
    public boolean await(Duration timeout) throws InterruptedException {
        return latch.poll(timeout.toMillis(), TimeUnit.MILLISECONDS) != null;
    }

    @Override
    public void close() {
        dispatcher.unsubscribe(subject);
    }
}
