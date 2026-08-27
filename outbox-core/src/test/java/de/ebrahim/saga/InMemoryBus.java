package de.ebrahim.saga;

import de.ebrahim.inbox.InboxMessage;
import de.ebrahim.inbox.transport.MessageSource;
import de.ebrahim.outbox.store.OutboxStore;
import de.ebrahim.outbox.transport.MessagePublisher;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A pub/sub broker in twenty lines, standing in for NATS.
 *
 * <p>It is a {@link MessagePublisher} on the relay's side and hands out a
 * {@link MessageSource} per subscriber on the participants' side, so the whole
 * saga runs end to end — outbox row, relay, broker, inbox claim, next outbox row
 * — against nothing but PostgreSQL. No Docker, no broker, no sleeping.
 *
 * <p>{@link #withDuplicates} makes every message arrive twice. That is not a
 * quirk of the fake: at-least-once is what the real relay guarantees, and
 * turning it up to "always" is the cheapest way to prove the saga does not care.
 * A broker that happened to deliver cleanly during a test run would prove
 * nothing about the day it does not.
 *
 * <p>The message id is taken straight from the outbox row, which is exactly what
 * {@code NatsPublisher} puts in the {@code Nats-Msg-Id} header and what
 * {@code NatsMessageSource} reads back out. The fake shortcut and the real path
 * carry the same value, so the deduplication being exercised here is the
 * deduplication that runs in production.
 */
final class InMemoryBus implements MessagePublisher {

    private final List<Subscriber> subscribers = Collections.synchronizedList(new ArrayList<>());
    private final AtomicInteger published = new AtomicInteger();
    private int deliveriesPerMessage = 1;

    /** Every message is delivered twice, to every subscriber. */
    InMemoryBus withDuplicates() {
        this.deliveriesPerMessage = 2;
        return this;
    }

    Subscriber subscribe(String name) {
        Subscriber subscriber = new Subscriber(name);
        subscribers.add(subscriber);
        return subscriber;
    }

    @Override
    public void publish(OutboxStore.Row row) {
        published.incrementAndGet();
        InboxMessage message = new InboxMessage(row.messageId(), row.subject(), row.headers(), row.payload());
        synchronized (subscribers) {
            for (Subscriber subscriber : subscribers) {
                for (int i = 0; i < deliveriesPerMessage; i++) {
                    subscriber.queue.add(message);
                }
            }
        }
    }

    int publishedCount() {
        return published.get();
    }

    boolean idle() {
        synchronized (subscribers) {
            return subscribers.stream().allMatch(s -> s.queue.isEmpty());
        }
    }

    /** One subscriber's mailbox, exposed to a participant as its {@code MessageSource}. */
    static final class Subscriber implements MessageSource {

        private final String name;
        private final ConcurrentLinkedQueue<InboxMessage> queue = new ConcurrentLinkedQueue<>();
        final AtomicInteger acknowledged = new AtomicInteger();
        final AtomicInteger retried = new AtomicInteger();

        Subscriber(String name) {
            this.name = name;
        }

        @Override
        public Delivery next(Duration timeout) {
            InboxMessage message = queue.poll();
            if (message == null) return null;
            return new Delivery() {
                @Override public InboxMessage message() { return message; }
                @Override public void acknowledge() { acknowledged.incrementAndGet(); }
                @Override public void retryLater() {
                    retried.incrementAndGet();
                    // Back of the queue, exactly as a nak with delay behaves.
                    queue.add(message);
                }
            };
        }

        @Override
        public String toString() {
            return "subscriber(" + name + ")";
        }
    }
}
