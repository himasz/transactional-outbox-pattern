package de.ebrahim.inbox;

import de.ebrahim.inbox.transport.MessageSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

/**
 * The receive loop: pull from the broker, hand to the inbox, acknowledge
 * according to the outcome.
 *
 * <p>Thin on purpose. The interesting decisions all live in {@link Inbox} and
 * {@link InboxGuard}, and this class is only here so that every consumer does
 * not rewrite the same six lines and get the acknowledgement rule subtly wrong.
 *
 * <p>The acknowledgement rule, in full: acknowledge unless the outcome was
 * {@link InboxResult#RETRY}. A duplicate is acknowledged because the work is
 * already done; a parked message is acknowledged because redelivering it
 * forever would block the queue behind a message that has already proven it
 * cannot be handled, and the {@code DEAD} row is the durable record that it
 * happened.
 */
public final class InboxConsumer implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(InboxConsumer.class);

    @FunctionalInterface
    private interface Step {
        InboxResult apply(InboxMessage message) throws Exception;
    }

    private final MessageSource source;
    private final Step step;
    private final String name;
    private final Duration pollTimeout;

    private volatile boolean running;
    private volatile Thread thread;

    private InboxConsumer(MessageSource source, Step step, String name, Duration pollTimeout) {
        this.source = source;
        this.step = step;
        this.name = name;
        this.pollTimeout = pollTimeout;
    }

    /**
     * Inline mode: the handler runs before the broker is acknowledged, so an ack
     * means the work is committed.
     */
    public static InboxConsumer inline(MessageSource source, Inbox inbox, InboxHandler handler) {
        return new InboxConsumer(source,
                message -> inbox.process(message, handler),
                inbox.config().consumer(),
                Duration.ofSeconds(1));
    }

    /**
     * Staging mode: the message is made durable and acknowledged immediately.
     * Pair this with a running {@link InboxProcessor} on the same consumer name,
     * or messages will pile up in the table and nothing will ever handle them.
     */
    public static InboxConsumer staging(MessageSource source, Inbox inbox) {
        return new InboxConsumer(source,
                inbox::stage,
                inbox.config().consumer(),
                Duration.ofSeconds(1));
    }

    public void start() {
        if (running) return;
        running = true;
        thread = Thread.ofVirtual().name("inbox-consumer-" + name).start(this::run);
    }

    private void run() {
        log.info("inbox consumer {} started", name);
        while (running) {
            try {
                MessageSource.Delivery delivery = source.next(pollTimeout);
                if (delivery == null) continue;
                dispatch(delivery);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                if (!running) break;
                log.error("consumer {} loop failed, backing off", name, e);
                sleep(1000);
            }
        }
        log.info("inbox consumer {} stopped", name);
    }

    /** Package-private so tests can drive one message through without a thread. */
    InboxResult dispatch(MessageSource.Delivery delivery) throws Exception {
        InboxResult result = step.apply(delivery.message());
        if (result.shouldAcknowledge()) {
            // Acknowledging can fail — a broken connection, an expired ack-wait.
            // That is survivable precisely because the claim is already durable:
            // the redelivery that follows is suppressed as a duplicate. This is
            // the crash-after-commit-before-ack window, arriving as an exception
            // rather than as a process death.
            delivery.acknowledge();
        } else {
            delivery.retryLater();
        }
        return result;
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void close() throws Exception {
        running = false;
        Thread t = thread;
        if (t != null) t.interrupt();
        source.close();
    }
}
