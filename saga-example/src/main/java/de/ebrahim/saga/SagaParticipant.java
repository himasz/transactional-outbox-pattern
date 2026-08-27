package de.ebrahim.saga;

import de.ebrahim.inbox.Inbox;
import de.ebrahim.inbox.InboxConfig;
import de.ebrahim.inbox.InboxMessage;
import de.ebrahim.inbox.InboxResult;
import de.ebrahim.inbox.transport.MessageSource;
import de.ebrahim.outbox.OutboxMessage;
import de.ebrahim.outbox.store.OutboxWriter;
import de.ebrahim.outbox.transport.WakeupSignal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A saga participant: a service that reacts to events by changing its own state
 * and emitting the next events.
 *
 * <p><b>This class is the entire "saga framework", and its size is the lesson.</b>
 * Choreography has no coordinator, no state machine and no workflow definition —
 * so once you already have an inbox and an outbox, what remains is a loop that
 * claims an event, calls a handler, and enqueues whatever the handler returns.
 * The interesting engineering all happened in the two libraries underneath;
 * nothing here does more than compose them.
 *
 * <p>The one line that matters is in {@link #handle}: the handler's writes and
 * its output events go through the <em>same connection</em> the inbox used to
 * claim the input. That single fact removes every partial-failure state a saga
 * step can otherwise land in:
 *
 * <table border="1">
 *   <caption>What one transaction buys</caption>
 *   <tr><th>Without it</th><th>With it</th></tr>
 *   <tr><td>State changed, next event lost → saga stalls forever, silently</td><td>Impossible</td></tr>
 *   <tr><td>Event emitted, state change rolled back → downstream acts on a fiction</td><td>Impossible</td></tr>
 *   <tr><td>Redelivery re-runs the step → double charge, double shipment</td><td>Suppressed by the claim</td></tr>
 * </table>
 *
 * <h2>Why this runs its own loop instead of using {@code InboxConsumer}</h2>
 *
 * <p>The relay has to be nudged after the step commits, and only if it committed.
 * {@code InboxConsumer} owns the transaction boundary and exposes no
 * after-commit hook, so it cannot fire the nudge at the right instant. Ten lines
 * of loop here buys the latency back — and losing a nudge costs latency, never
 * correctness, since the relay's idle tick picks the rows up regardless.
 *
 * <h2>Ordering</h2>
 *
 * <p>Participants deliberately run with {@code ordered = false}. A choreographed
 * saga does not need FIFO: causality is enforced by the data, not by the
 * transport. {@code inventory.rejected} cannot overtake {@code order.created},
 * because it does not exist until a step that consumed {@code order.created} has
 * committed. Paying for global ordering here — a {@code max_ack_pending} of one
 * across the whole consumer group — would buy a guarantee the workload already
 * has for free.
 */
public final class SagaParticipant implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(SagaParticipant.class);

    private final String name;
    private final Inbox inbox;
    private final OutboxWriter outbox = new OutboxWriter();
    private final MessageSource source;
    private final WakeupSignal wakeup;
    private final Map<String, SagaStep> steps = new HashMap<>();

    private volatile boolean running;
    private volatile Thread thread;

    public SagaParticipant(String name, DataSource dataSource, MessageSource source, WakeupSignal wakeup) {
        this.name = name;
        // Each participant is its own logical consumer, so each deduplicates
        // independently. Two services seeing the same event is normal in
        // choreography, and the consumer half of the inbox key is what keeps the
        // first one to arrive from swallowing it for the second.
        this.inbox = new Inbox(dataSource, InboxConfig.forConsumer(name).withOrdered(false));
        this.source = source;
        this.wakeup = wakeup;
    }

    /** Registers the reaction to one subject. Unregistered subjects are ignored. */
    public SagaParticipant on(String subject, SagaStep step) {
        steps.put(subject, step);
        return this;
    }

    public void start() {
        if (running) return;
        running = true;
        thread = Thread.ofVirtual().name("saga-" + name).start(this::run);
    }

    private void run() {
        log.info("participant {} listening for {}", name, steps.keySet());
        while (running) {
            try {
                if (!pump(Duration.ofSeconds(1))) continue;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                if (!running) break;
                log.error("participant {} failed, backing off", name, e);
                sleep(500);
            }
        }
        log.info("participant {} stopped", name);
    }

    /**
     * Pulls at most one event and processes it. Package-visible and synchronous
     * so tests can drive the whole choreography deterministically instead of
     * racing background threads.
     *
     * @return true if an event was pulled
     */
    public boolean pump(Duration timeout) throws Exception {
        MessageSource.Delivery delivery = source.next(timeout);
        if (delivery == null) return false;

        InboxMessage message = delivery.message();
        SagaStep step = steps.get(message.subject());
        if (step == null) {
            // Not ours. Acknowledged without touching the inbox: recording a
            // dedup row for an event this service will never act on would fill
            // the table with noise and make its retention window meaningless.
            delivery.acknowledge();
            return true;
        }

        InboxResult result = inbox.process(message, (tx, msg) -> handle(tx, msg, step));

        if (result == InboxResult.PROCESSED) {
            // The step committed, so any events it produced are now durable rows
            // in the outbox. Wake the relay to publish them. Deliberately after
            // the commit and only on success: on failure there is nothing to
            // publish, and nudging would suggest otherwise.
            wakeup.signal();
        }
        if (result.shouldAcknowledge()) {
            delivery.acknowledge();
        } else {
            delivery.retryLater();
        }
        return true;
    }

    private void handle(java.sql.Connection tx, InboxMessage message, SagaStep step) throws Exception {
        SagaEvent event = SagaEvent.fromJson(message.payload());
        List<OutboxMessage> next = step.apply(tx, event);
        for (OutboxMessage out : next) {
            // Same connection, same transaction, same commit as the state change
            // the step just made. This is the whole design.
            outbox.enqueue(tx, out);
        }
        if (log.isDebugEnabled()) {
            log.debug("{} handled {} for {} -> {}", name, message.subject(), event.ref(),
                    next.stream().map(OutboxMessage::subject).toList());
        }
    }

    public String name() {
        return name;
    }

    public Inbox inbox() {
        return inbox;
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
