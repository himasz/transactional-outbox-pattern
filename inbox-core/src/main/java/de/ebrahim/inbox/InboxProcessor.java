package de.ebrahim.inbox;

import de.ebrahim.inbox.store.InboxStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Optional;

/**
 * Drains staged messages. The consuming side's answer to {@code RelayEngine},
 * and instructive mostly for how much smaller it is.
 *
 * <p>The relay needs a leader elector, a fencing token, and a guarded cursor
 * write, because it advances <em>one shared watermark</em> and a leader that has
 * not noticed it was replaced can corrupt it. This class needs none of that,
 * because the inbox has no watermark: progress is recorded on each row, so a
 * second processor is redundant rather than dangerous. In ordered mode it simply
 * blocks on the head of the queue and finds the work already done when it wakes.
 *
 * <p>That difference is not an accident of implementation. Shared mutable
 * position is what forces consensus; per-row state does not need it.
 *
 * <h2>Head-of-line blocking</h2>
 *
 * <p>In ordered mode a message that keeps failing stalls everything behind it,
 * exactly as it does in the relay — skipping it would break the ordering the
 * relay worked so hard to produce. Unlike the relay, this class resolves the
 * stall itself: after {@link InboxConfig#maxAttempts()} the message is parked as
 * {@code DEAD}, stops matching the pending query, and the queue moves on. That
 * is the bounded-retry-then-park policy the outbox README leaves as an exercise;
 * the alternative, halting and alerting, is what you get by setting
 * {@code maxAttempts} very high.
 */
public final class InboxProcessor implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(InboxProcessor.class);

    private final DataSource dataSource;
    private final InboxStore store;
    private final InboxHandler handler;
    private final InboxConfig config;

    private volatile boolean running;
    private volatile boolean closed;
    private volatile Thread thread;
    private long nextPurgeNanos;

    public InboxProcessor(DataSource dataSource, InboxConfig config, InboxHandler handler) {
        this.dataSource = dataSource;
        this.config = config;
        this.store = new InboxStore(dataSource, config.consumer());
        this.handler = handler;
    }

    public void start() {
        if (running) return;
        running = true;
        // Virtual thread, for the same reason the relay uses one: the loop is
        // sequential by design and spends its life blocked on I/O.
        thread = Thread.ofVirtual().name("inbox-processor-" + config.consumer()).start(this::run);
    }

    private void run() {
        log.info("inbox processor started for consumer {}", config.consumer());
        while (running) {
            try {
                boolean didWork = tick();
                if (!didWork) sleep(config.pollInterval().toMillis());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                log.error("inbox tick failed, backing off", e);
                sleep(config.errorBackoff().toMillis());
            }
        }
        log.info("inbox processor stopped for consumer {}", config.consumer());
    }

    /**
     * One pass over the queue. Package-private return value so tests can drive
     * the loop synchronously rather than racing a background thread.
     *
     * @return true if at least one message was handled
     */
    public boolean tick() throws Exception {
        boolean didWork = false;
        while (!closed) {
            Outcome outcome = handleNext();
            if (outcome == Outcome.EMPTY) break;
            if (outcome == Outcome.HANDLED) {
                didWork = true;
                continue;
            }
            // FAILED. In ordered mode nothing behind this message can be touched
            // until it succeeds or is parked, so hammering it in a tight loop
            // would just burn the attempt budget in milliseconds. Yield and let
            // the poll interval space the retries out.
            didWork = true;
            break;
        }
        maybePurge();
        return didWork;
    }

    private enum Outcome { HANDLED, FAILED, EMPTY }

    private Outcome handleNext() throws SQLException {
        InboxMessage message = null;
        try (Connection tx = dataSource.getConnection()) {
            tx.setAutoCommit(false);
            try {
                Optional<InboxStore.Staged> next = store.nextPending(tx, config.ordered());
                if (next.isEmpty()) {
                    tx.rollback();
                    return Outcome.EMPTY;
                }
                InboxStore.Staged staged = next.get();
                message = staged.toMessage();

                // The row is locked for the rest of this transaction, so the
                // handler's writes and the completion marker commit as one unit.
                // Same single-resource argument as the inline path; the only
                // difference is that the message came from the table instead of
                // from the wire.
                handler.handle(tx, message);
                store.markDone(tx, staged.messageId());
                tx.commit();
                return Outcome.HANDLED;
            } catch (Exception failure) {
                try {
                    tx.rollback();
                } catch (SQLException rollbackFailure) {
                    failure.addSuppressed(rollbackFailure);
                }
                if (message == null) throw asSqlException(failure);
                recordFailure(message, failure);
                return Outcome.FAILED;
            }
        }
    }

    private void recordFailure(InboxMessage message, Exception failure) throws SQLException {
        InboxResult result = store.recordFailure(message, config.maxAttempts(), failure);
        if (result == InboxResult.PARKED) {
            log.error("staged message {} parked as DEAD after {} attempts; the queue can now advance",
                    message.messageId(), config.maxAttempts(), failure);
        } else {
            log.warn("staged message {} failed, will retry", message.messageId(), failure);
        }
    }

    private void maybePurge() {
        long now = System.nanoTime();
        if (now < nextPurgeNanos) return;
        nextPurgeNanos = now + config.purgeInterval().toNanos();
        try {
            int deleted = store.purgeProcessed(config.retention().toSeconds());
            if (deleted > 0) log.debug("purged {} processed inbox rows", deleted);
        } catch (Exception e) {
            log.warn("inbox retention sweep failed; will retry", e);
        }
    }

    private static SQLException asSqlException(Exception e) {
        return e instanceof SQLException sql ? sql : new SQLException(e);
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void close() {
        running = false;
        closed = true;
        Thread t = thread;
        if (t != null) t.interrupt();
    }
}
