package de.ebrahim.inbox;

import de.ebrahim.inbox.store.InboxGuard;
import de.ebrahim.inbox.store.InboxStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Optional;

/**
 * Entry point for the consuming side — the counterpart to {@code Outbox}.
 *
 * <p>The outbox makes delivery at-least-once on purpose: the relay publishes,
 * then advances its cursor, and a crash between the two replays the batch. That
 * is not a defect to be engineered away, it is the price of not having a
 * distributed transaction, and it is only safe because something downstream
 * turns it back into exactly-once. This class is that something.
 *
 * <h2>Two modes, one guarantee</h2>
 *
 * <p>Both modes give exactly-once <em>effects</em>. They differ in when the
 * broker is acknowledged, and therefore in what a crash costs.
 *
 * <table border="1">
 *   <caption>Choosing a mode</caption>
 *   <tr><th></th><th>{@link #process}, inline</th><th>{@link #stage} + {@link InboxProcessor}</th></tr>
 *   <tr><td>Broker acked</td><td>after the work is committed</td><td>as soon as the message is durable</td></tr>
 *   <tr><td>Ack means</td><td>handled</td><td>received</td></tr>
 *   <tr><td>Retries driven by</td><td>the broker, by redelivery</td><td>the database, by re-reading the row</td></tr>
 *   <tr><td>Costs</td><td>slow handlers hold the broker's ack-wait open</td><td>a second hop, and a queue to watch</td></tr>
 * </table>
 *
 * <p><b>Inline is the default and the one to reach for.</b> It has fewer moving
 * parts, no second queue to monitor, and an acknowledgement that means what
 * everyone assumes it means. Reach for staging when the handler is slow or
 * failure-prone enough that holding the broker's ack-wait open is a problem, or
 * when you want redelivery decisions made by your own database rather than by
 * the broker's consumer configuration.
 *
 * <p>Mixing modes for one consumer name is safe — the deduplication key does not
 * care which path wrote the row — but there is rarely a reason to.
 *
 * <h2>What is not covered</h2>
 *
 * <p>Exactly-once <em>effects</em>, not exactly-once <em>execution</em>. The
 * handler body may run more than once; what cannot happen twice is a committed
 * result. Anything the handler does that a rollback cannot undo — sending mail,
 * charging a card, writing to a second datastore — sits outside the transaction
 * and outside the guarantee. Enqueue those into an outbox on the same
 * connection instead.
 */
public final class Inbox implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(Inbox.class);

    private final DataSource dataSource;
    private final InboxConfig config;
    private final InboxStore store;
    private final InboxGuard guard = new InboxGuard();

    public Inbox(DataSource dataSource, InboxConfig config) {
        this.dataSource = dataSource;
        this.config = config;
        this.store = new InboxStore(dataSource, config.consumer());
    }

    /**
     * Inline mode: claim, handle, and commit as one transaction.
     *
     * <p>The ordering is the guarantee, so it is worth stating as a sequence of
     * crash points. Suppose the process dies:
     *
     * <ul>
     *   <li><b>before the commit</b> — the claim and every effect roll back
     *       together. The broker never saw an ack, redelivers, and the work
     *       happens once.</li>
     *   <li><b>after the commit, before the ack</b> — the effects are durable
     *       but the broker still thinks the message is outstanding, so it
     *       redelivers. The claim finds a {@code DONE} row, returns
     *       {@link InboxResult#DUPLICATE}, and the work does not happen again.
     *       <em>This window is the reason the inbox exists</em>; it is not rare,
     *       and no amount of care in the handler closes it.</li>
     *   <li><b>after the ack</b> — nothing to do.</li>
     * </ul>
     *
     * <p>There is no fourth case, because there is no point at which the effects
     * exist without the claim, or the claim without the effects.
     *
     * <p>Note that this method does not rethrow a handler failure: a failed
     * message is an outcome, not an exception, and callers need the
     * {@link InboxResult} to decide whether to acknowledge. The cause is logged
     * and stored in the row's {@code last_error}.
     *
     * @return what happened, and by extension whether to acknowledge the broker
     */
    public InboxResult process(InboxMessage message, InboxHandler handler) throws SQLException {
        try (Connection tx = dataSource.getConnection()) {
            tx.setAutoCommit(false);
            try {
                // Blocks if another replica is mid-handler on this same message.
                // That is deliberate: whoever loses the race must not proceed
                // until it can see the winner's outcome. Long handlers therefore
                // hold a competing replica's connection, which is one more reason
                // to keep handlers short.
                if (!guard.claim(tx, config.consumer(), message)) {
                    tx.rollback();
                    return alreadySeen(message);
                }
                handler.handle(tx, message);
                tx.commit();
                return InboxResult.PROCESSED;
            } catch (Exception failure) {
                rollbackQuietly(tx, failure);
                return afterFailure(message, failure);
            }
        }
    }

    /**
     * Staging mode: make the message durable and return, so the caller can
     * acknowledge the broker immediately. {@link InboxProcessor} does the work
     * later.
     *
     * @return {@link InboxResult#PROCESSED} if newly staged, {@link InboxResult#DUPLICATE}
     *         if this consumer has already seen it. Both mean "acknowledge".
     */
    public InboxResult stage(InboxMessage message) throws SQLException {
        return store.stage(message) ? InboxResult.PROCESSED : InboxResult.DUPLICATE;
    }

    /**
     * The raw claim, for callers who own their own transaction boundary — Spring's
     * {@code @Transactional}, Quarkus, or hand-rolled JDBC.
     *
     * <p>The contract is the same one {@code OutboxWriter} has on the producing
     * side: pass the connection your business writes are going through, and
     * nothing else. Claiming on a second connection is a second transaction, and
     * a second transaction commits independently — so a later failure leaves the
     * message marked handled with none of its effects applied, and the
     * redelivery is then correctly suppressed. That is the one failure mode this
     * whole design exists to prevent, and it is invisible to the library:
     * JDBC offers no portable way to ask whether two connections share a
     * transaction.
     */
    public InboxGuard guard() {
        return guard;
    }

    public InboxStore store() {
        return store;
    }

    public InboxConfig config() {
        return config;
    }

    /** Deletes DONE rows past the retention window. See {@link InboxConfig#retention()} first. */
    public int purge() throws SQLException {
        return store.purgeProcessed(config.retention().toSeconds());
    }

    /** Messages received but not yet handled. The metric to alert on. */
    public long pendingDepth() throws SQLException {
        return store.pendingDepth();
    }

    private InboxResult alreadySeen(InboxMessage message) throws SQLException {
        Optional<String> status = store.statusOf(message.messageId());
        if (status.filter("DEAD"::equals).isPresent()) {
            log.warn("message {} is parked as DEAD and will not be reprocessed", message.messageId());
            return InboxResult.PARKED;
        }
        log.debug("duplicate {} suppressed", message.messageId());
        return InboxResult.DUPLICATE;
    }

    private InboxResult afterFailure(InboxMessage message, Exception failure) {
        try {
            InboxResult result = store.recordFailure(message, config.maxAttempts(), failure);
            if (result == InboxResult.PARKED) {
                log.error("message {} parked as DEAD after {} attempts",
                        message.messageId(), config.maxAttempts(), failure);
            } else {
                log.warn("handler failed for {}, will retry", message.messageId(), failure);
            }
            return result;
        } catch (SQLException bookkeeping) {
            // The attempt counter is lost, so this message could in principle be
            // retried forever. Redelivering is still the safer choice than
            // acknowledging work that did not happen; the broker's own
            // max_deliver is the backstop for a database that stays unreachable.
            bookkeeping.addSuppressed(failure);
            log.error("could not record failure for {}; retrying anyway", message.messageId(), bookkeeping);
            return InboxResult.RETRY;
        }
    }

    private static void rollbackQuietly(Connection tx, Exception failure) {
        try {
            tx.rollback();
        } catch (SQLException rollbackFailure) {
            failure.addSuppressed(rollbackFailure);
        }
    }

    @Override
    public void close() {
        // Nothing owned: the DataSource belongs to the caller, exactly as the
        // outbox leaves the caller's DataSource alone. Present so try-with-
        // resources reads the same on both sides.
    }
}
