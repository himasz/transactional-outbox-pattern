package de.ebrahim.outbox;

import de.ebrahim.outbox.store.OutboxWriter;
import de.ebrahim.outbox.transport.WakeupSignal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * Entry point for the write side.
 *
 * <p>Two ways to use it, both correct:
 * <ol>
 *   <li>{@link #inTransaction(Work)} — the library manages the transaction
 *       boundary and fires the wake-up nudge at exactly the right moment;</li>
 *   <li>{@link #writer()} plus your own transaction (Spring, Quarkus, raw JDBC)
 *       — then you are responsible for calling {@link #nudge()} after commit
 *       returns, e.g. from {@code TransactionSynchronization.afterCommit()} in
 *       Spring or an interposed {@code Synchronization} in Quarkus.</li>
 * </ol>
 */
public final class Outbox implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(Outbox.class);
    private final DataSource dataSource;
    private final OutboxWriter writer = new OutboxWriter();
    private final WakeupSignal wakeup;

    public Outbox(DataSource dataSource, WakeupSignal wakeup) {
        this.dataSource = dataSource;
        this.wakeup = wakeup;
    }

    /**
     * A unit of work that may both mutate business tables and enqueue messages.
     */
    @FunctionalInterface
    public interface Work<T> {
        T run(Connection tx, OutboxWriter outbox) throws Exception;
    }

    /**
     * Runs {@code work} in a single transaction and, only if it commits, nudges
     * the relay.
     *
     * <p>The ordering here is the requirement, not an implementation detail. The
     * nudge sits after {@code commit()} has <em>returned</em>, and deliberately
     * not in a {@code finally} block: on rollback the outbox row is gone, so a
     * nudge would at best be wasted work and at worst suggest the message
     * survived the rollback.
     */
    public <T> T inTransaction(Work<T> work) throws Exception {
        T result;
        try (Connection tx = dataSource.getConnection()) {
            tx.setAutoCommit(false);
            try {
                result = work.run(tx, writer);
                tx.commit();
            } catch (Exception failure) {
                try {
                    tx.rollback();
                } catch (SQLException rollbackFailure) {
                    failure.addSuppressed(rollbackFailure);
                }
                throw failure;
            }
        }
        // Reached only when commit() returned without throwing.
        try {
            wakeup.signal();
        } catch (Exception e) {
            log.warn("outbox commit succeeded, but relay nudge failed", e);
        }
        return result;
    }

    /**
     * The raw writer, for callers that own their own transaction boundary.
     */
    public OutboxWriter writer() {
        return writer;
    }

    /**
     * Call after your own commit succeeds when not using {@link #inTransaction}.
     */
    public void nudge() {
        wakeup.signal();
    }

    @Override
    public void close() throws Exception {
        wakeup.close();
    }
}
