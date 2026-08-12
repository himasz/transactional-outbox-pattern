package de.ebrahim.outbox.transport;

import java.time.Duration;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * The low-latency path from "transaction committed" to "relay wakes up".
 *
 * <p>LISTEN/NOTIFY is excluded by the brief because it pins a database
 * connection for the lifetime of the listener. Instead the nudge travels over
 * the broker we already run. That choice matters beyond latency: because the
 * signal is a network message rather than an in-process channel, the relay can
 * be moved out of the writing service into its own deployment without changing
 * a line of this code.
 *
 * <p>The signal is a hint, never a guarantee. Losing one costs latency (the
 * relay still picks the message up on its next idle tick) but never
 * correctness, which is why fire-and-forget core NATS is the right transport.
 */
public interface WakeupSignal extends AutoCloseable {

    /** Called after a successful commit. Must not throw or block. */
    void signal();

    /**
     * Blocks until a nudge arrives or the timeout elapses.
     *
     * @return true if woken by a nudge, false if the timeout fired
     */
    boolean await(Duration timeout) throws InterruptedException;

    @Override
    default void close() { }

    /** In-process signal: useful for tests and for single-process deployments. */
    final class Local implements WakeupSignal {
        // Capacity 1 gives free coalescing: a burst of commits during one drain
        // collapses into a single wake-up instead of queueing redundant work.
        private final BlockingQueue<Object> latch = new ArrayBlockingQueue<>(1);

        @Override public void signal() { latch.offer(Boolean.TRUE); }

        @Override public boolean await(Duration timeout) throws InterruptedException {
            return latch.poll(timeout.toMillis(), TimeUnit.MILLISECONDS) != null;
        }
    }

    /** Disables nudging entirely; the relay falls back to pure polling. */
    final class Noop implements WakeupSignal {
        @Override public void signal() { }
        @Override public boolean await(Duration timeout) throws InterruptedException {
            Thread.sleep(timeout.toMillis());
            return false;
        }
    }
}
