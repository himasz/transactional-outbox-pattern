package de.ebrahim.outbox.election;

import java.util.Optional;

/**
 * Mutual exclusion for the relay.
 *
 * <p>Election is not decoration here — it is what buys FIFO. The fencing token
 * on the cursor keeps a stale leader from corrupting the watermark, but it
 * cannot un-publish: if two relays are live at once they interleave writes into
 * the broker and ordering is broken before any cursor update happens. Exactly
 * one active publisher is therefore a hard requirement, and this interface is
 * how the library gets one.
 *
 * <p>The shape is deliberate. A {@code boolean isLeader()} would imply
 * leadership is a stable property you can check once; every real
 * implementation is lease-based, and leases expire mid-work during a partition
 * or a long GC pause. A revocable {@link Lease} forces callers to re-check, and
 * the {@linkplain Lease#fencingToken() fencing token} gives the database a way
 * to reject a leader that has not noticed it was replaced.
 */
public interface LeaderElector extends AutoCloseable {

    /**
     * Acquires leadership, or renews it if already held.
     *
     * <p>Called on every relay iteration, so implementations should cache and
     * only touch the network when a renewal is actually due.
     *
     * @return the lease if this instance leads, empty if another instance does
     */
    Optional<Lease> tryAcquire();

    @Override
    default void close() { }

    /** A revocable claim to leadership. */
    interface Lease {

        /**
         * Strictly increasing across leadership handovers. Every write the relay
         * makes is conditional on this value, so a leader that has been replaced
         * fails its own guard instead of corrupting state.
         */
        long fencingToken();

        /**
         * Whether the lease is still safely held. Checked between individual
         * publishes so a relay that loses leadership mid-batch stops promptly
         * rather than at the end of the batch.
         */
        boolean isValid();
    }
}
