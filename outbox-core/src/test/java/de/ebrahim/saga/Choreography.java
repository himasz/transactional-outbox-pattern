package de.ebrahim.saga;

import de.ebrahim.outbox.store.OutboxStore;

import java.time.Duration;
import java.util.List;

/**
 * Runs the whole choreography on the calling thread, one step at a time, until
 * nothing is left to do.
 *
 * <p>Deterministic on purpose. A saga test that starts four background threads
 * and sleeps is a test that fails once a fortnight on someone else's laptop and
 * teaches nobody anything. Here every round is explicit: publish whatever the
 * outbox has, let each participant consume at most one event, repeat until a
 * full round changes nothing.
 *
 * <p>{@link #deliverPending} is a relay in six lines, using only
 * {@code OutboxStore}'s public API. It is not a reimplementation to be trusted —
 * {@code outbox-core}'s own suite is what proves the relay, and
 * {@code OrderSagaTest.realRelayDrivesTheWholeSaga} runs the genuine
 * {@code RelayEngine} against this same wiring so the shortcut cannot hide a
 * mismatch.
 */
final class Choreography implements AutoCloseable {

    /**
     * A round drains everything currently queued, so the round count is the depth
     * of the event chain — about six for this saga's longest compensating path.
     * Anything approaching this limit is a genuine cycle, not a slow drain.
     */
    private static final int MAX_ROUNDS = 200;

    private static final long FENCING_TOKEN = 1L;

    private final List<SagaParticipant> participants;
    private final OutboxStore store;
    private final InMemoryBus bus;

    Choreography(List<SagaParticipant> participants, OutboxStore store, InMemoryBus bus) {
        this.participants = participants;
        this.store = store;
        this.bus = bus;
    }

    /** Publishes every committed outbox row the cursor has not passed. */
    int deliverPending() throws Exception {
        long cursor = store.readCursor();
        List<OutboxStore.Row> batch = store.fetchBatch(cursor, 500);
        if (batch.isEmpty()) return 0;
        for (OutboxStore.Row row : batch) {
            bus.publish(row);
        }
        store.advanceCursor(batch.get(batch.size() - 1).id(), FENCING_TOKEN);
        return batch.size();
    }

    /**
     * Drains every participant's mailbox.
     *
     * <p>Draining rather than taking one each keeps the round count meaningful:
     * it becomes the length of the causal chain instead of a function of how many
     * messages happen to be in flight, which is what makes {@link #MAX_ROUNDS} a
     * cycle detector rather than a throughput limit.
     *
     * <p>Safe to drain fully, because events a participant emits land in the
     * outbox and are not visible to anyone until the next {@link #deliverPending}.
     */
    int pump() throws Exception {
        int consumed = 0;
        for (SagaParticipant participant : participants) {
            while (participant.pump(Duration.ZERO)) {
                consumed++;
            }
        }
        return consumed;
    }

    /** Lets each participant consume at most one event. Used by the real-relay test. */
    int pumpOnce() throws Exception {
        int consumed = 0;
        for (SagaParticipant participant : participants) {
            if (participant.pump(Duration.ZERO)) consumed++;
        }
        return consumed;
    }

    /**
     * Drives to quiescence: no rows left to publish and no events left to
     * consume. Returns the number of rounds it took, which is a useful thing to
     * assert on when a change accidentally introduces an event loop.
     */
    int settle() throws Exception {
        for (int round = 1; round <= MAX_ROUNDS; round++) {
            int delivered = deliverPending();
            int consumed = pump();
            if (delivered == 0 && consumed == 0) return round;
        }
        throw new IllegalStateException(
                "saga did not settle in " + MAX_ROUNDS + " rounds — most likely an event cycle");
    }

    boolean idle() {
        return bus.idle();
    }

    @Override
    public void close() throws Exception {
        for (SagaParticipant participant : participants) {
            participant.close();
        }
    }
}
