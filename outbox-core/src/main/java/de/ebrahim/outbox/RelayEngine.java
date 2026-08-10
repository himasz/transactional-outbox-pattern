package de.ebrahim.outbox;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;

/**
 * The relay: reads committed outbox rows in order and publishes them.
 *
 * <p>Runs on exactly one instance at a time. Followers still execute this loop,
 * but {@link LeaderElector#tryAcquire()} returns empty for them and they do
 * nothing but sleep, so a standby costs one idle virtual thread.
 *
 * <p>Deliberately identical whether embedded in the writing service or run as
 * its own deployment ({@code outbox-runner}). Embedded vs standalone is a
 * packaging decision, not an architectural one, and nothing in this class knows
 * which mode it is in.
 */
public final class RelayEngine implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(RelayEngine.class);

    private final OutboxStore store;
    private final MessagePublisher publisher;
    private final LeaderElector elector;
    private final WakeupSignal wakeup;
    private final RelayConfig config;

    private volatile boolean running;
    private volatile Thread thread;

    /** Token we have already stamped on the cursor; -1 forces a re-claim. */
    private long claimedToken = -1;
    private long nextPurgeNanos;

    public RelayEngine(OutboxStore store, MessagePublisher publisher,
                       LeaderElector elector, WakeupSignal wakeup, RelayConfig config) {
        this.store = store;
        this.publisher = publisher;
        this.elector = elector;
        this.wakeup = wakeup;
        this.config = config;
    }

    public void start() {
        if (running) return;
        running = true;
        // A virtual thread keeps the strictly sequential publish loop readable
        // without an executor: blocking on an ack costs no platform thread.
        thread = Thread.ofVirtual().name("outbox-relay").start(this::run);
    }

    private void run() {
        log.info("outbox relay started");
        while (running) {
            try {
                // Blocks until a commit nudges us, or until the safety tick fires.
                wakeup.await(config.idleTimeout());
                tick();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                log.error("relay tick failed, backing off", e);
                sleep(config.errorBackoff().toMillis());
            }
        }
        log.info("outbox relay stopped");
    }

    /** One pass: confirm leadership, then drain the outbox. Package-private for tests. */
    void tick() throws Exception {
        Optional<LeaderElector.Lease> maybeLease = elector.tryAcquire();
        if (maybeLease.isEmpty()) {
            claimedToken = -1;              // we are a follower; re-claim if we lead again
            return;
        }
        LeaderElector.Lease lease = maybeLease.get();

        // Stamp our token before publishing anything. From this point a previous
        // leader that has not noticed the handover fails every cursor write.
        if (lease.fencingToken() != claimedToken) {
            if (!store.claimCursor(lease.fencingToken())) {
                log.warn("could not claim cursor at token {}; a newer leader exists", lease.fencingToken());
                claimedToken = -1;
                return;
            }
            claimedToken = lease.fencingToken();
        }

        drain(lease);
        maybePurge();
    }

    private void drain(LeaderElector.Lease lease) throws Exception {
        while (running && lease.isValid()) {
            long cursor = store.readCursor();
            List<OutboxStore.Row> batch = store.fetchBatch(cursor, config.batchSize());
            if (batch.isEmpty()) return;

            long lastPublished = cursor;
            for (OutboxStore.Row row : batch) {
                // Re-checked per message, not per batch: losing the lease halfway
                // through must stop us immediately, while a second publisher is
                // possibly already running.
                if (!lease.isValid()) {
                    log.warn("lease lost mid-batch after outbox id {}", lastPublished);
                    break;
                }
                publisher.publish(row);
                lastPublished = row.id();
            }

            if (lastPublished > cursor) {
                if (!store.advanceCursor(lastPublished, lease.fencingToken())) {
                    // Fenced: a newer leader owns the cursor. Everything we just
                    // published will be republished by them and deduplicated
                    // downstream. Stop rather than fight for the watermark.
                    log.warn("fenced out at token {}; stepping down", lease.fencingToken());
                    claimedToken = -1;
                    return;
                }
            }
            // A short batch means the outbox is drained (or blocked behind an
            // in-flight transaction); stop and wait for the next nudge.
            if (batch.size() < config.batchSize()) return;
        }
    }

    private void maybePurge() {
        long now = System.nanoTime();
        if (now < nextPurgeNanos) return;
        nextPurgeNanos = now + config.purgeInterval().toNanos();
        try {
            int deleted = store.purgePublished(config.retention().toSeconds());
            if (deleted > 0) log.debug("purged {} published outbox rows", deleted);
        } catch (Exception e) {
            log.warn("retention purge failed; will retry", e);
        }
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
        Thread t = thread;
        if (t != null) t.interrupt();
    }
}
