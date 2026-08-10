package de.ebrahim.outbox;

import java.time.Duration;

/**
 * Relay tuning.
 *
 * @param batchSize        rows read per round trip; also the unit of work between
 *                         cursor advances, so larger batches mean more duplicates
 *                         after a crash
 * @param idleTimeout      how long to block waiting for a nudge before polling
 *                         anyway; this is the safety net for a dropped nudge, so
 *                         it bounds worst-case latency rather than typical latency
 * @param errorBackoff     pause after an unexpected failure, to avoid hot-looping
 *                         against a broker or database that is down
 * @param purgeInterval    how often to delete published rows behind the cursor
 * @param retention        how long published rows are kept before purging, which
 *                         is your window for debugging what was actually sent
 */
public record RelayConfig(
        int batchSize,
        Duration idleTimeout,
        Duration errorBackoff,
        Duration purgeInterval,
        Duration retention) {

    public static RelayConfig defaults() {
        return new RelayConfig(
                100,
                Duration.ofMillis(250),
                Duration.ofSeconds(1),
                Duration.ofMinutes(1),
                Duration.ofHours(1));
    }

    public RelayConfig withBatchSize(int size) {
        return new RelayConfig(size, idleTimeout, errorBackoff, purgeInterval, retention);
    }

    public RelayConfig withIdleTimeout(Duration timeout) {
        return new RelayConfig(batchSize, timeout, errorBackoff, purgeInterval, retention);
    }
}
