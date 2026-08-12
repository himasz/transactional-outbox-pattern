package de.ebrahim.outbox.election;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Optional;

/**
 * A real TTL-lease elector backed by the database the relay already talks to.
 *
 * <p>Included so the runnable example is genuinely distributed rather than
 * relying on a mock that says yes to everybody. It also makes an architectural
 * point: PostgreSQL is already a linearizable, durable, highly available store,
 * so standing up a separate consensus cluster purely to elect this leader would
 * be redundant consensus with a real operational bill (peer discovery, a
 * persistent log, snapshots, quorum-aware rolling deploys).
 *
 * <p>In Kubernetes the equivalent zero-infrastructure choice is a {@code Lease}
 * object, which is etcd — and therefore Raft — without operating Raft yourself.
 *
 * <p>Like every lease-based elector this one can briefly hand leadership to two
 * instances during a partition or a long pause. That window is closed by the
 * fencing token, not by the lease.
 */
public final class PostgresLeaseElector implements LeaderElector {

    private static final Logger log = LoggerFactory.getLogger(PostgresLeaseElector.class);

    /**
     * Acquire-or-renew in one statement.
     *
     * <p>The {@code WHERE} clause on the upsert is the mutual exclusion: the row
     * is only taken if we already own it or the incumbent's lease has expired.
     * The token increments only on a change of owner, so a leader renewing
     * keeps its token and does not needlessly invalidate its own in-flight work.
     */
    private static final String ACQUIRE_SQL = """
            INSERT INTO outbox_lease (name, owner, fencing_token, expires_at)
            VALUES ('relay', ?, 1, now() + make_interval(secs => ?))
            ON CONFLICT (name) DO UPDATE
               SET owner         = EXCLUDED.owner,
                   fencing_token = CASE WHEN outbox_lease.owner = EXCLUDED.owner
                                        THEN outbox_lease.fencing_token
                                        ELSE outbox_lease.fencing_token + 1 END,
                   expires_at    = EXCLUDED.expires_at
             WHERE outbox_lease.owner = EXCLUDED.owner
                OR outbox_lease.expires_at < now()
            RETURNING fencing_token
            """;

    private final DataSource dataSource;
    private final String owner;
    private final Duration ttl;
    private final Duration renewInterval;
    /**
     * Leases are treated as expired slightly early. The margin absorbs clock
     * drift and the round-trip of the renewal itself, so we stop publishing
     * before the database would consider the lease free.
     */
    private final Duration safetyMargin;

    private volatile Lease current;
    private volatile long nextRenewNanos;

    public PostgresLeaseElector(DataSource dataSource, String owner) {
        this(dataSource, owner, Duration.ofSeconds(10), Duration.ofSeconds(3), Duration.ofSeconds(2));
    }

    public PostgresLeaseElector(DataSource dataSource, String owner,
                                Duration ttl, Duration renewInterval, Duration safetyMargin) {
        this.dataSource = dataSource;
        this.owner = owner;
        this.ttl = ttl;
        this.renewInterval = renewInterval;
        this.safetyMargin = safetyMargin;
    }

    @Override
    public Optional<Lease> tryAcquire() {
        Lease held = current;
        // Fast path: a valid lease that is not yet due for renewal costs nothing.
        if (held != null && held.isValid() && System.nanoTime() < nextRenewNanos) {
            return Optional.of(held);
        }
        try (Connection c = dataSource.getConnection();
             PreparedStatement ps = c.prepareStatement(ACQUIRE_SQL)) {
            ps.setString(1, owner);
            ps.setDouble(2, ttl.toMillis() / 1000.0);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    current = null;             // somebody else holds a live lease
                    return Optional.empty();
                }
                long token = rs.getLong(1);
                // Deadline is derived from the monotonic clock, taken BEFORE the
                // round trip, so a slow query shortens the lease rather than
                // silently extending it past what the database recorded.
                long deadline = System.nanoTime() + ttl.minus(safetyMargin).toNanos();
                Lease lease = new Lease() {
                    @Override public long fencingToken() { return token; }
                    @Override public boolean isValid() { return System.nanoTime() < deadline; }
                };
                current = lease;
                nextRenewNanos = System.nanoTime() + renewInterval.toNanos();
                return Optional.of(lease);
            }
        } catch (SQLException e) {
            log.warn("lease acquisition failed, standing down for this tick", e);
            current = null;
            return Optional.empty();
        }
    }
}
