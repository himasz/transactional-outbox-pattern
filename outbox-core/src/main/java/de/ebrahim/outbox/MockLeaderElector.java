package de.ebrahim.outbox;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The mocked elector the brief permits.
 *
 * <p>It is controllable rather than always-true, which is the point: the
 * interesting behavior of the relay is what happens when leadership is
 * <em>revoked</em>, and only a driveable mock lets a test assert that
 * deterministically. See {@code FencingTest}.
 *
 * <p>{@link #grant()} bumps the token to model a handover, so a test can hold a
 * stale lease and verify the database rejects its writes.
 */
public final class MockLeaderElector implements LeaderElector {

    private final AtomicBoolean isLeader;
    private final AtomicLong token = new AtomicLong(1);

    public MockLeaderElector(boolean initiallyLeader) {
        this.isLeader = new AtomicBoolean(initiallyLeader);
    }

    public static MockLeaderElector alwaysLeader() {
        return new MockLeaderElector(true);
    }

    /**
     * Models a leadership handover: leadership is granted under a NEW token.
     */
    public void grant() {
        token.incrementAndGet();
        isLeader.set(true);
    }

    /**
     * Models a lost lease. Any lease already handed out becomes invalid at once.
     */
    public void revoke() {
        isLeader.set(false);
    }

    public long currentToken() {
        return token.get();
    }

    @Override
    public Optional<Lease> tryAcquire() {
        if (!isLeader.get()) return Optional.empty();
        long issued = token.get();
        return Optional.of(new Lease() {
            @Override
            public long fencingToken() {
                return issued;
            }

            // Invalid as soon as leadership is revoked OR a newer token is issued,
            // which is exactly how a real lease behaves after a handover.
            @Override
            public boolean isValid() {
                return isLeader.get() && token.get() == issued;
            }
        });
    }
}
