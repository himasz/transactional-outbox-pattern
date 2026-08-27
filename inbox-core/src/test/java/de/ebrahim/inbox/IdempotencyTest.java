package de.ebrahim.inbox;

import de.ebrahim.inbox.store.InboxGuard;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The tests that earn the module's keep.
 *
 * <p>{@code redeliveryAppliesTheEffectOnce} is the whole point of the pattern.
 * {@code racingReplicasApplyTheEffectOnce} is the one that would quietly pass
 * against a fake: it depends on PostgreSQL making the losing writer wait on the
 * winner's uncommitted row and then re-check the claim's {@code WHERE} clause,
 * and nothing else in the stack provides that.
 */
class IdempotencyTest extends InboxTestBase {

    @Test
    @DisplayName("the same message delivered twice applies its effect once")
    void redeliveryAppliesTheEffectOnce() throws Exception {
        Inbox inbox = inbox();
        CountingHandler handler = new CountingHandler();
        InboxMessage message = message("order-1");

        assertEquals(InboxResult.PROCESSED, inbox.process(message, handler));
        assertEquals(InboxResult.DUPLICATE, inbox.process(message, handler),
                "the second delivery must be recognised as a duplicate");

        assertEquals(1, handler.invocations.get(), "the handler must not run twice");
        assertEquals(1, countProjection("order-1"), "the effect must exist exactly once");
    }

    @Test
    @DisplayName("a crash after commit but before ack costs nothing on redelivery")
    void crashBetweenCommitAndAckIsAbsorbed() throws Exception {
        Inbox inbox = inbox();
        CountingHandler handler = new CountingHandler();
        InboxMessage message = message("order-2");

        // First pass commits. The "crash" is simply that nobody acknowledges the
        // broker, which is indistinguishable from a process death at that instant
        // and is the single most likely way this system loses a step in
        // production. The broker redelivers.
        assertEquals(InboxResult.PROCESSED, inbox.process(message, handler));

        assertEquals(InboxResult.DUPLICATE, inbox.process(message, handler));
        assertEquals(1, countProjection("order-2"));
        assertEquals("DONE", statusOf(message.messageId()));
    }

    @Test
    @DisplayName("two replicas handed the same message apply its effect once")
    void racingReplicasApplyTheEffectOnce() throws Exception {
        InboxMessage message = message("order-3");
        CountingHandler handler = new CountingHandler();

        CountDownLatch insideHandler = new CountDownLatch(1);
        CountDownLatch releaseHandler = new CountDownLatch(1);
        AtomicReference<InboxResult> slowResult = new AtomicReference<>();
        AtomicReference<InboxResult> fastResult = new AtomicReference<>();
        AtomicReference<Exception> failure = new AtomicReference<>();

        // Replica A claims and then sits inside the handler with its transaction
        // open, which is exactly the window a second replica must survive.
        Thread a = Thread.ofVirtual().start(() -> {
            try {
                slowResult.set(inbox().process(message, (tx, msg) -> {
                    handler.handle(tx, msg);
                    insideHandler.countDown();
                    releaseHandler.await(10, TimeUnit.SECONDS);
                }));
            } catch (Exception e) {
                failure.set(e);
            }
        });

        assertTrue(insideHandler.await(10, TimeUnit.SECONDS), "replica A never reached the handler");

        // Replica B claims the same message while A holds the row. It must block
        // rather than proceed, and must see DUPLICATE once A commits.
        Thread b = Thread.ofVirtual().start(() -> {
            try {
                fastResult.set(inbox().process(message, handler));
            } catch (Exception e) {
                failure.set(e);
            }
        });

        // Give B long enough to have finished if it were going to run the handler.
        Thread.sleep(500);
        assertEquals(1, handler.invocations.get(), "replica B must not run the handler while A holds the claim");

        releaseHandler.countDown();
        a.join(TimeUnit.SECONDS.toMillis(10));
        b.join(TimeUnit.SECONDS.toMillis(10));

        assertNull(failure.get(), () -> "unexpected failure: " + failure.get());
        assertEquals(InboxResult.PROCESSED, slowResult.get());
        assertEquals(InboxResult.DUPLICATE, fastResult.get(),
                "the losing replica must see a duplicate, not a unique-violation error");
        assertEquals(1, handler.invocations.get());
        assertEquals(1, countProjection("order-3"));
    }

    @Test
    @DisplayName("two handlers with different consumer names each get the message")
    void consumerNamesAreIndependent() throws Exception {
        InboxMessage message = message("order-4");
        AtomicInteger projected = new AtomicInteger();
        AtomicInteger audited = new AtomicInteger();

        // Two unrelated handlers in the same service, each with its own effect.
        assertEquals(InboxResult.PROCESSED, new Inbox(dataSource, InboxConfig.forConsumer("projector"))
                .process(message, (tx, msg) -> {
                    projected.incrementAndGet();
                    apply(tx, "projected:" + msg.payloadAsString());
                }));

        // The second must not be starved by the first. Deduplicating on
        // message_id alone would silently swallow this message, which is why
        // consumer is half the primary key.
        assertEquals(InboxResult.PROCESSED, new Inbox(dataSource, InboxConfig.forConsumer("auditor"))
                .process(message, (tx, msg) -> {
                    audited.incrementAndGet();
                    apply(tx, "audited:" + msg.payloadAsString());
                }));

        assertEquals(1, projected.get());
        assertEquals(1, audited.get());
        assertEquals(2, countProjection(), "each consumer applied its own effect once");
    }

    private static void apply(Connection tx, String ref) throws Exception {
        try (var ps = tx.prepareStatement("INSERT INTO projection (ref) VALUES (?)")) {
            ps.setString(1, ref);
            ps.executeUpdate();
        }
    }

    @Test
    @DisplayName("claiming on an auto-commit connection is rejected")
    void autoCommitIsRejected() throws Exception {
        try (Connection c = dataSource.getConnection()) {
            // Auto-commit would make the claim durable independently of the
            // handler's writes, so a later failure would leave the message marked
            // handled with none of its effects applied — and the redelivery would
            // then be correctly, catastrophically, suppressed.
            IllegalStateException thrown = assertThrows(IllegalStateException.class,
                    () -> guard.claim(c, CONSUMER, message("order-5")));
            assertTrue(thrown.getMessage().contains("explicit transaction"));
        }
    }

    @Test
    @DisplayName("the claim rolls back with the handler's writes")
    void claimAndEffectsShareOneTransaction() throws Exception {
        InboxMessage message = message("order-6");

        try (Connection tx = tx()) {
            assertTrue(guard.claim(tx, CONSUMER, message));
            try (var ps = tx.prepareStatement("INSERT INTO projection (ref) VALUES ('order-6')")) {
                ps.executeUpdate();
            }
            tx.rollback();
        }

        // Neither survived, which is the only acceptable pairing: a claim without
        // effects would suppress the redelivery that is supposed to apply them.
        assertEquals(0, countProjection(), "the effect must not survive the rollback");
        assertNull(statusOf(message.messageId()), "the claim must not survive the rollback either");

        // And the message is still eligible, so the redelivery does the work.
        CountingHandler handler = new CountingHandler();
        assertEquals(InboxResult.PROCESSED, inbox().process(message, handler));
        assertEquals(1, countProjection("order-6"));
    }
}
