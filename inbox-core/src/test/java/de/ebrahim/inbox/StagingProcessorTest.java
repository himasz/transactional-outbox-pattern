package de.ebrahim.inbox;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Store-and-forward mode.
 *
 * <p>{@code stagedHoleIsNotSkipped} is the direct counterpart of the relay's
 * {@code sequenceHoleIsNotSkipped}, and it fails in the same way for the same
 * reason if the {@code tx_id} predicate is removed from
 * {@code InboxStore.NEXT_PENDING_SQL}: {@code seq} is handed out at INSERT time,
 * so a receiver holding a lower seq can still be in flight when a higher one
 * commits. Order would then be lost on the consuming side after the relay had
 * gone to considerable trouble to preserve it on the producing side.
 *
 * <p>{@code secondProcessorDoesNotReorderOrRepeat} pins the claim in this
 * module's design: the inbox needs no leader election, because ordering is
 * enforced by a lock on the head of the queue rather than by a shared watermark.
 */
class StagingProcessorTest extends InboxTestBase {

    @Test
    @DisplayName("staged messages are handled in order, exactly once each")
    void stagedMessagesAreHandledInOrder() throws Exception {
        Inbox inbox = inbox();
        CountingHandler handler = new CountingHandler();

        for (int i = 1; i <= 5; i++) {
            assertEquals(InboxResult.PROCESSED, inbox.stage(message("order-" + i)));
        }
        assertEquals(5, inbox.pendingDepth());

        try (InboxProcessor processor = processor(handler)) {
            drain(processor);
        }

        assertEquals(List.of("order-1", "order-2", "order-3", "order-4", "order-5"), handler.seen);
        assertEquals(0, inbox.pendingDepth());
        assertEquals(5, countProjection());
    }

    @Test
    @DisplayName("staging the same message twice stages it once")
    void stagingIsIdempotent() throws Exception {
        Inbox inbox = inbox();
        InboxMessage message = message("order-1");

        assertEquals(InboxResult.PROCESSED, inbox.stage(message));
        assertEquals(InboxResult.DUPLICATE, inbox.stage(message));
        assertEquals(1, inbox.pendingDepth());

        // And once handled, a late redelivery does not re-stage it either.
        CountingHandler handler = new CountingHandler();
        try (InboxProcessor processor = processor(handler)) {
            drain(processor);
        }
        assertEquals(InboxResult.DUPLICATE, inbox.stage(message));
        assertEquals(1, handler.invocations.get());
    }

    @Test
    @DisplayName("a seq assigned earlier but committed later is not skipped")
    void stagedHoleIsNotSkipped() throws Exception {
        Inbox inbox = inbox();
        CountingHandler handler = new CountingHandler();

        // Two receiver replicas staging concurrently. The slow one takes the
        // lower seq and commits last, which is the hole.
        Connection slow = tx();
        Connection fast = tx();
        stageOn(slow, "order-low-seq-late-commit");
        stageOn(fast, "order-high-seq-early-commit");
        fast.commit();

        try (InboxProcessor processor = processor(handler)) {
            processor.tick();
            assertEquals(0, handler.invocations.get(),
                    "the processor must not step over an in-flight lower seq");

            slow.commit();
            slow.close();
            fast.close();

            drain(processor);
        }

        assertEquals(List.of("order-low-seq-late-commit", "order-high-seq-early-commit"), handler.seen,
                "both messages must be handled, in seq order");
        assertEquals(0, inbox.pendingDepth());
    }

    @Test
    @DisplayName("a second processor neither reorders nor repeats work, with no election")
    void secondProcessorDoesNotReorderOrRepeat() throws Exception {
        Inbox inbox = inbox();
        for (int i = 1; i <= 3; i++) {
            inbox.stage(message("order-" + i));
        }

        CountingHandler shared = new CountingHandler();
        CountDownLatch insideFirst = new CountDownLatch(1);
        CountDownLatch releaseFirst = new CountDownLatch(1);
        AtomicReference<Exception> failure = new AtomicReference<>();

        // Processor A takes the head of the queue and holds it.
        InboxProcessor a = new InboxProcessor(dataSource, config(), (tx, msg) -> {
            shared.handle(tx, msg);
            if (insideFirst.getCount() > 0) {
                insideFirst.countDown();
                releaseFirst.await(10, TimeUnit.SECONDS);
            }
        });
        Thread first = Thread.ofVirtual().start(() -> {
            try {
                a.tick();
            } catch (Exception e) {
                failure.set(e);
            }
        });
        assertTrue(insideFirst.await(10, TimeUnit.SECONDS), "processor A never reached the handler");

        // Processor B tries to work the same queue. In ordered mode it blocks on
        // the locked head row rather than skipping to order-2, which is what
        // buys FIFO without any leader election.
        CountingHandler second = new CountingHandler();
        try (InboxProcessor b = processor(second)) {
            Thread bThread = Thread.ofVirtual().start(() -> {
                try {
                    b.tick();
                } catch (Exception e) {
                    failure.set(e);
                }
            });
            Thread.sleep(500);
            assertEquals(0, second.invocations.get(), "processor B must not skip past the locked head");

            releaseFirst.countDown();
            first.join(TimeUnit.SECONDS.toMillis(10));
            bThread.join(TimeUnit.SECONDS.toMillis(10));
        }
        a.close();

        assertEquals(null, failure.get(), () -> "unexpected failure: " + failure.get());
        assertEquals(3, countProjection(), "every message applied exactly once");
        assertEquals(0, inbox.pendingDepth());
    }

    @Test
    @DisplayName("a poison message parks and lets the queue behind it move")
    void poisonMessageUnblocksTheQueue() throws Exception {
        Inbox inbox = inbox();
        InboxMessage poison = message("poison");
        inbox.stage(poison);
        inbox.stage(message("order-after"));

        CountingHandler handler = new CountingHandler();
        InboxConfig config = config().withMaxAttempts(2);

        try (InboxProcessor processor = new InboxProcessor(dataSource, config, (tx, msg) -> {
            if ("poison".equals(msg.payloadAsString())) throw new IllegalStateException("nope");
            handler.handle(tx, msg);
        })) {
            // Head-of-line blocking is real and deliberate: while attempts
            // remain, nothing behind the poison message is touched.
            processor.tick();
            assertEquals(0, handler.invocations.get(), "order-after must wait behind the poison message");
            assertEquals("PENDING", statusOf(poison.messageId()));

            processor.tick();
            assertEquals("DEAD", statusOf(poison.messageId()), "attempts are spent, so it parks");

            drain(processor);
        }

        assertEquals(List.of("order-after"), handler.seen, "the queue moves once the poison is parked");
        assertEquals(1, countProjection());
    }

    @Test
    @DisplayName("unordered mode trades FIFO for parallelism, keeping exactly-once")
    void unorderedModeStillAppliesOnce() throws Exception {
        Inbox inbox = inbox();
        for (int i = 1; i <= 20; i++) {
            inbox.stage(message("order-" + i));
        }

        InboxConfig unordered = config().withOrdered(false);
        CountingHandler handler = new CountingHandler();

        List<Thread> workers = List.of(
                worker(unordered, handler), worker(unordered, handler), worker(unordered, handler));
        for (Thread w : workers) w.join(TimeUnit.SECONDS.toMillis(30));

        assertEquals(20, countProjection(), "SKIP LOCKED must not cause double application");
        assertEquals(0, inbox.pendingDepth());
        // Nothing here asserts ordering, because unordered mode does not offer
        // any. That is the entire trade, and it should be visible in the tests.
        assertNotEquals(0, handler.invocations.get());
    }

    private Thread worker(InboxConfig config, InboxHandler handler) {
        return Thread.ofVirtual().start(() -> {
            try (InboxProcessor p = new InboxProcessor(dataSource, config, handler)) {
                for (int i = 0; i < 40; i++) {
                    if (!p.tick()) break;
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    private InboxConfig config() {
        return InboxConfig.forConsumer(CONSUMER);
    }

    private InboxProcessor processor(InboxHandler handler) {
        return new InboxProcessor(dataSource, config(), handler);
    }

    /** Drains repeatedly: the xmin watermark can hold rows back briefly. */
    private static void drain(InboxProcessor processor) throws Exception {
        for (int i = 0; i < 20; i++) {
            if (!processor.tick()) return;
        }
    }

    private void stageOn(Connection tx, String ref) throws Exception {
        try (var ps = tx.prepareStatement("""
                INSERT INTO inbox_message (consumer, message_id, subject, payload, status)
                VALUES (?, ?, 'orders.created', ?, 'PENDING')
                """)) {
            ps.setString(1, CONSUMER);
            ps.setObject(2, UUID.randomUUID());
            ps.setBytes(3, ref.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            ps.executeUpdate();
        }
    }
}
