package de.ebrahim.inbox;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Statement;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * What happens when the handler says no.
 *
 * <p>The subtle one is {@code attemptsSurviveTheRollbackThatCausedThem}. A retry
 * counter incremented inside the failing transaction is rolled back along with
 * everything else, so it is always zero and the message retries forever. The
 * counter therefore has to be written on a separate committed connection —
 * exactly the trick the demo's {@code rollback_audit} table uses on the
 * producing side, for exactly the same reason.
 */
class FailureHandlingTest extends InboxTestBase {

    private static final class Poison extends RuntimeException {
        Poison() {
            super("handler refused the message");
        }
    }

    @Test
    @DisplayName("a failing handler commits nothing and asks for a retry")
    void failureRollsBackEverything() throws Exception {
        Inbox inbox = inbox();
        InboxMessage message = message("order-1");

        InboxResult result = inbox.process(message, (tx, msg) -> {
            try (Statement st = tx.createStatement()) {
                st.execute("INSERT INTO projection (ref) VALUES ('order-1')");
            }
            throw new Poison();
        });

        assertEquals(InboxResult.RETRY, result);
        assertEquals(0, countProjection(), "a failed handler must leave no effects behind");
        assertEquals("PENDING", statusOf(message.messageId()),
                "the message must stay eligible so the redelivery can retry it");
        assertFalse(result.shouldAcknowledge(), "RETRY must not acknowledge the broker");
    }

    @Test
    @DisplayName("the attempt counter survives the rollback that caused it")
    void attemptsSurviveTheRollbackThatCausedThem() throws Exception {
        Inbox inbox = inbox(InboxConfig.forConsumer(CONSUMER).withMaxAttempts(10));
        InboxMessage message = message("order-2");

        for (int i = 1; i <= 3; i++) {
            assertEquals(InboxResult.RETRY, inbox.process(message, (tx, msg) -> {
                throw new Poison();
            }));
            assertEquals(i, attemptsOf(message.messageId()),
                    "attempt " + i + " was lost with the rollback it belongs to");
        }
    }

    @Test
    @DisplayName("a message that keeps failing is parked, not retried forever")
    void poisonMessageIsParked() throws Exception {
        Inbox inbox = inbox(InboxConfig.forConsumer(CONSUMER).withMaxAttempts(3));
        InboxMessage message = message("order-3");

        assertEquals(InboxResult.RETRY, inbox.process(message, failing()));
        assertEquals(InboxResult.RETRY, inbox.process(message, failing()));
        assertEquals(InboxResult.PARKED, inbox.process(message, failing()),
                "the third failure exhausts maxAttempts");

        assertEquals("DEAD", statusOf(message.messageId()));
        assertTrue(InboxResult.PARKED.shouldAcknowledge(),
                "a parked message must be acknowledged, or it blocks the queue forever");

        // And it stays parked. A later redelivery must not quietly resurrect a
        // message a human has not looked at yet — even one whose handler would
        // now succeed, because nothing here knows that.
        CountingHandler wouldSucceed = new CountingHandler();
        assertEquals(InboxResult.PARKED, inbox.process(message, wouldSucceed));
        assertEquals(0, wouldSucceed.invocations.get());
    }

    @Test
    @DisplayName("a message that fails then succeeds keeps its history and applies once")
    void retryAfterFailureAppliesOnce() throws Exception {
        Inbox inbox = inbox();
        InboxMessage message = message("order-4");

        assertEquals(InboxResult.RETRY, inbox.process(message, failing()));

        CountingHandler handler = new CountingHandler();
        assertEquals(InboxResult.PROCESSED, inbox.process(message, handler));
        assertEquals(InboxResult.DUPLICATE, inbox.process(message, handler));

        assertEquals(1, handler.invocations.get());
        assertEquals(1, countProjection("order-4"));
        assertEquals("DONE", statusOf(message.messageId()));
        assertEquals(2, attemptsOf(message.messageId()), "the failed attempt is still counted");
    }

    @Test
    @DisplayName("the failed payload is kept so a dead letter is worth reading")
    void deadLettersRetainTheirPayload() throws Exception {
        Inbox inbox = inbox(InboxConfig.forConsumer(CONSUMER).withMaxAttempts(1));
        InboxMessage message = message("order-5");

        assertEquals(InboxResult.PARKED, inbox.process(message, failing()));

        assertEquals(1, scalar("SELECT count(*) FROM inbox_message"
                + " WHERE status = 'DEAD' AND payload IS NOT NULL AND last_error IS NOT NULL"),
                "a dead letter with no body and no error is not much of a dead letter");
    }

    @Test
    @DisplayName("retention sweeps handled messages but never dead ones")
    void retentionKeepsDeadLetters() throws Exception {
        Inbox inbox = inbox(InboxConfig.forConsumer(CONSUMER).withMaxAttempts(1).withRetention(Duration.ZERO));

        InboxMessage handled = message("order-6");
        InboxMessage parked = message("order-7");
        assertEquals(InboxResult.PROCESSED, inbox.process(handled, new CountingHandler()));
        assertEquals(InboxResult.PARKED, inbox.process(parked, failing()));

        assertEquals(1, inbox.purge(), "only the DONE row is sweepable");

        assertNull(statusOf(handled.messageId()));
        assertEquals("DEAD", statusOf(parked.messageId()),
                "the dead-letter record is the one thing that must not be garbage-collected");

        // The point of the retention window, stated as a test: once the row is
        // gone there is nothing left to deduplicate against, so a late
        // redelivery is applied a second time. That is why retention is a
        // correctness setting and must exceed the broker's redelivery horizon.
        assertEquals(InboxResult.PROCESSED, inbox.process(handled, (tx, msg) -> { }),
                "a purged message is indistinguishable from one never seen");
        assertNotNull(statusOf(handled.messageId()));
    }

    private static InboxHandler failing() {
        return (tx, msg) -> {
            throw new Poison();
        };
    }
}
