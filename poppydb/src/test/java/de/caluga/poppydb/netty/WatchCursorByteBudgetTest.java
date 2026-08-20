package de.caluga.poppydb.netty;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.commands.WatchCommand;
import de.caluga.morphium.driver.inmem.InMemoryDriver;

/**
 * #321: watch-cursor queues used to be bounded by count only ({@code MAX_CURSOR_QUEUE_SIZE} =
 * 10,000). Each queued event shares its {@code fullDocument} payload with the replay-buffer
 * entry, so replay-buffer byte eviction frees nothing while a stalled cursor still references
 * the payloads - with ~300KB documents a single slow consumer could pin ~3GB on the primary,
 * the node whose OOM takes the whole cluster down (same failure family as the 2026-08-14 ACC
 * incident, one layer up).
 *
 * <p>The queue now carries a byte budget with the same semantics as the two sibling budgets
 * (replay buffer, replication event queue): overflow kills the cursor - the documented policy
 * of the count cap, because blocking would stall the writer thread that delivers events in
 * server mode, and dropping would silently lose events.
 */
public class WatchCursorByteBudgetTest {

    private static final String DB = "bytebudget";
    private static final String COLL = "events";

    private InMemoryDriver drv;
    private WatchCursorManager cursors;

    @BeforeEach
    public void setUp() {
        drv = new InMemoryDriver();
        drv.connect();
        drv.setServerMode(true);
        cursors = new WatchCursorManager();
    }

    @AfterEach
    public void tearDown() {
        if (cursors != null) {
            cursors.shutdown();
        }

        if (drv != null) {
            drv.close();
        }
    }

    private long watchCursor() {
        WatchCommand wcmd = new WatchCommand(drv).setDb(DB).setColl(COLL).setMaxTimeMS(30000);
        return cursors.createWatchCursor(drv, wcmd);
    }

    /** ~1KB of payload per event so a handful of events crosses a tiny byte budget. */
    private void insertLarge(int id) {
        drv.store(DB, COLL, List.of(Doc.of("_id", id, "payload", "x".repeat(1024))), null);
    }

    private void awaitCondition(java.util.function.BooleanSupplier cond) throws Exception {
        for (int i = 0; i < 250 && !cond.getAsBoolean(); i++) {
            Thread.sleep(20);
        }
    }

    /**
     * The count cap stays high and the byte budget is tiny, so the byte path is what must fire:
     * a parked consumer accumulating large events is killed long before 10,000 events.
     */
    @Test
    public void aSlowConsumerIsKilledByTheByteBudgetNotJustTheCountCap() throws Exception {
        cursors.setCursorQueueByteBudget(4096);
        long cursorId = watchCursor();

        // no getMore ever - the consumer is parked; ~20KB of events against a 4KB budget
        for (int i = 0; i < 20; i++) {
            insertLarge(i);
        }

        awaitCondition(() -> !cursors.hasCursor(cursorId));

        assertThat(cursors.hasCursor(cursorId))
            .as("a cursor whose queued bytes exceed the budget must be killed, "
                    + "same policy as the count cap").isFalse();
    }

    /**
     * Accounting invariant: offer-time add, drain-time subtract - after the consumer drains
     * everything, the byte counter is exactly zero (a drifting counter would silently disable
     * the budget, see the replay buffer's accountHistoryRemoval for the same rule).
     */
    @Test
    public void accountedBytesReturnToZeroAfterDrain() throws Exception {
        cursors.setCursorQueueByteBudget(64 * 1024 * 1024);
        long cursorId = watchCursor();

        for (int i = 0; i < 5; i++) {
            insertLarge(i);
        }

        awaitCondition(() -> cursors.bufferedEventCount(cursorId) >= 5);
        assertThat(cursors.queuedByteCount(cursorId))
            .as("buffered events must be accounted in bytes").isGreaterThan(5 * 1024L);

        while (cursors.bufferedEventCount(cursorId) > 0) {
            assertThat(cursors.getMore(cursorId, 100).get(5, TimeUnit.SECONDS)).isNotEmpty();
        }

        assertThat(cursors.queuedByteCount(cursorId))
            .as("after a full drain the byte counter must return to exactly zero").isEqualTo(0L);
    }

    /**
     * A single event larger than the whole budget must still be deliverable when the queue is
     * empty - the same newest-event-survives rule the replay-buffer byte budget applies.
     * Killing here would put an upper bound on document size that MongoDB does not have.
     */
    @Test
    public void aSingleEventLargerThanTheBudgetIsStillDelivered() throws Exception {
        cursors.setCursorQueueByteBudget(1024);
        long cursorId = watchCursor();

        drv.store(DB, COLL, List.of(Doc.of("_id", 1, "payload", "x".repeat(8192))), null);

        awaitCondition(() -> cursors.bufferedEventCount(cursorId) >= 1);

        assertThat(cursors.hasCursor(cursorId))
            .as("an oversized single event must not kill an otherwise empty cursor").isTrue();
        List<Map<String, Object>> batch = cursors.getMore(cursorId, 100).get(5, TimeUnit.SECONDS);
        assertThat(batch).as("the oversized event must reach the consumer").isNotEmpty();
    }
}
