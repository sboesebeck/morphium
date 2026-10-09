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

    /**
     * The global budget caps the fleet total even when every cursor is far under its own
     * per-cursor budget - the per-cursor bound alone leaves the total over N cursors unbounded.
     * On overflow the newest offending cursor is killed, and the global byte counter follows the
     * live cursors exactly.
     */
    @Test
    public void globalBudgetBoundsTheFleetTotalAcrossCursors() throws Exception {
        cursors.setCursorQueueByteBudget(64 * 1024 * 1024); // generous per-cursor budget
        cursors.setGlobalCursorByteBudget(4096);            // tiny fleet-wide cap
        long c1 = watchCursor();
        long c2 = watchCursor();

        // no getMore ever - the consumer is parked; each cursor buffers the same events, and the
        // fleet total crosses 4KB well before either cursor is anywhere near its 64MB budget.
        for (int i = 0; i < 40 && (cursors.hasCursor(c1) || cursors.hasCursor(c2)); i++) {
            insertLarge(i);
        }

        assertThat(cursors.hasCursor(c1) || cursors.hasCursor(c2))
            .as("the fleet total must not be allowed to grow unbounded; at least one cursor gets "
                + "killed by the global budget").isFalse();
    }

    /**
     * The global budget evicts the cursor actually holding the most buffered bytes, not the cursor
     * that happened to receive the triggering event. The replication watch receives every event and
     * would otherwise be the most likely victim while rarely being the culprit; a drained cursor
     * must survive while the parked one - the real fleet hog - is killed.
     */
    @Test
    public void globalBudgetEvictsTheLargestHolderNotTheOfferingCursor() throws Exception {
        cursors.setCursorQueueByteBudget(64 * 1024 * 1024);
        cursors.setGlobalCursorByteBudget(8192);
        long small = watchCursor();
        long big = watchCursor();

        // keep `small` drained so `big` is the clear largest holder
        for (int i = 0; i < 5; i++) {
            insertLarge(i);
            awaitCondition(() -> cursors.bufferedEventCount(small) > 0);
            while (cursors.bufferedEventCount(small) > 0) {
                cursors.getMore(small, 100).get(5, TimeUnit.SECONDS);
            }
        }

        // now let `big` grow past the global budget while `small` stays drained
        for (int i = 5; i < 60 && cursors.hasCursor(big); i++) {
            insertLarge(i);
            // keep `small` drained so `big` remains the clear largest holder
            while (cursors.bufferedEventCount(small) > 0) {
                cursors.getMore(small, 100).get(5, TimeUnit.SECONDS);
            }
        }

        assertThat(cursors.hasCursor(big))
            .as("the largest holder must be the one evicted").isFalse();
        assertThat(cursors.hasCursor(small))
            .as("a drained cursor is not the largest holder and must survive").isTrue();
    }

    /**
     * The global byte counter is kept exactly in lockstep with the sum of live cursors' queued
     * bytes: after draining every cursor it is zero, and after killing every cursor (leaving
     * unread bytes behind) it is zero again - a drifting counter would silently disable the
     * global budget, the same rule as the per-cursor accounting.
     */
    @Test
    public void globalByteCounterReturnsToZeroAfterKillAndDrain() throws Exception {
        cursors.setCursorQueueByteBudget(64 * 1024 * 1024);
        cursors.setGlobalCursorByteBudget(0); // don't kill here - we want to observe accounting
        long c1 = watchCursor();
        long c2 = watchCursor();

        for (int i = 0; i < 5; i++) {
            insertLarge(i);
        }
        awaitCondition(() -> cursors.getTotalQueuedBytes() > 5 * 1024L);
        assertThat(cursors.getTotalQueuedBytes())
            .as("both cursors' buffered bytes must be reflected in the global total")
            .isGreaterThan(5 * 1024L);

        // drain both fully => global total returns to zero
        for (long id : new long[] {c1, c2}) {
            while (cursors.bufferedEventCount(id) > 0) {
                cursors.getMore(id, 100).get(5, TimeUnit.SECONDS);
            }
        }
        assertThat(cursors.getTotalQueuedBytes())
            .as("after draining every cursor the global total must be exactly zero")
            .isEqualTo(0L);

        // now kill both cursors while keying a fresh backlog: unread bytes must be released too
        cursors.setGlobalCursorByteBudget(1024);
        long c3 = watchCursor();
        for (int i = 0; i < 30 && cursors.hasCursor(c3); i++) {
            insertLarge(i);
        }
        assertThat(cursors.hasCursor(c3))
            .as("the parked cursor must be killed by the global cap").isFalse();
        assertThat(cursors.getTotalQueuedBytes())
            .as("killing a cursor must release its unread bytes from the global total")
            .isEqualTo(0L);
    }
}
