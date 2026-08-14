package de.caluga.test.mongo.suite.inmem;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.bson.BsonEncoder;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Date;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for the byte budget on the change-stream replay buffer (spec
 * docs/superpowers/specs/2026-08-14-replay-buffer-byte-budget.md).
 *
 * <p>The buffer is count-capped ({@code setChangeStreamHistoryLimit}) but used to be unbounded
 * by bytes: every buffered event retains its full document, so 100k bulk-write events could
 * pin several GB of heap (ACC incident 2026-08-14). The byte budget evicts oldest events once
 * the estimated buffered bytes exceed it — same window-lost semantics as count overflow.
 */
@Tag("inmemory")
public class ChangeStreamHistoryByteBudgetTest {

    private static final String DB = "bytebudget";

    private static Map<String, Object> bigDoc(int i, int payloadBytes) {
        return Doc.of("_id", "big" + i, "payload", "x".repeat(payloadBytes));
    }

    private static InMemoryDriver freshDriver() throws Exception {
        InMemoryDriver drv = new InMemoryDriver();
        drv.connect();
        return drv;
    }

    @Test
    public void budgetEvictsOldestUntilUnderBudget() throws Exception {
        InMemoryDriver drv = freshDriver();
        try {
            drv.setChangeStreamHistoryByteBudget(100 * 1024);
            for (int i = 0; i < 50; i++) {
                drv.store(DB, "coll", List.of(bigDoc(i, 10 * 1024)), null);
            }
            assertTrue(drv.getChangeStreamHistorySize() > 1, "several events must fit the budget");
            assertTrue(drv.getChangeStreamHistorySize() < 50, "budget must have evicted old events");
            assertTrue(drv.getChangeStreamHistoryBytes() <= 100 * 1024,
                    "buffered bytes must not exceed the budget (was " + drv.getChangeStreamHistoryBytes() + ")");
            // oldest events are gone -> a resume token from the evicted range is window-lost
            assertFalse(drv.canResumeChangeStream(1),
                    "token in the evicted range must not be resumable");
            // the newest event is always retained -> caught-up consumers resume fine
            assertTrue(drv.canResumeChangeStream(drv.getChangeStreamSequence()),
                    "a caught-up consumer must be resumable");
        } finally {
            drv.close();
        }
    }

    @Test
    public void countLimitStillEnforcedIndependently() throws Exception {
        InMemoryDriver drv = freshDriver();
        try {
            drv.setChangeStreamHistoryLimit(5);
            drv.setChangeStreamHistoryByteBudget(Long.MAX_VALUE);
            for (int i = 0; i < 10; i++) {
                drv.store(DB, "coll", List.of(Doc.of("_id", "s" + i, "v", i)), null);
            }
            assertEquals(5, drv.getChangeStreamHistorySize(),
                    "count limit must evict independent of a generous byte budget");
        } finally {
            drv.close();
        }
    }

    @Test
    public void oversizedEventIsKeptAsOnlyEntry() throws Exception {
        InMemoryDriver drv = freshDriver();
        try {
            drv.setChangeStreamHistoryByteBudget(1024);
            drv.store(DB, "coll", List.of(bigDoc(1, 64 * 1024)), null);
            assertEquals(1, drv.getChangeStreamHistorySize(),
                    "an event bigger than the budget must still be buffered");
            // a second oversized event replaces the first instead of looping forever
            drv.store(DB, "coll", List.of(bigDoc(2, 64 * 1024)), null);
            assertEquals(1, drv.getChangeStreamHistorySize(),
                    "the newest oversized event must replace the previous one");
        } finally {
            drv.close();
        }
    }

    @Test
    public void dropPurgesKeepByteCounterConsistent() throws Exception {
        InMemoryDriver drv = freshDriver();
        try {
            drv.setChangeStreamHistoryByteBudget(Long.MAX_VALUE);
            for (int i = 0; i < 5; i++) {
                drv.store(DB, "collA", List.of(bigDoc(i, 8 * 1024)), null);
                drv.store(DB, "collB", List.of(bigDoc(100 + i, 8 * 1024)), null);
            }
            long before = drv.getChangeStreamHistoryBytes();
            assertTrue(before > 0);

            drv.drop(DB, "collA", null);
            long afterCollDrop = drv.getChangeStreamHistoryBytes();
            assertTrue(afterCollDrop < before, "dropping collA must release its buffered event bytes");
            assertTrue(afterCollDrop > 0, "collB events must still be buffered");

            // drop(db) purges all buffered events, then appends one small dropDatabase
            // notification event - only that may remain
            drv.drop(DB, null);
            assertTrue(drv.getChangeStreamHistorySize() <= 1,
                    "at most the dropDatabase notification may remain buffered");
            assertTrue(drv.getChangeStreamHistoryBytes() < 4096,
                    "all big event bytes must be purged, was " + drv.getChangeStreamHistoryBytes());
        } finally {
            drv.close();
        }
    }

    @Test
    public void shrinkingBudgetTrimsImmediately_zeroDisables() throws Exception {
        InMemoryDriver drv = freshDriver();
        try {
            for (int i = 0; i < 20; i++) {
                drv.store(DB, "coll", List.of(bigDoc(i, 10 * 1024)), null);
            }
            long unbounded = drv.getChangeStreamHistoryBytes();
            assertTrue(unbounded > 50 * 1024, "default budget 0 must not evict by bytes");
            assertEquals(20, drv.getChangeStreamHistorySize());

            drv.setChangeStreamHistoryByteBudget(50 * 1024);
            assertTrue(drv.getChangeStreamHistoryBytes() <= 50 * 1024,
                    "shrinking the budget must trim immediately");
            assertTrue(drv.getChangeStreamHistorySize() < 20);

            drv.setChangeStreamHistoryByteBudget(0); // off again
            for (int i = 100; i < 120; i++) {
                drv.store(DB, "coll", List.of(bigDoc(i, 10 * 1024)), null);
            }
            assertTrue(drv.getChangeStreamHistoryBytes() > 50 * 1024,
                    "budget 0 must disable byte eviction again");

            assertThrows(IllegalArgumentException.class, () -> drv.setChangeStreamHistoryByteBudget(-1));
        } finally {
            drv.close();
        }
    }

    @Test
    public void resumeWindowSurvivesWithinRetainedRange() throws Exception {
        InMemoryDriver drv = freshDriver();
        try {
            drv.setChangeStreamHistoryByteBudget(100 * 1024);
            for (int i = 0; i < 30; i++) {
                drv.store(DB, "coll", List.of(bigDoc(i, 10 * 1024)), null);
            }
            long newest = drv.getChangeStreamSequence();
            // a token just before the newest event lies inside the retained window
            assertTrue(drv.canResumeChangeStream(newest - 1),
                    "token within the retained window must be resumable");
            assertFalse(drv.canResumeChangeStream(1),
                    "token before the byte-evicted range must force a re-sync");
        } finally {
            drv.close();
        }
    }

    @Test
    public void estimatorTracksBsonSizeWithinFactorTwo() {
        Map<String, Object>[] docs = new Map[] {
            Doc.of("_id", "a", "s", "hello world", "n", 42, "d", 3.14, "b", true),
            Doc.of("_id", "b", "bin", new byte[4096], "date", new Date()),
            Doc.of("_id", "c", "nested", Doc.of("x", List.of(1, 2, 3), "y", Doc.of("z", "deep")),
                   "list", List.of("one", "two", "three")),
            bigDoc(1, 32 * 1024),
        };

        for (Map<String, Object> doc : docs) {
            long bson = BsonEncoder.encodeDocument(doc).length;
            long est = InMemoryDriver.estimateBsonSize(doc);
            assertTrue(est >= bson / 2 && est <= bson * 2,
                    "estimate " + est + " must be within factor 2 of BSON size " + bson + " for " + doc.keySet());
        }
    }
}
