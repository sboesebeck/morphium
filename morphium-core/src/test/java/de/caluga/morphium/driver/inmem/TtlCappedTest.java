package de.caluga.morphium.driver.inmem;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.commands.InsertMongoCommand;
import de.caluga.test.mongo.suite.base.TestUtils;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Driver-level tests for the TTL expiry queue and capped-collection byte counter (Phase B2, Task
 * 4): {@code InMemoryDriver}'s TTL sweep is queue-driven (a {@code PriorityQueue} ordered by
 * absolute expiry instant, fed on insert/update, bootstrapped from a range scan of the TTL field's
 * own {@link CollectionIndexStore} index) instead of scanning every live document on every tick,
 * and a capped collection's eviction loop compares against a running byte counter (each document's
 * size measured exactly once, at insertion) instead of re-measuring the whole collection with JOL
 * on every eviction iteration. Lives in the driver's own package to reach the package-private
 * {@code ttlEntriesChecked}/{@code cappedSizeOfCalls} counters and {@code cappedCurrentBytes}.
 */
@Tag("inmemory")
public class TtlCappedTest {
    private final String db = "ttlcappeddb";
    private final String coll = "ttlcappedcoll";

    private InMemoryDriver freshDriver() throws Exception {
        InMemoryDriver drv = new InMemoryDriver();
        drv.connect();
        return drv;
    }

    @Test
    void ttlDocExpiresWithinOneSweepAfterItsTime() throws Exception {
        InMemoryDriver drv = freshDriver();
        drv.setExpireCheck(100);
        drv.createIndex(db, coll, Doc.of("expiresAt", 1), Doc.of("name", "ttl_1", "expireAfterSeconds", 0));

        new InsertMongoCommand(drv).setDb(db).setColl(coll)
                .setDocuments(List.of(Doc.of("counter", 1, "expiresAt", new Date(System.currentTimeMillis() - 5000))))
                .execute();
        assertEquals(1, drv.find(db, coll, Doc.of(), null, null, 0, 0).size(),
                "sanity: document exists before the TTL sweep runs");

        TestUtils.waitForConditionToBecomeTrue(10_000, "TTL-expired document was never removed",
                () -> drv.find(db, coll, Doc.of(), null, null, 0, 0).isEmpty());

        assertTrue(drv.find(db, coll, Doc.of(), null, null, 0, 0).isEmpty());
    }

    @Test
    void ttlSweepOverLargeCollectionTouchesOnlyDueDocsNotAFullScan() throws Exception {
        InMemoryDriver drv = freshDriver();
        drv.setExpireCheck(100);
        drv.createIndex(db, coll, Doc.of("expiresAt", 1), Doc.of("name", "ttl_1", "expireAfterSeconds", 0));

        int notDueCount = 100_000;
        int dueCount = 10;
        List<Map<String, Object>> docs = new ArrayList<>(notDueCount + dueCount);
        long farFuture = System.currentTimeMillis() + 3_600_000L;
        long past = System.currentTimeMillis() - 5_000L;
        for (int i = 0; i < notDueCount; i++) {
            docs.add(Doc.of("counter", i, "expiresAt", new Date(farFuture)));
        }
        for (int i = 0; i < dueCount; i++) {
            docs.add(Doc.of("counter", notDueCount + i, "expiresAt", new Date(past)));
        }
        // NOTE: no "every document present right after insert()" sanity check here - the
        // background sweep's very first tick fires a fixed 100ms after scheduling regardless of
        // expireCheck (same as before this task), and inserting 100_010 documents reliably takes
        // longer than that. The 10 due documents can therefore legitimately already be gone by
        // the time insert() returns - that is correct behaviour (they were already due), not a
        // race to guard against.
        new InsertMongoCommand(drv).setDb(db).setColl(coll).setDocuments(docs).execute();

        TestUtils.waitForConditionToBecomeTrue(10_000, "the 10 due documents were never removed",
                () -> drv.find(db, coll, Doc.of(), null, null, 0, 0).size() == notDueCount);

        // Give the sweep a couple more ticks to prove it settles rather than keeps working.
        Thread.sleep(300);

        assertEquals(notDueCount, drv.find(db, coll, Doc.of(), null, null, 0, 0).size(),
                "only the 10 due documents must have been removed");
        assertTrue(drv.ttlEntriesChecked < 50,
                "a queue-driven sweep must touch O(#due) documents, not the whole " + (notDueCount + dueCount)
                        + "-document collection - checked " + drv.ttlEntriesChecked);
        assertTrue(drv.ttlEntriesChecked >= dueCount,
                "every one of the " + dueCount + " due documents must have been checked at least once");
    }

    @Test
    void updatingTheTtlFieldPushesOutTheOldExpiryNotJustTheNewOne() throws Exception {
        InMemoryDriver drv = freshDriver();
        drv.setExpireCheck(100);
        drv.createIndex(db, coll, Doc.of("expiresAt", 1), Doc.of("name", "ttl_1", "expireAfterSeconds", 2));

        Date original = new Date();
        var insertResult = new InsertMongoCommand(drv).setDb(db).setColl(coll)
                .setDocuments(List.of(Doc.of("counter", 1, "expiresAt", original)))
                .execute();
        List<Map<String, Object>> inserted = drv.find(db, coll, Doc.of(), null, null, 0, 0);
        assertEquals(1, inserted.size());
        Object id = inserted.get(0).get("_id");

        // Push the expiry far into the future before the original (2s) deadline arrives. The old
        // queue entry (keyed to "original") is never removed - it is left to become a stale,
        // discarded pop once the sweep gets to it.
        Date pushedOut = new Date(System.currentTimeMillis() + 3_600_000L);
        drv.update(db, coll, Doc.of("_id", id), null, Doc.of("$set", Doc.of("expiresAt", pushedOut)), false, false,
                null, null);

        // Wait well past the ORIGINAL (now stale) 2s deadline.
        Thread.sleep(4_000);

        assertEquals(1, drv.find(db, coll, Doc.of(), null, null, 0, 0).size(),
                "the document must NOT have expired at its old (superseded) TTL time");
        assertTrue(drv.ttlEntriesChecked >= 1,
                "the stale queue entry from before the update must actually have been popped and "
                        + "re-checked (then discarded), not merely ignored");
    }

    @Test
    void cappedCollectionEvictsOldestDocsOnceMaxCountIsExceeded() throws Exception {
        InMemoryDriver drv = freshDriver();
        drv.registerCappedCollection(db, coll, Integer.MAX_VALUE, 5);

        List<Map<String, Object>> docs = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            docs.add(Doc.of("counter", i));
        }
        new InsertMongoCommand(drv).setDb(db).setColl(coll).setDocuments(docs).execute();

        List<Map<String, Object>> remaining = drv.find(db, coll, Doc.of(), null, Doc.of("counter", 1), 0, 0);
        assertEquals(5, remaining.size(), "capped collection must respect its max document count");
        // FIFO eviction: the oldest (lowest counter) documents must be the ones gone.
        for (Map<String, Object> doc : remaining) {
            assertTrue(((Number) doc.get("counter")).intValue() >= 5,
                    "the surviving documents must be the most recently inserted ones");
        }
    }

    @Test
    void cappedCollectionByteEvictionUsesTheCounterNotAJolMeasurePerIteration() throws Exception {
        InMemoryDriver drv = freshDriver();
        // Generous cap for now - just enough to establish this driver's own measured per-document
        // size without triggering any eviction yet.
        drv.registerCappedCollection(db, coll, Integer.MAX_VALUE, Integer.MAX_VALUE);
        new InsertMongoCommand(drv).setDb(db).setColl(coll)
                .setDocuments(List.of(Doc.of("counter", -1, "payload", "probe"))).execute();
        long oneDocBytes = drv.cappedCurrentBytes(db, coll);
        assertTrue(oneDocBytes > 0, "sanity: the probe document must have been measured");
        drv.delete(db, coll, Doc.of("counter", -1), null, false, null, null);

        // Fill up with 20 individually-inserted documents (one cappedOnInsert/sizeOf call each).
        int initialCount = 20;
        for (int i = 0; i < initialCount; i++) {
            new InsertMongoCommand(drv).setDb(db).setColl(coll)
                    .setDocuments(List.of(Doc.of("counter", i, "payload", "payload" + i))).execute();
        }
        assertEquals(initialCount, drv.find(db, coll, Doc.of(), null, null, 0, 0).size());

        // Tighten the size cap well below what the 20 existing documents already occupy, so the
        // next batch's eviction loop has to evict most/all of them in one insert() call.
        drv.registerCappedCollection(db, coll, (int) (oneDocBytes * 5), Integer.MAX_VALUE);

        long sizeOfCallsBefore = drv.cappedSizeOfCalls;
        int batchSize = 5;
        List<Map<String, Object>> batch = new ArrayList<>();
        for (int i = 0; i < batchSize; i++) {
            batch.add(Doc.of("counter", 1000 + i, "payload", "batch" + i));
        }
        new InsertMongoCommand(drv).setDb(db).setColl(coll).setDocuments(batch).execute();

        long sizeOfCallsAfter = drv.cappedSizeOfCalls;
        List<Map<String, Object>> finalDocs = drv.find(db, coll, Doc.of(), null, null, 0, 0);

        assertTrue(finalDocs.size() < initialCount + batchSize,
                "the tightened cap must actually have evicted some of the 20 pre-existing documents - "
                        + "otherwise this test proves nothing about per-iteration sizeOf calls");
        assertEquals(batchSize + 1, sizeOfCallsAfter - sizeOfCallsBefore,
                "exactly one sizeOf call per newly-inserted document plus one for the incoming batch "
                        + "as a whole - NEVER one per evicted document, regardless of how many were evicted");
    }
}
