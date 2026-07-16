package de.caluga.poppydb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.junit.jupiter.api.Test;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.inmem.InMemoryDriver;

/**
 * Regression test for replication batching preserving global event order.
 *
 * This is a pure unit test around {@link ReplicationManager#applyEventsInOrder(List)},
 * the package-private seam that {@code processBatch} delegates to after draining the
 * event queue. It exercises the seam directly (no network, no full replica set) against
 * a local {@link InMemoryDriver}, which is exactly what the production code applies
 * events to.
 *
 * Placed in the same package as {@code ReplicationManager} (de.caluga.poppydb) rather
 * than under de.caluga.test.poppydb because the seam under test is intentionally kept
 * package-private (not exposed as public API) -- see task-6 report for rationale.
 */
public class ReplicationOrderingTest {

    private static long seq = 1;

    /**
     * Builds a change-stream-shaped event map matching the structure produced by
     * InMemoryDriver's buildChangeStreamEvent (operationType, ns, fullDocument,
     * documentKey, and a resume token carrying a monotonically increasing sequence
     * number in "_id._data").
     */
    private Map<String, Object> insertEvent(String db, String coll, Object id, Object value) {
        Map<String, Object> doc = new LinkedHashMap<>();
        doc.put("_id", id);
        doc.put("value", value);

        Map<String, Object> event = new LinkedHashMap<>();
        event.put("_id", Doc.of("_data", String.format(Locale.ROOT, "%016x", seq++)));
        event.put("operationType", "insert");
        event.put("ns", Doc.of("db", db, "coll", coll));
        event.put("fullDocument", doc);
        event.put("documentKey", Doc.of("_id", id));
        return event;
    }

    private Map<String, Object> deleteEvent(String db, String coll, Object id) {
        Map<String, Object> event = new LinkedHashMap<>();
        event.put("_id", Doc.of("_data", String.format(Locale.ROOT, "%016x", seq++)));
        event.put("operationType", "delete");
        event.put("ns", Doc.of("db", db, "coll", coll));
        event.put("documentKey", Doc.of("_id", id));
        return event;
    }

    /**
     * Core regression case: document A(id=1) already exists (from an earlier, already
     * applied batch), then one batch arrives containing delete(id=1) followed by
     * re-insert A(id=1, v=2). Sequential (correct) application: delete removes the old
     * doc, then the insert recreates it with v=2 -> final state: exists, v=2.
     *
     * Before the fix, processBatch grouped ALL inserts in the batch into one bulk insert
     * applied before ANY other event in the batch, so this became:
     *   bulk-insert[id=1,v=2] (duplicate-key error against the still-existing v=1 doc,
     *   silently swallowed by applyBulkInserts) -> delete(id=1)
     * i.e. insert-then-delete instead of delete-then-insert, which leaves the document
     * wrongly gone entirely instead of present with v=2. This is the "delete A -> re-insert
     * A in one batch becomes insert A (duplicate-key failure) then delete A" failure
     * described in the task.
     *
     * (Verified empirically: InMemoryDriver's insert() only checks _id uniqueness against
     * already-*committed* documents, not against duplicates within the very same bulk
     * insert call -- so a 3-events-in-one-batch reproduction (insert/delete/insert) would
     * not reliably surface the bug. Splitting the initial insert into its own, already-
     * applied batch is what makes the pre-existing document real/committed and the
     * duplicate-key check trigger, matching the task's documented concrete failure.)
     */
    @Test
    public void deleteThenReinsertOfExistingDocPreservesOrder() throws Exception {
        InMemoryDriver drv = new InMemoryDriver();
        drv.connect();

        try {
            ReplicationManager rm = new ReplicationManager(drv, "127.0.0.1", 1);

            // Pre-existing state: id=1 already committed by an earlier batch.
            rm.applyEventsInOrder(new ArrayList<>(List.of(insertEvent("testdb", "coll", 1, "v1"))));

            // The batch under test: delete(id=1) followed by re-insert A(id=1, v=2).
            List<Map<String, Object>> batch = new ArrayList<>();
            batch.add(deleteEvent("testdb", "coll", 1));
            batch.add(insertEvent("testdb", "coll", 1, "v2"));

            rm.applyEventsInOrder(batch);

            List<Map<String, Object>> found = drv.find("testdb", "coll", Doc.of("_id", 1), null, null, 0, 10);
            assertEquals(1, found.size(), "expected exactly one surviving document with id=1");
            assertNotNull(found.get(0));
            assertEquals("v2", found.get(0).get("value"));
        } finally {
            drv.close();
        }
    }

    /**
     * Sanity check that contiguous same-collection inserts interleaved with an insert
     * for a *different* collection still end up correct for both collections, and that
     * a trailing insert run (nothing after it) is flushed.
     */
    @Test
    public void contiguousRunsAreGroupedPerCollectionAndOrderAcrossCollectionsIsPreserved() throws Exception {
        InMemoryDriver drv = new InMemoryDriver();
        drv.connect();

        try {
            ReplicationManager rm = new ReplicationManager(drv, "127.0.0.1", 1);

            List<Map<String, Object>> batch = new ArrayList<>();
            batch.add(insertEvent("testdb", "a", 1, "a1"));
            batch.add(insertEvent("testdb", "a", 2, "a2"));
            batch.add(insertEvent("testdb", "b", 1, "b1"));
            batch.add(insertEvent("testdb", "a", 3, "a3"));

            rm.applyEventsInOrder(batch);

            List<Map<String, Object>> collA = drv.find("testdb", "a", Doc.of(), null, null, 0, 10);
            List<Map<String, Object>> collB = drv.find("testdb", "b", Doc.of(), null, null, 0, 10);

            assertEquals(3, collA.size());
            assertEquals(1, collB.size());
        } finally {
            drv.close();
        }
    }

    /**
     * Extracts the sequence number carried by an event's resume token, mirroring
     * ReplicationManager#extractSequenceFromEvent for test assertions.
     */
    @SuppressWarnings("unchecked")
    private long extractSeq(Map<String, Object> event) {
        Map<String, Object> idMap = (Map<String, Object>) event.get("_id");
        return Long.parseLong((String) idMap.get("_data"), 16);
    }

    /**
     * Regression test for A5: a failed bulk insert must not falsely advance
     * lastAppliedSequence to the batch max.
     *
     * Setup: id=1 already exists (committed by an earlier, already-applied batch). A
     * single contiguous insert run for the same collection then arrives containing
     * id=2 (new), id=3 (new), and id=1 again (duplicate of the pre-existing document).
     * InMemoryDriver's insert() is ordered by default and detects the duplicate _id
     * before writing anything, so the whole bulk insert command throws and (pre-fix)
     * none of id=2/id=3/id=1 are applied -- yet applyBulkInserts unconditionally
     * advanced lastAppliedSequence to the run's max sequence (the duplicate id=1
     * event, since it was queued last), falsely telling the primary this run was
     * fully replicated.
     *
     * Required behavior: on bulk failure, fall back to applying each event
     * one-by-one via applyChangeEvent, so id=2 and id=3 (which don't conflict) still
     * get applied and their sequence advanced, while the poison id=1 event fails on
     * its own without blocking or falsely acknowledging the rest of the run. Final
     * lastAppliedSequence must equal the sequence of the last event that actually
     * succeeded (id=3), not the batch max (the failed id=1 event).
     */
    @Test
    public void bulkInsertFailureOnlyAdvancesSequenceForSuccessfulEvents() throws Exception {
        InMemoryDriver drv = new InMemoryDriver();
        drv.connect();

        try {
            ReplicationManager rm = new ReplicationManager(drv, "127.0.0.1", 1);

            // Pre-existing state: id=1 already committed by an earlier, already-applied batch.
            rm.applyEventsInOrder(new ArrayList<>(List.of(insertEvent("testdb", "coll", 1, "v1"))));

            Map<String, Object> insert2 = insertEvent("testdb", "coll", 2, "v2");
            Map<String, Object> insert3 = insertEvent("testdb", "coll", 3, "v3");
            Map<String, Object> insertDup = insertEvent("testdb", "coll", 1, "dup");

            long seqOf3 = extractSeq(insert3);
            long seqOfDup = extractSeq(insertDup);
            assertTrue(seqOfDup > seqOf3, "test setup: duplicate event must have the highest sequence in the run");

            List<Map<String, Object>> batch = new ArrayList<>(List.of(insert2, insert3, insertDup));
            rm.applyEventsInOrder(batch);

            // id=1 stays as the original v1 -- the duplicate insert failed and did not
            // overwrite it.
            List<Map<String, Object>> found1 = drv.find("testdb", "coll", Doc.of("_id", 1), null, null, 0, 10);
            assertEquals(1, found1.size());
            assertEquals("v1", found1.get(0).get("value"));

            // id=2 and id=3 were applied via the per-event fallback despite the bulk failure.
            List<Map<String, Object>> found2 = drv.find("testdb", "coll", Doc.of("_id", 2), null, null, 0, 10);
            assertEquals(1, found2.size());
            List<Map<String, Object>> found3 = drv.find("testdb", "coll", Doc.of("_id", 3), null, null, 0, 10);
            assertEquals(1, found3.size());

            assertEquals(seqOf3, rm.getLastAppliedSequence(),
                "lastAppliedSequence must reflect the last successfully applied event, not the batch max");
        } finally {
            drv.close();
        }
    }

    @Test
    public void emptyBatchIsSafe() throws Exception {
        InMemoryDriver drv = new InMemoryDriver();
        drv.connect();

        try {
            ReplicationManager rm = new ReplicationManager(drv, "127.0.0.1", 1);
            rm.applyEventsInOrder(new ArrayList<>());
            // no exception, nothing to assert beyond "didn't blow up"
        } finally {
            drv.close();
        }
    }
}
