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

    /**
     * Same shape as {@link #insertEvent}, but with an "email" field instead of "value" --
     * used by the unique-secondary-index tests below, which need a field other than _id
     * to violate a unique index on.
     */
    private Map<String, Object> insertEmailEvent(String db, String coll, Object id, String email) {
        Map<String, Object> doc = new LinkedHashMap<>();
        doc.put("_id", id);
        doc.put("email", email);

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
     * lastAppliedSequence to the batch max, and the per-event fallback must be an
     * idempotent replay (not a strict insert) so a plain _id "conflict" against an
     * already-committed document converges instead of being permanently blocked.
     *
     * Setup: id=1 already exists (committed by an earlier, already-applied batch). A
     * single contiguous insert run for the same collection then arrives containing
     * id=2 (new), id=3 (new), and id=1 again with different content. InMemoryDriver's
     * insert() is ordered by default and detects the duplicate _id before writing
     * anything, so the whole bulk insert command throws.
     *
     * Required behavior: on bulk failure, fall back to applying each event
     * one-by-one via applyChangeEvent in replay mode (applyInsertIdempotent), which
     * applies an insert as a full-document upsert-by-key rather than a strict insert --
     * the same technique already used for replicated update/replace events. That makes
     * id=1's replay a harmless replace (converging to the new content) instead of a
     * permanent duplicate-key failure, so all three events in the run succeed and
     * lastAppliedSequence reaches the true batch max.
     */
    @Test
    public void bulkInsertFailureConvergesViaIdempotentReplayForDuplicateId() throws Exception {
        InMemoryDriver drv = new InMemoryDriver();
        drv.connect();

        try {
            ReplicationManager rm = new ReplicationManager(drv, "127.0.0.1", 1);

            // Pre-existing state: id=1 already committed by an earlier, already-applied batch.
            rm.applyEventsInOrder(new ArrayList<>(List.of(insertEvent("testdb", "coll", 1, "v1"))));

            Map<String, Object> insert2 = insertEvent("testdb", "coll", 2, "v2");
            Map<String, Object> insert3 = insertEvent("testdb", "coll", 3, "v3");
            Map<String, Object> insertDup = insertEvent("testdb", "coll", 1, "dup");
            long seqOfDup = extractSeq(insertDup);

            List<Map<String, Object>> batch = new ArrayList<>(List.of(insert2, insert3, insertDup));
            rm.applyEventsInOrder(batch);

            // id=1 converges to the replayed content -- the idempotent replay applies it
            // as a full-document upsert-by-key rather than a strict (permanently-failing)
            // insert.
            List<Map<String, Object>> found1 = drv.find("testdb", "coll", Doc.of("_id", 1), null, null, 0, 10);
            assertEquals(1, found1.size());
            assertEquals("dup", found1.get(0).get("value"));

            List<Map<String, Object>> found2 = drv.find("testdb", "coll", Doc.of("_id", 2), null, null, 0, 10);
            assertEquals(1, found2.size());
            List<Map<String, Object>> found3 = drv.find("testdb", "coll", Doc.of("_id", 3), null, null, 0, 10);
            assertEquals(1, found3.size());

            // All three events converged, so the sequence reaches the true batch max.
            assertEquals(seqOfDup, rm.getLastAppliedSequence(),
                "lastAppliedSequence must reach the batch max once every event in the run has converged");
        } finally {
            drv.close();
        }
    }

    /**
     * (a) Regression test for review issue #1 ("writeErrors-failures still falsely
     * ack"): InMemoryDriver does not throw for unique-secondary-index violations --
     * insert() silently commits the non-conflicting documents in the same call and
     * reports the rest as `writeErrors` in the command result, without ever throwing.
     * applyBulkInserts must inspect that result and treat a non-empty writeErrors as a
     * bulk failure -- not silently trust the call as full success -- falling back to
     * the same per-event idempotent replay used for a thrown failure.
     *
     * The conflicting document (id=3) doesn't exist yet, so its replay goes through
     * applyInsertIdempotent's insert-via-upsert path, which InMemoryDriver *does*
     * enforce uniqueness for (via storeInternal) -- so it correctly fails there too,
     * exactly as it did in the initial bulk call: this is a genuine, persistent
     * conflict, not a replay artifact, and must never be created. The bug this test
     * guards against is specifically the false ack: pre-fix, lastAppliedSequence was
     * unconditionally advanced to the run's max sequence regardless of writeErrors,
     * claiming id=3's insert was replicated when it never actually applied. Fixed
     * behavior: the sequence stops at the last event that *actually* succeeded (id=2),
     * not the batch max (id=3, which permanently failed).
     */
    @Test
    public void writeErrorsFromBulkInsertTriggerFallbackAndDoNotFalselyAckTheConflict() throws Exception {
        InMemoryDriver drv = new InMemoryDriver();
        drv.connect();

        try {
            drv.createIndex("testdb", "coll", Doc.of("email", 1), Doc.of("unique", true));

            ReplicationManager rm = new ReplicationManager(drv, "127.0.0.1", 1);

            // Pre-existing document already owns "taken@example.com".
            rm.applyEventsInOrder(new ArrayList<>(
                List.of(insertEmailEvent("testdb", "coll", 1, "taken@example.com"))));

            Map<String, Object> insertOk = insertEmailEvent("testdb", "coll", 2, "new@example.com");
            Map<String, Object> insertConflict = insertEmailEvent("testdb", "coll", 3, "taken@example.com");
            long seqOfOk = extractSeq(insertOk);
            long seqOfConflict = extractSeq(insertConflict);
            assertTrue(seqOfConflict > seqOfOk,
                "test setup: the unresolved conflict must be last/highest sequence in the run");

            List<Map<String, Object>> batch = new ArrayList<>(List.of(insertOk, insertConflict));
            rm.applyEventsInOrder(batch);

            // id=2 was written directly by the initial (partially-successful) bulk call,
            // and survives the fallback replay unchanged.
            List<Map<String, Object>> found2 = drv.find("testdb", "coll", Doc.of("_id", 2), null, null, 0, 10);
            assertEquals(1, found2.size());

            // id=3 is a genuine, persistent unique-index conflict (a different document
            // already owns that email) -- it must never be created, in the initial bulk
            // call or on replay.
            List<Map<String, Object>> found3 = drv.find("testdb", "coll", Doc.of("_id", 3), null, null, 0, 10);
            assertEquals(0, found3.size(), "a genuinely conflicting document must never be created");

            // The key regression check: lastAppliedSequence must NOT be falsely advanced
            // to the batch max (id=3's sequence) just because the initial bulk call
            // returned without throwing -- it must reflect only what actually succeeded.
            assertEquals(seqOfOk, rm.getLastAppliedSequence(),
                "lastAppliedSequence must not falsely ack the sequence of a document that was never applied");
        } finally {
            drv.close();
        }
    }

    /**
     * (b) Regression test for review issue #2 ("fallback replay can permanently stall
     * the sequence when the bulk fails after landing part of the run"): applyBulkInserts'
     * per-event fallback must converge the run and leave the watermark at the last
     * successfully-applied event, never at the batch max when the highest-sequence event
     * never applied.
     *
     * Actual mechanism exercised here (verified against InMemoryDriver's behaviour): the run
     * is [id=2 (already exists), id=3 (new), id=6 (new, but its email collides with the
     * already-present id=5)]. When the collection has a unique SECONDARY index, InMemoryDriver
     * does NOT throw on the run's conflicts -- it commits the non-conflicting document and
     * returns the rest as writeErrors. So the initial bulk PARTIALLY LANDS: it durably writes
     * id=3 (index 1) and reports writeErrors for id=2 (index 0, _id duplicate) and id=6
     * (index 2, email unique conflict). applyBulkInserts treats any writeErrors as a whole-bulk
     * failure and re-runs every event through the idempotent per-event fallback: id=2 and id=3
     * become harmless upsert no-ops (both already present), while id=6's genuine, persistent
     * unique-index conflict fails on its own without blocking the rest. Everything applicable
     * converges and lastAppliedSequence stops at id=3's sequence (the last successfully-applied
     * event), not id=6's (the batch max, which never applied).
     *
     * This is precisely the "bulk atomicity is not guaranteed in general" case: a partial
     * writeErrors failure durably lands some of the run before failing, which is why the
     * fallback must be an idempotent replay rather than a naive full re-insert.
     */
    @Test
    public void partiallyLandedRunConvergesAndSequenceStopsAtLastSuccess() throws Exception {
        InMemoryDriver drv = new InMemoryDriver();
        drv.connect();

        try {
            drv.createIndex("testdb", "coll", Doc.of("email", 1), Doc.of("unique", true));

            ReplicationManager rm = new ReplicationManager(drv, "127.0.0.1", 1);

            // Simulates already-landed state (e.g. from an earlier, already-applied
            // batch): id=1 and id=2 already exist, and id=5 already owns
            // "taken@example.com".
            rm.applyEventsInOrder(new ArrayList<>(List.of(
                insertEmailEvent("testdb", "coll", 1, "e1@example.com"),
                insertEmailEvent("testdb", "coll", 2, "e2@example.com"),
                insertEmailEvent("testdb", "coll", 5, "taken@example.com"))));

            // The run under test: id=2 is re-sent unchanged (replay of already-landed
            // data), id=3 is genuinely new and non-conflicting, and id=6 is genuinely
            // new but collides with id=5's email -- a real, persistent conflict, not a
            // replay artifact.
            Map<String, Object> replay2 = insertEmailEvent("testdb", "coll", 2, "e2@example.com");
            Map<String, Object> insert3 = insertEmailEvent("testdb", "coll", 3, "e3@example.com");
            Map<String, Object> insertConflict6 = insertEmailEvent("testdb", "coll", 6, "taken@example.com");

            long seqOf3 = extractSeq(insert3);
            long seqOfConflict6 = extractSeq(insertConflict6);
            assertTrue(seqOfConflict6 > seqOf3,
                "test setup: the unresolved conflict must be last/highest sequence in the run");

            List<Map<String, Object>> batch = new ArrayList<>(List.of(replay2, insert3, insertConflict6));
            rm.applyEventsInOrder(batch);

            // id=1 is untouched by this run.
            List<Map<String, Object>> found1 = drv.find("testdb", "coll", Doc.of("_id", 1), null, null, 0, 10);
            assertEquals(1, found1.size());
            assertEquals("e1@example.com", found1.get(0).get("email"));

            // id=2 is unchanged: it already existed, so the bulk reported it as a writeError
            // (index 0) and the per-event fallback re-applied it as a harmless upsert no-op.
            List<Map<String, Object>> found2 = drv.find("testdb", "coll", Doc.of("_id", 2), null, null, 0, 10);
            assertEquals(1, found2.size());
            assertEquals("e2@example.com", found2.get(0).get("email"));

            // id=3 is present -- durably written by the initial bulk (index 1 committed while
            // id=2 and id=6 were reported as writeErrors), then re-applied as an idempotent
            // no-op by the fallback replay.
            List<Map<String, Object>> found3 = drv.find("testdb", "coll", Doc.of("_id", 3), null, null, 0, 10);
            assertEquals(1, found3.size());

            // id=5 is untouched, and id=6 (the genuine conflict) was never created.
            List<Map<String, Object>> found5 = drv.find("testdb", "coll", Doc.of("_id", 5), null, null, 0, 10);
            assertEquals(1, found5.size());
            assertEquals("taken@example.com", found5.get(0).get("email"));
            List<Map<String, Object>> found6 = drv.find("testdb", "coll", Doc.of("_id", 6), null, null, 0, 10);
            assertEquals(0, found6.size(), "a genuinely conflicting document must never be created");

            // lastAppliedSequence stops at the last *successfully* applied event (id=3),
            // not the batch max (id=6, which never actually applied).
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
