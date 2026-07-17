package de.caluga.morphium.driver.inmem;

import de.caluga.morphium.IndexDescription;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.commands.CreateIndexesCommand;
import de.caluga.morphium.driver.commands.InsertMongoCommand;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Driver-level integration test for the {@link IndexPlanner}-backed read path (Phase B1, Task 3):
 * find/count must route through the planner and {@link CollectionIndexStore}, and a genuine
 * negative index lookup must NOT fall back to a full scan. Lives in the driver's own package to
 * reach the package-private {@code fullScans}/{@code indexHits} counters.
 */
@Tag("inmemory")
public class InMemoryDriverIndexPlanningTest {
    private final String db = "idxplandb";
    private final String coll = "idxplancoll";

    private InMemoryDriver freshDriverWithIndexedCollection(int docCount) throws Exception {
        InMemoryDriver drv = new InMemoryDriver();
        drv.connect();
        new CreateIndexesCommand(drv).setDb(db).setColl(coll)
                .addIndex(new IndexDescription().setKey(Doc.of("counter", 1)))
                .execute();

        List<Map<String, Object>> docs = new ArrayList<>();
        for (int i = 0; i < docCount; i++) {
            docs.add(Doc.of("counter", i));
        }
        new InsertMongoCommand(drv).setDb(db).setColl(coll).setDocuments(docs).execute();
        return drv;
    }

    @Test
    void findOnIndexedFieldHitsIndexAndReturnsCorrectResult() throws Exception {
        InMemoryDriver drv = freshDriverWithIndexedCollection(10_000);

        long fullScansBefore = drv.fullScans;
        long indexHitsBefore = drv.indexHits;

        List<Map<String, Object>> result = drv.find(db, coll, Doc.of("counter", 4711), null, null, 0, 0);

        assertEquals(1, result.size());
        assertEquals(4711, result.get(0).get("counter"));
        assertEquals(indexHitsBefore + 1, drv.indexHits, "index-backed find must increment indexHits");
        assertEquals(fullScansBefore, drv.fullScans, "index-backed find must NOT increment fullScans");
    }

    @Test
    void findOnUnindexedFieldFallsBackToFullScan() throws Exception {
        InMemoryDriver drv = freshDriverWithIndexedCollection(50);
        // "counter" is indexed, "other" is not.
        for (Map<String, Object> doc : drv.find(db, coll, Doc.of(), null, null, 0, 0)) {
            // no-op, just make sure the collection is populated
        }

        long fullScansBefore = drv.fullScans;
        long indexHitsBefore = drv.indexHits;

        List<Map<String, Object>> result = drv.find(db, coll, Doc.of("other", 5), null, null, 0, 0);

        assertTrue(result.isEmpty());
        assertEquals(fullScansBefore + 1, drv.fullScans, "query on an unindexed field must full-scan");
        assertEquals(indexHitsBefore, drv.indexHits);
    }

    @Test
    void findWithIndexButNoMatchReturnsEmptyWithoutFullScan() throws Exception {
        InMemoryDriver drv = freshDriverWithIndexedCollection(50);

        long fullScansBefore = drv.fullScans;
        long indexHitsBefore = drv.indexHits;

        List<Map<String, Object>> result = drv.find(db, coll, Doc.of("counter", 999_999), null, null, 0, 0);

        assertTrue(result.isEmpty(), "no document has counter=999999");
        assertEquals(indexHitsBefore + 1, drv.indexHits, "a negative index lookup is still an index hit");
        assertEquals(fullScansBefore, drv.fullScans, "a negative index lookup must NOT fall back to a full scan");
    }

    @Test
    void countOnIndexedFieldHitsIndexAndReturnsCorrectResult() throws Exception {
        InMemoryDriver drv = freshDriverWithIndexedCollection(10_000);

        long fullScansBefore = drv.fullScans;
        long indexHitsBefore = drv.indexHits;

        long count = drv.count(db, coll, Doc.of("counter", 4711), null, null);

        assertEquals(1, count);
        assertEquals(indexHitsBefore + 1, drv.indexHits);
        assertEquals(fullScansBefore, drv.fullScans);
    }

    @Test
    void renameOntoCollectionInvalidatesItsCachedPlanIndexes() throws Exception {
        // Target collection: indexed on counter, populated, and queried once so the
        // single-index plan cache holds a store built over the OLD documents.
        InMemoryDriver drv = freshDriverWithIndexedCollection(50);
        List<Map<String, Object>> before = drv.find(db, coll, de.caluga.morphium.driver.Doc.of("counter", 7), null, null, 0, 0);
        assertEquals(1, before.size(), "sanity: old doc must be found before the rename");

        // Source collection with entirely different counter values.
        String source = "renamesrc";
        List<Map<String, Object>> srcDocs = new ArrayList<>();
        for (int i = 100_000; i < 100_010; i++) {
            srcDocs.add(de.caluga.morphium.driver.Doc.of("counter", i));
        }
        new InsertMongoCommand(drv).setDb(db).setColl(source).setDocuments(srcDocs).execute();

        // Rename source ONTO the (existing) target - replaces target's document list.
        drv.runCommand(new de.caluga.morphium.driver.commands.RenameCollectionCommand(drv)
                .setDb(db).setColl(source).setTo(coll));

        // Queries against the target must now reflect the renamed-in documents, not a
        // stale cached index over the old ones.
        List<Map<String, Object>> newDoc = drv.find(db, coll, de.caluga.morphium.driver.Doc.of("counter", 100_005), null, null, 0, 0);
        assertEquals(1, newDoc.size(), "renamed-in document must be findable after the rename");

        List<Map<String, Object>> oldDoc = drv.find(db, coll, de.caluga.morphium.driver.Doc.of("counter", 7), null, null, 0, 0);
        assertTrue(oldDoc.isEmpty(), "pre-rename document must be gone after the rename");
    }

    @Test
    void rangeQueryWithMorphiumIdEqualityPrefixFindsMatchingDocs() throws Exception {
        // Owner-ref + timestamp shape: compound index [ref, ts], equality on a MorphiumId
        // ref plus $gt range on ts. Stored keys normalize MorphiumId/ObjectId to String -
        // the synthetic range bounds must apply the SAME normalization or the subMap slice
        // misses every real key (silently dropping matching docs from the prefilter).
        InMemoryDriver drv = new InMemoryDriver();
        drv.connect();
        new CreateIndexesCommand(drv).setDb(db).setColl(coll)
                .addIndex(new IndexDescription().setKey(Doc.of("ref", 1, "ts", 1)))
                .execute();

        de.caluga.morphium.driver.MorphiumId owner = new de.caluga.morphium.driver.MorphiumId();
        de.caluga.morphium.driver.MorphiumId other = new de.caluga.morphium.driver.MorphiumId();
        List<Map<String, Object>> docs = new ArrayList<>();
        for (int i = 1; i <= 10; i++) {
            docs.add(Doc.of("ref", owner, "ts", i));
        }
        docs.add(Doc.of("ref", other, "ts", 8));
        new InsertMongoCommand(drv).setDb(db).setColl(coll).setDocuments(docs).execute();

        List<Map<String, Object>> result = drv.find(db, coll,
                Doc.of("ref", owner, "ts", Doc.of("$gt", 5)), null, null, 0, 0);

        assertEquals(5, result.size(), "must find exactly the owner's docs with ts 6..10");
        for (Map<String, Object> d : result) {
            assertEquals(owner.toString(), d.get("ref").toString());
            assertTrue(((Number) d.get("ts")).intValue() > 5);
        }
    }

    @Test
    void dropAndRecreateServesFreshIndexData() throws Exception {
        InMemoryDriver drv = freshDriverWithIndexedCollection(50);
        // populate the plan-index cache
        assertEquals(1, drv.find(db, coll, Doc.of("counter", 7), null, null, 0, 0).size());

        drv.drop(db, coll, null);

        new CreateIndexesCommand(drv).setDb(db).setColl(coll)
                .addIndex(new IndexDescription().setKey(Doc.of("counter", 1)))
                .execute();
        new InsertMongoCommand(drv).setDb(db).setColl(coll)
                .setDocuments(List.of(Doc.of("counter", 999))).execute();

        assertTrue(drv.find(db, coll, Doc.of("counter", 7), null, null, 0, 0).isEmpty(),
                "old doc must be gone after drop+recreate");
        assertEquals(1, drv.find(db, coll, Doc.of("counter", 999), null, null, 0, 0).size(),
                "new doc must be served from fresh index data");
    }

    @Test
    void emptyQueryCountIncrementsFullScans() throws Exception {
        InMemoryDriver drv = freshDriverWithIndexedCollection(10);

        long fullScansBefore = drv.fullScans;
        long count = drv.count(db, coll, Doc.of(), null, null);

        assertEquals(10, count);
        assertEquals(fullScansBefore + 1, drv.fullScans,
                "empty-query count must count as a full scan, same as empty-query find");
    }

    @Test
    void findAllSortedByIndexedFieldAscendingUsesIndexOrder() throws Exception {
        InMemoryDriver drv = new InMemoryDriver();
        drv.connect();
        new CreateIndexesCommand(drv).setDb(db).setColl(coll)
                .addIndex(new IndexDescription().setKey(Doc.of("counter", 1)))
                .execute();

        // Insert in reverse order so a correct result can only come from actually sorting
        // (by index order or otherwise) - insertion order alone would be wrong.
        List<Map<String, Object>> docs = new ArrayList<>();
        for (int i = 49_999; i >= 0; i--) {
            docs.add(Doc.of("counter", i));
        }
        new InsertMongoCommand(drv).setDb(db).setColl(coll).setDocuments(docs).execute();

        long indexSortsBefore = drv.indexSorts;

        List<Map<String, Object>> result = drv.find(db, coll, Doc.of(), Doc.of("counter", 1), null, 0, 10);

        assertEquals(10, result.size());
        for (int i = 0; i < 10; i++) {
            assertEquals(i, ((Number) result.get(i).get("counter")).intValue(), "result must be the first 10 counters in ascending order");
        }
        assertEquals(indexSortsBefore + 1, drv.indexSorts, "index-order ascending sort must increment indexSorts");
    }

    @Test
    void findAllSortedByIndexedFieldDescendingUsesReverseIndexScan() throws Exception {
        InMemoryDriver drv = new InMemoryDriver();
        drv.connect();
        new CreateIndexesCommand(drv).setDb(db).setColl(coll)
                .addIndex(new IndexDescription().setKey(Doc.of("counter", 1)))
                .execute();

        List<Map<String, Object>> docs = new ArrayList<>();
        for (int i = 0; i < 50_000; i++) {
            docs.add(Doc.of("counter", i));
        }
        new InsertMongoCommand(drv).setDb(db).setColl(coll).setDocuments(docs).execute();

        long indexSortsBefore = drv.indexSorts;

        List<Map<String, Object>> result = drv.find(db, coll, Doc.of(), Doc.of("counter", -1), null, 0, 10);

        assertEquals(10, result.size());
        for (int i = 0; i < 10; i++) {
            assertEquals(49_999 - i, ((Number) result.get(i).get("counter")).intValue(),
                    "descending sort on an ascending index must scan it in reverse");
        }
        assertEquals(indexSortsBefore + 1, drv.indexSorts, "index-order descending sort must increment indexSorts");
    }

    @Test
    void findSortedByUnindexedFieldFallsBackAndStaysCorrect() throws Exception {
        InMemoryDriver drv = freshDriverWithIndexedCollection(200);
        // "counter" is indexed; "other" (derived, non-monotonic w.r.t. counter) is not.
        for (int i = 0; i < 200; i++) {
            new de.caluga.morphium.driver.commands.UpdateMongoCommand(drv).setDb(db).setColl(coll)
                    .addUpdate(Doc.of("q", Doc.of("counter", i), "u", Doc.of("$set", Doc.of("other", (i * 37) % 200))))
                    .execute();
        }

        long indexSortsBefore = drv.indexSorts;

        List<Map<String, Object>> result = drv.find(db, coll, Doc.of(), Doc.of("other", 1), null, 0, 0);

        assertEquals(200, result.size());
        for (int i = 1; i < result.size(); i++) {
            int prev = ((Number) result.get(i - 1).get("other")).intValue();
            int cur = ((Number) result.get(i).get("other")).intValue();
            assertTrue(prev <= cur, "fallback sort on unindexed field must still be correctly ordered");
        }
        assertEquals(indexSortsBefore, drv.indexSorts, "sort on an unindexed field must NOT use the index-order fast path");
    }

    @Test
    void findSortedByCompoundIndexPrefixUsesIndexOrder() throws Exception {
        InMemoryDriver drv = new InMemoryDriver();
        drv.connect();
        new CreateIndexesCommand(drv).setDb(db).setColl(coll)
                .addIndex(new IndexDescription().setKey(Doc.of("group", 1, "counter", 1)))
                .execute();

        List<Map<String, Object>> docs = new ArrayList<>();
        for (int i = 0; i < 300; i++) {
            docs.add(Doc.of("group", (299 - i) % 5, "counter", i));
        }
        new InsertMongoCommand(drv).setDb(db).setColl(coll).setDocuments(docs).execute();

        long indexSortsBefore = drv.indexSorts;

        // Sort spec is just the compound index's leading field - a valid prefix.
        List<Map<String, Object>> result = drv.find(db, coll, Doc.of(), Doc.of("group", 1), null, 0, 0);

        assertEquals(300, result.size());
        for (int i = 1; i < result.size(); i++) {
            int prev = ((Number) result.get(i - 1).get("group")).intValue();
            int cur = ((Number) result.get(i).get("group")).intValue();
            assertTrue(prev <= cur, "result must be non-decreasing on the sorted prefix field");
        }
        assertEquals(indexSortsBefore + 1, drv.indexSorts, "a sort spec that is a prefix of a compound index must use index order");
    }

    @Test
    void sameIndexFilterAndSortWithSkipCountsOnlyMatchingDocs() throws Exception {
        InMemoryDriver drv = new InMemoryDriver();
        drv.connect();
        new CreateIndexesCommand(drv).setDb(db).setColl(coll)
                .addIndex(new IndexDescription().setKey(Doc.of("counter", 1)))
                .execute();

        List<Map<String, Object>> docs = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            // Every other document fails the (non-indexed) "flag" predicate - the index-order
            // scan must skip those without letting them count against `skip`.
            docs.add(Doc.of("counter", i, "flag", i % 2 == 0));
        }
        new InsertMongoCommand(drv).setDb(db).setColl(coll).setDocuments(docs).execute();

        long indexSortsBefore = drv.indexSorts;
        long indexHitsBefore = drv.indexHits;

        // Filter plans a RangeScan on the SAME "counter" index used for the sort; "flag" is a
        // pure post-filter predicate applied per document during the index-order scan.
        List<Map<String, Object>> result = drv.find(db, coll,
                Doc.of("counter", Doc.of("$gte", 0), "flag", true), Doc.of("counter", 1), null, 3, 4);

        // Matching docs (flag=true) have counters 0,2,4,6,...,48 - skip the first 3 (0,2,4),
        // then take 4: 6,8,10,12.
        assertEquals(4, result.size());
        assertEquals(List.of(6, 8, 10, 12), result.stream().map(d -> ((Number) d.get("counter")).intValue()).toList());
        assertEquals(indexSortsBefore + 1, drv.indexSorts, "same-index filter+sort combination must use index order");
        assertEquals(indexHitsBefore + 1, drv.indexHits, "the filter's own RangeScan plan must still count as an index hit");
    }

    @Test
    void indexSortWithLimitOnlyTouchesAboutLimitManyIndexEntries() throws Exception {
        InMemoryDriver drv = new InMemoryDriver();
        drv.connect();
        new CreateIndexesCommand(drv).setDb(db).setColl(coll)
                .addIndex(new IndexDescription().setKey(Doc.of("counter", 1)))
                .execute();

        List<Map<String, Object>> docs = new ArrayList<>();
        for (int i = 0; i < 50_000; i++) {
            docs.add(Doc.of("counter", i));
        }
        new InsertMongoCommand(drv).setDb(db).setColl(coll).setDocuments(docs).execute();

        CollectionIndexStore store = drv.getIndexStore(db, coll);
        long scannedBefore = store.scannedEntries;

        List<Map<String, Object>> result = drv.find(db, coll, Doc.of(), Doc.of("counter", 1), null, 2, 10);

        assertEquals(10, result.size());
        assertEquals(2, ((Number) result.get(0).get("counter")).intValue());
        long scanned = store.scannedEntries - scannedBefore;
        assertTrue(scanned <= 20,
                "an index-order sort with skip=2/limit=10 over 50k entries must be lazy and touch only ~12 entries, but scanned " + scanned);
    }

    @Test
    void mixedDirectionCompoundIndexServesExactReverseSort() throws Exception {
        InMemoryDriver drv = new InMemoryDriver();
        drv.connect();
        new CreateIndexesCommand(drv).setDb(db).setColl(coll)
                .addIndex(new IndexDescription().setKey(Doc.of("a", 1, "b", -1)))
                .execute();

        List<Map<String, Object>> docs = new ArrayList<>();
        for (int i = 0; i < 200; i++) {
            docs.add(Doc.of("a", i % 4, "b", i % 7));
        }
        new InsertMongoCommand(drv).setDb(db).setColl(coll).setDocuments(docs).execute();

        long indexSortsBefore = drv.indexSorts;

        // Exact reverse of (a:1, b:-1) is (a:-1, b:1) - a single reverse scan of the index.
        List<Map<String, Object>> result = drv.find(db, coll, Doc.of(), Doc.of("a", -1, "b", 1), null, 0, 0);

        assertEquals(200, result.size());
        for (int i = 1; i < result.size(); i++) {
            int prevA = ((Number) result.get(i - 1).get("a")).intValue();
            int curA = ((Number) result.get(i).get("a")).intValue();
            assertTrue(prevA >= curA, "a must be non-increasing");
            if (prevA == curA) {
                int prevB = ((Number) result.get(i - 1).get("b")).intValue();
                int curB = ((Number) result.get(i).get("b")).intValue();
                assertTrue(prevB <= curB, "b must be non-decreasing within equal a");
            }
        }
        assertEquals(indexSortsBefore + 1, drv.indexSorts,
                "the exact per-field reverse of a mixed-direction compound index must be reverse-scan eligible");
    }

    @Test
    void mixedDirectionCompoundIndexRejectsPartiallyReversedSort() throws Exception {
        InMemoryDriver drv = new InMemoryDriver();
        drv.connect();
        new CreateIndexesCommand(drv).setDb(db).setColl(coll)
                .addIndex(new IndexDescription().setKey(Doc.of("a", 1, "b", -1)))
                .execute();

        List<Map<String, Object>> docs = new ArrayList<>();
        for (int i = 0; i < 200; i++) {
            docs.add(Doc.of("a", i % 4, "b", i % 7));
        }
        new InsertMongoCommand(drv).setDb(db).setColl(coll).setDocuments(docs).execute();

        long indexSortsBefore = drv.indexSorts;

        // (a:-1, b:-1) reverses a but NOT b relative to (a:1, b:-1) - no single directional
        // scan of that index realizes it, so the fallback sort must be used (and stay correct).
        List<Map<String, Object>> result = drv.find(db, coll, Doc.of(), Doc.of("a", -1, "b", -1), null, 0, 0);

        assertEquals(200, result.size());
        for (int i = 1; i < result.size(); i++) {
            int prevA = ((Number) result.get(i - 1).get("a")).intValue();
            int curA = ((Number) result.get(i).get("a")).intValue();
            assertTrue(prevA >= curA, "a must be non-increasing");
            if (prevA == curA) {
                int prevB = ((Number) result.get(i - 1).get("b")).intValue();
                int curB = ((Number) result.get(i).get("b")).intValue();
                assertTrue(prevB >= curB, "b must be non-increasing within equal a");
            }
        }
        assertEquals(indexSortsBefore, drv.indexSorts,
                "a partially-reversed sort spec must NOT be index-order eligible");
    }

    @Test
    void findRemainsCorrectAfterUpdateChangesTheIndexedField() throws Exception {
        InMemoryDriver drv = freshDriverWithIndexedCollection(50);

        // Move one document's indexed field via the driver's normal in-place update path.
        de.caluga.morphium.driver.commands.UpdateMongoCommand upd =
                new de.caluga.morphium.driver.commands.UpdateMongoCommand(drv).setDb(db).setColl(coll)
                        .addUpdate(Doc.of("q", Doc.of("counter", 7), "u", Doc.of("$set", Doc.of("counter", 70_000))));
        upd.execute();

        List<Map<String, Object>> oldValue = drv.find(db, coll, Doc.of("counter", 7), null, null, 0, 0);
        assertTrue(oldValue.isEmpty(), "old key must no longer be found after the update");

        List<Map<String, Object>> newValue = drv.find(db, coll, Doc.of("counter", 70_000), null, null, 0, 0);
        assertEquals(1, newValue.size(), "updated document must be found under its new key");
    }
}
