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
