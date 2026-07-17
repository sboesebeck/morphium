package de.caluga.morphium.driver.inmem;

import de.caluga.morphium.IndexDescription;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.commands.CreateIndexesCommand;
import de.caluga.morphium.driver.commands.InsertMongoCommand;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Driver-level tests for incremental (write-path) index maintenance (Phase B1, Task 4):
 * {@link CollectionIndexStore} instances are now persistent per collection and kept in sync by
 * every mutation path directly ({@code onInsert}/{@code onUpdate}/{@code onRemove}), instead of
 * being rebuilt from scratch on the next read (Task 3's interim strategy). Lives in the driver's
 * own package to reach the package-private {@code indexStoreRebuilds} counter and
 * {@code getIndexStore}.
 */
@Tag("inmemory")
public class IndexMaintenanceTest {
    private final String db = "idxmaintdb";
    private final String coll = "idxmaintcoll";

    private InMemoryDriver freshDriverWithIndexedCollection() throws Exception {
        InMemoryDriver drv = new InMemoryDriver();
        drv.connect();
        new CreateIndexesCommand(drv).setDb(db).setColl(coll)
                .addIndex(new IndexDescription().setKey(Doc.of("counter", 1)))
                .execute();
        return drv;
    }

    @Test
    void insertIsFoundViaIndexLookupWithoutFullScan() throws Exception {
        InMemoryDriver drv = freshDriverWithIndexedCollection();
        new InsertMongoCommand(drv).setDb(db).setColl(coll)
                .setDocuments(List.of(Doc.of("counter", 42))).execute();

        long fullScansBefore = drv.fullScans;
        long indexHitsBefore = drv.indexHits;

        List<Map<String, Object>> result = drv.find(db, coll, Doc.of("counter", 42), null, null, 0, 0);

        assertEquals(1, result.size(), "insert must be visible via an index-backed lookup");
        assertEquals(indexHitsBefore + 1, drv.indexHits);
        assertEquals(fullScansBefore, drv.fullScans, "an index-backed find must not fall back to a full scan");
    }

    @Test
    void updateChangingIndexedFieldMovesOldKeyGoneNewKeyFoundWithoutRebuilding() throws Exception {
        InMemoryDriver drv = freshDriverWithIndexedCollection();
        new InsertMongoCommand(drv).setDb(db).setColl(coll)
                .setDocuments(List.of(Doc.of("counter", 7))).execute();
        // Touch the store once so it exists before we start counting rebuilds.
        drv.find(db, coll, Doc.of("counter", 7), null, null, 0, 0);
        long rebuildsBefore = drv.indexStoreRebuilds;

        drv.update(db, coll, Doc.of("counter", 7), null, Doc.of("$set", Doc.of("counter", 70_000)),
                false, false, null, null);

        assertTrue(drv.find(db, coll, Doc.of("counter", 7), null, null, 0, 0).isEmpty(),
                "old key must be gone after the update");
        List<Map<String, Object>> updated = drv.find(db, coll, Doc.of("counter", 70_000), null, null, 0, 0);
        assertEquals(1, updated.size(), "new key must be found after the update");
        assertEquals(rebuildsBefore, drv.indexStoreRebuilds,
                "an update must be applied incrementally (onUpdate), never via a full store rebuild");
    }

    @Test
    void updateNotTouchingIndexedFieldLeavesStoreUntouched() throws Exception {
        InMemoryDriver drv = freshDriverWithIndexedCollection();
        new InsertMongoCommand(drv).setDb(db).setColl(coll)
                .setDocuments(List.of(Doc.of("counter", 5, "other", "a"))).execute();

        CollectionIndexStore storeBefore = drv.getIndexStore(db, coll);
        long rebuildsBefore = drv.indexStoreRebuilds;

        drv.update(db, coll, Doc.of("counter", 5), null, Doc.of("$set", Doc.of("other", "b")),
                false, false, null, null);

        CollectionIndexStore storeAfter = drv.getIndexStore(db, coll);
        assertSame(storeBefore, storeAfter,
                "the persistent store instance itself must not be replaced/rebuilt by an update");
        assertEquals(rebuildsBefore, drv.indexStoreRebuilds, "no rebuild for an update at all");

        // The "counter" index entry for this doc must be exactly the same as before: still
        // findable under the SAME (untouched) key.
        List<Map<String, Object>> found = drv.find(db, coll, Doc.of("counter", 5), null, null, 0, 0);
        assertEquals(1, found.size());
        assertEquals("b", found.get(0).get("other"), "the non-indexed field must still have been updated");
    }

    @Test
    void deleteRemovesTheKeyFromTheIndex() throws Exception {
        InMemoryDriver drv = freshDriverWithIndexedCollection();
        new InsertMongoCommand(drv).setDb(db).setColl(coll)
                .setDocuments(List.of(Doc.of("counter", 99))).execute();
        assertEquals(1, drv.find(db, coll, Doc.of("counter", 99), null, null, 0, 0).size());

        drv.delete(db, coll, Doc.of("counter", 99), null, false, null, null);

        long fullScansBefore = drv.fullScans;
        long indexHitsBefore = drv.indexHits;
        List<Map<String, Object>> result = drv.find(db, coll, Doc.of("counter", 99), null, null, 0, 0);
        assertTrue(result.isEmpty(), "deleted document must no longer be found");
        assertEquals(indexHitsBefore + 1, drv.indexHits, "the negative lookup is still index-backed");
        assertEquals(fullScansBefore, drv.fullScans);
    }

    @Test
    void ttlExpiryRemovesTheKeyFromTheIndex() throws Exception {
        InMemoryDriver drv = new InMemoryDriver();
        drv.setExpireCheck(100);
        drv.connect();
        new CreateIndexesCommand(drv).setDb(db).setColl(coll)
                .addIndex(new IndexDescription().setKey(Doc.of("counter", 1)))
                .execute();
        drv.createIndex(db, coll, Doc.of("expiresAt", 1), Doc.of("name", "ttl_1", "expireAfterSeconds", 0));

        // Already-expired document (5s in the past, TTL is 0s).
        new InsertMongoCommand(drv).setDb(db).setColl(coll)
                .setDocuments(List.of(Doc.of("counter", 13, "expiresAt", new Date(System.currentTimeMillis() - 5000))))
                .execute();
        assertEquals(1, drv.find(db, coll, Doc.of("counter", 13), null, null, 0, 0).size(),
                "sanity: document exists before the TTL sweep runs");

        long deadline = System.currentTimeMillis() + 10_000;
        List<Map<String, Object>> result;
        do {
            result = drv.find(db, coll, Doc.of("counter", 13), null, null, 0, 0);
            if (result.isEmpty()) {
                break;
            }
            Thread.sleep(50);
        } while (System.currentTimeMillis() < deadline);

        assertTrue(result.isEmpty(), "TTL-expired document must be gone from the index-backed lookup");
    }

    @Test
    void tenThousandDocUpdatesMaintainTheIndexIncrementallyWithoutRebuilding() throws Exception {
        InMemoryDriver drv = freshDriverWithIndexedCollection();
        int n = 10_000;
        List<Map<String, Object>> docs = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            docs.add(Doc.of("counter", i));
        }
        new InsertMongoCommand(drv).setDb(db).setColl(coll).setDocuments(docs).execute();
        // Touch the store once so it exists before we start counting rebuilds/timing updates.
        drv.find(db, coll, Doc.of("counter", 0), null, null, 0, 0);
        long rebuildsBefore = drv.indexStoreRebuilds;

        long start = System.currentTimeMillis();
        for (int i = 0; i < n; i++) {
            drv.update(db, coll, Doc.of("counter", i), null, Doc.of("$set", Doc.of("counter", i + n)),
                    false, false, null, null);
        }
        long dur = System.currentTimeMillis() - start;

        assertEquals(rebuildsBefore, drv.indexStoreRebuilds,
                "10k updates must never trigger a full index-store rebuild (O(n x indexes) per update)");
        // Loose sanity bound: an O(n) full collection rebuild per update (the pre-Task-4 shape
        // this replaces) would take on the order of tens of seconds for 10k updates over a
        // 10k-document collection; incremental maintenance should comfortably finish in a few
        // seconds even on a slow/shared CI machine.
        assertTrue(dur < 20_000, "10k incremental updates took " + dur + "ms - looks like a per-update rebuild");

        assertEquals(1, drv.find(db, coll, Doc.of("counter", n), null, null, 0, 0).size());
        assertTrue(drv.find(db, coll, Doc.of("counter", 0), null, null, 0, 0).isEmpty());
    }
}
