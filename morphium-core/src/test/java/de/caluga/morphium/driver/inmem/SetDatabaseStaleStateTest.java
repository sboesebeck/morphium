package de.caluga.morphium.driver.inmem;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.commands.InsertMongoCommand;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileOutputStream;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPOutputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Regression tests for #341: {@code setDatabase()} replaces a database's collection map
 * wholesale but used to leave every derived per-namespace structure describing the OLD
 * contents - index definitions, built index stores, TTL registration and expiry queue, capped
 * config, the identity-keyed capped size cache and the capped byte counter. A restore into a
 * driver that already holds data (the PoppyDB in-process restore case, or any direct
 * setDatabase over existing state) then served stale index results, listed indexes that no
 * longer enforce anything, and never expired restored documents.
 *
 * <p>This went unnoticed exactly because a restore into a FRESH driver finds all of those
 * structures empty - so every test here starts from a driver that already holds data.
 *
 * <p>Lives in the driver's own package for the package-private {@code runTtlSweepPass()} /
 * {@code cappedCurrentBytes()} and (via reflection, see
 * {@link #ttlQueueAndRegistrationAreRemovedNotCleared}) the private maps whose
 * removed-vs-emptied distinction is deliberately unobservable through the public API today
 * but becomes a silent never-expires bug the moment any path re-registers TTL without
 * bootstrapping (#269's exact shape).
 */
@Tag("inmemory")
public class SetDatabaseStaleStateTest {
    private static final String DB = "setdb_stale_db";
    private static final String COLL = "stale_coll";

    private InMemoryDriver quiescentDriver() throws Exception {
        InMemoryDriver drv = new InMemoryDriver();
        drv.setExpireCheck(3_600_000);
        drv.connect();
        Thread.sleep(400);
        return drv;
    }

    /** Driver with three indexed, index-store-warmed documents tagged "old". */
    private InMemoryDriver driverWithWarmIndexedData() throws Exception {
        InMemoryDriver drv = quiescentDriver();
        List<Map<String, Object>> docs = new ArrayList<>();
        for (int i = 1; i <= 3; i++) {
            docs.add(Doc.of("counter", i, "tag", "old"));
        }
        new InsertMongoCommand(drv).setDb(DB).setColl(COLL).setDocuments(docs).execute();
        drv.createIndex(DB, COLL, Doc.of("counter", 1), Doc.of("name", "counter_idx"));
        // Warm the index store: this read builds and caches it over the OLD documents.
        List<Map<String, Object>> warm = drv.find(DB, COLL, Doc.of("counter", 2), null, null, 0, 0);
        assertEquals(1, warm.size(), "sanity: the warmed index must serve the old data");
        assertEquals("old", warm.get(0).get("tag"));
        return drv;
    }

    private static Map<String, List<Map<String, Object>>> newContents() {
        List<Map<String, Object>> docs = new ArrayList<>();
        for (int i = 1; i <= 3; i++) {
            docs.add(Doc.of("_id", "new" + i, "counter", i, "tag", "new"));
        }
        Map<String, List<Map<String, Object>>> db = new HashMap<>();
        db.put(COLL, docs);
        return db;
    }

    private static boolean hasIndexNamed(List<Map<String, Object>> indexes, String name) {
        for (Map<String, Object> idx : indexes) {
            Map<?, ?> opts = (Map<?, ?>) idx.get("$options");
            if (opts != null && name.equals(opts.get("name"))) {
                return true;
            }
        }
        return false;
    }

    private static boolean hasTtlIndex(List<Map<String, Object>> indexes) {
        for (Map<String, Object> idx : indexes) {
            Map<?, ?> opts = (Map<?, ?>) idx.get("$options");
            if (opts != null && opts.get("expireAfterSeconds") != null) {
                return true;
            }
        }
        return false;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, ?> privateMap(InMemoryDriver drv, String fieldName) throws Exception {
        Field f = InMemoryDriver.class.getDeclaredField(fieldName);
        f.setAccessible(true);
        return (Map<String, ?>) f.get(drv);
    }

    /**
     * The core #341 symptom: after a wholesale replace, an indexed read must see the NEW
     * contents. Before the fix the cached index store (built over the old documents) kept
     * serving them, and {@code getIndexes()} kept listing definitions of contents that no
     * longer exist.
     */
    @Test
    public void indexedReadsServeTheNewContentsAfterSetDatabase() throws Exception {
        InMemoryDriver drv = driverWithWarmIndexedData();
        try {
            drv.setDatabase(DB, newContents());

            List<Map<String, Object>> found = drv.find(DB, COLL, Doc.of("counter", 2), null, null, 0, 0);
            assertEquals(1, found.size(), "#341: the read after setDatabase must see the new contents, got: " + found);
            assertEquals("new", found.get(0).get("tag"),
                    "#341: a stale index store must not serve the replaced documents");

            assertFalse(hasIndexNamed(drv.getIndexes(DB, COLL), "counter_idx"),
                    "#341: index definitions of the replaced contents must be discarded - "
                    + "setDatabase cannot know they hold for the new data");
        } finally {
            drv.close();
        }
    }

    /**
     * Same symptom through the real-world door: restoring a legacy (pre-#340, no index
     * section) dump into a driver that already holds indexed data. The dump is the source of
     * truth - nothing may survive that describes the replaced contents.
     */
    @Test
    public void legacyDumpRestoreOverExistingDataDiscardsStaleIndexState(@TempDir Path tmp) throws Exception {
        InMemoryDriver drv = driverWithWarmIndexedData();
        try {
            String json = "{ \"_id\" : 1723891234567, \"db\" : \"" + DB + "\", \"data\" : { \"" + COLL + "\" : [ "
                    + "{ \"_id\" : \"new2\", \"counter\" : 2, \"tag\" : \"new\" } ] } }";
            File f = new File(tmp.toFile(), DB + ".morphium.gz");
            try (GZIPOutputStream gz = new GZIPOutputStream(new FileOutputStream(f))) {
                gz.write(json.getBytes(StandardCharsets.UTF_8));
            }

            drv.restoreFromFile(f);

            // Query with a Long on purpose: JSON restores integral numbers as Long, and the
            // driver's Integer-vs-Long equality gap is a separate, pre-existing matter - this
            // test is about the stale index store, which (before #341) served the pre-restore
            // documents / missed the restored ones for this query either way.
            List<Map<String, Object>> found = drv.find(DB, COLL, Doc.of("counter", 2L), null, null, 0, 0);
            assertEquals(1, found.size(), "#341: reads after the restore must see the restored contents, got: " + found);
            assertEquals("new", found.get(0).get("tag"),
                    "#341: a stale index store must not serve pre-restore documents");

            assertFalse(hasIndexNamed(drv.getIndexes(DB, COLL), "counter_idx"),
                    "#341: a legacy dump carries no index definitions - the pre-restore ones must not "
                    + "survive as if they described the restored data");
        } finally {
            drv.close();
        }
    }

    /**
     * The TTL half: a legacy dump cannot carry the TTL index, so after restoring it over a
     * driver that had one, the driver must not KEEP CLAIMING a TTL index it will never
     * enforce - the restored (already due) documents stay, silently, while listIndexes
     * promises expiry. Consistency means: no TTL index listed, documents stay.
     */
    @Test
    public void ttlIndexIsNotClaimedAfterLegacyDumpRestoreOverExistingTtlData(@TempDir Path tmp) throws Exception {
        InMemoryDriver drv = quiescentDriver();
        try {
            new InsertMongoCommand(drv).setDb(DB).setColl(COLL)
                    .setDocuments(List.of(Doc.of("counter", 1, "expiresAt",
                            new Date(System.currentTimeMillis() + 3_600_000L))))
                    .execute();
            drv.createIndex(DB, COLL, Doc.of("expiresAt", 1),
                    Doc.of("name", "ttl_expires", "expireAfterSeconds", 0));

            // legacy dump: one ALREADY DUE document, no index section
            String json = "{ \"_id\" : 1723891234567, \"db\" : \"" + DB + "\", \"data\" : { \"" + COLL + "\" : [ "
                    + "{ \"_id\" : \"due\", \"counter\" : 9, \"expiresAt\" : "
                    + "{ \"class_name\" : \"java.util.Date\", \"value\" : 1723891234567 } } ] } }";
            File f = new File(tmp.toFile(), DB + ".morphium.gz");
            try (GZIPOutputStream gz = new GZIPOutputStream(new FileOutputStream(f))) {
                gz.write(json.getBytes(StandardCharsets.UTF_8));
            }

            drv.restoreFromFile(f);
            drv.runTtlSweepPass();

            assertEquals(1, drv.getDatabase(DB).get(COLL).size(),
                    "sanity: without index information the restored document must simply stay");
            assertFalse(hasTtlIndex(drv.getIndexes(DB, COLL)),
                    "#341: the driver must not keep listing a TTL index it will never enforce on the "
                    + "restored data - a TTL that looks configured but does nothing is the silent "
                    + "failure mode #340 was about, one level down");
        } finally {
            drv.close();
        }
    }

    /**
     * Capped bookkeeping: the byte counter counts documents that no longer exist, the capped
     * config claims a cap the new contents were never checked against - and the identity-keyed
     * size cache holds nothing but dead references to the replaced document instances (a
     * retention leak in the same shape as the poppydb commandResultsById one).
     */
    @Test
    public void cappedBookkeepingIsDiscardedOnSetDatabase() throws Exception {
        InMemoryDriver drv = quiescentDriver();
        try {
            drv.registerCappedCollection(DB, COLL, Integer.MAX_VALUE, 100);
            List<Map<String, Object>> docs = new ArrayList<>();
            for (int i = 0; i < 10; i++) {
                docs.add(Doc.of("counter", i, "payload", "x".repeat(500)));
            }
            new InsertMongoCommand(drv).setDb(DB).setColl(COLL).setDocuments(docs).execute();
            assertTrue(drv.cappedCurrentBytes(DB, COLL) > 0,
                    "sanity: the capped byte counter must track the inserted documents");

            drv.setDatabase(DB, newContents());

            assertEquals(0L, drv.cappedCurrentBytes(DB, COLL),
                    "#341: the capped byte counter must not keep counting replaced documents");
            assertFalse(drv.isCapped(DB, COLL),
                    "#341: the capped config describes the replaced contents and must be discarded");
            assertFalse(privateMap(drv, "cappedDocSizesByCollection").containsKey(DB + "." + COLL),
                    "#341: the identity-keyed size cache holds only dead references after the replace - "
                    + "keeping it is a retention leak");
        } finally {
            drv.close();
        }
    }

    /**
     * Guard for the one distinction the public API cannot show today: the TTL queue and the
     * TTL registration must be REMOVED for the replaced namespaces, never emptied in place.
     * The sweep and the insert path only re-bootstrap a queue that is {@code null} (#269) - an
     * empty-but-present queue, or a registration surviving without its data, silently pins the
     * collection in a never-expires state as soon as anything relies on the lazy rebuild.
     * Reflection instead of behavior because today every re-registration path happens to
     * re-bootstrap eagerly - this test is what fails when someone turns the removal into a
     * {@code clear()} while that invariant erodes.
     */
    @Test
    public void ttlQueueAndRegistrationAreRemovedNotCleared() throws Exception {
        InMemoryDriver drv = quiescentDriver();
        try {
            new InsertMongoCommand(drv).setDb(DB).setColl(COLL)
                    .setDocuments(List.of(Doc.of("counter", 1, "expiresAt",
                            new Date(System.currentTimeMillis() + 3_600_000L))))
                    .execute();
            drv.createIndex(DB, COLL, Doc.of("expiresAt", 1),
                    Doc.of("name", "ttl_expires", "expireAfterSeconds", 0));
            assertTrue(privateMap(drv, "ttlQueueByCollection").containsKey(DB + "." + COLL),
                    "sanity: creating the TTL index must have seeded an expiry queue");

            drv.setDatabase(DB, newContents());

            assertFalse(privateMap(drv, "ttlQueueByCollection").containsKey(DB + "." + COLL),
                    "#341: the TTL queue must be REMOVED (not emptied) so the next consumer "
                    + "re-bootstraps from the new contents");
            assertFalse(privateMap(drv, "collectionsWithTtlIndex").containsKey(DB + "." + COLL),
                    "#341: the TTL registration described the replaced contents' index and must go with it");
        } finally {
            drv.close();
        }
    }
}
