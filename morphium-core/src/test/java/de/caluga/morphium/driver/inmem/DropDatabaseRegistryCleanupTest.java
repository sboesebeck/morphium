package de.caluga.morphium.driver.inmem;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.commands.InsertMongoCommand;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * Regression test for #369: dropping a whole database must clear the per-collection registries
 * that hang off it, not just the documents and the index definitions.
 *
 * <p>{@code drop(String db, String collection, WriteConcern)} and {@code setDatabase()} both purge
 * the TTL registration, the expiry queue and the capped bookkeeping for what they remove.
 * {@code drop(String db, WriteConcern)} - the whole-database drop behind {@code dropDatabase} -
 * did not, so a collection recreated under the same name inherited rules that
 * {@code getIndexes()} no longer reports: documents silently expired by a TTL index that is gone,
 * and inserts silently evicted by a cap that was never re-declared.
 *
 * <p>Lives in the driver's own package to reach the package-private {@code runTtlSweepPass()},
 * which makes the sweep deterministic instead of racing the background scheduler.
 */
@Tag("inmemory")
public class DropDatabaseRegistryCleanupTest {
    private final String db = "dropregistrydb";
    private final String coll = "dropregistrycoll";

    /**
     * A driver whose background sweep is effectively disabled: {@code expireCheck} is set before
     * {@code connect()} (the period is fixed when the task is scheduled, so setting it afterwards
     * has no effect), and the one unconditional tick 100ms after scheduling is waited out here.
     * All sweeping in this test is then driven explicitly via
     * {@link InMemoryDriver#runTtlSweepPass()}.
     */
    private InMemoryDriver quiescentDriver() throws Exception {
        InMemoryDriver drv = new InMemoryDriver();
        drv.setExpireCheck(3_600_000);
        drv.connect();
        Thread.sleep(400);
        return drv;
    }

    private List<Map<String, Object>> dueDocs(int count) {
        long past = System.currentTimeMillis() - 5_000L;
        List<Map<String, Object>> docs = new ArrayList<>();

        for (int i = 0; i < count; i++) {
            docs.add(Doc.of("counter", i, "expiresAt", new Date(past)));
        }

        return docs;
    }

    @Test
    void droppingTheDatabaseMustTakeItsTtlRegistrationWithIt() throws Exception {
        InMemoryDriver drv = quiescentDriver();
        drv.createIndex(db, coll, Doc.of("expiresAt", 1), Doc.of("name", "ttl_1", "expireAfterSeconds", 0));
        new InsertMongoCommand(drv).setDb(db).setColl(coll).setDocuments(dueDocs(5)).execute();
        drv.runTtlSweepPass();
        assertEquals(0, drv.find(db, coll, Doc.of(), null, null, 0, 0).size(),
                "sanity: with the TTL index in place the due documents must be swept");

        drv.drop(db, null);

        // Same database and collection name, but nothing declares a TTL index this time.
        new InsertMongoCommand(drv).setDb(db).setColl(coll).setDocuments(dueDocs(5)).execute();
        assertFalse(drv.getIndexes(db, coll).stream().anyMatch(i -> i.containsKey("expireAfterSeconds")),
                "sanity: the recreated collection must not report a TTL index");

        drv.runTtlSweepPass();
        assertEquals(5, drv.find(db, coll, Doc.of(), null, null, 0, 0).size(),
                "documents must not be expired by the TTL registration of a dropped database - "
                        + "getIndexes() reports no TTL index, so nothing may sweep them");
    }

    @Test
    void droppingTheDatabaseMustTakeItsCappedRegistrationWithIt() throws Exception {
        InMemoryDriver drv = quiescentDriver();
        drv.registerCappedCollection(db, coll, Integer.MAX_VALUE, 5);
        List<Map<String, Object>> docs = new ArrayList<>();

        for (int i = 0; i < 10; i++) {
            docs.add(Doc.of("counter", i));
        }

        new InsertMongoCommand(drv).setDb(db).setColl(coll).setDocuments(docs).execute();
        assertEquals(5, drv.find(db, coll, Doc.of(), null, null, 0, 0).size(),
                "sanity: while the cap is declared it must evict down to its max count");

        drv.drop(db, null);

        // Same names again, but this collection was never declared capped.
        new InsertMongoCommand(drv).setDb(db).setColl(coll).setDocuments(docs).execute();
        assertEquals(10, drv.find(db, coll, Doc.of(), null, null, 0, 0).size(),
                "inserts must not be evicted by the cap of a dropped database - the recreated "
                        + "collection is an ordinary one");
    }
}
