package de.caluga.test.morphium.driver;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.driver.commands.CreateCommand;
import de.caluga.morphium.driver.commands.DbStatsCommand;
import de.caluga.morphium.driver.commands.DropIndexesCommand;
import de.caluga.morphium.driver.commands.FindCommand;
import de.caluga.morphium.driver.commands.InsertMongoCommand;
import de.caluga.morphium.driver.commands.ListIndexesCommand;
import de.caluga.morphium.driver.commands.RenameCollectionCommand;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Regression coverage for #239: renameCollection must carry a collection's capped and TTL
 * bookkeeping over to the renamed name, and DropIndexes on a TTL index must stop the driver-level
 * TTL sweep from running on a now-dropped index.
 *
 * The capped case is exercised behaviourally (eviction is synchronous on insert). The TTL sweep is
 * driven by a background scheduler with a coarse interval, so its registration state
 * (collectionsWithTtlIndex) is asserted directly via reflection rather than by waiting on the
 * timer - deterministic and fast, and it targets exactly the bookkeeping the bug leaves stale.
 */
@Tag("inmemory")
public class RenameDropBookkeepingTest {

    private InMemoryDriver drv;
    private final String db = "rename_bk_test";

    @BeforeEach
    public void setup() throws Exception {
        drv = new InMemoryDriver();
        drv.connect();
    }

    @AfterEach
    public void tearDown() {
        if (drv != null) {
            drv.close();
        }
    }

    private int count(String coll) throws Exception {
        FindCommand fnd = new FindCommand(drv).setDb(db).setColl(coll);
        fnd.setFilter(Doc.of());
        List<Map<String, Object>> res = fnd.execute();
        fnd.releaseConnection();
        return res.size();
    }

    private void insert(String coll, int n) throws Exception {
        List<Map<String, Object>> docs = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            docs.add(Doc.of("_id", new MorphiumId(), "value", i));
        }
        new InsertMongoCommand(drv).setDb(db).setColl(coll).setDocuments(docs).execute();
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> ttlRegistry() throws Exception {
        Field f = InMemoryDriver.class.getDeclaredField("collectionsWithTtlIndex");
        f.setAccessible(true);
        return (Map<String, Object>) f.get(drv);
    }

    @Test
    public void cappedCollection_stillEnforcedAfterRename() throws Exception {
        new CreateCommand(drv).setDb(db).setColl("capped_src")
            .setCapped(true).setSize(100000).setMax(3).execute();
        insert("capped_src", 3);
        assertEquals(3, count("capped_src"));

        new RenameCollectionCommand(drv).setDb(db).setColl("capped_src").setTo("capped_dst").execute();
        assertEquals(3, count("capped_dst"), "documents move with the rename");

        // Without the capped config migrating, the target is no longer capped and these grow it to 6.
        insert("capped_dst", 3);
        assertEquals(3, count("capped_dst"), "capped max=3 must still be enforced after the rename");
    }

    @Test
    public void ttlRegistration_migratedOnRename() throws Exception {
        drv.createIndex(db, "ttl_src", Doc.of("expireAt", 1), Doc.of("expireAfterSeconds", 3600));
        assertTrue(ttlRegistry().containsKey(db + ".ttl_src"), "precondition: source TTL index registered");

        new RenameCollectionCommand(drv).setDb(db).setColl("ttl_src").setTo("ttl_dst").execute();

        assertTrue(ttlRegistry().containsKey(db + ".ttl_dst"),
            "TTL sweep registration must follow the collection to its new name");
        assertFalse(ttlRegistry().containsKey(db + ".ttl_src"),
            "stale origin TTL registration must be removed on rename");
    }

    @Test
    public void dbStats_returnsPerDbStatsNotGlobalDatabaseCount() throws Exception {
        // #247: dbStats must report stats scoped to the requested db, not a global database count.
        insert("coll_a", 3);
        insert("coll_b", 2);

        Map<String, Object> stats = new DbStatsCommand(drv).setDb(db).execute();

        assertEquals(db, stats.get("db"), "dbStats must name the requested database");
        assertEquals(2, ((Number) stats.get("collections")).intValue(), "two collections were created in this db");
        assertEquals(5, ((Number) stats.get("objects")).intValue(), "total document count across the db's collections");
        assertFalse(stats.containsKey("databases"),
            "dbStats must not return a global count of all databases");
    }

    @Test
    public void indexDefinitions_migratedOnRename() throws Exception {
        // #248: a rename must carry the collection's index definitions to the new name.
        drv.createIndex(db, "idx_src", Doc.of("value", 1), Doc.of("name", "value_1", "unique", true));

        new RenameCollectionCommand(drv).setDb(db).setColl("idx_src").setTo("idx_dst").execute();

        ListIndexesCommand li = new ListIndexesCommand(drv).setDb(db).setColl("idx_dst");
        List<de.caluga.morphium.IndexDescription> idx = li.execute();
        boolean hasValueIdx = idx.stream().anyMatch(i -> "value_1".equals(i.getName()));
        assertTrue(hasValueIdx, "renamed collection must retain its index definitions, got: " + idx);
    }

    @Test
    public void ttlRegistration_clearedWhenTtlIndexDropped() throws Exception {
        drv.createIndex(db, "ttl_drop", Doc.of("expireAt", 1), Doc.of("expireAfterSeconds", 3600));
        assertTrue(ttlRegistry().containsKey(db + ".ttl_drop"), "precondition: TTL index registered");

        new DropIndexesCommand(drv).setDb(db).setColl("ttl_drop").setIndex(Doc.of("expireAt", 1)).execute();

        assertFalse(ttlRegistry().containsKey(db + ".ttl_drop"),
            "dropping the TTL index must clear its sweep registration so it stops deleting documents");
    }
}
