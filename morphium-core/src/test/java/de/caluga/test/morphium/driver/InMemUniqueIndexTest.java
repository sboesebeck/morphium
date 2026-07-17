package de.caluga.test.morphium.driver;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.commands.CreateIndexesCommand;
import de.caluga.morphium.driver.commands.InsertMongoCommand;
import de.caluga.morphium.driver.commands.UpdateMongoCommand;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@Tag("inmemory")
public class InMemUniqueIndexTest {
    String db = "uniqdb";
    String coll = "uniqcoll";

    @Test
    public void storeRespectsUniqueIndex() throws Exception {
        var drv = new InMemoryDriver();
        drv.connect();
        new CreateIndexesCommand(drv).setDb(db).setColl(coll)
            .addIndex(new de.caluga.morphium.IndexDescription().setKey(Doc.of("u", 1)).setUnique(true))
            .execute();

        new InsertMongoCommand(drv).setDb(db).setColl(coll)
            .setDocuments(List.of(Doc.of("u", 1))).execute();

        boolean threw = false;
        try {
            drv.store(db, coll, List.of(Doc.of("u", 1)), null);
        } catch (MorphiumDriverException ex) {
            threw = true;
        }
        assertTrue(threw, "store should enforce unique index");
    }

    @Test
    public void updateRespectsUniqueIndex() throws Exception {
        var drv = new InMemoryDriver();
        drv.connect();
        new CreateIndexesCommand(drv).setDb(db).setColl(coll)
            .addIndex(new de.caluga.morphium.IndexDescription().setKey(Doc.of("u", 1)).setUnique(true))
            .execute();

        var docs = new InsertMongoCommand(drv).setDb(db).setColl(coll)
            .setDocuments(List.of(Doc.of("u", 1), Doc.of("u", 2)))
            .execute();

        UpdateMongoCommand upd = new UpdateMongoCommand(drv).setDb(db).setColl(coll)
            .addUpdate(Doc.of("q", Doc.of("u", 2), "u", Doc.of("$set", Doc.of("u", 1))));
        try {
            upd.execute();
            fail("Expected duplicate key enforcement on update");
        } catch (MorphiumDriverException ex) {
            // Phase B1, Task 5: the scan-based enforceUniqueOrThrow() pre-check (message
            // "Duplicate key for unique index: ...", capital D) that this assertion originally
            // pinned was removed as redundant - CollectionIndexStore.onUpdate is now the sole
            // uniqueness check for this path and throws its own MongoDB-shaped duplicate-key
            // exception ("E11000 duplicate key error index: ...", lowercase). The functional
            // behavior (update rejected, MorphiumDriverException thrown) is unchanged; only the
            // message wording differs, so assert on the MongoDB-conform signal (code 11000)
            // instead of message text.
            assertEquals(11000, ex.getMongoCode());
        }
    }
}
