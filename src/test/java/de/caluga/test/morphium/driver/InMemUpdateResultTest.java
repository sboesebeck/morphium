package de.caluga.test.morphium.driver;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.commands.ClearCollectionCommand;
import de.caluga.morphium.driver.commands.FindCommand;
import de.caluga.morphium.driver.commands.InsertMongoCommand;
import de.caluga.morphium.driver.commands.UpdateMongoCommand;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

@Tag("inmemory")
public class InMemUpdateResultTest {

    private InMemoryDriver newDriver() {
        InMemoryDriver drv = new InMemoryDriver();
        drv.connect();
        return drv;
    }

    @Test
    public void testNoModificationNModifiedZero() throws Exception {
        var drv = newDriver();
        String db = "upd_res_db";
        String coll = "items";
        new ClearCollectionCommand(drv).setDb(db).setColl(coll).doClear();

        ObjectId id = new ObjectId();
        new InsertMongoCommand(drv)
            .setDb(db)
            .setColl(coll)
            .setDocuments(List.of(Doc.of("_id", id, "counter", 1)))
            .execute();

        Map<String, Object> res = new UpdateMongoCommand(drv)
            .setDb(db)
            .setColl(coll)
            .addUpdate(Doc.of(
                "q", Doc.of("_id", id),
                "u", Doc.of("$set", Doc.of("counter", 1))
            ))
            .execute();

        assertEquals(1, ((Number) res.get("n")).intValue(), "n should equal matched docs");
        assertEquals(0, ((Number) res.get("nModified")).intValue(), "nModified should be 0 when no change applied");
        assertFalse(res.containsKey("upserted"));
    }

    @Test
    public void testModificationNModifiedOne() throws Exception {
        var drv = newDriver();
        String db = "upd_res_db";
        String coll = "items";
        new ClearCollectionCommand(drv).setDb(db).setColl(coll).doClear();

        ObjectId id = new ObjectId();
        new InsertMongoCommand(drv)
            .setDb(db)
            .setColl(coll)
            .setDocuments(List.of(Doc.of("_id", id, "counter", 1)))
            .execute();

        Map<String, Object> res = new UpdateMongoCommand(drv)
            .setDb(db)
            .setColl(coll)
            .addUpdate(Doc.of(
                "q", Doc.of("_id", id),
                "u", Doc.of("$set", Doc.of("counter", 2))
            ))
            .execute();

        assertEquals(1, ((Number) res.get("n")).intValue());
        assertEquals(1, ((Number) res.get("nModified")).intValue());
        assertFalse(res.containsKey("upserted"));
    }

    @Test
    public void testUpsertReturnsUpserted() throws Exception {
        var drv = newDriver();
        String db = "upd_res_db";
        String coll = "items";
        new ClearCollectionCommand(drv).setDb(db).setColl(coll).doClear();

        // No matching docs; request upsert
        Map<String, Object> res = new UpdateMongoCommand(drv)
            .setDb(db)
            .setColl(coll)
            .addUpdate(Doc.of(
                "q", Doc.of("name", "upsert-me"),
                "u", Doc.of("$set", Doc.of("value", 42)),
                "upsert", true
            ))
            .execute();

        assertEquals(0, ((Number) res.get("n")).intValue(), "no matches when upsert created new doc");
        assertEquals(0, ((Number) res.get("nModified")).intValue(), "newly inserted, not modified");
        assertTrue(res.containsKey("upserted"));
        @SuppressWarnings("unchecked")
        List<Map<String,Object>> up = (List<Map<String,Object>>) res.get("upserted");
        assertThat(up).isNotNull();
        assertThat(up.size()).isGreaterThanOrEqualTo(1);
        Object upId = up.get(0).get("_id");
        assertNotNull(upId);

        // Verify the upserted doc exists
        List<Map<String, Object>> found = new FindCommand(drv)
            .setDb(db)
            .setColl(coll)
            .setFilter(Doc.of("_id", upId))
            .execute();
        assertEquals(1, found.size());
        assertEquals(42, ((Number) found.get(0).get("value")).intValue());
    }

    @Test
    public void testMultiUpdateCountsOnlyChanged() throws Exception {
        var drv = newDriver();
        String db = "upd_res_db";
        String coll = "items";
        new ClearCollectionCommand(drv).setDb(db).setColl(coll).doClear();

        List<Map<String,Object>> docs = new ArrayList<>();
        docs.add(Doc.of("_id", new ObjectId(), "status", "A", "value", 0));
        docs.add(Doc.of("_id", new ObjectId(), "status", "A", "value", 1));
        docs.add(Doc.of("_id", new ObjectId(), "status", "B", "value", 0));
        new InsertMongoCommand(drv).setDb(db).setColl(coll).setDocuments(docs).execute();

        Map<String, Object> res = new UpdateMongoCommand(drv)
            .setDb(db)
            .setColl(coll)
            .addUpdate(Doc.of(
                "q", Doc.of("status", "A"),
                "u", Doc.of("$set", Doc.of("value", 0)),
                "multi", true
            ))
            .execute();

        assertEquals(2, ((Number) res.get("n")).intValue(), "two matched with status A");
        assertEquals(1, ((Number) res.get("nModified")).intValue(), "only one actually changed value");
    }
}

