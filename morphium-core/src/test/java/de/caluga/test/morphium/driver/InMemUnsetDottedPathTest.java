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

/**
 * Regression test for the InMemoryDriver $unset handling on dotted (nested) field
 * paths. Previously $unset only removed top-level keys, so an unset of e.g.
 * "es_upload.acceptance.idx" was a silent no-op and nModified stayed 0, whereas
 * real MongoDB removes the nested leaf and reports the modification.
 */
@Tag("inmemory")
public class InMemUnsetDottedPathTest {

    private InMemoryDriver newDriver() {
        InMemoryDriver drv = new InMemoryDriver();
        drv.connect();
        return drv;
    }

    private Map<String, Object> docWithEsUpload(String idx) {
        return Doc.of(
            "_id", new ObjectId(),
            "source", "AQGS",
            "es_upload", Doc.of(
                "acceptance", Doc.of(idx, 111L, "keep_idx", 222L),
                "local", Doc.of("some_index", 333L)));
    }

    @Test
    public void testUnsetDottedPathRemovesLeafAndCountsModified() throws Exception {
        var drv = newDriver();
        String db = "unset_dotted_db";
        String coll = "items";
        new ClearCollectionCommand(drv).setDb(db).setColl(coll).doClear();

        List<Map<String, Object>> docs = new ArrayList<>();
        for (int i = 0; i < 7; i++) {
            docs.add(docWithEsUpload("db_marktdaten_international"));
        }
        new InsertMongoCommand(drv).setDb(db).setColl(coll).setDocuments(docs).execute();

        Map<String, Object> res = new UpdateMongoCommand(drv)
            .setDb(db)
            .setColl(coll)
            .addUpdate(Doc.of(
                "q", Doc.of("es_upload", Doc.of("$ne", null)),
                "u", Doc.of("$unset", Doc.of("es_upload.acceptance.db_marktdaten_international", "")),
                "multi", true))
            .execute();

        assertEquals(7, ((Number) res.get("n")).intValue(), "all 7 docs match es_upload $ne null");
        assertEquals(7, ((Number) res.get("nModified")).intValue(), "all 7 docs must be reported modified");

        // Verify the nested leaf is gone but siblings/parents remain
        List<Map<String, Object>> found = new FindCommand(drv).setDb(db).setColl(coll)
            .setFilter(Doc.of()).execute();
        assertEquals(7, found.size());
        for (Map<String, Object> d : found) {
            @SuppressWarnings("unchecked")
            Map<String, Object> esUpload = (Map<String, Object>) d.get("es_upload");
            assertThat(esUpload).isNotNull();
            @SuppressWarnings("unchecked")
            Map<String, Object> acceptance = (Map<String, Object>) esUpload.get("acceptance");
            assertThat(acceptance).doesNotContainKey("db_marktdaten_international");
            assertThat(acceptance).containsKey("keep_idx");
            assertThat(esUpload).containsKey("local");
        }
    }

    @Test
    public void testUnsetDottedPathMissingIsNoop() throws Exception {
        var drv = newDriver();
        String db = "unset_dotted_db";
        String coll = "items2";
        new ClearCollectionCommand(drv).setDb(db).setColl(coll).doClear();

        ObjectId id = new ObjectId();
        new InsertMongoCommand(drv).setDb(db).setColl(coll)
            .setDocuments(List.of(Doc.of("_id", id, "es_upload", Doc.of("acceptance", Doc.of("keep_idx", 1L)))))
            .execute();

        Map<String, Object> res = new UpdateMongoCommand(drv).setDb(db).setColl(coll)
            .addUpdate(Doc.of(
                "q", Doc.of("_id", id),
                "u", Doc.of("$unset", Doc.of("es_upload.acceptance.not_there", "")),
                "multi", true))
            .execute();

        assertEquals(1, ((Number) res.get("n")).intValue());
        assertEquals(0, ((Number) res.get("nModified")).intValue(), "unset of a missing nested key is a no-op");
    }

    @Test
    public void testUnsetFlatFieldStillWorks() throws Exception {
        var drv = newDriver();
        String db = "unset_dotted_db";
        String coll = "items3";
        new ClearCollectionCommand(drv).setDb(db).setColl(coll).doClear();

        ObjectId id = new ObjectId();
        new InsertMongoCommand(drv).setDb(db).setColl(coll)
            .setDocuments(List.of(Doc.of("_id", id, "flag", true, "keep", 1)))
            .execute();

        Map<String, Object> res = new UpdateMongoCommand(drv).setDb(db).setColl(coll)
            .addUpdate(Doc.of(
                "q", Doc.of("_id", id),
                "u", Doc.of("$unset", Doc.of("flag", "")),
                "multi", true))
            .execute();

        assertEquals(1, ((Number) res.get("n")).intValue());
        assertEquals(1, ((Number) res.get("nModified")).intValue());

        List<Map<String, Object>> found = new FindCommand(drv).setDb(db).setColl(coll)
            .setFilter(Doc.of("_id", id)).execute();
        assertThat(found.get(0)).doesNotContainKey("flag");
        assertThat(found.get(0)).containsKey("keep");
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testUnsetDottedPathThroughArrayIndex() throws Exception {
        var drv = newDriver();
        String db = "unset_dotted_db";
        String coll = "array_items";
        new ClearCollectionCommand(drv).setDb(db).setColl(coll).doClear();

        ObjectId id = new ObjectId();
        new InsertMongoCommand(drv).setDb(db).setColl(coll).setDocuments(List.of(Doc.of(
            "_id", id,
            "ratings", List.of(Doc.of("rating", 5, "keep", true), Doc.of("rating", 3)),
            "tags", List.of("a", "b", "c")))).execute();

        Map<String, Object> result = new UpdateMongoCommand(drv).setDb(db).setColl(coll)
            .addUpdate(Doc.of(
                "q", Doc.of("_id", id),
                "u", Doc.of("$unset", Doc.of("ratings.0.rating", "", "tags.1", "")),
                "multi", false))
            .execute();

        assertEquals(1, ((Number) result.get("nModified")).intValue());
        Map<String, Object> stored = new FindCommand(drv).setDb(db).setColl(coll)
            .setFilter(Doc.of("_id", id)).execute().get(0);
        List<Map<String, Object>> ratings = (List<Map<String, Object>>) stored.get("ratings");
        List<Object> tags = (List<Object>) stored.get("tags");
        assertThat(ratings.get(0)).doesNotContainKey("rating").containsEntry("keep", true);
        assertThat(ratings.get(1)).containsEntry("rating", 3);
        assertThat(tags).containsExactly("a", null, "c");
    }
}
