package de.caluga.test.morphium.driver;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.driver.commands.FindCommand;
import de.caluga.morphium.driver.commands.InsertMongoCommand;
import de.caluga.morphium.driver.commands.UpdateMongoCommand;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Regression coverage for #249: the InMemoryDriver update-operator cluster. Each of these
 * operators either silently did nothing, crashed, or applied only part of the requested change
 * while still reporting success.
 */
@Tag("inmemory")
public class UpdateOperatorTest {

    private InMemoryDriver drv;
    private final String db = "update_op_test";
    private final String coll = "docs";

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

    private MorphiumId seed(Map<String, Object> doc) throws Exception {
        MorphiumId id = new MorphiumId();
        doc.put("_id", id);
        List<Map<String, Object>> docs = new ArrayList<>();
        docs.add(doc);
        new InsertMongoCommand(drv).setDb(db).setColl(coll).setDocuments(docs).execute();
        return id;
    }

    private void update(MorphiumId id, Map<String, Object> updateDoc) throws Exception {
        UpdateMongoCommand upd = new UpdateMongoCommand(drv).setDb(db).setColl(coll);
        upd.addUpdate(Doc.of("_id", id), updateDoc, null, false, false, null, null, null);
        upd.execute();
    }

    private Map<String, Object> reload(MorphiumId id) throws Exception {
        FindCommand fnd = new FindCommand(drv).setDb(db).setColl(coll);
        fnd.setFilter(Doc.of("_id", id));
        List<Map<String, Object>> res = fnd.execute();
        fnd.releaseConnection();
        assertEquals(1, res.size(), "document must still exist");
        return res.get(0);
    }

    @Test
    public void pullWithElemMatch_removesMatchingElements() throws Exception {
        MorphiumId id = seed(Doc.of("results", new ArrayList<>(List.of(
            Doc.of("item", "A", "score", 5),
            Doc.of("item", "B", "score", 8),
            Doc.of("item", "C", "score", 8)
        ))));

        update(id, Doc.of("$pull", Doc.of("results", Doc.of("$elemMatch", Doc.of("score", 8, "item", "B")))));

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> results = (List<Map<String, Object>>) reload(id).get("results");
        assertEquals(2, results.size(), "$pull with $elemMatch must remove the matching element");
        assertTrue(results.stream().noneMatch(r -> "B".equals(r.get("item"))), "element B must be gone");
    }

    @Test
    public void renameWithDottedSource_movesFieldAndKeepsTargetIntact() throws Exception {
        MorphiumId id = seed(Doc.of("a", Doc.of("b", "value"), "target", "must-survive-if-source-missing"));

        update(id, Doc.of("$rename", Doc.of("a.b", "moved")));

        Map<String, Object> doc = reload(id);
        assertEquals("value", doc.get("moved"), "dotted source must be resolved and moved");
        @SuppressWarnings("unchecked")
        Map<String, Object> a = (Map<String, Object>) doc.get("a");
        assertTrue(a == null || !a.containsKey("b"), "dotted source field must be removed");
    }

    @Test
    public void renameWithMissingSource_doesNotDestroyTarget() throws Exception {
        MorphiumId id = seed(Doc.of("target", "keep-me"));

        update(id, Doc.of("$rename", Doc.of("doesNotExist", "target")));

        assertEquals("keep-me", reload(id).get("target"),
            "a $rename whose source does not exist must not remove the target field");
    }

    @Test
    public void minOnAbsentField_setsValueInsteadOfThrowing() throws Exception {
        MorphiumId id = seed(Doc.of("other", 1));

        update(id, Doc.of("$min", Doc.of("newField", 5)));

        assertEquals(5, ((Number) reload(id).get("newField")).intValue(),
            "$min on an absent field must set it, not throw");
    }

    @Test
    public void maxOnAbsentField_setsValueInsteadOfThrowing() throws Exception {
        MorphiumId id = seed(Doc.of("other", 1));

        update(id, Doc.of("$max", Doc.of("newField", 5)));

        assertEquals(5, ((Number) reload(id).get("newField")).intValue(),
            "$max on an absent field must set it, not throw");
    }

    @Test
    public void mulOnAbsentField_createsZero() throws Exception {
        MorphiumId id = seed(Doc.of("other", 1));

        update(id, Doc.of("$mul", Doc.of("counter", 5)));

        Map<String, Object> doc = reload(id);
        assertTrue(doc.containsKey("counter"), "$mul on an absent field must create it");
        assertEquals(0, ((Number) doc.get("counter")).intValue(),
            "$mul on an absent field yields 0 (MongoDB semantics)");
    }

    @Test
    public void currentDate_appliesToAllFields() throws Exception {
        MorphiumId id = seed(Doc.of("x", 1));

        update(id, Doc.of("$currentDate", Doc.of("updatedAt", true, "lastModified", true)));

        Map<String, Object> doc = reload(id);
        assertInstanceOf(Date.class, doc.get("updatedAt"), "first $currentDate field must be set");
        assertInstanceOf(Date.class, doc.get("lastModified"),
            "$currentDate must apply to every listed field, not only the first");
    }

    @Test
    public void pushWithSortModifier_sortsResultingArray() throws Exception {
        MorphiumId id = seed(Doc.of("scores", new ArrayList<>(List.of(
            Doc.of("n", 3), Doc.of("n", 1)
        ))));

        update(id, Doc.of("$push", Doc.of("scores", Doc.of(
            "$each", List.of(Doc.of("n", 2)),
            "$sort", Doc.of("n", 1)
        ))));

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> scores = (List<Map<String, Object>>) reload(id).get("scores");
        assertEquals(3, scores.size());
        assertEquals(List.of(1, 2, 3), scores.stream().map(s -> ((Number) s.get("n")).intValue()).toList(),
            "$push with a $sort modifier must sort the resulting array");
    }
}
