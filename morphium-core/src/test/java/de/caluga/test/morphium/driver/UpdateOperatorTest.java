package de.caluga.test.morphium.driver;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.driver.commands.ClearCollectionCommand;
import de.caluga.morphium.driver.commands.FindCommand;
import de.caluga.morphium.driver.commands.InsertMongoCommand;
import de.caluga.morphium.driver.commands.UpdateMongoCommand;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Regression coverage for #249: the InMemoryDriver update-operator cluster. Each of these
 * operators either silently did nothing, crashed, or applied only part of the requested change
 * while still reporting success.
 *
 * <p>Also covers #256: the positional operator {@code $}, the all-positional operator
 * {@code $[]}, the filtered positional operator {@code $[<identifier>]} with
 * {@code arrayFilters}, and the bitwise operator {@code $bit}.
 */
@Tag("inmemory")
public class UpdateOperatorTest {

    private InMemoryDriver drv;
    private final String db = "update_op_test";
    private final String coll = "docs";

    // separate namespace for the #256 positional/$bit tests (helpers clear the collection per test)
    private static final String DB = "upd_op_db";
    private static final String COLL = "items";

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

    // ==========================================================================================
    // #256: positional $, $[], $[<identifier>] + arrayFilters, $bit
    // ==========================================================================================

    private ObjectId insert(InMemoryDriver drv, Map<String, Object> doc) throws Exception {
        new ClearCollectionCommand(drv).setDb(DB).setColl(COLL).doClear();
        ObjectId id = new ObjectId();
        doc.put("_id", id);
        new InsertMongoCommand(drv).setDb(DB).setColl(COLL).setDocuments(List.of(doc)).execute();
        return id;
    }

    private Map<String, Object> findById(InMemoryDriver drv, Object id) throws Exception {
        List<Map<String, Object>> found = new FindCommand(drv).setDb(DB).setColl(COLL)
        .setFilter(Doc.of("_id", id)).execute();
        assertEquals(1, found.size());
        return found.get(0);
    }

    private Map<String, Object> update(InMemoryDriver drv, Map<String, Object> q, Map<String, Object> u)
    throws Exception {
        return new UpdateMongoCommand(drv).setDb(DB).setColl(COLL)
               .addUpdate(Doc.of("q", q, "u", u)).execute();
    }

    private Map<String, Object> updateWithFilters(InMemoryDriver drv, Map<String, Object> q,
            Map<String, Object> u, List<Map<String, Object>> arrayFilters) throws Exception {
        return new UpdateMongoCommand(drv).setDb(DB).setColl(COLL)
               .addUpdate(Doc.of("q", q, "u", u, "arrayFilters", arrayFilters)).execute();
    }

    // ---------------------------------------------------------------- positional $

    @Test
    public void positionalSetScalarElement() throws Exception {
        ObjectId id = insert(drv, Doc.of("arr", new ArrayList<>(List.of(1, 5, 9))));

        var res = update(drv, Doc.of("arr", Doc.of("$gte", 5)), Doc.of("$set", Doc.of("arr.$", 100)));
        assertEquals(1, ((Number) res.get("nModified")).intValue());

        Map<String, Object> doc = findById(drv, id);
        // $ targets the FIRST element matched by the query predicate on 'arr' (5, index 1)
        assertEquals(List.of(1, 100, 9), doc.get("arr"));
    }

    @Test
    public void positionalSetNestedField() throws Exception {
        ObjectId id = insert(drv, Doc.of("items", new ArrayList<>(List.of(
                                             Doc.of("name", "a", "qty", 1),
                                             Doc.of("name", "b", "qty", 2),
                                             Doc.of("name", "b", "qty", 3)))));

        update(drv, Doc.of("items.name", "b"), Doc.of("$set", Doc.of("items.$.qty", 42)));

        Map<String, Object> doc = findById(drv, id);
        List<Map<String, Object>> items = (List<Map<String, Object>>) doc.get("items");
        assertEquals(1, items.get(0).get("qty"));
        // only the FIRST matching element is updated
        assertEquals(42, items.get(1).get("qty"));
        assertEquals(3, items.get(2).get("qty"));
    }

    @Test
    public void positionalIncNestedField() throws Exception {
        ObjectId id = insert(drv, Doc.of("items", new ArrayList<>(List.of(
                                             Doc.of("name", "a", "qty", 1),
                                             Doc.of("name", "b", "qty", 2)))));

        update(drv, Doc.of("items.name", "a"), Doc.of("$inc", Doc.of("items.$.qty", 5)));

        Map<String, Object> doc = findById(drv, id);
        List<Map<String, Object>> items = (List<Map<String, Object>>) doc.get("items");
        assertEquals(6, items.get(0).get("qty"));
        assertEquals(2, items.get(1).get("qty"));
    }

    @Test
    public void positionalMulNestedField() throws Exception {
        ObjectId id = insert(drv, Doc.of("items", new ArrayList<>(List.of(
                                             Doc.of("name", "a", "qty", 3),
                                             Doc.of("name", "b", "qty", 2)))));

        update(drv, Doc.of("items.name", "a"), Doc.of("$mul", Doc.of("items.$.qty", 4)));

        List<Map<String, Object>> items = (List<Map<String, Object>>) findById(drv, id).get("items");
        assertEquals(12, items.get(0).get("qty"));
        assertEquals(2, items.get(1).get("qty"));
    }

    @Test
    public void positionalWithElemMatchQuery() throws Exception {
        ObjectId id = insert(drv, Doc.of("items", new ArrayList<>(List.of(
                                             Doc.of("name", "a", "qty", 2),
                                             Doc.of("name", "b", "qty", 2)))));

        update(drv, Doc.of("items", Doc.of("$elemMatch", Doc.of("name", "b", "qty", 2))),
               Doc.of("$set", Doc.of("items.$.qty", 7)));

        List<Map<String, Object>> items = (List<Map<String, Object>>) findById(drv, id).get("items");
        assertEquals(2, items.get(0).get("qty"));
        assertEquals(7, items.get(1).get("qty"));
    }

    @Test
    public void positionalUnsetNullsElement() throws Exception {
        ObjectId id = insert(drv, Doc.of("arr", new ArrayList<>(List.of(1, 5, 9))));

        update(drv, Doc.of("arr", 5), Doc.of("$unset", Doc.of("arr.$", "")));

        // MongoDB does not shrink the array on $unset - the element becomes null
        assertEquals(Arrays.asList(1, null, 9), findById(drv, id).get("arr"));
    }

    @Test
    public void positionalPushIntoNestedArray() throws Exception {
        ObjectId id = insert(drv, Doc.of("items", new ArrayList<>(List.of(
                                             Doc.of("name", "a", "tags", new ArrayList<>(List.of("x"))),
                                             Doc.of("name", "b")))));

        update(drv, Doc.of("items.name", "a"), Doc.of("$push", Doc.of("items.$.tags", "y")));
        update(drv, Doc.of("items.name", "b"), Doc.of("$push", Doc.of("items.$.tags", "z")));

        List<Map<String, Object>> items = (List<Map<String, Object>>) findById(drv, id).get("items");
        assertEquals(List.of("x", "y"), items.get(0).get("tags"));
        // pushing onto a missing field of the matched element creates the array
        assertEquals(List.of("z"), items.get(1).get("tags"));
    }

    @Test
    public void positionalWithoutArrayPredicateThrows() throws Exception {
        insert(drv, Doc.of("other", 1, "arr", new ArrayList<>(List.of(1, 2))));

        var ex = assertThrows(MorphiumDriverException.class,
                              () -> update(drv, Doc.of("other", 1), Doc.of("$set", Doc.of("arr.$", 5))));
        assertThat(ex.getMessage()).contains("positional operator");
    }

    // ---------------------------------------------------------------- all positional $[]

    @Test
    public void allPositionalSetScalars() throws Exception {
        ObjectId id = insert(drv, Doc.of("arr", new ArrayList<>(List.of(1, 5, 9))));

        update(drv, Doc.of(), Doc.of("$set", Doc.of("arr.$[]", 0)));

        assertEquals(List.of(0, 0, 0), findById(drv, id).get("arr"));
    }

    @Test
    public void allPositionalIncNestedFields() throws Exception {
        ObjectId id = insert(drv, Doc.of("items", new ArrayList<>(List.of(
                                             Doc.of("name", "a", "qty", 1),
                                             Doc.of("name", "b", "qty", 2)))));

        update(drv, Doc.of(), Doc.of("$inc", Doc.of("items.$[].qty", 10)));

        List<Map<String, Object>> items = (List<Map<String, Object>>) findById(drv, id).get("items");
        assertEquals(11, items.get(0).get("qty"));
        assertEquals(12, items.get(1).get("qty"));
    }

    @Test
    public void allPositionalMissingArrayThrows() throws Exception {
        insert(drv, Doc.of("x", 1));

        var ex = assertThrows(MorphiumDriverException.class,
                              () -> update(drv, Doc.of(), Doc.of("$set", Doc.of("nope.$[]", 1))));
        assertThat(ex.getMessage()).contains("must exist");
    }

    // ---------------------------------------------------------------- $[<identifier>] + arrayFilters

    @Test
    public void arrayFiltersSetScalars() throws Exception {
        ObjectId id = insert(drv, Doc.of("grades", new ArrayList<>(List.of(95, 102, 90, 150))));

        updateWithFilters(drv, Doc.of(), Doc.of("$set", Doc.of("grades.$[e]", 100)),
                          List.of(Doc.of("e", Doc.of("$gte", 100))));

        assertEquals(List.of(95, 100, 90, 100), findById(drv, id).get("grades"));
    }

    @Test
    public void arrayFiltersIncNestedFields() throws Exception {
        ObjectId id = insert(drv, Doc.of("items", new ArrayList<>(List.of(
                                             Doc.of("name", "a", "qty", 1),
                                             Doc.of("name", "b", "qty", 7),
                                             Doc.of("name", "c", "qty", 3)))));

        updateWithFilters(drv, Doc.of(), Doc.of("$inc", Doc.of("items.$[it].qty", 2)),
                          List.of(Doc.of("it.qty", Doc.of("$lt", 5))));

        List<Map<String, Object>> items = (List<Map<String, Object>>) findById(drv, id).get("items");
        assertEquals(3, items.get(0).get("qty"));
        assertEquals(7, items.get(1).get("qty"));
        assertEquals(5, items.get(2).get("qty"));
    }

    @Test
    public void arrayFiltersNestedPositional() throws Exception {
        ObjectId id = insert(drv, Doc.of("rows", new ArrayList<>(List.of(
                                             Doc.of("vals", new ArrayList<>(List.of(1, 2, 3))),
                                             Doc.of("vals", new ArrayList<>(List.of(4, 5, 6)))))));

        updateWithFilters(drv, Doc.of(), Doc.of("$set", Doc.of("rows.$[].vals.$[v]", 0)),
                          List.of(Doc.of("v", Doc.of("$gt", 2))));

        List<Map<String, Object>> rows = (List<Map<String, Object>>) findById(drv, id).get("rows");
        assertEquals(List.of(1, 2, 0), rows.get(0).get("vals"));
        assertEquals(List.of(0, 0, 0), rows.get(1).get("vals"));
    }

    @Test
    public void arrayFiltersNoMatchIsNoop() throws Exception {
        ObjectId id = insert(drv, Doc.of("grades", new ArrayList<>(List.of(1, 2, 3))));

        var res = updateWithFilters(drv, Doc.of(), Doc.of("$set", Doc.of("grades.$[e]", 100)),
                                    List.of(Doc.of("e", Doc.of("$gte", 100))));

        assertEquals(1, ((Number) res.get("n")).intValue());
        assertEquals(0, ((Number) res.get("nModified")).intValue());
        assertEquals(List.of(1, 2, 3), findById(drv, id).get("grades"));
    }

    @Test
    public void arrayFiltersUnknownIdentifierThrows() throws Exception {
        insert(drv, Doc.of("grades", new ArrayList<>(List.of(1, 2))));

        var ex = assertThrows(MorphiumDriverException.class,
                              () -> updateWithFilters(drv, Doc.of(), Doc.of("$set", Doc.of("grades.$[x]", 1)),
                                      List.of(Doc.of("y", Doc.of("$gt", 0)))));
        assertThat(ex.getMessage()).contains("x");
        assertThat(ex.getMessage().toLowerCase()).contains("array filter");
    }

    @Test
    public void arrayFiltersUnusedFilterThrows() throws Exception {
        insert(drv, Doc.of("grades", new ArrayList<>(List.of(1, 2))));

        var ex = assertThrows(MorphiumDriverException.class,
                              () -> updateWithFilters(drv, Doc.of(), Doc.of("$set", Doc.of("grades.0", 1)),
                                      List.of(Doc.of("e", Doc.of("$gt", 0)))));
        assertThat(ex.getMessage()).contains("e");
        assertThat(ex.getMessage()).contains("not used");
    }

    @Test
    public void arrayFiltersMissingArrayThrows() throws Exception {
        insert(drv, Doc.of("x", 1));

        var ex = assertThrows(MorphiumDriverException.class,
                              () -> updateWithFilters(drv, Doc.of(), Doc.of("$set", Doc.of("nope.$[e]", 1)),
                                      List.of(Doc.of("e", Doc.of("$gt", 0)))));
        assertThat(ex.getMessage()).contains("must exist");
    }

    @Test
    public void arrayFiltersMultiDocUpdate() throws Exception {
        new ClearCollectionCommand(drv).setDb(DB).setColl(COLL).doClear();
        ObjectId id1 = new ObjectId();
        ObjectId id2 = new ObjectId();
        new InsertMongoCommand(drv).setDb(DB).setColl(COLL).setDocuments(List.of(
                    Doc.of("_id", id1, "vals", new ArrayList<>(List.of(1, 10))),
                    Doc.of("_id", id2, "vals", new ArrayList<>(List.of(20, 2))))).execute();

        new UpdateMongoCommand(drv).setDb(DB).setColl(COLL)
        .addUpdate(Doc.of("q", Doc.of(), "u", Doc.of("$set", Doc.of("vals.$[big]", -1)),
                          "multi", true,
                          "arrayFilters", List.of(Doc.of("big", Doc.of("$gte", 10)))))
        .execute();

        assertEquals(List.of(1, -1), findById(drv, id1).get("vals"));
        assertEquals(List.of(-1, 2), findById(drv, id2).get("vals"));
    }

    // ---------------------------------------------------------------- $bit

    @Test
    public void bitAndOrXorInt() throws Exception {
        ObjectId id = insert(drv, Doc.of("a", 13, "b", 13, "c", 13));

        update(drv, Doc.of("_id", id), Doc.of("$bit", Doc.of(
                    "a", Doc.of("and", 10),
                    "b", Doc.of("or", 10),
                    "c", Doc.of("xor", 10))));

        Map<String, Object> doc = findById(drv, id);
        assertEquals(13 & 10, doc.get("a"));
        assertEquals(13 | 10, doc.get("b"));
        assertEquals(13 ^ 10, doc.get("c"));
    }

    @Test
    public void bitLongPromotion() throws Exception {
        ObjectId id = insert(drv, Doc.of("a", 6L, "b", 6));

        update(drv, Doc.of("_id", id), Doc.of("$bit", Doc.of(
                    "a", Doc.of("or", 1),
                    "b", Doc.of("or", 1L))));

        Map<String, Object> doc = findById(drv, id);
        // any long involved (field or operand) promotes the result to long
        assertEquals(7L, doc.get("a"));
        assertEquals(7L, doc.get("b"));
    }

    @Test
    public void bitMissingFieldTreatedAsZero() throws Exception {
        ObjectId id = insert(drv, Doc.of("x", 1));

        update(drv, Doc.of("_id", id), Doc.of("$bit", Doc.of("flags", Doc.of("or", 5))));

        assertEquals(5, findById(drv, id).get("flags"));
    }

    @Test
    public void bitOnNonIntegerFieldThrows() throws Exception {
        ObjectId id = insert(drv, Doc.of("a", "notANumber"));

        var ex = assertThrows(MorphiumDriverException.class,
                              () -> update(drv, Doc.of("_id", id), Doc.of("$bit", Doc.of("a", Doc.of("or", 1)))));
        assertThat(ex.getMessage()).contains("$bit");
    }

    @Test
    public void bitUnknownOperationThrows() throws Exception {
        ObjectId id = insert(drv, Doc.of("a", 1));

        var ex = assertThrows(MorphiumDriverException.class,
                              () -> update(drv, Doc.of("_id", id), Doc.of("$bit", Doc.of("a", Doc.of("nand", 1)))));
        assertThat(ex.getMessage()).contains("and");
    }

    @Test
    public void bitNonIntegerOperandThrows() throws Exception {
        ObjectId id = insert(drv, Doc.of("a", 1));

        var ex = assertThrows(MorphiumDriverException.class,
                              () -> update(drv, Doc.of("_id", id), Doc.of("$bit", Doc.of("a", Doc.of("and", 2.5)))));
        assertThat(ex.getMessage()).contains("$bit");
    }

    @Test
    public void bitWithPositionalPath() throws Exception {
        ObjectId id = insert(drv, Doc.of("items", new ArrayList<>(List.of(
                                             Doc.of("name", "a", "flags", 12),
                                             Doc.of("name", "b", "flags", 12)))));

        update(drv, Doc.of("items.name", "b"), Doc.of("$bit", Doc.of("items.$.flags", Doc.of("and", 10))));

        List<Map<String, Object>> items = (List<Map<String, Object>>) findById(drv, id).get("items");
        assertEquals(12, items.get(0).get("flags"));
        assertEquals(12 & 10, items.get(1).get("flags"));
    }

    // ---------------------------------------------------------------- combinations

    @Test
    public void positionalCombinedWithPlainSet() throws Exception {
        ObjectId id = insert(drv, Doc.of("status", "old", "items", new ArrayList<>(List.of(
                                             Doc.of("name", "a", "qty", 1)))));

        update(drv, Doc.of("items.name", "a"),
               Doc.of("$set", Doc.of("items.$.qty", 9, "status", "new")));

        Map<String, Object> doc = findById(drv, id);
        assertEquals("new", doc.get("status"));
        List<Map<String, Object>> items = (List<Map<String, Object>>) doc.get("items");
        assertEquals(9, items.get(0).get("qty"));
    }

    @Test
    public void renameWithPositionalThrows() throws Exception {
        insert(drv, Doc.of("items", new ArrayList<>(List.of(Doc.of("name", "a")))));

        assertThrows(MorphiumDriverException.class,
                     () -> update(drv, Doc.of("items.name", "a"),
                                  Doc.of("$rename", Doc.of("items.$.name", "items.$.label"))));
    }
}
