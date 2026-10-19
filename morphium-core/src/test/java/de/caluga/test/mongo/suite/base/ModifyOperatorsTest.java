package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.annotations.Embedded;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.annotations.LastChange;
import de.caluga.morphium.annotations.Version;
import de.caluga.morphium.driver.commands.FindCommand;
import de.caluga.morphium.query.PushOptions;
import de.caluga.morphium.query.Query;
import de.caluga.morphium.query.ReturnDocument;
import de.caluga.morphium.query.UpdateBuilder;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * MOR-007: the update builder knows {@code min}, {@code max}, {@code mul}, {@code rename}, {@code currentDate}, {@code push} and {@code addToSet} with {@code $each} and the push
 * modifiers ({@code PushOptions}), next to {@code set}, {@code setOnInsert}, {@code inc} and {@code unset}. Every test reads the raw document, because the defects that this guards
 * against (a stray field under the Java name, an unmarshalled element, an element that is added twice) are silent.
 */
@Tag("core")
public class ModifyOperatorsTest extends MultiDriverTestBase {

    @Embedded
    public static class Line {
        public String lineName;
        public int qty;

        public Line() {
        }

        public Line(String lineName, int qty) {
            this.lineName = lineName;
            this.qty = qty;
        }
    }

    public enum Stamp { lastSeen }

    @Entity
    public static class Ops {
        @Id
        public String id;
        public int minValue;
        public int maxValue;
        public int factor;
        public Integer optionalCount;
        public String legacyName;
        public String fullName;
        public Date lastSeen;
        public Date firstSeen;
        public List<Line> lineList = new ArrayList<>();
        public List<String> tagList = new ArrayList<>();
        public List<Integer> scoreList = new ArrayList<>();
    }

    @Entity
    public static class Tracked {
        @Id
        public String id;
        public int hits;
        @Version
        public Long version;
        @LastChange
        public long changedAt;
    }

    private UpdateBuilder<Ops> on(Morphium m, String id) {
        return m.createQueryFor(Ops.class).f("id").eq(id).modify();
    }

    private Ops fresh(Morphium m, String id) {
        m.dropCollection(Ops.class);
        return add(m, id);
    }

    private Ops add(Morphium m, String id) {
        Ops o = new Ops();
        o.id = id;
        o.minValue = 5;
        o.maxValue = 5;
        o.factor = 3;
        o.legacyName = "legacy";
        o.scoreList = new ArrayList<>(List.of(1, 2, 3));
        m.store(o);
        return o;
    }

    private Map<String, Object> raw(Morphium m, Class<?> type, String id) throws Exception {
        FindCommand find = new FindCommand(m.getDriver().getPrimaryConnection(null)).setDb(m.getDatabase()).setColl(m.getMapper().getCollectionName(type)).setFilter(Map.of("_id", id));

        try {
            List<Map<String, Object>> res = find.execute();
            return res.isEmpty() ? null : res.get(0);
        } finally {
            find.releaseConnection();
        }
    }

    // ------------------------------------------------------------------ min, max, mul

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void minMaxAndMulWriteTheStoredNames(Morphium m) throws Exception {
        try (m) {
            fresh(m, "o1");
            Ops r = on(m, "o1").min("minValue", 3).max("maxValue", 9).mul("factor", 4).findOneAndUpdate(ReturnDocument.AFTER);
            assertEquals(3, r.minValue);
            assertEquals(9, r.maxValue);
            assertEquals(12, r.factor);
            Map<String, Object> raw = raw(m, Ops.class, "o1");
            assertEquals(3, ((Number) raw.get("min_value")).intValue());
            assertFalse(raw.containsKey("minValue"), "no stray field under the Java name: " + raw);
            assertFalse(raw.containsKey("maxValue"), String.valueOf(raw));
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void minAndMaxKeepAValueThatIsNotBeyond(Morphium m) throws Exception {
        try (m) {
            fresh(m, "o1");
            Ops r = on(m, "o1").min("minValue", 7).max("maxValue", 2).findOneAndUpdate(ReturnDocument.AFTER);
            assertEquals(5, r.minValue);
            assertEquals(5, r.maxValue);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void aMissingFieldIsSeededByMinAndMaxAndMultipliedToZero(Morphium m) throws Exception {
        try (m) {
            fresh(m, "o1");
            add(m, "o2");
            // store() writes the null of an Integer field, so the field is removed first: a field that is null is not a missing field ($mul refuses it, code 14)
            on(m, "o1").unset("optionalCount").findOneAndUpdate(ReturnDocument.AFTER);
            on(m, "o2").unset("optionalCount").findOneAndUpdate(ReturnDocument.AFTER);
            Ops viaMul = on(m, "o1").mul("optionalCount", 5).findOneAndUpdate(ReturnDocument.AFTER);
            assertEquals(0, viaMul.optionalCount, "$mul creates a missing field as zero");
            Ops viaMax = on(m, "o2").max("optionalCount", 8).findOneAndUpdate(ReturnDocument.AFTER);
            assertEquals(8, viaMax.optionalCount, "$max seeds a missing field");
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void mulRejectsWhatItCannotSend(Morphium m) throws Exception {
        try (m) {
            assertThrows(IllegalArgumentException.class, () -> on(m, "o1").mul("factor", null));
            assertThrows(IllegalArgumentException.class, () -> on(m, "o1").mul("factor", new BigDecimal("1.5")));
        }
    }

    // ------------------------------------------------------------------ rename

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void renameMovesTheStoredFieldAndTranslatesBothNames(Morphium m) throws Exception {
        try (m) {
            fresh(m, "o1");
            Ops r = on(m, "o1").rename("legacyName", "fullName").findOneAndUpdate(ReturnDocument.AFTER);
            assertEquals("legacy", r.fullName);
            Map<String, Object> raw = raw(m, Ops.class, "o1");
            assertEquals("legacy", raw.get("full_name"));
            assertFalse(raw.containsKey("legacy_name"), String.valueOf(raw));
            assertFalse(raw.containsKey("fullName"), "the target is translated, too: " + raw);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void renameConflictsAreRejectedAtTheCall(Morphium m) throws Exception {
        try (m) {
            assertThrows(IllegalArgumentException.class, () -> on(m, "o1").rename("legacyName", "fullName").set("fullName", "x"), "the target is also set");
            assertThrows(IllegalArgumentException.class, () -> on(m, "o1").set("fullName", "x").rename("legacyName", "fullName"), "the target was set before");
            assertThrows(IllegalArgumentException.class, () -> on(m, "o1").rename("legacyName", "legacyName"), "a field onto itself");
            assertThrows(IllegalArgumentException.class, () -> on(m, "o1").rename("legacyName", null));
        }
    }

    // ------------------------------------------------------------------ currentDate

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void currentDateSetsTheDateOfTheServerFields(Morphium m) throws Exception {
        try (m) {
            fresh(m, "o1");
            long before = System.currentTimeMillis();
            Ops r = on(m, "o1").currentDate("lastSeen", "firstSeen").findOneAndUpdate(ReturnDocument.AFTER);
            long after = System.currentTimeMillis();
            assertNotNull(r.lastSeen);
            assertNotNull(r.firstSeen);
            assertTrue(r.lastSeen.getTime() >= before - 10_000 && r.lastSeen.getTime() <= after + 10_000, "the date is now: " + r.lastSeen);
            assertTrue(raw(m, Ops.class, "o1").get("last_seen") instanceof Date, "stored as a BSON date: " + raw(m, Ops.class, "o1").get("last_seen"));
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void currentDateWorksWithTheEnumOverloadAndInAnUpsert(Morphium m) throws Exception {
        try (m) {
            fresh(m, "o1");
            Ops inserted = on(m, "brand-new").currentDate(Stamp.lastSeen).upsert().findOneAndUpdate(ReturnDocument.AFTER);
            assertNotNull(inserted.lastSeen, "an upsert that inserts sets the date");
        }
    }

    // ------------------------------------------------------------------ push and addToSet

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void pushAppendsTheElementInTheShapeOfQueryPush(Morphium m) throws Exception {
        try (m) {
            fresh(m, "o1");
            add(m, "o2");
            on(m, "o1").push("lineList", new Line("a", 1)).findOneAndUpdate(ReturnDocument.AFTER);
            m.createQueryFor(Ops.class).f("id").eq("o2").push("lineList", new Line("a", 1));
            assertEquals("a", m.createQueryFor(Ops.class).f("id").eq("o1").get().lineList.get(0).lineName);
            assertEquals(raw(m, Ops.class, "o2").get("line_list"), raw(m, Ops.class, "o1").get("line_list"), "the same raw element as Query.push writes");
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void pushEachAppendsEveryElementMarshalled(Morphium m) throws Exception {
        try (m) {
            fresh(m, "o1");
            Ops r = on(m, "o1").pushEach("lineList", List.of(new Line("a", 1), new Line("b", 2))).findOneAndUpdate(ReturnDocument.AFTER);
            assertEquals(List.of("a", "b"), r.lineList.stream().map(l -> l.lineName).toList());
            List<?> stored = (List<?>) raw(m, Ops.class, "o1").get("line_list");
            assertTrue(stored.get(0) instanceof Map && ((Map<?, ?>) stored.get(0)).containsKey("line_name"), "a document with the stored names: " + stored);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void pushEachWithSliceKeepsTheLastElements(Morphium m) throws Exception {
        try (m) {
            fresh(m, "o1");
            Ops r = on(m, "o1").pushEach("scoreList", List.of(4, 5, 6), PushOptions.options().slice(-4)).findOneAndUpdate(ReturnDocument.AFTER);
            assertEquals(List.of(3, 4, 5, 6), r.scoreList);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void pushEachWithPositionInsertsInFront(Morphium m) throws Exception {
        try (m) {
            fresh(m, "o1");
            Ops r = on(m, "o1").pushEach("scoreList", List.of(8, 9), PushOptions.options().position(0)).findOneAndUpdate(ReturnDocument.AFTER);
            assertEquals(List.of(8, 9, 1, 2, 3), r.scoreList);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void pushEachWithSortOrdersTheWholeArray(Morphium m) throws Exception {
        try (m) {
            fresh(m, "o1");
            add(m, "o2");
            Ops numbers = on(m, "o1").pushEach("scoreList", List.of(0, 7), PushOptions.options().sortDescending()).findOneAndUpdate(ReturnDocument.AFTER);
            assertEquals(List.of(7, 3, 2, 1, 0), numbers.scoreList);
            Ops lines = on(m, "o2").pushEach("lineList", List.of(new Line("c", 3), new Line("a", 1), new Line("b", 2)), PushOptions.options().sortBy("qty", true))
                         .findOneAndUpdate(ReturnDocument.AFTER);
            assertEquals(List.of("a", "b", "c"), lines.lineList.stream().map(l -> l.lineName).toList());
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void addToSetStoresEachElementOnceAndMarshalled(Morphium m) throws Exception {
        try (m) {
            Ops o = fresh(m, "o1");
            o.lineList.add(new Line("a", 1));
            m.store(o);
            Ops r = on(m, "o1").addToSetEach("lineList", List.of(new Line("a", 1), new Line("b", 2), new Line("b", 2))).findOneAndUpdate(ReturnDocument.AFTER);
            assertEquals(List.of("a", "b"), r.lineList.stream().map(l -> l.lineName).toList(), "the stored a is recognised, the two b are one element");
            assertEquals(2, ((List<?>) raw(m, Ops.class, "o1").get("line_list")).size());
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void addToSetOfSingleValuesIgnoresDuplicates(Morphium m) throws Exception {
        try (m) {
            fresh(m, "o1");
            on(m, "o1").addToSet("tagList", "x").findOneAndUpdate(ReturnDocument.AFTER);
            Ops r = on(m, "o1").addToSet("tagList", "x").findOneAndUpdate(ReturnDocument.AFTER);
            assertEquals(List.of("x"), r.tagList);
        }
    }

    // ------------------------------------------------------------------ rules of the builder

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void theSameFieldInTwoOperatorsIsRejectedAtTheCall(Morphium m) throws Exception {
        try (m) {
            assertThrows(IllegalArgumentException.class, () -> on(m, "o1").push("tagList", "x").addToSet("tagList", "y"));
            assertThrows(IllegalArgumentException.class, () -> on(m, "o1").min("minValue", 1).set("minValue", 2));
            assertThrows(IllegalArgumentException.class, () -> on(m, "o1").max("maxValue", 1).mul("maxValue", 2));
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void theNewOperatorsAreCheckedLikeTheOldOnes(Morphium m) throws Exception {
        try (m) {
            assertThrows(IllegalArgumentException.class, () -> on(m, "o1").pushEach("tagList", null));
            assertThrows(IllegalArgumentException.class, () -> on(m, "o1").addToSetEach("tagList", null));
            assertThrows(IllegalArgumentException.class, () -> on(m, "o1").pushEach("scoreList", List.of(1), null));
            assertThrows(IllegalArgumentException.class, () -> on(m, "o1").currentDate(new String[0]));
            assertThrows(IllegalArgumentException.class, () -> PushOptions.options().sortBy("", true));
            assertThrows(IllegalArgumentException.class, () -> PushOptions.options().sortAscending().sortBy("qty", true), "a sort of the whole elements and of a field exclude each other");
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void theUpdateDocumentShowsTheOperators(Morphium m) throws Exception {
        try (m) {
            Map<String, Object> doc = on(m, "o1").min("minValue", 1).max("maxValue", 2).mul("factor", 3).rename("legacyName", "fullName").currentDate("lastSeen")
                                      .pushEach("scoreList", List.of(4), PushOptions.options().slice(-3).position(0)).addToSet("tagList", "t").toUpdateDocument();
            assertEquals(Map.of("min_value", 1), doc.get("$min"));
            assertEquals(Map.of("max_value", 2), doc.get("$max"));
            assertEquals(Map.of("factor", 3), doc.get("$mul"));
            assertEquals(Map.of("legacy_name", "full_name"), doc.get("$rename"));
            assertEquals(Map.of("last_seen", true), doc.get("$currentDate"));
            @SuppressWarnings("unchecked") Map<String, Object> each = (Map<String, Object>) ((Map<String, Object>) doc.get("$push")).get("score_list");
            assertEquals(List.of(4), each.get("$each"));
            assertEquals(-3, ((Number) each.get("$slice")).intValue());
            assertEquals(0, ((Number) each.get("$position")).intValue());
            assertEquals(Map.of("tag_list", "t"), doc.get("$addToSet"));
        }
    }

    // ------------------------------------------------------------------ version and last change

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void everyModifyingOperatorBumpsTheVersionAndTheLastChange(Morphium m) throws Exception {
        try (m) {
            m.dropCollection(Tracked.class);
            Tracked t = new Tracked();
            t.id = "t1";
            t.hits = 1;
            m.store(t);
            Tracked first = m.createQueryFor(Tracked.class).f("id").eq("t1").get();
            long v0 = first.version;
            long c0 = first.changedAt;
            Thread.sleep(5);
            Query<Tracked> q = m.createQueryFor(Tracked.class).f("id").eq("t1");
            Tracked after = q.modify().max("hits", 5).findOneAndUpdate(ReturnDocument.AFTER);
            assertEquals(5, after.hits);
            assertEquals(v0 + 1, after.version, "a max-only update is a modification: the version grows");
            assertTrue(after.changedAt > c0, "and the last change is set");
        }
    }
}
