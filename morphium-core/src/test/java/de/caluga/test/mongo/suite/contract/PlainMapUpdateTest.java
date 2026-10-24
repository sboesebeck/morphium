package de.caluga.test.mongo.suite.contract;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.annotations.caching.NoCache;
import de.caluga.morphium.query.FindAndModifyOptions;
import de.caluga.morphium.query.ReturnDocument;
import de.caluga.test.mongo.suite.base.MultiDriverTestBase;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * MOR-013: a map without operators is an update of the fields it names ({@code $set}), as in Spring Data and Panache. It was a replacement document: every field that the
 * map did not name was gone. A map with operators is unchanged.
 */
@Tag("core")
public class PlainMapUpdateTest extends MultiDriverTestBase {

    @Entity
    @NoCache
    public static class Doc {
        @Id
        public String id;
        public String label;
        public String note;
        public int counterValue;
    }

    private void seed(Morphium m) {
        m.dropCollection(Doc.class);
        Doc d = new Doc();
        d.id = "a";
        d.label = "old";
        d.note = "keep me";
        d.counterValue = 1;
        m.store(d);
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void aMapWithoutOperatorsSetsTheNamedFieldsAndKeepsTheOthers(Morphium m) throws Exception {
        try (m) {
            seed(m);
            Map<String, Object> update = new HashMap<>();
            update.put("label", "new");
            update.put("counterValue", 5);

            Doc r = m.createQueryFor(Doc.class).f("id").eq("a").findOneAndUpdate(update, false, true);

            assertNotNull(r);
            assertEquals("new", r.label);
            assertEquals(5, r.counterValue, "a Java name is translated like in the operator maps");
            assertEquals("keep me", r.note, "a field that the map does not name is kept");
            Map<String, Object> raw = ModifyFixturesAccess.rawDocs(m.createQueryFor(Doc.class).f("id").eq("a")).get(0);
            assertEquals("keep me", raw.get("note"), String.valueOf(raw));
            assertEquals(5, ((Number) raw.get("counter_value")).intValue());
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void aMapWithoutOperatorsUpsertsLikeSet(Morphium m) throws Exception {
        try (m) {
            seed(m);
            Doc r = m.createQueryFor(Doc.class).f("id").eq("b")
                    .findOneAndUpdate(Map.of("label", "created"), FindAndModifyOptions.options().upsert(true).returnDocument(ReturnDocument.AFTER));
            assertNotNull(r);
            assertEquals("created", r.label);
            assertEquals("b", r.id, "the equality of the filter is the id of the new document");
            assertNull(r.note);
            assertEquals(2, m.createQueryFor(Doc.class).countAll());
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void aMapWithOperatorsIsNotChanged(Morphium m) throws Exception {
        try (m) {
            seed(m);
            Doc r = m.createQueryFor(Doc.class).f("id").eq("a")
                    .findOneAndUpdate(Map.of("$inc", Map.of("counterValue", 2), "$set", Map.of("label", "via set")), false, true);
            assertEquals(3, r.counterValue);
            assertEquals("via set", r.label);
            assertEquals("keep me", r.note);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void findOneAndUpdateEnumsSetsTheNamedFields(Morphium m) throws Exception {
        try (m) {
            seed(m);
            m.createQueryFor(Doc.class).f("id").eq("a").findOneAndUpdateEnums(Map.of(F.label, "enum"));
            Doc read = m.createQueryFor(Doc.class).f("id").eq("a").get();
            assertEquals("enum", read.label);
            assertEquals("keep me", read.note, "the enum variant does not replace the document either");
        }
    }

    public enum F { label }
}
