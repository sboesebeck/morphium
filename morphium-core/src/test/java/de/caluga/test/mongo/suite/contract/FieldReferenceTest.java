package de.caluga.test.mongo.suite.contract;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.annotations.Property;
import de.caluga.morphium.annotations.caching.NoCache;
import de.caluga.test.mongo.suite.base.MultiDriverTestBase;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * MOR-012: the update builder takes a field as a String, as an enum constant and as the name of a generated reference (the constants of Lombok's {@code @FieldNameConstants}, in
 * its class form and in its enum form, and the {@code name()} of a static metamodel attribute), and resolves all of them to the same stored name: a camel case Java name, a
 * {@code @Property} name, a dotted path. Every operator has the three spellings; the raw documents are compared.
 */
@Tag("core")
public class FieldReferenceTest extends MultiDriverTestBase {

    @Entity
    @NoCache
    public static class Item {
        @Id
        public String id;
        @Property(fieldName = "lbl")
        public String label;
        public String createdBy;
        public int hitCount;
        public int bonusPoints;
        public String oldName;
        public String movedName;
        public List<String> tagList = new ArrayList<>();
        public List<String> labelSet = new ArrayList<>();
        public String cleanUp;
        public java.util.Date seenAt;

        /** what Lombok's {@code @FieldNameConstants} generates by default */
        public static final class Fields {
            public static final String label = "label";
            public static final String createdBy = "createdBy";
            public static final String hitCount = "hitCount";
            public static final String tagList = "tagList";
        }
    }

    /** what {@code @FieldNameConstants(asEnum = true)} generates */
    public enum F {
        label, createdBy, hitCount, bonusPoints, oldName, movedName, tagList, labelSet, cleanUp, seenAt
    }

    /** a static metamodel attribute: only {@code name()} matters, it is the Java name of the field */
    record Attribute(String name) {
    }

    private void seed(Morphium m) {
        m.dropCollection(Item.class);
        Item i = new Item();
        i.id = "i";
        i.label = "old";
        i.createdBy = "me";
        i.hitCount = 1;
        i.oldName = "was";
        i.cleanUp = "x";
        i.tagList.add("a");
        m.store(i);
    }

    private Map<String, Object> raw(Morphium m) throws Exception {
        return ModifyFixturesAccess.rawDocs(m.createQueryFor(Item.class).f("id").eq("i")).get(0);
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void theValueOperatorsResolveTheThreeSpellingsToTheSameStoredName(Morphium m) throws Exception {
        try (m) {
            seed(m);
            // String constants of Lombok's class form, enum constants, the name of a metamodel attribute
            m.createQueryFor(Item.class).f("id").eq("i").modify().set(Item.Fields.label, "s1").set(F.createdBy, "s2").inc(new Attribute("hitCount").name(), 1)
             .updateOne();
            Map<String, Object> r = raw(m);
            assertEquals("s1", r.get("lbl"), String.valueOf(r));
            assertEquals("s2", r.get("created_by"), String.valueOf(r));
            assertEquals(2, ((Number) r.get("hit_count")).intValue(), String.valueOf(r));
            assertFalse(r.containsKey("label") || r.containsKey("createdBy") || r.containsKey("hitCount"), "no stray field under the Java name: " + r);

            m.createQueryFor(Item.class).f("id").eq("i").modify().max(F.hitCount, 10).min(F.bonusPoints, -4).updateOne();
            assertEquals(10, ((Number) raw(m).get("hit_count")).intValue(), String.valueOf(raw(m)));
            assertEquals(-4, ((Number) raw(m).get("bonus_points")).intValue(), String.valueOf(raw(m)));
            m.createQueryFor(Item.class).f("id").eq("i").modify().mul(F.hitCount, 3).updateOne();
            assertEquals(30, ((Number) raw(m).get("hit_count")).intValue(), String.valueOf(raw(m)));
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void renameTakesEnumConstantsToo(Morphium m) throws Exception {
        try (m) {
            seed(m);
            m.createQueryFor(Item.class).f("id").eq("i").modify().rename(F.oldName, F.movedName).updateOne();
            Map<String, Object> r = raw(m);
            assertEquals("was", r.get("moved_name"), String.valueOf(r));
            assertFalse(r.containsKey("old_name"), String.valueOf(r));

            // and back with the String spelling, Java name or stored name
            m.createQueryFor(Item.class).f("id").eq("i").modify().rename("moved_name", "oldName").updateOne();
            assertEquals("was", raw(m).get("old_name"), String.valueOf(raw(m)));
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void theArrayOperatorsTakeEnumConstantsAlsoWithEach(Morphium m) throws Exception {
        try (m) {
            seed(m);
            m.createQueryFor(Item.class).f("id").eq("i").modify().push(F.tagList, "b").pushEach(F.labelSet, List.of("x", "y")).updateOne();
            m.createQueryFor(Item.class).f("id").eq("i").modify().addToSetEach(F.labelSet, List.of("y", "z")).addToSet(F.tagList, "a").updateOne();
            Map<String, Object> r = raw(m);
            assertEquals(List.of("a", "b"), r.get("tag_list"), String.valueOf(r));
            assertEquals(List.of("x", "y", "z"), r.get("label_set"), String.valueOf(r));
            assertFalse(r.containsKey("tagList") || r.containsKey("labelSet"), String.valueOf(r));
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void unsetAndCurrentDateTakeEnumConstants(Morphium m) throws Exception {
        try (m) {
            seed(m);
            m.createQueryFor(Item.class).f("id").eq("i").modify().unset(F.cleanUp).currentDate(F.seenAt).updateOne();
            Map<String, Object> r = raw(m);
            assertFalse(r.containsKey("clean_up"), String.valueOf(r));
            assertNull(r.get("cleanUp"));
            assertFalse(r.get("seen_at") == null, String.valueOf(r));
        }
    }
}
