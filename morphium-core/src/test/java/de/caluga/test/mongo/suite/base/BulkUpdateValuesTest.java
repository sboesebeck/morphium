package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.annotations.Embedded;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.bulk.MorphiumBulkContext;
import de.caluga.morphium.driver.commands.FindCommand;
import de.caluga.morphium.query.Query;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * MOR-047: the update requests of the bulk context gave their values to the driver as they were, so an embedded object, a Set or an enum in a bulk update failed on a real
 * MongoDB ({@code Unhandled Data type}) or was stored as the live Java object by the InMemoryDriver. They are marshalled with the update-value walk of the other update
 * paths now, and a bulk update stores what {@code Query.set} / {@code Query.push} store.
 */
@Tag("core")
public class BulkUpdateValuesTest extends MultiDriverTestBase {

    public enum Mode { A, B }

    @Embedded
    public static class Item {
        public String name;
        public int qty;

        public Item() {
        }

        public Item(String name, int qty) {
            this.name = name;
            this.qty = qty;
        }
    }

    @Entity
    public static class Bulky {
        @Id
        public String id;
        public Item single;
        public Mode mode;
        public Set<String> flags;
        public List<Item> items = new ArrayList<>();
        public LocalDate day;
    }

    private Query<Bulky> byId(Morphium m, String id) {
        return m.createQueryFor(Bulky.class).f("id").eq(id);
    }

    private void fresh(Morphium m, String... ids) {
        m.dropCollection(Bulky.class);

        for (String id : ids) {
            Bulky b = new Bulky();
            b.id = id;
            m.store(b);
        }
    }

    private Map<String, Object> raw(Morphium m, String id) throws Exception {
        FindCommand find = new FindCommand(m.getDriver().getPrimaryConnection(null)).setDb(m.getDatabase()).setColl(m.getMapper().getCollectionName(Bulky.class)).setFilter(Map.of("_id", id));

        try {
            List<Map<String, Object>> res = find.execute();
            return res.get(0);
        } finally {
            find.releaseConnection();
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void setRequestStoresEmbeddedValuesEnumsAndSets(Morphium m) throws Exception {
        try (m) {
            fresh(m, "a");
            MorphiumBulkContext<Bulky> bulk = m.createBulkRequestContext(Bulky.class, true);
            bulk.addSetRequest(byId(m, "a"), "single", new Item("one", 1), false, false);
            bulk.addSetRequest(byId(m, "a"), "mode", Mode.B, false, false);
            bulk.addSetRequest(byId(m, "a"), "flags", Set.of("x"), false, false);
            bulk.runBulk();
            Bulky b = byId(m, "a").get();
            assertEquals("one", b.single.name);
            assertEquals(Mode.B, b.mode);
            assertEquals(Set.of("x"), b.flags);
            assertTrue(raw(m, "a").get("flags") instanceof List, "a Set is stored as a list: " + raw(m, "a").get("flags"));
            assertTrue(raw(m, "a").get("single") instanceof Map, "an embedded object is stored as a document: " + raw(m, "a").get("single"));
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void theMapVariantOfSetMarshalsToo(Morphium m) throws Exception {
        try (m) {
            fresh(m, "a");
            MorphiumBulkContext<Bulky> bulk = m.createBulkRequestContext(Bulky.class, true);
            Map<String, Object> values = new LinkedHashMap<>();
            values.put("single", new Item("two", 2));
            values.put("mode", Mode.A);
            bulk.addSetRequest(byId(m, "a"), values, false, false);
            bulk.runBulk();
            Bulky b = byId(m, "a").get();
            assertEquals("two", b.single.name);
            assertEquals(Mode.A, b.mode);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void pushRequestStoresTheElementShapeOfStore(Morphium m) throws Exception {
        try (m) {
            fresh(m, "a", "b");
            MorphiumBulkContext<Bulky> bulk = m.createBulkRequestContext(Bulky.class, true);
            bulk.addPushRequest(byId(m, "a"), "items", new Item("p", 1), false, false);
            bulk.runBulk();
            byId(m, "b").push("items", new Item("p", 1));
            assertEquals("p", byId(m, "a").get().items.get(0).name);
            assertEquals(raw(m, "b").get("items"), raw(m, "a").get("items"), "the same document as Query.push writes");
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void aCustomMappedValueIsWrittenLikeQuerySet(Morphium m) throws Exception {
        try (m) {
            fresh(m, "a", "b");
            MorphiumBulkContext<Bulky> bulk = m.createBulkRequestContext(Bulky.class, true);
            bulk.addSetRequest(byId(m, "a"), "day", LocalDate.ofEpochDay(19000), false, false);
            bulk.runBulk();
            byId(m, "b").set("day", LocalDate.ofEpochDay(19000));
            assertNotNull(raw(m, "a").get("day"));
            assertEquals(raw(m, "b").get("day"), raw(m, "a").get("day"));
            assertEquals(LocalDate.ofEpochDay(19000), byId(m, "a").get().day);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void plainValuesAndOtherOperatorsStayAsTheyWere(Morphium m) throws Exception {
        try (m) {
            fresh(m, "a");
            byId(m, "a").set("mode", Mode.A);
            MorphiumBulkContext<Bulky> bulk = m.createBulkRequestContext(Bulky.class, true);
            bulk.addSetRequest(byId(m, "a"), "flags", Set.of("k"), false, false);
            bulk.addUnsetRequest(byId(m, "a"), "mode", 1, false, false);
            bulk.runBulk();
            Bulky b = byId(m, "a").get();
            assertEquals(Set.of("k"), b.flags);
            assertEquals(null, b.mode);
        }
    }
}
