package de.caluga.test.mongo.suite.repro;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.annotations.Reference;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.query.ReturnDocument;
import de.caluga.test.mongo.suite.base.MultiDriverTestBase;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Hypothesis 1 of the update-path research for values that are an {@code @Entity} or go into a {@code @Reference} field (MOR-036): what an update does with an entity.
 * <ul>
 * <li>An entity into an {@code Object} field or a field typed with the entity class becomes a document with {@code class_name} and its {@code _id}, and reads back typed.</li>
 * <li>An entity into a {@code @Reference} field is rejected by {@code findOneAndUpdate} and by the update builder (MOR-002), because it would be embedded.</li>
 * <li>PINNED DEVIATION MOR-049: the writer path ({@code Query.set}, {@code Query.push}) has no such guard. It embeds the entity in the {@code @Reference} field and the document cannot
 * be read back any more ({@code ClassCastException}: the mapper expects a reference there). When MOR-049 is done these two pins flip to the rejection.</li>
 * </ul>
 * The results of each case are logged as {@code REPRO-RESULT} lines for the topology matrix in {@code docs/research/reproduction-results.md}.
 */
@Tag("core")
public class EntityAndReferenceValueReproductionTest extends MultiDriverTestBase {
    @Entity(translateCamelCase = false)
    public static class Target {
        @Id
        public MorphiumId id;
        public String label;
    }

    @Entity(translateCamelCase = false)
    public static class Holder {
        @Id
        public MorphiumId id;
        public String key;
        public Object any;
        public List<Object> anyList;
        public Target embeddedTarget;
        @Reference
        public Target ref;
        @Reference
        public List<Target> refs;
    }

    private Target target(Morphium m) {
        Target t = new Target();
        t.label = "t";
        m.store(t);
        return t;
    }

    private void holder(Morphium m, String key) {
        Holder h = new Holder();
        h.key = key;
        h.refs = new ArrayList<>();
        m.store(h);
    }

    private Map<String, Object> raw(Morphium m, String key) {
        return m.createQueryFor(Holder.class).f("key").eq(key).asMapList().get(0);
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void anEntityInAnObjectFieldIsADocumentWithItsIdAndClassNameAndReadsBackAsAMap(Morphium m) throws Exception {
        try (m) {
            Target t = target(m);
            holder(m, "k1");
            m.createQueryFor(Holder.class).f("key").eq("k1").findOneAndUpdate(Map.of("$set", Map.of("any", t)), false, true);
            Holder viaStore = new Holder();
            viaStore.key = "k1s";
            viaStore.any = t;
            viaStore.refs = new ArrayList<>();
            m.store(viaStore);

            Map<?, ?> any = (Map<?, ?>) raw(m, "k1").get("any");
            ReproSupport.report(m.getDriver().getName(), "E1 entity in Object field", String.valueOf(any));
            assertEquals(t.id.toString(), String.valueOf(any.get("_id")), "an entity keeps its _id");
            assertEquals(Target.class.getName(), any.get("class_name"));
            // the accepted difference to store() (decided with the walk of the update paths): an update does not know the declared type of the field, so it writes the
            // class_name and the _id; store() writes the fields only, and the type is lost on the way back
            Map<?, ?> stored = (Map<?, ?>) raw(m, "k1s").get("any");
            ReproSupport.report(m.getDriver().getName(), "E1 store() writes", String.valueOf(stored));
            assertEquals("t", stored.get("label"));
            Object viaUpdate = m.createQueryFor(Holder.class).f("key").eq("k1").get().any;
            Object viaStoreRead = m.createQueryFor(Holder.class).f("key").eq("k1s").get().any;
            ReproSupport.report(m.getDriver().getName(), "E1 read back (update / store)", viaUpdate.getClass().getSimpleName() + " / " + viaStoreRead.getClass().getSimpleName());
            // a single Object field reads back as a Map, whoever wrote it: the class_name is not used for it (it is for the members of a List<Object> or Map<String, Object>, see below)
            assertTrue(viaUpdate instanceof Map && viaStoreRead instanceof Map, "update: " + viaUpdate.getClass() + ", store: " + viaStoreRead.getClass());
            assertEquals("t", ((Map<?, ?>) viaUpdate).get("label"));
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void entitiesInAnObjectListReadBackTypedForAnUpdateAsForStore(Morphium m) throws Exception {
        try (m) {
            Target t = target(m);
            holder(m, "k1b");
            m.createQueryFor(Holder.class).f("key").eq("k1b").findOneAndUpdate(Map.of("$set", Map.of("anyList", List.of(t))), false, true);
            Holder viaStore = new Holder();
            viaStore.key = "k1c";
            viaStore.anyList = new ArrayList<>(List.of(t));
            viaStore.refs = new ArrayList<>();
            m.store(viaStore);
            Object viaUpdate = m.createQueryFor(Holder.class).f("key").eq("k1b").get().anyList.get(0);
            Object viaStoreRead = m.createQueryFor(Holder.class).f("key").eq("k1c").get().anyList.get(0);
            ReproSupport.report(m.getDriver().getName(), "E1b entity in a List<Object>, read back (update / store)", viaUpdate.getClass().getSimpleName() + " / " + viaStoreRead.getClass().getSimpleName());
            assertTrue(viaUpdate instanceof Target, "the class_name brings the type back: " + viaUpdate.getClass());
            assertTrue(viaStoreRead instanceof Target, "and store() does the same: " + viaStoreRead.getClass());
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void anEntityInAFieldTypedWithTheEntityClassReadsBackTyped(Morphium m) throws Exception {
        try (m) {
            Target t = target(m);
            holder(m, "k2");
            m.createQueryFor(Holder.class).f("key").eq("k2").set("embeddedTarget", t);
            ReproSupport.report(m.getDriver().getName(), "E2 entity in a field typed with the entity", String.valueOf(raw(m, "k2").get("embeddedTarget")));
            assertEquals("t", m.createQueryFor(Holder.class).f("key").eq("k2").get().embeddedTarget.label);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void findOneAndUpdateAndTheBuilderRejectAnEntityForAReferenceField(Morphium m) throws Exception {
        try (m) {
            Target t = target(m);
            holder(m, "k3");
            IllegalArgumentException viaMap = assertThrows(IllegalArgumentException.class,
                    () -> m.createQueryFor(Holder.class).f("key").eq("k3").findOneAndUpdate(Map.of("$set", Map.of("ref", t)), false, true));
            IllegalArgumentException viaBuilder = assertThrows(IllegalArgumentException.class,
                    () -> m.createQueryFor(Holder.class).f("key").eq("k3").modify().set("ref", t).findOneAndUpdate(ReturnDocument.AFTER));
            ReproSupport.report(m.getDriver().getName(), "E3 findOneAndUpdate and builder, @Reference <- entity", "rejected: " + viaMap.getMessage());
            assertTrue(viaMap.getMessage().contains("@Reference") && viaBuilder.getMessage().contains("@Reference"));
            assertEquals(null, raw(m, "k3").get("ref"), "nothing was written");
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void pinnedMor049TheWriterPathEmbedsAnEntityInAReferenceFieldAndTheDocumentCannotBeReadBack(Morphium m) throws Exception {
        try (m) {
            Target t = target(m);
            holder(m, "k4");
            m.createQueryFor(Holder.class).f("key").eq("k4").set("ref", t);
            Object ref = raw(m, "k4").get("ref");
            ReproSupport.report(m.getDriver().getName(), "E4 Query.set, @Reference <- entity (MOR-049)", String.valueOf(ref));
            assertTrue(ref instanceof Map && ((Map<?, ?>) ref).containsKey("label"), "the whole entity is embedded: " + ref);
            RuntimeException e = assertThrows(RuntimeException.class, () -> m.createQueryFor(Holder.class).f("key").eq("k4").get());
            ReproSupport.report(m.getDriver().getName(), "E4 read back", ReproSupport.describe(e));
            assertTrue(ReproSupport.describe(e).contains("ClassCastException"), ReproSupport.describe(e));
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void pinnedMor049PushOfAnEntityIntoAReferenceListIsEmbeddedToo(Morphium m) throws Exception {
        try (m) {
            Target t = target(m);
            holder(m, "k5");
            m.createQueryFor(Holder.class).f("key").eq("k5").push("refs", t);
            List<?> refs = (List<?>) raw(m, "k5").get("refs");
            ReproSupport.report(m.getDriver().getName(), "E5 Query.push, @Reference list <- entity (MOR-049)", String.valueOf(refs));
            assertNotNull(refs);
            assertEquals(1, refs.size());
            assertTrue(refs.get(0) instanceof Map && ((Map<?, ?>) refs.get(0)).containsKey("label"), "the whole entity is embedded: " + refs);
        }
    }
}
