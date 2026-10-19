package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.annotations.Embedded;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.annotations.caching.WriteBuffer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * MOR-046: the buffered writer of a {@code @WriteBuffer} entity gave the values of {@code set}, {@code push}, {@code pushAll}, {@code addToSet} and {@code pull} to the
 * driver as they were and sent {@code $push} for a {@code pullAll}. The same call on a synchronous entity stores embedded values, Sets and enums and removes the elements;
 * the buffered one now stores the same.
 */
@Tag("core")
public class BufferedWriterUpdateValuesTest extends MultiDriverTestBase {

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
    @WriteBuffer(size = 100, timeout = 200)
    public static class Buffered {
        @Id
        public String id;
        public Item single;
        public Mode mode;
        public List<Item> items = new ArrayList<>();
        public Set<String> flags;
        public List<String> tags = new ArrayList<>();
    }

    private Buffered fresh(Morphium m) {
        // no dropCollection: for a @WriteBuffer entity it is queued with the other writes and runs asynchronously, so it can arrive after the store below and delete the
        // document. The database of the Morphium instance is dropped when the instance is created, the collection is empty.
        Buffered b = new Buffered();
        b.id = "b";
        b.tags = new ArrayList<>(List.of("a", "b", "c"));
        m.store(b);
        TestUtils.waitForConditionToBecomeTrue(30000, "the document was not written", () -> m.createQueryFor(Buffered.class).f("id").eq("b").get() != null);
        return b;
    }

    private Buffered load(Morphium m) {
        return m.createQueryFor(Buffered.class).f("id").eq("b").get();
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void setStoresEmbeddedValuesSetsAndEnums(Morphium m) throws Exception {
        try (m) {
            fresh(m);
            m.createQueryFor(Buffered.class).f("id").eq("b").set(Map.of("single", new Item("one", 1), "mode", Mode.B, "flags", Set.of("x")), false, false);
            TestUtils.waitForConditionToBecomeTrue(30000, "the set was not written", () -> load(m).single != null);
            Buffered b = load(m);
            assertEquals("one", b.single.name);
            assertEquals(Mode.B, b.mode);
            assertEquals(Set.of("x"), b.flags);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void pushAndAddToSetStoreEmbeddedValues(Morphium m) throws Exception {
        try (m) {
            fresh(m);
            m.createQueryFor(Buffered.class).f("id").eq("b").push("items", new Item("p", 1));
            m.addToSet(m.createQueryFor(Buffered.class).f("id").eq("b"), "items", new Item("s", 2));
            TestUtils.waitForConditionToBecomeTrue(30000, "the pushes were not written", () -> load(m).items.size() == 2);
            Buffered b = load(m);
            assertEquals(List.of("p", "s"), b.items.stream().map(i -> i.name).toList());
            assertEquals(2, b.items.get(1).qty);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void pushAllStoresEmbeddedValues(Morphium m) throws Exception {
        try (m) {
            fresh(m);
            m.createQueryFor(Buffered.class).f("id").eq("b").pushAll("items", List.of(new Item("x", 1), new Item("y", 2)));
            TestUtils.waitForConditionToBecomeTrue(30000, "the pushAll was not written", () -> load(m).items.size() == 2);
            assertEquals(List.of("x", "y"), load(m).items.stream().map(i -> i.name).toList());
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void pullRemovesTheElement(Morphium m) throws Exception {
        try (m) {
            fresh(m);
            m.createQueryFor(Buffered.class).f("id").eq("b").pull("tags", "b");
            TestUtils.waitForConditionToBecomeTrue(30000, "the pull was not written", () -> load(m).tags.size() == 2);
            assertEquals(List.of("a", "c"), load(m).tags);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void pullAllRemovesTheElements(Morphium m) throws Exception {
        try (m) {
            fresh(m);
            m.createQueryFor(Buffered.class).f("id").eq("b").pullAll("tags", List.of("a", "c"));
            TestUtils.waitForConditionToBecomeTrue(30000, "the pullAll was not written", () -> load(m).tags.size() != 3);
            assertNotNull(load(m).tags);
            assertEquals(List.of("b"), load(m).tags, "pullAll removes the elements; it used to push them");
        }
    }
}
