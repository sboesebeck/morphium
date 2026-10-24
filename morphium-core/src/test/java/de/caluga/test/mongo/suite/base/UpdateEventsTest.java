package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumAccessVetoException;
import de.caluga.morphium.MorphiumStorageAdapter;
import de.caluga.morphium.MorphiumStorageListener.UpdateTypes;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.query.ReturnDocument;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * MOR-073: an update of the writer path fires one {@code preUpdate} and one {@code postUpdate}, both with the type of the call. {@code push}, {@code pull} and
 * {@code addToSet} used to fire each of them twice, and the {@code postUpdate} of {@code pull}, {@code addToSet}, {@code pushAll} and {@code pullAll} was always
 * {@code PUSH}: a listener counted every push twice and could not pair a pull.
 */
@Tag("core")
public class UpdateEventsTest extends MultiDriverTestBase {

    @Entity
    public static class Holder {
        @Id
        public String id;
        public int hits;
        public List<String> tags = new ArrayList<>();
    }

    private static class Recorder extends MorphiumStorageAdapter<Object> {
        final List<String> events = new ArrayList<>();

        @Override
        public void preUpdate(Morphium m, Class<?> cls, Enum updateType) {
            events.add("pre " + updateType);
        }

        @Override
        public void postUpdate(Morphium m, Class<?> cls, Enum updateType) {
            events.add("post " + updateType);
        }
    }

    private Recorder prepare(Morphium m) {
        m.dropCollection(Holder.class);
        Holder h = new Holder();
        h.id = "h1";
        h.tags = new ArrayList<>(List.of("a", "b", "c"));
        m.store(h);
        Recorder r = new Recorder();
        m.addListener(r);
        return r;
    }

    private void assertOnePair(Recorder r, UpdateTypes type) {
        assertEquals(List.of("pre " + type, "post " + type), r.events);
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void pushFiresOnePair(Morphium m) throws Exception {
        try (m) {
            Recorder r = prepare(m);
            m.createQueryFor(Holder.class).f("id").eq("h1").push("tags", "x");
            assertOnePair(r, UpdateTypes.PUSH);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void pullFiresOnePairOfTypePull(Morphium m) throws Exception {
        try (m) {
            Recorder r = prepare(m);
            m.createQueryFor(Holder.class).f("id").eq("h1").pull("tags", "b");
            assertOnePair(r, UpdateTypes.PULL);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void addToSetFiresOnePairOfTypeAddToSet(Morphium m) throws Exception {
        try (m) {
            Recorder r = prepare(m);
            m.addToSet(m.createQueryFor(Holder.class).f("id").eq("h1"), "tags", "x");
            assertOnePair(r, UpdateTypes.ADD_TO_SET);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void pushAllFiresOnePair(Morphium m) throws Exception {
        try (m) {
            Recorder r = prepare(m);
            m.createQueryFor(Holder.class).f("id").eq("h1").pushAll("tags", List.of("x", "y"));
            assertOnePair(r, UpdateTypes.PUSH);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void pullAllFiresOnePairOfTypePull(Morphium m) throws Exception {
        try (m) {
            Recorder r = prepare(m);
            m.createQueryFor(Holder.class).f("id").eq("h1").pullAll("tags", List.of("a", "b"));
            assertOnePair(r, UpdateTypes.PULL);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void setAndIncFireOnePair(Morphium m) throws Exception {
        try (m) {
            Recorder r = prepare(m);
            m.createQueryFor(Holder.class).f("id").eq("h1").set("hits", 5);
            assertOnePair(r, UpdateTypes.SET);
            r.events.clear();
            m.createQueryFor(Holder.class).f("id").eq("h1").inc("hits", 1);
            assertOnePair(r, UpdateTypes.INC);
        }
    }

    // ------------------------------------------------------------------ MOR-072: the builder, findOneAndUpdate(Map), updateOne and updateMany

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void theBuilderFiresOnePairWithTheTypeOfItsOperator(Morphium m) throws Exception {
        try (m) {
            Recorder r = prepare(m);
            m.createQueryFor(Holder.class).f("id").eq("h1").modify().inc("hits", 2).findOneAndUpdate(ReturnDocument.AFTER);
            assertOnePair(r, UpdateTypes.INC);
            r.events.clear();
            m.createQueryFor(Holder.class).f("id").eq("h1").modify().push("tags", "x").findOneAndUpdate(ReturnDocument.AFTER);
            assertOnePair(r, UpdateTypes.PUSH);
            r.events.clear();
            m.createQueryFor(Holder.class).f("id").eq("h1").modify().set("hits", 7).tryFindOneAndUpdate(ReturnDocument.AFTER);
            assertOnePair(r, UpdateTypes.SET);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void aBuilderWithSeveralOperatorsFiresCustom(Morphium m) throws Exception {
        try (m) {
            Recorder r = prepare(m);
            m.createQueryFor(Holder.class).f("id").eq("h1").modify().set("hits", 1).push("tags", "x").findOneAndUpdate(ReturnDocument.AFTER);
            assertOnePair(r, UpdateTypes.CUSTOM);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void updateOneAndUpdateManyFireOnePair(Morphium m) throws Exception {
        try (m) {
            Recorder r = prepare(m);
            m.createQueryFor(Holder.class).f("id").eq("h1").modify().inc("hits", 1).updateOne();
            assertOnePair(r, UpdateTypes.INC);
            r.events.clear();
            m.createQueryFor(Holder.class).modify().set("hits", 3).updateMany();
            assertOnePair(r, UpdateTypes.SET);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void findOneAndUpdateOfAMapFiresOnePair(Morphium m) throws Exception {
        try (m) {
            Recorder r = prepare(m);
            m.createQueryFor(Holder.class).f("id").eq("h1").findOneAndUpdate(Map.of("$set", Map.of("hits", 4)), false, true);
            assertOnePair(r, UpdateTypes.SET);
            r.events.clear();
            m.createQueryFor(Holder.class).f("id").eq("h1").findOneAndUpdate(Map.of("$inc", Map.of("hits", 1), "$set", Map.of("tags", List.of("z"))), false, true);
            assertOnePair(r, UpdateTypes.CUSTOM);
            r.events.clear();
            m.createQueryFor(Holder.class).f("id").eq("h1").findOneAndUpdate(Map.of("$set", Map.of("hits", 5)));
            assertOnePair(r, UpdateTypes.SET);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void anInvalidBuilderFiresNothing(Morphium m) throws Exception {
        try (m) {
            Recorder r = prepare(m);
            assertThrows(IllegalStateException.class, () -> m.createQueryFor(Holder.class).f("id").eq("h1").modify().findOneAndUpdate(ReturnDocument.AFTER));
            assertEquals(List.of(), r.events);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void aVetoOfPreUpdateStopsTheBuilderAndTheMapUpdate(Morphium m) throws Exception {
        try (m) {
            prepare(m);
            m.addListener(new MorphiumStorageAdapter<Object>() {
                @Override
                public void preUpdate(Morphium mm, Class<?> cls, Enum updateType) {
                    throw new MorphiumAccessVetoException("no updates");
                }
            });

            assertThrows(MorphiumAccessVetoException.class, () -> m.createQueryFor(Holder.class).f("id").eq("h1").modify().set("hits", 9).findOneAndUpdate(ReturnDocument.AFTER));
            assertThrows(MorphiumAccessVetoException.class, () -> m.createQueryFor(Holder.class).f("id").eq("h1").modify().set("hits", 9).updateOne());
            assertThrows(MorphiumAccessVetoException.class, () -> m.createQueryFor(Holder.class).f("id").eq("h1").findOneAndUpdate(Map.of("$set", Map.of("hits", 9)), false, true));
            assertEquals(0, m.createQueryFor(Holder.class).f("id").eq("h1").get().hits, "nothing was written");
        }
    }
}
