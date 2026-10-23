package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.FailedStore;
import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumStorageAdapter;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

/**
 * MOR-064: {@code store()} and {@code storeList()} of an entity that already exists is an update (a {@code $set} upsert by {@code _id}). A rejected one - a unique index
 * violation - used to be ignored: the reply was never looked at. It raises now, and {@code storeList(list, collection, true)} reports the rejected entities as
 * {@link FailedStore} and stores the others.
 */
@Tag("core")
public class StoreRaisesOnRejectedUpdateTest extends MultiDriverTestBase {

    @Entity
    public static class Unique {
        @Id
        public String id;
        public String code;
        public int counter;
    }

    private void setup(Morphium m) throws Exception {
        m.dropCollection(Unique.class);
        m.ensureIndex(Unique.class, m.getMapper().getCollectionName(Unique.class), Map.of("code", 1), Map.of("unique", true));

        for (int i = 0; i < 4; i++) {
            Unique u = new Unique();
            u.id = "u" + i;
            u.code = "c" + i;
            m.store(u);
        }
    }

    private Unique load(Morphium m, String id) {
        return m.createQueryFor(Unique.class).f("id").eq(id).get();
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void storeOfAnExistingEntityRaisesTheDuplicateKey(Morphium m) throws Exception {
        try (m) {
            setup(m);
            Unique u = load(m, "u0");
            u.code = "c1";
            MorphiumDriverException e = assertThrows(MorphiumDriverException.class, () -> m.store(u));
            assertTrue(e.isDuplicateKey(), String.valueOf(e));
            assertEquals("c0", load(m, "u0").code, "the rejected update changed nothing");
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void storeOfAListRaisesAndDoesNotFirePostStoreForTheRejectedOne(Morphium m) throws Exception {
        try (m) {
            setup(m);
            Set<String> stored = ConcurrentHashMap.newKeySet();
            m.addListener(new MorphiumStorageAdapter<Unique>() {
                @Override
                public void postStore(Morphium mo, Unique r, boolean isNew) {
                    stored.add(r.id);
                }
            });
            Unique ok = load(m, "u0");
            ok.counter = 5;
            Unique dup = load(m, "u1");
            dup.code = "c2";
            assertThrows(MorphiumDriverException.class, () -> m.storeList(new ArrayList<>(List.of(ok, dup))));
            assertEquals("c1", load(m, "u1").code);
            assertTrue(!stored.contains("u1"), "no postStore for the rejected entity: " + stored);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void storeListWithContinueOnErrorReportsTheRejectedEntitiesAndStoresTheOthers(Morphium m) throws Exception {
        try (m) {
            setup(m);
            Unique a = load(m, "u0");
            a.counter = 10;
            Unique dup = load(m, "u1");
            dup.code = "c2"; // collides with u2
            Unique b = load(m, "u3");
            b.counter = 30;
            List<FailedStore<Unique>> failures = m.storeList(new ArrayList<>(List.of(a, dup, b)), m.getMapper().getCollectionName(Unique.class), true);
            assertEquals(1, failures.size(), String.valueOf(failures));
            assertEquals(1, failures.get(0).getIndex(), "the index in the original list");
            assertEquals("u1", failures.get(0).getEntity().id);
            assertTrue(failures.get(0).getCause() instanceof MorphiumDriverException ex && ex.isDuplicateKey(), String.valueOf(failures.get(0).getCause()));
            assertEquals(10, load(m, "u0").counter, "the entity before the rejected one is stored");
            assertEquals(30, load(m, "u3").counter, "the entity after the rejected one is stored, too (unordered)");
            assertEquals("c1", load(m, "u1").code);
        }
    }


    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void aSuccessfulStoreOfAnExistingEntityStillWorks(Morphium m) throws Exception {
        try (m) {
            setup(m);
            Unique u = load(m, "u0");
            u.code = "fresh";
            m.store(u);
            assertEquals("fresh", load(m, "u0").code);
            assertNotNull(load(m, "u1"));
        }
    }
}
