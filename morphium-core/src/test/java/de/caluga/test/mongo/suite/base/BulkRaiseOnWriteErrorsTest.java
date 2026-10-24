package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumStorageAdapter;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.bulk.MorphiumBulkContext;
import de.caluga.morphium.driver.BulkWriteException;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.query.Query;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** MOR-059: the bulk context raises one {@link BulkWriteException} for rejected requests (it used to log and swallow them), ordered and unordered. */
@Tag("core")
public class BulkRaiseOnWriteErrorsTest extends MultiDriverTestBase {

    @Entity
    public static class Slot {
        @Id
        public String id;
        public String code;
        public int counter;
    }

    private void setup(Morphium m) throws Exception {
        m.dropCollection(Slot.class);
        m.ensureIndex(Slot.class, m.getMapper().getCollectionName(Slot.class), Map.of("code", 1), Map.of("unique", true));

        for (int i = 0; i < 3; i++) {
            Slot s = new Slot();
            s.id = "s" + i;
            s.code = "c" + i;
            m.store(s);
        }
    }

    private Query<Slot> byId(Morphium m, String id) {
        return m.createQueryFor(Slot.class).f("id").eq(id);
    }

    /** s0.code := x (ok), s1.code := x (duplicate key), s2.counter := 99 (ok) */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private Map<String, Object> run(Morphium m, boolean ordered) {
        MorphiumBulkContext<Slot> bulk = m.createBulkRequestContext(Slot.class, ordered);
        bulk.addSetRequest(byId(m, "s0"), "code", "x", false, false);
        bulk.addSetRequest(byId(m, "s1"), "code", "x", false, false);
        bulk.addSetRequest(byId(m, "s2"), "counter", 99, false, false);
        return bulk.runBulk();
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void anOrderedBulkStopsAtTheFirstRejectedRequestAndSaysWhatWasApplied(Morphium m) throws Exception {
        try (m) {
            setup(m);
            BulkWriteException e = assertThrows(BulkWriteException.class, () -> run(m, true));
            assertTrue(e.isDuplicateKey(), String.valueOf(e));
            assertEquals(1, e.getFailures().size());
            assertEquals(1, ((Number) e.getPartialResult().get("num_matched")).intValue(), "the first request was applied: " + e.getPartialResult());
            assertTrue(e.getMessage().contains("remaining requests were not executed"), e.getMessage());
            assertEquals("x", byId(m, "s0").get().code);
            assertEquals("c1", byId(m, "s1").get().code);
            assertEquals(0, byId(m, "s2").get().counter, "ordered: the request after the rejected one did not run");
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void anUnorderedBulkRunsEveryRequestAndThenThrowsOnce(Morphium m) throws Exception {
        try (m) {
            setup(m);
            BulkWriteException e = assertThrows(BulkWriteException.class, () -> run(m, false));
            assertTrue(e.isDuplicateKey(), String.valueOf(e));
            assertEquals(1, e.getFailures().size());
            assertEquals(2, ((Number) e.getPartialResult().get("num_matched")).intValue(), "s0 and s2 were applied: " + e.getPartialResult());
            assertTrue(e.getMessage().contains("all other requests were executed"), e.getMessage());
            assertEquals("x", byId(m, "s0").get().code);
            assertEquals(99, byId(m, "s2").get().counter, "unordered: the request after the rejected one ran");
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void aRejectedInsertIsRaisedToo(Morphium m) throws Exception {
        try (m) {
            setup(m);
            Slot dup = new Slot();
            dup.id = "new";
            dup.code = "c0";
            MorphiumBulkContext<Slot> bulk = m.createBulkRequestContext(Slot.class, true);
            bulk.addInsertRequest(List.of(dup));
            MorphiumDriverException e = assertThrows(MorphiumDriverException.class, bulk::runBulk);
            assertTrue(e.isDuplicateKey(), String.valueOf(e));
            assertEquals(3, m.createQueryFor(Slot.class).countAll());
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void aCleanBulkReturnsItsResult(Morphium m) throws Exception {
        try (m) {
            setup(m);
            @SuppressWarnings({"unchecked", "rawtypes"})
            MorphiumBulkContext<Slot> bulk = m.createBulkRequestContext(Slot.class, true);
            bulk.addSetRequest(byId(m, "s0"), "counter", 5, false, false);
            Map<String, Object> result = bulk.runBulk();
            assertEquals(1, ((Number) result.get("num_matched")).intValue());
            assertEquals(5, byId(m, "s0").get().counter);
        }
    }


    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void thePostEventsStillFireForWhatWasApplied(Morphium m) throws Exception {
        try (m) {
            setup(m);
            AtomicInteger postUpdates = new AtomicInteger();
            m.addListener(new MorphiumStorageAdapter<Slot>() {
                @Override
                public void postUpdate(Morphium mo, Class<? extends Slot> cls, Enum updateType) {
                    postUpdates.incrementAndGet();
                }
            });
            assertThrows(BulkWriteException.class, () -> run(m, false));
            assertTrue(postUpdates.get() > 0, "the cluster has to learn about the applied requests (caches)");
        }
    }
}
