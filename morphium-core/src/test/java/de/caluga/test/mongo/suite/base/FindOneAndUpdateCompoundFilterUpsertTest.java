package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.UtilsMap;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import de.caluga.morphium.query.Query;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

/**
 * MOR-003: an upsert whose filter reaches an equality on {@code _id} through {@code $and} (the shape of a lease filter
 * {@code _id = X AND (expiresAt < now OR owner = me)}) must keep the {@code _id} from the filter, not add a generated one.
 * The held lease raises a duplicate key error on both drivers (MOR-030: the InMemoryDriver used to overwrite the held document).
 */
@Tag("core")
public class FindOneAndUpdateCompoundFilterUpsertTest extends MultiDriverTestBase {

    @Entity(translateCamelCase = false)
    public static class Lease {
        @Id
        public String id;
        public String owner;
        public Date expiresAt;
    }

    private Query<Lease> acquireFilter(Morphium m, String leaseId, String me) {
        Query<Lease> q = m.createQueryFor(Lease.class).f("_id").eq(leaseId);
        q.or(m.createQueryFor(Lease.class).f("expiresAt").lt(new Date()), m.createQueryFor(Lease.class).f("owner").eq(me));
        return q;
    }

    private Lease acquire(Morphium m, String leaseId, String me, long ttlMs) {
        Map<String, Object> update = UtilsMap.of("$set", UtilsMap.of("owner", me, "expiresAt", new Date(System.currentTimeMillis() + ttlMs)));
        return acquireFilter(m, leaseId, me).findOneAndUpdate(update, true, true);
    }

    private static boolean isInMem(Morphium m) {
        return m.getDriver() instanceof InMemoryDriver;
    }

    private static boolean isDuplicateKey(Throwable t) {
        for (; t != null; t = t.getCause()) {
            if (t instanceof MorphiumDriverException mde && mde.getMongoCode() != null && ((Number) mde.getMongoCode()).intValue() == 11000) {
                return true;
            }
        }
        return false;
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void firstAcquireInsertsUnderTheFilterIdAndRenewalKeepsOneDocument(Morphium m) throws Exception {
        try (m) {
            Lease first = acquire(m, "job", "A", 60_000);
            assertNotNull(first);
            assertEquals("job", first.id);
            assertEquals("A", first.owner);
            List<Map<String, Object>> raw = m.createQueryFor(Lease.class).asMapList();
            assertEquals(1, raw.size(), String.valueOf(raw));
            assertEquals("job", raw.get(0).get("_id"));

            Lease renewed = acquire(m, "job", "A", 120_000);
            assertEquals("A", renewed.owner);
            assertEquals(1, m.createQueryFor(Lease.class).countAll());
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void heldLeaseRaisesDuplicateKey(Morphium m) throws Exception {
        try (m) {
            acquire(m, "job", "A", 60_000);
            Throwable t = null;
            try {
                acquire(m, "job", "B", 60_000);
            } catch (Throwable e) {
                t = e;
            }
            assertNotNull(t, "the second contender must not get the held lease");
            assertTrue(isDuplicateKey(t), String.valueOf(t));
            assertEquals("A", m.createQueryFor(Lease.class).f("_id").eq("job").get().owner);
            assertEquals(1, m.createQueryFor(Lease.class).countAll());
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void expiredLeaseCanBeTakenOver(Morphium m) throws Exception {
        try (m) {
            acquire(m, "job", "A", -1000);
            Lease b = acquire(m, "job", "B", 60_000);
            assertEquals("B", b.owner);
            assertEquals(1, m.createQueryFor(Lease.class).countAll());
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void contendersRaceForOneLease(Morphium m) throws Exception {
        try (m) {
            int contenders = 8;
            ExecutorService pool = Executors.newFixedThreadPool(contenders);
            CountDownLatch go = new CountDownLatch(1);
            List<Future<String>> results = new ArrayList<>();

            for (int i = 0; i < contenders; i++) {
                String me = "C" + i;
                results.add(pool.submit(() -> {
                    go.await();
                    try {
                        return acquire(m, "race", me, 60_000).owner;
                    } catch (RuntimeException e) {
                        assertTrue(isDuplicateKey(e), "unexpected failure: " + e);
                        return null;
                    }
                }));
            }

            go.countDown();
            List<String> winners = new ArrayList<>();

            for (Future<String> f : results) {
                String w = f.get(30, TimeUnit.SECONDS);
                if (w != null) {
                    winners.add(w);
                }
            }

            pool.shutdown();
            assertEquals(1, winners.size(), "exactly one contender may win: " + winners);
            assertEquals(1, m.createQueryFor(Lease.class).countAll());
            assertEquals(winners.get(0), m.createQueryFor(Lease.class).f("_id").eq("race").get().owner);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void orWithSeveralIdBranchesStillGetsAGeneratedId(Morphium m) throws Exception {
        try (m) {
            // the server does not seed _id from a multi-branch $or: the generated id must still be added
            Query<Lease> q = m.createQueryFor(Lease.class);
            q.or(m.createQueryFor(Lease.class).f("_id").eq("X"), m.createQueryFor(Lease.class).f("_id").eq("Y"));
            Lease l = q.findOneAndUpdate(UtilsMap.of("$set", UtilsMap.of("owner", "me")), true, true);
            assertNotNull(l);
            assertFalse("X".equals(l.id) || "Y".equals(l.id), "id " + l.id);
            assertEquals(1, m.createQueryFor(Lease.class).countAll());
        }
    }
}
