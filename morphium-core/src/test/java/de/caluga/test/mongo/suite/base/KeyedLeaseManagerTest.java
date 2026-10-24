package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.annotations.Index;
import de.caluga.morphium.query.LeaseManager;
import de.caluga.morphium.query.LeaseResult;
import de.caluga.morphium.query.LeaseResult.Status;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * MOR-099: a lease that is keyed by another field than the {@code @Id} (a natural key with its own unique index), with a generated {@code _id}, a fencing
 * token that grows with every grant and a renewal that keeps the token (the shape of leases in a service with a job scheduler). On a real MongoDB and on the InMemoryDriver.
 */
@Tag("core")
public class KeyedLeaseManagerTest extends MultiDriverTestBase {

    private static final Instant NOW = Instant.parse("2026-10-23T12:00:00Z");
    private static final Duration TTL = Duration.ofSeconds(60);

    @Entity
    public static class JobLease {
        @Id
        public String id;
        @Index(options = {"unique:true"})
        public String leaseName;
        public String holder;
        public Instant acquiredAt;
        public Instant expiresAt;
        public Long fencingToken;
    }

    /** the same without a declared unique index on the key */
    @Entity
    public static class UnindexedLease {
        @Id
        public String id;
        public String leaseName;
        public String holder;
        public Instant expiresAt;
    }

    private LeaseManager<JobLease> leases(Morphium m) {
        m.ensureIndicesFor(JobLease.class);
        return LeaseManager.builder(m, JobLease.class)
                .keyField("leaseName").ownerField("holder").expiresAtField("expiresAt")
                .acquiredAtField("acquiredAt").fencingField("fencingToken")
                .idGenerator(() -> UUID.randomUUID().toString())
                .build();
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void aFreeKeyedLeaseIsAcquiredWithAGeneratedIdAndTheFirstToken(Morphium m) throws Exception {
        try (m) {
            LeaseResult<JobLease> r = leases(m).tryAcquire("job", "A", TTL, NOW);
            assertEquals(Status.ACQUIRED, r.getStatus());
            JobLease l = r.getLease();
            assertEquals("job", l.leaseName);
            assertNotNull(l.id, "the generated _id");
            assertEquals(36, l.id.length(), "a UUID, not an ObjectId");
            assertEquals("A", l.holder);
            assertEquals(NOW, l.acquiredAt);
            assertEquals(NOW.plus(TTL), l.expiresAt);
            assertEquals(1L, l.fencingToken);
            assertEquals(1L, r.getFencingToken());
            assertEquals(1, m.createQueryFor(JobLease.class).countAll());
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void aHeldLeaseIsReportedAsHeldAndKeepsItsToken(Morphium m) throws Exception {
        try (m) {
            LeaseManager<JobLease> lm = leases(m);
            lm.tryAcquire("job", "A", TTL, NOW);
            LeaseResult<JobLease> r = lm.tryAcquire("job", "B", TTL, NOW.plusSeconds(10));
            assertEquals(Status.HELD, r.getStatus());
            assertNull(r.getLease());
            assertNull(r.getFencingToken());
            JobLease stored = m.createQueryFor(JobLease.class).f("leaseName").eq("job").get();
            assertEquals("A", stored.holder);
            assertEquals(1L, stored.fencingToken);
            assertEquals(1, m.createQueryFor(JobLease.class).countAll());
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void everyGrantCountsAlsoTheReacquisitionByTheHolder(Morphium m) throws Exception {
        try (m) {
            LeaseManager<JobLease> lm = leases(m);
            assertEquals(1L, lm.tryAcquire("job", "A", TTL, NOW).getFencingToken());
            assertEquals(2L, lm.tryAcquire("job", "A", TTL, NOW.plusSeconds(10)).getFencingToken(), "the holder acquires again: a new grant");
            assertEquals(3L, lm.tryAcquire("job", "B", TTL, NOW.plusSeconds(200)).getFencingToken(), "an expired lease is taken over");
            assertEquals("B", m.createQueryFor(JobLease.class).f("leaseName").eq("job").get().holder);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void aLeaseIsExpiredAtItsDeadline(Morphium m) throws Exception {
        try (m) {
            LeaseManager<JobLease> lm = leases(m);
            lm.tryAcquire("job", "A", TTL, NOW);
            assertEquals(Status.HELD, lm.tryAcquire("job", "B", TTL, NOW.plus(TTL).minusMillis(1)).getStatus());
            assertEquals(Status.ACQUIRED, lm.tryAcquire("job", "B", TTL, NOW.plus(TTL)).getStatus(), "at the deadline it is expired");
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void renewMovesTheDeadlineAndKeepsTheTokenAndTheAcquisitionTime(Morphium m) throws Exception {
        try (m) {
            LeaseManager<JobLease> lm = leases(m);
            long token = lm.tryAcquire("job", "A", TTL, NOW).getFencingToken();
            LeaseResult<JobLease> renewed = lm.renew("job", "A", token, TTL, NOW.plusSeconds(30));
            assertTrue(renewed.isAcquired());
            assertEquals(NOW.plusSeconds(90), renewed.getLease().expiresAt);
            assertEquals(token, renewed.getFencingToken(), "a renewal does not count");
            assertEquals(NOW, renewed.getLease().acquiredAt);
            LeaseResult<JobLease> again = lm.renew("job", "A", token, TTL, NOW.plusSeconds(60));
            assertTrue(again.isAcquired(), "the token stays valid for the next renewal");
            assertEquals(NOW.plusSeconds(120), again.getLease().expiresAt);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void renewWithAStaleTokenOrAnotherOwnerIsLost(Morphium m) throws Exception {
        try (m) {
            LeaseManager<JobLease> lm = leases(m);
            long first = lm.tryAcquire("job", "A", TTL, NOW).getFencingToken();
            long second = lm.tryAcquire("job", "B", TTL, NOW.plusSeconds(100)).getFencingToken();
            assertNotEquals(first, second);

            LeaseResult<JobLease> staleOwner = lm.renew("job", "A", first, TTL, NOW.plusSeconds(101));
            assertEquals(Status.LOST, staleOwner.getStatus());
            assertFalse(staleOwner.isAcquired());
            assertNull(staleOwner.getLease());
            assertEquals(Status.LOST, lm.renew("job", "B", first, TTL, NOW.plusSeconds(101)).getStatus(), "the right owner with a stale token");
            assertEquals(Status.LOST, lm.renew("job", "A", second, TTL, NOW.plusSeconds(101)).getStatus(), "the right token of the wrong owner");
            assertEquals(Status.LOST, lm.renew("unknown", "A", 1L, TTL, NOW).getStatus());

            JobLease stored = m.createQueryFor(JobLease.class).f("leaseName").eq("job").get();
            assertEquals("B", stored.holder);
            assertEquals(second, stored.fencingToken);
            assertEquals(NOW.plusSeconds(160), stored.expiresAt, "nothing was changed by the lost renewals");
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void anExpiredLeaseThatNobodyTookOverCanStillBeRenewedByItsHolder(Morphium m) throws Exception {
        try (m) {
            LeaseManager<JobLease> lm = leases(m);
            long token = lm.tryAcquire("job", "A", TTL, NOW).getFencingToken();
            LeaseResult<JobLease> r = lm.renew("job", "A", token, TTL, NOW.plusSeconds(500));
            assertTrue(r.isAcquired(), "any takeover would have changed the token");
            assertEquals(token, r.getFencingToken());
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void releaseKeepsTheCounter(Morphium m) throws Exception {
        try (m) {
            LeaseManager<JobLease> lm = leases(m);
            lm.tryAcquire("job", "A", TTL, NOW);
            assertTrue(lm.release("job", "A"));
            assertFalse(lm.release("job", "B"));
            assertEquals(2L, lm.tryAcquire("job", "B", TTL, NOW.plusSeconds(1)).getFencingToken(), "the token does not start again");
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void releaseWithATokenGivesUpOnlyThatGrant(Morphium m) throws Exception {
        try (m) {
            LeaseManager<JobLease> lm = leases(m);
            long old = lm.tryAcquire("job", "A", TTL, NOW).getFencingToken();
            long newer = lm.tryAcquire("job", "A", TTL, NOW.plusSeconds(1)).getFencingToken();
            assertFalse(lm.release("job", "A", old), "the older grant is not the lease any more");
            assertEquals(Status.HELD, lm.tryAcquire("job", "B", TTL, NOW.plusSeconds(2)).getStatus());
            assertTrue(lm.release("job", "A", newer));
            assertTrue(lm.tryAcquire("job", "B", TTL, NOW.plusSeconds(3)).isAcquired());
            assertThrows(IllegalArgumentException.class, () -> LeaseManager.builder(m, JobLease.class).ownerField("holder").expiresAtField("expiresAt").keyField("leaseName")
                    .assumeUniqueKey().build().release("job", "A", 1L), "a manager without a fencing field has no token");
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void withoutAnIdGeneratorTheDatabaseAssignsTheId(Morphium m) throws Exception {
        try (m) {
            m.ensureIndicesFor(JobLease.class);
            LeaseManager<JobLease> lm = LeaseManager.builder(m, JobLease.class).keyField("leaseName").ownerField("holder").expiresAtField("expiresAt").build();
            LeaseResult<JobLease> r = lm.tryAcquire("job", "A", TTL, NOW);
            assertTrue(r.isAcquired());
            assertNotNull(r.getLease().id);
            assertEquals("job", r.getLease().leaseName);
            assertEquals(Status.HELD, lm.tryAcquire("job", "B", TTL, NOW.plusSeconds(1)).getStatus());
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void eightContendersOnAFreshKeyHaveOneWinnerAndOneDocument(Morphium m) throws Exception {
        try (m) {
            LeaseManager<JobLease> lm = leases(m);

            for (int round = 0; round < 5; round++) {
                String key = "race-" + round;
                ExecutorService pool = Executors.newFixedThreadPool(8);
                CountDownLatch go = new CountDownLatch(1);
                List<Future<LeaseResult<JobLease>>> futures = new ArrayList<>();

                for (int i = 0; i < 8; i++) {
                    String me = "C" + i;
                    futures.add(pool.submit(() -> {
                        go.await();
                        return lm.tryAcquire(key, me, TTL, NOW);
                    }));
                }

                go.countDown();
                int winners = 0;

                for (Future<LeaseResult<JobLease>> f : futures) {
                    if (f.get(60, TimeUnit.SECONDS).isAcquired()) {
                        winners++;
                    }
                }

                pool.shutdown();
                assertEquals(1, winners, "round " + round);
                assertEquals(1, m.createQueryFor(JobLease.class).f("leaseName").eq(key).countAll(), "one document per key");
            }
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void aLeaseWithoutFencingHasNoToken(Morphium m) throws Exception {
        try (m) {
            m.ensureIndicesFor(JobLease.class);
            LeaseManager<JobLease> lm = LeaseManager.builder(m, JobLease.class)
                    .keyField("leaseName").ownerField("holder").expiresAtField("expiresAt")
                    .idGenerator(() -> UUID.randomUUID().toString()).build();
            LeaseResult<JobLease> r = lm.tryAcquire("job", "A", TTL, NOW);
            assertTrue(r.isAcquired());
            assertNull(r.getFencingToken());
            assertNull(r.getLease().fencingToken);
            assertTrue(lm.renew("job", "A", TTL, NOW.plusSeconds(10)).isAcquired(), "renew without a token");
            assertEquals(Status.LOST, lm.renew("job", "B", TTL, NOW.plusSeconds(10)).getStatus());
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void aKeyWithoutADeclaredUniqueIndexIsRefusedUnlessAssumed(Morphium m) throws Exception {
        try (m) {
            assertThrows(IllegalArgumentException.class, () -> LeaseManager.builder(m, UnindexedLease.class)
                    .keyField("leaseName").ownerField("holder").expiresAtField("expiresAt").build());
            assertNotNull(LeaseManager.builder(m, UnindexedLease.class)
                    .keyField("leaseName").ownerField("holder").expiresAtField("expiresAt").assumeUniqueKey().build());
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void invalidDefinitionsAreRejected(Morphium m) throws Exception {
        try (m) {
            assertThrows(IllegalArgumentException.class, () -> LeaseManager.builder(m, JobLease.class).ownerField("holder").expiresAtField("expiresAt")
                    .fencingField("holder").build(), "the token is a number");
            assertThrows(IllegalArgumentException.class, () -> LeaseManager.builder(m, JobLease.class).ownerField("holder").expiresAtField("expiresAt")
                    .acquiredAtField("holder").build(), "the acquisition time is a point in time");
            assertThrows(IllegalArgumentException.class, () -> LeaseManager.builder(m, JobLease.class).ownerField("holder").expiresAtField("expiresAt")
                    .keyField("noSuchField").build());
            assertThrows(IllegalArgumentException.class, () -> LeaseManager.builder(m, JobLease.class).ownerField("holder").expiresAtField("expiresAt")
                    .idGenerator(() -> "x").build(), "an id generator needs a key field other than the id");
            assertThrows(IllegalArgumentException.class, () -> LeaseManager.builder(m, JobLease.class).expiresAtField("expiresAt").build(), "the owner field is required");
            LeaseManager<JobLease> lm = leases(m);
            assertThrows(IllegalArgumentException.class, () -> lm.renew("job", "A", Duration.ZERO, NOW));
            assertThrows(IllegalArgumentException.class, () -> lm.renew("job", null, 1L, TTL, NOW));
            assertThrows(IllegalArgumentException.class, () -> lm.renew("job", "A", TTL, NOW), "this manager has a fencing field: the token is required");
        }
    }
}
