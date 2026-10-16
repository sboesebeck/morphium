package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.annotations.Version;
import de.caluga.morphium.query.LeaseManager;
import de.caluga.morphium.query.LeaseResult;
import de.caluga.morphium.query.LeaseResult.Status;
import de.caluga.test.mongo.suite.base.ModifyFixtures.Lease;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** MOR-011: the typed lease primitive on top of the update builder, on a real MongoDB and on the InMemoryDriver. */
@Tag("core")
public class LeaseManagerTest extends MultiDriverTestBase {

    private static final Instant NOW = Instant.parse("2026-10-16T12:00:00Z");
    private static final Duration TTL = Duration.ofSeconds(60);

    @Entity
    public static class DateLease {
        @Id
        public String id;
        public String holder;
        public Date until;
        @Version
        public Long version;
    }

    private LeaseManager<Lease> leases(Morphium m) {
        return LeaseManager.of(m, Lease.class, "ownerName", "expiresAt");
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void aFreeLeaseIsAcquired(Morphium m) throws Exception {
        try (m) {
            LeaseResult<Lease> r = leases(m).tryAcquire("job", "A", TTL, NOW);
            assertEquals(Status.ACQUIRED, r.getStatus());
            assertTrue(r.isAcquired());
            assertEquals("job", r.getLease().id);
            assertEquals("A", r.getLease().ownerName);
            assertEquals(NOW.plus(TTL), r.getLease().expiresAt);
            assertEquals(1L, r.getLease().entityVersion);
            assertEquals(1, m.createQueryFor(Lease.class).countAll());
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void aHeldLeaseIsReportedAsHeldAndStaysUntouched(Morphium m) throws Exception {
        try (m) {
            leases(m).tryAcquire("job", "A", TTL, NOW);
            LeaseResult<Lease> r = leases(m).tryAcquire("job", "B", TTL, NOW.plusSeconds(10));
            assertEquals(Status.HELD, r.getStatus());
            assertFalse(r.isAcquired());
            assertNull(r.getLease());
            Lease stored = m.createQueryFor(Lease.class).f("id").eq("job").get();
            assertEquals("A", stored.ownerName);
            assertEquals(NOW.plus(TTL), stored.expiresAt);
            assertEquals(1L, stored.entityVersion);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void theHolderRenewsAndTheDeadlineMoves(Morphium m) throws Exception {
        try (m) {
            leases(m).tryAcquire("job", "A", TTL, NOW);
            LeaseResult<Lease> renewed = leases(m).tryAcquire("job", "A", TTL, NOW.plusSeconds(30));
            assertTrue(renewed.isAcquired());
            assertEquals(NOW.plusSeconds(90), renewed.getLease().expiresAt);
            assertEquals(2L, renewed.getLease().entityVersion, "the version is a fencing token: it grows with every grant");
            assertEquals(1, m.createQueryFor(Lease.class).countAll());
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void anExpiredLeaseCanBeTakenOver(Morphium m) throws Exception {
        try (m) {
            leases(m).tryAcquire("job", "A", TTL, NOW);
            LeaseResult<Lease> r = leases(m).tryAcquire("job", "B", TTL, NOW.plusSeconds(61));
            assertTrue(r.isAcquired());
            assertEquals("B", r.getLease().ownerName);
            assertEquals(2L, r.getLease().entityVersion);
            assertEquals(LeaseResult.Status.HELD, leases(m).tryAcquire("job", "A", TTL, NOW.plusSeconds(62)).getStatus(), "the former holder lost it");
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void releaseFreesTheLeaseForAnotherOwner(Morphium m) throws Exception {
        try (m) {
            leases(m).tryAcquire("job", "A", TTL, NOW);
            assertTrue(leases(m).release("job", "A"));
            LeaseResult<Lease> r = leases(m).tryAcquire("job", "B", TTL, NOW.plusSeconds(1));
            assertTrue(r.isAcquired(), "released means expired at once, whatever the clock says");
            assertEquals("B", r.getLease().ownerName);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void aStrangerCannotReleaseAndAnUnknownIdIsFalse(Morphium m) throws Exception {
        try (m) {
            leases(m).tryAcquire("job", "A", TTL, NOW);
            assertFalse(leases(m).release("job", "B"));
            assertFalse(leases(m).release("nothing", "A"));
            assertEquals(Status.HELD, leases(m).tryAcquire("job", "B", TTL, NOW.plusSeconds(1)).getStatus());
            assertEquals(0, m.createQueryFor(Lease.class).f("id").eq("nothing").countAll(), "release never creates a document");
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void aDateExpiryFieldWorksToo(Morphium m) throws Exception {
        try (m) {
            LeaseManager<DateLease> dl = LeaseManager.of(m, DateLease.class, "holder", "until");
            assertTrue(dl.tryAcquire("d", "A", TTL, NOW).isAcquired());
            assertFalse(dl.tryAcquire("d", "B", TTL, NOW.plusSeconds(5)).isAcquired());
            DateLease stored = m.createQueryFor(DateLease.class).f("id").eq("d").get();
            assertEquals(Date.from(NOW.plus(TTL)), stored.until);
            assertTrue(dl.tryAcquire("d", "B", TTL, NOW.plusSeconds(120)).isAcquired());
            assertTrue(dl.release("d", "B"));
            assertTrue(dl.tryAcquire("d", "C", TTL, NOW.plusSeconds(121)).isAcquired());
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void theDefaultClockIsTheSystemClock(Morphium m) throws Exception {
        try (m) {
            Instant before = Instant.now();
            LeaseResult<Lease> r = leases(m).tryAcquire("job", "A", TTL);
            assertTrue(r.isAcquired());
            assertFalse(r.getLease().expiresAt.isBefore(before.plus(TTL).minusMillis(1)));
            assertFalse(r.getLease().expiresAt.isAfter(Instant.now().plus(TTL).plusMillis(1)));
            assertFalse(leases(m).tryAcquire("job", "B", TTL).isAcquired());
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void invalidDefinitionsAndArgumentsAreRejected(Morphium m) throws Exception {
        try (m) {
            assertThrows(IllegalArgumentException.class, () -> LeaseManager.of(m, Lease.class, "noSuchField", "expiresAt"));
            assertThrows(IllegalArgumentException.class, () -> LeaseManager.of(m, Lease.class, "ownerName", "noSuchField"));
            assertThrows(IllegalArgumentException.class, () -> LeaseManager.of(m, Lease.class, "ownerName", "retryCount"), "an int cannot hold a deadline");
            assertThrows(IllegalArgumentException.class, () -> LeaseManager.of(m, Lease.class, "retryCount", "expiresAt"), "the owner is a String");
            assertThrows(IllegalArgumentException.class, () -> LeaseManager.of(m, Lease.class, "ownerName", "ownerName"));
            assertThrows(IllegalArgumentException.class, () -> LeaseManager.of(null, Lease.class, "ownerName", "expiresAt"));
            LeaseManager<Lease> lm = leases(m);
            assertThrows(IllegalArgumentException.class, () -> lm.tryAcquire("job", null, TTL, NOW));
            assertThrows(IllegalArgumentException.class, () -> lm.tryAcquire(null, "A", TTL, NOW));
            assertThrows(IllegalArgumentException.class, () -> lm.tryAcquire("job", "A", Duration.ZERO, NOW));
            assertThrows(IllegalArgumentException.class, () -> lm.tryAcquire("job", "A", Duration.ofSeconds(-1), NOW));
            assertThrows(IllegalArgumentException.class, () -> lm.tryAcquire("job", "A", null, NOW));
            assertThrows(IllegalArgumentException.class, () -> lm.release("job", null));
            assertEquals(0, m.createQueryFor(Lease.class).countAll());
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void eightContendersOnAFreshLeaseHaveExactlyOneWinnerInEveryRound(Morphium m) throws Exception {
        try (m) {
            for (int round = 0; round < 5; round++) {
                String id = "race-" + round;
                List<LeaseResult<Lease>> results = race(m, id, NOW, 8);
                assertEquals(1, results.stream().filter(LeaseResult::isAcquired).count(), "round " + round);
                assertEquals(7, results.stream().filter(r -> r.getStatus() == Status.HELD).count());
                String winner = results.stream().filter(LeaseResult::isAcquired).findFirst().get().getLease().ownerName;
                assertEquals(winner, m.createQueryFor(Lease.class).f("id").eq(id).get().ownerName);
            }

            assertEquals(5, m.createQueryFor(Lease.class).countAll());
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void eightContendersOnAnExpiredLeaseHaveExactlyOneWinner(Morphium m) throws Exception {
        try (m) {
            leases(m).tryAcquire("job", "old", TTL, NOW);
            List<LeaseResult<Lease>> results = race(m, "job", NOW.plusSeconds(120), 8);
            assertEquals(1, results.stream().filter(LeaseResult::isAcquired).count());
            Lease stored = m.createQueryFor(Lease.class).f("id").eq("job").get();
            assertNotNull(stored);
            assertEquals(2L, stored.entityVersion, "one takeover, one version step");
            assertEquals(1, m.createQueryFor(Lease.class).countAll());
        }
    }

    private List<LeaseResult<Lease>> race(Morphium m, String id, Instant now, int contenders) throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(contenders);
        CountDownLatch go = new CountDownLatch(1);
        List<Future<LeaseResult<Lease>>> futures = new ArrayList<>();
        LeaseManager<Lease> lm = leases(m);

        for (int i = 0; i < contenders; i++) {
            String me = "C" + i;
            futures.add(pool.submit(() -> {
                go.await();
                return lm.tryAcquire(id, me, TTL, now);
            }));
        }

        go.countDown();
        List<LeaseResult<Lease>> results = new ArrayList<>();

        for (Future<LeaseResult<Lease>> f : futures) {
            results.add(f.get(60, TimeUnit.SECONDS));
        }

        pool.shutdown();
        return results;
    }
}
