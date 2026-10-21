package de.caluga.morphium.data;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import de.caluga.morphium.query.ReturnDocument;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Date;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * MOR-021: {@link AtomicUpdate} through {@link JdqlMethodBridge#executeAtomicCount} and {@link JdqlMethodBridge#executeAtomicDocument}. The lease is the example of the
 * task: one conditional upsert that acquires, renews and takes over, and loses against a holder. Runs on the InMemoryDriver and, with
 * {@code MORPHIUM_TEST_MONGO=host:port}, on a standalone MongoDB.
 */
class AtomicUpdateTest {

    @Entity
    static class Lease {
        @Id
        private String id;
        private String owner;
        private Date expiresAt;
        private long fencing;
        private String createdBy;
        private int priority;

        Lease() {}

        Lease(String id, String owner, int priority) {
            this.id = id;
            this.owner = owner;
            this.priority = priority;
            this.expiresAt = new Date(0);
        }
    }

    static class LeaseRepo extends AbstractMorphiumRepository<Lease, String> {
        LeaseRepo(Morphium morphium) {
            super(new RepositoryMetadata(Lease.class, String.class, "id"));
            setMorphium(morphium);
        }
    }

    // the method tryAcquire(id, owner, now, until): parameters 0 to 3
    private static final String PARAMS = "id:0,owner:1,now:2,until:3";

    private static AtomicUpdateSpec acquire(ReturnDocument returns) {
        return new AtomicUpdateSpec("id = :id AND (expiresAt < :now OR owner = :owner)", List.of("owner=:owner", "expiresAt=:until"), List.of("createdBy=:owner"),
                List.of("fencing=1"), true, false, returns, "", PARAMS);
    }

    static Stream<String> drivers() {
        return System.getenv("MORPHIUM_TEST_MONGO") == null ? Stream.of("inmem") : Stream.of("inmem", "mongo");
    }

    private static Morphium morphium(String driver) {
        MorphiumConfig cfg = new MorphiumConfig();
        cfg.setDatabase("mor021_" + System.nanoTime());

        if (driver.equals("mongo")) {
            String[] hp = System.getenv("MORPHIUM_TEST_MONGO").split(":");
            cfg.addHostToSeed(hp[0], Integer.parseInt(hp[1]));
            cfg.setDriverName("PooledDriver");
        } else {
            cfg.addHostToSeed("localhost");
            cfg.setDriverName(InMemoryDriver.driverName);
        }

        return new Morphium(cfg);
    }

    private static Lease tryAcquire(LeaseRepo repo, String id, String owner, long now, long until) {
        return (Lease) JdqlMethodBridge.executeAtomicDocument(repo, acquire(ReturnDocument.AFTER).encode(), new Object[] {id, owner, new Date(now), new Date(until)});
    }

    // ------------------------------------------------------------------ the spec

    @Test
    @DisplayName("the encoded spec survives the trip, an assignment needs exactly one of param and value")
    void spec() {
        AtomicUpdateSpec spec = acquire(ReturnDocument.BEFORE);

        assertThat(AtomicUpdateSpec.decode(spec.encode())).isEqualTo(spec);
        assertThat(AtomicUpdateSpec.assignment("a", "p", "")).isEqualTo("a=:p");
        assertThat(AtomicUpdateSpec.assignment("a", "", "'x'")).isEqualTo("a='x'");
        assertThatThrownBy(() -> AtomicUpdateSpec.assignment("a", "p", "1")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> AtomicUpdateSpec.assignment("a", "", "")).isInstanceOf(IllegalArgumentException.class);
    }

    // ------------------------------------------------------------------ the lease: find-and-modify form

    @ParameterizedTest
    @MethodSource("drivers")
    @DisplayName("one conditional upsert acquires, loses against the holder, renews and takes over")
    void leaseLifecycle(String driver) throws Exception {
        try (Morphium m = morphium(driver)) {
            LeaseRepo repo = new LeaseRepo(m);

            Lease first = tryAcquire(repo, "job", "A", 1000, 5000);
            assertThat(first).isNotNull();
            assertThat(first.owner).isEqualTo("A");
            assertThat(first.fencing).isEqualTo(1);
            assertThat(first.createdBy).isEqualTo("A");
            assertThat(first.expiresAt).isEqualTo(new Date(5000));

            // B tries while A holds: the filter excludes the document, the insert collides with it, the result is empty and nothing changed
            assertThat(tryAcquire(repo, "job", "B", 2000, 6000)).isNull();
            assertThat(m.findById(Lease.class, "job", null).owner).isEqualTo("A");

            // A renews, the fencing token grows, createdBy stays (it is only set on insert)
            Lease renewed = tryAcquire(repo, "job", "A", 3000, 8000);
            assertThat(renewed.fencing).isEqualTo(2);
            assertThat(renewed.expiresAt).isEqualTo(new Date(8000));

            // B takes over after the expiry
            Lease takenOver = tryAcquire(repo, "job", "B", 9000, 12000);
            assertThat(takenOver.owner).isEqualTo("B");
            assertThat(takenOver.fencing).isEqualTo(3);
            assertThat(takenOver.createdBy).isEqualTo("A");
            assertThat(m.createQueryFor(Lease.class).countAll()).isEqualTo(1);
        }
    }

    @ParameterizedTest
    @MethodSource("drivers")
    @DisplayName("BEFORE returns the old state, and null for an upsert that inserts")
    void before(String driver) throws Exception {
        try (Morphium m = morphium(driver)) {
            LeaseRepo repo = new LeaseRepo(m);
            String spec = acquire(ReturnDocument.BEFORE).encode();

            assertThat(JdqlMethodBridge.executeAtomicDocument(repo, spec, new Object[] {"job", "A", new Date(1000), new Date(5000)})).isNull();
            assertThat(m.findById(Lease.class, "job", null).owner).isEqualTo("A");

            Lease before = (Lease) JdqlMethodBridge.executeAtomicDocument(repo, spec, new Object[] {"job", "A", new Date(2000), new Date(6000)});
            assertThat(before.fencing).isEqualTo(1);
            assertThat(before.expiresAt).isEqualTo(new Date(5000));
        }
    }

    @ParameterizedTest
    @MethodSource("drivers")
    @DisplayName("without upsert nothing is created; an order picks the document")
    void noUpsertAndOrder(String driver) throws Exception {
        try (Morphium m = morphium(driver)) {
            LeaseRepo repo = new LeaseRepo(m);
            m.store(new Lease("a", "X", 1));
            m.store(new Lease("b", "X", 5));
            m.store(new Lease("c", "X", 3));

            AtomicUpdateSpec none = new AtomicUpdateSpec("id = :id", List.of("owner='new'"), List.of(), List.of(), false, false, ReturnDocument.AFTER, "", "id:0");
            assertThat(JdqlMethodBridge.executeAtomicDocument(repo, none.encode(), new Object[] {"missing"})).isNull();
            assertThat(m.findById(Lease.class, "missing", null)).isNull();

            AtomicUpdateSpec highest = new AtomicUpdateSpec("owner = 'X'", List.of("owner='taken'"), List.of(), List.of(), false, false, ReturnDocument.AFTER, "priority:DESC", "");
            Lease picked = (Lease) JdqlMethodBridge.executeAtomicDocument(repo, highest.encode(), new Object[0]);
            assertThat(picked.id).isEqualTo("b");
            assertThat(m.findById(Lease.class, "a", null).owner).isEqualTo("X");
        }
    }

    // ------------------------------------------------------------------ the count form

    @ParameterizedTest
    @MethodSource("drivers")
    @DisplayName("the count is matched or inserted, a lost upsert is 0, many updates all matches")
    void countForm(String driver) throws Exception {
        try (Morphium m = morphium(driver)) {
            LeaseRepo repo = new LeaseRepo(m);
            String spec = new AtomicUpdateSpec("id = :id AND (expiresAt < :now OR owner = :owner)", List.of("owner=:owner", "expiresAt=:until"), List.of(), List.of("fencing=1"), true,
                    false, ReturnDocument.AFTER, "", PARAMS).encode();

            assertThat(JdqlMethodBridge.executeAtomicCount(repo, spec, new Object[] {"job", "A", new Date(1000), new Date(5000)})).isEqualTo(1);    // inserted
            assertThat(JdqlMethodBridge.executeAtomicCount(repo, spec, new Object[] {"job", "B", new Date(2000), new Date(6000)})).isZero();       // lost
            assertThat(JdqlMethodBridge.executeAtomicCount(repo, spec, new Object[] {"job", "A", new Date(3000), new Date(8000)})).isEqualTo(1);    // renewed
            assertThat(m.findById(Lease.class, "job", null).fencing).isEqualTo(2);

            m.store(new Lease("a", "X", 1));
            m.store(new Lease("b", "X", 2));
            AtomicUpdateSpec all = new AtomicUpdateSpec("owner = 'X'", List.of(), List.of(), List.of("priority=10"), false, true, ReturnDocument.AFTER, "", "");
            assertThat(JdqlMethodBridge.executeAtomicCount(repo, all.encode(), new Object[0])).isEqualTo(2);
            assertThat(m.findById(Lease.class, "a", null).priority).isEqualTo(11);

            AtomicUpdateSpec one = new AtomicUpdateSpec("owner = 'X'", List.of(), List.of(), List.of("priority=1"), false, false, ReturnDocument.AFTER, "", "");
            assertThat(JdqlMethodBridge.executeAtomicCount(repo, one.encode(), new Object[0])).isEqualTo(1);
        }
    }

    @ParameterizedTest
    @MethodSource("drivers")
    @DisplayName("a missing argument and a non-number for inc are refused and change nothing")
    void rejected(String driver) throws Exception {
        try (Morphium m = morphium(driver)) {
            LeaseRepo repo = new LeaseRepo(m);
            m.store(new Lease("a", "X", 1));

            AtomicUpdateSpec missing = new AtomicUpdateSpec("id = :id", List.of("owner=:nobody"), List.of(), List.of(), false, false, ReturnDocument.AFTER, "", "id:0");
            assertThatThrownBy(() -> JdqlMethodBridge.executeAtomicCount(repo, missing.encode(), new Object[] {"a"})).isInstanceOf(IllegalArgumentException.class).hasMessageContaining(":nobody");

            AtomicUpdateSpec text = new AtomicUpdateSpec("id = :id", List.of(), List.of(), List.of("priority=:id"), false, false, ReturnDocument.AFTER, "", "id:0");
            assertThatThrownBy(() -> JdqlMethodBridge.executeAtomicCount(repo, text.encode(), new Object[] {"a"})).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("needs a number");
            assertThat(m.findById(Lease.class, "a", null).priority).isEqualTo(1);
        }
    }
}
