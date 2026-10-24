package de.caluga.morphium.data;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.annotations.Version;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * MOR-022: derived update methods ({@code updateByStatus(String status, int priority)}): the spec that the name stands for, and the update through
 * {@link JdqlMethodBridge#executeAtomicCount} and {@link JdqlMethodBridge#executeAtomicDocument}. Runs on the InMemoryDriver and, with {@code MORPHIUM_TEST_MONGO=host:port}, on a
 * standalone MongoDB.
 */
class DerivedUpdateTest {

    @Entity
    static class Job {
        @Id
        private String id;
        private String status;
        private String owner;
        private int priority;
        private Instant seenAt;
        private boolean archived;
        @Version
        private Long version;

        Job() {}

        Job(String id, String status, String owner, int priority) {
            this.id = id;
            this.status = status;
            this.owner = owner;
            this.priority = priority;
        }
    }

    static class JobRepo extends AbstractMorphiumRepository<Job, String> {
        JobRepo(Morphium morphium) {
            super(new RepositoryMetadata(Job.class, String.class, "id"));
            setMorphium(morphium);
        }
    }

    private static final Set<String> FIELDS = Set.of("id", "status", "owner", "priority", "seenAt", "archived", "version");

    private static Morphium morphium;
    private JobRepo repo;

    @BeforeAll
    static void setUp() {
        MorphiumConfig cfg = new MorphiumConfig();
        cfg.setDatabase("derived_update_test");

        if (System.getenv("MORPHIUM_TEST_MONGO") != null) {
            String[] hp = System.getenv("MORPHIUM_TEST_MONGO").split(":");
            cfg.addHostToSeed(hp[0], Integer.parseInt(hp[1]));
            cfg.setDriverName("PooledDriver");
        } else {
            cfg.addHostToSeed("localhost");
            cfg.setDriverName(InMemoryDriver.driverName);
        }

        morphium = new Morphium(cfg);
    }

    @AfterAll
    static void tearDown() {
        morphium.close();
    }

    private void seed() {
        morphium.dropCollection(Job.class);
        repo = new JobRepo(morphium);
        morphium.store(new Job("a", "OPEN", "x", 1));
        morphium.store(new Job("b", "OPEN", "y", 2));
        morphium.store(new Job("c", "DONE", "x", 3));
    }

    private Job load(String id) {
        return morphium.createQueryFor(Job.class).f("id").eq(id).get();
    }

    @Test
    @DisplayName("the name is the condition, the parameters after it are the values: one $set, all matching documents")
    void aDerivedUpdateSetsTheParametersAfterTheConditions() {
        seed();
        AtomicUpdateSpec spec = DerivedUpdate.derive("updateByStatus", "status:0,priority:1,seenAt:2", true, FIELDS);
        assertThat(spec.where()).isEqualTo("status = :status");
        assertThat(spec.set()).containsExactly("priority=:priority", "seenAt=:seenAt");
        assertThat(spec.many()).isTrue();

        Instant now = Instant.parse("2026-10-24T10:00:00Z");
        long n = JdqlMethodBridge.executeAtomicCount(repo, spec.encode(), new Object[] {"OPEN", 9, now});

        assertThat(n).isEqualTo(2);
        assertThat(load("a").priority).isEqualTo(9);
        assertThat(load("b").priority).isEqualTo(9);
        assertThat(load("a").seenAt).isEqualTo(now);
        assertThat(load("c").priority).as("a document that does not match is untouched").isEqualTo(3);
        assertThat(load("a").version).as("the @Version is incremented, like for the builder").isEqualTo(2L);
        assertThat(load("a").owner).as("a field that is not a parameter is kept").isEqualTo("x");
    }

    @Test
    @DisplayName("several conditions, operators and the find-and-modify form")
    void conditionsAndTheDocumentForm() {
        seed();
        AtomicUpdateSpec spec = DerivedUpdate.derive("updateByStatusAndPriorityGreaterThan", "status:0,minPriority:1,owner:2", false, FIELDS);
        assertThat(spec.where()).isEqualTo("status = :status AND priority > :minPriority");
        assertThat(spec.many()).isFalse();

        Object job = JdqlMethodBridge.executeAtomicDocument(repo, spec.encode(), new Object[] {"OPEN", 1, "z"});
        assertThat(job).isInstanceOf(Job.class);
        assertThat(((Job) job).owner).isEqualTo("z");
        assertThat(((Job) job).id).isEqualTo("b");
        assertThat(load("a").owner).isEqualTo("x");

        AtomicUpdateSpec between = DerivedUpdate.derive("updateByPriorityBetween", "lo:0,hi:1,archived:2", true, FIELDS);
        assertThat(between.where()).isEqualTo("priority BETWEEN :lo AND :hi");
        assertThat(JdqlMethodBridge.executeAtomicCount(repo, between.encode(), new Object[] {1, 2, true})).isEqualTo(2);
        assertThat(load("a").archived).isTrue();

        AtomicUpdateSpec or = DerivedUpdate.derive("updateByStatusOrOwner", "status:0,owner:1,archived:2", true, FIELDS);
        assertThat(or.where()).isEqualTo("status = :status OR owner = :owner");
        assertThat(DerivedUpdate.derive("updateByOwnerIsNull", "archived:0", true, FIELDS).where()).isEqualTo("owner IS NULL");
        assertThat(DerivedUpdate.derive("updateByStatusIn", "statuses:0,archived:1", true, FIELDS).where()).isEqualTo("status IN :statuses");
    }

    @Test
    @DisplayName("what cannot be derived is refused with a message that names the method")
    void wrongMethodsAreRefused() {
        assertThatThrownBy(() -> DerivedUpdate.derive("updateByStatus", "status:0", true, FIELDS)).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("updateByStatus").hasMessageContaining("no parameter is left to set");
        assertThatThrownBy(() -> DerivedUpdate.derive("updateByStatusStartsWith", "prefix:0,priority:1", true, FIELDS)).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("STARTS_WITH").hasMessageContaining("@AtomicUpdate");
        assertThatThrownBy(() -> DerivedUpdate.derive("updateByStatusIgnoreCase", "status:0,priority:1", true, FIELDS)).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("IgnoreCase");
        assertThatThrownBy(() -> DerivedUpdate.derive("updateByUnknownField", "x:0,priority:1", true, FIELDS)).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("updateByUnknownField");
        assertThatThrownBy(() -> DerivedUpdate.derive("updateByStatus", "status:0,priority:1,priority:2", true, FIELDS).validate("m", AtomicUpdateSpec.Form.COUNT, Set.of("status", "priority"), FIELDS))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("priority");
        assertThatThrownBy(() -> DerivedUpdate.derive("updateByStatus", "arg0:0,arg1:1", true, FIELDS)).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("-parameters").hasMessageContaining("@Param");
        assertThatThrownBy(() -> DerivedUpdate.derive("updateByStatus", "status:0,colour:1", true, FIELDS).validate("m", AtomicUpdateSpec.Form.COUNT, Set.of("status", "colour"), FIELDS))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("colour").hasMessageContaining("the entity does not have");
    }

    @Test
    @DisplayName("only updateBy... is a derived update; the plain update is the lifecycle method")
    void theNameIsRecognised() {
        assertThat(DerivedUpdate.isDerivedUpdateName("updateByStatus")).isTrue();
        assertThat(DerivedUpdate.isDerivedUpdateName("update")).isFalse();
        assertThat(DerivedUpdate.isDerivedUpdateName("updateAll")).isFalse();
        assertThat(DerivedUpdate.isDerivedUpdateName("findByStatus")).isFalse();
    }
}
