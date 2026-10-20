package de.caluga.morphium.data;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.annotations.Version;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import jakarta.data.exceptions.OptimisticLockingFailureException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * MOR-025: {@code @Update} reports a missing entity and a stale version with the exception of the specification,
 * {@link OptimisticLockingFailureException}; an entity with {@code @Version} is updated by the versioned store alone, without the
 * extra {@code findById}. Runs on the InMemoryDriver and, when the environment variable {@code MORPHIUM_TEST_MONGO=host:port} names a
 * standalone MongoDB, on that server with the pooled driver.
 */
class AbstractMorphiumRepositoryOptimisticLockingTest {

    @Entity
    static class Versioned {
        @Id
        private String id;
        private String name;
        @Version
        private Long version;

        Versioned() {}

        Versioned(String id, String name) {
            this.id = id;
            this.name = name;
        }
    }

    @Entity
    static class Plain {
        @Id
        private String id;
        private String name;

        Plain() {}

        Plain(String id, String name) {
            this.id = id;
            this.name = name;
        }
    }

    static class VersionedRepo extends AbstractMorphiumRepository<Versioned, String> {
        VersionedRepo(Morphium morphium) {
            super(new RepositoryMetadata(Versioned.class, String.class, "id"));
            setMorphium(morphium);
        }
    }

    static class PlainRepo extends AbstractMorphiumRepository<Plain, String> {
        PlainRepo(Morphium morphium) {
            super(new RepositoryMetadata(Plain.class, String.class, "id"));
            setMorphium(morphium);
        }
    }

    static Stream<String> drivers() {
        return System.getenv("MORPHIUM_TEST_MONGO") == null ? Stream.of("inmem") : Stream.of("inmem", "mongo");
    }

    private static Morphium morphium(String driver) {
        MorphiumConfig cfg = new MorphiumConfig();
        cfg.setDatabase("mor025_" + System.nanoTime());

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

    @ParameterizedTest
    @MethodSource("drivers")
    @DisplayName("a versioned entity is updated, and its version grows")
    void aVersionedEntityIsUpdated(String driver) throws Exception {
        try (Morphium m = morphium(driver)) {
            VersionedRepo repo = new VersionedRepo(m);
            Versioned v = new Versioned("v1", "one");
            m.store(v);
            long before = v.version;

            v.name = "two";
            assertThat(repo.doUpdate(v)).isSameAs(v);

            Versioned reloaded = m.findById(Versioned.class, "v1", null);
            assertThat(reloaded.name).isEqualTo("two");
            assertThat(reloaded.version).isEqualTo(before + 1);
        }
    }

    @ParameterizedTest
    @MethodSource("drivers")
    @DisplayName("a stale version is an OptimisticLockingFailureException and changes nothing")
    void aStaleVersionIsRejected(String driver) throws Exception {
        try (Morphium m = morphium(driver)) {
            VersionedRepo repo = new VersionedRepo(m);
            m.store(new Versioned("v1", "one"));
            Versioned mine = m.findById(Versioned.class, "v1", null);
            Versioned theirs = m.findById(Versioned.class, "v1", null);
            theirs.name = "theirs";
            repo.doUpdate(theirs);

            mine.name = "mine";
            assertThatThrownBy(() -> repo.doUpdate(mine)).isInstanceOf(OptimisticLockingFailureException.class).hasMessageContaining("v1");

            assertThat(m.findById(Versioned.class, "v1", null).name).isEqualTo("theirs");
        }
    }

    @ParameterizedTest
    @MethodSource("drivers")
    @DisplayName("a versioned entity that does not exist is an OptimisticLockingFailureException and is not created")
    void aMissingVersionedEntityIsRejected(String driver) throws Exception {
        try (Morphium m = morphium(driver)) {
            VersionedRepo repo = new VersionedRepo(m);
            Versioned v = new Versioned("v1", "one");
            m.store(v);
            m.delete(v);

            assertThatThrownBy(() -> repo.doUpdate(v)).isInstanceOf(OptimisticLockingFailureException.class);
            assertThat(m.findById(Versioned.class, "v1", null)).isNull();
        }
    }

    @ParameterizedTest
    @MethodSource("drivers")
    @DisplayName("an entity without a version that does not exist is an OptimisticLockingFailureException and is not created")
    void aMissingPlainEntityIsRejected(String driver) throws Exception {
        try (Morphium m = morphium(driver)) {
            PlainRepo repo = new PlainRepo(m);

            assertThatThrownBy(() -> repo.doUpdate(new Plain("ghost", "x"))).isInstanceOf(OptimisticLockingFailureException.class).hasMessageContaining("ghost");
            assertThat(m.findById(Plain.class, "ghost", null)).isNull();
        }
    }

    @ParameterizedTest
    @MethodSource("drivers")
    @DisplayName("updateAll with a missing entity rejects the batch, with a stale one it raises the same exception")
    void updateAllRaisesTheSameException(String driver) throws Exception {
        try (Morphium m = morphium(driver)) {
            PlainRepo plain = new PlainRepo(m);
            Plain p = new Plain("p1", "one");
            m.store(p);
            p.name = "must-not-be-applied";
            assertThatThrownBy(() -> plain.doUpdateAll(List.of(p, new Plain("missing", "x")))).isInstanceOf(OptimisticLockingFailureException.class);
            assertThat(m.findById(Plain.class, "p1", null).name).isEqualTo("one");

            VersionedRepo repo = new VersionedRepo(m);
            m.store(new Versioned("v1", "one"));
            Versioned mine = m.findById(Versioned.class, "v1", null);
            Versioned theirs = m.findById(Versioned.class, "v1", null);
            theirs.name = "theirs";
            repo.doUpdate(theirs);
            mine.name = "mine";
            assertThatThrownBy(() -> repo.doUpdateAll(List.of(mine))).isInstanceOf(OptimisticLockingFailureException.class);
        }
    }
}
