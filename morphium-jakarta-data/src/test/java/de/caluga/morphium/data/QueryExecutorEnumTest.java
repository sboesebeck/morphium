package de.caluga.morphium.data;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * MOR-090: a derived query with an enum parameter: {@code findByKind}, {@code findByKindNot}, {@code findByKindIn(Set)}, {@code findByKindNotIn} and {@code findByKindAndAmountLessThan}.
 */
class QueryExecutorEnumTest {

    enum Kind { PRIME, COMPOSITE, ONE }

    @Entity
    static class Num {
        @Id
        private long id;
        private Kind kind;
        private int amount;

        Num() {}

        Num(long id, Kind kind, int amount) {
            this.id = id;
            this.kind = kind;
            this.amount = amount;
        }
    }

    static class NumRepo extends AbstractMorphiumRepository<Num, Long> {
        NumRepo(Morphium morphium) {
            super(new RepositoryMetadata(Num.class, Long.class, "id"));
            setMorphium(morphium);
        }
    }

    private Morphium morphium;
    private NumRepo repo;

    @BeforeEach
    void setUp() {
        MorphiumConfig cfg = new MorphiumConfig();
        cfg.setDatabase("mor090_" + System.nanoTime());
        cfg.addHostToSeed("localhost");
        cfg.setDriverName(InMemoryDriver.driverName);
        morphium = new Morphium(cfg);
        repo = new NumRepo(morphium);
        morphium.store(new Num(1, Kind.ONE, 1));
        morphium.store(new Num(2, Kind.PRIME, 2));
        morphium.store(new Num(3, Kind.PRIME, 3));
        morphium.store(new Num(4, Kind.COMPOSITE, 4));
        morphium.store(new Num(5, Kind.PRIME, 5));
        morphium.store(new Num(6, Kind.COMPOSITE, 6));
        morphium.store(new Num(7, Kind.PRIME, 7));
    }

    @AfterEach
    void tearDown() {
        morphium.close();
    }

    @SuppressWarnings("unchecked")
    private List<Long> ids(String method, Object... args) {
        Object result = QueryMethodBridge.executeQuery(repo, method, args, false, false, false, false);
        return ((List<Num>) result).stream().map(n -> n.id).sorted().toList();
    }

    @Test
    @DisplayName("equality, Not, In and NotIn with enum values")
    void enumOperators() {
        assertThat(ids("findByKind", Kind.PRIME)).containsExactly(2L, 3L, 5L, 7L);
        assertThat(ids("findByKindNot", Kind.PRIME)).containsExactly(1L, 4L, 6L);
        assertThat(ids("findByKindIn", Set.of(Kind.COMPOSITE, Kind.ONE))).containsExactly(1L, 4L, 6L);
        assertThat(ids("findByKindNotIn", Set.of(Kind.COMPOSITE, Kind.ONE))).containsExactly(2L, 3L, 5L, 7L);
        assertThat(ids("findByKindAndAmountLessThan", Kind.PRIME, 5)).containsExactly(2L, 3L);
    }
}
