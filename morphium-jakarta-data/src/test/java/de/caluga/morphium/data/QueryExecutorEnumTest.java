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
        @de.caluga.morphium.annotations.Enumerated(de.caluga.morphium.annotations.EnumType.ORDINAL)
        private Kind ranked;
        private int amount;

        Num() {}

        Num(long id, Kind kind, int amount) {
            this.id = id;
            this.kind = kind;
            this.ranked = kind;
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

    private void seed() {
        morphium.clearCollection(Num.class);
        morphium.store(new Num(1, Kind.ONE, 1));
        morphium.store(new Num(2, Kind.PRIME, 2));
        morphium.store(new Num(3, Kind.PRIME, 3));
        morphium.store(new Num(4, Kind.COMPOSITE, 4));
        morphium.store(new Num(5, Kind.PRIME, 5));
        morphium.store(new Num(6, Kind.COMPOSITE, 6));
        morphium.store(new Num(7, Kind.PRIME, 7));
    }

    @BeforeEach
    void setUp() {
        MorphiumConfig cfg = new MorphiumConfig();
        cfg.setDatabase("mor090_" + System.nanoTime());
        cfg.addHostToSeed("localhost");
        cfg.setDriverName(InMemoryDriver.driverName);
        morphium = new Morphium(cfg);
        repo = new NumRepo(morphium);
        seed();
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

    @SuppressWarnings("unchecked")
    private List<Long> jdql(String jdql, Object... args) {
        StringBuilder spec = new StringBuilder();

        for (int i = 0; i < args.length; i++) {
            spec.append(i == 0 ? "" : ",").append("p").append(i + 1).append(':').append(i);
        }

        Object result = JdqlMethodBridge.executeJdql(repo, jdql, spec.toString(), -1, -1, -1, -1, args, false, false, false, false, false, "", false, null);
        return ((List<Num>) result).stream().map(n -> n.id).sorted().toList();
    }

    private void assertAllForms(String field) {
        String cap = Character.toUpperCase(field.charAt(0)) + field.substring(1);

        assertThat(ids("findBy" + cap, Kind.PRIME)).containsExactly(2L, 3L, 5L, 7L);
        assertThat(ids("findBy" + cap + "Not", Kind.PRIME)).containsExactly(1L, 4L, 6L);
        assertThat(ids("findBy" + cap + "In", Set.of(Kind.COMPOSITE, Kind.ONE))).containsExactly(1L, 4L, 6L);
        assertThat(ids("findBy" + cap + "NotIn", Set.of(Kind.COMPOSITE, Kind.ONE))).containsExactly(2L, 3L, 5L, 7L);
        assertThat(jdql("WHERE " + field + " = com.example.Kind.PRIME")).containsExactly(2L, 3L, 5L, 7L);
        assertThat(jdql("WHERE " + field + " = 'COMPOSITE'")).containsExactly(4L, 6L);
        assertThat(jdql("WHERE " + field + " IN ('ONE', 'COMPOSITE')")).containsExactly(1L, 4L, 6L);
        assertThat(jdql("WHERE " + field + " <> ?1", Kind.PRIME)).containsExactly(1L, 4L, 6L);
        assertThat(jdql("WHERE " + field + " IN ?1", List.of(Kind.PRIME, Kind.ONE))).containsExactly(1L, 2L, 3L, 5L, 7L);
    }

    @Test
    @DisplayName("MOR-093: derived queries and JDQL find enums by name and by ordinal, per field and by the setting")
    void storedByNameAndByOrdinal() {
        assertAllForms("kind");
        assertAllForms("ranked");

        morphium.getConfig().objectMappingSettings().setEnumType(de.caluga.morphium.annotations.EnumType.ORDINAL);
        seed();

        assertAllForms("kind");
        assertAllForms("ranked");
        assertThat(morphium.getMapper().serialize(morphium.createQueryFor(Num.class).f("id").eq(2L).get()).get("kind")).isEqualTo(Kind.PRIME.ordinal());
    }

    @Test
    @DisplayName("MOR-093: UPDATE ... SET enum = 'NAME' stores the enum as the field says")
    void updateStoresTheFieldForm() {
        morphium.getConfig().objectMappingSettings().setEnumType(de.caluga.morphium.annotations.EnumType.ORDINAL);
        seed();

        JdqlMethodBridge.executeJdqlUpdate(repo, "UPDATE Num SET kind = 'COMPOSITE', ranked = :r WHERE id = 1", "r:0", new Object[] {Kind.PRIME});

        Num one = morphium.findById(Num.class, 1L, null);
        assertThat(one.kind).isEqualTo(Kind.COMPOSITE);
        assertThat(one.ranked).isEqualTo(Kind.PRIME);
        assertThat(morphium.createQueryFor(Num.class).f("kind").eq(Kind.COMPOSITE).countAll()).isEqualTo(3);
    }
}
