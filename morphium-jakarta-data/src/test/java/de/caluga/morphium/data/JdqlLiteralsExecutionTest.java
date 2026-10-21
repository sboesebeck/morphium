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

import static org.assertj.core.api.Assertions.assertThat;

/**
 * MOR-082: the queries that the Jakarta Data TCK runs against the parser of the previous test, executed. Runs on the InMemoryDriver.
 */
class JdqlLiteralsExecutionTest {

    enum Kind { PRIME, COMPOSITE, ONE }

    @Entity
    static class Num {
        @Id
        private long id;
        private boolean odd;
        private Kind kind;
        private String hex;
        private String note;

        Num() {}

        Num(long id, boolean odd, Kind kind, String hex, String note) {
            this.id = id;
            this.odd = odd;
            this.kind = kind;
            this.hex = hex;
            this.note = note;
        }

        long id() { return id; }
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
        cfg.setDatabase("mor082_" + System.nanoTime());
        cfg.addHostToSeed("localhost");
        cfg.setDriverName(InMemoryDriver.driverName);
        morphium = new Morphium(cfg);
        repo = new NumRepo(morphium);
        morphium.store(new Num(1, true, Kind.ONE, "31", "plain"));
        morphium.store(new Num(2, false, Kind.PRIME, "32", "it's"));
        morphium.store(new Num(3, true, Kind.PRIME, "33", "a=b"));
        morphium.store(new Num(4, false, Kind.COMPOSITE, "34", " ORDER BY isn't a keyword"));
        morphium.store(new Num(5, true, Kind.PRIME, "4a", null));
        morphium.store(new Num(6, false, Kind.COMPOSITE, "4b", "x"));
    }

    @AfterEach
    void tearDown() {
        morphium.close();
    }

    @SuppressWarnings("unchecked")
    private List<Long> ids(String jdql, Object... args) {
        StringBuilder spec = new StringBuilder();

        for (int i = 0; i < args.length; i++) {
            spec.append(i == 0 ? "" : ",").append("p").append(i + 1).append(':').append(i);
        }

        Object result = JdqlMethodBridge.executeJdql(repo, jdql, spec.toString(), -1, -1, -1, -1, args, false, false, false, false, false, "", false, null);
        return ((List<Num>) result).stream().map(Num::id).sorted().toList();
    }

    @Test
    @DisplayName("AND binds stronger than OR")
    void precedence() {
        // odd AND kind = PRIME OR id = 6: (3, 5) and 6; evaluated left to right it would be 3, 5, 6 as well, so a second shape tells them apart
        assertThat(ids("WHERE id = 6 OR odd = true AND kind = ee.example.Kind.PRIME")).containsExactly(3L, 5L, 6L);
        assertThat(ids("WHERE id = 1 OR id = 2 AND odd = true")).containsExactly(1L);
        assertThat(ids("WHERE id = 1 AND odd = false OR id = 2 AND odd = false")).containsExactly(2L);
        assertThat(ids("WHERE (id = 1 OR id = 2) AND odd = true")).containsExactly(1L);
    }

    @Test
    @DisplayName("an IN list of literals, with a parameter inside, and NOT IN")
    void inList() {
        assertThat(ids("WHERE hex IN ('4a', '4b', '99')")).containsExactly(5L, 6L);
        assertThat(ids("WHERE hex IN ('4a', ?1)", "33")).containsExactly(3L, 5L);
        assertThat(ids("WHERE id NOT IN (1, 2, 3)")).containsExactly(4L, 5L, 6L);
        assertThat(ids("where hex not in ('31', '32', '33', '34')")).containsExactly(5L, 6L);
    }

    @Test
    @DisplayName("a collection parameter inside an IN list contributes its elements")
    void collectionParameterInList() {
        assertThat(ids("WHERE hex IN (:p1, '4a')", List.of("31", "32"))).containsExactly(1L, 2L, 5L);
    }

    @Test
    @DisplayName("a dotted name is the constant of an enum only for an enum field: for another field it stays the text")
    void dottedNameOnlyForEnums() {
        morphium.store(new Num(7, true, Kind.ONE, "77", "foo.bar"));
        morphium.store(new Num(8, true, Kind.ONE, "78", "bar"));
        assertThat(ids("WHERE note = foo.bar")).containsExactly(7L);
        assertThat(ids("WHERE kind IN (a.b.Kind.PRIME, a.b.Kind.ONE) AND id >= 7")).containsExactly(7L, 8L);
    }

    @Test
    @DisplayName("a quote inside a literal, and keywords and operators inside a literal are text")
    void literalsAreText() {
        assertThat(ids("WHERE note = 'it''s'")).containsExactly(2L);
        assertThat(ids("WHERE note = 'a=b'")).containsExactly(3L);
        assertThat(ids("WHERE note <> ' ORDER BY isn''t a keyword' AND hex IN ('31', '34')")).containsExactly(1L);
        assertThat(ids("WHERE note = ' ORDER BY isn''t a keyword' ORDER BY id")).containsExactly(4L);
    }

    @Test
    @DisplayName("NOT LIKE, NOT BETWEEN and the constant of an enum")
    void notAndEnum() {
        assertThat(ids("WHERE hex LIKE '3_' AND hex NOT LIKE '%1'")).containsExactly(2L, 3L, 4L);
        assertThat(ids("WHERE id NOT BETWEEN 2 AND 5")).containsExactly(1L, 6L);
        assertThat(ids("WHERE odd = false AND kind = ee.example.Kind.PRIME")).containsExactly(2L);
        assertThat(ids("WHERE kind = com.example.Kind.COMPOSITE")).containsExactly(4L, 6L);
    }

    @Test
    @DisplayName("COUNT(THIS) with literals, in any case")
    void countWithLiterals() {
        Object upper = JdqlMethodBridge.executeJdql(repo, "SELECT COUNT(THIS) WHERE id <= 4 AND id >= 2", "", -1, -1, -1, -1, new Object[0], true, true, false, false, false, "", false, null);
        Object lower = JdqlMethodBridge.executeJdql(repo, "SELECT COUNT(this) WHERE id <= 4 AND id >= 2", "", -1, -1, -1, -1, new Object[0], true, true, false, false, false, "", false, null);

        assertThat(((Number) upper).longValue()).isEqualTo(3L);
        assertThat(((Number) lower).longValue()).isEqualTo(3L);
    }

    @Test
    @DisplayName("ORDER BY and HAVING with COUNT(THIS) in upper case find the aggregate")
    @SuppressWarnings("unchecked")
    void orderAndHavingWithUpperCaseThis() {
        Object result = JdqlMethodBridge.executeJdql(repo, "SELECT kind, COUNT(THIS) GROUP BY kind HAVING COUNT(THIS) > 1 ORDER BY COUNT(THIS) DESC", "", -1, -1, -1, -1, new Object[0], false, false,
                false, false, false, "", false, KindCount.class.getName());

        assertThat((List<Object>) result).containsExactly(new KindCount("PRIME", 3), new KindCount("COMPOSITE", 2));
    }

    record KindCount(String kind, long count) {}

    @Test
    @DisplayName("several OR groups and NOT BETWEEN in one AND chain narrow each other (they are not merged into one OR)")
    void severalOrsAreAnded() {
        assertThat(ids("WHERE (id = 1 OR id = 2) AND (hex = '33' OR hex = '34')")).isEmpty();
        assertThat(ids("WHERE (id = 1 OR id = 2) AND (hex = '32' OR hex = '34')")).containsExactly(2L);
        assertThat(ids("WHERE id NOT BETWEEN 2 AND 5 AND (id = 1 OR id = 3)")).containsExactly(1L);
        assertThat(ids("WHERE (id = 1 OR id = 3) AND id NOT BETWEEN 2 AND 5 AND (id = 1 OR id = 6)")).containsExactly(1L);
    }

    @Test
    @DisplayName("numeric literals with the suffix of Java and with an exponent: 2L, 4.5d, 3f, 1e1")
    void numericLiterals() {
        assertThat(ids("WHERE id >= 2L AND id < 4.5d")).containsExactly(2L, 3L, 4L);
        assertThat(ids("WHERE id <= 3f AND id > 0.0d")).containsExactly(1L, 2L, 3L);
        assertThat(ids("WHERE id < 1e1 AND id > 4E0")).containsExactly(5L, 6L);
        assertThat(ids("WHERE id BETWEEN 2L AND 3.0f")).containsExactly(2L, 3L);
        assertThat(ids("WHERE id IN (1L, 2d, 3)")).containsExactly(1L, 2L, 3L);
    }
}
