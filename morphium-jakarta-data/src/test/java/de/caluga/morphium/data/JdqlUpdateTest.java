package de.caluga.morphium.data;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.annotations.Version;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * MOR-018: {@code @Query("UPDATE Entity SET ... WHERE ...")}. {@link JdqlUpdate} splits the statement and refuses what cannot be ONE update command,
 * {@link JdqlMethodBridge#executeJdqlUpdate} runs it with the update builder and returns the number of matched documents. Runs on the InMemoryDriver and, with
 * {@code MORPHIUM_TEST_MONGO=host:port}, on a standalone MongoDB, where the test also counts the update commands that the server received.
 */
class JdqlUpdateTest {

    @Entity
    static class Counter {
        @Id
        private String id;
        private String category;
        private String label;
        private int hits;
        private double score;
        @Version
        private Long version;

        Counter() {}

        Counter(String id, String category, int hits) {
            this.id = id;
            this.category = category;
            this.hits = hits;
            this.label = "start";
            this.score = 2.0;
        }
    }

    static class CounterRepo extends AbstractMorphiumRepository<Counter, String> {
        CounterRepo(Morphium morphium) {
            super(new RepositoryMetadata(Counter.class, String.class, "id"));
            setMorphium(morphium);
        }
    }

    // ------------------------------------------------------------------ the statement

    @Test
    @DisplayName("UPDATE statements are recognised, queries and deletes are not")
    void recognised() {
        assertThat(JdqlUpdate.is("UPDATE Counter SET hits = 1")).isTrue();
        assertThat(JdqlUpdate.is("  update Counter set hits = 1")).isTrue();
        assertThat(JdqlUpdate.is("UPDATED")).isFalse();
        assertThat(JdqlUpdate.is("DELETE FROM Counter")).isFalse();
        assertThat(JdqlUpdate.is("WHERE hits = 1")).isFalse();
        assertThat(JdqlUpdate.is(null)).isFalse();
    }

    @Test
    @DisplayName("entity, items and WHERE clause are split, with the operator of every item")
    void split() {
        JdqlUpdate.Statement s = JdqlUpdate.parse("UPDATE Counter SET label = :l, hits = hits + 1, score = score * 1.5, x = NULL, flag = TRUE, n = n - :d WHERE id = :id");

        assertThat(s.entityName()).isEqualTo("Counter");
        assertThat(s.where()).isEqualTo("WHERE id = :id");
        assertThat(s.items()).extracting(JdqlUpdate.Item::field).containsExactly("label", "hits", "score", "x", "flag", "n");
        assertThat(s.items()).extracting(JdqlUpdate.Item::op).containsExactly(JdqlUpdate.Op.SET, JdqlUpdate.Op.INC, JdqlUpdate.Op.MUL, JdqlUpdate.Op.UNSET, JdqlUpdate.Op.SET,
                JdqlUpdate.Op.DEC);
        assertThat(s.items()).extracting(JdqlUpdate.Item::value).containsExactly(":l", "1", "1.5", null, "TRUE", ":d");
    }

    @Test
    @DisplayName("no WHERE, no entity name, a comma and a keyword inside a string literal")
    void formsAndLiterals() {
        JdqlUpdate.Statement s = JdqlUpdate.parse("UPDATE SET label = 'a, b WHERE c', hits = hits + 2");

        assertThat(s.entityName()).isNull();
        assertThat(s.where()).isEmpty();
        assertThat(s.items()).hasSize(2);
        assertThat(JdqlUpdate.literal(s.items().get(0).value())).isEqualTo("a, b WHERE c");
        assertThat(JdqlUpdate.literal("'it''s'")).isEqualTo("it's");
        assertThat(JdqlUpdate.literal("5")).isEqualTo(5);
        assertThat(JdqlUpdate.literal("5000000000")).isEqualTo(5000000000L);
        assertThat(JdqlUpdate.literal("-1.5")).isEqualTo(-1.5);
        assertThat(JdqlUpdate.literal("true")).isEqualTo(true);
    }

    @Test
    @DisplayName("ordinal parameters are accepted as values and operands")
    void ordinals() {
        JdqlUpdate.Statement s = JdqlUpdate.parse("UPDATE Counter SET label = ?1, hits = hits + ?2, score = score * ?3 WHERE id = ?4");

        assertThat(s.items()).extracting(JdqlUpdate.Item::op).containsExactly(JdqlUpdate.Op.SET, JdqlUpdate.Op.INC, JdqlUpdate.Op.MUL);
        assertThat(s.items()).extracting(JdqlUpdate.Item::value).containsExactly("?1", "?2", "?3");
    }

    @Test
    @DisplayName("MOR-077: a division by a number or a parameter is an item of its own")
    void division() {
        JdqlUpdate.Statement s = JdqlUpdate.parse("UPDATE Counter SET ratio = ratio / 2, share = share / :d, part = part / ?1");

        assertThat(s.items()).extracting(JdqlUpdate.Item::op).containsExactly(JdqlUpdate.Op.DIV, JdqlUpdate.Op.DIV, JdqlUpdate.Op.DIV);
        assertThat(s.items()).extracting(JdqlUpdate.Item::value).containsExactly("2", ":d", "?1");
    }

    @Test
    @DisplayName("what is not one update command is refused, and the message names the item")
    void refused() {
        assertThatThrownBy(() -> JdqlUpdate.parse("UPDATE Counter SET hits = other + 1")).isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("hits = other + 1").hasMessageContaining("another field");
        assertThatThrownBy(() -> JdqlUpdate.parse("UPDATE Counter SET hits = hits / 0")).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("divides by zero");
        assertThatThrownBy(() -> JdqlUpdate.parse("UPDATE Counter SET hits = hits / other")).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("parameter or a number");
        assertThatThrownBy(() -> JdqlUpdate.parse("UPDATE Counter SET label = label || 'x'")).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("not supported");
        assertThatThrownBy(() -> JdqlUpdate.parse("UPDATE Counter SET label = UPPER(label)")).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("not supported");
        assertThatThrownBy(() -> JdqlUpdate.parse("UPDATE Counter SET hits = hits + other")).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("parameter or a number");
        assertThatThrownBy(() -> JdqlUpdate.parse("UPDATE Counter SET hits = 1, hits = 2")).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("twice");
        assertThatThrownBy(() -> JdqlUpdate.parse("UPDATE Counter SET")).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("no SET items");
        assertThatThrownBy(() -> JdqlUpdate.parse("UPDATE Counter hits = 1")).isInstanceOf(IllegalArgumentException.class);
    }

    // ------------------------------------------------------------------ execution

    static Stream<String> drivers() {
        return System.getenv("MORPHIUM_TEST_MONGO") == null ? Stream.of("inmem") : Stream.of("inmem", "mongo");
    }

    private static Morphium morphium(String driver) {
        MorphiumConfig cfg = new MorphiumConfig();
        cfg.setDatabase("mor018_" + System.nanoTime());

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

    private static void seed(Morphium m) {
        m.store(new Counter("c1", "a", 10));
        m.store(new Counter("c2", "a", 20));
        m.store(new Counter("c3", "b", 30));
    }

    private static Counter load(Morphium m, String id) {
        return m.createQueryFor(Counter.class).f("id").eq(id).get();
    }

    /** the number of update commands that the server has received so far (its opcounters) */
    @SuppressWarnings("unchecked")
    private static long serverUpdateCommands(Morphium m) throws Exception {
        Map<String, Object> status = m.runCommand("serverStatus", null, Doc.of("serverStatus", 1)).get(0);
        return ((Number) ((Map<String, Object>) status.get("opcounters")).get("update")).longValue();
    }

    @Test
    @DisplayName("two fields, a literal and an increment are ONE update command on a real MongoDB")
    void oneCommand() throws Exception {
        if (System.getenv("MORPHIUM_TEST_MONGO") == null) {
            org.junit.jupiter.api.Assumptions.abort("needs MORPHIUM_TEST_MONGO=host:port");
        }

        try (Morphium m = morphium("mongo")) {
            seed(m);
            CounterRepo repo = new CounterRepo(m);
            long before = serverUpdateCommands(m);

            JdqlMethodBridge.executeJdqlUpdate(repo, "UPDATE Counter SET label = 'x', hits = hits + 1, score = score * 2, category = NULL WHERE id = :id", "id:0", new Object[] {"c1"});

            // opcounters.update counts every update statement: one command with four operators is one
            assertThat(serverUpdateCommands(m) - before).isEqualTo(1);
            Counter c = load(m, "c1");
            assertThat(c.label).isEqualTo("x");
            assertThat(c.hits).isEqualTo(11);
            assertThat(c.score).isEqualTo(4.0);
            assertThat(c.category).isNull();
        }
    }

    @ParameterizedTest
    @MethodSource("drivers")
    @DisplayName("SET a = :x, n = n + 1 WHERE ... changes the matching documents and returns the matched count")
    void updatesAndCounts(String driver) throws Exception {
        try (Morphium m = morphium(driver)) {
            seed(m);
            CounterRepo repo = new CounterRepo(m);
            long versionBefore = load(m, "c1").version;

            long matched = JdqlMethodBridge.executeJdqlUpdate(repo, "UPDATE Counter SET label = :l, hits = hits + 1 WHERE category = :c", "l:0,c:1", new Object[] {"changed", "a"});

            assertThat(matched).isEqualTo(2);
            assertThat(load(m, "c1").label).isEqualTo("changed");
            assertThat(load(m, "c1").hits).isEqualTo(11);
            assertThat(load(m, "c2").hits).isEqualTo(21);
            assertThat(load(m, "c3").hits).isEqualTo(30);
            assertThat(load(m, "c3").label).isEqualTo("start");
            assertThat(load(m, "c1").version).isEqualTo(versionBefore + 1);
        }
    }

    @ParameterizedTest
    @MethodSource("drivers")
    @DisplayName("minus, times, a literal and NULL")
    void operators(String driver) throws Exception {
        try (Morphium m = morphium(driver)) {
            seed(m);
            CounterRepo repo = new CounterRepo(m);

            long matched = JdqlMethodBridge.executeJdqlUpdate(repo, "UPDATE Counter SET hits = hits - ?2, score = score * 1.5, label = NULL WHERE id = ?1", "", new Object[] {"c2", 5});

            assertThat(matched).isEqualTo(1);
            Counter c = load(m, "c2");
            assertThat(c.hits).isEqualTo(15);
            assertThat(c.score).isEqualTo(3.0);
            assertThat(c.label).isNull();
            assertThat(load(m, "c1").label).isEqualTo("start");
        }
    }

    @ParameterizedTest
    @MethodSource("drivers")
    @DisplayName("without WHERE every document is updated; nothing matching is zero")
    void everythingAndNothing(String driver) throws Exception {
        try (Morphium m = morphium(driver)) {
            seed(m);
            CounterRepo repo = new CounterRepo(m);

            assertThat(JdqlMethodBridge.executeJdqlUpdate(repo, "UPDATE Counter SET hits = 0", "", new Object[0])).isEqualTo(3);
            assertThat(JdqlMethodBridge.executeJdqlUpdate(repo, "UPDATE Counter SET hits = 9 WHERE hits > ?1", "", new Object[] {100})).isZero();
            assertThat(load(m, "c3").hits).isZero();
        }
    }

    @ParameterizedTest
    @MethodSource("drivers")
    @DisplayName("the count is the matched one: a document that already has the value is counted")
    void matchedNotModified(String driver) throws Exception {
        try (Morphium m = morphium(driver)) {
            seed(m);
            CounterRepo repo = new CounterRepo(m);

            assertThat(JdqlMethodBridge.executeJdqlUpdate(repo, "UPDATE Counter SET label = 'start' WHERE category = 'a'", "", new Object[0])).isEqualTo(2);
        }
    }

    @ParameterizedTest
    @MethodSource("drivers")
    @DisplayName("a wrong entity name, a non-number for + and a missing argument are refused and change nothing")
    void rejected(String driver) throws Exception {
        try (Morphium m = morphium(driver)) {
            seed(m);
            CounterRepo repo = new CounterRepo(m);

            assertThatThrownBy(() -> JdqlMethodBridge.executeJdqlUpdate(repo, "UPDATE Other SET hits = 1", "", new Object[0])).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Other").hasMessageContaining("Counter");
            assertThatThrownBy(() -> JdqlMethodBridge.executeJdqlUpdate(repo, "UPDATE Counter SET hits = hits + :n", "n:0", new Object[] {"text"})).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("needs a number");
            assertThatThrownBy(() -> JdqlMethodBridge.executeJdqlUpdate(repo, "UPDATE Counter SET hits = :missing", "", new Object[0])).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(":missing");
            assertThat(List.of(load(m, "c1").hits, load(m, "c2").hits, load(m, "c3").hits)).containsExactly(10, 20, 30);
        }
    }
}
