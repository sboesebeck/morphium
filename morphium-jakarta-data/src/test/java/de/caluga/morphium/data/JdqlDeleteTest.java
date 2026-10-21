package de.caluga.morphium.data;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * MOR-017: {@code @Query("DELETE FROM Entity WHERE ...")}. The statement is split by {@link JdqlDelete} and executed by {@link JdqlMethodBridge#executeJdqlDelete} as one
 * server-side delete that returns the number of removed documents. Runs on the InMemoryDriver and, with {@code MORPHIUM_TEST_MONGO=host:port}, on a standalone MongoDB.
 */
class JdqlDeleteTest {

    @Entity
    static class Item {
        @Id
        private String id;
        private String category;
        private int price;

        Item() {}

        Item(String id, String category, int price) {
            this.id = id;
            this.category = category;
            this.price = price;
        }
    }

    static class ItemRepo extends AbstractMorphiumRepository<Item, String> {
        ItemRepo(Morphium morphium) {
            super(new RepositoryMetadata(Item.class, String.class, "id"));
            setMorphium(morphium);
        }
    }

    // ------------------------------------------------------------------ the statement

    @Test
    @DisplayName("DELETE statements are recognised, queries are not")
    void recognised() {
        assertThat(JdqlDelete.is("DELETE FROM Item WHERE price > 1")).isTrue();
        assertThat(JdqlDelete.is("  delete from Item")).isTrue();
        assertThat(JdqlDelete.is("DELETEX")).isFalse();
        assertThat(JdqlDelete.is("WHERE price > 1")).isFalse();
        assertThat(JdqlDelete.is("SELECT price FROM Item")).isFalse();
        assertThat(JdqlDelete.is(null)).isFalse();
    }

    @Test
    @DisplayName("entity name and WHERE clause are split")
    void split() {
        assertThat(JdqlDelete.entityName("DELETE FROM Item WHERE price > :p")).isEqualTo("Item");
        assertThat(JdqlDelete.whereClause("DELETE FROM Item WHERE price > :p")).isEqualTo("WHERE price > :p");
        assertThat(JdqlDelete.entityName("DELETE FROM Item")).isEqualTo("Item");
        assertThat(JdqlDelete.whereClause("DELETE FROM Item")).isEmpty();
        assertThat(JdqlDelete.entityName("delete from Item where a = 1")).isEqualTo("Item");
        assertThat(JdqlDelete.entityName("DELETE FROM WHERE a = 1")).isNull();
        assertThat(JdqlDelete.whereClause("DELETE FROM WHERE a = 1")).isEqualTo("WHERE a = 1");
        assertThat(JdqlDelete.entityName("DELETE FROM")).isNull();
    }

    @Test
    @DisplayName("a statement that is not DELETE FROM is rejected")
    void malformed() {
        assertThatThrownBy(() -> JdqlDelete.whereClause("DELETE Item WHERE a = 1")).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("DELETE FROM");
        assertThatThrownBy(() -> JdqlDelete.whereClause("WHERE a = 1")).isInstanceOf(IllegalArgumentException.class);
    }

    // ------------------------------------------------------------------ execution

    static Stream<String> drivers() {
        return System.getenv("MORPHIUM_TEST_MONGO") == null ? Stream.of("inmem") : Stream.of("inmem", "mongo");
    }

    private static Morphium morphium(String driver) {
        MorphiumConfig cfg = new MorphiumConfig();
        cfg.setDatabase("mor017_" + System.nanoTime());

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
        m.store(new Item("i1", "tools", 10));
        m.store(new Item("i2", "tools", 30));
        m.store(new Item("i3", "toys", 30));
    }

    private static List<String> ids(Morphium m) {
        return m.createQueryFor(Item.class).sort("id").asList().stream().map(i -> i.id).toList();
    }

    @ParameterizedTest
    @MethodSource("drivers")
    @DisplayName("DELETE FROM Item WHERE ... removes the matching documents and returns their number")
    void deletesAndCounts(String driver) throws Exception {
        try (Morphium m = morphium(driver)) {
            seed(m);
            ItemRepo repo = new ItemRepo(m);

            long n = JdqlMethodBridge.executeJdqlDelete(repo, "DELETE FROM Item WHERE category = :c AND price >= :p", "c:0,p:1", new Object[] {"tools", 20});

            assertThat(n).isEqualTo(1);
            assertThat(ids(m)).containsExactly("i1", "i3");
        }
    }

    @ParameterizedTest
    @MethodSource("drivers")
    @DisplayName("without WHERE every document goes, and ordinal parameters work")
    void everythingAndOrdinals(String driver) throws Exception {
        try (Morphium m = morphium(driver)) {
            seed(m);
            ItemRepo repo = new ItemRepo(m);

            assertThat(JdqlMethodBridge.executeJdqlDelete(repo, "DELETE FROM Item WHERE price = ?1", "", new Object[] {30})).isEqualTo(2);
            assertThat(ids(m)).containsExactly("i1");
            assertThat(JdqlMethodBridge.executeJdqlDelete(repo, "DELETE FROM Item", "", new Object[0])).isEqualTo(1);
            assertThat(ids(m)).isEmpty();
        }
    }

    @ParameterizedTest
    @MethodSource("drivers")
    @DisplayName("nothing matches: zero, nothing removed")
    void nothingMatches(String driver) throws Exception {
        try (Morphium m = morphium(driver)) {
            seed(m);
            assertThat(JdqlMethodBridge.executeJdqlDelete(new ItemRepo(m), "DELETE FROM Item WHERE price > :p", "p:0", new Object[] {1000})).isZero();
            assertThat(ids(m)).hasSize(3);
        }
    }

    @ParameterizedTest
    @MethodSource("drivers")
    @DisplayName("a wrong entity name and clauses that a DELETE does not have are rejected and delete nothing")
    void rejected(String driver) throws Exception {
        try (Morphium m = morphium(driver)) {
            seed(m);
            ItemRepo repo = new ItemRepo(m);

            assertThatThrownBy(() -> JdqlMethodBridge.executeJdqlDelete(repo, "DELETE FROM Other WHERE price > 1", "", new Object[0]))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("Other").hasMessageContaining("Item");
            assertThatThrownBy(() -> JdqlMethodBridge.executeJdqlDelete(repo, "DELETE FROM Item WHERE price > 1 ORDER BY price", "", new Object[0]))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("ORDER BY");
            assertThat(ids(m)).hasSize(3);
        }
    }
}
