package de.caluga.morphium.data;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * MOR-024: a JDQL query with {@code ?1} style parameters runs through {@link JdqlMethodBridge} like the one with named parameters. Runs on the InMemoryDriver and,
 * with {@code MORPHIUM_TEST_MONGO=host:port}, on a standalone MongoDB with the pooled driver.
 */
class JdqlOrdinalParametersQueryTest {

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

        public String getId() { return id; }
    }

    static class ItemRepo extends AbstractMorphiumRepository<Item, String> {
        ItemRepo(Morphium morphium) {
            super(new RepositoryMetadata(Item.class, String.class, "id"));
            setMorphium(morphium);
        }
    }

    static Stream<String> drivers() {
        return System.getenv("MORPHIUM_TEST_MONGO") == null ? Stream.of("inmem") : Stream.of("inmem", "mongo");
    }

    private static Morphium morphium(String driver) {
        MorphiumConfig cfg = new MorphiumConfig();
        cfg.setDatabase("mor024_" + System.nanoTime());

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

    @SuppressWarnings("unchecked")
    private static List<Item> run(ItemRepo repo, String jdql, Object... args) {
        return (List<Item>) JdqlMethodBridge.executeJdql(repo, jdql, "", -1, -1, -1, -1, args, false, false, false, false, false, "", false, null);
    }

    private static void seed(Morphium m) {
        m.store(new Item("i1", "tools", 10));
        m.store(new Item("i2", "tools", 30));
        m.store(new Item("i3", "toys", 30));
    }

    @ParameterizedTest
    @MethodSource("drivers")
    @DisplayName("?1 and ?2 are bound to the first and second argument")
    void ordinalsAreBound(String driver) throws Exception {
        try (Morphium m = morphium(driver)) {
            seed(m);
            ItemRepo repo = new ItemRepo(m);

            assertThat(run(repo, "WHERE price >= ?1 AND category = ?2", 20, "tools")).extracting(Item::getId).containsExactly("i2");
            // the same ordinal twice, in a different order than the arguments
            assertThat(run(repo, "WHERE category = ?2 AND price >= ?1 AND price <= ?1", 30, "toys")).extracting(Item::getId).containsExactly("i3");
        }
    }

    @ParameterizedTest
    @MethodSource("drivers")
    @DisplayName("BETWEEN, IN and LIKE take ordinals too")
    void otherOperators(String driver) throws Exception {
        try (Morphium m = morphium(driver)) {
            seed(m);
            ItemRepo repo = new ItemRepo(m);

            assertThat(run(repo, "WHERE price BETWEEN ?1 AND ?2", 5, 15)).extracting(Item::getId).containsExactly("i1");
            assertThat(run(repo, "WHERE category IN ?1", List.of("toys", "games"))).extracting(Item::getId).containsExactly("i3");
            assertThat(run(repo, "WHERE category LIKE ?1", "to%")).hasSize(3);
        }
    }

    @ParameterizedTest
    @MethodSource("drivers")
    @DisplayName("special parameters do not count: ?1 is the first query parameter")
    void specialParametersAreSkipped(String driver) throws Exception {
        try (Morphium m = morphium(driver)) {
            seed(m);
            ItemRepo repo = new ItemRepo(m);
            // method (Limit limit, String category): the Limit sits at index 0 and is not ?1
            @SuppressWarnings("unchecked")
            List<Item> found = (List<Item>) JdqlMethodBridge.executeJdql(repo, "WHERE category = ?1", "", -1, -1, -1, 0,
                new Object[] {jakarta.data.Limit.of(10), "tools"}, false, false, false, false, false, "", false, null);
            assertThat(found).hasSize(2);
        }
    }

    @ParameterizedTest
    @MethodSource("drivers")
    @DisplayName("an ordinal without an argument, and named mixed with ordinal, are rejected")
    void rejected(String driver) throws Exception {
        try (Morphium m = morphium(driver)) {
            ItemRepo repo = new ItemRepo(m);
            assertThatThrownBy(() -> run(repo, "WHERE price >= ?2", 20)).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("?2");
            assertThatThrownBy(() -> run(repo, "WHERE price >= ?1 AND category = :c", 20)).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("mix");
        }
    }
}
