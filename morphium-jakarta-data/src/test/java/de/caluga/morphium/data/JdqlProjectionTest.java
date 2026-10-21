package de.caluga.morphium.data;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import jakarta.data.page.Page;
import jakarta.data.page.PageRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * MOR-083: {@code SELECT field} answers the values of the field when the method returns them ({@code List<String>}, {@code Optional<Long>}, {@code Stream<Character>},
 * {@code Page<Integer>}, {@code String}), not partly filled entities. The bridge learns the element type from the class name that the caller passes.
 */
class JdqlProjectionTest {

    enum Kind { PRIME, COMPOSITE }

    @Entity
    static class Item {
        @Id
        private long id;
        private String hex;
        private char letter;
        private Kind kind;

        Item() {}

        Item(long id, String hex, char letter, Kind kind) {
            this.id = id;
            this.hex = hex;
            this.letter = letter;
            this.kind = kind;
        }
    }

    static class ItemRepo extends AbstractMorphiumRepository<Item, Long> {
        ItemRepo(Morphium morphium) {
            super(new RepositoryMetadata(Item.class, Long.class, "id"));
            setMorphium(morphium);
        }
    }

    private Morphium morphium;
    private ItemRepo repo;

    @BeforeEach
    void setUp() {
        MorphiumConfig cfg = new MorphiumConfig();
        cfg.setDatabase("mor083_" + System.nanoTime());
        cfg.addHostToSeed("localhost");
        cfg.setDriverName(InMemoryDriver.driverName);
        morphium = new Morphium(cfg);
        repo = new ItemRepo(morphium);
        morphium.store(new Item(1, "31", 'a', Kind.COMPOSITE));
        morphium.store(new Item(2, "32", 'b', Kind.PRIME));
        morphium.store(new Item(3, "33", 'c', Kind.PRIME));
    }

    @AfterEach
    void tearDown() {
        morphium.close();
    }

    @SuppressWarnings("unchecked")
    private static List<Object> values(Object result) {
        return (List<Object>) result;
    }

    private Object run(String jdql, boolean single, boolean optional, boolean stream, Object[] args, int pageIdx, String resultClass) {
        return JdqlMethodBridge.executeJdql(repo, jdql, "", -1, -1, pageIdx, -1, args, single, false, false, optional, false, "", stream, resultClass);
    }

    @Test
    @DisplayName("a list of values: String, Long converted from the field, Character, enum")
    void listOfValues() {
        assertThat(values(run("SELECT hex WHERE id < 3 ORDER BY id", false, false, false, new Object[0], -1, "java.lang.String"))).containsExactly("31", "32");
        assertThat(values(run("SELECT id ORDER BY id DESC", false, false, false, new Object[0], -1, "java.lang.Long"))).containsExactly(3L, 2L, 1L);
        assertThat(values(run("SELECT id ORDER BY id", false, false, false, new Object[0], -1, "java.lang.Integer"))).containsExactly(1, 2, 3);
        assertThat(values(run("SELECT letter ORDER BY id", false, false, false, new Object[0], -1, "java.lang.Character"))).containsExactly('a', 'b', 'c');
        assertThat(values(run("SELECT kind WHERE id >= 2 ORDER BY id", false, false, false, new Object[0], -1, Kind.class.getName()))).containsExactly(Kind.PRIME, Kind.PRIME);
    }

    @Test
    @DisplayName("a single value, an Optional and a Stream")
    void otherShapes() {
        assertThat(run("SELECT hex WHERE id = 2", true, false, false, new Object[0], -1, "java.lang.String")).isEqualTo("32");
        assertThat(run("SELECT hex WHERE id = 9", false, true, false, new Object[0], -1, "java.lang.String")).isEqualTo(Optional.empty());
        assertThat(run("SELECT hex WHERE id = 3", false, true, false, new Object[0], -1, "java.lang.String")).isEqualTo(Optional.of("33"));
        @SuppressWarnings("unchecked")
        Stream<Object> values = (Stream<Object>) run("SELECT hex ORDER BY id", false, false, true, new Object[0], -1, "java.lang.String");
        assertThat(values.toList()).containsExactly("31", "32", "33");
    }

    @Test
    @DisplayName("a Page of values keeps its totals and its request")
    void page() {
        PageRequest request = PageRequest.ofPage(2).size(2).withTotal();
        @SuppressWarnings("unchecked")
        Page<Object> page = (Page<Object>) run("SELECT hex ORDER BY id", false, false, false, new Object[] {request}, 0, "java.lang.String");

        assertThat(page.content()).containsExactly("33");
        assertThat(page.totalElements()).isEqualTo(3);
        assertThat(page.pageRequest()).isEqualTo(request);
    }

    @Test
    @DisplayName("without a result class, or with the entity class, the entities come back as before")
    void entitiesStay() {
        assertThat(values(run("SELECT hex ORDER BY id", false, false, false, new Object[0], -1, null)).get(0)).isInstanceOf(Item.class);
        assertThat(values(run("SELECT hex ORDER BY id", false, false, false, new Object[0], -1, Item.class.getName())).get(0)).isInstanceOf(Item.class);
        assertThat(values(run("WHERE id = 1", false, false, false, new Object[0], -1, "java.lang.String")).get(0)).isInstanceOf(Item.class);
    }
}
