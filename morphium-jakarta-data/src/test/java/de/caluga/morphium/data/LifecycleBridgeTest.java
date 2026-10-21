package de.caluga.morphium.data;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import jakarta.data.exceptions.OptimisticLockingFailureException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * MOR-077: the lifecycle methods under any name and in every shape, on a repository for one entity that also stores another one.
 */
class LifecycleBridgeTest {

    @Entity
    static class Box {
        @Id
        String id;
        int length;

        Box() {}

        Box(String id, int length) {
            this.id = id;
            this.length = length;
        }
    }

    @Entity
    static class Coordinate {
        @Id
        String id;
        double x;

        Coordinate() {}

        Coordinate(String id, double x) {
            this.id = id;
            this.x = x;
        }
    }

    static class BoxRepo extends AbstractMorphiumRepository<Box, String> {
        BoxRepo(Morphium morphium) {
            super(new RepositoryMetadata(Box.class, String.class, "id"));
            setMorphium(morphium);
        }
    }

    private Morphium morphium;
    private BoxRepo repo;

    @BeforeEach
    void setUp() {
        MorphiumConfig cfg = new MorphiumConfig();
        cfg.setDatabase("mor077_" + System.nanoTime());
        cfg.addHostToSeed("localhost");
        cfg.setDriverName(InMemoryDriver.driverName);
        morphium = new Morphium(cfg);
        repo = new BoxRepo(morphium);
    }

    @AfterEach
    void tearDown() {
        morphium.close();
    }

    @Test
    @DisplayName("the shape of a return type")
    void shapes() {
        assertThat(LifecycleBridge.returnShape(void.class)).isEqualTo(LifecycleBridge.VOID);
        assertThat(LifecycleBridge.returnShape(Box.class)).isEqualTo(LifecycleBridge.ENTITY);
        assertThat(LifecycleBridge.returnShape(Box[].class)).isEqualTo(LifecycleBridge.ARRAY);
        assertThat(LifecycleBridge.returnShape(List.class)).isEqualTo(LifecycleBridge.LIST);
        assertThat(LifecycleBridge.returnShape(boolean.class)).isEqualTo(LifecycleBridge.BOOLEAN);
        assertThat(LifecycleBridge.returnShape(int.class)).isEqualTo(LifecycleBridge.INT);
        assertThat(LifecycleBridge.returnShape(Long.class)).isEqualTo(LifecycleBridge.LONG);
    }

    @Test
    @DisplayName("insert: one entity, a list, an array, and the entity of another repository")
    void insert() {
        assertThat(LifecycleBridge.execute(repo, "INSERT", new Box("a", 1), LifecycleBridge.ENTITY)).isInstanceOf(Box.class);
        assertThat(LifecycleBridge.execute(repo, "INSERT", List.of(new Box("b", 2), new Box("c", 3)), LifecycleBridge.LIST)).asList().hasSize(2);
        assertThat(LifecycleBridge.execute(repo, "INSERT", new Box[] {new Box("d", 4)}, LifecycleBridge.ARRAY)).asList().hasSize(1);
        assertThat(LifecycleBridge.execute(repo, "INSERT", new Coordinate("p", 1.5), LifecycleBridge.VOID)).isNull();

        assertThat(morphium.createQueryFor(Box.class).countAll()).isEqualTo(4);
        assertThat(morphium.findById(Coordinate.class, "p", null).x).isEqualTo(1.5);
    }

    @Test
    @DisplayName("save and update, with a boolean, an int and a long as result")
    void saveAndUpdate() {
        assertThat(LifecycleBridge.execute(repo, "SAVE", new Box("a", 1), LifecycleBridge.VOID)).isNull();
        assertThat(LifecycleBridge.execute(repo, "UPDATE", new Box("a", 5), LifecycleBridge.BOOLEAN)).isEqualTo(true);
        assertThat(LifecycleBridge.execute(repo, "UPDATE", List.of(new Box("a", 6)), LifecycleBridge.INT)).isEqualTo(1);
        assertThat(LifecycleBridge.execute(repo, "UPDATE", new Box[] {new Box("a", 7)}, LifecycleBridge.LONG)).isEqualTo(1L);
        assertThat(morphium.findById(Box.class, "a", null).length).isEqualTo(7);
    }

    @Test
    @DisplayName("update of an entity of another type checks that entity, not the one of the repository")
    void updateChecksTheTypeOfTheEntity() {
        assertThatThrownBy(() -> LifecycleBridge.execute(repo, "UPDATE", new Coordinate("missing", 1), LifecycleBridge.VOID)).isInstanceOf(OptimisticLockingFailureException.class);

        morphium.store(new Coordinate("here", 1));
        assertThat(LifecycleBridge.execute(repo, "UPDATE", new Coordinate("here", 2), LifecycleBridge.BOOLEAN)).isEqualTo(true);
    }

    @Test
    @DisplayName("delete: one entity, a list; the count is the number of entities")
    void delete() {
        morphium.store(new Box("a", 1));
        morphium.store(new Box("b", 2));
        morphium.store(new Box("c", 3));

        assertThat(LifecycleBridge.execute(repo, "DELETE", new Box("a", 1), LifecycleBridge.VOID)).isNull();
        assertThat(LifecycleBridge.execute(repo, "DELETE", List.of(new Box("b", 2), new Box("c", 3)), LifecycleBridge.LONG)).isEqualTo(2L);
        assertThat(morphium.createQueryFor(Box.class).countAll()).isZero();
    }

    @Test
    @DisplayName("several entities cannot come back as one entity; an unknown kind is refused")
    void refusals() {
        assertThatThrownBy(() -> LifecycleBridge.execute(repo, "INSERT", List.of(new Box("a", 1)), LifecycleBridge.ENTITY)).isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("several entities");
        assertThatThrownBy(() -> LifecycleBridge.execute(repo, "MERGE", new Box("a", 1), LifecycleBridge.VOID)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("forEntity gives the repository for another entity, once, and this repository for its own")
    void forEntity() {
        assertThat(repo.forEntity(Box.class)).isSameAs(repo);

        AbstractMorphiumRepository<?, ?> coordinates = repo.forEntity(Coordinate.class);
        assertThat(coordinates).isNotSameAs(repo);
        assertThat(coordinates.getMetadata().entityClass()).isEqualTo(Coordinate.class);
        assertThat(coordinates.getMetadata().idFieldName()).isEqualTo("id");
        assertThat(coordinates.getMetadata().idClass()).isEqualTo(String.class);
        assertThat(repo.forEntity(Coordinate.class)).isSameAs(coordinates);
        assertThat(coordinates.getMorphium()).isSameAs(morphium);
    }
}
