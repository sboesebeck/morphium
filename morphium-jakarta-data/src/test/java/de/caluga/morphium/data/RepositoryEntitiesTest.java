package de.caluga.morphium.data;

import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import jakarta.data.repository.BasicRepository;
import jakarta.data.repository.By;
import jakarta.data.repository.Delete;
import jakarta.data.repository.Find;
import jakarta.data.repository.Insert;
import jakarta.data.repository.Param;
import jakarta.data.repository.Query;
import jakarta.data.repository.Save;
import jakarta.data.repository.Update;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * MOR-077: the entity of a repository without a primary entity and the entity of each method of a repository with several.
 */
class RepositoryEntitiesTest {

    @Entity
    static class Box {
        @Id
        String id;
        int length;
    }

    @Entity
    static class Coordinate {
        @Id
        String id;
        double x;
    }

    static class NoEntity {
    }

    interface Generic extends BasicRepository<Box, String> {
        @Find
        Stream<Coordinate> coordinates(@By("x") double x);
    }

    interface OnlyBoxes {
        @Insert
        Box[] addAll(Box... boxes);

        @Insert
        void add(List<Box> boxes);

        @Delete
        void remove(List<Box> boxes);

        @Delete
        long deleteByLength(@By("length") int length);

        long countByLength(int length);
    }

    interface Two {
        @Insert
        Box[] addAll(Box... boxes);

        @Save
        Coordinate save(Coordinate coordinate);

        @Update
        boolean update(Box box);

        @Query("DELETE FROM Box")
        long removeAll();

        @Query("UPDATE Coordinate SET x = :x WHERE id = :id")
        boolean move(@Param("id") String id, @Param("x") double x);

        @Query("WHERE id = ?1")
        Optional<Coordinate> withId(String id);

        @Query("SELECT length FROM Box WHERE length > 1")
        List<Integer> lengths();

        @Query("DELETE FROM Nobody")
        long wrong();

        @Query("DELETE FROM WHERE id = ?1")
        long unnamed(String id);
    }

    interface Nothing {
        @Insert
        void add(NoEntity thing);
    }

    private static Method method(Class<?> repository, String name) throws Exception {
        for (Method m : repository.getMethods()) {
            if (m.getName().equals(name)) {
                return m;
            }
        }

        throw new NoSuchMethodException(name);
    }

    @Test
    @DisplayName("the primary entity: the type argument, else the entity of the lifecycle methods if they agree, else none")
    void primary() {
        assertThat(RepositoryEntities.primary(Generic.class)).isEqualTo(Box.class);
        assertThat(RepositoryEntities.primary(OnlyBoxes.class)).isEqualTo(Box.class);
        assertThat(RepositoryEntities.primary(Two.class)).isNull();
        assertThat(RepositoryEntities.primary(Nothing.class)).isNull();
    }

    @Test
    @DisplayName("a lifecycle method is @Insert, @Save, @Update, or a @Delete that takes entities; a @Delete by condition is none")
    void lifecycle() throws Exception {
        assertThat(RepositoryEntities.lifecycleEntity(method(OnlyBoxes.class, "addAll"))).isEqualTo(Box.class);
        assertThat(RepositoryEntities.lifecycleEntity(method(OnlyBoxes.class, "add"))).isEqualTo(Box.class);
        assertThat(RepositoryEntities.lifecycleEntity(method(OnlyBoxes.class, "remove"))).isEqualTo(Box.class);
        assertThat(RepositoryEntities.lifecycleEntity(method(Two.class, "save"))).isEqualTo(Coordinate.class);
        assertThat(RepositoryEntities.isLifecycle(method(OnlyBoxes.class, "deleteByLength"))).isFalse();
        assertThat(RepositoryEntities.isLifecycle(method(OnlyBoxes.class, "countByLength"))).isFalse();
        assertThat(RepositoryEntities.isLifecycle(method(Nothing.class, "add"))).isFalse();
    }

    @Test
    @DisplayName("every method of a repository with several entities gets its own: from the statement, the parameter or the result")
    void perMethod() throws Exception {
        Map<String, Class<?>> known = RepositoryEntities.known(Two.class);

        assertThat(known).containsOnlyKeys("box", "coordinate");
        assertThat(RepositoryEntities.forMethod(method(Two.class, "addAll"), null, known)).isEqualTo(Box.class);
        assertThat(RepositoryEntities.forMethod(method(Two.class, "save"), null, known)).isEqualTo(Coordinate.class);
        assertThat(RepositoryEntities.forMethod(method(Two.class, "removeAll"), null, known)).isEqualTo(Box.class);
        assertThat(RepositoryEntities.forMethod(method(Two.class, "move"), null, known)).isEqualTo(Coordinate.class);
        assertThat(RepositoryEntities.forMethod(method(Two.class, "withId"), null, known)).isEqualTo(Coordinate.class);
        assertThat(RepositoryEntities.forMethod(method(Two.class, "lengths"), null, known)).isEqualTo(Box.class);
    }

    @Test
    @DisplayName("a statement without an entity name and a result that is no entity gets the primary entity, or none")
    void fallbacks() throws Exception {
        Map<String, Class<?>> known = RepositoryEntities.known(Two.class);

        assertThat(RepositoryEntities.forMethod(method(Two.class, "unnamed"), Box.class, known)).isEqualTo(Box.class);
        assertThat(RepositoryEntities.forMethod(method(Two.class, "unnamed"), null, known)).isNull();
        assertThat(RepositoryEntities.forMethod(method(OnlyBoxes.class, "countByLength"), Box.class, RepositoryEntities.known(OnlyBoxes.class))).isEqualTo(Box.class);
        assertThat(RepositoryEntities.forMethod(method(Generic.class, "coordinates"), Box.class, RepositoryEntities.known(Generic.class))).isEqualTo(Coordinate.class);
    }

    @Test
    @DisplayName("a statement that names an entity that is none of the repository fails and names the method")
    void unknownEntity() throws Exception {
        assertThatThrownBy(() -> RepositoryEntities.forMethod(method(Two.class, "wrong"), null, RepositoryEntities.known(Two.class))).isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("wrong").hasMessageContaining("Nobody");
    }

    @Test
    @DisplayName("the entity of a statement: DELETE, UPDATE and SELECT ... FROM, in any case, none for a statement without one")
    void jdqlEntityNames() {
        assertThat(JdqlEntityName.of("DELETE FROM Box WHERE length > 1")).isEqualTo("Box");
        assertThat(JdqlEntityName.of("delete from Box")).isEqualTo("Box");
        assertThat(JdqlEntityName.of("DELETE FROM WHERE id = ?1")).isNull();
        assertThat(JdqlEntityName.of("UPDATE Coordinate SET x = 1")).isEqualTo("Coordinate");
        assertThat(JdqlEntityName.of("UPDATE Coordinate SET x = 1 WHERE id = :id")).isEqualTo("Coordinate");
        assertThat(JdqlEntityName.of("UPDATE SET x = 1")).isNull();
        assertThat(JdqlEntityName.of("SELECT length FROM Box WHERE length > 1")).isEqualTo("Box");
        assertThat(JdqlEntityName.of("FROM Box WHERE length > 1")).isEqualTo("Box");
        assertThat(JdqlEntityName.of("WHERE id = ?1")).isNull();
        assertThat(JdqlEntityName.of("WHERE name = ' FROM Nobody '")).isNull();
        assertThat(JdqlEntityName.of("SELECT COUNT(THIS) WHERE length > 1")).isNull();
        assertThat(JdqlEntityName.of(null)).isNull();
    }
}
