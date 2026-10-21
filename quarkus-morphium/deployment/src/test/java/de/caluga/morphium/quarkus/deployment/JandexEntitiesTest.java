/*
 * Copyright 2025 The Quarkiverse Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package de.caluga.morphium.quarkus.deployment;

import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import jakarta.data.repository.Delete;
import jakarta.data.repository.Insert;
import jakarta.data.repository.Param;
import jakarta.data.repository.Query;
import jakarta.data.repository.Repository;
import jakarta.data.repository.Save;
import jakarta.data.repository.Update;
import org.jboss.jandex.ClassInfo;
import org.jboss.jandex.DotName;
import org.jboss.jandex.IndexView;
import org.jboss.jandex.Indexer;
import org.jboss.jandex.MethodInfo;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** MOR-077: the entity of a repository without a primary entity and of each of its methods, at build time. The same cases as {@code RepositoryEntitiesTest} of the runtime. */
@DisplayName("JandexEntities")
class JandexEntitiesTest {

    @Entity
    public static class Box {
        @Id
        public String id;
        public int length;
    }

    @Entity
    public static class Coordinate {
        @Id
        public String id;
        public double x;
    }

    @Repository
    public interface OnlyBoxes {
        @Insert
        Box[] addAll(Box... boxes);

        @Delete
        void remove(List<Box> boxes);

        @Delete
        long deleteByLength(int length);

        long countByLength(int length);
    }

    @Repository
    public interface Two {
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

        @Query("SELECT length FROM Product WHERE length > 1")
        List<Integer> selectFromAnotherName();

        @Query("DELETE FROM Nobody")
        long wrong();
    }

    private static IndexView index(Class<?>... classes) throws Exception {
        Indexer indexer = new Indexer();

        for (Class<?> c : classes) {
            try (InputStream in = c.getClassLoader().getResourceAsStream(c.getName().replace('.', '/') + ".class")) {
                indexer.index(in);
            }
        }

        return indexer.complete();
    }

    private static MethodInfo method(IndexView index, Class<?> repository, String name) {
        ClassInfo info = index.getClassByName(DotName.createSimple(repository.getName()));
        return info.methods().stream().filter(m -> m.name().equals(name)).findFirst().orElseThrow();
    }

    @Test
    @DisplayName("the primary entity is the entity of the lifecycle methods if they agree, none if they do not")
    void primary() throws Exception {
        IndexView index = index(Box.class, Coordinate.class, OnlyBoxes.class, Two.class);

        assertThat(JandexEntities.primaryFromLifecycle(index.getClassByName(DotName.createSimple(OnlyBoxes.class.getName())).methods(), index)).isEqualTo(DotName.createSimple(Box.class.getName()));
        assertThat(JandexEntities.primaryFromLifecycle(index.getClassByName(DotName.createSimple(Two.class.getName())).methods(), index)).isNull();
    }

    @Test
    @DisplayName("a lifecycle method gets its entity from the parameter; a @Delete by condition is none")
    void lifecycle() throws Exception {
        IndexView index = index(Box.class, Coordinate.class, OnlyBoxes.class, Two.class);

        assertThat(JandexEntities.lifecycleEntity(method(index, OnlyBoxes.class, "addAll"), index)).isEqualTo(DotName.createSimple(Box.class.getName()));
        assertThat(JandexEntities.lifecycleEntity(method(index, OnlyBoxes.class, "remove"), index)).isEqualTo(DotName.createSimple(Box.class.getName()));
        assertThat(JandexEntities.lifecycleEntity(method(index, Two.class, "save"), index)).isEqualTo(DotName.createSimple(Coordinate.class.getName()));
        assertThat(JandexEntities.lifecycleEntity(method(index, OnlyBoxes.class, "deleteByLength"), index)).isNull();
        assertThat(JandexEntities.lifecycleEntity(method(index, OnlyBoxes.class, "countByLength"), index)).isNull();
        assertThat(JandexEntities.lifecycleKind(method(index, OnlyBoxes.class, "remove"))).isEqualTo("DELETE");
        assertThat(JandexEntities.lifecycleKind(method(index, Two.class, "update"))).isEqualTo("UPDATE");
    }

    @Test
    @DisplayName("every method of a repository with several entities gets its own: from the statement, the parameter or the result")
    void perMethod() throws Exception {
        IndexView index = index(Box.class, Coordinate.class, OnlyBoxes.class, Two.class);
        List<MethodInfo> methods = index.getClassByName(DotName.createSimple(Two.class.getName())).methods();
        Map<String, String> known = JandexEntities.known(methods, null, index);
        String box = Box.class.getName();
        String coordinate = Coordinate.class.getName();

        assertThat(known.values()).containsExactlyInAnyOrder(box, coordinate);
        assertThat(JandexEntities.forMethod(method(index, Two.class, "addAll"), null, known, index)).isEqualTo(box);
        assertThat(JandexEntities.forMethod(method(index, Two.class, "save"), null, known, index)).isEqualTo(coordinate);
        assertThat(JandexEntities.forMethod(method(index, Two.class, "removeAll"), null, known, index)).isEqualTo(box);
        assertThat(JandexEntities.forMethod(method(index, Two.class, "move"), null, known, index)).isEqualTo(coordinate);
        assertThat(JandexEntities.forMethod(method(index, Two.class, "withId"), null, known, index)).isEqualTo(coordinate);
        assertThat(JandexEntities.forMethod(method(index, Two.class, "selectFromAnotherName"), box, known, index)).isEqualTo(box);
    }

    @Test
    @DisplayName("a DELETE that names an entity that is none of the repository fails and names the method")
    void unknownEntity() throws Exception {
        IndexView index = index(Box.class, Coordinate.class, OnlyBoxes.class, Two.class);
        Map<String, String> known = JandexEntities.known(index.getClassByName(DotName.createSimple(Two.class.getName())).methods(), null, index);

        assertThatThrownBy(() -> JandexEntities.forMethod(method(index, Two.class, "wrong"), null, known, index)).isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("Two.wrong").hasMessageContaining("Nobody");
    }
}
