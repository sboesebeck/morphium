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
import de.caluga.morphium.data.MorphiumRepository;
import io.quarkus.deployment.annotations.BuildProducer;
import io.quarkus.deployment.builditem.nativeimage.ReflectiveClassBuildItem;
import io.quarkus.gizmo.ClassCreator;
import io.quarkus.gizmo.ClassOutput;
import jakarta.data.Sort;
import jakarta.data.repository.Param;
import jakarta.data.repository.Query;
import jakarta.data.repository.Repository;
import org.jboss.jandex.ClassInfo;
import org.jboss.jandex.DotName;
import org.jboss.jandex.IndexView;
import org.jboss.jandex.Indexer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** MOR-017: the build accepts {@code @Query("DELETE FROM ...")} with void, int and long, and refuses what cannot work. */
@DisplayName("MorphiumDataProcessor — @Query DELETE FROM statements")
class MorphiumDataProcessorJdqlDeleteTest {

    @Entity
    public static class Fixture {
        @Id
        public String id;
        public String name;
    }

    @Repository
    public interface Accepted extends MorphiumRepository<Fixture, String> {
        @Query("DELETE FROM Fixture WHERE name = :n")
        long asLong(@Param("n") String n);

        @Query("DELETE FROM Fixture WHERE name = ?1")
        int asInt(String n);

        @Query("DELETE FROM Fixture")
        void everything();

        @Query("DELETE FROM WHERE name = :n")
        void withoutEntityName(@Param("n") String n);
    }

    @Repository
    public interface UpdateAccepted extends MorphiumRepository<Fixture, String> {
        @Query("UPDATE Fixture SET name = :n WHERE name = :old")
        long asLong(@Param("n") String n, @Param("old") String old);

        @Query("UPDATE Fixture SET name = ?1 WHERE name = ?2")
        int asInt(String n, String old);

        @Query("UPDATE Fixture SET name = NULL")
        void all();
    }

    @Repository
    public interface UpdateOtherField extends MorphiumRepository<Fixture, String> {
        @Query("UPDATE Fixture SET name = id + 1")
        long del();
    }

    @Repository
    public interface UpdateDivision extends MorphiumRepository<Fixture, String> {
        @Query("UPDATE Fixture SET name = name / 2")
        long del();
    }

    @Repository
    public interface UpdateDivisionByZero extends MorphiumRepository<Fixture, String> {
        @Query("UPDATE Fixture SET name = name / 0")
        long del();
    }

    @Repository
    public interface UpdateFunction extends MorphiumRepository<Fixture, String> {
        @Query("UPDATE Fixture SET name = UPPER(name)")
        void del();
    }

    @Repository
    public interface UpdateBooleanResult extends MorphiumRepository<Fixture, String> {
        @Query("UPDATE Fixture SET name = :n")
        boolean del(@Param("n") String n);
    }

    @Repository
    public interface UpdateOtherEntity extends MorphiumRepository<Fixture, String> {
        @Query("UPDATE Other SET name = :n")
        long del(@Param("n") String n);
    }

    @Repository
    public interface BooleanResult extends MorphiumRepository<Fixture, String> {
        @Query("DELETE FROM Fixture WHERE name = :n")
        boolean del(@Param("n") String n);
    }

    @Repository
    public interface WrapperResult extends MorphiumRepository<Fixture, String> {
        @Query("DELETE FROM Fixture WHERE name = :n")
        Long del(@Param("n") String n);
    }

    @Repository
    public interface OtherEntity extends MorphiumRepository<Fixture, String> {
        @Query("DELETE FROM Other WHERE name = :n")
        long del(@Param("n") String n);
    }

    @Repository
    public interface WithSort extends MorphiumRepository<Fixture, String> {
        @Query("DELETE FROM Fixture WHERE name = :n")
        long del(@Param("n") String n, Sort<Fixture> sort);
    }

    @Repository
    public interface WithOrderBy extends MorphiumRepository<Fixture, String> {
        @Query("DELETE FROM Fixture WHERE name = :n ORDER BY name")
        long del(@Param("n") String n);
    }

    @Repository
    public interface OrdinalWithoutParameter extends MorphiumRepository<Fixture, String> {
        @Query("DELETE FROM Fixture WHERE name = ?2")
        long del(String n);
    }

    @Repository
    public interface FindWithIs extends MorphiumRepository<Fixture, String> {
        @jakarta.data.repository.Find
        java.util.List<Fixture> cheap(@jakarta.data.repository.By("name") @jakarta.data.repository.Is(Object.class) String name);
    }

    @Repository
    public interface FindWithRestriction extends MorphiumRepository<Fixture, String> {
        @jakarta.data.repository.Find
        java.util.List<Fixture> matching(jakarta.data.restrict.Restriction<Fixture> restriction);
    }

    @Repository
    public interface DeleteWithIs extends MorphiumRepository<Fixture, String> {
        @jakarta.data.repository.Delete
        long drop(@jakarta.data.repository.By("name") @jakarta.data.repository.Is(Object.class) String name);
    }

    @Repository
    public interface QueryWithFirst extends MorphiumRepository<Fixture, String> {
        @jakarta.data.repository.First(2)
        @Query("WHERE name = :n")
        java.util.List<Fixture> top(@Param("n") String n);
    }

    @Repository
    public interface FindFine extends MorphiumRepository<Fixture, String> {
        @jakarta.data.repository.Find
        java.util.List<Fixture> byName(@jakarta.data.repository.By("name") String name);
    }

    private static class Collecting implements BuildProducer<ReflectiveClassBuildItem> {
        @Override
        public void produce(ReflectiveClassBuildItem item) {
        }
    }

    private static void generate(Class<?> repo) throws Exception {
        Indexer indexer = new Indexer();

        for (Class<?> c : new Class<?>[] {Fixture.class, repo, MorphiumRepository.class}) {
            try (InputStream in = c.getClassLoader().getResourceAsStream(c.getName().replace('.', '/') + ".class")) {
                indexer.index(in);
            }
        }

        IndexView index = indexer.complete();
        ClassInfo repoInfo = index.getClassByName(DotName.createSimple(repo.getName()));
        Method collect = MorphiumDataProcessor.class.getDeclaredMethod("collectEntityFields", ClassInfo.class, IndexView.class);
        collect.setAccessible(true);
        @SuppressWarnings("unchecked")
        Set<String> fields = (Set<String>) collect.invoke(new MorphiumDataProcessor(), index.getClassByName(DotName.createSimple(Fixture.class.getName())), index);
        Method m = MorphiumDataProcessor.class.getDeclaredMethod("generateCustomQueryMethods", ClassCreator.class, ClassInfo.class, IndexView.class, String.class, Set.class,
                BuildProducer.class);
        m.setAccessible(true);
        ClassOutput discard = (name, data) -> {
        };

        try (ClassCreator cc = ClassCreator.builder().classOutput(discard).className(repo.getName() + "_Impl")
                .superClass(de.caluga.morphium.data.AbstractMorphiumRepository.class.getName()).interfaces(repo.getName()).build()) {
            try {
                m.invoke(new MorphiumDataProcessor(), cc, repoInfo, index, Fixture.class.getName(), new HashSet<>(fields), new Collecting());
            } catch (InvocationTargetException e) {
                if (e.getCause() instanceof RuntimeException re) {
                    throw re;
                }

                throw e;
            }
        }
    }

    @Test
    @DisplayName("long, int, void, and a statement without an entity name are generated")
    void accepted() {
        assertThatCode(() -> generate(Accepted.class)).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("Long is refused: void, int, long or boolean only")
    void returnTypes() {
        assertThatCode(() -> generate(BooleanResult.class)).doesNotThrowAnyException();    // MOR-077
        assertThatThrownBy(() -> generate(WrapperResult.class)).isInstanceOf(IllegalStateException.class).hasMessageContaining("void, int, long");
    }

    @Test
    @DisplayName("another entity, a Sort parameter, ORDER BY and an ordinal without a parameter are refused")
    void otherMistakes() {
        assertThatThrownBy(() -> generate(OtherEntity.class)).isInstanceOf(IllegalStateException.class).hasMessageContaining("Other").hasMessageContaining("Fixture");
        assertThatThrownBy(() -> generate(WithSort.class)).isInstanceOf(IllegalStateException.class).hasMessageContaining("Sort");
        assertThatThrownBy(() -> generate(WithOrderBy.class)).isInstanceOf(IllegalStateException.class).hasMessageContaining("ORDER BY");
        assertThatThrownBy(() -> generate(OrdinalWithoutParameter.class)).isInstanceOf(IllegalStateException.class).hasMessageContaining("?2");
    }

    @Test
    @DisplayName("UPDATE: void, int and long are generated")
    void updateAccepted() {
        assertThatCode(() -> generate(UpdateAccepted.class)).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("UPDATE: an item that is not one update command fails the build and names the item")
    void updateUnsupportedExpressions() {
        assertThatThrownBy(() -> generate(UpdateOtherField.class)).isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("name = id + 1").hasMessageContaining("another field");
        assertThatCode(() -> generate(UpdateDivision.class)).doesNotThrowAnyException();    // a division is the multiplication by the inverse (MOR-077)
        assertThatThrownBy(() -> generate(UpdateDivisionByZero.class)).isInstanceOf(IllegalStateException.class).hasMessageContaining("divides by zero");
        assertThatThrownBy(() -> generate(UpdateFunction.class)).isInstanceOf(IllegalStateException.class).hasMessageContaining("UPPER(name)");
    }

    @Test
    @DisplayName("UPDATE: boolean is a result, another entity fails the build")
    void updateOtherMistakes() {
        assertThatCode(() -> generate(UpdateBooleanResult.class)).doesNotThrowAnyException();    // MOR-077
        assertThatThrownBy(() -> generate(UpdateOtherEntity.class)).isInstanceOf(IllegalStateException.class).hasMessageContaining("Other");
    }

    @Test
    @DisplayName("MOR-019: @Is, Restriction and @First fail the build and name the method")
    void unsupportedJakartaData11Features() {
        assertThatThrownBy(() -> generate(FindWithIs.class)).isInstanceOf(IllegalStateException.class).hasMessageContaining("FindWithIs.cheap").hasMessageContaining("@Is");
        assertThatThrownBy(() -> generate(FindWithRestriction.class)).isInstanceOf(IllegalStateException.class).hasMessageContaining("FindWithRestriction.matching")
            .hasMessageContaining("Restriction");
        assertThatThrownBy(() -> generate(DeleteWithIs.class)).isInstanceOf(IllegalStateException.class).hasMessageContaining("DeleteWithIs.drop");
        assertThatThrownBy(() -> generate(QueryWithFirst.class)).isInstanceOf(IllegalStateException.class).hasMessageContaining("QueryWithFirst.top").hasMessageContaining("@First");
        assertThatCode(() -> generate(FindFine.class)).doesNotThrowAnyException();
    }
}
