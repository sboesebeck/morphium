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
import de.caluga.morphium.data.Assign;
import de.caluga.morphium.data.AtomicUpdate;
import de.caluga.morphium.data.MorphiumRepository;
import io.quarkus.deployment.annotations.BuildProducer;
import io.quarkus.deployment.builditem.nativeimage.ReflectiveClassBuildItem;
import io.quarkus.gizmo.ClassCreator;
import io.quarkus.gizmo.ClassOutput;
import jakarta.data.Sort;
import jakarta.data.repository.OrderBy;
import jakarta.data.repository.Param;
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
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** MOR-021: the build generates {@code @AtomicUpdate} methods in both forms and refuses what cannot work, naming the method. */
@DisplayName("MorphiumDataProcessor — @AtomicUpdate")
class MorphiumDataProcessorAtomicUpdateTest {

    @Entity
    public static class Lease {
        @Id
        public String id;
        public String owner;
        public long fencing;
    }

    @Repository
    public interface Accepted extends MorphiumRepository<Lease, String> {
        @AtomicUpdate(where = "id = :id AND (fencing < :max OR owner = :owner)", set = @Assign(field = "owner", param = "owner"), setOnInsert = @Assign(field = "owner", value = "'x'"),
                      inc = @Assign(field = "fencing", value = "1"), upsert = true)
        Optional<Lease> acquire(@Param("id") String id, @Param("owner") String owner, @Param("max") long max);

        @AtomicUpdate(where = "WHERE id = :id", inc = @Assign(field = "fencing", param = "by"))
        boolean bump(@Param("id") String id, @Param("by") long by);

        @AtomicUpdate(set = @Assign(field = "owner", value = "'none'"), many = true)
        int clear();

        @AtomicUpdate(set = @Assign(field = "owner", value = "'none'"))
        long one();

        @AtomicUpdate(set = @Assign(field = "owner", value = "'none'"))
        void quiet();

        @AtomicUpdate(set = @Assign(field = "owner", value = "'x'"))
        Lease entity();

        @OrderBy("fencing")
        @AtomicUpdate(set = @Assign(field = "owner", value = "'x'"))
        Optional<Lease> first();
    }

    @Repository
    public interface Collection extends MorphiumRepository<Lease, String> {
        @AtomicUpdate(set = @Assign(field = "owner", value = "'x'"))
        List<Lease> bad();
    }

    @Repository
    public interface WrapperResult extends MorphiumRepository<Lease, String> {
        @AtomicUpdate(set = @Assign(field = "owner", value = "'x'"))
        Long bad();
    }

    @Repository
    public interface StreamResult extends MorphiumRepository<Lease, String> {
        @AtomicUpdate(set = @Assign(field = "owner", value = "'x'"))
        Stream<Lease> bad();
    }

    @Repository
    public interface UnknownField extends MorphiumRepository<Lease, String> {
        @AtomicUpdate(set = @Assign(field = "ownr", value = "'x'"))
        void bad();
    }

    @Repository
    public interface UnknownAssignParam extends MorphiumRepository<Lease, String> {
        @AtomicUpdate(set = @Assign(field = "owner", param = "nobody"))
        void bad(@Param("owner") String owner);
    }

    @Repository
    public interface UnknownConditionParam extends MorphiumRepository<Lease, String> {
        @AtomicUpdate(where = "id = :ghost", set = @Assign(field = "owner", value = "'x'"))
        void bad(@Param("id") String id);
    }

    @Repository
    public interface NoAssignment extends MorphiumRepository<Lease, String> {
        @AtomicUpdate(where = "id = :id")
        void bad(@Param("id") String id);
    }

    @Repository
    public interface BothParamAndValue extends MorphiumRepository<Lease, String> {
        @AtomicUpdate(set = @Assign(field = "owner", param = "o", value = "'x'"))
        void bad(@Param("o") String o);
    }

    @Repository
    public interface IncByText extends MorphiumRepository<Lease, String> {
        @AtomicUpdate(inc = @Assign(field = "fencing", value = "'one'"))
        void bad();
    }

    @Repository
    public interface SetOnInsertWithoutUpsert extends MorphiumRepository<Lease, String> {
        @AtomicUpdate(setOnInsert = @Assign(field = "owner", value = "'x'"))
        void bad();
    }

    @Repository
    public interface ManyOnDocumentForm extends MorphiumRepository<Lease, String> {
        @AtomicUpdate(set = @Assign(field = "owner", value = "'x'"), many = true)
        Optional<Lease> bad();
    }

    @Repository
    public interface OrderOnCountForm extends MorphiumRepository<Lease, String> {
        @OrderBy("fencing")
        @AtomicUpdate(set = @Assign(field = "owner", value = "'x'"))
        int bad();
    }

    @Repository
    public interface SortParameter extends MorphiumRepository<Lease, String> {
        @AtomicUpdate(set = @Assign(field = "owner", value = "'x'"))
        Optional<Lease> bad(Sort<Lease> sort);
    }

    @Repository
    public interface OrdinalCondition extends MorphiumRepository<Lease, String> {
        @AtomicUpdate(where = "id = ?1", set = @Assign(field = "owner", value = "'x'"))
        void bad(String id);
    }

    @Repository
    public interface BadCondition extends MorphiumRepository<Lease, String> {
        @AtomicUpdate(where = "id == :id", set = @Assign(field = "owner", value = "'x'"))
        void bad(@Param("id") String id);
    }

    @Repository
    public interface FindFirstWithLimit extends MorphiumRepository<Lease, String> {
        List<Lease> findFirst2ByOwner(String owner, jakarta.data.Limit limit);
    }

    @Repository
    public interface FindFirstFine extends MorphiumRepository<Lease, String> {
        List<Lease> findFirst2ByOwner(String owner);
    }

    @Repository
    public interface CursoredWithoutPageRequest extends MorphiumRepository<Lease, String> {
        jakarta.data.page.CursoredPage<Lease> findByOwnerOrderByFencingAsc(String owner);
    }

    @Repository
    public interface CursoredFine extends MorphiumRepository<Lease, String> {
        jakarta.data.page.CursoredPage<Lease> findByOwnerOrderByFencingAsc(String owner, jakarta.data.page.PageRequest pageRequest);
    }

    @Repository
    public interface LifecycleShapes {
        @jakarta.data.repository.Insert
        Lease add(Lease lease);

        @jakarta.data.repository.Insert
        Lease[] addAll(Lease... leases);

        @jakarta.data.repository.Insert
        List<Lease> addList(List<Lease> leases);

        @jakarta.data.repository.Update
        boolean change(List<Lease> leases);

        @jakarta.data.repository.Delete
        long remove(Lease lease);

        @jakarta.data.repository.Save
        void save(Lease lease);

        @jakarta.data.repository.Update
        Boolean changeBoxed(Lease lease);

        @jakarta.data.repository.Delete
        Long removeBoxed(List<Lease> leases);
    }

    @Repository
    public interface LifecycleWrongResult {
        @jakarta.data.repository.Insert
        String add(Lease lease);
    }

    @Repository
    public interface LifecycleOneForMany {
        @jakarta.data.repository.Insert
        Lease addAll(List<Lease> leases);
    }

    private static class Collecting implements BuildProducer<ReflectiveClassBuildItem> {
        @Override
        public void produce(ReflectiveClassBuildItem item) {
        }
    }

    private static void generate(Class<?> repo) throws Exception {
        Indexer indexer = new Indexer();

        for (Class<?> c : new Class<?>[] {Lease.class, repo, MorphiumRepository.class}) {
            try (InputStream in = c.getClassLoader().getResourceAsStream(c.getName().replace('.', '/') + ".class")) {
                indexer.index(in);
            }
        }

        IndexView index = indexer.complete();
        ClassInfo repoInfo = index.getClassByName(DotName.createSimple(repo.getName()));
        Method collect = MorphiumDataProcessor.class.getDeclaredMethod("collectEntityFields", ClassInfo.class, IndexView.class);
        collect.setAccessible(true);
        @SuppressWarnings("unchecked")
        Set<String> fields = (Set<String>) collect.invoke(new MorphiumDataProcessor(), index.getClassByName(DotName.createSimple(Lease.class.getName())), index);
        Method m = MorphiumDataProcessor.class.getDeclaredMethod("generateCustomQueryMethods", ClassCreator.class, ClassInfo.class, IndexView.class, String.class, Set.class,
                BuildProducer.class);
        m.setAccessible(true);
        ClassOutput discard = (name, data) -> {
        };

        try (ClassCreator cc = ClassCreator.builder().classOutput(discard).className(repo.getName() + "_Impl")
                .superClass(de.caluga.morphium.data.AbstractMorphiumRepository.class.getName()).interfaces(repo.getName()).build()) {
            try {
                m.invoke(new MorphiumDataProcessor(), cc, repoInfo, index, Lease.class.getName(), new HashSet<>(fields), new Collecting());
            } catch (InvocationTargetException e) {
                if (e.getCause() instanceof RuntimeException re) {
                    throw re;
                }

                throw e;
            }
        }
    }

    private static void refused(Class<?> repo, String... fragments) {
        var assertion = assertThatThrownBy(() -> generate(repo)).isInstanceOf(IllegalStateException.class).hasMessageContaining("@AtomicUpdate");

        for (String fragment : fragments) {
            assertion.hasMessageContaining(fragment);
        }
    }

    @Test
    @DisplayName("both forms and all result types are generated")
    void accepted() {
        assertThatCode(() -> generate(Accepted.class)).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("collections, streams and wrapper numbers are refused: the return type chooses the form")
    void returnTypes() {
        refused(Collection.class, "Collection.bad", "not supported");
        refused(WrapperResult.class, "WrapperResult.bad", "not supported");
        refused(StreamResult.class, "StreamResult.bad", "not supported");
    }

    @Test
    @DisplayName("an unknown field, a parameter that does not exist and a condition that is not JDQL are refused")
    void names() {
        refused(UnknownField.class, "UnknownField.bad", "ownr");
        refused(UnknownAssignParam.class, "UnknownAssignParam.bad", "nobody");
        refused(UnknownConditionParam.class, "UnknownConditionParam.bad", ":ghost");
        refused(BadCondition.class, "BadCondition.bad", "not valid JDQL");
        refused(OrdinalCondition.class, "OrdinalCondition.bad", "named parameters");
    }

    @Test
    @DisplayName("no assignment, param and value together, a text for inc and setOnInsert without upsert are refused")
    void assignments() {
        refused(NoAssignment.class, "NoAssignment.bad", "at least one assignment");
        refused(BothParamAndValue.class, "BothParamAndValue.bad", "exactly one");
        refused(IncByText.class, "IncByText.bad", "not a number");
        refused(SetOnInsertWithoutUpsert.class, "SetOnInsertWithoutUpsert.bad", "upsert");
    }

    @Test
    @DisplayName("many on the document form, an order on the count form and a Sort parameter are refused")
    void formRules() {
        refused(ManyOnDocumentForm.class, "ManyOnDocumentForm.bad", "many");
        refused(OrderOnCountForm.class, "OrderOnCountForm.bad", "@OrderBy");
        refused(SortParameter.class, "SortParameter.bad", "Sort");
    }

    @Test
    @DisplayName("MOR-079: findFirstN with a Limit parameter fails the build, without one it is generated")
    void findFirstWithLimit() {
        assertThatThrownBy(() -> generate(FindFirstWithLimit.class)).isInstanceOf(IllegalStateException.class).hasMessageContaining("findFirst2ByOwner").hasMessageContaining("Limit");
        assertThatCode(() -> generate(FindFirstFine.class)).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("MOR-086: a CursoredPage result needs a PageRequest parameter")
    void cursoredPageNeedsAPageRequest() {
        assertThatThrownBy(() -> generate(CursoredWithoutPageRequest.class)).isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("findByOwnerOrderByFencingAsc").hasMessageContaining("PageRequest");
        assertThatCode(() -> generate(CursoredFine.class)).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("MOR-077: lifecycle methods in every shape are generated; a result they cannot give fails the build")
    void lifecycleShapes() {
        assertThatCode(() -> generate(LifecycleShapes.class)).doesNotThrowAnyException();
        assertThatThrownBy(() -> generate(LifecycleWrongResult.class)).isInstanceOf(IllegalStateException.class).hasMessageContaining("LifecycleWrongResult.add").hasMessageContaining("returns nothing");
        assertThatThrownBy(() -> generate(LifecycleOneForMany.class)).isInstanceOf(IllegalStateException.class).hasMessageContaining("several entities");
    }
}
