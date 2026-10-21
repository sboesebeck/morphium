package de.caluga.morphium.spring.autoconfigure;

import de.caluga.morphium.Morphium;
import jakarta.data.Order;
import jakarta.data.Sort;
import jakarta.data.page.Page;
import jakarta.data.page.PageRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest(classes = TestApplication.class)
@ActiveProfiles("test")
class MorphiumRepositoryProxyTest {

    @Autowired
    TestEntityRepository repository;

    @Autowired
    Morphium morphium;

    @BeforeEach
    void cleanUp() {
        morphium.clearCollection(TestEntity.class);
    }

    @Test
    void repositoryIsInjected() {
        assertNotNull(repository);
    }

    @Test
    void saveAndFindById() {
        var entity = new TestEntity("test", "active", 1);
        var saved = (TestEntity) repository.save(entity);
        assertNotNull(saved.getId());

        var found = repository.findById(saved.getId());
        assertTrue(found.isPresent());
        assertEquals("test", ((TestEntity) found.get()).getName());
    }

    @Test
    void findByStatus() {
        repository.save(new TestEntity("a", "active", 1));
        repository.save(new TestEntity("b", "active", 2));
        repository.save(new TestEntity("c", "inactive", 3));

        List<TestEntity> active = repository.findByStatus("active");
        assertEquals(2, active.size());
    }

    @Test
    void findByStatusAndPriority() {
        repository.save(new TestEntity("a", "active", 1));
        repository.save(new TestEntity("b", "active", 2));
        repository.save(new TestEntity("c", "active", 1));

        List<TestEntity> result = repository.findByStatusAndPriority("active", 1);
        assertEquals(2, result.size());
    }

    @Test
    void countByStatus() {
        repository.save(new TestEntity("a", "active", 1));
        repository.save(new TestEntity("b", "active", 2));
        repository.save(new TestEntity("c", "inactive", 3));

        assertEquals(2, repository.countByStatus("active"));
        assertEquals(1, repository.countByStatus("inactive"));
    }

    @Test
    void deleteById() {
        var entity = new TestEntity("test", "active", 1);
        var saved = (TestEntity) repository.save(entity);

        repository.deleteById(saved.getId());

        var found = repository.findById(saved.getId());
        assertTrue(found.isEmpty());
    }

    @Test
    void morphiumAccessViaMorphiumRepository() {
        assertNotNull(repository.morphium());
        assertSame(morphium, repository.morphium());
    }

    @Test
    void queryAccessViaMorphiumRepository() {
        repository.save(new TestEntity("test", "active", 1));

        var query = repository.query();
        assertNotNull(query);
        assertEquals(1, query.countAll());
    }

    // -- Regression: @Find methods must honor @By parameter bindings, not @Param --

    @Test
    void findWithByAnnotationBindsTheAnnotatedField() throws Exception {
        repository.save(new TestEntity("a", "active", 1));
        repository.save(new TestEntity("b", "active", 2));
        repository.save(new TestEntity("c", "inactive", 3));

        // buildConditionsSpec() previously read @Param (never present here) or the
        // reflected parameter name -- without -parameters that name is "arg0", so the
        // condition became "arg0:0" instead of "status:0" and matched nothing.
        List<TestEntity> active = repository.byStatus("active");
        assertEquals(2, active.size());
    }

    // -- Regression: derived query methods returning CompletionStage must actually
    //    run asynchronously, not throw ClassCastException on the raw sync result --

    @Test
    void derivedQueryWithCompletionStageReturnTypeExecutesAsynchronously() throws Exception {
        repository.save(new TestEntity("a", "active", 1));
        repository.save(new TestEntity("b", "active", 2));
        repository.save(new TestEntity("c", "inactive", 3));

        CompletionStage<List<TestEntity>> stage = repository.findByStatusAsync("active");
        List<TestEntity> active = stage.toCompletableFuture().get(5, TimeUnit.SECONDS);
        assertEquals(2, active.size());
    }

    // ---- Review finding 1: derived queries dropped dynamic Sort/PageRequest ----

    @Test
    void derivedQueryHonoursDynamicSortArgument() {
        repository.save(new TestEntity("low", "active", 1));
        repository.save(new TestEntity("high", "active", 9));
        repository.save(new TestEntity("mid", "active", 5));

        // Without the fix the Sort argument was silently dropped: no exception, but the
        // result came back in insertion order. Assert the ORDER, not just the size.
        List<TestEntity> desc = repository.findByStatus("active", Sort.desc("priority"));
        assertEquals(3, desc.size());
        assertEquals(List.of(9, 5, 1), desc.stream().map(TestEntity::getPriority).toList());

        List<TestEntity> asc = repository.findByStatus("active", Sort.asc("priority"));
        assertEquals(List.of(1, 5, 9), asc.stream().map(TestEntity::getPriority).toList());
    }

    @Test
    void derivedQueryWithPageReturnTypeYieldsAPage() {
        for (int i = 1; i <= 5; i++) {
            repository.save(new TestEntity("e" + i, "active", i));
        }

        // Without the fix this threw ClassCastException: the simple bridge overload
        // returned a plain ArrayList where the proxy expected a Page.
        Page<TestEntity> page = repository.findByStatus("active", PageRequest.ofSize(2));
        assertNotNull(page);
        assertEquals(2, page.content().size());
    }

    // ---- Review finding 2: @Delete with a numeric return type returned null ----

    @Test
    void annotatedDeleteWithLongReturnTypeReturnsTheDeleteCount() {
        repository.save(new TestEntity("a", "active", 1));
        repository.save(new TestEntity("b", "active", 2));
        repository.save(new TestEntity("c", "inactive", 3));

        // Without the fix the void bridge ran and the handler returned null, which blew up
        // as a NullPointerException unboxing null into the primitive long return value.
        long deleted = repository.deleteCountedByStatus("active");
        assertEquals(2, deleted);

        // ... and the rows really are gone, not just counted.
        assertEquals(0, repository.findByStatus("active").size());
        assertEquals(1, repository.findByStatus("inactive").size());
    }

    // ---- MOR-024: ordinal parameters (?1) in @Query ----

    @Test
    void jdqlQueryWithOrdinalParametersBindsThemByPosition() {
        repository.save(new TestEntity("a", "active", 1));
        repository.save(new TestEntity("b", "active", 5));
        repository.save(new TestEntity("c", "inactive", 9));

        List<TestEntity> found = repository.queryOrdinal("active", 2);

        assertEquals(1, found.size());
        assertEquals("b", found.get(0).getName());
    }

    @Test
    void jdqlQueryWithOrdinalParametersSkipsTheSortParameter() {
        repository.save(new TestEntity("low", "active", 1));
        repository.save(new TestEntity("high", "active", 9));

        List<TestEntity> found = repository.queryOrdinalSorted("active", Sort.desc("priority"));

        assertEquals(List.of("high", "low"), found.stream().map(TestEntity::getName).toList());
    }

    // ---- MOR-017: @Query("DELETE FROM ...") ----

    @Test
    void jdqlDeleteReturnsTheNumberOfDeletedDocuments() {
        repository.save(new TestEntity("a", "active", 1));
        repository.save(new TestEntity("b", "active", 5));
        repository.save(new TestEntity("c", "inactive", 9));

        assertEquals(2L, repository.deleteJdqlByStatus("active"));
        assertEquals(1, repository.findByStatus("inactive").size());
        assertEquals(0, repository.findByStatus("active").size());
    }

    @Test
    void jdqlDeleteWithOrdinalParametersAndIntResult() {
        repository.save(new TestEntity("a", "active", 1));
        repository.save(new TestEntity("b", "active", 5));

        assertEquals(1, repository.deleteJdqlOrdinal("active", 3));
        assertEquals(List.of("b"), repository.findByStatus("active").stream().map(TestEntity::getName).toList());
    }

    @Test
    void jdqlDeleteWithVoidResult() {
        repository.save(new TestEntity("a", "active", 1));
        repository.save(new TestEntity("b", "active", 5));

        repository.deleteJdqlByName("a");

        assertEquals(1, repository.findByStatus("active").size());
    }

    // ---- MOR-018: @Query("UPDATE ... SET ...") ----

    @Test
    void jdqlUpdateSetsAndIncrementsAndReturnsTheMatchedCount() {
        repository.save(new TestEntity("a", "active", 1));
        repository.save(new TestEntity("b", "active", 5));

        assertEquals(1L, repository.updateJdql("inactive", "a"));
        assertEquals(0L, repository.updateJdql("inactive", "nobody"));

        List<TestEntity> inactive = repository.findByStatus("inactive");
        assertEquals(1, inactive.size());
        assertEquals(2, inactive.get(0).getPriority());
    }

    @Test
    void jdqlUpdateWithOrdinalParametersAndIntResult() {
        repository.save(new TestEntity("a", "active", 2));
        repository.save(new TestEntity("b", "active", 3));
        repository.save(new TestEntity("c", "inactive", 4));

        assertEquals(2, repository.updateJdqlOrdinal("active", 10));
        assertEquals(List.of(20, 30), repository.findByStatus("active").stream().map(TestEntity::getPriority).sorted().toList());
        assertEquals(4, repository.findByStatus("inactive").get(0).getPriority());
    }

    @Test
    void jdqlUpdateWithVoidResult() {
        repository.save(new TestEntity("a", "active", 1));

        repository.updateJdqlVoid("a");

        assertEquals(1, repository.findByStatus("done").size());
    }

    // ---- MOR-021: @AtomicUpdate ----

    @Test
    void atomicUpdateUpsertsAndReturnsTheDocument() {
        var first = repository.touch("job", "running");
        assertTrue(first.isPresent());
        assertEquals(1, first.get().getPriority());

        var second = repository.touch("job", "again");
        assertEquals(2, second.get().getPriority());
        assertEquals("again", second.get().getStatus());
        assertEquals(1, repository.findByStatus("again").size());
    }

    @Test
    void atomicUpdateCountForms() {
        repository.save(new TestEntity("a", "active", 1));
        repository.save(new TestEntity("b", "active", 5));

        assertEquals(2, repository.bumpAll("active", 10));
        assertEquals(List.of(11, 15), repository.findByStatus("active").stream().map(TestEntity::getPriority).sorted().toList());
        assertTrue(repository.finish("a"));
        assertFalse(repository.finish("nobody"));
    }

    private void saveSortFixture() {
        repository.save(new TestEntity("a", "x", 2));
        repository.save(new TestEntity("b", "x", 1));
        repository.save(new TestEntity("c", "x", 2));
        repository.save(new TestEntity("d", "x", 1));
    }

    @Test
    void severalSortParametersAreAppliedInTheOrderOfTheParameters() {
        saveSortFixture();

        assertEquals(List.of("d", "b", "c", "a"),
                repository.findByStatus("x", Sort.asc("priority"), Sort.desc("name")).stream().map(TestEntity::getName).toList());
        assertEquals(List.of("d", "b", "c", "a"),
                repository.findByStatus("x", Sort.asc("priority"), Sort.desc("name"), Sort.asc("id")).stream().map(TestEntity::getName).toList());
    }

    @Test
    void theStaticOrderComesFirstAndTheDynamicOrderBreaksTheTies() {
        saveSortFixture();

        assertEquals(List.of("d", "b", "c", "a"),
                repository.findByStatusOrderByPriorityAsc("x", Order.by(Sort.desc("priority"), Sort.desc("name"))).stream().map(TestEntity::getName).toList());
    }

    @Test
    void aMethodMayReturnAnArray() {
        repository.save(new TestEntity("a", "active", 2));
        repository.save(new TestEntity("b", "active", 1));

        assertEquals(List.of("b", "a"), java.util.Arrays.stream(repository.findByStatusOrderByPriorityAsc("active")).map(TestEntity::getName).toList());
        assertEquals(List.of("a", "b"), java.util.Arrays.stream(repository.arrayQueryByStatus("active")).map(TestEntity::getName).toList());
        assertEquals(0, repository.findByStatusOrderByPriorityAsc("none").length);
    }

    @Test
    void findFirstLimitsTheResult() {
        repository.save(new TestEntity("a", "active", 1));
        repository.save(new TestEntity("b", "active", 5));
        repository.save(new TestEntity("c", "active", 3));

        assertEquals("b", repository.findFirstByStatusOrderByPriorityDesc("active").get().getName());
        assertEquals(List.of("a", "c"), repository.findFirst2ByStatusOrderByPriorityAsc("active").stream().map(TestEntity::getName).toList());
    }

    @Test
    void selectFieldAnswersTheValuesWhenTheMethodReturnsThem() {
        repository.save(new TestEntity("a", "active", 2));
        repository.save(new TestEntity("b", "active", 1));
        repository.save(new TestEntity("c", "inactive", 3));

        assertEquals(List.of("b", "a"), repository.namesByStatus("active"));
        assertEquals(1, repository.priorityOf("b").get());
        assertTrue(repository.priorityOf("zzz").isEmpty());
        assertEquals("c", repository.nameOf("c"));
        assertEquals(List.of("b", "a"), repository.streamedNames("active").toList());

        jakarta.data.page.Page<String> page = repository.pagedNames("active", PageRequest.ofPage(1).size(1).withTotal());
        assertEquals(List.of("b"), page.content());
        assertEquals(2, page.totalElements());
    }

    @Test
    void findFirstCannotBeCombinedWithALimitParameter() {
        repository.save(new TestEntity("a", "active", 1));

        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> repository.findFirst2ByStatus("active", jakarta.data.Limit.of(1)));
        assertTrue(e.getMessage().contains("findFirst"), e.getMessage());
    }

    @Test
    void countAllHasNoByAndCountsEveryDocument() {
        repository.save(new TestEntity("a", "active", 1));
        repository.save(new TestEntity("b", "inactive", 2));

        assertEquals(2L, repository.countAll());
    }

    // ---- Review finding 3: @Query / @Find with CompletionStage ran synchronously ----

    @Test
    void jdqlQueryWithCompletionStageReturnTypeYieldsAList() throws Exception {
        repository.save(new TestEntity("a", "active", 1));
        repository.save(new TestEntity("b", "active", 2));
        repository.save(new TestEntity("c", "inactive", 3));

        CompletionStage<List<TestEntity>> stage = repository.queryByStatusAsync("active");
        Object result = stage.toCompletableFuture().get(5, TimeUnit.SECONDS);

        // Two distinct defects were possible here. Without the async branch the proxy threw
        // ClassCastException outright. With the async branch but a returnsSingle that still
        // ignored CompletionStage, the stage completed with ONE entity instead of a list --
        // no exception, wrong result. Assert the shape explicitly to catch both.
        assertInstanceOf(List.class, result, "stage must complete with a List, not a single entity");
        assertEquals(2, ((List<?>) result).size());
    }

    @Test
    void annotatedFindWithCompletionStageReturnTypeYieldsAList() throws Exception {
        repository.save(new TestEntity("a", "active", 1));
        repository.save(new TestEntity("b", "active", 2));
        repository.save(new TestEntity("c", "inactive", 3));

        CompletionStage<List<TestEntity>> stage = repository.findAsyncByStatus("active");
        Object result = stage.toCompletableFuture().get(5, TimeUnit.SECONDS);

        assertInstanceOf(List.class, result, "stage must complete with a List, not a single entity");
        assertEquals(2, ((List<?>) result).size());
    }

    // ---- Review finding 4: default methods hit "Unsupported repository method" ----

    @Test
    void defaultMethodRunsItsOwnImplementation() {
        repository.save(new TestEntity("a", "active", 1));
        repository.save(new TestEntity("b", "active", 2));
        repository.save(new TestEntity("c", "inactive", 3));

        // Deliberately named countBy* so this also proves the isDefault() check wins over
        // derived-query parsing. Without the fix: UnsupportedOperationException.
        assertEquals(2, repository.countByStatusViaDefaultMethod("active"));
        assertEquals(1, repository.countByStatusViaDefaultMethod("inactive"));
    }
}
