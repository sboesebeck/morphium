package de.caluga.morphium.spring.autoconfigure;

import de.caluga.morphium.Morphium;
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
