package de.caluga.morphium.spring.autoconfigure;

import de.caluga.morphium.Morphium;
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
}
