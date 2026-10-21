package de.caluga.morphium.spring.autoconfigure;

import de.caluga.morphium.Morphium;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * MOR-077: repositories that extend nothing. The lifecycle methods ({@code @Insert}, {@code @Save}, {@code @Update}, {@code @Delete} with entities) run under any name and in every shape;
 * the primary entity of such a repository is the entity of its lifecycle methods; with several entities, each method works on the one it names, takes or returns.
 */
@SpringBootTest(classes = TestApplication.class)
@ActiveProfiles("test")
class NoPrimaryEntityRepositoryTest {

    @Autowired
    LifecycleOnlyRepository lifecycle;

    @Autowired
    TwoEntitiesRepository two;

    @Autowired
    Morphium morphium;

    @BeforeEach
    void cleanUp() {
        morphium.clearCollection(TestEntity.class);
        morphium.clearCollection(OtherEntity.class);
    }

    private static List<String> names(List<TestEntity> entities) {
        return entities.stream().map(TestEntity::getName).sorted().toList();
    }

    @Test
    void repositoriesThatExtendNothingAreRegistered() {
        assertNotNull(lifecycle);
        assertNotNull(two);
    }

    @Test
    void insertInEveryShape() {
        TestEntity one = lifecycle.add(new TestEntity("a", "active", 1));
        assertNotNull(one.getId());

        assertEquals(2, lifecycle.addAll(List.of(new TestEntity("b", "active", 2), new TestEntity("c", "active", 3))).size());

        TestEntity[] array = lifecycle.addArray(new TestEntity("d", "inactive", 4), new TestEntity("e", "inactive", 5));
        assertEquals(2, array.length);
        assertNotNull(array[0].getId());

        lifecycle.addQuietly(new TestEntity("f", "inactive", 6));

        assertEquals(6, morphium.createQueryFor(TestEntity.class).countAll());
    }

    @Test
    void saveUpdateAndDelete() {
        TestEntity a = lifecycle.add(new TestEntity("a", "active", 1));
        TestEntity b = lifecycle.add(new TestEntity("b", "active", 2));

        lifecycle.saveOne(new TestEntity("c", "active", 3));
        assertEquals(3, morphium.createQueryFor(TestEntity.class).countAll());

        a.setPriority(10);
        assertEquals(10, lifecycle.change(a).getPriority());
        b.setPriority(20);
        assertTrue(lifecycle.changeAll(List.of(b)));
        assertEquals(20, morphium.findById(TestEntity.class, b.getId(), null).getPriority());

        assertEquals(1L, lifecycle.removeOne(a));
        lifecycle.remove(List.of(b));
        assertEquals(List.of("c"), names(morphium.createQueryFor(TestEntity.class).asList()));
    }

    @Test
    void derivedAndQueryMethodsUseThePrimaryEntityOfTheLifecycleMethods() {
        lifecycle.addAll(List.of(new TestEntity("a", "active", 1), new TestEntity("b", "active", 2), new TestEntity("c", "inactive", 3)));

        assertEquals(2L, lifecycle.countByStatus("active"));
        assertEquals(List.of("a", "b"), names(lifecycle.findByStatus("active")));
        assertEquals("c", lifecycle.byName("c").get().getName());
        assertEquals(2L, lifecycle.dropStatus("active"));
        assertEquals(1, morphium.createQueryFor(TestEntity.class).countAll());
    }

    @Test
    void everyMethodOfARepositoryWithSeveralEntitiesWorksOnItsOwn() {
        two.add(new TestEntity("a", "active", 2));
        two.add(new TestEntity("b", "active", 1));
        two.add(new TestEntity("c", "gone", 3));
        OtherEntity[] others = two.add(new OtherEntity("x", 1), new OtherEntity("y", 5), new OtherEntity("z", 9));
        assertEquals(3, others.length);

        assertEquals(List.of("b", "a"), two.testsByStatus("active").stream().map(TestEntity::getName).toList());
        assertEquals("y", two.otherByLabel("y").get().getLabel());
        assertTrue(two.otherByLabel("nobody").isEmpty());
        assertEquals(List.of("y", "z"), two.labels(1));

        assertEquals(1L, two.grow("x", 10));
        assertEquals(List.of("x", "y", "z"), two.labels(1));

        assertTrue(two.shrink("z", 3));
        assertEquals(3, morphium.createQueryFor(OtherEntity.class).f("label").eq("z").get().getSize());
        assertTrue(!two.shrink("nobody", 3));
        assertTrue(two.removeLabel("z"));
        assertTrue(!two.removeLabel("z"));

        assertEquals(1L, two.removeTests("gone"));
        assertEquals(2, morphium.createQueryFor(TestEntity.class).countAll());
        assertEquals(2L, two.removeOthers());
        assertEquals(0, morphium.createQueryFor(OtherEntity.class).countAll());
        assertEquals(2, morphium.createQueryFor(TestEntity.class).countAll());
    }
}
