package de.caluga.morphium;

import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Verifies that Morphium works correctly via the ClassGraph fallback path
 * (no EntityRegistry pre-registration). This tests the refactored scan
 * methods in ClassGraphHelper end-to-end.
 */
@Tag("core")
class ClassGraphFallbackTest {

    @BeforeEach
    void ensureNoPreRegistration() {
        // Make sure no pre-registration is active
        EntityRegistry.clear();
        AnnotationAndReflectionHelper.clearTypeIdCache();
        ObjectMapperImpl.clearEntityCache();
    }

    @AfterEach
    void cleanup() {
        EntityRegistry.clear();
        AnnotationAndReflectionHelper.clearTypeIdCache();
        ObjectMapperImpl.clearEntityCache();
    }

    @Test
    void morphiumWorksWithoutPreRegistration() {
        assertFalse(EntityRegistry.hasPreRegisteredEntities(),
            "Pre-condition: no entities should be pre-registered");

        MorphiumConfig cfg = new MorphiumConfig("classgraph_fallback_test", 10, 10_000, 1_000);
        cfg.driverSettings().setDriverName(InMemoryDriver.driverName);
        try (Morphium m = new Morphium(cfg)) {
            // ObjectMapper should have found entities via ClassGraph scan
            String collName = m.getMapper().getCollectionName(TestEntity.class);
            assertNotNull(collName);
            assertFalse(collName.isEmpty());
        }
    }

    @Test
    void typeIdResolutionWorksViaClassGraphFallback() {
        assertFalse(EntityRegistry.hasPreRegisteredEntities());

        // Create a fresh AnnotationAndReflectionHelper — should use ClassGraph fallback
        AnnotationAndReflectionHelper arh = new AnnotationAndReflectionHelper(true);
        // The FQCN should at least resolve to itself
        assertDoesNotThrow(() -> arh.getClassForTypeId(TestEntity.class.getName()));
    }

    @Entity(collectionName = "classgraph_fallback_test_entities", typeId = "cg_fallback_test")
    static class TestEntity {
        String name;
    }
}
