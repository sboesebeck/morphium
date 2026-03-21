package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.AnnotationAndReflectionHelper;
import de.caluga.morphium.EntityRegistry;
import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.ObjectMapperImpl;
import de.caluga.morphium.annotations.Embedded;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import org.junit.jupiter.api.*;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for {@link EntityRegistry} — the pre-registration API that makes
 * ClassGraph optional for frameworks like Quarkus.
 */
@Tag("core")
class EntityRegistryTest {

    @AfterEach
    void cleanup() {
        EntityRegistry.clear();
        AnnotationAndReflectionHelper.clearTypeIdCache();
        ObjectMapperImpl.clearEntityCache();
    }

    @Test
    void preRegisterEntities_hasPreRegisteredEntitiesReturnsTrue() {
        EntityRegistry.preRegisterEntities(List.of(SampleEntity.class, SampleEmbedded.class));
        assertTrue(EntityRegistry.hasPreRegisteredEntities());
    }

    @Test
    void preRegisterEntityNames_hasPreRegisteredEntitiesReturnsTrue() {
        EntityRegistry.preRegisterEntityNames(List.of(
            SampleEntity.class.getName(),
            SampleEmbedded.class.getName()
        ));
        assertTrue(EntityRegistry.hasPreRegisteredEntities());
    }

    @Test
    void clear_resetsAllState() {
        EntityRegistry.preRegisterEntities(List.of(SampleEntity.class));
        assertTrue(EntityRegistry.hasPreRegisteredEntities());

        EntityRegistry.clear();
        assertFalse(EntityRegistry.hasPreRegisteredEntities());
    }

    @Test
    void hasPreRegisteredEntities_falseWhenEmpty() {
        assertFalse(EntityRegistry.hasPreRegisteredEntities());
    }

    @Test
    void morphiumUsesPreRegisteredEntities() {
        // Pre-register before creating Morphium
        EntityRegistry.preRegisterEntities(List.of(SampleEntity.class));
        AnnotationAndReflectionHelper.clearTypeIdCache();
        ObjectMapperImpl.clearEntityCache();

        MorphiumConfig cfg = new MorphiumConfig("entity_registry_test_db", 10, 10_000, 1_000);
        cfg.driverSettings().setDriverName(InMemoryDriver.driverName);
        try (Morphium m = new Morphium(cfg)) {
            // The ObjectMapper should know about SampleEntity via pre-registration
            String collName = m.getMapper().getCollectionName(SampleEntity.class);
            assertEquals("sample_entities", collName);
        }
    }

    @Test
    void typeIdResolutionWorksWithPreRegistration() throws Exception {
        // Pre-register and reset type ID cache so it initialises from registry
        EntityRegistry.preRegisterEntities(List.of(SampleEntity.class, SampleEmbedded.class));
        AnnotationAndReflectionHelper.clearTypeIdCache();

        AnnotationAndReflectionHelper arh = new AnnotationAndReflectionHelper(true);
        // The typeId "sample" should resolve to the entity class
        assertEquals(SampleEntity.class, arh.getClassForTypeId("sample"));
        assertEquals(SampleEmbedded.class, arh.getClassForTypeId("sample_embedded"));
    }

    // ------------------------------------------------------------------
    // Test data classes
    // ------------------------------------------------------------------

    @Entity(collectionName = "sample_entities", typeId = "sample")
    static class SampleEntity {
        String name;
    }

    @Embedded(typeId = "sample_embedded")
    static class SampleEmbedded {
        String value;
    }
}
