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

import java.util.ArrayList;
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

    // ------------------------------------------------------------------
    // Basic registration + state
    // ------------------------------------------------------------------

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

    // ------------------------------------------------------------------
    // Null / invalid input validation
    // ------------------------------------------------------------------

    @Test
    void preRegisterEntities_nullCollection_throwsIllegalArgument() {
        assertThrows(IllegalArgumentException.class, () ->
            EntityRegistry.preRegisterEntities(null));
    }

    @Test
    void preRegisterEntities_nullElementsAreSkipped() {
        List<Class<?>> withNull = new ArrayList<>();
        withNull.add(SampleEntity.class);
        withNull.add(null);
        withNull.add(SampleEmbedded.class);
        EntityRegistry.preRegisterEntities(withNull);
        assertTrue(EntityRegistry.hasPreRegisteredEntities());
    }

    @Test
    void preRegisterEntities_unannotatedClassesAreSkipped() {
        EntityRegistry.preRegisterEntities(List.of(UnannotatedClass.class));
        // Unannotated class should not count as a registered entity
        assertFalse(EntityRegistry.hasPreRegisteredEntities());
    }

    @Test
    void preRegisterEntities_mixedAnnotatedAndUnannotated() throws Exception {
        EntityRegistry.preRegisterEntities(List.of(
            SampleEntity.class, UnannotatedClass.class, SampleEmbedded.class));
        assertTrue(EntityRegistry.hasPreRegisteredEntities());

        AnnotationAndReflectionHelper.clearTypeIdCache();
        AnnotationAndReflectionHelper arh = new AnnotationAndReflectionHelper(true);
        // Annotated classes resolve, unannotated does not
        assertEquals(SampleEntity.class, arh.getClassForTypeId("sample"));
        assertEquals(SampleEmbedded.class, arh.getClassForTypeId("sample_embedded"));
    }

    // ------------------------------------------------------------------
    // Hierarchy annotation support
    // ------------------------------------------------------------------

    @Test
    void preRegisterEntities_subclassInheritsEntityFromSuperclass() throws Exception {
        // ChildEntity does not have @Entity itself — it inherits from SampleEntity
        EntityRegistry.preRegisterEntities(List.of(ChildEntity.class));
        assertTrue(EntityRegistry.hasPreRegisteredEntities());

        AnnotationAndReflectionHelper.clearTypeIdCache();
        AnnotationAndReflectionHelper arh = new AnnotationAndReflectionHelper(true);
        // The inherited typeId "sample" should resolve to ChildEntity
        assertEquals(ChildEntity.class, arh.getClassForTypeId("sample"));
    }

    @Test
    void preRegisterEntities_classInheritsEmbeddedFromInterface() throws Exception {
        EntityRegistry.preRegisterEntities(List.of(EmbeddedImpl.class));
        assertTrue(EntityRegistry.hasPreRegisteredEntities());

        AnnotationAndReflectionHelper.clearTypeIdCache();
        AnnotationAndReflectionHelper arh = new AnnotationAndReflectionHelper(true);
        assertEquals(EmbeddedImpl.class, arh.getClassForTypeId("iface_embedded"));
    }

    // ------------------------------------------------------------------
    // Hot-reload scenario (clear + re-register)
    // ------------------------------------------------------------------

    @Test
    void hotReload_clearAndReRegisterWithDifferentEntities() throws Exception {
        EntityRegistry.preRegisterEntities(List.of(SampleEntity.class));
        AnnotationAndReflectionHelper.clearTypeIdCache();

        AnnotationAndReflectionHelper arh1 = new AnnotationAndReflectionHelper(true);
        assertEquals(SampleEntity.class, arh1.getClassForTypeId("sample"));

        // Simulate hot-reload: clear everything and re-register with a different entity
        EntityRegistry.clear();
        AnnotationAndReflectionHelper.clearTypeIdCache();
        ObjectMapperImpl.clearEntityCache();

        EntityRegistry.preRegisterEntities(List.of(SampleEmbedded.class));
        AnnotationAndReflectionHelper arh2 = new AnnotationAndReflectionHelper(true);
        // "sample" should no longer resolve (only SampleEmbedded registered now)
        assertThrows(ClassNotFoundException.class, () -> arh2.getClassForTypeId("sample"));
        assertEquals(SampleEmbedded.class, arh2.getClassForTypeId("sample_embedded"));
    }

    // ------------------------------------------------------------------
    // Morphium / ObjectMapper integration
    // ------------------------------------------------------------------

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

    @Test
    void objectMapperUsesPreRegisteredEntities() {
        EntityRegistry.preRegisterEntities(List.of(SampleEntity.class, SampleEmbedded.class));
        AnnotationAndReflectionHelper.clearTypeIdCache();
        ObjectMapperImpl.clearEntityCache();

        MorphiumConfig cfg = new MorphiumConfig("objmapper_test_db", 10, 10_000, 1_000);
        cfg.driverSettings().setDriverName(InMemoryDriver.driverName);
        try (Morphium m = new Morphium(cfg)) {
            // ObjectMapper should resolve collection name for pre-registered entity
            assertEquals("sample_entities", m.getMapper().getCollectionName(SampleEntity.class));
            // And getClassForCollectionName should work too
            Class<?> resolved = m.getMapper().getClassForCollectionName("sample_entities");
            assertEquals(SampleEntity.class, resolved);
        }
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

    /** Subclass that inherits @Entity from SampleEntity (no own annotation). */
    static class ChildEntity extends SampleEntity {
        String extra;
    }

    /** Class without any Morphium annotation. */
    static class UnannotatedClass {
        String data;
    }

    @Embedded(typeId = "iface_embedded")
    interface EmbeddedInterface {}

    /** Implements an @Embedded interface — annotation is inherited via interface hierarchy. */
    static class EmbeddedImpl implements EmbeddedInterface {
        String value;
    }
}
