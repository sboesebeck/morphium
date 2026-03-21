package de.caluga.morphium;

import de.caluga.morphium.annotations.Embedded;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import org.junit.jupiter.api.*;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for entity inheritance scenarios in the ClassGraph-optional flow.
 * <p>
 * Verifies that:
 * <ul>
 *   <li>Subclasses that inherit @Entity from a superclass are correctly handled
 *       when explicitly passed to EntityRegistry (N4)</li>
 *   <li>Jandex-style discovery (only directly annotated classes) works correctly
 *       when combined with EntityRegistry hierarchy-aware registration</li>
 *   <li>End-to-end Morphium instance creation works with inherited annotations</li>
 * </ul>
 */
@Tag("core")
class EntityRegistryInheritanceTest {

    @AfterEach
    void cleanup() {
        EntityRegistry.clear();
        AnnotationAndReflectionHelper.clearTypeIdCache();
        ObjectMapperImpl.clearEntityCache();
    }

    @Test
    @DisplayName("Subclass inheriting @Entity from superclass resolves typeId correctly")
    void subclass_inheritsEntityAnnotation_resolvesTypeId() throws Exception {
        // Simulate Quarkus-style registration: only the parent is discovered by Jandex,
        // but we explicitly add the child (e.g. via additional configuration)
        EntityRegistry.preRegisterEntities(List.of(ParentEntity.class, ChildEntity.class));
        AnnotationAndReflectionHelper.clearTypeIdCache();

        assertTrue(EntityRegistry.hasPreRegisteredEntities());
        assertTrue(EntityRegistry.getPreRegisteredEntities().contains(ParentEntity.class));
        assertTrue(EntityRegistry.getPreRegisteredEntities().contains(ChildEntity.class));

        // Both should resolve the inherited typeId
        AnnotationAndReflectionHelper arh = new AnnotationAndReflectionHelper(true);
        // The last registered class with typeId "inh_parent" wins
        Class<?> resolved = arh.getClassForTypeId("inh_parent");
        assertTrue(resolved == ParentEntity.class || resolved == ChildEntity.class,
            "TypeId 'inh_parent' should resolve to either parent or child: " + resolved);
    }

    @Test
    @DisplayName("Subclass inheriting @Entity works in ObjectMapper collection mapping")
    void subclass_inheritsEntityAnnotation_objectMapperWorks() {
        EntityRegistry.preRegisterEntities(List.of(ParentEntity.class, ChildEntity.class));
        AnnotationAndReflectionHelper.clearTypeIdCache();
        ObjectMapperImpl.clearEntityCache();

        MorphiumConfig cfg = new MorphiumConfig("inheritance_test_db", 10, 10_000, 1_000);
        cfg.driverSettings().setDriverName(InMemoryDriver.driverName);
        try (Morphium m = new Morphium(cfg)) {
            // Both parent and child should map to the same collection
            String parentColl = m.getMapper().getCollectionName(ParentEntity.class);
            String childColl = m.getMapper().getCollectionName(ChildEntity.class);
            assertEquals("inh_parent_coll", parentColl);
            assertEquals("inh_parent_coll", childColl,
                "Child class should inherit the parent's collection name");
        }
    }

    @Test
    @DisplayName("Class implementing @Embedded interface is correctly registered")
    void implementor_inheritsEmbeddedFromInterface_isRegistered() throws Exception {
        EntityRegistry.preRegisterEntities(List.of(EmbeddedInterfaceImpl.class));
        AnnotationAndReflectionHelper.clearTypeIdCache();

        assertTrue(EntityRegistry.hasPreRegisteredEntities());

        AnnotationAndReflectionHelper arh = new AnnotationAndReflectionHelper(true);
        assertEquals(EmbeddedInterfaceImpl.class, arh.getClassForTypeId("inh_embedded_iface"));
    }

    @Test
    @DisplayName("Jandex-style discovery: only directly annotated classes, subclasses excluded")
    void jandexStyleDiscovery_onlyDirectAnnotations() {
        // Simulate what Jandex discovers: only classes with DIRECT @Entity annotation
        // ChildEntity does NOT have its own @Entity — Jandex would not discover it
        EntityRegistry.preRegisterEntities(List.of(ParentEntity.class));
        AnnotationAndReflectionHelper.clearTypeIdCache();
        ObjectMapperImpl.clearEntityCache();

        assertTrue(EntityRegistry.hasPreRegisteredEntities());
        assertTrue(EntityRegistry.getPreRegisteredEntities().contains(ParentEntity.class));
        // ChildEntity should NOT be in the set — it wasn't explicitly passed
        assertFalse(EntityRegistry.getPreRegisteredEntities().contains(ChildEntity.class),
            "Jandex-style discovery should only include directly annotated classes");
    }

    @Test
    @DisplayName("Multi-level inheritance: grandchild with no annotation")
    void grandchild_inheritsEntityAnnotation_isRegistered() throws Exception {
        EntityRegistry.preRegisterEntities(List.of(GrandchildEntity.class));
        AnnotationAndReflectionHelper.clearTypeIdCache();

        assertTrue(EntityRegistry.hasPreRegisteredEntities());
        AnnotationAndReflectionHelper arh = new AnnotationAndReflectionHelper(true);
        // Grandchild inherits @Entity from ParentEntity (two levels up)
        assertEquals(GrandchildEntity.class, arh.getClassForTypeId("inh_parent"));
    }

    // ------------------------------------------------------------------
    // Test hierarchy
    // ------------------------------------------------------------------

    @Entity(collectionName = "inh_parent_coll", typeId = "inh_parent")
    static class ParentEntity {
        String name;
    }

    /** Inherits @Entity from ParentEntity — no own annotation. */
    static class ChildEntity extends ParentEntity {
        String extra;
    }

    /** Grandchild — inherits @Entity through ChildEntity from ParentEntity. */
    static class GrandchildEntity extends ChildEntity {
        String moreData;
    }

    @Embedded(typeId = "inh_embedded_iface")
    interface EmbeddedInterface {}

    /** Implements @Embedded interface. */
    static class EmbeddedInterfaceImpl implements EmbeddedInterface {
        String value;
    }
}
