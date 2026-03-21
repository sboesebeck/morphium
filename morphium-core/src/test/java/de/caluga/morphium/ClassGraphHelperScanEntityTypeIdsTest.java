package de.caluga.morphium;

import de.caluga.morphium.annotations.Entity;
import de.caluga.test.mongo.suite.data.EmbeddedObject;
import de.caluga.test.mongo.suite.data.UncachedObject;
import org.junit.jupiter.api.*;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for {@link ClassGraphHelper#scanEntityTypeIds()} — specifically verifies
 * that the fix for H1 (default typeId "." not filtered) and H2 (FQCN mapping
 * outside inner parameter loop) work correctly.
 * <p>
 * Uses existing test entities from the classpath that are known to be discoverable
 * by ClassGraph (top-level classes, not inner static classes).
 */
@Tag("core")
class ClassGraphHelperScanEntityTypeIdsTest {

    @Test
    @DisplayName("H1: Default typeId '.' is NOT added as a key in the typeId map")
    void scanEntityTypeIds_defaultTypeIdDotIsNotAKey() {
        Map<String, String> typeIds = ClassGraphHelper.scanEntityTypeIds();
        assertFalse(typeIds.containsKey("."),
            "The default typeId '.' should never appear as a key in the typeId map. "
            + "Keys found: " + typeIds.keySet().stream()
                .filter(k -> k.length() < 10).toList());
    }

    @Test
    @DisplayName("H1: Custom typeId 'uc' is added as a key (UncachedObject)")
    void scanEntityTypeIds_customTypeIdIsRegistered() {
        // UncachedObject has @Entity(typeId = "uc")
        Map<String, String> typeIds = ClassGraphHelper.scanEntityTypeIds();
        assertTrue(typeIds.containsKey("uc"),
            "Custom typeId 'uc' from UncachedObject should be in the map");
        assertEquals(UncachedObject.class.getName(), typeIds.get("uc"));
    }

    @Test
    @DisplayName("H1: Custom typeId 'embedded' is added for @Embedded class")
    void scanEntityTypeIds_embeddedCustomTypeIdIsRegistered() {
        // EmbeddedObject has @Embedded(typeId = "embedded")
        Map<String, String> typeIds = ClassGraphHelper.scanEntityTypeIds();
        assertTrue(typeIds.containsKey("embedded"),
            "Custom typeId 'embedded' from EmbeddedObject should be in the map");
        assertEquals(EmbeddedObject.class.getName(), typeIds.get("embedded"));
    }

    @Test
    @DisplayName("H2: FQCN→FQCN mapping is always present for entity with custom typeId")
    void scanEntityTypeIds_fqcnMappingPresentForCustomTypeId() {
        Map<String, String> typeIds = ClassGraphHelper.scanEntityTypeIds();
        String fqcn = UncachedObject.class.getName();
        assertTrue(typeIds.containsKey(fqcn),
            "FQCN→FQCN mapping must be present: " + fqcn);
        assertEquals(fqcn, typeIds.get(fqcn));
    }

    @Test
    @DisplayName("H2: FQCN→FQCN mapping is present for @Embedded with custom typeId")
    void scanEntityTypeIds_fqcnMappingPresentForEmbedded() {
        Map<String, String> typeIds = ClassGraphHelper.scanEntityTypeIds();
        String fqcn = EmbeddedObject.class.getName();
        assertTrue(typeIds.containsKey(fqcn),
            "FQCN→FQCN mapping must be present for @Embedded: " + fqcn);
        assertEquals(fqcn, typeIds.get(fqcn));
    }

    @Test
    @DisplayName("H2: FQCN→FQCN mapping is present for entities with default typeId")
    void scanEntityTypeIds_fqcnMappingPresentForDefaultTypeId() {
        // Find an entity with default typeId (no custom typeId set)
        // Many entities in the test classpath use the default "."
        Map<String, String> typeIds = ClassGraphHelper.scanEntityTypeIds();
        // The result should contain many entities; at minimum the ones with custom typeIds
        // plus their FQCN→FQCN mappings
        assertTrue(typeIds.size() > 10,
            "Should find many entities on the classpath, found: " + typeIds.size());
        // Every value should be a FQCN (no "." values)
        for (Map.Entry<String, String> entry : typeIds.entrySet()) {
            assertNotEquals(".", entry.getKey(),
                "Key '.' should not appear in the map");
            assertTrue(entry.getValue().contains("."),
                "All values should be FQCNs: " + entry.getValue());
        }
    }

    @Test
    @DisplayName("Consistency: ClassGraph and EntityRegistry agree on custom typeIds")
    void scanEntityTypeIds_consistentWithEntityRegistry() {
        Map<String, String> scanned = ClassGraphHelper.scanEntityTypeIds();

        // Pre-register the same entities via EntityRegistry
        EntityRegistry.clear();
        EntityRegistry.preRegisterEntities(List.of(UncachedObject.class, EmbeddedObject.class));
        Map<String, String> preRegistered = EntityRegistry.getPreRegisteredTypeIds();

        try {
            // Both should agree on custom typeIds
            assertEquals(preRegistered.get("uc"), scanned.get("uc"),
                "TypeId 'uc' mapping should be consistent");
            assertEquals(preRegistered.get("embedded"), scanned.get("embedded"),
                "TypeId 'embedded' mapping should be consistent");

            // Neither should have "." as a key
            assertFalse(scanned.containsKey("."));
            assertFalse(preRegistered.containsKey("."));
        } finally {
            EntityRegistry.clear();
        }
    }
}
