package de.caluga.morphium;

import de.caluga.morphium.annotations.Entity;
import org.junit.jupiter.api.*;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for {@link ClassGraphHelper} — verifies that the ClassGraph scan methods
 * return correct results when ClassGraph is on the classpath (test scope).
 * <p>
 * These tests ensure the refactoring (extracting scan logic into ClassGraphHelper)
 * preserved the original behaviour of the inline ClassGraph code.
 */
@Tag("core")
class ClassGraphHelperTest {

    @Test
    void isAvailable_trueInTestScope() {
        assertTrue(ClassGraphHelper.isAvailable(),
            "ClassGraph should be available in test scope");
    }

    @Test
    void scanEntityTypeIds_findsEntitiesOnClasspath() {
        Map<String, String> typeIds = ClassGraphHelper.scanEntityTypeIds();
        assertFalse(typeIds.isEmpty(), "Should find at least some @Entity/@Embedded classes");
        // Every value should be a FQCN
        for (String fqcn : typeIds.values()) {
            assertTrue(fqcn.contains("."), "Values should be FQCNs: " + fqcn);
        }
    }

    @Test
    void scanEntityCollections_findsEntitiesOnClasspath() {
        // Use a simple collection name resolver (just the class simple name)
        Map<String, Class<?>> collections = ClassGraphHelper.scanEntityCollections(
            cls -> {
                Entity ann = cls.getAnnotation(Entity.class);
                return (ann != null && !ann.collectionName().equals("."))
                        ? ann.collectionName()
                        : cls.getSimpleName().toLowerCase();
            });
        assertFalse(collections.isEmpty(), "Should find at least some @Entity classes");
    }

    @Test
    void scanForAnnotatedClasses_findsEntityClasses() {
        List<Class<?>> entities = ClassGraphHelper.scanForAnnotatedClasses(Entity.class.getName());
        assertFalse(entities.isEmpty(), "Should find at least some @Entity classes");
        for (Class<?> cls : entities) {
            // Should not contain JDK/framework classes (filtered out)
            assertFalse(cls.getName().startsWith("sun."));
            assertFalse(cls.getName().startsWith("javax."));
        }
    }

    @Test
    void scanForAnnotatedClasses_returnsEmptyForUnknownAnnotation() {
        List<Class<?>> result = ClassGraphHelper.scanForAnnotatedClasses(
            "com.nonexistent.Annotation");
        assertTrue(result.isEmpty());
    }

    @Test
    void scanForDriverImpl_findsInMemoryDriver() {
        // InMemoryDriver has @Driver annotation
        Class<?> driverClass = ClassGraphHelper.scanForDriverImpl("InMemDriver");
        assertNotNull(driverClass, "Should find InMemoryDriver via @Driver annotation");
        assertEquals("de.caluga.morphium.driver.inmem.InMemoryDriver", driverClass.getName());
    }

    @Test
    void scanForDriverImpl_returnsNullForUnknown() {
        Class<?> result = ClassGraphHelper.scanForDriverImpl("NonExistentDriver_" + System.nanoTime());
        assertNull(result);
    }

    @Test
    void scanForMessagingImpl_findsStandardMessaging() {
        // SingleCollectionMessaging has @Messaging(name="StandardMessaging")
        Class<?> msgClass = ClassGraphHelper.scanForMessagingImpl("StandardMessaging");
        assertNotNull(msgClass, "Should find StandardMessaging via @Messaging annotation");
    }

    @Test
    void scanForMessagingImpl_returnsNullForUnknown() {
        Class<?> result = ClassGraphHelper.scanForMessagingImpl("NonExistent_" + System.nanoTime());
        assertNull(result);
    }
}
