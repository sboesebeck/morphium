package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.AnnotationAndReflectionHelper;
import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.annotations.Capped;
import de.caluga.morphium.annotations.Embedded;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for {@link AnnotationAndReflectionHelper#classForName(String)} and
 * {@link AnnotationAndReflectionHelper#getClassForTypeId(String)}.
 *
 * <p>Verifies that class loading uses the thread context classloader first,
 * which is required for frameworks with isolated classloaders (e.g. Quarkus).
 */
@Tag("core")
public class ClassForNameTest {

    private AnnotationAndReflectionHelper helper;
    private Morphium morphium;

    @BeforeEach
    public void setup() {
        helper = new AnnotationAndReflectionHelper(true);
    }

    @AfterEach
    public void tearDown() {
        if (morphium != null) {
            morphium.close();
            morphium = null;
        }
    }

    // ------------------------------------------------------------------
    // classForName – basic resolution
    // ------------------------------------------------------------------

    @Test
    public void classForName_loadsClassFromSamePackage() throws ClassNotFoundException {
        Class<?> cls = AnnotationAndReflectionHelper.classForName(
                "de.caluga.test.mongo.suite.base.ClassForNameTest");
        assertEquals(ClassForNameTest.class, cls);
    }

    @Test
    public void classForName_loadsJdkClass() throws ClassNotFoundException {
        Class<?> cls = AnnotationAndReflectionHelper.classForName("java.util.HashMap");
        assertEquals(java.util.HashMap.class, cls);
    }

    @Test
    public void classForName_throwsForNonexistentClass() {
        assertThrows(ClassNotFoundException.class,
                () -> AnnotationAndReflectionHelper.classForName("com.does.not.Exist"));
    }

    // ------------------------------------------------------------------
    // classForName – context classloader preference
    // ------------------------------------------------------------------

    @Test
    public void classForName_prefersContextClassLoader() throws Exception {
        // Create an isolated classloader that can only see classes from the test classpath.
        // This simulates a Quarkus-style isolated classloader scenario.
        ClassLoader original = Thread.currentThread().getContextClassLoader();
        ClassLoader isolated = new TrackingClassLoader(original);

        Thread.currentThread().setContextClassLoader(isolated);
        try {
            Class<?> cls = AnnotationAndReflectionHelper.classForName(
                    SampleEmbedded.class.getName());
            assertNotNull(cls);
            assertEquals(SampleEmbedded.class.getName(), cls.getName());
            // The class was loaded through our tracking classloader
            assertTrue(((TrackingClassLoader) isolated).wasUsed(),
                    "Context classloader should have been consulted");
        } finally {
            Thread.currentThread().setContextClassLoader(original);
        }
    }

    @Test
    public void classForName_fallsBackWhenContextClassLoaderCannotFindClass() throws Exception {
        ClassLoader original = Thread.currentThread().getContextClassLoader();
        // Classloader that always throws ClassNotFoundException
        ClassLoader failing = new ClassLoader(null) {
            @Override
            protected Class<?> findClass(String name) throws ClassNotFoundException {
                throw new ClassNotFoundException("deliberately failing: " + name);
            }

            @Override
            public Class<?> loadClass(String name) throws ClassNotFoundException {
                throw new ClassNotFoundException("deliberately failing: " + name);
            }
        };

        Thread.currentThread().setContextClassLoader(failing);
        try {
            // Should fall back to Class.forName() and still find JDK classes
            Class<?> cls = AnnotationAndReflectionHelper.classForName("java.lang.String");
            assertEquals(String.class, cls);
        } finally {
            Thread.currentThread().setContextClassLoader(original);
        }
    }

    @Test
    public void classForName_worksWhenContextClassLoaderIsNull() throws Exception {
        ClassLoader original = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(null);
        try {
            Class<?> cls = AnnotationAndReflectionHelper.classForName("java.lang.Integer");
            assertEquals(Integer.class, cls);
        } finally {
            Thread.currentThread().setContextClassLoader(original);
        }
    }

    // ------------------------------------------------------------------
    // getClassForTypeId – integration with classForName
    // ------------------------------------------------------------------

    @Test
    public void getClassForTypeId_resolvesFullClassName() throws ClassNotFoundException {
        Class<?> cls = helper.getClassForTypeId(SampleEntity.class.getName());
        assertEquals(SampleEntity.class, cls);
    }

    @Test
    public void getClassForTypeId_resolvesEmbeddedClassName() throws ClassNotFoundException {
        Class<?> cls = helper.getClassForTypeId(SampleEmbedded.class.getName());
        assertEquals(SampleEmbedded.class, cls);
    }

    @Test
    public void getClassForTypeId_usesContextClassLoader() throws Exception {
        ClassLoader original = Thread.currentThread().getContextClassLoader();
        TrackingClassLoader tracking = new TrackingClassLoader(original);
        Thread.currentThread().setContextClassLoader(tracking);
        try {
            Class<?> cls = helper.getClassForTypeId(SampleEmbedded.class.getName());
            assertNotNull(cls);
            assertTrue(tracking.wasUsed(),
                    "getClassForTypeId should delegate to classForName which uses the context classloader");
        } finally {
            Thread.currentThread().setContextClassLoader(original);
        }
    }

    @Test
    public void getTypeIdForClassName_usesContextClassLoader() throws Exception {
        ClassLoader original = Thread.currentThread().getContextClassLoader();
        TrackingClassLoader tracking = new TrackingClassLoader(original);
        Thread.currentThread().setContextClassLoader(tracking);
        try {
            String typeId = helper.getTypeIdForClassName(SampleEntity.class.getName());
            assertNotNull(typeId);
            assertTrue(tracking.wasUsed(),
                    "getTypeIdForClassName should delegate to classForName which uses the context classloader");
        } finally {
            Thread.currentThread().setContextClassLoader(original);
        }
    }

    // ------------------------------------------------------------------
    // checkCapped / checkIndices – context classloader in Morphium core
    // ------------------------------------------------------------------

    /**
     * Verifies that {@link Morphium#checkCapped()} uses the thread context classloader
     * to load entity classes found via ClassGraph.
     *
     * <p>In Quarkus dev mode application classes are loaded by an isolated
     * QuarkusClassLoader that is set as the thread context classloader.
     * Without this fix (bare {@code Class.forName}), those classes would produce a
     * {@code ClassNotFoundException} because the Morphium library JAR's own classloader
     * has no visibility into the application's isolated classloader.
     * With the fix ({@code AnnotationAndReflectionHelper.classForName}), the thread
     * context classloader is tried first and the class is found.
     */
    @Test
    public void checkCapped_usesContextClassLoader() throws Exception {
        MorphiumConfig cfg = new MorphiumConfig("classloader_capped_test_db", 10, 10_000, 1_000);
        cfg.driverSettings().setDriverName(InMemoryDriver.driverName);
        morphium = new Morphium(cfg);

        ClassLoader original = Thread.currentThread().getContextClassLoader();
        TrackingClassLoader tracking = new TrackingClassLoader(original);
        Thread.currentThread().setContextClassLoader(tracking);
        try {
            Map<Class<?>, Map<String, Integer>> result = morphium.checkCapped();
            // Result may be empty (no @Capped entity in test scope needs capping), but the call
            // must not throw and must have consulted the thread context classloader.
            assertNotNull(result);
            assertTrue(tracking.wasUsed(),
                    "checkCapped() must use the thread context classloader (AnnotationAndReflectionHelper.classForName)");
        } finally {
            Thread.currentThread().setContextClassLoader(original);
        }
    }

    /**
     * Verifies that {@link Morphium#checkIndices(io.github.classgraph.ClassInfoList.ClassInfoFilter)}
     * uses the thread context classloader for the same reason as {@link #checkCapped_usesContextClassLoader()}.
     */
    @Test
    public void checkIndices_usesContextClassLoader() throws Exception {
        MorphiumConfig cfg = new MorphiumConfig("classloader_indices_test_db", 10, 10_000, 1_000);
        cfg.driverSettings().setDriverName(InMemoryDriver.driverName);
        morphium = new Morphium(cfg);

        ClassLoader original = Thread.currentThread().getContextClassLoader();
        TrackingClassLoader tracking = new TrackingClassLoader(original);
        Thread.currentThread().setContextClassLoader(tracking);
        try {
            // null filter = check all entities
            assertDoesNotThrow(() -> morphium.checkIndices(null));
            assertTrue(tracking.wasUsed(),
                    "checkIndices() must use the thread context classloader (AnnotationAndReflectionHelper.classForName)");
        } finally {
            Thread.currentThread().setContextClassLoader(original);
        }
    }

    // ------------------------------------------------------------------
    // Test data classes
    // ------------------------------------------------------------------

    @Capped(maxEntries = 100, maxSize = 1024)
    @Entity(collectionName = "sample_capped_entity")
    public static class SampleCappedEntity {
        @Id
        public MorphiumId id;
        public String msg;
    }

    @Entity
    public static class SampleEntity {
        @Id
        public MorphiumId id;
        public String name;
    }

    @Embedded
    public static class SampleEmbedded {
        public String value;
    }

    // ------------------------------------------------------------------
    // Helper classloader that tracks whether it was consulted
    // ------------------------------------------------------------------

    private static class TrackingClassLoader extends ClassLoader {
        private volatile boolean used = false;

        TrackingClassLoader(ClassLoader parent) {
            super(parent);
        }

        @Override
        public Class<?> loadClass(String name) throws ClassNotFoundException {
            used = true;
            return super.loadClass(name);
        }

        boolean wasUsed() {
            return used;
        }
    }
}
