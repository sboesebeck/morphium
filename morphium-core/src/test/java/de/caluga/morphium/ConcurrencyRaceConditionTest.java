package de.caluga.morphium;

import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import org.junit.jupiter.api.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for concurrency race conditions fixed in the ClassGraph-optional review:
 * <ul>
 *   <li>K1: NPE race in ObjectMapperImpl due to double volatile read of cachedClassByCollectionName</li>
 *   <li>H3: Race in AnnotationAndReflectionHelper.classNameByType (not volatile, unsynchronized init)</li>
 * </ul>
 *
 * These tests create concurrent pressure on the static caches to verify the fixes
 * prevent NPE and inconsistent state.
 */
@Tag("core")
class ConcurrencyRaceConditionTest {

    @BeforeEach
    void setup() {
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
    @DisplayName("K1: Concurrent ObjectMapperImpl creation + clearEntityCache does not cause NPE")
    void objectMapperImpl_concurrentClearDuringConstruction_noNpe() throws Exception {
        // Pre-register entities so the constructor uses the pre-registration path
        EntityRegistry.preRegisterEntities(List.of(RaceTestEntity.class));

        int iterations = 200;
        AtomicReference<Throwable> failure = new AtomicReference<>();
        ExecutorService pool = Executors.newFixedThreadPool(4);

        try {
            for (int i = 0; i < iterations; i++) {
                // Reset caches for this iteration
                ObjectMapperImpl.clearEntityCache();

                CountDownLatch latch = new CountDownLatch(1);
                Future<?> constructor1 = pool.submit(() -> {
                    try {
                        latch.await();
                        new ObjectMapperImpl();
                    } catch (Throwable t) {
                        failure.compareAndSet(null, t);
                    }
                });
                Future<?> constructor2 = pool.submit(() -> {
                    try {
                        latch.await();
                        new ObjectMapperImpl();
                    } catch (Throwable t) {
                        failure.compareAndSet(null, t);
                    }
                });
                Future<?> clearer = pool.submit(() -> {
                    try {
                        latch.await();
                        ObjectMapperImpl.clearEntityCache();
                    } catch (Throwable t) {
                        failure.compareAndSet(null, t);
                    }
                });

                latch.countDown();
                constructor1.get(5, TimeUnit.SECONDS);
                constructor2.get(5, TimeUnit.SECONDS);
                clearer.get(5, TimeUnit.SECONDS);
            }
        } finally {
            pool.shutdownNow();
        }

        assertNull(failure.get(),
            "No NPE or other failure should occur during concurrent ObjectMapper creation + clear: "
            + (failure.get() != null ? failure.get().toString() : ""));
    }

    @Test
    @DisplayName("H3: Concurrent AnnotationAndReflectionHelper creation is safe")
    void annotationHelper_concurrentConstruction_noRace() throws Exception {
        EntityRegistry.preRegisterEntities(List.of(RaceTestEntity.class));

        int threadCount = 8;
        int iterations = 100;
        AtomicReference<Throwable> failure = new AtomicReference<>();
        AtomicInteger successCount = new AtomicInteger(0);
        ExecutorService pool = Executors.newFixedThreadPool(threadCount);

        try {
            for (int i = 0; i < iterations; i++) {
                AnnotationAndReflectionHelper.clearTypeIdCache();
                CountDownLatch latch = new CountDownLatch(1);
                List<Future<?>> futures = new ArrayList<>();

                for (int t = 0; t < threadCount; t++) {
                    futures.add(pool.submit(() -> {
                        try {
                            latch.await();
                            AnnotationAndReflectionHelper arh = new AnnotationAndReflectionHelper(true);
                            // Verify the helper works correctly after concurrent init
                            Class<?> resolved = arh.getClassForTypeId("race_test_entity");
                            assertEquals(RaceTestEntity.class, resolved);
                            successCount.incrementAndGet();
                        } catch (Throwable t1) {
                            failure.compareAndSet(null, t1);
                        }
                    }));
                }

                latch.countDown();
                for (Future<?> f : futures) {
                    f.get(5, TimeUnit.SECONDS);
                }
            }
        } finally {
            pool.shutdownNow();
        }

        assertNull(failure.get(),
            "Concurrent AnnotationAndReflectionHelper creation should be safe: "
            + (failure.get() != null ? failure.get().toString() : ""));
        assertEquals(threadCount * iterations, successCount.get(),
            "All threads should have resolved the typeId successfully");
    }

    @Test
    @DisplayName("H3: clearTypeIdCache + concurrent getClassForTypeId does not throw NPE")
    void annotationHelper_clearDuringTypeIdLookup_noNpe() throws Exception {
        EntityRegistry.preRegisterEntities(List.of(RaceTestEntity.class));
        // Ensure the cache is populated
        AnnotationAndReflectionHelper.clearTypeIdCache();
        new AnnotationAndReflectionHelper(true);

        int iterations = 500;
        AtomicReference<Throwable> failure = new AtomicReference<>();
        ExecutorService pool = Executors.newFixedThreadPool(4);

        try {
            for (int i = 0; i < iterations; i++) {
                CountDownLatch latch = new CountDownLatch(1);
                AnnotationAndReflectionHelper arh = new AnnotationAndReflectionHelper(true);

                Future<?> reader = pool.submit(() -> {
                    try {
                        latch.await();
                        // This should not NPE even if clearTypeIdCache is called concurrently
                        try {
                            arh.getClassForTypeId("race_test_entity");
                        } catch (ClassNotFoundException e) {
                            // Acceptable: cache cleared between check and resolve
                        }
                    } catch (NullPointerException e) {
                        failure.compareAndSet(null, e);
                    } catch (Throwable t) {
                        // Other exceptions are acceptable
                    }
                });
                Future<?> clearer = pool.submit(() -> {
                    try {
                        latch.await();
                        AnnotationAndReflectionHelper.clearTypeIdCache();
                    } catch (Throwable t) {
                        failure.compareAndSet(null, t);
                    }
                });

                latch.countDown();
                reader.get(5, TimeUnit.SECONDS);
                clearer.get(5, TimeUnit.SECONDS);
            }
        } finally {
            pool.shutdownNow();
        }

        assertNull(failure.get(),
            "No NPE should occur during concurrent typeId lookup + clear");
    }

    // ------------------------------------------------------------------
    // Test data class
    // ------------------------------------------------------------------

    @Entity(collectionName = "race_test", typeId = "race_test_entity")
    static class RaceTestEntity {
        String name;
    }
}
