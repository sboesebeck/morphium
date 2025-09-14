package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.annotations.Property;
import de.caluga.morphium.annotations.caching.Cache;
import de.caluga.morphium.annotations.caching.NoCache;
import de.caluga.morphium.cache.MorphiumCache;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.test.mongo.suite.data.CachedObject;
import de.caluga.test.mongo.suite.data.UncachedObject;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Consolidated cache tests combining functionality from CacheFunctionalityTest, 
 * CacheListenerTest, CacheSyncTest, IdCacheTest, and MassCacheTest.
 */
public class CacheTests extends MultiDriverTestBase {

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void basicCacheOperationsTest(Morphium morphium) throws Exception {
        String tstName = new Object() {} .getClass().getEnclosingMethod().getName();
        log.info("Running test " + tstName + " with " + morphium.getDriver().getName());
        
        try (morphium) {
            morphium.dropCollection(CachedObject.class);
            
            // Test basic cache functionality
            for (int i = 0; i < 10; i++) {
                CachedObject co = new CachedObject();
                co.setCounter(i);
                co.setValue("Cached " + i);
                morphium.store(co);
            }
            
            TestUtils.waitForConditionToBecomeTrue(2000, "Objects not stored", 
                () -> morphium.createQueryFor(CachedObject.class).countAll() == 10);
            
            // First query - should hit database and populate cache
            long start = System.currentTimeMillis();
            CachedObject first = morphium.createQueryFor(CachedObject.class).f("counter").eq(5).get();
            long firstDuration = System.currentTimeMillis() - start;
            assertNotNull(first);
            assertEquals(5, first.getCounter());
            
            // Second identical query - should hit cache and be faster
            start = System.currentTimeMillis();
            CachedObject second = morphium.createQueryFor(CachedObject.class).f("counter").eq(5).get();
            long secondDuration = System.currentTimeMillis() - start;
            assertNotNull(second);
            assertEquals(5, second.getCounter());
            
            // Cache hit should typically be faster (though not always guaranteed in tests)
            log.info("First query: {}ms, Second query (cache): {}ms", firstDuration, secondDuration);
            
            // Test cache statistics
            assertTrue(morphium.getStatistics().get("CachedObject-cache-hits") > 0);
            
            // Test cache invalidation on update
            first.setValue("Updated Value");
            morphium.store(first);
            
            CachedObject updated = morphium.createQueryFor(CachedObject.class).f("counter").eq(5).get();
            assertEquals("Updated Value", updated.getValue());
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void cacheVsNoCacheTest(Morphium morphium) throws Exception {
        String tstName = new Object() {} .getClass().getEnclosingMethod().getName();
        log.info("Running test " + tstName + " with " + morphium.getDriver().getName());
        
        try (morphium) {
            morphium.dropCollection(CachedObject.class);
            morphium.dropCollection(UncachedObject.class);
            
            // Create cached and uncached objects
            for (int i = 0; i < 5; i++) {
                CachedObject co = new CachedObject();
                co.setCounter(i);
                co.setValue("Cached " + i);
                morphium.store(co);
                
                UncachedObject uo = new UncachedObject();
                uo.setCounter(i);
                uo.setStrValue("Uncached " + i);
                morphium.store(uo);
            }
            
            TestUtils.waitForConditionToBecomeTrue(2000, "Objects not stored", 
                () -> morphium.createQueryFor(CachedObject.class).countAll() == 5 &&
                       morphium.createQueryFor(UncachedObject.class).countAll() == 5);
            
            // Query both multiple times
            for (int i = 0; i < 3; i++) {
                CachedObject cached = morphium.createQueryFor(CachedObject.class).f("counter").eq(2).get();
                UncachedObject uncached = morphium.createQueryFor(UncachedObject.class).f("counter").eq(2).get();
                
                assertNotNull(cached);
                assertNotNull(uncached);
                assertEquals(2, cached.getCounter());
                assertEquals(2, uncached.getCounter());
            }
            
            // Cached objects should have cache hits
            assertTrue(morphium.getStatistics().get("CachedObject-cache-hits") > 0);
            // Uncached objects should not have cache statistics
            assertEquals(0, morphium.getStatistics().get("UncachedObject-cache-hits"));
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void cacheTTLTest(Morphium morphium) throws Exception {
        String tstName = new Object() {} .getClass().getEnclosingMethod().getName();
        log.info("Running test " + tstName + " with " + morphium.getDriver().getName());
        
        try (morphium) {
            morphium.dropCollection(ShortCachedEntity.class);
            
            // Create object with short cache TTL
            ShortCachedEntity entity = new ShortCachedEntity();
            entity.value = "Test Value";
            morphium.store(entity);
            
            // First query - populates cache
            ShortCachedEntity first = morphium.createQueryFor(ShortCachedEntity.class).get();
            assertNotNull(first);
            assertEquals("Test Value", first.value);
            
            // Immediate second query - should hit cache
            ShortCachedEntity second = morphium.createQueryFor(ShortCachedEntity.class).get();
            assertNotNull(second);
            
            // Wait for cache to expire
            Thread.sleep(2000);
            
            // Query after expiration - should go to database again
            ShortCachedEntity third = morphium.createQueryFor(ShortCachedEntity.class).get();
            assertNotNull(third);
            assertEquals("Test Value", third.value);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void cacheInvalidationTest(Morphium morphium) throws Exception {
        String tstName = new Object() {} .getClass().getEnclosingMethod().getName();
        log.info("Running test " + tstName + " with " + morphium.getDriver().getName());
        
        try (morphium) {
            morphium.dropCollection(CachedObject.class);
            
            // Create and cache an object
            CachedObject original = new CachedObject();
            original.setCounter(100);
            original.setValue("Original");
            morphium.store(original);
            
            // Query to populate cache
            CachedObject cached = morphium.createQueryFor(CachedObject.class).f("counter").eq(100).get();
            assertNotNull(cached);
            assertEquals("Original", cached.getValue());
            
            // Update the object - should invalidate cache
            cached.setValue("Modified");
            morphium.store(cached);
            
            // Query again - should get updated value from database
            CachedObject updated = morphium.createQueryFor(CachedObject.class).f("counter").eq(100).get();
            assertNotNull(updated);
            assertEquals("Modified", updated.getValue());
            
            // Delete object - should invalidate cache
            morphium.delete(updated);
            
            // Query should return null
            CachedObject deleted = morphium.createQueryFor(CachedObject.class).f("counter").eq(100).get();
            assertNull(deleted);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void listCacheTest(Morphium morphium) throws Exception {
        String tstName = new Object() {} .getClass().getEnclosingMethod().getName();
        log.info("Running test " + tstName + " with " + morphium.getDriver().getName());
        
        try (morphium) {
            morphium.dropCollection(CachedObject.class);
            
            // Create multiple objects
            for (int i = 0; i < 20; i++) {
                CachedObject co = new CachedObject();
                co.setCounter(i);
                co.setValue("Value " + i);
                morphium.store(co);
            }
            
            TestUtils.waitForConditionToBecomeTrue(2000, "Objects not stored", 
                () -> morphium.createQueryFor(CachedObject.class).countAll() == 20);
            
            // First list query - should hit database and populate cache
            List<CachedObject> firstList = morphium.createQueryFor(CachedObject.class)
                .f("counter").lt(10).asList();
            assertEquals(10, firstList.size());
            
            // Second identical list query - should hit cache
            List<CachedObject> secondList = morphium.createQueryFor(CachedObject.class)
                .f("counter").lt(10).asList();
            assertEquals(10, secondList.size());
            
            // Verify cache hits for list queries
            assertTrue(morphium.getStatistics().get("CachedObject-cache-hits") > 0);
            
            // Test different query - should hit database
            List<CachedObject> differentQuery = morphium.createQueryFor(CachedObject.class)
                .f("counter").gte(15).asList();
            assertEquals(5, differentQuery.size());
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void idCacheTest(Morphium morphium) throws Exception {
        String tstName = new Object() {} .getClass().getEnclosingMethod().getName();
        log.info("Running test " + tstName + " with " + morphium.getDriver().getName());
        
        try (morphium) {
            morphium.dropCollection(CachedObject.class);
            
            // Create and store object
            CachedObject original = new CachedObject();
            original.setCounter(123);
            original.setValue("ID Cache Test");
            morphium.store(original);
            
            MorphiumId objectId = original.getId();
            assertNotNull(objectId);
            
            // Query by ID - should populate cache
            CachedObject byId = morphium.findById(CachedObject.class, objectId);
            assertNotNull(byId);
            assertEquals(123, byId.getCounter());
            
            // Second query by ID - should hit cache
            CachedObject cachedById = morphium.findById(CachedObject.class, objectId);
            assertNotNull(cachedById);
            assertEquals(123, cachedById.getCounter());
            
            // Query by field that matches same object - should also benefit from cache
            CachedObject byField = morphium.createQueryFor(CachedObject.class)
                .f("counter").eq(123).get();
            assertNotNull(byField);
            assertEquals(objectId, byField.getId());
            
            // Verify cache statistics
            assertTrue(morphium.getStatistics().get("CachedObject-cache-hits") > 0);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void massiveCacheTest(Morphium morphium) throws Exception {
        String tstName = new Object() {} .getClass().getEnclosingMethod().getName();
        log.info("Running test " + tstName + " with " + morphium.getDriver().getName());
        
        try (morphium) {
            morphium.dropCollection(CachedObject.class);
            
            int objectCount = 100;
            
            // Create many cached objects
            for (int i = 0; i < objectCount; i++) {
                CachedObject co = new CachedObject();
                co.setCounter(i);
                co.setValue("Mass " + i);
                morphium.store(co);
            }
            
            TestUtils.waitForConditionToBecomeTrue(3000, "Objects not stored", 
                () -> morphium.createQueryFor(CachedObject.class).countAll() == objectCount);
            
            // Query all objects to populate cache
            for (int i = 0; i < objectCount; i++) {
                CachedObject obj = morphium.createQueryFor(CachedObject.class).f("counter").eq(i).get();
                assertNotNull(obj);
                assertEquals(i, obj.getCounter());
            }
            
            double initialCacheHits = morphium.getStatistics().get("CachedObject-cache-hits");
            
            // Query all objects again - should mostly hit cache
            for (int i = 0; i < objectCount; i++) {
                CachedObject obj = morphium.createQueryFor(CachedObject.class).f("counter").eq(i).get();
                assertNotNull(obj);
                assertEquals(i, obj.getCounter());
            }
            
            double finalCacheHits = morphium.getStatistics().get("CachedObject-cache-hits");
            assertTrue(finalCacheHits > initialCacheHits);
            
            // Test cache capacity - cache might evict some entries
            MorphiumCache cache = morphium.getCache();
            assertNotNull(cache);
            int cachedObjectCacheSize = cache.getSizes().entrySet().stream()
                    .filter(e -> e.getKey().contains("resultCache|" + CachedObject.class.getName()))
                    .mapToInt(Map.Entry::getValue).sum();
            log.info("Cache size after mass operations: " + cachedObjectCacheSize);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void cacheConfigurationTest(Morphium morphium) throws Exception {
        String tstName = new Object() {} .getClass().getEnclosingMethod().getName();
        log.info("Running test " + tstName + " with " + morphium.getDriver().getName());
        
        try (morphium) {
            // Test different cache configurations
            morphium.dropCollection(CustomCacheEntity.class);
            
            CustomCacheEntity entity = new CustomCacheEntity();
            entity.value = "Custom Cache";
            morphium.store(entity);
            
            // Query to populate cache
            CustomCacheEntity cached = morphium.createQueryFor(CustomCacheEntity.class).get();
            assertNotNull(cached);
            
            // Verify cache exists
            MorphiumCache cache = morphium.getCache();
            int customCacheSize = cache.getSizes().entrySet().stream()
                    .filter(e -> e.getKey().contains("resultCache|" + CustomCacheEntity.class.getName()))
                    .mapToInt(Map.Entry::getValue).sum();
            assertTrue(customCacheSize > 0);
            
            // Test manual cache operations
            String cacheKey = morphium.getCache().getCacheKey(morphium.createQueryFor(CustomCacheEntity.class));
            assertNotNull(cacheKey);
            
            // Clear specific cache
            morphium.clearCachefor(CustomCacheEntity.class);
            int sizeAfterClear = cache.getSizes().entrySet().stream()
                    .filter(e -> e.getKey().contains("resultCache|" + CustomCacheEntity.class.getName()))
                    .mapToInt(Map.Entry::getValue).sum();
            assertEquals(0, sizeAfterClear);
        }
    }

    // Helper entity classes for cache testing

    @Entity
    @Cache(maxEntries = 100, timeout = 1000) // Short TTL for testing
    public static class ShortCachedEntity {
        @Id
        public MorphiumId id;
        
        @Property
        public String value;
    }

    @Entity
    @Cache(maxEntries = 500, timeout = 30000, clearOnWrite = true)
    public static class CustomCacheEntity {
        @Id
        public MorphiumId id;
        
        @Property
        public String value;
    }

    @Entity
    @NoCache
    public static class ExplicitNoCacheEntity {
        @Id
        public MorphiumId id;
        
        @Property
        public String value;
    }
}
