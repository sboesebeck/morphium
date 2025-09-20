package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.annotations.Property;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.query.Query;
import de.caluga.test.mongo.suite.data.CachedObject;
import de.caluga.test.mongo.suite.data.UncachedObject;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Consolidated core functionality tests combining basic CRUD operations.
 * Consolidates functionality from UpdateTest, DeleteTest, MultiUpdateTests,
 * and core CRUD operations from BasicFunctionalityTest.
 */
@Tag("core")
public class CoreFunctionalityTests extends MultiDriverTestBase {

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void basicCreateReadTest(Morphium morphium) throws Exception {
        String tstName = new Object() {} .getClass().getEnclosingMethod().getName();
        log.info("Running test " + tstName + " with " + morphium.getDriver().getName());

        try (morphium) {
            // Test basic create and read operations
            createUncachedObjects(morphium, 10);
            TestUtils.waitForConditionToBecomeTrue(2000, "Objects not created",
                                                   () -> TestUtils.countUC(morphium) == 10);

            // Test single object retrieval
            UncachedObject single = morphium.createQueryFor(UncachedObject.class).get();
            assertNotNull(single);
            assertNotNull(single.getMorphiumId());
            assertTrue(single.getCounter() > 0);

            // Test list retrieval
            List<UncachedObject> list = morphium.createQueryFor(UncachedObject.class).asList();
            assertEquals(10, list.size());

            // Test count
            long count = morphium.createQueryFor(UncachedObject.class).countAll();
            assertEquals(10, count);

            // Test find by field
            UncachedObject found = morphium.createQueryFor(UncachedObject.class)
                                           .f("counter").eq(single.getCounter()).get();
            assertNotNull(found);
            assertEquals(single.getMorphiumId(), found.getMorphiumId());
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void basicUpdateTest(Morphium morphium) throws Exception {
        String tstName = new Object() {} .getClass().getEnclosingMethod().getName();
        log.info("Running test " + tstName + " with " + morphium.getDriver().getName());

        try (morphium) {
            morphium.dropCollection(UncachedMultipleCounter.class);

            // Create test objects
            for (int i = 1; i <= 10; i++) {
                UncachedMultipleCounter o = new UncachedMultipleCounter();
                o.setCounter(i);
                o.setStrValue("Initial " + i);
                o.setCounter2((double) i / 2.0);
                morphium.store(o);
            }
            Thread.sleep(200);

            // Test single field update
            Query<UncachedMultipleCounter> q = morphium.createQueryFor(UncachedMultipleCounter.class);
            q.f("counter").eq(5);
            morphium.set(q, "strValue", "Updated Value");

            UncachedMultipleCounter updated = morphium.createQueryFor(UncachedMultipleCounter.class)
                                              .f("counter").eq(5).get();
            assertEquals("Updated Value", updated.getStrValue());

            // Test increment operation
            morphium.inc(morphium.createQueryFor(UncachedMultipleCounter.class).f("counter").eq(1),
                         "counter", 10);

            UncachedMultipleCounter incremented = morphium.createQueryFor(UncachedMultipleCounter.class)
                                                  .f("counter").eq(11).get();
            assertNotNull(incremented);
            assertEquals(11, incremented.getCounter());

            // Test multiple field update
            Map<String, Object> updates = new HashMap<>();
            updates.put("strValue", "Multi Update");
            updates.put("counter2", 99.9);
            morphium.set(morphium.createQueryFor(UncachedMultipleCounter.class)
                         .f("counter").lt(5), updates, false, true);

            List<UncachedMultipleCounter> multiUpdated = morphium.createQueryFor(UncachedMultipleCounter.class)
                .f("strValue").eq("Multi Update").asList();
            assertTrue(multiUpdated.size() > 0);
            for (UncachedMultipleCounter obj : multiUpdated) {
                assertEquals("Multi Update", obj.getStrValue());
                assertEquals(99.9, obj.getCounter2(), 0.001);
            }
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void basicDeleteTest(Morphium morphium) throws Exception {
        String tstName = new Object() {} .getClass().getEnclosingMethod().getName();
        log.info("Running test " + tstName + " with " + morphium.getDriver().getName());

        try (morphium) {
            // Test single object delete
            createUncachedObjects(morphium, 10);
            TestUtils.waitForConditionToBecomeTrue(2000, "Objects not created",
                                                   () -> TestUtils.countUC(morphium) == 10);

            UncachedObject toDelete = morphium.createQueryFor(UncachedObject.class).get();
            log.info("First deletion - will delete object with counter: " + toDelete.getCounter());
            morphium.delete(toDelete);

            TestUtils.waitForConditionToBecomeTrue(2000, "Object not deleted",
                                                   () -> TestUtils.countUC(morphium) == 9);

            // Verify object is actually gone
            UncachedObject shouldBeNull = morphium.createQueryFor(UncachedObject.class)
                                                  .f(UncachedObject.Fields.morphiumId).eq(toDelete.getMorphiumId()).get();
            assertNull(shouldBeNull);

            // Test query-based delete
            long initialCount = TestUtils.countUC(morphium);
            morphium.delete(morphium.createQueryFor(UncachedObject.class).f("counter").eq(2));

            TestUtils.waitForConditionToBecomeTrue(2000, "Query delete failed",
                                                   () -> TestUtils.countUC(morphium) == initialCount - 1);

            // Test multiple delete
            long countBeforeMultiDelete = TestUtils.countUC(morphium);
            log.info("Count before multiple delete: " + countBeforeMultiDelete);

            // Debug: show all remaining objects
            var allObjects = morphium.createQueryFor(UncachedObject.class).asList();
            log.info("All remaining objects: " + allObjects.size());
            for (var obj : allObjects) {
                log.info("Remaining object with counter: " + obj.getCounter());
            }

            var toDeleteList = morphium.createQueryFor(UncachedObject.class).f("counter").lt(5).asList();
            log.info("Objects to delete: " + toDeleteList.size());
            for (var obj : toDeleteList) {
                log.info("Will delete object with counter: " + obj.getCounter());
            }
            morphium.delete(morphium.createQueryFor(UncachedObject.class).f("counter").lt(5));
            long countAfterMultiDelete = TestUtils.countUC(morphium);
            log.info("Count after multiple delete: " + countAfterMultiDelete);
            TestUtils.waitForConditionToBecomeTrue(2000, "Multiple delete failed",
                                                   () -> TestUtils.countUC(morphium) < 5);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void batchOperationsTest(Morphium morphium) throws Exception {
        String tstName = new Object() {} .getClass().getEnclosingMethod().getName();
        log.info("Running test " + tstName + " with " + morphium.getDriver().getName());

        try (morphium) {
            morphium.dropCollection(UncachedObject.class);

            // Test batch insert
            List<UncachedObject> batch = new ArrayList<>();
            for (int i = 1; i <= 100; i++) {
                UncachedObject o = new UncachedObject();
                o.setCounter(i);
                o.setStrValue("Batch " + i);
                batch.add(o);
            }

            morphium.storeList(batch);
            TestUtils.waitForConditionToBecomeTrue(3000, "Batch insert failed",
                                                   () -> TestUtils.countUC(morphium) == 100);

            // Test batch update
            morphium.set(morphium.createQueryFor(UncachedObject.class).f("counter").mod(2, 0),
                         "strValue", "Even Number", false, true);

            long evenCount = morphium.createQueryFor(UncachedObject.class)
                             .f("strValue").eq("Even Number").countAll();
            assertEquals(50, evenCount);

            // Test find and modify operations (using set on query to simulate)
            morphium.set(morphium.createQueryFor(UncachedObject.class).f("counter").eq(1),
                         "strValue", "Found and Modified");

            UncachedObject verified = morphium.createQueryFor(UncachedObject.class)
                                      .f("counter").eq(1).get();
            assertEquals("Found and Modified", verified.getStrValue());
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void cachedObjectOperationsTest(Morphium morphium) throws Exception {
        String tstName = new Object() {} .getClass().getEnclosingMethod().getName();
        log.info("Running test " + tstName + " with " + morphium.getDriver().getName());

        try (morphium) {
            // Test cached object operations
            for (int i = 1; i <= 10; i++) {
                CachedObject co = new CachedObject();
                co.setCounter(i);
                co.setValue("Cached " + i);
                morphium.store(co);
            }

            TestUtils.waitForConditionToBecomeTrue(2000, "Cached objects not created",
                                                   () -> morphium.createQueryFor(CachedObject.class).countAll() == 10);

            // Test cache hit by retrieving same object twice
            CachedObject first = morphium.createQueryFor(CachedObject.class).f("counter").eq(5).get();
            assertNotNull(first);

            CachedObject second = morphium.createQueryFor(CachedObject.class).f("counter").eq(5).get();
            assertNotNull(second);
            // Note: Depending on cache implementation, these might be the same instance

            // Test cached object update
            first.setValue("Updated Cached Value");
            morphium.store(first);

            // Verify update
            CachedObject updated = morphium.createQueryFor(CachedObject.class)
                                   .f("counter").eq(5).get();
            assertEquals("Updated Cached Value", updated.getValue());

            // Test cached object deletion
            morphium.delete(first);
            CachedObject shouldBeNull = morphium.createQueryFor(CachedObject.class)
                                        .f("counter").eq(5).get();
            assertNull(shouldBeNull);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void idOperationsTest(Morphium morphium) throws Exception {
        String tstName = new Object() {} .getClass().getEnclosingMethod().getName();
        log.info("Running test " + tstName + " with " + morphium.getDriver().getName());

        try (morphium) {
            // Test ID generation and retrieval
            UncachedObject obj = new UncachedObject();
            obj.setStrValue("ID Test");
            obj.setCounter(42);

            assertNull(obj.getMorphiumId()); // Should be null before save
            morphium.store(obj);
            assertNotNull(obj.getMorphiumId()); // Should be generated after save

            MorphiumId savedId = obj.getMorphiumId();

            // Test find by ID
            UncachedObject foundById = morphium.findById(UncachedObject.class, savedId);
            assertNotNull(foundById);
            assertEquals(obj.getCounter(), foundById.getCounter());
            assertEquals(obj.getStrValue(), foundById.getStrValue());
            assertEquals(savedId, foundById.getMorphiumId());

            // Test ID query
            UncachedObject queryById = morphium.createQueryFor(UncachedObject.class)
                                               .f(UncachedObject.Fields.morphiumId).eq(savedId).get();
            assertNotNull(queryById);
            assertEquals(savedId, queryById.getMorphiumId());

            // Test multiple ID queries
            List<MorphiumId> ids = new ArrayList<>();
            for (int i = 0; i < 5; i++) {
                UncachedObject o = new UncachedObject();
                o.setCounter(i);
                morphium.store(o);
                ids.add(o.getMorphiumId());
            }

            List<UncachedObject> foundByIds = morphium.createQueryFor(UncachedObject.class)
                                              .f(UncachedObject.Fields.morphiumId).in(ids).asList();
            assertEquals(5, foundByIds.size());
        }
    }

    /**
     * Helper entity for testing multiple counter updates
     */
    @Entity
    public static class UncachedMultipleCounter {
        @Id
        private MorphiumId morphiumId;

        @Property
        private int counter;

        @Property
        private String strValue;

        @Property
        private Double counter2;

        public MorphiumId getMorphiumId() { return morphiumId; }
        public void setMorphiumId(MorphiumId morphiumId) { this.morphiumId = morphiumId; }
        public int getCounter() { return counter; }
        public void setCounter(int counter) { this.counter = counter; }
        public String getStrValue() { return strValue; }
        public void setStrValue(String strValue) { this.strValue = strValue; }
        public Double getCounter2() { return counter2; }
        public void setCounter2(Double counter2) { this.counter2 = counter2; }
    }
}
