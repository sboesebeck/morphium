package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.annotations.Property;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.test.mongo.suite.data.ListContainer;
import de.caluga.test.mongo.suite.data.UncachedObject;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Consolidated data type tests combining functionality from EnumTest, ListTests,
 * ListOfListTests, SetsTests, MapListTest, and data type handling from BasicFunctionalityTest.
 */
@Tag("core")
public class DataTypeTests extends MultiDriverTestBase {

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void listOperationsTest(Morphium morphium) throws Exception {
        String tstName = new Object() {} .getClass().getEnclosingMethod().getName();
        log.info("Running test " + tstName + " with " + morphium.getDriver().getName());

        try (morphium) {
            morphium.dropCollection(ListContainer.class);

            // Test basic list operations
            ListContainer lc = new ListContainer();
            lc.addLong(1L);
            lc.addLong(2L);
            lc.addLong(3L);
            lc.addString("String1");
            lc.addString("String2");

            morphium.store(lc);
            TestUtils.waitForConditionToBecomeTrue(5000, "ListContainer was not stored",
                    () -> morphium.createQueryFor(ListContainer.class).countAll() == 1);
            ListContainer stored = morphium.createQueryFor(ListContainer.class).get();
            assertNotNull(stored);
            assertEquals(3, stored.getLongList().size());
            assertEquals(2, stored.getStringList().size());
            assertTrue(stored.getLongList().contains(2L));
            assertTrue(stored.getStringList().contains("String1"));

            // Test list queries
            ListContainer found = morphium.createQueryFor(ListContainer.class)
                                          .f("longList").eq(2L).get();
            assertNotNull(found);

            // Test adding to existing list
            stored.addLong(4L);
            morphium.store(stored);
            TestUtils.waitForConditionToBecomeTrue(5000, "Added long not visible",
                    () -> morphium.createQueryFor(ListContainer.class).get().getLongList().size() == 4);
            ListContainer updated = morphium.createQueryFor(ListContainer.class).get();
            assertTrue(updated.getLongList().contains(4L));

            // Test list removal
            updated.getLongList().remove(Long.valueOf(1L));
            morphium.store(updated);
            TestUtils.waitForConditionToBecomeTrue(5000, "Removed long still visible",
                    () -> morphium.createQueryFor(ListContainer.class).get().getLongList().size() == 3);
            ListContainer removed = morphium.createQueryFor(ListContainer.class).get();
            assertFalse(removed.getLongList().contains(1L));
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void nestedListTest(Morphium morphium) throws Exception {
        String tstName = new Object() {} .getClass().getEnclosingMethod().getName();
        log.info("Running test " + tstName + " with " + morphium.getDriver().getName());

        try (morphium) {
            morphium.dropCollection(NestedListEntity.class);

            // Test list of lists
            NestedListEntity entity = new NestedListEntity();

            List<String> list1 = Arrays.asList("a", "b", "c");
            List<String> list2 = Arrays.asList("d", "e", "f");
            List<String> list3 = Arrays.asList("g", "h", "i");

            entity.listOfLists.add(list1);
            entity.listOfLists.add(list2);
            entity.listOfLists.add(list3);

            morphium.store(entity);

            TestUtils.waitForConditionToBecomeTrue(5000, "NestedListEntity was not stored",
                    () -> morphium.createQueryFor(NestedListEntity.class).countAll() == 1);
            NestedListEntity stored = morphium.createQueryFor(NestedListEntity.class).get();
            assertNotNull(stored);
            assertEquals(3, stored.listOfLists.size());
            assertEquals(3, stored.listOfLists.get(0).size());
            assertEquals("a", stored.listOfLists.get(0).get(0));
            assertEquals("f", stored.listOfLists.get(1).get(2));

            // Test querying nested lists
            NestedListEntity found = morphium.createQueryFor(NestedListEntity.class)
                                             .f("list_of_lists").eq(list1).get();
            assertNotNull(found);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void setOperationsTest(Morphium morphium) throws Exception {
        String tstName = new Object() {} .getClass().getEnclosingMethod().getName();
        log.info("Running test " + tstName + " with " + morphium.getDriver().getName());

        try (morphium) {
            morphium.dropCollection(SetEntity.class);

            // Test Set operations
            SetEntity entity = new SetEntity();
            entity.stringSet.add("value1");
            entity.stringSet.add("value2");
            entity.stringSet.add("value3");
            entity.stringSet.add("value2"); // Duplicate - should be ignored

            entity.intSet.add(1);
            entity.intSet.add(2);
            entity.intSet.add(3);
            entity.intSet.add(2); // Duplicate - should be ignored

            morphium.store(entity);
            TestUtils.waitForConditionToBecomeTrue(5000, "SetEntity was not stored",
                    () -> morphium.createQueryFor(SetEntity.class).countAll() == 1);
            SetEntity stored = morphium.createQueryFor(SetEntity.class).get();
            assertNotNull(stored);
            assertEquals(3, stored.stringSet.size());
            assertEquals(3, stored.intSet.size());
            assertTrue(stored.stringSet.contains("value1"));
            assertTrue(stored.intSet.contains(2));

            // Test set queries
            SetEntity found = morphium.createQueryFor(SetEntity.class)
                                      .f("stringSet").eq("value2").get();
            assertNotNull(found);

            // Test set modifications
            stored.stringSet.add("value4");
            stored.stringSet.remove("value1");
            morphium.store(stored);
            TestUtils.waitForConditionToBecomeTrue(5000, "Set modification not visible",
                    () -> morphium.createQueryFor(SetEntity.class).get().stringSet.contains("value4"));
            SetEntity modified = morphium.createQueryFor(SetEntity.class).get();
            assertEquals(3, modified.stringSet.size());
            assertFalse(modified.stringSet.contains("value1"));
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void mapOperationsTest(Morphium morphium) throws Exception {
        String tstName = new Object() {} .getClass().getEnclosingMethod().getName();
        log.info("Running test " + tstName + " with " + morphium.getDriver().getName());

        try (morphium) {
            morphium.dropCollection(MapEntity.class);

            // Test Map operations
            MapEntity entity = new MapEntity();
            entity.stringMap.put("key1", "value1");
            entity.stringMap.put("key2", "value2");
            entity.stringMap.put("key3", "value3");

            entity.intMap.put("counter1", 10);
            entity.intMap.put("counter2", 20);
            entity.intMap.put("counter3", 30);

            morphium.store(entity);

            TestUtils.waitForConditionToBecomeTrue(5000, "MapEntity was not stored",
                    () -> morphium.createQueryFor(MapEntity.class).countAll() == 1);
            MapEntity stored = morphium.createQueryFor(MapEntity.class).get();
            assertNotNull(stored);
            assertEquals(3, stored.stringMap.size());
            assertEquals(3, stored.intMap.size());
            assertEquals("value1", stored.stringMap.get("key1"));
            assertEquals(Integer.valueOf(20), stored.intMap.get("counter2"));

            // Test map queries - Note: Map queries can be complex depending on driver
            MapEntity found = morphium.createQueryFor(MapEntity.class)
                                      .f("string_map.key1").eq("value1").get();
            assertNotNull(found);

            // Test map modifications
            stored.stringMap.put("key4", "value4");
            stored.stringMap.remove("key1");
            stored.intMap.put("counter2", 25);
            morphium.store(stored);
            TestUtils.waitForConditionToBecomeTrue(5000, "Map modification not visible",
                    () -> morphium.createQueryFor(MapEntity.class).get().stringMap.containsKey("key4"));

            MapEntity modified = morphium.createQueryFor(MapEntity.class).get();
            assertEquals(3, modified.stringMap.size());
            assertFalse(modified.stringMap.containsKey("key1"));
            assertEquals(Integer.valueOf(25), modified.intMap.get("counter2"));
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void enumOperationsTest(Morphium morphium) throws Exception {
        String tstName = new Object() {} .getClass().getEnclosingMethod().getName();
        log.info("Running test " + tstName + " with " + morphium.getDriver().getName());

        try (morphium) {
            morphium.dropCollection(EnumEntity.class);

            // Test Enum operations
            EnumEntity entity = new EnumEntity();
            entity.status = TestStatus.ACTIVE;
            entity.priority = TestPriority.HIGH;
            entity.statusList.add(TestStatus.ACTIVE);
            entity.statusList.add(TestStatus.INACTIVE);
            entity.statusList.add(TestStatus.PENDING);

            morphium.store(entity);
            TestUtils.waitForConditionToBecomeTrue(5000, "EnumEntity was not stored",
                    () -> morphium.createQueryFor(EnumEntity.class).countAll() == 1);
            EnumEntity stored = morphium.createQueryFor(EnumEntity.class).get();
            assertNotNull(stored);
            assertEquals(TestStatus.ACTIVE, stored.status);
            assertEquals(TestPriority.HIGH, stored.priority);
            assertEquals(3, stored.statusList.size());
            assertTrue(stored.statusList.contains(TestStatus.PENDING));

            // Test enum queries
            EnumEntity foundByStatus = morphium.createQueryFor(EnumEntity.class)
                                               .f("status").eq(TestStatus.ACTIVE).get();
            assertNotNull(foundByStatus);

            EnumEntity foundByPriority = morphium.createQueryFor(EnumEntity.class)
                                                 .f("priority").eq(TestPriority.HIGH).get();
            assertNotNull(foundByPriority);

            // Test enum list queries
            EnumEntity foundByStatusList = morphium.createQueryFor(EnumEntity.class)
                                                   .f("statusList").eq(TestStatus.INACTIVE).get();
            assertNotNull(foundByStatusList);

            // Test enum updates
            stored.status = TestStatus.COMPLETED;
            stored.statusList.add(TestStatus.COMPLETED);
            morphium.store(stored);

            TestUtils.waitForConditionToBecomeTrue(5000, "Enum update not visible",
                    () -> morphium.createQueryFor(EnumEntity.class).get().status == TestStatus.COMPLETED);
            EnumEntity updated = morphium.createQueryFor(EnumEntity.class).get();
            assertEquals(4, updated.statusList.size());
            assertTrue(updated.statusList.contains(TestStatus.COMPLETED));
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void binaryDataTest(Morphium morphium) throws Exception {
        String tstName = new Object() {} .getClass().getEnclosingMethod().getName();
        log.info("Running test " + tstName + " with " + morphium.getDriver().getName());

        try (morphium) {
            morphium.dropCollection(BinaryDataEntity.class);

            // Test binary data operations
            BinaryDataEntity entity = new BinaryDataEntity();
            byte[] testData = "This is test binary data".getBytes();
            entity.binaryData = testData;
            entity.description = "Binary test";

            morphium.store(entity);
            TestUtils.waitForConditionToBecomeTrue(5000, "BinaryDataEntity was not stored",
                    () -> morphium.createQueryFor(BinaryDataEntity.class).countAll() == 1);
            BinaryDataEntity stored = morphium.createQueryFor(BinaryDataEntity.class).get();
            assertNotNull(stored);
            assertNotNull(stored.binaryData);
            assertArrayEquals(testData, stored.binaryData);
            assertEquals("Binary test", stored.description);

            // Test binary data update
            byte[] newData = "Updated binary data".getBytes();
            stored.binaryData = newData;
            morphium.store(stored);
            TestUtils.waitForConditionToBecomeTrue(5000, "Binary data update not visible",
                    () -> Arrays.equals(newData, morphium.createQueryFor(BinaryDataEntity.class).get().binaryData));
            BinaryDataEntity updated = morphium.createQueryFor(BinaryDataEntity.class).get();

            // Test large binary data
            byte[] largeData = new byte[10000];
            Arrays.fill(largeData, (byte) 42);
            updated.binaryData = largeData;
            morphium.store(updated);
            TestUtils.waitForConditionToBecomeTrue(5000, "Large binary data not visible",
                    () -> morphium.createQueryFor(BinaryDataEntity.class).get().binaryData.length == 10000);
            BinaryDataEntity withLargeData = morphium.createQueryFor(BinaryDataEntity.class).get();
            assertEquals(42, withLargeData.binaryData[5000]);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void arrayOfPrimitivesTest(Morphium morphium) throws Exception {
        String tstName = new Object() {} .getClass().getEnclosingMethod().getName();
        log.info("Running test " + tstName + " with " + morphium.getDriver().getName());

        try (morphium) {
            morphium.dropCollection(PrimitiveArrayEntity.class);

            // Test primitive arrays
            PrimitiveArrayEntity entity = new PrimitiveArrayEntity();
            entity.intArray = new int[] {1, 2, 3, 4, 5};
            entity.doubleArray = new double[] {1.1, 2.2, 3.3, 4.4, 5.5};
            entity.booleanArray = new boolean[] {true, false, true, false, true};
            entity.stringArray = new String[] {"a", "b", "c", "d", "e"};

            morphium.store(entity);
            TestUtils.waitForConditionToBecomeTrue(5000, "PrimitiveArrayEntity was not stored",
                    () -> morphium.createQueryFor(PrimitiveArrayEntity.class).countAll() == 1);
            PrimitiveArrayEntity stored = morphium.createQueryFor(PrimitiveArrayEntity.class).get();
            assertNotNull(stored);
            assertArrayEquals(new int[] {1, 2, 3, 4, 5}, stored.intArray);
            assertArrayEquals(new double[] {1.1, 2.2, 3.3, 4.4, 5.5}, stored.doubleArray, 0.001);
            assertArrayEquals(new boolean[] {true, false, true, false, true}, stored.booleanArray);
            assertArrayEquals(new String[] {"a", "b", "c", "d", "e"}, stored.stringArray);

            // Test array queries
            PrimitiveArrayEntity found = morphium.createQueryFor(PrimitiveArrayEntity.class)
                                         .f("intArray").eq(3).get();
            assertNotNull(found);

            // Test array updates
            stored.intArray[2] = 33;
            stored.stringArray[1] = "modified";
            morphium.store(stored);
            TestUtils.waitForConditionToBecomeTrue(5000, "Array update not visible",
                    () -> morphium.createQueryFor(PrimitiveArrayEntity.class).get().intArray[2] == 33);
            PrimitiveArrayEntity updated = morphium.createQueryFor(PrimitiveArrayEntity.class).get();
            assertEquals("modified", updated.stringArray[1]);
        }
    }

    // Helper entity classes for testing

    @Entity
    public static class NestedListEntity {
        @Id
        public MorphiumId id;

        @Property
        public List<List<String>> listOfLists = new ArrayList<>();
    }

    @Entity
    public static class SetEntity {
        @Id
        public MorphiumId id;

        @Property
        public Set<String> stringSet = new HashSet<>();

        @Property
        public Set<Integer> intSet = new HashSet<>();
    }

    @Entity
    public static class MapEntity {
        @Id
        public MorphiumId id;

        @Property
        public Map<String, String> stringMap = new HashMap<>();

        @Property
        public Map<String, Integer> intMap = new HashMap<>();
    }

    @Entity
    public static class EnumEntity {
        @Id
        public MorphiumId id;

        @Property
        public TestStatus status;

        @Property
        public TestPriority priority;

        @Property
        public List<TestStatus> statusList = new ArrayList<>();
    }

    @Entity
    public static class BinaryDataEntity {
        @Id
        public MorphiumId id;

        @Property
        public byte[] binaryData;

        @Property
        public String description;
    }

    @Entity
    public static class PrimitiveArrayEntity {
        @Id
        public MorphiumId id;

        @Property
        public int[] intArray;

        @Property
        public double[] doubleArray;

        @Property
        public boolean[] booleanArray;

        @Property
        public String[] stringArray;
    }

    public enum TestStatus {
        ACTIVE, INACTIVE, PENDING, COMPLETED
    }

    public enum TestPriority {
        LOW, MEDIUM, HIGH, URGENT
    }
}
