package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.annotations.ReadPreferenceLevel;
import de.caluga.morphium.annotations.caching.WriteBuffer;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.query.Query;
import de.caluga.test.mongo.suite.data.*;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import de.caluga.morphium.Morphium;


/**
 * User: Stephan Bösebeck
 * Date: 28.05.12
 * Time: 17:17
 * <p>
 */
@SuppressWarnings("AssertWithSideEffects")
@Tag("core")
public class ListTests extends MultiDriverTestBase {

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void listStoringTest(Morphium morphium) throws Exception  {
        morphium.dropCollection(Uc.class);
        TestUtils.waitForConditionToBecomeTrue(10000, "Collection was not dropped?!?!", ()-> {
            try {
                return !morphium.exists(morphium.getDatabase(), morphium.getMapper().getCollectionName(Uc.class));
            } catch (MorphiumDriverException e) {
            }
            return false;
        });
        List<UncachedObject> lst = new ArrayList<>();

        for (int i = 0; i < 100; i++) {
            Uc u = new Uc();
            u.setCounter(i);
            u.setStrValue("V: " + i);
            lst.add(u);
        }

        morphium.storeList(lst);
        Thread.sleep(200);
        long count = morphium.createQueryFor(UncachedObject.class, "UCTest").countAll();
        assert(count == 100) : "Count wrong " + count;
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void simpleListTest(Morphium morphium) throws Exception  {
        ListContainer lst = new ListContainer();
        int count = 2;

        for (int i = 0; i < count; i++) {
            EmbeddedObject eo = new EmbeddedObject();
            eo.setName("Embedded");
            eo.setValue("" + i);
            eo.setTest(i);
            lst.addEmbedded(eo);
        }

        for (int i = 0; i < count; i++) {
            UncachedObject uc = new UncachedObject();
            uc.setCounter(i);
            uc.setStrValue("A value - uncached!");
            //references should be stored automatically...
            lst.addRef(uc);
        }

        for (int i = 0; i < count; i++) {
            lst.addLong(i);
        }

        for (int i = 0; i < count; i++) {
            lst.addString("Value " + i);
        }

        morphium.store(lst);
        TestUtils.waitForConditionToBecomeTrue(5000, "Did not write", ()->morphium.createQueryFor(ListContainer.class).f("_id").eq(lst.getId()).get() != null);
        Thread.sleep(100);
        Query<ListContainer> q = morphium.createQueryFor(ListContainer.class).f("id").eq(lst.getId());
        q.setReadPreferenceLevel(ReadPreferenceLevel.PRIMARY);
        ListContainer lst2 = q.get();
        assertNotNull(lst2, "Error - not found?");
        assertNotNull(lst2.getEmbeddedObjectList(), "Embedded list null?");
        assertNotNull(lst2.getLongList(), "Long list null?");
        assertNotNull(lst2.getRefList(), "Ref list null?");
        assertNotNull(lst2.getStringList(), "String list null?");
        assertEquals(count, lst2.getLongList().size());
        assertEquals(count, lst2.getRefList().size());
        assertEquals(count, lst2.getEmbeddedObjectList().size());
        assertEquals(count, lst2.getStringList().size());

        for (int i = 0; i < count; i++) {
            assertEquals(lst2.getEmbeddedObjectList().get(i), lst.getEmbeddedObjectList().get(i), "Embedded objects list differ? - " + i);
            assertEquals(lst2.getLongList().get(i), lst.getLongList().get(i), "long list differ? - " + i);
            assertEquals(lst2.getStringList().get(i), lst.getStringList().get(i), "string list differ? - " + i);
            assertEquals(lst2.getRefList().get(i), lst.getRefList().get(i), "reference list differ? - " + i);
        }

        Thread.sleep(1000);
        q = morphium.createQueryFor(ListContainer.class).f("refList").eq(lst2.getRefList().get(0));
        assertNotEquals(0, q.countAll());
        log.info("found " + q.countAll() + " entries");
        assertEquals(1, q.countAll());
        ListContainer c = q.get();
        assertEquals(c.getId(), lst2.getId());
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void nullValueListTest(Morphium morphium) throws InterruptedException  {
        morphium.dropCollection(ListContainer.class);
        ListContainer lst = new ListContainer();
        int count = 2;

        for (int i = 0; i < count; i++) {
            EmbeddedObject eo = new EmbeddedObject();
            eo.setName("Embedded");
            eo.setValue("" + i);
            eo.setTest(i);
            lst.addEmbedded(eo);
        }

        lst.addEmbedded(null);

        for (int i = 0; i < count; i++) {
            UncachedObject uc = new UncachedObject();
            uc.setCounter(i);
            uc.setStrValue("A value - uncached!");
            //references should be stored automatically...
            lst.addRef(uc);
        }

        lst.addRef(null);

        for (int i = 0; i < count; i++) {
            lst.addLong(i);
        }

        for (int i = 0; i < count; i++) {
            lst.addString("Value " + i);
        }

        lst.addString(null);
        morphium.store(lst);
        Thread.sleep(100);
        Query q = morphium.createQueryFor(ListContainer.class).f("id").eq(lst.getId());
        q.setReadPreferenceLevel(ReadPreferenceLevel.PRIMARY);
        ListContainer lst2 = (ListContainer) q.get();
        assert(lst2.getStringList().get(count) == null);
        assert(lst2.getRefList().get(count) == null);
        assert(lst2.getEmbeddedObjectList().get(count) == null);
    }


    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void singleEntryListTest(Morphium morphium) throws Exception  {
        morphium.dropCollection(UncachedObject.class);
        List<UncachedObject> lst = new ArrayList<>();
        lst.add(new UncachedObject());
        lst.get(0).setStrValue("hello");
        lst.get(0).setCounter(1);
        morphium.storeList(lst);
        Thread.sleep(100);
        assertNotNull(lst.get(0).getMorphiumId());
        ;
        lst.get(0).setCounter(999);
        morphium.storeList(lst);
        Thread.sleep(100);
        assert(morphium.createQueryFor(UncachedObject.class).asList().get(0).getCounter() == 999);
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void testWildcardList(Morphium morphium) throws Exception  {
        morphium.dropCollection(ListWildcardContainer.class);
        ListWildcardContainer testObjectToStore = new ListWildcardContainer();
        testObjectToStore.setId(new MorphiumId("100000000000000000000001"));
        List<EmbeddedObject> embeddedObjectList = (List<EmbeddedObject>) testObjectToStore.getEmbeddedObjectList();
        ExtendedEmbeddedObject extendedEmbeddedObject = new ExtendedEmbeddedObject();
        extendedEmbeddedObject.setName("testName");
        extendedEmbeddedObject.setAdditionalValue("additonalValue");
        extendedEmbeddedObject.setTest(4711);
        extendedEmbeddedObject.setValue("value");
        embeddedObjectList.add(extendedEmbeddedObject);
        morphium.store(testObjectToStore);
        Thread.sleep(100);
        Query q = morphium.createQueryFor(ListWildcardContainer.class).f("id").eq(testObjectToStore.getId());

        try {
            ListWildcardContainer testObejectToLoadFromDB = (ListWildcardContainer) q.get();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            fail("error while restoring object from db | " + e.getMessage(), e);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void testHybridList(Morphium morphium) throws InterruptedException  {
        morphium.dropCollection(MyListContainer.class);
        MyListContainer mc = new MyListContainer();
        mc.name = "test";
        mc.number = 42;
        mc.objectList = new ArrayList<>();
        ExtendedEmbeddedObject extendedEmbeddedObject = new ExtendedEmbeddedObject();
        extendedEmbeddedObject.setName("testName");
        extendedEmbeddedObject.setAdditionalValue("additionalValue");
        extendedEmbeddedObject.setTest(4711);
        extendedEmbeddedObject.setValue("value");
        UncachedObject uc = new UncachedObject();
        uc.setCounter(42);
        uc.setStrValue("val");
        EmbeddedObject eo = new EmbeddedObject();
        eo.setValue("Embedded");
        eo.setName("Fred");
        eo.setTest(System.currentTimeMillis());
        mc.objectList.add(uc);
        mc.objectList.add(eo);
        mc.objectList.add(extendedEmbeddedObject);
        morphium.store(mc);
        Thread.sleep(100);
        MyListContainer mc2 = morphium.createQueryFor(MyListContainer.class).asList().get(0);
        assert(mc2.id.equals(mc.id));
        assert(mc2.objectList.size() == mc.objectList.size());
        assert(mc2.objectList.get(0) instanceof UncachedObject);
        assert(mc2.objectList.get(1) instanceof EmbeddedObject);
        assert(mc2.objectList.get(2) instanceof ExtendedEmbeddedObject);
        assert(((UncachedObject) mc2.objectList.get(0)).getStrValue().equals("val"));
        assert(((UncachedObject) mc2.objectList.get(0)).getCounter() == 42);
        assert(((EmbeddedObject) mc2.objectList.get(1)).getValue().equals("Embedded"));
        assert(((EmbeddedObject) mc2.objectList.get(1)).getName().equals("Fred"));
        assert(((EmbeddedObject) mc2.objectList.get(1)).getTest() != 0);
        assert(((ExtendedEmbeddedObject) mc2.objectList.get(2)).getName().equals("testName"));
        assert(((ExtendedEmbeddedObject) mc2.objectList.get(2)).getAdditionalValue().equals("additionalValue"));
        assert(((ExtendedEmbeddedObject) mc2.objectList.get(2)).getTest() == 4711);
        assert(((ExtendedEmbeddedObject) mc2.objectList.get(2)).getValue().equals("value"));
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void idListTest(Morphium morphium) throws Exception  {
        MyIdListContainer ilst = new MyIdListContainer();
        ilst.idList = new ArrayList<>();
        ilst.idList.add(new MorphiumId());
        ilst.idList.add(new MorphiumId());
        ilst.idList.add(new MorphiumId());
        ilst.idList.add(new MorphiumId());
        ilst.name = "A test";
        ilst.number = 1;
        morphium.store(ilst);
        Thread.sleep(1000);
        assertNotNull(ilst.id);
        ;
        MyIdListContainer ilst2 = morphium.createQueryFor(MyIdListContainer.class).get();
        assert(ilst2.idList.size() == ilst.idList.size());
        assert(ilst2.idList.get(0).equals(ilst.idList.get(0)));
        ilst2.idList.add(new MorphiumId());
        ilst2.number = 234;
        morphium.store(ilst2);
        Thread.sleep(100);
        assert(ilst2.idList.get(0) instanceof MorphiumId);
        assert(ilst2.idList.get(0).equals(ilst.idList.get(0)));
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void unGenericListTest(Morphium morphium) throws Exception  {
        MyNoGenericListContainer c = new MyNoGenericListContainer();
        c.aList = new ArrayList();
        c.aList.add("String");
        c.aList.add(new Integer(12));
        c.aList.add(new UncachedObject("value", 42));
        c.name = "test";
        c.number = 44;
        morphium.store(c);
        Thread.sleep(100);
        morphium.reread(c);
        assert(c.name.equals("test"));
        assert(c.number == 44);
        assert(c.aList.size() == 3);
        assert(c.aList.get(0) instanceof String);
        assert(c.aList.get(1) instanceof Integer);
        assert(c.aList.get(2) instanceof UncachedObject);
    }

    @Entity(collectionName = "UCTest")
    public static class Uc extends UncachedObject {
    }


    @Entity
    public static class MyNoGenericListContainer {
        @Id
        public MorphiumId id;
        public List aList;
        public String name;
        public int number;
    }

    @Entity
    public static class MyListContainer {
        @Id
        public MorphiumId id;
        public List<Object> objectList;
        public String name;
        public int number;
    }


    @Entity
    @WriteBuffer(value = true, size = 10, timeout = 100)
    public static class MyIdListContainer {
        @Id
        public MorphiumId id;
        public List<MorphiumId> idList;
        public String name;
        public int number;
    }


}
