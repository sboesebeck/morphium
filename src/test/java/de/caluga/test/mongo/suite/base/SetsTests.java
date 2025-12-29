package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.annotations.ReadPreferenceLevel;
import de.caluga.morphium.annotations.caching.WriteBuffer;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.query.Query;
import de.caluga.test.mongo.suite.data.EmbeddedObject;
import de.caluga.test.mongo.suite.data.ExtendedEmbeddedObject;
import de.caluga.test.mongo.suite.data.SetContainer;
import de.caluga.test.mongo.suite.data.UncachedObject;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertNotNull;
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
public class SetsTests extends MultiDriverTestBase {

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void setStoringTest(Morphium morphium) throws Exception  {
        morphium.dropCollection(Uc.class);
        Set<UncachedObject> lst = new LinkedHashSet<>();

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
    public void simpleSetTest(Morphium morphium) throws Exception  {
        SetContainer lst = new SetContainer();
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
        Thread.sleep(100);
        Query<SetContainer> q = morphium.createQueryFor(SetContainer.class).f("id").eq(lst.getId());
        q.setReadPreferenceLevel(ReadPreferenceLevel.PRIMARY);
        SetContainer lst2 = q.get();
        assertNotNull(lst2, "Error - not found?");
        assertNotNull(lst2.getEmbeddedObjectsSet(), "Embedded list null?");
        assertNotNull(lst2.getLongSet(), "Long list null?");
        assertNotNull(lst2.getRefSet(), "Ref list null?");
        assertNotNull(lst2.getStringSet(), "String list null?");

        for (int i = 0; i < count; i++) {
            assert(lst2.getEmbeddedObjectsSet().toArray()[i].equals(lst.getEmbeddedObjectsSet().toArray()[i])) : "Embedded objects list differ? - " + i;
            assert(lst2.getLongSet().toArray()[i].equals(lst.getLongSet().toArray()[i])) : "long list differ? - " + i;
            assert(lst2.getStringSet().toArray()[i].equals(lst.getStringSet().toArray()[i])) : "string list differ? - " + i;
            assert(lst2.getRefSet().toArray()[i].equals(lst.getRefSet().toArray()[i])) : "reference list differ? - " + i;
        }

        Thread.sleep(1000);
        q = morphium.createQueryFor(SetContainer.class).f("refSet").eq(lst2.getRefSet().toArray()[0]);
        assert(q.countAll() != 0);
        log.info("found " + q.countAll() + " entries");
        assert(q.countAll() == 1);
        SetContainer c = q.get();
        assert(c.getId().equals(lst2.getId()));
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void nullValueListTest(Morphium morphium) throws Exception  {
        morphium.dropCollection(SetContainer.class);
        SetContainer lst = new SetContainer();
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
        Thread.sleep(250);
        Query q = morphium.createQueryFor(SetContainer.class).f("id").eq(lst.getId());
        q.setReadPreferenceLevel(ReadPreferenceLevel.PRIMARY);
        SetContainer lst2 = (SetContainer) q.get();
        assert(lst2.getStringSet().toArray()[count] == null);
        assert(lst2.getRefSet().toArray()[count] == null);
        assert(lst2.getEmbeddedObjectsSet().toArray()[count] == null);
    }


    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void singleEntryListTest(Morphium morphium) throws Exception  {
        morphium.dropCollection(UncachedObject.class);
        Set<UncachedObject> lst = new LinkedHashSet<>();
        lst.add(new UncachedObject());
        lst.toArray(new UncachedObject[] {})[0].setStrValue("hello");
        lst.toArray(new UncachedObject[] {})[0].setCounter(1);
        morphium.storeList(lst);
        Thread.sleep(100);
        assertNotNull(lst.toArray(new UncachedObject[] {})[0].getMorphiumId());
        ;
        lst.toArray(new UncachedObject[] {})[0].setCounter(999);
        morphium.storeList(lst);
        Thread.sleep(100);
        assert(morphium.createQueryFor(UncachedObject.class).asList().get(0).getCounter() == 999);
    }


    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void testHybridSet(Morphium morphium) throws InterruptedException  {
        morphium.dropCollection(MySetContainer.class);
        MySetContainer mc = new MySetContainer();
        mc.name = "test";
        mc.number = 42;
        mc.objectList = new LinkedHashSet<>();
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
        MySetContainer mc2 = morphium.createQueryFor(MySetContainer.class).asList().get(0);
        assert(mc2.id.equals(mc.id));
        assert(mc2.objectList.size() == mc.objectList.size());
        assert(mc2.objectList.toArray()[0] instanceof UncachedObject);
        assert(mc2.objectList.toArray()[1] instanceof EmbeddedObject);
        assert(mc2.objectList.toArray()[2] instanceof ExtendedEmbeddedObject);
        assert(((UncachedObject) mc2.objectList.toArray()[0]).getStrValue().equals("val"));
        assert(((UncachedObject) mc2.objectList.toArray()[0]).getCounter() == 42);
        assert(((EmbeddedObject) mc2.objectList.toArray()[1]).getValue().equals("Embedded"));
        assert(((EmbeddedObject) mc2.objectList.toArray()[1]).getName().equals("Fred"));
        assert(((EmbeddedObject) mc2.objectList.toArray()[1]).getTest() != 0);
        assert(((ExtendedEmbeddedObject) mc2.objectList.toArray()[2]).getName().equals("testName"));
        assert(((ExtendedEmbeddedObject) mc2.objectList.toArray()[2]).getAdditionalValue().equals("additionalValue"));
        assert(((ExtendedEmbeddedObject) mc2.objectList.toArray()[2]).getTest() == 4711);
        assert(((ExtendedEmbeddedObject) mc2.objectList.toArray()[2]).getValue().equals("value"));
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void idListTest(Morphium morphium) throws Exception  {
        // Ensure clean state
        morphium.dropCollection(MyIdSetContainer.class);
        Thread.sleep(100);
        MyIdSetContainer ilst = new MyIdSetContainer();
        ilst.idList = new LinkedHashSet<>();
        ilst.idList.add(new MorphiumId());
        ilst.idList.add(new MorphiumId());
        ilst.idList.add(new MorphiumId());
        ilst.idList.add(new MorphiumId());
        ilst.name = "A test";
        ilst.number = 1;
        morphium.store(ilst);
        TestUtils.waitForWrites(morphium, log);
        assertNotNull(ilst.id);
        // Wait for the specific object to be queryable in replica set
        final MorphiumId expectedId = ilst.id;
        TestUtils.waitForConditionToBecomeTrue(15000, "Object not queryable",
            () -> morphium.findById(MyIdSetContainer.class, expectedId) != null);
        MyIdSetContainer ilst2 = morphium.findById(MyIdSetContainer.class, expectedId);
        assertNotNull(ilst2);
        assert(ilst2.idList.size() == ilst.idList.size());
        assert(ilst2.idList.toArray()[0].equals(ilst.idList.toArray()[0]));
        ilst2.idList.add(new MorphiumId());
        ilst2.number = 234;
        morphium.store(ilst2);
        assert(ilst2.idList.toArray()[0] instanceof MorphiumId);
        assert(ilst2.idList.toArray()[0].equals(ilst.idList.toArray()[0]));
    }


    @Entity(collectionName = "UCTest")
    public static class Uc extends UncachedObject {
    }


    @Entity
    public static class MySetContainer {
        @Id
        public MorphiumId id;
        public Set<Object> objectList;
        public String name;
        public int number;
    }


    @Entity
    @WriteBuffer(value = true, size = 10, timeout = 100)
    public static class MyIdSetContainer {
        @Id
        public MorphiumId id;
        public Set<MorphiumId> idList;
        public String name;
        public int number;
    }


}
