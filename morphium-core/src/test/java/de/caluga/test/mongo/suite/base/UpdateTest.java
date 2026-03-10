package de.caluga.test.mongo.suite.base;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.Utils;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.annotations.Property;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.query.Query;
import de.caluga.test.mongo.suite.data.EmbeddedObject;
import de.caluga.test.mongo.suite.data.ListContainer;
import de.caluga.test.mongo.suite.data.UncachedObject;

/**
 * User: Stephan Bösebeck Date: 09.05.12 Time: 10:46
 *
 * <p>
 */
@SuppressWarnings("Duplicates")
@Tag("core")
public class UpdateTest extends MultiDriverTestBase {
    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void incMultipleFieldsTest(Morphium morphium) throws Exception {
        try (morphium) {
            morphium.dropCollection(UncachedMultipleCounter.class);

            for (int i = 1; i <= 50; i++) {
                UncachedMultipleCounter o = new UncachedMultipleCounter();
                o.setCounter(i);
                o.setStrValue("Uncached " + i);
                o.setCounter2((double) i / 2.0);
                morphium.store(o);
            }

            Thread.sleep(150);
            Query<UncachedMultipleCounter> q =
                            morphium.createQueryFor(UncachedMultipleCounter.class);
            q = q.f("strValue").eq("Uncached " + 5);
            Map<String, Number> toInc = new HashMap<>();
            toInc.put("counter", 10.0);
            toInc.put("counter2", 0.5);
            morphium.inc(q, toInc, false, true, null);
            final Query<UncachedMultipleCounter> finalQ = q; // Capture for lambda
            TestUtils.waitForConditionToBecomeTrue(3000, "Counter increment to 15 not completed",
                () -> finalQ.get().getCounter() == 15);
            assert(q.get().getCounter2() == 3);
            morphium.inc(q, toInc, false, true, null);
            TestUtils.waitForConditionToBecomeTrue(1000, "Counter increment to 25 not completed",
                () -> finalQ.get().getCounter() == 25);
            assert(q.get().getCounter2() == 3.5);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void incTest(Morphium morphium) throws Exception {
        try (morphium) {
            for (int i = 1; i <= 50; i++) {
                UncachedObject o = new UncachedObject();
                o.setCounter(i);
                o.setStrValue("Uncached " + i);
                morphium.store(o);
            }
            TestUtils.waitForConditionToBecomeTrue(5000, "Did not write?", ()->TestUtils.countUC(morphium) == 50);
            Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
            q = q.f("str_value").eq("Uncached " + 5);
            UncachedObject uc = q.get();
            morphium.inc(uc, "counter", 1);
            assert(uc.getCounter() == 6) : "Counter is not correct: " + uc.getCounter();
            // inc without object - single update, no upsert
            q = morphium.createQueryFor(UncachedObject.class);
            q = q.f("counter").gte(10).f("counter").lte(25).sort("counter");
            morphium.inc(q, "counter", 100);
            Thread.sleep(100);
            uc = q.get();
            assert(uc.getCounter() == 11) : "Counter is wrong: " + uc.getCounter();
            // inc without object directly in DB - multiple update
            q = morphium.createQueryFor(UncachedObject.class);
            q = q.f("counter").gt(10).f("counter").lte(25);
            morphium.inc(q, "counter", 100, false, true);
            q = morphium.createQueryFor(UncachedObject.class);
            q = q.f("counter").gt(110).f("counter").lte(125);
            List<UncachedObject> lst = q.asList(); // read the data after update

            for (UncachedObject u : lst) {
                assert(u.getCounter() > 110
                       && u.getCounter() <= 125
                       && u.getStrValue().equals("Uncached " + (u.getCounter() - 100)))
                    : "Counter wrong: " + u.getCounter();
            }
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void decTest(Morphium morphium) throws Exception {
        try (morphium) {
            for (int i = 1; i <= 50; i++) {
                UncachedObject o = new UncachedObject();
                o.setCounter(i);
                o.setStrValue("Uncached " + i);
                morphium.store(o);
            }

            Thread.sleep(150);
            Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
            q = q.f("str_value").eq("Uncached " + 5);
            UncachedObject uc = q.get();
            morphium.dec(uc, "counter", 1);
            Thread.sleep(300);
            assert(uc.getCounter() == 4) : "Counter is not correct: " + uc.getCounter();
            // inc without object - single update, no upsert
            q = morphium.createQueryFor(UncachedObject.class);
            q = q.f("counter").gte(40).f("counter").lte(55).sort("counter");
            morphium.dec(q, "counter", 40);
            var q1 = q;
            TestUtils.waitForConditionToBecomeTrue(5000, "Object not found?!?!", ()->q1.get() != null);
            uc = q.get();
            assert(uc.getCounter() == 41) : "Counter is wrong: " + uc.getCounter();
            // inc without object directly in DB - multiple update
            q = morphium.createQueryFor(UncachedObject.class);
            q = q.f("counter").gt(40).f("counter").lte(55);
            morphium.dec(q, "counter", 40, false, true);
            Thread.sleep(300);
            q = morphium.createQueryFor(UncachedObject.class);
            q = q.f("counter").gt(0).f("counter").lte(55);
            List<UncachedObject> lst = q.asList(); // read the data after update

            for (UncachedObject u : lst) {
                assert(u.getCounter() > 0 && u.getCounter() <= 55)
                    : "Counter wrong: " + u.getCounter();
                //            assert(u.getValue().equals("Uncached "+(u.getCounter()-40))):"Value
                // wrong: Counter: "+u.getCounter()+" Value;: "+u.getValue();
            }
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void setEntityTest(Morphium morphium) throws Exception {
        try (morphium) {
            for (int i = 1; i <= 50; i++) {
                UncachedObject o = new UncachedObject();
                o.setCounter(i);
                o.setStrValue("Uncached " + i);
                morphium.store(o);
            }

            Thread.sleep(250);
            Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
            q = q.f("counter").eq(42);
            UncachedObject uc = q.get();
            morphium.setInEntity(uc, UncachedObject.Fields.strValue, "meaning of life", false, null);
            checkValue(morphium, uc, "meaning of life");
            morphium.setInEntity(uc, "str_value", "the answer", false, null);
            checkValue(morphium, uc, "the answer");
            String collectionName = morphium.getMapper().getCollectionName(UncachedObject.class);
            morphium.setInEntity(uc, collectionName, UncachedObject.Fields.strValue, "none");
            checkValue(morphium, uc, "none");
            morphium.setInEntity(uc, collectionName, "str_value", "dunno", false, null);
            checkValue(morphium, uc, "dunno");
            morphium.setInEntity(uc, collectionName, UncachedObject.Fields.strValue, "dunno", false, null);
            checkValue(morphium, uc, "dunno");
        }
    }

    private void checkValue(Morphium morphium, UncachedObject uc, String value) throws Exception {
        Thread.sleep(100);
        assert(uc.getStrValue().equals(value))
            : "Value wrong: " + uc.getStrValue() + " but should be " + value;
        uc = morphium.reread(uc);
        assert(uc.getStrValue().equals(value))
            : "Value after reread wrong: " + uc.getStrValue() + ", expected " + value;
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void setTest(Morphium morphium) throws Exception {
        try (morphium) {
            for (int i = 1; i <= 50; i++) {
                UncachedObject o = new UncachedObject();
                o.setCounter(i);
                o.setStrValue("Uncached " + i);
                morphium.store(o);
            }

            Thread.sleep(100);
            Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
            q = q.f("strValue").eq("unexistent");
            q.set("counter", 999, true, false);
            Thread.sleep(220);
            UncachedObject uc = q.get(); // should now work
            assertNotNull(uc, "Not found?!?!?");
            assert(uc.getStrValue().equals("unexistent")) : "Value wrong: " + uc.getStrValue();
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void setTestEnum(Morphium morphium) {
        try (morphium) {
            EnumUC u = new EnumUC();
            u.setCounter(1);
            u.setStrValue("something");
            morphium.store(u);
            morphium.setInEntity(u, "val", Value.v2);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void addAllToSetTest(Morphium morphium) throws Exception {
        try (morphium) {
            morphium.dropCollection(ListContainer.class);

            for (int i = 1; i <= 50; i++) {
                ListContainer lc = new ListContainer();
                lc.addLong(12 + i);
                lc.addString("string");
                lc.setName("LC" + i);
                morphium.store(lc);
            }

            Thread.sleep(150);
            Query<ListContainer> lc = morphium.createQueryFor(ListContainer.class);
            lc = lc.f("name").eq("LC15");
            morphium.addAllToSet(lc, "long_list", Arrays.asList(12345L, 12345L, 123L, 42L), true);
            Thread.sleep(100);
            ListContainer cont = lc.get();
            assertTrue(cont.getLongList().contains(12345L));
            assertEquals(cont.getLongList().size(), 4);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void addToSetTest(Morphium morphium) throws Exception {
        try (morphium) {
            morphium.dropCollection(ListContainer.class);

            for (int i = 1; i <= 50; i++) {
                ListContainer lc = new ListContainer();
                lc.addLong(12 + i);
                lc.addString("string");
                lc.setName("LC" + i);
                morphium.store(lc);
            }

            Thread.sleep(150);
            Query<ListContainer> lc = morphium.createQueryFor(ListContainer.class);
            lc = lc.f("name").eq("LC15");
            morphium.addToSet(lc, "long_list", 12345L);
            morphium.addToSet(lc, "long_list", 12345L);
            Thread.sleep(100);
            ListContainer cont = lc.get();
            assertTrue(cont.getLongList().contains(12345L));
            assertEquals(cont.getLongList().size(), 2);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void pushTest(Morphium morphium) throws Exception {
        try (morphium) {
            morphium.dropCollection(ListContainer.class);

            for (int i = 1; i <= 50; i++) {
                ListContainer lc = new ListContainer();
                lc.addLong(12 + i);
                lc.addString("string");
                lc.setName("LC" + i);
                morphium.store(lc);
            }

            Thread.sleep(150);
            Query<ListContainer> lc = morphium.createQueryFor(ListContainer.class);
            lc = lc.f("name").eq("LC15");
            morphium.push(lc, "long_list", 12345L);
            ListContainer cont = lc.get();
            assert(cont.getLongList().contains(12345L)) : "No push?";
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void pushEntityTest(Morphium morphium) throws Exception {
        try (morphium) {
            morphium.dropCollection(ListContainer.class);

            for (int i = 1; i <= 50; i++) {
                ListContainer lc = new ListContainer();
                lc.addLong(12 + i);
                lc.addString("string");
                lc.setName("LC" + i);
                morphium.store(lc);
            }

            Thread.sleep(150);
            Query<ListContainer> lc = morphium.createQueryFor(ListContainer.class);
            lc = lc.f("name").eq("LC15");
            EmbeddedObject em = new EmbeddedObject();
            em.setValue("emb Value");
            em.setTest(1);
            morphium.push(lc, "embedded_object_list", em);
            em = new EmbeddedObject();
            em.setValue("emb Value 2");
            em.setTest(2);
            morphium.push(lc, "embedded_object_list", em);
            TestUtils.waitForWrites(morphium, log);
            ListContainer lc2 = lc.get();
            assertNotNull(lc2.getEmbeddedObjectList());
            ;
            assert(lc2.getEmbeddedObjectList().size() == 2);
            assert(lc2.getEmbeddedObjectList().get(0).getTest() == 1L);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void unsetTest(Morphium morphium) throws Exception {
        try (morphium) {
            createUncachedObjects(morphium, 100);
            Query<UncachedObject> q =
            morphium.createQueryFor(UncachedObject.class).f("counter").eq(50);
            // morphium.unsetQ(q, "strValue");
            q.unset( "strValue");
            Thread.sleep(300);
            UncachedObject uc = q.get();
            assert(uc.getStrValue() == null);
            q = morphium.createQueryFor(UncachedObject.class).f("counter").gt(90);
            q.unset(false, "str_value");
            Thread.sleep(300);
            List<UncachedObject> lst = q.asList();
            boolean found = false;

            for (UncachedObject u : lst) {
                if (u.getStrValue() == null) {
                    assert(!found);
                    found = true;
                }
            }

            assert(found);
            // morphium.unsetQ(q, true, "binary_data", "bool_data", "str_value");
            q.unset(true, "binary_data", "bool_data", "str_value");

            Thread.sleep(300);
            lst = q.asList();

            for (UncachedObject u : lst) {
                assert(u.getStrValue() == null);
            }
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void pushEntityListTest(Morphium morphium) throws Exception {
        try (morphium) {
            morphium.dropCollection(ListContainer.class);

            for (int i = 1; i <= 50; i++) {
                ListContainer lc = new ListContainer();
                lc.addLong(12 + i);
                lc.addString("string");
                lc.setName("LC" + i);
                morphium.store(lc);
            }

            Thread.sleep(150);
            List<EmbeddedObject> obj = new ArrayList<>();
            Query<ListContainer> lc = morphium.createQueryFor(ListContainer.class);
            lc = lc.f("name").eq("LC15");
            EmbeddedObject em = new EmbeddedObject();
            em.setValue("emb Value");
            em.setTest(1);
            obj.add(em);
            em = new EmbeddedObject();
            em.setValue("emb Value 2");
            em.setTest(2);
            obj.add(em);
            em = new EmbeddedObject();
            em.setValue("emb Value 3");
            em.setTest(3);
            obj.add(em);
            morphium.pushAll(lc, "embedded_object_list", obj, false, true);
            TestUtils.waitForWrites(morphium, log);
            Thread.sleep(2500);
            ListContainer lc2 = lc.get();
            assertNotNull(lc2.getEmbeddedObjectList());
            ;
            assert(lc2.getEmbeddedObjectList().size() == 3)
                : "Size wrong, should be 3 is " + lc2.getEmbeddedObjectList().size();
            assert(lc2.getEmbeddedObjectList().get(0).getTest() == 1L);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void updateUsingFieldsTest(Morphium morphium) throws Exception {
        try (morphium) {
            UncachedObject uc = new UncachedObject("value", 1001);
            morphium.store(uc);
            Thread.sleep(100);
            uc.setStrValue("new Value");
            uc.setCounter(0);
            uc.setDval(4.0d);
            uc.setLongData(new long[] {42l});
            morphium.updateUsingFields(uc, "str_value", "longData");
            Thread.sleep(100);
            UncachedObject uc2 = morphium.findById(UncachedObject.class, uc.getMorphiumId());
            assert(uc2.getCounter() == 1001);
            assertNotNull(uc2.getLongData());
            ;
            assert(uc2.getLongData()[0] == 42);
            assert(uc2.getDval() == 0);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void updateLimitTest(Morphium morphium) throws Exception {
        log.info("Running test with " + morphium.getDriver().getName());
        try(morphium) {
            morphium.dropCollection(UncachedObject.class);
            TestUtils.waitForCollectionToBeDeleted(morphium, UncachedObject.class);
            createUncachedObjects(morphium, 1000);
            var q = morphium.createQueryFor(UncachedObject.class).f(UncachedObject.Fields.counter).lt(900)
                            .f(UncachedObject.Fields.counter).gte(850);

            q.set(UncachedObject.Fields.strValue, "i was updated", false, true);
            var chk = q.clone().f(UncachedObject.Fields.strValue).eq("i was updated");
            //this should set all values
            TestUtils.waitForConditionToBecomeTrue(5000, "Update failed!", ()->chk.countAll() == 50);
            var lst = q.asList();
            assertEquals(50, lst.size());
            for (var o : lst) {
                assertEquals("i was updated", o.getStrValue(), "Wrong result?");
            }

            q = q.q().f("counter").gte(900).f("counter").lt(950).f("str_value").ne("not all updated").limit(5);
            var ret = q.set(UncachedObject.Fields.strValue, "not all updated", false, true);
            log.info(Utils.toJsonString(ret));
            var chk2 = q.q().f("counter").gte(900).f("counter").lt(950).f("str_value").eq("not all updated");
            Thread.sleep(1000);
            log.info("Updated: " + chk2.countAll());
            TestUtils.waitForConditionToBecomeTrue(5000, "Update failed!", ()->chk2.countAll() == 5);
            lst = q.q().f("counter").gte(900).f("counter").lt(950).asList();
            int count = 0;
            for (var o : lst) {
                if (o.getStrValue().equals("not all updated")) count++;
            }
            assertEquals(5, count, "Update failed " + count);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void updateProperty(Morphium morphium) throws Exception {
        try (morphium) {
            UncachedSubClass uc = new UncachedSubClass();
            uc.theString = "not set";
            morphium.store(uc);
            morphium.reread(uc);
            assert(uc.theString.equals("not set"));
            // uc.theString="it is set";
            morphium.setInEntity(uc,
                                 morphium.getMapper().getCollectionName(UncachedSubClass.class),
                                 "THE_STRING",
                                 "it is set",
                                 false,
                                 null);
            Thread.sleep(100);
            assert(uc.theString.equals("it is set"));
            morphium.reread(uc);
            assert(uc.theString.equals("it is set"));
            uc.setTheString("another value");
            morphium.updateUsingFields(uc, "theString");
            Thread.sleep(100);
            morphium.reread(uc);
            assert(uc.theString.equals("another value"));

            for (UncachedSubClass u : morphium.createQueryFor(UncachedSubClass.class).asList()) {
                log.info(Utils.toJsonString(u));
            }
        }
    }

    public enum Value {
        v1,
        v2,
        v3
    }

    public static class EnumUC extends UncachedObject {
        private Value val;
    }

    public static class UncachedMultipleCounter extends UncachedObject {
        private double counter2;

        public double getCounter2() {
            return counter2;
        }

        public void setCounter2(double counter2) {
            this.counter2 = counter2;
        }
    }

    @Entity
    public static class UncachedSubClass {
        @Id private MorphiumId id;

        @Property(fieldName = "THE_STRING")
        private String theString;

        public String getTheString() {
            return theString;
        }

        public void setTheString(String theString) {
            this.theString = theString;
        }

        @Override
        public String toString() {
            return "UncachedSubClass{" + "id=" + id + ", theString='" + theString + '\'' + '}';
        }
    }
}
