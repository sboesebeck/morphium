package de.caluga.test.mongo.suite;

import de.caluga.morphium.query.Query;
import de.caluga.test.mongo.suite.data.EmbeddedObject;
import de.caluga.test.mongo.suite.data.UncachedObject;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * User: Stephan Bösebeck
 * Date: 09.05.12
 * Time: 10:46
 * <p>
 */
public class UpdateTest extends MongoTest {
    @Test
    public void incMultipleFieldsTest() throws Exception {
        morphium.dropCollection(UncachedMultipleCounter.class);
        for (int i = 1; i <= 50; i++) {
            UncachedMultipleCounter o = new UncachedMultipleCounter();
            o.setCounter(i);
            o.setValue("Uncached " + i);
            o.setCounter2((double) i / 2.0);
            morphium.store(o);
        }
        Query<UncachedMultipleCounter> q = morphium.createQueryFor(UncachedMultipleCounter.class);
        q = q.f("value").eq("Uncached " + 5);

        Map<String, Number> toInc = new HashMap<>();
        toInc.put("counter", 10.0);
        toInc.put("counter2", 0.5);
        morphium.inc(q, toInc, false, true, null);
        Thread.sleep(1000);
        assert (q.get().getCounter() == 15) : "counter is:" + q.get().getCounter();
        assert (q.get().getCounter2() == 3);
        morphium.inc(q, toInc, false, true, null);
        assert (q.get().getCounter() == 25) : "counter is:" + q.get().getCounter();
        assert (q.get().getCounter2() == 3.5);

    }

    @Test
    public void incTest() throws Exception {
        for (int i = 1; i <= 50; i++) {
            UncachedObject o = new UncachedObject();
            o.setCounter(i);
            o.setValue("Uncached " + i);
            morphium.store(o);
        }

        Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
        q = q.f("value").eq("Uncached " + 5);
        UncachedObject uc = q.get();
        morphium.inc(uc, "counter", 1);

        assert (uc.getCounter() == 6) : "Counter is not correct: " + uc.getCounter();

        //inc without object - single update, no upsert
        q = morphium.createQueryFor(UncachedObject.class);
        q = q.f("counter").gte(10).f("counter").lte(25).sort("counter");
        morphium.inc(q, "counter", 100);

        uc = q.get();
        assert (uc.getCounter() == 11) : "Counter is wrong: " + uc.getCounter();

        //inc without object directly in DB - multiple update
        q = morphium.createQueryFor(UncachedObject.class);
        q = q.f("counter").gt(10).f("counter").lte(25);
        morphium.inc(q, "counter", 100, false, true);

        q = morphium.createQueryFor(UncachedObject.class);
        q = q.f("counter").gt(110).f("counter").lte(125);
        List<UncachedObject> lst = q.asList(); //read the data after update
        for (UncachedObject u : lst) {
            assert (u.getCounter() > 110 && u.getCounter() <= 125 && u.getValue().equals("Uncached " + (u.getCounter() - 100))) : "Counter wrong: " + u.getCounter();
        }

    }

    @Test
    public void decTest() throws Exception {
        for (int i = 1; i <= 50; i++) {
            UncachedObject o = new UncachedObject();
            o.setCounter(i);
            o.setValue("Uncached " + i);
            morphium.store(o);
        }

        Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
        q = q.f("value").eq("Uncached " + 5);
        UncachedObject uc = q.get();
        morphium.dec(uc, "counter", 1);
        Thread.sleep(300);

        assert (uc.getCounter() == 4) : "Counter is not correct: " + uc.getCounter();

        //inc without object - single update, no upsert
        q = morphium.createQueryFor(UncachedObject.class);
        q = q.f("counter").gte(40).f("counter").lte(55).sort("counter");
        morphium.dec(q, "counter", 40);
        Thread.sleep(300);
        uc = q.get();
        assert (uc.getCounter() == 41) : "Counter is wrong: " + uc.getCounter();

        //inc without object directly in DB - multiple update
        q = morphium.createQueryFor(UncachedObject.class);
        q = q.f("counter").gt(40).f("counter").lte(55);
        morphium.dec(q, "counter", 40, false, true);
        Thread.sleep(300);

        q = morphium.createQueryFor(UncachedObject.class);
        q = q.f("counter").gt(0).f("counter").lte(55);
        List<UncachedObject> lst = q.asList(); //read the data after update
        for (UncachedObject u : lst) {
            assert (u.getCounter() > 0 && u.getCounter() <= 55) : "Counter wrong: " + u.getCounter();
            //            assert(u.getValue().equals("Uncached "+(u.getCounter()-40))):"Value wrong: Counter: "+u.getCounter()+" Value;: "+u.getValue();
        }

    }

    @Test
    public void setTest() throws Exception {
        for (int i = 1; i <= 50; i++) {
            UncachedObject o = new UncachedObject();
            o.setCounter(i);
            o.setValue("Uncached " + i);
            morphium.store(o);
        }
        Thread.sleep(100);
        Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
        q = q.f("value").eq("unexistent");
        morphium.set(q, "counter", 999, true, false);
        Thread.sleep(100);
        UncachedObject uc = q.get(); //should now work

        assert (uc != null) : "Not found?!?!?";
        assert (uc.getValue().equals("unexistent")) : "Value wrong: " + uc.getValue();
    }

    @Test
    public void setTestEnum() throws Exception {
        EnumUC u = new EnumUC();
        u.setCounter(1);
        u.setValue("something");
        morphium.store(u);

        morphium.set(u, "val", Value.v2);

    }

    @Test
    public void pushTest() throws Exception {
        morphium.dropCollection(ListContainer.class);
        for (int i = 1; i <= 50; i++) {
            ListContainer lc = new ListContainer();
            lc.addLong(12 + i);
            lc.addString("string");
            lc.setName("LC" + i);
            morphium.store(lc);
        }

        Query<ListContainer> lc = morphium.createQueryFor(ListContainer.class);
        lc = lc.f("name").eq("LC15");
        morphium.push(lc, "long_list", 12345L);
        ListContainer cont = lc.get();
        assert (cont.getLongList().contains(12345L)) : "No push?";

    }

    @Test
    public void pushEntityTest() throws Exception {
        morphium.dropCollection(ListContainer.class);

        for (int i = 1; i <= 50; i++) {
            ListContainer lc = new ListContainer();
            lc.addLong(12 + i);
            lc.addString("string");
            lc.setName("LC" + i);
            morphium.store(lc);
        }
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
        waitForWrites();

        ListContainer lc2 = lc.get();
        assert (lc2.getEmbeddedObjectList() != null);
        assert (lc2.getEmbeddedObjectList().size() == 2);
        assert (lc2.getEmbeddedObjectList().get(0).getTest() == 1L);
    }

    @Test
    public void unsetTest() throws Exception {
        createUncachedObjects(100);
        Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class).f("counter").eq(50);
        morphium.unsetQ(q, "value");
        Thread.sleep(300);
        UncachedObject uc = q.get();
        assert (uc.getValue() == null);
        q = morphium.createQueryFor(UncachedObject.class).f("counter").gt(90);
        morphium.unsetQ(q, false, "value");
        Thread.sleep(300);
        List<UncachedObject> lst = q.asList();
        boolean found = false;
        for (UncachedObject u : lst) {
            if (u.getValue() == null) {
                assert (!found);
                found = true;
            }
        }
        assert (found);
        morphium.unsetQ(q, true, "binary_data", "bool_data", "value");
        Thread.sleep(300);
        lst = q.asList();
        for (UncachedObject u : lst) {
            assert (u.getValue() == null);
        }

    }

    @Test
    public void pushEntityListTest() throws Exception {
        morphium.dropCollection(ListContainer.class);

        for (int i = 1; i <= 50; i++) {
            ListContainer lc = new ListContainer();
            lc.addLong(12 + i);
            lc.addString("string");
            lc.setName("LC" + i);
            morphium.store(lc);
        }

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
        waitForWrites();
        Thread.sleep(2500);
        ListContainer lc2 = lc.get();
        assert (lc2.getEmbeddedObjectList() != null);
        assert (lc2.getEmbeddedObjectList().size() == 3) : "Size wrong should be 3 is " + lc2.getEmbeddedObjectList().size();
        assert (lc2.getEmbeddedObjectList().get(0).getTest() == 1L);
    }

    public enum Value {
        v1, v2, v3
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

}
