package de.caluga.test.mongo.suite;

import de.caluga.morphium.Utils;
import de.caluga.morphium.query.Query;
import de.caluga.test.mongo.suite.data.EmbeddedObject;
import de.caluga.test.mongo.suite.data.UncachedObject;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * User: Stpehan Bösebeck
 * Date: 29.03.12
 * Time: 15:56
 * <p>
 * testing compley queryies on Morphium
 */
public class ComplexTest extends MongoTest {

    @Test
    public void testStoreAndRead() {
        UncachedObject o = new UncachedObject();
        o.setCounter(111);
        o.setValue("Embedded object");
        //morphium.store(o);

        EmbeddedObject eo = new EmbeddedObject();
        eo.setName("Embedded object 1");
        eo.setValue("A value");
        eo.setTest(System.currentTimeMillis());

        ComplexObject co = new ComplexObject();
        co.setEmbed(eo);

        co.setEntityEmbeded(o);

        UncachedObject ref = new UncachedObject();
        ref.setCounter(100);
        ref.setValue("The reference");
        morphium.store(ref);

        co.setRef(ref);
        co.setEinText("This is a very complex object");
        morphium.store(co);

        //object stored!!!
        waitForWrites();

        //now read it again...
        Query<ComplexObject> q = morphium.createQueryFor(ComplexObject.class);
        ComplexObject co2 = q.getById(co.getId());

        log.info("Just loaded: " + co2.toString());
        log.info("Stored     : " + co.toString());
        assert (co2.getId().equals(co.getId())) : "Ids not equal?";


    }

    @Test
    public void testAccessTimestamps() {
        ComplexObject o = new ComplexObject();
        o.setEinText("A test");
        o.setTrans("Tansient");
        o.setNullValue(15);

        //And test for null-References!
        morphium.store(o);
        assert (o.getChanged() != 0) : "Last change not set!?!?";

        Query<ComplexObject> q = morphium.createQueryFor(ComplexObject.class).f("ein_text").eq("A test");
        o = q.get();
        assert (o.getLastAccess() != 0) : "Last access not set!";

        o = new ComplexObject();
        o.setEinText("A test2");
        o.setTrans("Tansient");
        o.setNullValue(18);
        List<ComplexObject> lst = morphium.readAll(ComplexObject.class);
        for (ComplexObject co : lst) {
            assert (co.getChanged() != 0) : "Last Access not set!";
        }


    }

    @Test
    public void testCopmplexQuery() {
        for (int i = 1; i <= 100; i++) {
            UncachedObject o = new UncachedObject();
            o.setCounter(i);
            o.setValue("Uncached " + i);
            morphium.store(o);
        }

        Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
        q.f("counter").lt(50).or(q.q().f("counter").eq(10), q.q().f("value").eq("Uncached 15"));
        List<UncachedObject> lst = q.asList();
        assert (lst.size() == 2) : "List size wrong: " + lst.size();
        for (UncachedObject o : lst) {
            assert (o.getCounter() < 50 && (o.getCounter() == 10 || o.getCounter() == 15)) : "Counter wrong: " + o.getCounter();
        }

        q = morphium.createQueryFor(UncachedObject.class);
        q.f("counter").lt(50).or(q.q().f("counter").eq(10), q.q().f("value").eq("Uncached 15"), q.q().f("counter").eq(52));
        lst = q.asList();
        assert (lst.size() == 2) : "List size wrong: " + lst.size();
        for (UncachedObject o : lst) {
            assert (o.getCounter() < 50 && (o.getCounter() == 10 || o.getCounter() == 15)) : "Counter wrong: " + o.getCounter();
        }

        q = morphium.createQueryFor(UncachedObject.class);
        q.f("counter").lt(50).f("counter").gt(10).or(q.q().f("counter").eq(22), q.q().f("value").eq("Uncached 15"), q.q().f("counter").gte(70));
        lst = q.asList();
        assert (lst.size() == 2) : "List size wrong: " + lst.size();
        for (UncachedObject o : lst) {
            assert (o.getCounter() < 50 && o.getCounter() > 10 && (o.getCounter() == 22 || o.getCounter() == 15)) : "Counter wrong: " + o.getCounter();
        }
    }


    @Test
    public void testNorQuery() throws Exception {
        for (int i = 1; i <= 100; i++) {
            UncachedObject o = new UncachedObject();
            o.setCounter(i);
            o.setValue("Uncached " + i);
            morphium.store(o);
        }
        Thread.sleep(500);
        Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
        q.nor(q.q().f("counter").lt(90), q.q().f("counter").gt(95));
        log.info("Query: " + q.toQueryObject().toString());
        List<UncachedObject> lst = q.asList();
        assert (lst.size() == 6) : "List size wrong: " + lst.size();
        for (UncachedObject o : lst) {
            assert (!(o.getCounter() < 90 || o.getCounter() > 95)) : "Counter wrong: " + o.getCounter();
        }
    }


    @Test
    public void complexQuery() {
        for (int i = 1; i <= 100; i++) {
            UncachedObject o = new UncachedObject();
            o.setCounter(i);
            o.setValue("Uncached " + i);
            morphium.store(o);
        }

        Map<String, Object> query = new HashMap<>();
        query.put("counter", Utils.getMap("$lt", 10));
        Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
        List<UncachedObject> lst = q.complexQuery(query);
        assert (lst != null && !lst.isEmpty()) : "Nothing found?";
        for (UncachedObject o : lst) {
            assert (o.getCounter() < 10) : "Wrong counter: " + o.getCounter();
        }
    }


    @Test
    public void referenceQuery() throws Exception {

        UncachedObject o = new UncachedObject();
        o.setCounter(15);
        o.setValue("Uncached " + 15);
        morphium.store(o);


        ComplexObject co = new ComplexObject();
        co.setEinText("Text");
        co.setRef(o);
        co.setTrans("trans");

        morphium.store(co);

        Query<ComplexObject> qc = morphium.createQueryFor(ComplexObject.class);
        qc.f("ref").eq(o);

        ComplexObject fnd = qc.get();
        assert (fnd != null) : "not found?!?!";
        assert (fnd.getEinText().equals(co.getEinText())) : "Text different?";
        assert (fnd.getRef().getCounter() == co.getRef().getCounter()) : "Reference broken?";
    }

    @Test
    public void searchForSubObj() throws Exception {
        UncachedObject o = new UncachedObject();
        o.setCounter(15);
        o.setValue("Uncached " + 15);
        morphium.store(o);


        ComplexObject co = new ComplexObject();
        co.setEinText("Text");
        co.setRef(o);
        co.setTrans("trans");

        EmbeddedObject eo = new EmbeddedObject();
        eo.setName("embedded1");
        eo.setValue("154");

        co.setEmbed(eo);

        morphium.store(co);

        waitForWrites();
        Query<ComplexObject> qc = morphium.createQueryFor(ComplexObject.class);
        co = qc.f("embed.name").eq("embedded1").get();
        assert (co != null);
        assert (co.getEmbed() != null);
        assert (co.getEmbed().getName().equals("embedded1"));
        assert (co.getEinText().equals("Text"));
    }

}
