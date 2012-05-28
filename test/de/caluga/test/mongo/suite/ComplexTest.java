package de.caluga.test.mongo.suite;

import com.mongodb.BasicDBObject;
import de.caluga.morphium.MorphiumSingleton;
import de.caluga.morphium.Query;
import org.junit.Test;

import java.util.List;

/**
 * User: Stpehan Bösebeck
 * Date: 29.03.12
 * Time: 15:56
 * <p/>
 * testing compley queryies on Morphium
 */
public class ComplexTest extends MongoTest {

    @Test
    public void testStoreAndRead() {
        UncachedObject o = new UncachedObject();
        o.setCounter(111);
        o.setValue("Embedded object");
        //MorphiumSingleton.get().store(o);

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
        MorphiumSingleton.get().store(ref);

        co.setRef(ref);
        co.setEinText("This is a very complex object");
        MorphiumSingleton.get().store(co);

        //object stored!!!

        //now read it again...
        Query<ComplexObject> q = MorphiumSingleton.get().createQueryFor(ComplexObject.class);
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

        MorphiumSingleton.get().store(o);
        assert (o.getChanged() != 0) : "Last change not set!?!?";

        Query<ComplexObject> q = MorphiumSingleton.get().createQueryFor(ComplexObject.class).f("ein_text").eq("A test");
        o = q.get();
        assert (o.getLastAccess() != 0) : "Last access not set!";
        assert (o.getLastAccess() <= o.getChanged()) : "Timestamp lastAccess BEFORE creation?!?!?";
        o = new ComplexObject();
        o.setEinText("A test2");
        o.setTrans("Tansient");
        o.setNullValue(18);
        List<ComplexObject> lst = MorphiumSingleton.get().readAll(ComplexObject.class);
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
            MorphiumSingleton.get().store(o);
        }

        Query<UncachedObject> q = MorphiumSingleton.get().createQueryFor(UncachedObject.class);
        q.f("counter").lt(50).or(q.q().f("counter").eq(10), q.q().f("value").eq("Uncached 15"));
        List<UncachedObject> lst = q.asList();
        assert (lst.size() == 2) : "List size wrong: " + lst.size();
        for (UncachedObject o : lst) {
            assert (o.getCounter() < 50 && (o.getCounter() == 10 || o.getCounter() == 15)) : "Counter wrong: " + o.getCounter();
        }

        q = MorphiumSingleton.get().createQueryFor(UncachedObject.class);
        q.f("counter").lt(50).or(q.q().f("counter").eq(10), q.q().f("value").eq("Uncached 15"), q.q().f("counter").eq(52));
        lst = q.asList();
        assert (lst.size() == 2) : "List size wrong: " + lst.size();
        for (UncachedObject o : lst) {
            assert (o.getCounter() < 50 && (o.getCounter() == 10 || o.getCounter() == 15)) : "Counter wrong: " + o.getCounter();
        }

        q = MorphiumSingleton.get().createQueryFor(UncachedObject.class);
        q.f("counter").lt(50).f("counter").gt(10).or(q.q().f("counter").eq(22), q.q().f("value").eq("Uncached 15"), q.q().f("counter").gte(70));
        lst = q.asList();
        assert (lst.size() == 2) : "List size wrong: " + lst.size();
        for (UncachedObject o : lst) {
            assert (o.getCounter() < 50 && o.getCounter() > 10 && (o.getCounter() == 22 || o.getCounter() == 15)) : "Counter wrong: " + o.getCounter();
        }
    }


    @Test
    public void testNorQuery() {
        for (int i = 1; i <= 100; i++) {
            UncachedObject o = new UncachedObject();
            o.setCounter(i);
            o.setValue("Uncached " + i);
            MorphiumSingleton.get().store(o);
        }

        Query<UncachedObject> q = MorphiumSingleton.get().createQueryFor(UncachedObject.class);
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
            MorphiumSingleton.get().store(o);
        }

        BasicDBObject query = new BasicDBObject();
        query = query.append("counter", new BasicDBObject("$lt", 10));
        Query<UncachedObject> q = MorphiumSingleton.get().createQueryFor(UncachedObject.class);
        List<UncachedObject> lst = q.complexQuery(query);
        assert (lst != null && !lst.isEmpty()) : "Nothing found?";
        for (UncachedObject o : lst) {
            assert (o.getCounter() < 10) : "Wrong counter: " + o.getCounter();
        }
    }


}
