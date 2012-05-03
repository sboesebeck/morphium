package de.caluga.test.mongo.suite;

import de.caluga.morphium.MorphiumSingleton;
import de.caluga.morphium.Query;
import org.junit.Test;

import java.util.List;

/**
 * User: Stpehan Bösebeck
 * Date: 29.03.12
 * Time: 15:56
 * <p/>
 * TODO: Add documentation here
 */
public class ComplexTest extends MongoTest {

    @Test
    public void testStoreAndRead() {
        UncachedObject o = new UncachedObject();
        o.setCounter(111);
        o.setValue("Embedded object");
        //MorphiumSingleton.get().store(o);

        ComplexObject co = new ComplexObject();
        co.setEmbed(o);

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
}
