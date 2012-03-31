package de.caluga.test.mongo.suite;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.Query;
import org.junit.Test;

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
        //Morphium.get().store(o);

        ComplexObject co = new ComplexObject();
        co.setEmbed(o);

        UncachedObject ref = new UncachedObject();
        ref.setCounter(100);
        ref.setValue("The reference");
        Morphium.get().store(ref);

        co.setRef(ref);
        co.setEinText("This is a very complex object");
        Morphium.get().store(co);

        //object stored!!!

        //now read it again...
        Query<ComplexObject> q = Morphium.get().createQueryFor(ComplexObject.class);
        ComplexObject co2 = q.getById(co.getId());

        log.info("Just loaded: " + co2.toString());
        log.info("Stored     : " + co.toString());
        assert (co2.getId().equals(co.getId())) : "Ids not equal?";


    }
}
