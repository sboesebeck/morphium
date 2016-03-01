package de.caluga.test.mongo.suite;

import de.caluga.morphium.annotations.Capped;
import de.caluga.test.mongo.suite.data.UncachedObject;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by stephan on 08.08.14.
 */
@SuppressWarnings("AssertWithSideEffects")
public class CappedCollectionTest extends MongoTest {

    @Test
    public void testCreationOfCappedCollection() throws Exception {
        morphium.dropCollection(CappedCol.class);

        CappedCol cc = new CappedCol();
        cc.setValue("A value");
        cc.setCounter(-1);
        morphium.store(cc);


        assert (morphium.getDriver().isCapped(morphium.getConfig().getDatabase(), "capped_col"));
        //storing more than max entries
        for (int i = 0; i < 1000; i++) {
            cc = new CappedCol();
            cc.setValue("Value " + i);
            cc.setCounter(i);
            morphium.store(cc);
        }

        assert (morphium.createQueryFor(CappedCol.class).countAll() <= 10);
        for (CappedCol cp : morphium.createQueryFor(CappedCol.class).sort("counter").asIterable(10)) {
            log.info("Capped: " + cp.getCounter() + " - " + cp.getValue());
        }

    }


    @Test
    public void testListCreationOfCappedCollection() throws Exception {
        morphium.dropCollection(CappedCol.class);

        List<CappedCol> lst = new ArrayList<>();

        //storing more than max entries
        for (int i = 0; i < 100; i++) {
            CappedCol cc = new CappedCol();
            cc.setValue("Value " + i);
            cc.setCounter(i);
            lst.add(cc);
        }

        morphium.storeList(lst);
        assert (morphium.getDriver().isCapped(morphium.getConfig().getDatabase(), "capped_col"));
        assert (morphium.createQueryFor(CappedCol.class).countAll() <= 10);
        for (CappedCol cp : morphium.createQueryFor(CappedCol.class).sort("counter").asIterable(10)) {
            log.info("Capped: " + cp.getCounter() + " - " + cp.getValue());
        }

    }


    @Test
    public void convertToCappedTest() throws Exception {
        morphium.dropCollection(UncachedObject.class);
        createUncachedObjects(1000);

        morphium.convertToCapped(UncachedObject.class, 100, null);

        assert (morphium.getDriver().isCapped(morphium.getConfig().getDatabase(), "uncached_object"));
        assert (morphium.createQueryFor(UncachedObject.class).countAll() <= 100);
    }


    @Capped(maxEntries = 10, maxSize = 100000)
    public static class CappedCol extends UncachedObject {

    }
}
