package de.caluga.test.mongo.suite;

import de.caluga.morphium.MorphiumSingleton;
import de.caluga.morphium.annotations.Capped;
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
        MorphiumSingleton.get().dropCollection(CappedCol.class);

        CappedCol cc = new CappedCol();
        cc.setValue("A value");
        cc.setCounter(-1);
        MorphiumSingleton.get().store(cc);


        assert (MorphiumSingleton.get().getDatabase().getCollection("capped_col").isCapped());
        //storing more than max entries
        for (int i = 0; i < 1000; i++) {
            cc = new CappedCol();
            cc.setValue("Value " + i);
            cc.setCounter(i);
            MorphiumSingleton.get().store(cc);
        }

        assert (MorphiumSingleton.get().createQueryFor(CappedCol.class).countAll() <= 10);
        for (CappedCol cp : MorphiumSingleton.get().createQueryFor(CappedCol.class).sort("counter").asIterable(10)) {
            log.info("Capped: " + cp.getCounter() + " - " + cp.getValue());
        }

    }


    @Test
    public void testListCreationOfCappedCollection() throws Exception {
        MorphiumSingleton.get().dropCollection(CappedCol.class);

        List<CappedCol> lst = new ArrayList<>();

        //storing more than max entries
        for (int i = 0; i < 100; i++) {
            CappedCol cc = new CappedCol();
            cc.setValue("Value " + i);
            cc.setCounter(i);
            lst.add(cc);
        }

        MorphiumSingleton.get().storeList(lst);
        assert (MorphiumSingleton.get().getDatabase().getCollection("capped_col").isCapped());
        assert (MorphiumSingleton.get().createQueryFor(CappedCol.class).countAll() <= 10);
        for (CappedCol cp : MorphiumSingleton.get().createQueryFor(CappedCol.class).sort("counter").asIterable(10)) {
            log.info("Capped: " + cp.getCounter() + " - " + cp.getValue());
        }

    }


    @Test
    public void convertToCappedTest() throws Exception {
        MorphiumSingleton.get().dropCollection(UncachedObject.class);
        createUncachedObjects(1000);

        MorphiumSingleton.get().convertToCapped(UncachedObject.class, 100, null);

        assert (MorphiumSingleton.get().getDatabase().getCollection("uncached_object").isCapped());
        assert (MorphiumSingleton.get().createQueryFor(UncachedObject.class).countAll() <= 100);
    }


    @Capped(maxEntries = 10, maxSize = 100000)
    public static class CappedCol extends UncachedObject {

    }
}
