package de.caluga.test.mongo.suite;

import de.caluga.morphium.MorphiumSingleton;
import de.caluga.morphium.Query;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * User: Stephan Bösebeck
 * Date: 01.07.12
 * Time: 11:34
 * <p/>
 * TODO: Add documentation here
 */
public class BulkInsertTest extends MongoTest {
    @Test
    public void bulkInsert() throws Exception {
        MorphiumSingleton.get().clearCollection(UncachedObject.class);
        log.info("Start storing single");
        long start = System.currentTimeMillis();
        for (int i = 0; i < 1000; i++) {
            UncachedObject uc = new UncachedObject();
            uc.setCounter(i + 1);
            uc.setValue("nix " + i);
            MorphiumSingleton.get().store(uc);
        }
        long dur = System.currentTimeMillis() - start;
        log.info("storing objects one by one took " + dur + " ms");
        MorphiumSingleton.get().clearCollection(UncachedObject.class);

        log.info("Start storing ist");
        List<UncachedObject> lst = new ArrayList<UncachedObject>();
        start = System.currentTimeMillis();
        for (int i = 0; i < 1000; i++) {
            UncachedObject uc = new UncachedObject();
            uc.setCounter(i + 1);
            uc.setValue("nix " + i);
            lst.add(uc);
        }
        MorphiumSingleton.get().storeList(lst);
        dur = System.currentTimeMillis() - start;
        assert (MorphiumSingleton.get().writeBufferCount() == 0) : "WriteBufferCount not 0!?";
        log.info("storing objects one by one took " + dur + " ms");
        Query<UncachedObject> q = MorphiumSingleton.get().createQueryFor(UncachedObject.class);
        assert (q.countAll() == 1000) : "Assert not all stored yet????";

    }

}
