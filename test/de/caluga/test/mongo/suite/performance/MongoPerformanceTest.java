package de.caluga.test.mongo.suite.performance;/**
 * Created by stephan on 25.11.15.
 */

import de.caluga.morphium.query.Query;
import de.caluga.test.mongo.suite.MongoTest;
import de.caluga.test.mongo.suite.data.UncachedObject;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * TODO: Add Documentation here
 **/
public class MongoPerformanceTest extends MongoTest {

    @Test
    public void timeToStore() throws Exception {
        int c = 0;
        UncachedObject uc = new UncachedObject();
        uc.setCounter(c);

        Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
        morphium.store(uc);
        long start = System.currentTimeMillis();
        while (q.countAll() == 0) {
            Thread.yield();
            c++;
        }
        long dur = System.currentTimeMillis() - start;
        log.info("Single document storage took " + dur + "ms  - " + c + " retries");


        List<UncachedObject> lst = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            uc = new UncachedObject();
            uc.setCounter(i + 100);
            lst.add(uc);
        }
        morphium.storeList(lst);
        start = System.currentTimeMillis();
        c = 0;
        while (q.countAll() != 101) {
            Thread.yield();
            c++;
        }
        dur = System.currentTimeMillis() - start;
        log.info("100 document storage took " + dur + "ms  - " + c + " retries");


        uc = new UncachedObject();
        uc.setCounter(c);

        q = morphium.createQueryFor(UncachedObject.class);
        c = 0;
        morphium.store(uc);
        start = System.currentTimeMillis();
        while (q.countAll() != 102) {
            Thread.yield();
            c++;
        }
        dur = System.currentTimeMillis() - start;
        log.info("Single document storage took " + dur + "ms  - " + c + " retries");

    }
}
