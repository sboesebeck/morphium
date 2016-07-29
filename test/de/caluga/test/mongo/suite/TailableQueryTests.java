package de.caluga.test.mongo.suite;

import de.caluga.morphium.MorphiumAccessVetoException;
import de.caluga.morphium.async.AsyncCallbackAdapter;
import de.caluga.morphium.async.AsyncOperationType;
import de.caluga.morphium.query.Query;
import org.junit.Test;

import java.util.List;

/**
 * Created by stephan on 29.07.16.
 */
public class TailableQueryTests extends MongoTest {
    boolean found = false;

    @Test
    public void tailAbleTest() throws Exception {
        morphium.dropCollection(CappedCollectionTest.CappedCol.class);
        CappedCollectionTest.CappedCol o = new CappedCollectionTest.CappedCol("Test1", 1);
        morphium.store(o);
        morphium.store(new CappedCollectionTest.CappedCol("Test 2", 2));
        found = false;
        new Thread() {
            public void run() {
                Query<CappedCollectionTest.CappedCol> q = morphium.createQueryFor(CappedCollectionTest.CappedCol.class);
                q.tail(10, 0, new AsyncCallbackAdapter<CappedCollectionTest.CappedCol>() {
                    @Override
                    public void onOperationSucceeded(AsyncOperationType type, Query<CappedCollectionTest.CappedCol> q, long duration, List<CappedCollectionTest.CappedCol> result, CappedCollectionTest.CappedCol entity, Object... param) {
                        log.info("Got incoming!!! " + entity.getValue() + " " + entity.getCounter());
                        found = true;
                        if (entity.getValue().equals("Test 3 - quit")) {
                            throw new MorphiumAccessVetoException("Quitting");
                        }
                    }
                });
            }
        }.start();

        Thread.sleep(1000);
        assert (found);
        found = false;
        morphium.store(new CappedCollectionTest.CappedCol("Test 3 - quit", 3));
        Thread.sleep(1000);
        assert (found);


    }
}
