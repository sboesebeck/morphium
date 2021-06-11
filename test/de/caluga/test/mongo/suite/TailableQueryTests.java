package de.caluga.test.mongo.suite;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumAccessVetoException;
import de.caluga.morphium.async.AsyncCallbackAdapter;
import de.caluga.morphium.async.AsyncOperationType;
import de.caluga.morphium.query.Query;
import de.caluga.test.mongo.suite.data.CappedCol;
import org.junit.Test;

import java.util.List;

/**
 * Created by stephan on 29.07.16.
 */

public class TailableQueryTests extends MorphiumTestBase {
    boolean found = false;

    @Test
    public void tailableTest() throws Exception {
        Morphium m = morphium;
        m.dropCollection(CappedCol.class);
        CappedCol o = new CappedCol("Test1", 1);
        m.store(o);
        m.store(new CappedCol("Test 2", 2));
        Thread.sleep(100);
        found = false;
        new Thread(() -> {
            Query<CappedCol> q = m.createQueryFor(CappedCol.class);
            q.tail(10, 0, new AsyncCallbackAdapter<CappedCol>() {
                @Override
                public void onOperationSucceeded(AsyncOperationType type, Query<CappedCol> q, long duration, List<CappedCol> result, CappedCol entity, Object... param) {
                    log.info("Got incoming!!! " + entity.getValue() + " " + entity.getCounter());
                    found = true;
                    if (entity.getValue().equals("Test 3 - quit")) {
                        throw new MorphiumAccessVetoException("Quitting");
                    }
                }
            });
            assert (found);
        }).start();

        Thread.sleep(2500);
        assert (found);
        found = false;
        log.info("Storing 3...");
        m.store(new CappedCol("Test 3 - quit", 3));
        Thread.sleep(2500);
        assert (found);

    }
}
