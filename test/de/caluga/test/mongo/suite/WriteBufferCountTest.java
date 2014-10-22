package de.caluga.test.mongo.suite;

import de.caluga.morphium.MorphiumSingleton;
import de.caluga.morphium.async.AsyncOperationCallback;
import de.caluga.morphium.async.AsyncOperationType;
import de.caluga.morphium.query.Query;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * User: Stephan Bösebeck
 * Date: 15.03.13
 * Time: 19:14
 * <p/>
 * TODO: Add documentation here
 */
public class WriteBufferCountTest extends MongoTest {
    @Test
    public void testWbCount() throws Exception {
        List<UncachedObject> lst = new ArrayList<UncachedObject>();
        for (int i = 0; i < 10000; i++) {
            UncachedObject uc = new UncachedObject();
            uc.setCounter(1);
            StringBuilder longText = new StringBuilder();
            for (int t = 0; t < 1000; t++) {
                longText.append("-test-" + i);
            }
            lst.add(uc);
        }

        MorphiumSingleton.get().store(lst, new AsyncOperationCallback<UncachedObject>() {
            @Override
            public void onOperationSucceeded(AsyncOperationType type, Query<UncachedObject> q, long duration, List<UncachedObject> result, UncachedObject entity, Object... param) {
                log.info("finished after " + duration);
            }

            @Override
            public void onOperationError(AsyncOperationType type, Query<UncachedObject> q, long duration, String error, Throwable t, UncachedObject entity, Object... param) {
                //To change body of implemented methods use File | Settings | File Templates.
            }
        });
        waitForWriteProcessToBeScheduled();
        int c = MorphiumSingleton.get().getWriteBufferCount();
        assert (c != 0);
    }

    private int waitForWriteProcessToBeScheduled() {
        int cnt = 0;
        int c = MorphiumSingleton.get().getWriteBufferCount();
        while (c == 0) {
            c = MorphiumSingleton.get().getWriteBufferCount();
            ++cnt;
            Thread.yield();
            assert (cnt < 1000000);
        }
        return c;
    }
}
