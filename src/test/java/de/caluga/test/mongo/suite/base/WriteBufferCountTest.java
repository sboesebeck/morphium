package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.async.AsyncOperationCallback;
import de.caluga.morphium.async.AsyncOperationType;
import de.caluga.morphium.query.Query;
import de.caluga.test.mongo.suite.data.UncachedObject;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import de.caluga.morphium.Morphium;

/**
 * User: Stephan Bösebeck
 * Date: 15.03.13
 * Time: 19:14
 * <p>
 * TODO: Add documentation here
 */
@Tag("core")
public class WriteBufferCountTest extends MultiDriverTestBase {
    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void testWbCount(Morphium morphium) throws Exception  {
        morphium.dropCollection(UncachedObject.class);

        List<UncachedObject> lst = new ArrayList<>();
        for (int i = 0; i < 10000; i++) {
            UncachedObject uc = new UncachedObject();
            uc.setCounter(1);
            StringBuilder longText = new StringBuilder();
            for (int t = 0; t < 1000; t++) {
                longText.append("-test-").append(i);
            }
            uc.setStrValue(longText.toString());
            lst.add(uc);
        }

        morphium.store(lst, new AsyncOperationCallback<UncachedObject>() {
            @Override
            public void onOperationSucceeded(AsyncOperationType type, Query<UncachedObject> q, long duration, List<UncachedObject> result, UncachedObject entity, Object... param) {
                log.info("finished after " + duration);
            }

            @Override
            public void onOperationError(AsyncOperationType type, Query<UncachedObject> q, long duration, String error, Throwable t, UncachedObject entity, Object... param) {
                log.error("Could not write: " + error);
            }
        });
        waitForWriteProcessToBeScheduled();
        int c = morphium.getWriteBufferCount();
        assert (c != 0);

        long s = System.currentTimeMillis();
        while (TestUtils.countUC(morphium) < 10000) {
            log.info("Count: " + TestUtils.countUC(morphium));
            Thread.sleep(1500);
            assertTrue (System.currentTimeMillis() - s < 15000);
        }
    }

    private int waitForWriteProcessToBeScheduled() {
        int cnt = 0;
        int c = morphium.getWriteBufferCount();
        while (c == 0) {
            c = morphium.getWriteBufferCount();
            ++cnt;
            Thread.yield();
            assert (cnt < 1000000);
        }
        return c;
    }


    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void threadNumberTest(Morphium morphium) {
        ThreadMXBean thbean = ManagementFactory.getThreadMXBean();
        log.info("Running threads: " + thbean.getThreadCount());
    }
}
