package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumAccessVetoException;
import de.caluga.morphium.async.AsyncCallbackAdapter;
import de.caluga.morphium.async.AsyncOperationType;
import de.caluga.morphium.query.Query;
import de.caluga.test.mongo.suite.data.CappedCol;
import org.junit.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;

/**
 * Created by stephan on 29.07.16.
 */

public class TailableQueryTests extends MultiDriverTestBase {
    boolean found = false;

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void tailableTest(Morphium m) throws Exception {
        try (m) {
            m.dropCollection(CappedCol.class);
            CappedCol o = new CappedCol("Test1", 1);
            m.store(o);
            m.store(new CappedCol("Test 2", 2));
            Thread.sleep(1000);
            found = false;
            new Thread(() -> {
                Query<CappedCol> q = m.createQueryFor(CappedCol.class);
                q.tail(10, 0, new AsyncCallbackAdapter<CappedCol>() {
                    @Override
                    public void onOperationSucceeded(AsyncOperationType type, Query<CappedCol> q, long duration, List<CappedCol> result, CappedCol entity, Object... param) {
                        log.info("Got incoming!!! " + entity.getStrValue() + " " + entity.getCounter());
                        found = true;
                        if (entity.getStrValue().equals("Test 3 - quit")) {
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
}
