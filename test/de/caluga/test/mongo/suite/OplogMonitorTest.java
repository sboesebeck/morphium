package de.caluga.test.mongo.suite;

import de.caluga.morphium.Utils;
import de.caluga.morphium.replicaset.OplogListener;
import de.caluga.morphium.replicaset.OplogMonitor;
import de.caluga.test.mongo.suite.data.UncachedObject;
import org.junit.Test;

import java.util.Map;

/**
 * Created by stephan on 15.11.16.
 */
public class OplogMonitorTest extends MongoTest {
    private boolean gotIt = false;

    @Test
    public void oplogNotificationTest() throws Exception {
        OplogListener lst = new OplogListener() {
            @Override
            public void incomingData(Map<String, Object> data) {
                log.info(Utils.toJsonString(data));
                gotIt = true;
            }
        };
        OplogMonitor olm = new OplogMonitor(morphium);
        olm.addListener(lst);
        olm.start();

        Thread.sleep(100);
        UncachedObject u = new UncachedObject("test", 123);
        morphium.store(u);

        Thread.sleep(100);
        assert (gotIt);
        gotIt = false;

        olm.removeListener(lst);
        u = new UncachedObject("test", 123);
        morphium.store(u);
        Thread.sleep(200);
        assert (!gotIt);

        olm.stop();
    }

}
