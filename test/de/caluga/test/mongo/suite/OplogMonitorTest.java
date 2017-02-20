package de.caluga.test.mongo.suite;

import de.caluga.morphium.Utils;
import de.caluga.morphium.replicaset.OplogListener;
import de.caluga.morphium.replicaset.OplogMonitor;
import de.caluga.test.mongo.suite.data.CachedObject;
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

        Thread.sleep(250);
        assert (gotIt);
        gotIt = false;

        olm.removeListener(lst);
        u = new UncachedObject("test", 123);
        morphium.store(u);
        Thread.sleep(200);
        assert (!gotIt);

        olm.stop();
    }


    @Test
    public void oplogNameSpaceNotificationTest() throws Exception {
        OplogListener lst = new OplogListener() {
            @Override
            public void incomingData(Map<String, Object> data) {
                gotIt = true;
                log.info("Got data:" + Utils.toJsonString(data));
            }
        };
        OplogMonitor olm = new OplogMonitor(morphium, UncachedObject.class);
        olm.addListener(lst);
        olm.start();

        Thread.sleep(100);
        UncachedObject u = new UncachedObject("test", 123);
        morphium.store(u);

        Thread.sleep(1200);
        assert (gotIt);
        gotIt = false;

        CachedObject co = new CachedObject();
        co.setCounter(1000);
        co.setValue("123");
        morphium.store(co);

        Thread.sleep(600);
        assert (!gotIt);

        olm.removeListener(lst);
        u = new UncachedObject("test", 123);
        morphium.store(u);
        Thread.sleep(200);
        assert (!gotIt);

        olm.stop();
    }

    @Test
    public void oplogNameSpaceRegexNotificationTest() throws Exception {
        OplogListener lst = new OplogListener() {
            @Override
            public void incomingData(Map<String, Object> data) {
                gotIt = true;
                log.info("Got data:" + Utils.toJsonString(data));
            }
        };
        OplogMonitor olm = new OplogMonitor(morphium, ".*cached.*", true);
        olm.addListener(lst);
        olm.start();

        Thread.sleep(100);
        UncachedObject u = new UncachedObject("test", 123);
        morphium.store(u);

        Thread.sleep(1200);
        assert (gotIt);
        gotIt = false;

        CachedObject co = new CachedObject();
        co.setCounter(1000);
        co.setValue("123");
        morphium.store(co);

        Thread.sleep(600);
        assert (gotIt);


        olm.removeListener(lst);
        gotIt = false;

        u = new UncachedObject("test", 123);
        morphium.store(u);
        Thread.sleep(200);
        assert (!gotIt);

        olm.stop();
    }
}
