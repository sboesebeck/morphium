package de.caluga.test.mongo.suite;

import de.caluga.morphium.Utils;
import de.caluga.morphium.changestream.ChangeStreamEvent;
import de.caluga.morphium.changestream.ChangeStreamMonitor;
import de.caluga.test.mongo.suite.data.ComplexObject;
import de.caluga.test.mongo.suite.data.UncachedObject;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

public class ChangeStreamTest extends MongoTest {
    long start;
    long count;


    @Test
    public void changeStreamDatabaseTest() throws Exception {
        morphium.dropCollection(ComplexObject.class);
        morphium.dropCollection(UncachedObject.class);
        count = 0;
        final boolean run[] = {true};
        morphium.watchDbAsync(true, evt -> {
            printevent(evt);
            count++;
            return run[0];
        });
        morphium.store(new UncachedObject("test", 123));
        ComplexObject o = new ComplexObject();
        o.setEinText("Text");
        morphium.store(o);

        log.info("waiting for some time!");
        Thread.sleep(1000);
        run[0] = false;
        assert (count >= 2 && count <= 3) : "Count = " + count;
    }

    @Test
    public void changeStreamBackgroundTest() throws Exception {
        morphium.dropCollection(UncachedObject.class);
        final boolean run[] = {true};
        final int count[] = {0};
        final int written[] = {0};
        new Thread() {
            public void run() {
                while (run[0]) {
                    try {
                        sleep(500);
                    } catch (InterruptedException e) {
                    }
                    morphium.store(new UncachedObject("value", (int) (1 + (Math.random() * 100.0))));
                    written[0]++;
                    morphium.set(morphium.createQueryFor(UncachedObject.class).f("counter").lt(50), "value", "newVal");
                    written[0]++;
                }
            }
        }.start();
        start = System.currentTimeMillis();
        morphium.watchAsync(UncachedObject.class, true, evt -> {
            count[0]++;
            printevent(evt);
            return run[0];
        });


        log.info("waiting for some time!");
        Thread.sleep(5500);
        run[0] = false;
        assert (count[0] > 0 && count[0] >= written[0] - 2) : "Wrong count: " + count[0] + " written: " + written[0];

    }

    @Test
    public void changeStreamInsertTest() throws Exception {
        morphium.dropCollection(UncachedObject.class);
        final boolean run[] = {true};
        int count[] = {0};
        int written[] = {0};
        new Thread(() -> {
            while (run[0]) {
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                }
                log.info("Writing...");
                morphium.store(new UncachedObject("value", 123));
                written[0]++;
            }
        }).start();
        start = System.currentTimeMillis();
        morphium.watch(UncachedObject.class, true, evt -> {
            printevent(evt);
            count[0]++;
            return System.currentTimeMillis() - start < 5500;
        });

        assert (count[0] >= written[0] - 1 && count[0] <= written[0]);
        log.info("Stopped!");
        run[0] = false;
        Thread.sleep(1000);

    }

    @Test
    public void changeStreamUpdateTest() {
        createUncachedObjects(100);
        log.info("Init finished...");
        final boolean run[] = {true};
        final int count[] = {0};
        start = System.currentTimeMillis();

        long start = System.currentTimeMillis();
        new Thread(() -> {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
            }
            while (run[0]) {
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                }
                assert (System.currentTimeMillis() - start < 25000) : "Error - took too long!";
                log.info("Updating");
                morphium.set(morphium.createQueryFor(UncachedObject.class).f("counter").lte(50), "value", "new value", false, true);
            }
        }).start();
        log.info("Watching...");
        morphium.watch(UncachedObject.class, true, evt -> {
            printevent(evt);
            count[0]++;
            log.info("count: " + count[0]);
            return count[0] < 50;
        });
        assert (count[0] == 50);
        log.info("Quitting");
        run[0] = false;

    }


    private void printevent(ChangeStreamEvent evt) {
        log.info("type: " + evt.getOperationType());
        log.info("time: " + evt.getClusterTime());
        log.info("dkey: " + evt.getDocumentKey());
        log.info("tx  : " + evt.getTxnNumber());
        if (evt.getLsid() != null) {
            log.info("lsid: " + Utils.toJsonString(evt.getLsid()));
        }
        if (evt.getUpdateDescription() != null) {
            log.info("desc:" + Utils.toJsonString(evt.getUpdateDescription()));
        }
        log.info(Utils.toJsonString(evt.getFullDocument()));
        if (!evt.getOperationType().equals("invalidate")) {
            UncachedObject obj = evt.getEntityFromData(UncachedObject.class, morphium);
            assert (obj != null);
        }
    }

    @Test
    public void changeStreamMonitorTest() throws Exception {
        morphium.dropCollection(UncachedObject.class);
        ChangeStreamMonitor m = new ChangeStreamMonitor(morphium, UncachedObject.class);
        m.start();
        final AtomicInteger cnt = new AtomicInteger(0);

        m.addListener(evt -> {
            printevent(evt);
            cnt.set(cnt.get() + 1);
            return true;
        });
        Thread.sleep(1000);
        for (int i = 0; i < 100; i++) {
            morphium.store(new UncachedObject("value " + i, i));
        }
        Thread.sleep(5000);
        m.stop();
        assert (cnt.get() >= 100 && cnt.get() <= 101) : "count is wrong: " + cnt.get();
        morphium.store(new UncachedObject("killing", 0));

    }
}
