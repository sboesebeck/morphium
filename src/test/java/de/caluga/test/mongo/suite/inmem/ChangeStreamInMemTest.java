package de.caluga.test.mongo.suite.inmem;

import de.caluga.morphium.Utils;
import de.caluga.morphium.changestream.ChangeStreamEvent;
import de.caluga.morphium.changestream.ChangeStreamMonitor;
import de.caluga.test.mongo.suite.base.TestUtils;
import de.caluga.test.mongo.suite.data.ComplexObject;
import de.caluga.test.mongo.suite.data.UncachedObject;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class ChangeStreamInMemTest extends MorphiumInMemTestBase {
    long start;
    long count;

    @Test
    public void changeStreamDatabaseTest() throws Exception {
        morphium.dropCollection(ComplexObject.class);
        morphium.dropCollection(UncachedObject.class);
        count = 0;
        final boolean[] run = {true};
        morphium.watchDbAsync(morphium.getDatabase(), true, null, evt->{
            if (evt.getOperationType().equals("drop")) return true;
            printevent(evt);
            count++;
            log.info("Returning " + run[0]);
            log.info("===========");
            return run[0];
        });
        Thread.sleep(200); //waiting for async listener to be installed
        morphium.store(new UncachedObject("test", 123));
        ComplexObject o = new ComplexObject();
        o.setEinText("Text");
        morphium.store(o);
        log.info("waiting for some time!");
        Thread.sleep(2500);
        run[0] = false;
        assert(count == 2) : "Count = " + count;
        morphium.set(morphium.createQueryFor(UncachedObject.class).f("counter").eq(123), "counter", 7777);
        Thread.sleep(1550);
        assert(count == 3) : "Count wrong " + count + "!=3";  //the listener needs to be called to return false ;-)
        morphium.store(new UncachedObject("test", 123)); //to have the monitor stop
        assert(3 == count) : "Count wrong " + count + "!=3";
        morphium.store(new UncachedObject("test again", 124));
        assert(3 == count) : "Count wrong " + count + "!=3";  //monitor should have stopped by now
    }

    @Test
    public void changeStreamBackgroundTest() throws Exception {
        morphium.dropCollection(UncachedObject.class);
        final boolean[] run = {true};

        try {
            final int[] count = {0};
            final int[] written = {0};
            new Thread(()->{
                while (run[0]) {
                    try {
                        Thread.sleep(2500);
                    } catch (InterruptedException e) {
                    }

                    morphium.store(new UncachedObject("value", (int)(1 + (Math.random() * 100.0))));
                    log.info("Written");
                    written[0]++;
                    morphium.set(morphium.createQueryFor(UncachedObject.class).f("counter").lt(50), UncachedObject.Fields.strValue, "newVal");
                    log.info("updated");
                    written[0]++;
                }
                log.info("Thread finished");
            }).start();
            start = System.currentTimeMillis();
            morphium.watchAsync(UncachedObject.class, true, evt->{
                if (evt.getOperationType().equals("drop")) return true;
                count[0]++;
                printevent(evt);
                return run[0];
            });
            log.info("waiting for some time!");
            Thread.sleep(8500);
            run[0] = false;
            assert(count[0] > 0 && count[0] >= written[0] - 3) : "Wrong count: " + count[0] + " written: " + written[0];
        } finally {
            run[0] = false;
            // morphium.store(new UncachedObject("value", (int) (1 + (Math.random() * 100.0))));
        }

        Thread.sleep(2000);
    }

    @Test
    public void changeStreamInsertTest() throws Exception {
        morphium.dropCollection(UncachedObject.class);
        final boolean[] run = {true};
        int[] count = {0};
        int[] written = {0};
        new Thread(()->{
            while (run[0]) {
                try {
                    Thread.sleep(2500);
                } catch (InterruptedException e) {
                }

                log.info("Writing...");

                if (morphium != null) {
                    //when shutting down test, this might be null
                    morphium.store(new UncachedObject("value", 123));
                }

                written[0]++;
            }
        }).start();
        start = System.currentTimeMillis();
        morphium.watch(UncachedObject.class, true, evt->{
            if (evt.getOperationType().equals("drop")) return true;
            printevent(evt);
            count[0]++;
            if (count[0] > 10) {
                run[0] = false;
            }
            return System.currentTimeMillis() - start < 8500;
        });
        assert(count[0] >= written[0] - 1 && count[0] <= written[0]);
        log.info("Stopped!");
        run[0] = false;
        Thread.sleep(1000);
    }

    @Test
    @Disabled
    public void changeStreamUpdateTest() throws Exception {
        morphium.dropCollection(UncachedObject.class);
        TestUtils.waitForCollectionToBeDeleted(morphium, UncachedObject.class);
        createUncachedObjects(100);
        log.info("Init finished...");
        final AtomicBoolean run = new AtomicBoolean(true);
        final AtomicInteger count = new AtomicInteger();
        start = System.currentTimeMillis();
        long start = System.currentTimeMillis();
        new Thread(()->{
            try {
                Thread.sleep(2500);
            } catch (InterruptedException e) {
            }
            int i = 50;

            while (run.get()) {
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                }

                if (System.currentTimeMillis() - start > 26000) {
                    log.error("Error - took too long!");
                    run.set(false);
                }

                log.info("Setting to value " + i);
                morphium.set(morphium.createQueryFor(UncachedObject.class).f("counter").lte(50), "strValue", "new value " + i, false, true);
            }
            log.info("Writing thread finished...");
        }).start();
        log.info("Watching...");
        morphium.watch(UncachedObject.class, true, evt->{
            if (evt.getOperationType().equals("drop")) return true;
            printevent(evt);
            count.incrementAndGet();
            log.info("count: " + count.get());
            if (count.get() == 50) {
                run.set(false);
                return false;
            }
            return true;
        });
        assertEquals(50,count.get());
        log.info("Quitting");
        run.set(false);
        Thread.sleep(500);
    }

    private void printevent(ChangeStreamEvent evt) {
        log.info("---------");
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
            assertNotNull(obj);
            ;
        }
    }

    @Test
    public void changeStreamMonitorTest() throws Exception {
        morphium.dropCollection(UncachedObject.class);
        ChangeStreamMonitor m = new ChangeStreamMonitor(morphium, UncachedObject.class);
        m.start();
        final AtomicInteger cnt = new AtomicInteger(0);
        m.addListener(evt->{
            if (evt.getOperationType().equals("drop")) return true;
            printevent(evt);
            return cnt.incrementAndGet() != 100;
        });
        Thread.sleep(1000);

        for (int i = 0; i < 100; i++) {
            morphium.store(new UncachedObject("value " + i, i));
        }

        Thread.sleep(5000);
        m.terminate();
        assert(cnt.get() == 100) : "count is wrong: " + cnt.get();
    }
}
