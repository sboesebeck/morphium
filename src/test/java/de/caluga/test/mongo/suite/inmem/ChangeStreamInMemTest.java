package de.caluga.test.mongo.suite.inmem;

import de.caluga.morphium.Utils;
import de.caluga.morphium.changestream.ChangeStreamEvent;
import de.caluga.morphium.changestream.ChangeStreamMonitor;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.DriverTailableIterationCallback;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.commands.UpdateMongoCommand;
import de.caluga.morphium.driver.commands.WatchCommand;
import de.caluga.morphium.driver.wire.MongoConnection;
import de.caluga.test.mongo.suite.base.TestUtils;
import de.caluga.test.mongo.suite.data.ComplexObject;
import de.caluga.test.mongo.suite.data.UncachedObject;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@Tag("inmemory")
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
        TestUtils.waitForConditionToBecomeTrue(2000, "Change stream listener not ready",
            () -> true); // Give listener time to start
        morphium.store(new UncachedObject("test", 123));
        ComplexObject o = new ComplexObject();
        o.setEinText("Text");
        morphium.store(o);
        log.info("waiting for some time!");
        TestUtils.waitForConditionToBecomeTrue(5000, "Expected 2 change events but got " + count,
            () -> count == 2);
        run[0] = false;
        morphium.set(morphium.createQueryFor(UncachedObject.class).f("counter").eq(123), "counter", 7777);
        TestUtils.waitForConditionToBecomeTrue(3000, "Expected 3 change events but got " + count,
            () -> count == 3); //the listener needs to be called to return false ;-)
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
            new Thread(()-> {
                while (run[0]) {
                    try {
                        Thread.sleep(2500);
                    } catch (InterruptedException e) {
                    }

                    morphium.store(new UncachedObject("value", (int)(1 + (Math.random() * 100.0))));
                    log.info("Written");
                    written[0]++;
                    morphium.createQueryFor(UncachedObject.class).f("counter").lt(50).set(UncachedObject.Fields.strValue, "newVal");
                    // morphium.set(morphium.createQueryFor(UncachedObject.class).f("counter").lt(50), UncachedObject.Fields.strValue, "newVal");
                    log.info("updated");
                    written[0]++;
                }
                log.info("Thread finished");
            }).start();
            start = System.currentTimeMillis();
            morphium.watchAsync(UncachedObject.class, true, evt-> {
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
        assertEquals(50, count.get());
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

    @Test
    public void changeStreamResumeAfterTest() throws Exception {
        morphium.dropCollection(UncachedObject.class);
        TestUtils.waitForCollectionToBeDeleted(morphium, UncachedObject.class);

        MongoConnection firstConnection = morphium.getDriver().getPrimaryConnection(null);
        AtomicReference<Map<String, Object>> firstToken = new AtomicReference<>();
        CountDownLatch firstLatch = new CountDownLatch(1);

        DriverTailableIterationCallback firstCallback = new DriverTailableIterationCallback() {
            private volatile boolean running = true;

            @Override
            public void incomingData(Map<String, Object> data, long dur) {
                firstToken.set((Map<String, Object>) data.get("_id"));
                running = false;
                firstLatch.countDown();
            }

            @Override
            public boolean isContinued() {
                return running;
            }
        };

        WatchCommand initialWatch = new WatchCommand(firstConnection)
        .setDb(morphium.getDatabase())
        .setColl(morphium.getMapper().getCollectionName(UncachedObject.class))
        .setFullDocument(WatchCommand.FullDocumentEnum.updateLookup)
        .setBatchSize(1)
        .setMaxTimeMS(5000)
        .setCb(firstCallback);

        Thread initialThread = Thread.ofVirtual().start(() -> {
            try {
                initialWatch.watch();
            } catch (MorphiumDriverException e) {
                throw new RuntimeException(e);
            } finally {
                initialWatch.releaseConnection();
            }
        });

        Thread.sleep(100);

        morphium.store(new UncachedObject("resume-initial", 1));

        if (!firstLatch.await(5, TimeUnit.SECONDS)) {
            fail("did not receive initial change event");
        }

        initialThread.join();
        assertNotNull(firstToken.get());

        morphium.store(new UncachedObject("resume-next", 2));

        MongoConnection resumeConnection = morphium.getDriver().getPrimaryConnection(null);
        CountDownLatch resumeLatch = new CountDownLatch(1);
        List<Map<String, Object>> resumedEvents = Collections.synchronizedList(new ArrayList<>());

        DriverTailableIterationCallback resumeCallback = new DriverTailableIterationCallback() {
            private volatile boolean running = true;

            @Override
            public void incomingData(Map<String, Object> data, long dur) {
                resumedEvents.add(data);
                running = false;
                resumeLatch.countDown();
            }

            @Override
            public boolean isContinued() {
                return running;
            }
        };

        WatchCommand resumeWatch = new WatchCommand(resumeConnection)
        .setDb(morphium.getDatabase())
        .setColl(morphium.getMapper().getCollectionName(UncachedObject.class))
        .setFullDocument(WatchCommand.FullDocumentEnum.updateLookup)
        .setResumeAfter(firstToken.get())
        .setBatchSize(1)
        .setMaxTimeMS(5000)
        .setCb(resumeCallback);

        Thread resumeThread = Thread.ofVirtual().start(() -> {
            try {
                resumeWatch.watch();
            } catch (MorphiumDriverException e) {
                throw new RuntimeException(e);
            } finally {
                resumeWatch.releaseConnection();
            }
        });

        if (!resumeLatch.await(5, TimeUnit.SECONDS)) {
            fail("resumeAfter watch did not receive any event");
        }

        resumeThread.join();

        assertEquals(1, resumedEvents.size());
        Map<String, Object> event = resumedEvents.get(0);
        Map<String, Object> fullDocument = (Map<String, Object>) event.get("fullDocument");
        assertNotNull(fullDocument);
        assertEquals(2, ((Number) fullDocument.get("counter")).intValue());
        assertEquals("resume-next", fullDocument.get("str_value"));
    }

    @Test
    public void changeStreamFullDocumentBeforeChangeTest() throws Exception {
        morphium.dropCollection(UncachedObject.class);
        TestUtils.waitForCollectionToBeDeleted(morphium, UncachedObject.class);

        UncachedObject obj = new UncachedObject("pre-image", 10);
        morphium.store(obj);

        MongoConnection watchConnection = morphium.getDriver().getPrimaryConnection(null);
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Map<String, Object>> eventRef = new AtomicReference<>();

        DriverTailableIterationCallback callback = new DriverTailableIterationCallback() {
            private volatile boolean running = true;

            @Override
            public void incomingData(Map<String, Object> data, long dur) {
                eventRef.set(data);
                running = false;
                latch.countDown();
            }

            @Override
            public boolean isContinued() {
                return running;
            }
        };

        WatchCommand watch = new WatchCommand(watchConnection)
        .setDb(morphium.getDatabase())
        .setColl(morphium.getMapper().getCollectionName(UncachedObject.class))
        .setFullDocument(WatchCommand.FullDocumentEnum.updateLookup)
        .setFullDocumentBeforeChange(WatchCommand.FullDocumentBeforeChangeEnum.whenAvailable)
        .setBatchSize(1)
        .setMaxTimeMS(5000)
        .setCb(callback);

        Thread watcher = Thread.ofVirtual().start(() -> {
            try {
                watch.watch();
            } catch (MorphiumDriverException e) {
                throw new RuntimeException(e);
            } finally {
                watch.releaseConnection();
            }
        });

        Thread.sleep(100);

        MongoConnection updateConnection = morphium.getDriver().getPrimaryConnection(null);
        try {
            new UpdateMongoCommand(updateConnection)
            .setDb(morphium.getDatabase())
            .setColl(morphium.getMapper().getCollectionName(UncachedObject.class))
            .addUpdate(Doc.of(
                                       "q", Doc.of("_id", obj.getMorphiumId()),
                                       "u", Doc.of("$set", Doc.of("counter", 77), "$unset", Doc.of("str_value", "")),
                                       "multi", false,
                                       "upsert", false
                       ))
            .execute();
        } finally {
            morphium.getDriver().releaseConnection(updateConnection);
        }

        if (!latch.await(5, TimeUnit.SECONDS)) {
            fail("no change stream event for update");
        }

        watcher.join();

        Map<String, Object> event = eventRef.get();
        assertNotNull(event);
        Map<String, Object> updateDescription = (Map<String, Object>) event.get("updateDescription");
        assertNotNull(updateDescription);

        Map<String, Object> updatedFields = (Map<String, Object>) updateDescription.get("updatedFields");
        List<String> removedFields = (List<String>) updateDescription.get("removedFields");

        assertTrue(updatedFields.containsKey("counter"));
        assertEquals(77, ((Number) updatedFields.get("counter")).intValue());
        assertTrue(removedFields.contains("str_value"));

        Map<String, Object> beforeDoc = (Map<String, Object>) event.get("fullDocumentBeforeChange");
        assertNotNull(beforeDoc);
        assertEquals(10, ((Number) beforeDoc.get("counter")).intValue());
        assertEquals("pre-image", beforeDoc.get("str_value"));

        Map<String, Object> fullDoc = (Map<String, Object>) event.get("fullDocument");
        assertNotNull(fullDoc);
        assertEquals(77, ((Number) fullDoc.get("counter")).intValue());
        assertFalse(fullDoc.containsKey("str_value"));
    }
}
