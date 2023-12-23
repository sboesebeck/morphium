package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.Utils;
import de.caluga.morphium.UtilsMap;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.changestream.ChangeStreamEvent;
import de.caluga.morphium.changestream.ChangeStreamMonitor;
import de.caluga.morphium.driver.wire.SingleMongoConnectDriver;
import de.caluga.test.mongo.suite.data.ComplexObject;
import de.caluga.test.mongo.suite.data.UncachedObject;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

public class ChangeStreamTest extends MultiDriverTestBase {
    long start;
    long count;

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void changeStreamDatabaseTest(Morphium morphium) throws Exception {
        try (morphium) {
            log.info(String.format("=====================> Running Test with %s <===============================",morphium.getDriver().getName()));
            morphium.dropCollection(ComplexObject.class);
            morphium.dropCollection(UncachedObject.class);
            count = 0;
            Thread.sleep(1500);
            var runningFlag = morphium.watchDbAsync(morphium.getDatabase(), true, null, evt->{
                log.info("Incoming event!");
                printevent(morphium, evt);
                count++;
                return true;
            });
            Thread.sleep(1000);
            MorphiumConfig cfg = MorphiumConfig.fromProperties(morphium.getConfig().asProperties());
            Morphium m2 = morphium;

            if ((morphium.getDriver() instanceof SingleMongoConnectDriver)) {
                cfg.setCredentialsEncryptionKey("1234567890abcdef");
                cfg.setCredentialsDecryptionKey("1234567890abcdef");
                m2 = new Morphium(cfg);
            }

            m2.store(new UncachedObject("test", 123));
            ComplexObject o = new ComplexObject();
            o.setEinText("Text");
            m2.store(o);
            log.info("waiting for some time!");
            TestUtils.waitForConditionToBecomeTrue(15000, "count did not raise", ()->count > 1);
            runningFlag.set(false);
            assertTrue(count >= 2 && count <= 3);
            long cnt = count;
            m2.set(m2.createQueryFor(UncachedObject.class).f("counter").eq(123), "counter", 7777);
            Thread.sleep(1550);
            assertEquals(cnt + 1, count);

            if ((morphium.getDriver() instanceof SingleMongoConnectDriver)) {
                m2.close();
            }

            log.info("Finished!");
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void changeStreamBackgroundTest(Morphium morphium) throws Exception {
        try (morphium) {
            if (morphium.getDriver() instanceof SingleMongoConnectDriver) { return; }

            log.info("================================> Running test with: " + morphium.getDriver().getName());
            morphium.dropCollection(UncachedObject.class);
            final AtomicBoolean run = new AtomicBoolean(true);

            try {
                final var count = new AtomicInteger(0);
                final var written = new AtomicInteger(0);
                var t = new Thread(()->{
                    try {
                        while (run.get()) {
                            try {
                                Thread.sleep(2500);
                            } catch (InterruptedException e) {
                            }

                            morphium.store(new UncachedObject("value", (int)(1 + (Math.random() * 100.0))));
                            log.info(morphium.getDriver().getName() + ": Written");
                            morphium.set(morphium.createQueryFor(UncachedObject.class).f("counter").lt(50), "strValue", "newVal");
                            log.info(morphium.getDriver().getName() + ": updated");
                            written.incrementAndGet();
                        }

                        log.info(morphium.getDriver().getName() + ": Thread finished");
                    } catch (Exception e) {
                        log.error(morphium.getDriver().getName() + ": Error in Thread", e);
                    }
                });
                t.start();
                start = System.currentTimeMillis();
                morphium.watchAsync(UncachedObject.class, true, evt->{
                    count.incrementAndGet();
                    printevent(morphium, evt);
                    return run.get();
                });
                Thread.sleep(500);
                TestUtils.waitForConditionToBecomeTrue(5000, morphium.getDriver().getName() + ": no writes?", ()->written.get() > 0);
                TestUtils.waitForConditionToBecomeTrue(5000, morphium.getDriver().getName() + ": no incoming events?", ()->count.get() > 1);
                Thread.sleep(1000);
                log.info(morphium.getDriver().getName() + ": Stopping thread");
                run.set(false);
                t.join();
                start = System.currentTimeMillis();

                while (!(count.get() > 0 && count.get() * 2 >= written.get() - 2)) {
                    Thread.sleep(500);
                    log.info(morphium.getDriver().getName() + ": Wrong count: " + count.get() + " written: " + written.get());
                    assert(System.currentTimeMillis() - start < 10000);
                }

                log.info("finished.");
            } finally {
                run.set(false);
                morphium.store(new UncachedObject("value", (int)(1 + (Math.random() * 100.0))));
            }

            Thread.sleep(2000);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void changeStreamInsertTest(Morphium morphium) throws Exception {
        try (morphium) {
            log.info(String.format("=====================> Running Test with %s <===============================",morphium.getDriver().getName()));
            //morphium.dropCollection(UncachedObject.class);
            final var run = new AtomicBoolean(true);
            final var count = new AtomicInteger(0);
            final var written = new AtomicInteger(0);
            new Thread(()->{
                while (run.get()) {
                    try {
                        Thread.sleep(2500);
                    } catch (InterruptedException e) {
                    }

                    log.info("Writing...");
                    MorphiumConfig cfg = MorphiumConfig.fromProperties(morphium.getConfig().asProperties());
                    Morphium m2 = morphium;

                    if ((morphium.getDriver() instanceof SingleMongoConnectDriver)) {
                        cfg.setCredentialsEncryptionKey("1234567890abcdef");
                        cfg.setCredentialsDecryptionKey("1234567890abcdef");
                        m2 = new Morphium(cfg);
                    }

                    m2.store(new UncachedObject("value", 123));
                    written.incrementAndGet();

                    if ((morphium.getDriver() instanceof SingleMongoConnectDriver)) {
                        m2.close();
                    }
                }
            }).start();
            start = System.currentTimeMillis();
            morphium.watch(UncachedObject.class, true, evt->{
                printevent(morphium, evt);
                count.incrementAndGet();
                return System.currentTimeMillis() - start < 8500;
            });
            assert(count.get() >= written.get() - 1 && count.get() <= written.get());
            log.info("Stopped!");
            run.set(false);
            Thread.sleep(2500);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void changeStreamUpdateTest(Morphium morphium) throws Exception {
        try (morphium) {
            log.info(String.format("=====================> Running Test with %s <===============================",morphium.getDriver().getName()));
            //morphium.dropCollection(UncachedObject.class);
            Thread.sleep(1500);
            createUncachedObjects(morphium, 100);
            log.info("Init finished...");
            final var run = new AtomicBoolean(true);
            final var count = new AtomicInteger(0);
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
                    MorphiumConfig cfg = MorphiumConfig.fromProperties(morphium.getConfig().asProperties());
                    Morphium m2 = morphium;

                    if ((morphium.getDriver() instanceof SingleMongoConnectDriver)) {
                        cfg.setCredentialsEncryptionKey("1234567890abcdef");
                        cfg.setCredentialsDecryptionKey("1234567890abcdef");
                        m2 = new Morphium(cfg);
                    }

                    m2.set(morphium.createQueryFor(UncachedObject.class).f("counter").lte(50), "str_value", "new value " + i, false, true);

                    if ((morphium.getDriver() instanceof SingleMongoConnectDriver)) {
                        m2.close();
                    }
                }
                log.info("Writing thread finished...");
            }).start();
            log.info("Watching...");
            morphium.watch(UncachedObject.class, true, evt->{
                printevent(morphium, evt);
                count.incrementAndGet();
                log.info("count: " + count.get());

                if (count.get() == 50) {
                    run.set(false);
                    return false;
                }
                return true;
            });
            assertTrue(count.get() >= 50);
            assertFalse(run.get());
            log.info("Quitting");
        }
    }

    private void printevent(Morphium morphium, ChangeStreamEvent evt) {
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

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void changeStreamMonitorTest(Morphium morphium) throws Exception {
        try (morphium) {
            log.info(String.format("=====================> Running Test with %s <===============================",morphium.getDriver().getName()));
            morphium.dropCollection(UncachedObject.class);
            ChangeStreamMonitor m = new ChangeStreamMonitor(morphium, UncachedObject.class);
            m.start();
            final AtomicInteger cnt = new AtomicInteger(0);
            m.addListener(evt->{
                printevent(morphium, evt);
                cnt.incrementAndGet();
                return true;
            });
            Thread.sleep(1000);

            for (int i = 0; i < 100; i++) {
                morphium.store(new UncachedObject("value " + i, i));
            }

            Thread.sleep(5000);
            m.terminate();
            assert(cnt.get() >= 100 && cnt.get() <= 101) : "count is wrong: " + cnt.get();
            morphium.store(new UncachedObject("killing", 0));
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void changeStreamMonitorCollectionTest(Morphium morphium) throws Exception {
        try (morphium) {
            log.info(String.format("=====================> Running Test with %s <===============================",morphium.getDriver().getName()));
            morphium.dropCollection(UncachedObject.class, "uncached_object", null);
            ChangeStreamMonitor m = new ChangeStreamMonitor(morphium, "uncached_object", false, null);
            m.start();
            final AtomicInteger cnt = new AtomicInteger(0);
            m.addListener(evt->{
                printevent(morphium, evt);
                cnt.set(cnt.get() + 1);
                return true;
            });
            Thread.sleep(1000);

            for (int i = 0; i < 100; i++) {
                morphium.store(new UncachedObject("value " + i, i), "uncached_object", null);
            }

            Thread.sleep(5000);
            m.terminate();
            assert(cnt.get() >= 100 && cnt.get() <= 101) : "count is wrong: " + cnt.get();
            morphium.store(new UncachedObject("killing", 0));
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void terminateChangeStreamTest(Morphium morphium) throws Exception {
        try (morphium) {
            log.info(String.format("=====================> Running Test with %s <===============================",morphium.getDriver().getName()));
            for (int i = 0; i < 3; i++) {
                ChangeStreamMonitor m = new ChangeStreamMonitor(morphium, UncachedObject.class);
                m.start();
                m.addListener(evt->{
                    printevent(morphium, evt);
                    return true;
                });
                morphium.store(new UncachedObject("value", 42));
                Thread.sleep(100);
                m.terminate();
            }
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoInMem")
    public void changeStreamPipelineTest(Morphium morphium) throws Exception {
        try (morphium) {
            log.info(String.format("=====================> Running Test with %s <===============================",morphium.getDriver().getName()));
            List<Map<String, Object>> pipeline = new ArrayList<>();
            pipeline.add(UtilsMap.of("$match", UtilsMap.of("operationType", UtilsMap.of("$in", Arrays.asList("insert")))));
            ChangeStreamMonitor mon = new ChangeStreamMonitor(morphium, "uncached_object", false, pipeline);
            final AtomicInteger inserts = new AtomicInteger();
            final AtomicInteger updates = new AtomicInteger();
            final AtomicInteger deletes = new AtomicInteger();
            mon.addListener(evt->{
                if (evt.getOperationType().equals("insert")) {
                    log.info("The strValue inserted: " + evt.getFullDocument().get("str_value"));
                    inserts.incrementAndGet();
                }
                if (evt.getOperationType().equals("update")) {
                    updates.incrementAndGet();
                }
                if (evt.getOperationType().equals("delete")) {
                    deletes.incrementAndGet();
                }
                assert(evt.getOperationType().equals("insert"));
                return true;
            });
            mon.start();

            for (int i = 0; i < 10; i++) {
                morphium.store(new UncachedObject("changeStreamPipelineTestValue " + i, i), "uncached_object", null);
            }

            morphium.set(morphium.createQueryFor(UncachedObject.class).setCollectionName("uncached_object"), "strValue", "updated");
            morphium.delete(morphium.createQueryFor(UncachedObject.class).setCollectionName("uncached_object"));
            Thread.sleep(2500);
            assert(inserts.get() == 10) : "Wrong number of inserts: " + inserts.get();
            assert(updates.get() == 0);
            assert(deletes.get() == 0);
            mon.terminate();
            log.info("Resetting counters");
            inserts.set(0);
            updates.set(0);
            deletes.set(0);
            pipeline = new ArrayList<>();
            pipeline.add(UtilsMap.of("$match", UtilsMap.of("operationType", UtilsMap.of("$in", Arrays.asList("update")))));
            mon = new ChangeStreamMonitor(morphium, "uncached_object", false, pipeline);
            mon.addListener(evt->{
                if (evt.getOperationType().equals("insert")) {
                    if (evt.getFullDocument().get("str_value").equals("value")) {
                        log.info("got an old store");
                    }

                    inserts.incrementAndGet();
                }
                if (evt.getOperationType().equals("update")) {
                    updates.incrementAndGet();
                }
                if (evt.getOperationType().equals("delete")) {
                    deletes.incrementAndGet();
                }
                return true;
            });
            mon.start();
            Thread.sleep(1000);

            for (int i = 0; i < 10; i++) {
                morphium.store(new UncachedObject("changeStreamPipelineTestOtherValue " + i, i), "uncached_object", null);
            }

            morphium.set(morphium.createQueryFor(UncachedObject.class).setCollectionName("uncached_object"), "strValue", "updated");
            Thread.sleep(2000);
            assertEquals(0, inserts.get());
            assertEquals(1, updates.get());
            assertEquals(0, deletes.get());
            morphium.set(morphium.createQueryFor(UncachedObject.class).setCollectionName("uncached_object"), "str_value", "updated", false, true);
            Thread.sleep(1000);
            assert(updates.get() >= 10) : "Updates wrong: " + updates.get();
            mon.terminate();
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void testStringId(Morphium morphium) throws Exception {
        try (morphium) {
            log.info(String.format("=====================> Running Test with %s <===============================",morphium.getDriver().getName()));
            final AtomicInteger cnt = new AtomicInteger();
            morphium.dropCollection(StringIdEntity.class);
            Thread.sleep(1000);
            ChangeStreamMonitor m = new ChangeStreamMonitor(morphium, "string_id_entity", true, null);
            m.start();
            m.addListener(evt->{
                log.info("incoming event " + evt.getOperationType());
                log.info(Utils.toJsonString(evt.getFullDocument()));
                //printevent(morphium,evt);
                cnt.incrementAndGet();
                return true;
            });
            StringIdEntity i = new StringIdEntity();
            i.id = "test1";
            i.name = "test";
            i.value = new Integer(23);
            morphium.store(i);
            Thread.sleep(1000);
            assertEquals(1, cnt.get());
            i.name = "neuer Testt";
            morphium.store(i);
            Thread.sleep(1000);
            assert(cnt.get() == 2);
            m.terminate();
        }
    }

    @Entity
    public static class StringIdEntity {
        @Id
        public String id;
        public String name;
        public Integer value;
    }
}
