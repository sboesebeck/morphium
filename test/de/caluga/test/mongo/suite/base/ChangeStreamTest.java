package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.Utils;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.changestream.ChangeStreamEvent;
import de.caluga.morphium.changestream.ChangeStreamMonitor;
import de.caluga.test.mongo.suite.data.ComplexObject;
import de.caluga.test.mongo.suite.data.UncachedObject;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class ChangeStreamTest extends MorphiumTestBase {
    long start;
    long count;


    @Test
    public void changeStreamDatabaseTest() throws Exception {
        morphium.dropCollection(ComplexObject.class);
        morphium.dropCollection(UncachedObject.class);
        count = 0;
        final boolean[] run = {true};
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
        Thread.sleep(1500);
        run[0] = false;
        assert (count >= 2 && count <= 3) : "Count = " + count;
        long cnt = count;
        morphium.set(morphium.createQueryFor(UncachedObject.class).f("counter").eq(123), "counter", 7777);
        Thread.sleep(550);
        assert (cnt + 1 == count) : "Count wrong " + cnt + "!=" + count + "+1";
    }

    @Test
    public void changeStreamBackgroundTest() throws Exception {
        morphium.dropCollection(UncachedObject.class);
        final boolean[] run = {true};
        try {
            final int[] count = {0};
            final int[] written = {0};
            new Thread(() -> {
                while (run[0]) {
                    try {
                        Thread.sleep(2500);
                    } catch (InterruptedException e) {
                    }
                    morphium.store(new UncachedObject("value", (int) (1 + (Math.random() * 100.0))));
                    log.info("Written");
                    written[0]++;
                    morphium.set(morphium.createQueryFor(UncachedObject.class).f("counter").lt(50), "value", "newVal");
                    log.info("updated");
                    written[0]++;
                }
                log.info("Thread finished");
            }).start();
            start = System.currentTimeMillis();
            morphium.watchAsync(UncachedObject.class, true, evt -> {
                count[0]++;
                printevent(evt);
                return run[0];
            });


            log.info("waiting for some time!");
            Thread.sleep(8500);
            run[0] = false;
            assert (count[0] > 0 && count[0] >= written[0] - 2) : "Wrong count: " + count[0] + " written: " + written[0];
        } finally {
            run[0] = false;
            morphium.store(new UncachedObject("value", (int) (1 + (Math.random() * 100.0))));
        }
        Thread.sleep(2000);

    }

    @Test
    public void changeStreamInsertTest() throws Exception {
        //morphium.dropCollection(UncachedObject.class);
        final boolean[] run = {true};
        int[] count = {0};
        int[] written = {0};
        new Thread(() -> {
            while (run[0]) {
                try {
                    Thread.sleep(2500);
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
            return System.currentTimeMillis() - start < 8500;
        });

        assert (count[0] >= written[0] - 1 && count[0] <= written[0]);
        log.info("Stopped!");
        run[0] = false;
        Thread.sleep(2500);

    }

    @Test
    public void changeStreamUpdateTest() throws Exception {
        //morphium.dropCollection(UncachedObject.class);
        Thread.sleep(1500);
        createUncachedObjects(100);
        log.info("Init finished...");
        final boolean[] run = {true};
        final int[] count = {0};
        start = System.currentTimeMillis();

        long start = System.currentTimeMillis();
        new Thread(() -> {
            try {
                Thread.sleep(2500);
            } catch (InterruptedException e) {
            }
            int i = 50;
            while (run[0]) {
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                }
                if (System.currentTimeMillis() - start > 26000) {
                    log.error("Error - took too long!");
                    run[0] = false;
                }
                log.info("Setting to value " + i);
                morphium.set(morphium.createQueryFor(UncachedObject.class).f("counter").lte(50), "str_value", "new value " + i, false, true);
            }
            log.info("Writing thread finished...");
        }).start();
        log.info("Watching...");
        morphium.watch(UncachedObject.class, true, evt -> {
            printevent(evt);
            count[0]++;
            log.info("count: " + count[0]);
            if (count[0] == 50) {
                run[0] = false;
                return false;
            }
            return true;
        });
        assert (count[0] == 50);
        log.info("Quitting");
        run[0] = false;
        Thread.sleep(500);

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
        m.terminate();
        assert (cnt.get() >= 100 && cnt.get() <= 101) : "count is wrong: " + cnt.get();
        morphium.store(new UncachedObject("killing", 0));

    }

    @Test
    public void changeStreamMonitorCollectionTest() throws Exception {
        morphium.dropCollection(UncachedObject.class, "uncached_object", null);
        ChangeStreamMonitor m = new ChangeStreamMonitor(morphium, "uncached_object", false, null);
        m.start();
        final AtomicInteger cnt = new AtomicInteger(0);

        m.addListener(evt -> {
            printevent(evt);
            cnt.set(cnt.get() + 1);
            return true;
        });
        Thread.sleep(1000);
        for (int i = 0; i < 100; i++) {
            morphium.store(new UncachedObject("value " + i, i), "uncached_object", null);
        }
        Thread.sleep(5000);
        m.terminate();
        assert (cnt.get() >= 100 && cnt.get() <= 101) : "count is wrong: " + cnt.get();
        morphium.store(new UncachedObject("killing", 0));

    }

    @Test
    public void terminateChangeStreamTest() throws Exception {
        for (int i = 0; i < 10; i++) {
            ChangeStreamMonitor m = new ChangeStreamMonitor(morphium, UncachedObject.class);
            m.start();
            m.addListener(evt -> {
                printevent(evt);
                return true;
            });
            morphium.store(new UncachedObject("value", 42));
            Thread.sleep(100);
            m.terminate();
        }
    }


    @Test
    public void changeStreamPipelineTest() throws Exception {
        List<Map<String, Object>> pipeline = new ArrayList<>();
        pipeline.add(Utils.getMap("$match", Utils.getMap("operationType", Utils.getMap("$in", Arrays.asList("insert")))));
        ChangeStreamMonitor mon = new ChangeStreamMonitor(morphium, "uncached_object", false, pipeline);
        final AtomicInteger inserts = new AtomicInteger();
        final AtomicInteger updates = new AtomicInteger();
        final AtomicInteger deletes = new AtomicInteger();
        mon.addListener(evt -> {
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
            assert (evt.getOperationType().equals("insert"));
            return true;
        });
        mon.start();

        for (int i = 0; i < 10; i++)
            morphium.store(new UncachedObject("changeStreamPipelineTestValue " + i, i), "uncached_object", null);

        morphium.set(morphium.createQueryFor(UncachedObject.class).setCollectionName("uncached_object"), "strValue", "updated");
        morphium.delete(morphium.createQueryFor(UncachedObject.class).setCollectionName("uncached_object"));
        Thread.sleep(1500);
        assert (inserts.get() == 10) : "Wrong number of inserts: " + inserts.get();
        assert (updates.get() == 0);
        assert (deletes.get() == 0);
        mon.terminate();

        log.info("Resetting counters");
        inserts.set(0);
        updates.set(0);
        deletes.set(0);
        pipeline = new ArrayList<>();
        pipeline.add(Utils.getMap("$match", Utils.getMap("operationType", Utils.getMap("$in", Arrays.asList("update")))));
        mon = new ChangeStreamMonitor(morphium, "uncached_object", false, pipeline);

        mon.addListener(evt -> {
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

        for (int i = 0; i < 10; i++)
            morphium.store(new UncachedObject("changeStreamPipelineTestOtherValue " + i, i), "uncached_object", null);

        morphium.set(morphium.createQueryFor(UncachedObject.class).setCollectionName("uncached_object"), "strValue", "updated");
        Thread.sleep(1000);
        assert (inserts.get() == 0);
        assert (updates.get() == 1);
        assert (deletes.get() == 0);

        morphium.set(morphium.createQueryFor(UncachedObject.class).setCollectionName("uncached_object"), "str_value", "updated", false, true);
        Thread.sleep(1000);

        assert (updates.get() == 10) : "Updates wrong: " + updates.get();
        mon.terminate();
    }

    @Test
    public void testStringId() throws Exception {
        final AtomicInteger cnt = new AtomicInteger();
        morphium.dropCollection(StringIdEntity.class);
        Thread.sleep(100);
        ChangeStreamMonitor m = new ChangeStreamMonitor(morphium, "string_id_entity", true, null);
        m.start();
        m.addListener(evt -> {
            cnt.incrementAndGet();
            return true;
        });

        StringIdEntity i = new StringIdEntity();
        i.id = "test1";
        i.name = "test";
        i.value = new Integer(23);

        morphium.store(i);
        Thread.sleep(1000);
        assert (cnt.get() == 1);

        i.name = "neuer Testt";
        morphium.store(i);
        Thread.sleep(1000);
        assert (cnt.get() == 2);

        m.terminate();

    }


    @Entity
    public static class StringIdEntity {
        @Id
        public String id;
        public String name;
        public Integer value;
    }
}
