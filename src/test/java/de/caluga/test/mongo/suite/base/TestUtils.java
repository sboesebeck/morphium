package de.caluga.test.mongo.suite.base;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.driver.commands.ListCollectionsCommand;
import de.caluga.morphium.driver.wire.MongoConnection;
import de.caluga.test.mongo.suite.data.UncachedObject;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

public class TestUtils {
    public interface Condition {
        boolean test() throws Exception;
    }

    public static long waitForConditionToBecomeTrue(long maxDuration, String failMessage, Condition tst) {
        return waitForConditionToBecomeTrue(maxDuration, failMessage, tst, null);
    }

    /**
     * wait until cond.get() <= valueToReach
     */
    public static long waitForIntegerValueMax(long maxDuration, String failMessage, AtomicInteger cond, int valueToReach, WaitCallback statusMessage) {
        return waitForConditionToBecomeTrue(maxDuration, failMessage, ()-> {return cond.get() <= valueToReach;}, statusMessage);
    }
    /**
     * wait until cond.get() >= valueToReach
     */
    public static long waitForIntegerValueMin(long maxDuration, String failMessage, AtomicInteger cond, int valueToReach, WaitCallback statusMessage) {
        return waitForConditionToBecomeTrue(maxDuration, failMessage, ()-> {return cond.get() >= valueToReach;}, statusMessage);
    }
    /**
     * wait until cond.get() == valueToReach
     */
    public static long waitForIntegerValue(long maxDuration, String failMessage, AtomicInteger cond, int valueToReach, WaitCallback statusMessage) {
        return waitForConditionToBecomeTrue(maxDuration, failMessage, ()-> {return cond.get() == valueToReach;}, statusMessage);
    }
    public static long waitForBooleanToBecomeFalse(long maxDuration, String failMessage, AtomicBoolean cond, WaitCallback statusMessage) {
        return waitForConditionToBecomeTrue(maxDuration, failMessage, ()-> {return !cond.get();}, statusMessage);
    }
    public static long waitForBooleanToBecomeTrue(long maxDuration, String failMessage, AtomicBoolean cond, WaitCallback statusCB) {
        return waitForConditionToBecomeTrue(maxDuration, failMessage, ()-> { return cond.get();}, statusCB);
    }

    public static void waitWithMessage(long duration, WaitCallback status) {
        waitWithMessage(duration, 1000, status);
    }
    public static void waitWithMessage(long duration, long interval, WaitCallback status) {
        long start = System.currentTimeMillis();
        while (System.currentTimeMillis() - start < duration) {
            status.stillWaiting(System.currentTimeMillis() - start);
            try {
                Thread.sleep(interval);
            } catch (InterruptedException e) {
                //swallow
            }
        }
    }
    public static long waitForConditionToBecomeTrue(long maxDuration, String failMessage, Condition tst, WaitCallback statusMessage) {
        Logger log = LoggerFactory.getLogger(TestUtils.class);
        return waitForConditionToBecomeTrue(maxDuration, (dur, e)-> { log.info(failMessage, e);}, tst, statusMessage, null);
    }
    public static long waitForConditionToBecomeTrue(long maxDuration, FailCallback failCB, Condition tst, WaitCallback statusMessage) {
        return waitForConditionToBecomeTrue(maxDuration, failCB, tst, statusMessage, null);
    }
    public static long waitForConditionToBecomeTrue(long maxDuration, FailCallback failCB, Condition tst, WaitCallback statusCB, FinishCallback finalCB) {
        long start = System.currentTimeMillis();
        int last = 0;

        try {
            while (!tst.test()) {
                if (System.currentTimeMillis() - start > maxDuration) {
                    throw new AssertionError(failCB);
                }

                if (statusCB != null && ((System.currentTimeMillis() - start) / 1000) > last) {
                    last = (int) (System.currentTimeMillis() - start) / 1000;
                    statusCB.stillWaiting(System.currentTimeMillis() - start);
                }

                Thread.sleep(10); // Much more CPU-friendly than Thread.yield()
            }

            var dur = System.currentTimeMillis() - start;
            if (finalCB != null) {
                finalCB.finished(dur);
            }
            return dur;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt(); // Restore interrupt status
            throw new RuntimeException("Interrupted while waiting for condition", e);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static long waitForCollectionToBeDeleted(Morphium m, String coll) {
        return waitForConditionToBecomeTrue(10000, "Collection was not deleted - db:" + m.getDatabase() + " coll: " + coll, ()->!m.exists(m.getDatabase(), coll));
    }

    public static long waitForCollectionToBeDeleted(Morphium m, String db, String coll) {
        return waitForConditionToBecomeTrue(10000, "Collection was not deleted - db:" + db + " coll: " + coll, ()->!m.exists(db, coll));
    }

    public static long waitForCollectionToBeDeleted(Morphium m, Class<?> type) {
        return waitForConditionToBecomeTrue(10000, "Collection for type was not deleted: " + type.getName(), () -> !m.exists(type));
    }

    public static long countUC(Morphium m) {
        return m.createQueryFor(UncachedObject.class).countAll();
    }

    public static void logDriverStats(Logger log, Morphium m) {
        var stats = m.getDriver().getDriverStats();

        for (var e : stats.entrySet()) {
            log.info(String.format("Stats %s: %f ", e.getKey().name(), e.getValue()));
        }
    }

    public static Morphium newMorphiumFrom(Morphium morphium) {
        var cfg = MorphiumConfig.fromProperties(morphium.getConfig().asProperties());
        cfg.encryptionSettings().setCredentialsDecryptionKey(morphium.getConfig().encryptionSettings().getCredentialsDecryptionKey());
        cfg.encryptionSettings().setCredentialsEncryptionKey(morphium.getConfig().encryptionSettings().getCredentialsEncryptionKey());
        var m2 = new Morphium(cfg);
        return m2;
    }

    public static void check(Logger log, String msg) {
        log.info("{}: ✅", msg);
    }

    public static void clearDB(Morphium morphium) {
        MongoConnection con = null;
        ListCollectionsCommand cmd = null;
        Logger log = LoggerFactory.getLogger(TestUtils.class);
        try {
            boolean retry = true;

            while (retry) {
                con = morphium.getDriver().getPrimaryConnection(null);
                cmd = new ListCollectionsCommand(con).setDb(morphium.getDatabase());
                var lst = cmd.execute();
                cmd.releaseConnection();

                for (var collMap : lst) {
                    String coll = (String) collMap.get("name");
                    log.info("Dropping collection " + coll);

                    morphium.dropCollection(UncachedObject.class, coll, null); //faking it a bit ;-)
                }

                long start = System.currentTimeMillis();
                retry = false;
                boolean collectionsExist = true;

                while (collectionsExist) {
                    Thread.sleep(100);
                    con = morphium.getDriver().getPrimaryConnection(null);
                    cmd = new ListCollectionsCommand(con).setDb(morphium.getDatabase());
                    lst = cmd.execute();
                    cmd.releaseConnection();

                    for (var k : lst) {
                        log.info("Collections still there..." + k.get("name"));
                    }

                    if (System.currentTimeMillis() - start > 1500) {
                        retry = true;
                        break;
                    }

                    if (lst.size() == 0) {
                        collectionsExist = false;
                    }
                }
            }
        } catch (Exception e) {
            log.info("Could not clean up: ", e);
        } finally {
            if (cmd != null) {
                cmd.releaseConnection();
            }
        }
    }

    public static void wait(int secs) {
        wait("Waiting " + secs + "s: ", secs);
    }

    public static void wait(String msg, int secs) {
        System.out.print(msg);

        for (int i = secs; i > 0; i--) {
            if (i % 10 == 0) {
                System.out.print(i);
            }

            System.out.print(".");

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
            }
        }

        System.out.println("done");
    }

    public static void waitForWrites(Morphium morphium, Logger log) {
        int count = 0;

        while (morphium.getWriteBufferCount() > 0) {
            count++;

            if (count % 100 == 0) {
                log.info("still " + morphium.getWriteBufferCount() + " writers active (" + morphium.getBufferedWriterBufferCount() + " + " + morphium.getWriterBufferCount() + ")");
            }

            try {
                Thread.sleep(100);
            } catch (InterruptedException ex) {
            }
        }

        //waiting for it to be persisted
        try {
            Thread.sleep(1500);
        } catch (InterruptedException e) {
        }
    }

    /**
     * Waits for the object to be delivered by the supplier.
     * The first attempt is made after the initialDelay (in ms). Then if the object is still not ready,
     * it calls the supplier every step (in ms) until
     * either maxDelay is reached or the object is delivered.
     * It returns null, if the object is not delivered within maxDelay time (in ms).
     *
     * @param supplier Supplier
     * @param maxDelay long
     * @return T
     * @param <T>
     * @throws InterruptedException
     */
    public static <T> T waitForObject(Supplier<T> supplier, long initialDelay, long step, long maxDelay) throws InterruptedException {
        Thread.sleep(initialDelay);
        T value = supplier.get();
        long currentDelay = initialDelay;
        while (value == null) {
            if (currentDelay + step > maxDelay) {
                break;
            }
            currentDelay += step;
            Thread.sleep(step);
            value = supplier.get();
        }
        return value;
    }

    /**
     * Waits for the object to be delivered by the supplier.
     * The first attempt is made after 10ms. Then if the object is still not ready,
     * it calls the supplier every 30 ms until
     * either 10 seconds is reached or the object is delivered.
     * It returns null, if the object is not delivered within 10 seconds (in ms).
     *
     * @param supplier
     * @return
     * @param <T>
     * @throws InterruptedException
     */
    public static <T> T waitForObject(Supplier<T> supplier) throws InterruptedException {
        return (T) TestUtils.waitForObject(supplier, 10L, 30L, 10000L);
    }

    /**
     * Waits for the object to be delivered by the supplier.
     * The first attempt is made after 10ms. Then if the object is still not ready,
     * it calls the supplier every 30 ms until
     * either maxDelay (in ms) is reached or the object is delivered.
     * It returns null, if the object is not delivered within maxDelay time (in ms).
     *
      * @param supplier Supplier
     * @param maxDelay long
     * @return T
     * @param <T>
     * @throws InterruptedException
     */
    public static <T> T waitForObject(Supplier<T> supplier, long maxDelay) throws InterruptedException {
        return (T) TestUtils.waitForObject(supplier, 10L, 30L, maxDelay);
    }


    public interface WaitCallback {
        public void stillWaiting(long dur);
    }
    public interface FinishCallback {
        public void finished(long dur);
    }
    public interface FailCallback {
        public void fail(long dur, Throwable e);
    }
}
