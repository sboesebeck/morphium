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
    public static long waitForIntegerValueMax(long maxDuration, String failMessage, AtomicInteger cond, int valueToReach, Runnable statusMessage) {
        return waitForConditionToBecomeTrue(maxDuration, failMessage, ()-> {return cond.get() <= valueToReach;}, statusMessage);
    }
    /**
     * wait until cond.get() >= valueToReach
     */
    public static long waitForIntegerValueMin(long maxDuration, String failMessage, AtomicInteger cond, int valueToReach, Runnable statusMessage) {
        return waitForConditionToBecomeTrue(maxDuration, failMessage, ()-> {return cond.get() >= valueToReach;}, statusMessage);
    }
    /**
     * wait until cond.get() == valueToReach
     */
    public static long waitForIntegerValue(long maxDuration, String failMessage, AtomicInteger cond, int valueToReach, Runnable statusMessage) {
        return waitForConditionToBecomeTrue(maxDuration, failMessage, ()-> {return cond.get() == valueToReach;}, statusMessage);
    }
    public static long waitForBooleanToBecomeFalse(long maxDuration, String failMessage, AtomicBoolean cond, Runnable statusMessage) {
        return waitForConditionToBecomeTrue(maxDuration, failMessage, ()-> {return !cond.get();}, statusMessage);
    }
    public static long waitForBooleanToBecomeTrue(long maxDuration, String failMessage, AtomicBoolean cond, Runnable statusMessage) {
        return waitForConditionToBecomeTrue(maxDuration, failMessage, ()-> { return cond.get();}, statusMessage);
    }
    public static long waitForConditionToBecomeTrue(long maxDuration, String failMessage, Condition tst, Runnable statusMessage) {
        long start = System.currentTimeMillis();
        int last = 0;

        try {
            while (!tst.test()) {
                if (System.currentTimeMillis() - start > maxDuration) {
                    throw new AssertionError(failMessage);
                }

                if (statusMessage != null && ((System.currentTimeMillis() - start) / 1000) > last) {
                    last = (int) (System.currentTimeMillis() - start) / 1000;
                    statusMessage.run();
                }

                Thread.sleep(10); // Much more CPU-friendly than Thread.yield()
            }

            if (statusMessage != null)
                statusMessage.run();
            return System.currentTimeMillis() - start;
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
        cfg.setCredentialsDecryptionKey(morphium.getConfig().getCredentialsDecryptionKey());
        cfg.setCredentialsEncryptionKey(morphium.getConfig().getCredentialsEncryptionKey());
        var m2 = new Morphium(cfg);
        return m2;
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
}
