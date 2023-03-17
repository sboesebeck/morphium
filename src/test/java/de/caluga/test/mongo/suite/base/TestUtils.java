package de.caluga.test.mongo.suite.base;

import org.slf4j.Logger;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.test.mongo.suite.data.UncachedObject;

public class TestUtils {
    public interface Condition {
        boolean test() throws Exception;
    }

    public static long waitForConditionToBecomeTrue(long maxDuration, String failMessage, Condition tst) {
        return waitForConditionToBecomeTrue(maxDuration, failMessage, tst, null);
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

                Thread.yield();
            }

            return System.currentTimeMillis() - start;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static long waitForCollectionToBeDeleted(Morphium m, String coll) {
        return waitForConditionToBecomeTrue(5000, "Collection was not deleted - db:" + m.getDatabase() + " coll: " + coll, ()->!m.exists(m.getDatabase(), coll));
    }

    public static long waitForCollectionToBeDeleted(Morphium m, String db, String coll) {
        return waitForConditionToBecomeTrue(5000, "Collection was not deleted - db:" + db + " coll: " + coll, ()->!m.exists(db, coll));
    }

    public static long waitForCollectionToBeDeleted(Morphium m, Class<?> type) {
        return waitForConditionToBecomeTrue(5000, "Collection for type was not deleted: " + type.getName(), () -> !m.exists(type));
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

}
