package de.caluga.test.mongo.suite.base;

import org.slf4j.Logger;

import de.caluga.morphium.Morphium;

public class TestUtils {
    public interface Condition {
        boolean test() throws Exception;
    }
    public static long waitForConditionToBecomeTrue(long maxDuration, String failMessage, Condition tst) {
        long start = System.currentTimeMillis();

        try {
            while (!tst.test()) {
                if (System.currentTimeMillis() - start > maxDuration) {
                    throw new AssertionError(failMessage);
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
        return waitForConditionToBecomeTrue(5000, "Collection for type was not deleted: " + type.getName(), ()->!m.exists(type));
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
