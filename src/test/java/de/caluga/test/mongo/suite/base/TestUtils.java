package de.caluga.test.mongo.suite.base;

import org.slf4j.Logger;

import de.caluga.morphium.Morphium;

public class TestUtils {
    public interface Condition {
        boolean test();
    }
    public static long waitForConditionToBecomeTrue(long maxDuration, String failMessage, Condition tst) {
        long start = System.currentTimeMillis();
        while (!tst.test()) {
            if (System.currentTimeMillis() - start > maxDuration) {
                throw new AssertionError(failMessage);
            }
            Thread.yield();
        }
        return System.currentTimeMillis() - start;
    }

    public static void waitForWrites(Morphium morphium,Logger log) {
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
