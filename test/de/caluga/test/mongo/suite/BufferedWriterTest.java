package de.caluga.test.mongo.suite;

import de.caluga.morphium.MorphiumSingleton;
import de.caluga.morphium.annotations.caching.WriteBuffer;
import org.junit.Test;

/**
 * User: Stephan Bösebeck
 * Date: 22.03.13
 * Time: 14:41
 * <p/>
 * TODO: Add documentation here
 */
public class BufferedWriterTest extends MongoTest {

    @Test
    public void testWriteBufferBySizeWithWriteNewStrategy() throws Exception {
        MorphiumSingleton.get().dropCollection(BufferedBySizeWriteNewObject.class);
        waitForAsyncOperationToStart(10000);
        waitForWrites();
        int amount = 1500;
        for (int i = 0; i < amount; i++) {
            BufferedBySizeWriteNewObject bo = new BufferedBySizeWriteNewObject();
            bo.setCounter(i);
            bo.setValue("a value");
            MorphiumSingleton.get().store(bo);
        }
        long start = System.currentTimeMillis();
        while (true) {

            long count = MorphiumSingleton.get().createQueryFor(BufferedBySizeWriteNewObject.class).countAll();
            if (count == amount) break;
            System.out.println("Amount written: " + count + " but Write buffer: " + MorphiumSingleton.get().getWriteBufferCount());
            assert (System.currentTimeMillis() - start < 15000);
            Thread.sleep(100);
        }
        assert (System.currentTimeMillis() - start < 15000);
    }

    @Test
    public void testWriteBufferBySize() throws Exception {
        MorphiumSingleton.get().dropCollection(BufferedBySizeObject.class);
        int amount = 1500;
        for (int i = 0; i < amount; i++) {
            BufferedBySizeObject bo = new BufferedBySizeObject();
            bo.setCounter(i);
            bo.setValue("a value");
            MorphiumSingleton.get().store(bo);
        }
        long start = System.currentTimeMillis();
        while (true) {

            long count = MorphiumSingleton.get().createQueryFor(BufferedBySizeObject.class).countAll();
            if (count == amount) break;
            System.out.println("Amount written: " + count + " but Write buffer: " + MorphiumSingleton.get().getWriteBufferCount());
            Thread.sleep(100);
        }
        assert (System.currentTimeMillis() - start < 120000);
    }

    @Test
    public void testWriteBufferByTime() throws Exception {
        MorphiumSingleton.get().dropCollection(BufferedByTimeObject.class);
        int amount = 1500;
        for (int i = 0; i < amount; i++) {
            BufferedByTimeObject bo = new BufferedByTimeObject();
            bo.setCounter(i);
            bo.setValue("a value");
            MorphiumSingleton.get().store(bo);
        }
        long start = System.currentTimeMillis();
        while (true) {

            long count = MorphiumSingleton.get().createQueryFor(BufferedByTimeObject.class).countAll();
            if (count == amount) break;
            System.out.println("Amount written: " + count + " but Write buffer: " + MorphiumSingleton.get().getWriteBufferCount());
            assert (MorphiumSingleton.get().getWriteBufferCount() != 0);
            Thread.sleep(100);
        }
        assert (System.currentTimeMillis() - start < 120000);
    }

    @Test
    public void testWriteBufferBySizeWithDelOldStrategy() throws Exception {
        MorphiumSingleton.get().dropCollection(BufferedBySizeDelOldObject.class);
        waitForAsyncOperationToStart(10000);
        waitForWrites();
        int amount = 1500;
        for (int i = 0; i < amount; i++) {
            BufferedBySizeDelOldObject bo = new BufferedBySizeDelOldObject();
            bo.setCounter(i);
            bo.setValue("a value");
            MorphiumSingleton.get().store(bo);
        }
        log.info("Writes prepared - waiting");
        Thread.sleep(4000);
        long count = MorphiumSingleton.get().createQueryFor(BufferedBySizeDelOldObject.class).countAll();
        assert (count < 1500);
    }

    @Test
    public void testWriteBufferBySizeWithIngoreNewStrategy() throws Exception {
        MorphiumSingleton.get().dropCollection(BufferedBySizeIgnoreNewObject.class);
        waitForAsyncOperationToStart(10000);
        waitForWrites();
        int amount = 1500;
        for (int i = 0; i < amount; i++) {
            BufferedBySizeIgnoreNewObject bo = new BufferedBySizeIgnoreNewObject();
            bo.setCounter(i);
            bo.setValue("a value");
            MorphiumSingleton.get().store(bo);
        }
        log.info("Writes prepared - waiting");
        Thread.sleep(4000);
        long count = MorphiumSingleton.get().createQueryFor(BufferedBySizeIgnoreNewObject.class).countAll();
        assert (count < 1500);
    }


    @WriteBuffer(timeout = 5500)
    public static class BufferedByTimeObject extends UncachedObject {

    }

    @WriteBuffer(size = 150, strategy = WriteBuffer.STRATEGY.IGNORE_NEW)
    public static class BufferedBySizeIgnoreNewObject extends UncachedObject {

    }

    @WriteBuffer(size = 150, strategy = WriteBuffer.STRATEGY.WRITE_NEW)
    public static class BufferedBySizeWriteNewObject extends UncachedObject {

    }

    @WriteBuffer(size = 150, strategy = WriteBuffer.STRATEGY.DEL_OLD)
    public static class BufferedBySizeDelOldObject extends UncachedObject {

    }

    @WriteBuffer(size = 150)
    public static class BufferedBySizeObject extends UncachedObject {

    }
}
