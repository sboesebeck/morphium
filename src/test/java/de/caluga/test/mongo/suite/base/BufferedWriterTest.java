package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.annotations.caching.NoCache;
import de.caluga.morphium.annotations.caching.WriteBuffer;
import de.caluga.morphium.query.Query;
import de.caluga.morphium.writer.BufferedMorphiumWriterImpl;
import de.caluga.test.mongo.suite.data.CachedObject;
import de.caluga.test.mongo.suite.data.ComplexObject;
import de.caluga.test.mongo.suite.data.EmbeddedObject;
import de.caluga.test.mongo.suite.data.UncachedObject;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * User: Stephan Bösebeck
 * Date: 22.03.13
 * Time: 14:41
 * <p/>
 * TODO: Add documentation here
 */
public class BufferedWriterTest extends MorphiumTestBase {

    @Test
    @Disabled
    public void testWriteBufferUpsert() throws Exception {
        List<BufferedBySizeObject> lst = new ArrayList<>();

        for (int i = 0; i < 200; i++) {
            BufferedBySizeObject buf = new BufferedBySizeObject();
            buf.setCounter(i);
            buf.setStrValue("V: " + i);
            lst.add(buf);
        }

        for (BufferedBySizeObject b : lst) {
            morphium.store(b);
        }

        Thread.sleep(3000);
        assertTrue(waitForAsyncOperationsToStart(morphium, 15000));
        TestUtils.waitForWrites(morphium, log);

        for (BufferedBySizeObject b : lst) {
            b.setStrValue(b.getStrValue() + 100);
            morphium.store(b);
        }

        Thread.sleep(3000);
        waitForAsyncOperationsToStart(morphium, 2000);
        TestUtils.waitForWrites(morphium, log);
        assertEquals(200, morphium.createQueryFor(BufferedBySizeObject.class).countAll());
    }

    @Test
    public void testWriteBufferUpdate() throws Exception {
        Query<BufferedBySizeObject> q = morphium.createQueryFor(BufferedBySizeObject.class).f("counter").eq(100);
        morphium.inc(q, UncachedObject.Fields.counter, 1, true, false);
        q = morphium.createQueryFor(BufferedBySizeObject.class).f(BufferedBySizeObject.Fields.counter).eq(101);
        morphium.inc(q, UncachedObject.Fields.counter, 1.0, true, false);
        q = morphium.createQueryFor(BufferedBySizeObject.class).f(BufferedBySizeObject.Fields.counter).eq(201);
        morphium.dec(q, UncachedObject.Fields.counter, 1.0, true, false);
        q = morphium.createQueryFor(BufferedBySizeObject.class).f(BufferedBySizeObject.Fields.counter).eq(300);
        morphium.set(q, UncachedObject.Fields.counter, 1, true, false);
        waitForAsyncOperationsToStart(morphium, 2000);
        TestUtils.waitForWrites(morphium, log);
        Thread.sleep(3000);
        q = morphium.createQueryFor(BufferedBySizeObject.class);
        assert(q.countAll() == 3);

        for (BufferedBySizeObject o : q.asList()) {
            log.info("Counter: " + o.getCounter());
        }
    }

    @Test
    public void testWriteBufferUpdateMap() throws Exception {

        for (int i = 0; i < 100; i++) {
            BufferedByTimeObject bo = new BufferedByTimeObject();
            bo.setCounter(100);
            bo.setDval(1.0);
            morphium.store(bo);
        }

        waitForAsyncOperationsToStart(morphium, 2000);
        TestUtils.waitForWrites(morphium, log);
        Thread.sleep(1000);

        while (morphium.createQueryFor(BufferedByTimeObject.class).countAll() != 100) {
            Thread.sleep(100);
        }

        Map<String, Number> toInc = new LinkedHashMap<>();
        toInc.put("counter", 1);
        toInc.put("dval", 0.1);
        Query<BufferedByTimeObject> q = morphium.createQueryFor(BufferedByTimeObject.class).f("counter").eq(100);
        morphium.inc(q, toInc, false, true, null);
        waitForAsyncOperationsToStart(morphium, 1000);
        TestUtils.waitForWrites(morphium, log);
        Thread.sleep(1000);
        assert(morphium.createQueryFor(BufferedByTimeObject.class).countAll() == 100);
        assert(morphium.createQueryFor(BufferedByTimeObject.class).f(UncachedObject.Fields.counter).eq(101).countAll() == 100);
        assert(morphium.createQueryFor(BufferedByTimeObject.class).f(UncachedObject.Fields.dval).eq(1.1).countAll() == 100);
        q = morphium.createQueryFor(BufferedByTimeObject.class).f("counter").eq(201);
        morphium.inc(q, toInc, true, false, null);
        waitForAsyncOperationsToStart(morphium, 1000);
        TestUtils.waitForWrites(morphium, log);
        Thread.sleep(1000);
        assert(morphium.createQueryFor(BufferedByTimeObject.class).countAll() == 101);
        assert(morphium.createQueryFor(BufferedByTimeObject.class).f(UncachedObject.Fields.counter).eq(101).countAll() == 100);
        assert(morphium.createQueryFor(BufferedByTimeObject.class).f(UncachedObject.Fields.counter).eq(202).countAll() == 1);
        assert(morphium.createQueryFor(BufferedByTimeObject.class).f(UncachedObject.Fields.dval).eq(0.1).countAll() == 1);
        assert(morphium.createQueryFor(BufferedByTimeObject.class).f(UncachedObject.Fields.dval).eq(1.1).countAll() == 100);
    }

    @Test
    public void testWriteBufferIncs() throws Exception {
        BufferedMorphiumWriterImpl wr = (BufferedMorphiumWriterImpl) morphium.getWriterForClass(BufferedBySizeObject.class);
        Query<BufferedBySizeObject> q = morphium.createQueryFor(BufferedBySizeObject.class).f(UncachedObject.Fields.counter).eq(100);
        morphium.inc(q, "dval", 1, true, false);
        assert(wr.writeBufferCount() >= 1);
        q = morphium.createQueryFor(BufferedBySizeObject.class).f(UncachedObject.Fields.counter).eq(100);
        morphium.inc(q, "dval", 1.0, true, false);
        assert(wr.writeBufferCount() >= 1);
        q = morphium.createQueryFor(BufferedBySizeObject.class).f(UncachedObject.Fields.counter).eq(100);
        morphium.dec(q, "dval", 1.0, true, false);
        assert(wr.writeBufferCount() >= 1);

        while (wr.writeBufferCount() > 0) {
            Thread.sleep(1000); //waiting
            log.info("Waiting for writes to finish...");
        }

        waitForAsyncOperationsToStart(morphium, 2000);
        TestUtils.waitForWrites(morphium, log);
        Thread.sleep(5000);
        q = morphium.createQueryFor(BufferedBySizeObject.class);
        assert(q.countAll() == 1) : "Counted " + q.countAll();
        BufferedBySizeObject o = q.get();
        log.info("Counter: " + o.getCounter());
        assert(o.getCounter() == 100);
        assert(o.getDval() == 1.0);
    }

    @Test
    public void testWriteBufferBySizeWithWriteNewStrategy() throws Exception {
        TestUtils.waitForWrites(morphium, log);
        int amount = 1500;

        for (int i = 0; i < amount; i++) {
            BufferedBySizeWriteNewObject bo = new BufferedBySizeWriteNewObject();
            bo.setCounter(i);
            bo.setStrValue("a value");
            morphium.store(bo);
        }

        long start = System.currentTimeMillis();
        long l = System.currentTimeMillis() - start;

        while (true) {
            long count = morphium.createQueryFor(BufferedBySizeWriteNewObject.class).countAll();

            if (count == amount) {
                break;
            }

            log.info("Amount written: " + count + " but Write buffer: " + morphium.getWriteBufferCount());
            Thread.sleep(1000);
        }

        log.info("All written " + amount);
    }

    @Test
    public void testWriteBufferBySize() throws Exception {
        int amount = 1500;
        morphium.getConfig().setMaxWaitTime(15000);
        morphium.getDriver().setMaxWaitTime(15000);
        for (int i = 0; i < amount; i++) {
            BufferedBySizeObject bo = new BufferedBySizeObject();
            bo.setCounter(i);
            bo.setStrValue("a value");
            morphium.store(bo);
        }

        long start = System.currentTimeMillis();

        while (true) {
            long count = morphium.createQueryFor(BufferedBySizeObject.class).countAll();
            int writeBufferCount = morphium.getWriteBufferCount();
            System.out.println("Amount written: " + count + " but Write buffer: " + writeBufferCount);
            Thread.sleep(10);

            if (writeBufferCount == 0 && count == 1500) {
                break;
            }
        }

        assert(System.currentTimeMillis() - start < 120000);
    }

    @Test
    public void testWriteBufferByTime() throws Exception {
        int amount = 1500;

        for (int i = 0; i < amount; i++) {
            BufferedByTimeObject bo = new BufferedByTimeObject();
            bo.setCounter(i);
            bo.setStrValue("a value");
            morphium.store(bo);
        }

        long start = System.currentTimeMillis();

        while (true) {
            long count = morphium.createQueryFor(BufferedByTimeObject.class).countAll();

            if (count == amount) {
                log.info("Found proper amount...");
                break;
            }

            System.out.println("Amount written: " + count + " but Write buffer: " + morphium.getWriteBufferCount());
            //            assert (morphium.getWriteBufferCount() != 0);
            Thread.sleep(100);
        }

        assert(System.currentTimeMillis() - start < 120000);
    }

    @Test
    public void testWriteBufferBySizeWithDelOldStrategy() throws Exception {
        morphium.getConfig().setMaxWaitTime(15000);
        int amount = 1500;

        for (int i = 0; i < amount; i++) {
            BufferedBySizeDelOldObject bo = new BufferedBySizeDelOldObject();
            bo.setCounter(i);
            bo.setStrValue("a value");
            morphium.store(bo);
        }

        log.info("Writes prepared - waiting");
        Thread.sleep(4000);
        long count = morphium.createQueryFor(BufferedBySizeDelOldObject.class).countAll();
        log.info("Amount written: " + count);
    }

    @Test
    public void testWriteBufferBySizeWithIngoreNewStrategy() throws Exception {
        int amount = 1500;

        for (int i = 0; i < amount; i++) {
            BufferedBySizeIgnoreNewObject bo = new BufferedBySizeIgnoreNewObject();
            bo.setCounter(i);
            bo.setStrValue("a value");
            morphium.store(bo);
        }

        log.info("Writes prepared - waiting");
        Thread.sleep(4000);
        long count = morphium.createQueryFor(BufferedBySizeIgnoreNewObject.class).countAll();
        assert(count < 1500);
    }

    @Test
    public void testWriteBufferBySizeWithWaitStrategy() throws Exception {
        int amount = 1500;
        morphium.getConfig().setMaxWaitTime(15000);
        morphium.getDriver().setMaxWaitTime(10000);
        for (int i = 0; i < amount; i++) {
            BufferedBySizeWaitObject bo = new BufferedBySizeWaitObject();
            bo.setCounter(i);
            bo.setStrValue("a value");
            morphium.store(bo);
        }

        log.info("Writes prepared - waiting");
        Thread.sleep(6000);
        long count = morphium.createQueryFor(BufferedBySizeIgnoreNewObject.class).countAll();
        assert(count < 1500);
    }

    @Test
    public void testComplexObject() throws Exception {
        Morphium m = morphium;


        for (int i = 0; i < 100; i++) {
            ComplexObjectBuffered buf = new ComplexObjectBuffered();
            buf.setEinText("The text " + i);
            EmbeddedObject e = new EmbeddedObject();
            e.setName("just a test");
            buf.setEmbed(e);
            UncachedObject uc = new UncachedObject();
            uc.setCounter(i);
            uc.setStrValue("value");
            buf.setEntityEmbeded(uc);
            m.store(buf);
        }

        while (m.getWriteBufferCount() != 0) {
            Thread.sleep(100);
        }

        Thread.sleep(500);
        ComplexObjectBuffered buf = m.createQueryFor(ComplexObjectBuffered.class).f("ein_text").eq("The text " + 0).get();
        assertNotNull(buf);;
        assert(m.createQueryFor(ComplexObjectBuffered.class).countAll() == 100);
    }

    @Test
    public void parallelWritingTest() throws Exception {
        if (morphium.exists(morphium.getDatabase(), morphium.getMapper().getCollectionName(SimpleObject.class))) {
            morphium.dropCollection(SimpleObject.class);

            TestUtils.waitForCollectionToBeDeleted(morphium,SimpleObject.class);
        }
        morphium.getDriver().setMaxWaitTime(15000);
        morphium.getConfig().setMaxWaitTime(15000);
        int threads = 5;
        int count = 100;
        Vector<Thread> thr = new Vector<>();

        for (int i = 0; i < threads; i++) {
            final var num = i;
            Thread t = new Thread() {
                public void run() {
                    try {
                        for (int j = 0; j < count; j++) {
                            SimpleObject o = new SimpleObject();
                            o.setCount(j);
                            o.setValue(getName());
                            morphium.store(o);
                            log.info("Thr " + num + ": wrote " + j + "/" + count);
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                    thr.remove(this);
                }
            };
            t.setName("Thread-" + i);
            thr.add(t);
            t.start();
        }

        TestUtils.waitForWrites(morphium, log);

        while (!thr.isEmpty()) {
            log.info("waiting for threads: " + thr.size());
            Thread.sleep(1000);
        }

        log.info("Writing finished");
        TestUtils.waitForConditionToBecomeTrue(10000, "Not all written", ()->morphium.createQueryFor(SimpleObject.class).countAll() == 500);
        long c = morphium.createQueryFor(SimpleObject.class).countAll();
        log.info("waiting..." + c);
        assertEquals(500, c);
    }

    @Test
    public void testNonObjectIdID() throws Exception {
        Morphium m = morphium;

        if (m.exists(m.getDatabase(), m.getMapper().getCollectionName(SimpleObject.class))) {
            m.dropCollection(SimpleObject.class);

            TestUtils.waitForCollectionToBeDeleted(m,SimpleObject.class);
        }

        for (int i = 0; i < 100; i++) {
            SimpleObject so = new SimpleObject();
            so.setCount(i);
            so.setValue("value " + i);
            so.setMyId("id_" + i);
            m.store(so);
        }

        while (m.getWriteBufferCount() != 0) {
            Thread.sleep(100);
        }

        TestUtils.waitForConditionToBecomeTrue(10000, "data not written", ()->m.createQueryFor(SimpleObject.class).countAll()==100);

        for (int i = 0; i < 100; i++) {
            SimpleObject so = new SimpleObject();
            so.setCount(i);
            so.setValue("value " + i);
            so.setMyId("id_" + i);
            m.store(so);
        }

        while (m.getWriteBufferCount() != 0) {
            Thread.sleep(100);
        }

        assert(m.createQueryFor(SimpleObject.class).countAll() == 100);
    }

    @WriteBuffer(size = 100, timeout = 1000)
    @Entity
    public static class SimpleObject {
        @Id
        private String myId;
        private String value;
        private int count;

        public String getMyId() {
            return myId;
        }

        public void setMyId(String myId) {
            this.myId = myId;
        }

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }

        public int getCount() {
            return count;
        }

        public void setCount(int count) {
            this.count = count;
        }

        public enum Fields {
            myId, value, count
        }
    }

    @WriteBuffer(size = 10, timeout = 1000)
    public static class ComplexObjectBuffered extends ComplexObject {

    }

    @WriteBuffer(timeout = 5500)
    @NoCache
    public static class BufferedByTimeObject extends UncachedObject {

    }

    @WriteBuffer(size = 150, strategy = WriteBuffer.STRATEGY.WAIT)
    public static class BufferedBySizeWaitObject extends UncachedObject {

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

    @WriteBuffer(size = 150, timeout = 3000)
    public static class BufferedBySizeObject extends UncachedObject {

    }
}
