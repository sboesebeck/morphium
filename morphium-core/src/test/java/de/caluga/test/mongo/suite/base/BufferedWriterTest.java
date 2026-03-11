package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.annotations.caching.NoCache;
import de.caluga.morphium.annotations.caching.WriteBuffer;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import de.caluga.morphium.query.Query;
import de.caluga.morphium.writer.BufferedMorphiumWriterImpl;
import de.caluga.test.mongo.suite.data.ComplexObject;
import de.caluga.test.mongo.suite.data.EmbeddedObject;
import de.caluga.test.mongo.suite.data.UncachedObject;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * User: Stephan Bösebeck
 * Date: 22.03.13
 * Time: 14:41
 * <p>
 * TODO: Add documentation here
 */
@Tag("core")
public class BufferedWriterTest extends MultiDriverTestBase {

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    @Disabled
    public void testWriteBufferUpsert(Morphium morphium) throws Exception  {
        if (morphium.getConfig().driverSettings().getDriverName().equals(InMemoryDriver.driverName)) {
            log.info("Skipping write-buffer test for InMemoryDriver");
            return;
        }
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

        assertTrue(waitForAsyncOperationsToStart(morphium, 15000));
        TestUtils.waitForWrites(morphium, log);
        TestUtils.waitForConditionToBecomeTrue(5000, "Initial objects not persisted",
            () -> morphium.createQueryFor(BufferedBySizeObject.class).countAll() == 100);

        for (BufferedBySizeObject b : lst) {
            b.setStrValue(b.getStrValue() + 100);
            morphium.store(b);
        }

        waitForAsyncOperationsToStart(morphium, 2000);
        TestUtils.waitForWrites(morphium, log);
        TestUtils.waitForConditionToBecomeTrue(5000, "Updated objects not persisted",
            () -> morphium.createQueryFor(BufferedBySizeObject.class).countAll() == 200);
        assertEquals(200, morphium.createQueryFor(BufferedBySizeObject.class).countAll());
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void testWriteBufferUpdate(Morphium morphium) throws Exception  {
        Query<BufferedBySizeObject> q = morphium.createQueryFor(BufferedBySizeObject.class).f("counter").eq(100);
        morphium.inc(q, UncachedObject.Fields.counter, 1, true, false);
        q = morphium.createQueryFor(BufferedBySizeObject.class).f(BufferedBySizeObject.Fields.counter).eq(101);
        morphium.inc(q, UncachedObject.Fields.counter, 1.0, true, false);
        q = morphium.createQueryFor(BufferedBySizeObject.class).f(BufferedBySizeObject.Fields.counter).eq(201);
        morphium.dec(q, UncachedObject.Fields.counter, 1.0, true, false);
        q = morphium.createQueryFor(BufferedBySizeObject.class).f(BufferedBySizeObject.Fields.counter).eq(300);
        q.set(UncachedObject.Fields.counter, 1, true, false);
        waitForAsyncOperationsToStart(morphium, 2000);
        TestUtils.waitForWrites(morphium, log);
        TestUtils.waitForConditionToBecomeTrue(5000, "Update operations not persisted",
            () -> morphium.createQueryFor(BufferedBySizeObject.class).countAll() == 3);
        q = morphium.createQueryFor(BufferedBySizeObject.class);
        assert(q.countAll() == 3);

        for (BufferedBySizeObject o : q.asList()) {
            log.info("Counter: " + o.getCounter());
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void testWriteBufferUpdateMap(Morphium morphium) throws Exception  {
        if (morphium.getConfig().driverSettings().getDriverName().equals(InMemoryDriver.driverName)) {
            log.info("Skipping write-buffer test for InMemoryDriver");
            return;
        }

        for (int i = 0; i < 100; i++) {
            BufferedByTimeObject bo = new BufferedByTimeObject();
            bo.setCounter(100);
            bo.setDval(1.0);
            morphium.store(bo);
        }

        waitForAsyncOperationsToStart(morphium, 2000);
        TestUtils.waitForWrites(morphium, log);
        TestUtils.waitForConditionToBecomeTrue(5000, "Initial objects not persisted",
            () -> morphium.createQueryFor(BufferedByTimeObject.class).countAll() == 100);

        Map<String, Number> toInc = new LinkedHashMap<>();
        toInc.put("counter", 1);
        toInc.put("dval", 0.1);
        Query<BufferedByTimeObject> q = morphium.createQueryFor(BufferedByTimeObject.class).f("counter").eq(100);
        morphium.inc(q, toInc, false, true, null);
        waitForAsyncOperationsToStart(morphium, 1000);
        TestUtils.waitForWrites(morphium, log);
        TestUtils.waitForConditionToBecomeTrue(5000, "Expected 100 BufferedByTimeObject documents",
            () -> morphium.createQueryFor(BufferedByTimeObject.class).countAll() == 100);
        assert(morphium.createQueryFor(BufferedByTimeObject.class).f(UncachedObject.Fields.counter).eq(101).countAll() == 100);
        assert(morphium.createQueryFor(BufferedByTimeObject.class).f(UncachedObject.Fields.dval).eq(1.1).countAll() == 100);
        q = morphium.createQueryFor(BufferedByTimeObject.class).f("counter").eq(201);
        morphium.inc(q, toInc, true, false, null);
        waitForAsyncOperationsToStart(morphium, 1000);
        TestUtils.waitForWrites(morphium, log);
        TestUtils.waitForConditionToBecomeTrue(5000, "Expected 101 BufferedByTimeObject documents",
            () -> morphium.createQueryFor(BufferedByTimeObject.class).countAll() == 101);
        assert(morphium.createQueryFor(BufferedByTimeObject.class).f(UncachedObject.Fields.counter).eq(101).countAll() == 100);
        assert(morphium.createQueryFor(BufferedByTimeObject.class).f(UncachedObject.Fields.counter).eq(202).countAll() == 1);
        assert(morphium.createQueryFor(BufferedByTimeObject.class).f(UncachedObject.Fields.dval).eq(0.1).countAll() == 1);
        assert(morphium.createQueryFor(BufferedByTimeObject.class).f(UncachedObject.Fields.dval).eq(1.1).countAll() == 100);
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void testWriteBufferIncs(Morphium morphium) throws Exception  {
        if (morphium.getConfig().driverSettings().getDriverName().equals(InMemoryDriver.driverName)) {
            log.info("Skipping write-buffer test for InMemoryDriver");
            return;
        }
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

        TestUtils.waitForConditionToBecomeTrue(10000, "Write buffer not flushing",
            () -> {
                log.info("Waiting for writes to finish...");
                return wr.writeBufferCount() == 0;
            });

        waitForAsyncOperationsToStart(morphium, 2000);
        TestUtils.waitForWrites(morphium, log);
        TestUtils.waitForConditionToBecomeTrue(10000, "Inc operations not persisted",
            () -> morphium.createQueryFor(BufferedBySizeObject.class).countAll() == 1);
        q = morphium.createQueryFor(BufferedBySizeObject.class);
        assert(q.countAll() == 1) : "Counted " + q.countAll();
        BufferedBySizeObject o = q.get();
        log.info("Counter: " + o.getCounter());
        assert(o.getCounter() == 100);
        assert(o.getDval() == 1.0);
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void testWriteBufferBySizeWithWriteNewStrategy(Morphium morphium) throws Exception  {
        if (morphium.getConfig().driverSettings().getDriverName().equals(InMemoryDriver.driverName)) {
            log.info("Skipping write-buffer test for InMemoryDriver");
            return;
        }
        TestUtils.waitForWrites(morphium, log);
        int amount = 1500;

        for (int i = 0; i < amount; i++) {
            BufferedBySizeWriteNewObject bo = new BufferedBySizeWriteNewObject();
            bo.setCounter(i);
            bo.setStrValue("a value");
            morphium.store(bo);
        }

        long start = System.currentTimeMillis();

        TestUtils.waitForConditionToBecomeTrue(120000, "Not all objects persisted",
            () -> {
                long count = morphium.createQueryFor(BufferedBySizeWriteNewObject.class).countAll();
                log.info("Amount written: " + count + " but Write buffer: " + morphium.getWriteBufferCount());
                return count == amount;
            });

        log.info("All written " + amount);
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void testWriteBufferBySize(Morphium morphium) throws Exception  {
        int amount = 1500;
        morphium.getConfig().connectionSettings().setMaxWaitTime(15000);
        morphium.getDriver().setMaxWaitTime(15000);
        for (int i = 0; i < amount; i++) {
            BufferedBySizeObject bo = new BufferedBySizeObject();
            bo.setCounter(i);
            bo.setStrValue("a value");
            morphium.store(bo);
        }

        long start = System.currentTimeMillis();

        TestUtils.waitForConditionToBecomeTrue(120000, "Not all objects persisted or buffer not flushed",
            () -> {
                long count = morphium.createQueryFor(BufferedBySizeObject.class).countAll();
                int writeBufferCount = morphium.getWriteBufferCount();
                System.out.println("Amount written: " + count + " but Write buffer: " + writeBufferCount);
                return writeBufferCount == 0 && count == 1500;
            });

        assert(System.currentTimeMillis() - start < 120000);
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void testWriteBufferByTime(Morphium morphium) throws Exception  {
        if (morphium.getConfig().driverSettings().getDriverName().equals(InMemoryDriver.driverName)) {
            log.info("Skipping write-buffer test for InMemoryDriver");
            return;
        }
        int amount = 1500;

        for (int i = 0; i < amount; i++) {
            BufferedByTimeObject bo = new BufferedByTimeObject();
            bo.setCounter(i);
            bo.setStrValue("a value");
            morphium.store(bo);
        }

        long start = System.currentTimeMillis();

        TestUtils.waitForConditionToBecomeTrue(120000, "Not all objects persisted",
            () -> {
                long count = morphium.createQueryFor(BufferedByTimeObject.class).countAll();
                System.out.println("Amount written: " + count + " but Write buffer: " + morphium.getWriteBufferCount());
                return count == amount;
            });
        log.info("Found proper amount...");

        assert(System.currentTimeMillis() - start < 120000);
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void testWriteBufferBySizeWithDelOldStrategy(Morphium morphium) throws Exception  {
        if (morphium.getConfig().driverSettings().getDriverName().equals(InMemoryDriver.driverName)) {
            log.info("Skipping write-buffer test for InMemoryDriver");
            return;
        }
        morphium.getConfig().connectionSettings().setMaxWaitTime(15000);
        int amount = 1500;

        for (int i = 0; i < amount; i++) {
            BufferedBySizeDelOldObject bo = new BufferedBySizeDelOldObject();
            bo.setCounter(i);
            bo.setStrValue("a value");
            morphium.store(bo);
        }

        log.info("Writes prepared - waiting");
        TestUtils.waitForWrites(morphium, log);
        TestUtils.waitForConditionToBecomeTrue(10000, "Waiting for buffer to flush",
            () -> morphium.getWriteBufferCount() == 0);
        long count = morphium.createQueryFor(BufferedBySizeDelOldObject.class).countAll();
        log.info("Amount written: " + count);
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void testWriteBufferBySizeWithIngoreNewStrategy(Morphium morphium) throws Exception  {
        if (morphium.getConfig().driverSettings().getDriverName().equals(InMemoryDriver.driverName)) {
            log.info("Skipping write-buffer test for InMemoryDriver");
            return;
        }
        int amount = 1500;

        for (int i = 0; i < amount; i++) {
            BufferedBySizeIgnoreNewObject bo = new BufferedBySizeIgnoreNewObject();
            bo.setCounter(i);
            bo.setStrValue("a value");
            morphium.store(bo);
        }

        log.info("Writes prepared - waiting");
        TestUtils.waitForWrites(morphium, log);
        TestUtils.waitForConditionToBecomeTrue(10000, "Waiting for buffer to flush",
            () -> morphium.getWriteBufferCount() == 0);
        long count = morphium.createQueryFor(BufferedBySizeIgnoreNewObject.class).countAll();
        assert(count < 1500);
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void testWriteBufferBySizeWithWaitStrategy(Morphium morphium) throws Exception  {
        if (morphium.getConfig().driverSettings().getDriverName().equals(InMemoryDriver.driverName)) {
            log.info("Skipping write-buffer test for InMemoryDriver");
            return;
        }
        int amount = 1500;
        morphium.getConfig().connectionSettings().setMaxWaitTime(60000);
        morphium.getDriver().setMaxWaitTime(60000);
        for (int i = 0; i < amount; i++) {
            BufferedBySizeWaitObject bo = new BufferedBySizeWaitObject();
            bo.setCounter(i);
            bo.setStrValue("a value");
            morphium.store(bo);
        }

        log.info("Writes prepared - waiting");
        TestUtils.waitForWrites(morphium, log);
        TestUtils.waitForConditionToBecomeTrue(10000, "Waiting for buffer to flush",
            () -> morphium.getWriteBufferCount() == 0);
        long count = morphium.createQueryFor(BufferedBySizeIgnoreNewObject.class).countAll();
        assert(count < 1500);
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void testComplexObject(Morphium morphium) throws Exception  {
        if (morphium.getConfig().driverSettings().getDriverName().equals(InMemoryDriver.driverName)) {
            log.info("Skipping write-buffer test for InMemoryDriver");
            return;
        }
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

        TestUtils.waitForConditionToBecomeTrue(10000, "Write buffer not flushing",
            () -> m.getWriteBufferCount() == 0);

        TestUtils.waitForConditionToBecomeTrue(5000, "ComplexObjectBuffered not persisted",
            () -> m.createQueryFor(ComplexObjectBuffered.class).countAll() == 100);
        ComplexObjectBuffered buf = m.createQueryFor(ComplexObjectBuffered.class).f("ein_text").eq("The text " + 0).get();
        assertNotNull(buf);;
        assert(m.createQueryFor(ComplexObjectBuffered.class).countAll() == 100);
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void parallelWritingTest(Morphium morphium) throws Exception  {
        if (morphium.getConfig().driverSettings().getDriverName().equals(InMemoryDriver.driverName)) {
            log.info("Skipping write-buffer test for InMemoryDriver");
            return;
        }
        if (morphium.exists(morphium.getDatabase(), morphium.getMapper().getCollectionName(SimpleObject.class))) {
            morphium.dropCollection(SimpleObject.class);

            TestUtils.waitForCollectionToBeDeleted(morphium, SimpleObject.class);
        }
        morphium.getDriver().setMaxWaitTime(15000);
        morphium.getConfig().connectionSettings().setMaxWaitTime(15000);
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

        TestUtils.waitForConditionToBecomeTrue(30000, "Threads not finishing",
            () -> {
                log.info("waiting for threads: " + thr.size());
                return thr.isEmpty();
            });

        log.info("Writing finished");
        TestUtils.waitForConditionToBecomeTrue(10000, "Not all written", ()->morphium.createQueryFor(SimpleObject.class).countAll() == 500);
        long c = morphium.createQueryFor(SimpleObject.class).countAll();
        log.info("waiting..." + c);
        assertEquals(500, c);
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void testNonObjectIdID(Morphium morphium) throws Exception  {
        if (morphium.getConfig().driverSettings().getDriverName().equals(InMemoryDriver.driverName)) {
            log.info("Skipping write-buffer test for InMemoryDriver");
            return;
        }
        Morphium m = morphium;

        if (m.exists(m.getDatabase(), m.getMapper().getCollectionName(SimpleObject.class))) {
            m.dropCollection(SimpleObject.class);

            TestUtils.waitForCollectionToBeDeleted(m, SimpleObject.class);
        }

        for (int i = 0; i < 100; i++) {
            SimpleObject so = new SimpleObject();
            so.setCount(i);
            so.setValue("value " + i);
            so.setMyId("id_" + i);
            m.store(so);
        }

        TestUtils.waitForConditionToBecomeTrue(10000, "Write buffer not flushing",
            () -> m.getWriteBufferCount() == 0);

        TestUtils.waitForConditionToBecomeTrue(10000, "data not written", ()->m.createQueryFor(SimpleObject.class).countAll() == 100);

        for (int i = 0; i < 100; i++) {
            SimpleObject so = new SimpleObject();
            so.setCount(i);
            so.setValue("value " + i);
            so.setMyId("id_" + i);
            m.store(so);
        }

        TestUtils.waitForConditionToBecomeTrue(10000, "Write buffer not flushing on second batch",
            () -> m.getWriteBufferCount() == 0);

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
