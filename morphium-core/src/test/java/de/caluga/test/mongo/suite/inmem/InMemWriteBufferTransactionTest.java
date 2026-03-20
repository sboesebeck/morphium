package de.caluga.test.mongo.suite.inmem;

import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.caching.WriteBuffer;
import de.caluga.test.mongo.suite.data.UncachedObject;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Verifies that the write buffer is correctly managed during transactions:
 * <ul>
 *   <li>startTransaction() disables write buffering for the calling thread,
 *       so @WriteBuffer entities are written directly within the transaction</li>
 *   <li>commitTransaction()/abortTransaction() restore the previous write buffer state</li>
 *   <li>Other threads' buffered writes are NOT pulled into the transaction</li>
 * </ul>
 */
@Tag("inmemory")
public class InMemWriteBufferTransactionTest extends MorphiumInMemTestBase {

    @Test
    public void writeBufferDisabledDuringTransaction() throws Exception {
        morphium.dropCollection(BufferedEntity.class);

        morphium.startTransaction();

        // Write buffer is now disabled for this thread — @WriteBuffer entity
        // goes directly through the direct writer, inside the transaction.
        assertFalse(morphium.isWriteBufferEnabledForThread(),
                "Write buffer should be disabled after startTransaction()");

        BufferedEntity entity = new BufferedEntity();
        entity.setCounter(42);
        entity.setStrValue("buffered-in-transaction");
        morphium.store(entity);

        morphium.commitTransaction();

        // After commit, the entity must be persisted
        long count = morphium.createQueryFor(BufferedEntity.class).countAll();
        assertEquals(1, count,
                "@WriteBuffer entity was not persisted within the transaction");

        BufferedEntity loaded = morphium.createQueryFor(BufferedEntity.class)
                .f("counter").eq(42)
                .get();
        assertEquals("buffered-in-transaction", loaded.getStrValue());
    }

    @Test
    public void writeBufferStateRestoredAfterCommit() {
        // Write buffer starts enabled (default)
        assertTrue(morphium.isWriteBufferEnabledForThread());

        morphium.startTransaction();
        assertFalse(morphium.isWriteBufferEnabledForThread());

        morphium.commitTransaction();

        // Must be restored to original state
        assertTrue(morphium.isWriteBufferEnabledForThread(),
                "Write buffer state should be restored after commitTransaction()");
    }

    @Test
    public void writeBufferStateRestoredAfterAbort() {
        assertTrue(morphium.isWriteBufferEnabledForThread());

        morphium.startTransaction();
        assertFalse(morphium.isWriteBufferEnabledForThread());

        morphium.abortTransaction();

        assertTrue(morphium.isWriteBufferEnabledForThread(),
                "Write buffer state should be restored after abortTransaction()");
    }

    @Test
    public void previouslyDisabledBufferPreservedAcrossTransaction() {
        // Caller has already disabled write buffer before the transaction
        morphium.disableWriteBufferForThread();
        assertFalse(morphium.isWriteBufferEnabledForThread());

        morphium.startTransaction();
        assertFalse(morphium.isWriteBufferEnabledForThread());

        morphium.commitTransaction();

        // Must still be disabled — the pre-transaction state was "disabled"
        assertFalse(morphium.isWriteBufferEnabledForThread(),
                "Pre-transaction disabled state must be preserved after commit");

        // Cleanup
        morphium.resetThreadLocalOverrides();
    }

    @Test
    public void otherThreadBufferedWritesNotPulledIntoTransaction() throws Exception {
        morphium.dropCollection(BufferedEntity.class);
        morphium.dropCollection(UncachedObject.class);

        CountDownLatch otherThreadReady = new CountDownLatch(1);
        CountDownLatch transactionDone = new CountDownLatch(1);
        AtomicInteger otherThreadCount = new AtomicInteger(0);

        // Other thread: store an entity outside any transaction (unbuffered for simplicity)
        Thread otherThread = new Thread(() -> {
            try {
                UncachedObject obj = new UncachedObject("other-thread", 99);
                morphium.store(obj);
                otherThreadCount.set((int) morphium.createQueryFor(UncachedObject.class).countAll());
                otherThreadReady.countDown();
                transactionDone.await();
            } catch (Exception e) {
                // ignore
            }
        });
        otherThread.start();
        otherThreadReady.await();

        // This thread: start transaction, store, then abort
        morphium.startTransaction();

        BufferedEntity entity = new BufferedEntity();
        entity.setCounter(1);
        entity.setStrValue("will-be-rolled-back");
        morphium.store(entity);

        morphium.abortTransaction();
        transactionDone.countDown();
        otherThread.join(5000);

        // Other thread's write must still be there (not rolled back with our transaction)
        assertEquals(1, otherThreadCount.get(),
                "Other thread's write must not be affected by this thread's transaction");
        long uncachedCount = morphium.createQueryFor(UncachedObject.class).countAll();
        assertEquals(1, uncachedCount,
                "Other thread's UncachedObject must still exist after abort");

        // Our aborted entity must NOT exist
        assertEquals(0, morphium.createQueryFor(BufferedEntity.class).countAll(),
                "Aborted transaction entity must not be persisted");
    }

    @WriteBuffer(size = 200, timeout = 5000)
    @Entity(collectionName = "buffered_tx_test")
    public static class BufferedEntity extends UncachedObject {
    }
}
