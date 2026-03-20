package de.caluga.test.mongo.suite.inmem;

import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.caching.WriteBuffer;
import de.caluga.test.mongo.suite.data.UncachedObject;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Verifies that commitTransaction() flushes the write buffer before
 * committing, so that entities annotated with @WriteBuffer are
 * actually included in the transaction.
 *
 * Before the fix in Morphium.commitTransaction(), buffered writes
 * were silently excluded from the transaction — they would either
 * be flushed later (outside the transaction) or lost entirely.
 */
@Tag("inmemory")
public class InMemWriteBufferTransactionTest extends MorphiumInMemTestBase {

    @Test
    public void commitTransactionFlushesWriteBuffer() throws Exception {
        morphium.dropCollection(BufferedEntity.class);
        assertEquals(0, morphium.createQueryFor(BufferedEntity.class).countAll());

        morphium.startTransaction();

        // Store a @WriteBuffer entity — this goes into the buffer, not directly to DB
        BufferedEntity entity = new BufferedEntity();
        entity.setCounter(42);
        entity.setStrValue("buffered-in-transaction");
        morphium.store(entity);

        // The buffer should contain the write (not yet flushed to DB)
        // commitTransaction() now calls flush() before committing,
        // ensuring the buffered entity becomes part of the transaction.
        morphium.commitTransaction();

        // After commit, the entity must be persisted
        long count = morphium.createQueryFor(BufferedEntity.class).countAll();
        assertEquals(1, count,
                "@WriteBuffer entity was not persisted — flush() before commit may be missing");

        BufferedEntity loaded = morphium.createQueryFor(BufferedEntity.class)
                .f("counter").eq(42)
                .get();
        assertEquals("buffered-in-transaction", loaded.getStrValue());
    }

    @WriteBuffer(size = 200, timeout = 5000)
    @Entity(collectionName = "buffered_tx_test")
    public static class BufferedEntity extends UncachedObject {
    }
}
