package de.caluga.test.morphium.driver.inmem;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Pins the concurrency contract that the collection-storage change (CopyOnWriteArrayList ->
 * ArrayList + explicit snapshots) must preserve.
 *
 * <p>Several driver code paths iterate a collection's whole document list WITHOUT holding that
 * collection's lock and relied on CopyOnWriteArrayList's copy-on-iterate semantics for safety:
 * <ul>
 *   <li>the dump/backup path ({@code dumpToFile} -> serializer walks every list), and</li>
 *   <li>the transaction snapshot path ({@code startTransaction} -> {@code deepCloneDatabase}
 *       clones every collection of every database).</li>
 * </ul>
 * The TTL sweep scans a collection the same lock-free way. With a plain ArrayList backing store
 * these iterations would throw {@link java.util.ConcurrentModificationException} the moment a
 * concurrent writer appends - unless every such reader is converted to iterate an explicit
 * snapshot taken under the read lock.
 *
 * <p>This test hammers ONE collection with N concurrent single-doc writers while two lock-free
 * reader threads (dump-style and transaction-clone-style) continuously iterate the whole store.
 * It MUST pass on the CopyOnWriteArrayList baseline (it pins the pre-existing contract) and MUST
 * keep passing after the ArrayList+snapshot conversion; a naive ArrayList swap without the
 * snapshot conversions makes the reader threads throw ConcurrentModificationException and fails
 * the test. It also asserts no write is lost (exact final count).
 */
@Tag("inmemory")
public class StorageSnapshotTest {

    @Test
    void concurrentWritesWithLockFreeReaders() throws Exception {
        InMemoryDriver drv = new InMemoryDriver();
        drv.connect();
        File dumpFile = File.createTempFile("inmem-snapshot", ".morphium.gz");
        dumpFile.deleteOnExit();

        try {
            final String db = "snaptest";
            final String coll = "docs";
            // materialize the collection with a seed document
            drv.store(db, coll, List.of(Doc.of("_id", 0, "v", "seed")), null);

            final int writerCount = 6;
            final long durationMs = 2000;
            final AtomicInteger idGen = new AtomicInteger(1);   // 0 is the seed
            final AtomicInteger stored = new AtomicInteger(1);  // seed already stored
            final AtomicBoolean running = new AtomicBoolean(true);
            final AtomicReference<Throwable> failure = new AtomicReference<>();
            final CountDownLatch startGate = new CountDownLatch(1);
            final List<Thread> threads = new ArrayList<>();

            // N writer threads: each stores single, uniquely-keyed docs as fast as it can.
            for (int w = 0; w < writerCount; w++) {
                Thread t = new Thread(() -> {
                    try {
                        startGate.await();
                        while (running.get()) {
                            int id = idGen.getAndIncrement();
                            drv.store(db, coll, List.of(Doc.of("_id", id, "v", "w" + id)), null);
                            stored.incrementAndGet();
                        }
                    } catch (Throwable e) {
                        failure.compareAndSet(null, e);
                    }
                }, "snap-writer-" + w);
                threads.add(t);
            }

            // Reader A - dump/backup-style: serializes the whole collection lock-free.
            Thread dumpReader = new Thread(() -> {
                try {
                    startGate.await();
                    while (running.get()) {
                        drv.dumpToFile(db, dumpFile);
                    }
                } catch (Throwable e) {
                    failure.compareAndSet(null, e);
                }
            }, "snap-dump-reader");
            threads.add(dumpReader);

            // Reader B - transaction-snapshot-style: deepClones every collection lock-free.
            Thread txReader = new Thread(() -> {
                try {
                    startGate.await();
                    while (running.get()) {
                        drv.startTransaction(false);
                        drv.abortTransaction();
                    }
                } catch (Throwable e) {
                    failure.compareAndSet(null, e);
                }
            }, "snap-tx-reader");
            threads.add(txReader);

            threads.forEach(Thread::start);
            startGate.countDown();
            Thread.sleep(durationMs);
            running.set(false);
            for (Thread t : threads) {
                t.join(30_000);
            }

            if (failure.get() != null) {
                throw new AssertionError("Concurrent storage access raised: " + failure.get(), failure.get());
            }

            long finalCount = drv.find(db, coll, Doc.of(), null, null, 0, 0).size();
            assertEquals(stored.get(), finalCount,
                    "every stored document must be present in the collection - no lost writes");
            assertTrue(stored.get() > writerCount + 1,
                    "writers should have made real progress during the run (stored=" + stored.get() + ")");
        } finally {
            dumpFile.delete();
            drv.shutdown(true);
        }
    }
}
