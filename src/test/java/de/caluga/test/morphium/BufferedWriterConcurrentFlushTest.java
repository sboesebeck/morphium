package de.caluga.test.morphium;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.annotations.caching.WriteBuffer;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import de.caluga.morphium.writer.BufferedMorphiumWriterImpl;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that {@link BufferedMorphiumWriterImpl#flush()} does not write the same
 * buffered entries twice when called concurrently from multiple threads.
 *
 * <h2>Root cause (upstream bug)</h2>
 * {@code flush()} used {@code opLog.get(clz)} to obtain the queue for a class, then
 * passed the live list reference to {@code flushQueueToMongo()}.  That method takes a
 * snapshot ({@code new ArrayList<>(q)}) but only removes entries <em>after</em> the
 * bulk write completes.  When two threads both call {@code flush()} at the same time
 * they each snapshot the same entries and both execute a bulk insert, producing
 * {@code E11000 duplicate key} errors and leaving the collection in an inconsistent
 * state.
 *
 * <p>This race is triggered in practice when Quarkus (or any framework running the
 * REST handler on a Virtual Thread) calls {@code Morphium.saveList()} — which ends
 * with an explicit {@code flush()} — while the {@code BufferedWriter_thread} background
 * housekeeping thread is simultaneously flushing the same class queue.
 *
 * <h2>Fix</h2>
 * {@code flush()} and {@code flush(Class)} now use {@code opLog.remove(clz)} instead of
 * {@code opLog.get(clz)}.  {@code ConcurrentHashMap.remove()} is atomic: if two threads
 * race, exactly one receives the list and the other gets {@code null} — preventing any
 * double-write.  This mirrors the pattern already used by the background thread's
 * {@code runIt()} method.
 *
 * <p>All tests use InMemoryDriver — no external MongoDB required.
 */
@Tag("inmemory")
public class BufferedWriterConcurrentFlushTest {

    private Morphium morphium;

    @AfterEach
    public void tearDown() {
        if (morphium != null) {
            morphium.close();
            morphium = null;
        }
    }

    // ------------------------------------------------------------------
    // Test entities
    // ------------------------------------------------------------------

    @Entity(collectionName = "buffered_concurrent_entity")
    @WriteBuffer(size = 500, strategy = WriteBuffer.STRATEGY.WRITE_NEW)
    public static class BufferedConcurrentEntity {
        @Id
        public MorphiumId id;
        public String name;

        public BufferedConcurrentEntity() {}

        public BufferedConcurrentEntity(String name) {
            this.name = name;
        }
    }

    // ------------------------------------------------------------------
    // Helper
    // ------------------------------------------------------------------

    private Morphium buildMorphium() {
        MorphiumConfig cfg = new MorphiumConfig("concurrent_flush_test_db", 10, 10_000, 10_000);
        cfg.driverSettings().setDriverName(InMemoryDriver.driverName);
        // Set a very long granularity so the background housekeeping thread does not
        // interfere with the test's own concurrent flush calls.
        cfg.writerSettings().setWriteBufferTimeGranularity(300_000);
        return new Morphium(cfg);
    }

    // ------------------------------------------------------------------
    // Tests
    // ------------------------------------------------------------------

    /**
     * Sequential double-flush must be idempotent: after the first flush writes all
     * N entries, a second flush must find the queue empty and write nothing.
     */
    @Test
    public void sequentialDoubleFlush_isIdempotent() {
        morphium = buildMorphium();
        BufferedMorphiumWriterImpl bw = (BufferedMorphiumWriterImpl)
                morphium.getConfig().writerSettings().getBufferedWriter();

        List<BufferedConcurrentEntity> entities = new ArrayList<>();
        for (int i = 0; i < 30; i++) {
            entities.add(new BufferedConcurrentEntity("seq-entity-" + i));
        }

        bw.insert(entities, null);

        // First flush writes all 30.
        bw.flush();
        long afterFirst = morphium.createQueryFor(BufferedConcurrentEntity.class).countAll();

        // Second flush must find nothing to write.
        bw.flush();
        long afterSecond = morphium.createQueryFor(BufferedConcurrentEntity.class).countAll();

        assertThat(afterFirst).as("first flush: all 30 entities written").isEqualTo(30);
        assertThat(afterSecond).as("second flush must not write anything (idempotent)").isEqualTo(30);
    }

    /**
     * Concurrent flush from two threads must write each buffered entry exactly once.
     *
     * <p>Before the fix, both threads could snapshot the same opLog queue and both
     * execute a bulk insert, causing {@code E11000 duplicate key} errors.  With the
     * fix ({@code opLog.remove()}), exactly one thread takes ownership; the other
     * finds {@code null} and skips.
     */
    @Test
    public void concurrentFlush_writesEachEntryExactlyOnce() throws Exception {
        morphium = buildMorphium();
        BufferedMorphiumWriterImpl bw = (BufferedMorphiumWriterImpl)
                morphium.getConfig().writerSettings().getBufferedWriter();

        final int entityCount = 50;
        List<BufferedConcurrentEntity> entities = new ArrayList<>();
        for (int i = 0; i < entityCount; i++) {
            entities.add(new BufferedConcurrentEntity("concurrent-entity-" + i));
        }

        // Insert into the buffer (the entries now sit in opLog, not yet in MongoDB).
        bw.insert(entities, null);

        // Use a CyclicBarrier so both threads start flush() as simultaneously as possible.
        CyclicBarrier barrier = new CyclicBarrier(2);
        AtomicReference<Throwable> thread1Error = new AtomicReference<>();
        AtomicReference<Throwable> thread2Error = new AtomicReference<>();

        Thread t1 = new Thread(() -> {
            try {
                barrier.await();
                bw.flush();
            } catch (Throwable e) {
                thread1Error.set(e);
            }
        }, "flush-thread-1");

        Thread t2 = new Thread(() -> {
            try {
                barrier.await();
                bw.flush();
            } catch (Throwable e) {
                thread2Error.set(e);
            }
        }, "flush-thread-2");

        t1.start();
        t2.start();
        t1.join(5_000);
        t2.join(5_000);

        assertThat(thread1Error.get()).as("flush-thread-1 must not throw").isNull();
        assertThat(thread2Error.get()).as("flush-thread-2 must not throw").isNull();

        long count = morphium.createQueryFor(BufferedConcurrentEntity.class).countAll();
        assertThat(count)
                .as("each of the %d entities must be written exactly once — not twice", entityCount)
                .isEqualTo(entityCount);
    }

    /**
     * The flush(Class) overload has the same race condition as flush().
     * This test verifies the typed overload is also fixed.
     */
    @Test
    public void concurrentFlushByClass_writesEachEntryExactlyOnce() throws Exception {
        morphium = buildMorphium();
        BufferedMorphiumWriterImpl bw = (BufferedMorphiumWriterImpl)
                morphium.getConfig().writerSettings().getBufferedWriter();

        final int entityCount = 40;
        List<BufferedConcurrentEntity> entities = new ArrayList<>();
        for (int i = 0; i < entityCount; i++) {
            entities.add(new BufferedConcurrentEntity("typed-flush-entity-" + i));
        }

        bw.insert(entities, null);

        CyclicBarrier barrier = new CyclicBarrier(2);
        AtomicReference<Throwable> thread1Error = new AtomicReference<>();
        AtomicReference<Throwable> thread2Error = new AtomicReference<>();

        Thread t1 = new Thread(() -> {
            try {
                barrier.await();
                bw.flush(BufferedConcurrentEntity.class);
            } catch (Throwable e) {
                thread1Error.set(e);
            }
        }, "flush-class-thread-1");

        Thread t2 = new Thread(() -> {
            try {
                barrier.await();
                bw.flush(BufferedConcurrentEntity.class);
            } catch (Throwable e) {
                thread2Error.set(e);
            }
        }, "flush-class-thread-2");

        t1.start();
        t2.start();
        t1.join(5_000);
        t2.join(5_000);

        assertThat(thread1Error.get()).as("flush-class-thread-1 must not throw").isNull();
        assertThat(thread2Error.get()).as("flush-class-thread-2 must not throw").isNull();

        long count = morphium.createQueryFor(BufferedConcurrentEntity.class).countAll();
        assertThat(count)
                .as("each of the %d entities must be written exactly once — not twice", entityCount)
                .isEqualTo(entityCount);
    }
}
