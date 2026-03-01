package de.caluga.test.morphium;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.Sequence;
import de.caluga.morphium.Sequence.SeqLock;
import de.caluga.morphium.SequenceGenerator;
import de.caluga.morphium.driver.inmem.InMemoryDriver;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link SequenceGenerator#getNextBatch(int)}.
 *
 * <p>Verifies that the batch allocation method:
 * <ul>
 *   <li>returns the correct number of values</li>
 *   <li>returns strictly sequential values respecting the configured {@code inc}</li>
 *   <li>does not produce any value already returned by {@link SequenceGenerator#getNextValue()}</li>
 *   <li>produces non-overlapping ranges when called concurrently</li>
 *   <li>is significantly faster than N individual {@code getNextValue()} calls</li>
 * </ul>
 *
 * <p>All tests run with {@link InMemoryDriver} — no MongoDB required.
 */
@Tag("inmemory")
public class SequenceGeneratorBatchTest {

    private Morphium morphium;

    @BeforeEach
    public void setUp() {
        MorphiumConfig cfg = new MorphiumConfig("seq_batch_test_db", 10, 10_000, 1_000);
        cfg.setDriverName(InMemoryDriver.driverName);
        morphium = new Morphium(cfg);
        morphium.dropCollection(Sequence.class);
        morphium.dropCollection(SeqLock.class);
    }

    @AfterEach
    public void tearDown() {
        if (morphium != null) {
            morphium.close();
        }
    }

    // -------------------------------------------------------------------------
    // Basic correctness
    // -------------------------------------------------------------------------

    @Test
    public void getNextBatch_returnsCorrectCount() {
        SequenceGenerator sg = new SequenceGenerator(morphium, "batch_count", 1, 1);
        long[] batch = sg.getNextBatch(10);
        assertThat(batch).hasSize(10);
    }

    @Test
    public void getNextBatch_valuesAreSequential_inc1() {
        SequenceGenerator sg = new SequenceGenerator(morphium, "batch_seq1", 1, 1);
        long[] batch = sg.getNextBatch(5);
        assertThat(batch).containsExactly(1L, 2L, 3L, 4L, 5L);
    }

    @Test
    public void getNextBatch_valuesAreSequential_inc3() {
        SequenceGenerator sg = new SequenceGenerator(morphium, "batch_seq3", 3, 1);
        long[] batch = sg.getNextBatch(4);
        // inc=3, startValue=1: first getNextValue() would return 1, 4, 7, 10
        assertThat(batch).containsExactly(1L, 4L, 7L, 10L);
    }

    @Test
    public void getNextBatch_valuesStartAtConfiguredStartValue() {
        SequenceGenerator sg = new SequenceGenerator(morphium, "batch_start100", 1, 100);
        long[] batch = sg.getNextBatch(3);
        assertThat(batch).containsExactly(100L, 101L, 102L);
    }

    @Test
    public void getNextBatch_allValuesUnique() {
        SequenceGenerator sg = new SequenceGenerator(morphium, "batch_unique", 1, 1);
        long[] batch = sg.getNextBatch(100);
        Set<Long> seen = new HashSet<>();
        for (long v : batch) {
            assertThat(seen.add(v)).as("duplicate value %d in batch", v).isTrue();
        }
    }

    // -------------------------------------------------------------------------
    // Interoperability with getNextValue()
    // -------------------------------------------------------------------------

    @Test
    public void getNextBatch_followedByGetNextValue_continuesSequence() {
        SequenceGenerator sg = new SequenceGenerator(morphium, "batch_interop", 1, 1);
        long[] batch = sg.getNextBatch(3);
        assertThat(batch).containsExactly(1L, 2L, 3L);

        // next individual call must continue right after the batch
        long next = sg.getNextValue();
        assertThat(next).isEqualTo(4L);
    }

    @Test
    public void getNextValue_followedByGetNextBatch_continuesSequence() {
        SequenceGenerator sg = new SequenceGenerator(morphium, "batch_interop2", 1, 1);
        long first = sg.getNextValue();
        assertThat(first).isEqualTo(1L);

        long[] batch = sg.getNextBatch(3);
        assertThat(batch).containsExactly(2L, 3L, 4L);
    }

    @Test
    public void consecutiveBatches_noGaps_noOverlaps() {
        SequenceGenerator sg = new SequenceGenerator(morphium, "consecutive_batches", 1, 1);
        long[] first  = sg.getNextBatch(5);
        long[] second = sg.getNextBatch(5);

        assertThat(first).containsExactly(1L, 2L, 3L, 4L, 5L);
        assertThat(second).containsExactly(6L, 7L, 8L, 9L, 10L);
    }

    // -------------------------------------------------------------------------
    // Edge cases
    // -------------------------------------------------------------------------

    @Test
    public void getNextBatch_countOne_equivalentToGetNextValue() {
        morphium.dropCollection(Sequence.class);
        morphium.dropCollection(SeqLock.class);

        SequenceGenerator sgA = new SequenceGenerator(morphium, "single_batch_a", 1, 1);
        long[] batch = sgA.getNextBatch(1);
        assertThat(batch).hasSize(1).containsExactly(1L);
    }

    @Test
    public void getNextBatch_invalidCount_zero_throws() {
        SequenceGenerator sg = new SequenceGenerator(morphium, "batch_invalid", 1, 1);
        assertThatThrownBy(() -> sg.getNextBatch(0))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("count must be > 0");
    }

    @Test
    public void getNextBatch_invalidCount_negative_throws() {
        SequenceGenerator sg = new SequenceGenerator(morphium, "batch_invalid2", 1, 1);
        assertThatThrownBy(() -> sg.getNextBatch(-5))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("count must be > 0");
    }

    // -------------------------------------------------------------------------
    // Concurrency
    // -------------------------------------------------------------------------

    @Test
    public void concurrentBatches_noOverlap() throws InterruptedException {
        final int threads    = 4;
        final int batchSize  = 50;
        SequenceGenerator sg = new SequenceGenerator(morphium, "concurrent_batch", 1, 1);

        List<long[]> results = new CopyOnWriteArrayList<>();
        CountDownLatch ready  = new CountDownLatch(threads);
        CountDownLatch start  = new CountDownLatch(1);
        CountDownLatch done   = new CountDownLatch(threads);

        ExecutorService pool = Executors.newFixedThreadPool(threads);
        for (int i = 0; i < threads; i++) {
            pool.submit(() -> {
                ready.countDown();
                try {
                    start.await();
                    results.add(sg.getNextBatch(batchSize));
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    done.countDown();
                }
            });
        }

        ready.await();
        start.countDown();
        assertThat(done.await(30, TimeUnit.SECONDS)).isTrue();
        pool.shutdown();

        // Flatten all returned values into one set — every value must be unique
        Set<Long> allValues = new HashSet<>();
        int totalCount = 0;
        for (long[] batch : results) {
            assertThat(batch).hasSize(batchSize);
            for (long v : batch) {
                assertThat(allValues.add(v))
                    .as("value %d appears in more than one batch (overlap!)", v)
                    .isTrue();
            }
            totalCount += batch.length;
        }
        assertThat(totalCount).isEqualTo(threads * batchSize);
    }

    // -------------------------------------------------------------------------
    // Performance: batch must be substantially faster than N individual calls
    // -------------------------------------------------------------------------

    @Test
    public void getNextBatch_fasterThanNIndividualCalls() {
        final int n = 100;

        // Warm up
        SequenceGenerator sgWarm = new SequenceGenerator(morphium, "perf_warm", 1, 1);
        sgWarm.getNextBatch(5);
        sgWarm.getNextValue();

        morphium.dropCollection(Sequence.class);
        morphium.dropCollection(SeqLock.class);

        // Measure individual calls
        SequenceGenerator sgIndividual = new SequenceGenerator(morphium, "perf_individual", 1, 1);
        long tIndividual = System.currentTimeMillis();
        for (int i = 0; i < n; i++) {
            sgIndividual.getNextValue();
        }
        tIndividual = System.currentTimeMillis() - tIndividual;

        morphium.dropCollection(Sequence.class);
        morphium.dropCollection(SeqLock.class);

        // Measure batch
        SequenceGenerator sgBatch = new SequenceGenerator(morphium, "perf_batch", 1, 1);
        long tBatch = System.currentTimeMillis();
        sgBatch.getNextBatch(n);
        tBatch = System.currentTimeMillis() - tBatch;

        // Batch should be at least 5× faster than N individual calls
        // (conservative factor — in practice it's 10–50× for in-memory driver)
        assertThat(tBatch)
            .as("getNextBatch(%d) [%dms] should be at least 5x faster than %d×getNextValue() [%dms]",
                n, tBatch, n, tIndividual)
            .isLessThan(tIndividual / 5);
    }
}
