package de.caluga.test.morphium.driver.inmem;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Phase B2, Task 3: bounded top-N heap for unindexed sort+limit queries.
 *
 * <p>Pins two things at once on a 100k-document, UNINDEXED-field dataset:
 * <ul>
 *   <li><b>Correctness</b> - {@code find(sort, skip, limit)} must return exactly the same
 *       documents, in the same order, as an independently-computed full-sort oracle (built
 *       without going through the driver at all).</li>
 *   <li><b>Not a full sort</b> - the number of field-comparator invocations a bounded
 *       {@code sort+limit=10} query performs must be a small fraction of what an UNBOUNDED sort
 *       over the same data performs. A full {@code Collections.sort} is O(n log n) comparisons;
 *       a bounded top-N heap of capacity {@code skip+limit} is O(n + k log n) - the ratio between
 *       the two comparator counts on the same 100k-document dataset is the operation-count
 *       evidence that the bounded query does not sort the whole collection.</li>
 * </ul>
 *
 * <p>The comparator-call counter is wired in via a custom {@link Comparable} field value
 * ({@link CountingScore}) rather than instrumenting the driver - the driver treats it exactly
 * like any other {@code Comparable} field value (in particular, it survives find()'s deep-copy
 * of returned documents unchanged, since non-Map/non-List field values are shared by reference,
 * not cloned - see {@code InMemoryDriver.deepCopyDoc}).
 */
@Tag("inmemory")
public class TopNHeapSortTest {

    private static final int DOC_COUNT = 100_000;

    /** Comparable wrapper that counts every compareTo() invocation via a shared counter. */
    private static final class CountingScore implements Comparable<CountingScore> {
        final int value;
        final AtomicLong counter;

        CountingScore(int value, AtomicLong counter) {
            this.value = value;
            this.counter = counter;
        }

        @Override
        public int compareTo(CountingScore o) {
            counter.incrementAndGet();
            return Integer.compare(value, o.value);
        }
    }

    @Test
    void topNHeapMatchesOracleAndAvoidsFullSort() throws Exception {
        InMemoryDriver drv = new InMemoryDriver();
        drv.connect();
        AtomicLong compareCount = new AtomicLong();

        try {
            String db = "topn";
            String coll = "docs";

            // Unique, shuffled scores 0..DOC_COUNT-1 on an UNINDEXED field ("score" - only the
            // default _id index exists on a freshly created collection), so `find` cannot take
            // the index-ordered-iterator path (B1 Task 6) and must fall through to this task's
            // heap/full-sort logic. Uniqueness sidesteps tie-breaking/stability entirely - the
            // expected top-K is unambiguous regardless of sort stability.
            List<Integer> values = new ArrayList<>(DOC_COUNT);
            for (int i = 0; i < DOC_COUNT; i++) {
                values.add(i);
            }
            Collections.shuffle(values, new java.util.Random(42));

            // Independent oracle: the id -> value assignment recorded WITHOUT ever touching
            // CountingScore.compareTo, so building it doesn't pollute the comparator counters
            // measured below. Sorting a plain int[] via Arrays.sort avoids Integer boxing
            // comparisons being conflated with the driver's own field comparisons too.
            int[] scoreById = new int[DOC_COUNT];
            List<Map<String, Object>> batch = new ArrayList<>(DOC_COUNT);
            for (int id = 0; id < DOC_COUNT; id++) {
                int v = values.get(id);
                scoreById[id] = v;
                batch.add(Doc.of("_id", id, "score", new CountingScore(v, compareCount)));
            }
            drv.store(db, coll, batch, null);

            // Oracle: ascending top (skip=5, limit=10) computed independently of the driver.
            int skip = 5;
            int limit = 10;
            List<Integer> sortedValues = new ArrayList<>();
            for (int v : scoreById) {
                sortedValues.add(v);
            }
            Collections.sort(sortedValues);
            List<Integer> expectedWindow = sortedValues.subList(skip, skip + limit);

            // --- Correctness: bounded query must match the oracle exactly, in order ---
            compareCount.set(0);
            List<Map<String, Object>> boundedResult = drv.find(db, coll, Doc.of(), Doc.of("score", 1), null, skip, limit);
            long boundedCompares = compareCount.get();

            assertEquals(limit, boundedResult.size(), "must return exactly `limit` documents");
            List<Integer> actualWindow = new ArrayList<>();
            for (Map<String, Object> doc : boundedResult) {
                actualWindow.add(((CountingScore) doc.get("score")).value);
            }
            assertEquals(expectedWindow, actualWindow,
                    "bounded sort+skip+limit must match the independently-computed full-sort oracle");

            // --- Not a full sort: compare against an UNBOUNDED sort over the same dataset ---
            compareCount.set(0);
            List<Map<String, Object>> fullResult = drv.find(db, coll, Doc.of(), Doc.of("score", 1), null, 0, 0);
            long fullSortCompares = compareCount.get();

            assertEquals(DOC_COUNT, fullResult.size(), "sanity: unbounded query returns everything");
            // Sanity: the unbounded fallback is untouched by this task and must still be a real
            // full sort - if this ever drops to O(n) something else broke, not what we're testing.
            assertTrue(fullSortCompares > (long) DOC_COUNT * 10,
                    "sanity: full sort should be roughly O(n log n) comparisons, was " + fullSortCompares);

            assertTrue(boundedCompares * 3 < fullSortCompares,
                    "bounded sort+limit=10 should need far fewer comparator calls than an unbounded sort "
                    + "over the same " + DOC_COUNT + " docs (bounded=" + boundedCompares
                    + ", full=" + fullSortCompares + ") - a bounded query performing anywhere close "
                    + "to the full-sort comparator count means it is still sorting everything");
        } finally {
            drv.shutdown(true);
        }
    }
}
