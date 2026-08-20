package de.caluga.test.morphium.driver.inmem;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The replay buffer's size/byte counters must track the buffer exactly - if they drift below the
 * real weight, {@code changeStreamHistoryBytes > budget} stops firing and the byte budget no
 * longer bounds memory at all. That is the failure mode the budget was introduced for
 * (ACC 2026-08-14, ~4GB pinned by 100k buffered events).
 *
 * <p>Drift was possible because two independent parties remove entries from the history deque -
 * the eviction loop ({@code pollFirst}) and a drop's purge ({@code removeIf}) - and
 * {@code ConcurrentLinkedDeque.removeIf} evaluates its predicate BEFORE the CAS that actually
 * unlinks the node. A predicate that decrements has therefore already decremented when it loses
 * the race, and the entry gets booked out twice.
 *
 * <p>The race itself is not deterministic, but the damage is: drift is monotonic and never
 * repaired, so a single lost round trips the assertions for the rest of the run. To make the
 * collision likely, writers and the dropper work on the SAME collection with a history limit of
 * 2 - the entry the eviction loop pops off the head is then exactly the entry the purge is
 * scanning. Against the pre-fix code this reproduces reliably (observed: size counter at -19);
 * the assertions compare the counters against the buffer's real content rather than against an
 * expected number, so they stay valid whatever the residue happens to be.
 */
@Tag("inmemory")
public class ChangeStreamHistoryAccountingTest {

    @Test
    public void countersStayExactWhenDropsRaceEviction() throws Exception {
        InMemoryDriver drv = new InMemoryDriver();
        drv.connect();

        try {
            // small buffer -> every write evicts, so eviction and purge are permanently in flight
            drv.setChangeStreamHistoryLimit(2);
            final String db = "acct";
            final int writers = 4;
            final int perWriter = 4000;
            CountDownLatch start = new CountDownLatch(1);
            CountDownLatch done = new CountDownLatch(writers + 1);
            AtomicReference<Throwable> failure = new AtomicReference<>();

            for (int w = 0; w < writers; w++) {
                final int id = w;
                Thread t = new Thread(() -> {
                    try {
                        start.await();

                        for (int i = 0; i < perWriter; i++) {
                            // Same collection the dropper purges, so the entry the eviction loop
                            // pops off the head is exactly the entry the purge is scanning - that
                            // is the collision window (predicate runs before the unlink CAS).
                            drv.store(db, "churn", List.of(Doc.of("_id", id * 10000 + i, "v", "x" + i)), null);
                        }
                    } catch (Throwable e) {
                        failure.compareAndSet(null, e);
                    } finally {
                        done.countDown();
                    }
                }, "writer-" + w);
                t.setDaemon(true);
                t.start();
            }

            Thread dropper = new Thread(() -> {
                try {
                    start.await();

                    for (int i = 0; i < 3000; i++) {
                        drv.drop(db, "churn", null);
                    }
                } catch (Throwable e) {
                    failure.compareAndSet(null, e);
                } finally {
                    done.countDown();
                }
            }, "dropper");
            dropper.setDaemon(true);
            dropper.start();

            start.countDown();
            assertTrue(done.await(60, TimeUnit.SECONDS), "workers must finish");

            if (failure.get() != null) {
                throw new AssertionError("worker failed", failure.get());
            }

            // counters may never go below zero at any point in time
            assertTrue(drv.getChangeStreamHistorySize() >= 0,
                    "history size must never be negative: " + drv.getChangeStreamHistorySize());
            assertTrue(drv.getChangeStreamHistoryBytes() >= 0,
                    "history bytes must never be negative: " + drv.getChangeStreamHistoryBytes());

            // the counters must equal what is really in the buffer - that is the whole contract
            assertEquals(drv.getChangeStreamHistoryActualCount(), drv.getChangeStreamHistorySize(),
                    "the size counter must match the buffer exactly after eviction raced drops");
            assertEquals(drv.getChangeStreamHistoryActualBytes(), drv.getChangeStreamHistoryBytes(),
                    "byte accounting must be exact - drift silently disables the budget");

            // and again after dropping everything, which purges through the second removal path
            drv.drop(db, "churn", null);
            drv.drop(db, null);

            assertEquals(drv.getChangeStreamHistoryActualCount(), drv.getChangeStreamHistorySize(),
                    "every buffered event must be booked out exactly once");
            assertEquals(drv.getChangeStreamHistoryActualBytes(), drv.getChangeStreamHistoryBytes());
        } finally {
            drv.close();
        }
    }

    @Test
    public void shrinkingTheBudgetKeepsTheCountersExact() throws Exception {
        InMemoryDriver drv = new InMemoryDriver();
        drv.connect();

        try {
            for (int i = 0; i < 50; i++) {
                drv.store("acct2", "docs", List.of(Doc.of("_id", i, "v", "payload-" + i)), null);
            }

            assertTrue(drv.getChangeStreamHistoryBytes() > 0, "events must be buffered");

            // trim by budget, then by count - both setters evict, and both must book exactly once
            drv.setChangeStreamHistoryByteBudget(200);
            drv.setChangeStreamHistoryLimit(3);
            assertTrue(drv.getChangeStreamHistorySize() <= 3,
                    "count limit must be enforced: " + drv.getChangeStreamHistorySize());

            assertEquals(drv.getChangeStreamHistoryActualCount(), drv.getChangeStreamHistorySize(),
                    "trimming via the setters must book each evicted event exactly once");
            assertEquals(drv.getChangeStreamHistoryActualBytes(), drv.getChangeStreamHistoryBytes());

            drv.drop("acct2", "docs", null);
            drv.drop("acct2", null);

            assertEquals(drv.getChangeStreamHistoryActualCount(), drv.getChangeStreamHistorySize());
            assertEquals(drv.getChangeStreamHistoryActualBytes(), drv.getChangeStreamHistoryBytes());
        } finally {
            drv.close();
        }
    }
}
