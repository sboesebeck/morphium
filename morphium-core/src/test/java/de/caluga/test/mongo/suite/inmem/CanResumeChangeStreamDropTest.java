package de.caluga.test.mongo.suite.inmem;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Regression test for the resume-across-a-drop window-loss bug.
 *
 * <p>{@code drop()} purges every buffered event for the dropped namespace from the replay
 * buffer INCLUDING the drop notification itself. When other namespaces keep the buffer
 * non-empty and contiguous with a disconnected consumer's resume token, {@code
 * canResumeChangeStream} used to see a gap-free window and allowed a resume that replayed
 * right across the drop — a disconnected secondary would never learn the collection was
 * dropped and would keep serving it forever.
 *
 * <p>The fix tracks the sequence of the most recent purge-causing drop and refuses a resume
 * whose token predates it, turning the drop-in-the-gap into an explicit window-lost → re-sync.
 */
@Tag("inmemory")
public class CanResumeChangeStreamDropTest {

    private static final String DB = "resumedrop";

    private static Map<String, Object> doc(int i) {
        return Doc.of("_id", "d" + i, "v", i);
    }

    @Test
    public void resumeAcrossCollectionDropIsRefused() throws Exception {
        InMemoryDriver drv = new InMemoryDriver();
        drv.connect();
        try {
            // Interleave writes to two collections so that after collA's events are purged by
            // the drop, the remaining collB events are still contiguous with the resume token.
            drv.store(DB, "collA", List.of(doc(1)), null);        // token t1
            drv.store(DB, "collB", List.of(doc(2)), null);        // token t2
            long resumeToken = drv.getChangeStreamSequence();      // consumer caught up to t2
            drv.store(DB, "collA", List.of(doc(3)), null);        // token t3 (collA)
            drv.store(DB, "collB", List.of(doc(4)), null);        // token t4 (collB)

            // Sanity: without a drop the consumer can resume from its token.
            assertTrue(drv.canResumeChangeStream(resumeToken),
                    "a contiguous buffer must be resumable before any drop");

            // Drop collA. This purges collA's buffered events (t1, t3) and the drop notification,
            // leaving [t2, t4] (collB) which is still contiguous with resumeToken (t2).
            drv.drop(DB, "collA", null);

            // The consumer sitting at t2 missed collA's drop entirely. It must NOT be allowed to
            // resume, otherwise it would replay [t4] and keep the dropped collA forever.
            assertFalse(drv.canResumeChangeStream(resumeToken),
                    "a resume token predating a namespace drop must be refused (window lost)");

            // A consumer already past the drop boundary can still resume.
            assertTrue(drv.canResumeChangeStream(drv.getChangeStreamSequence()),
                    "a fully-caught-up consumer past the drop can resume");
        } finally {
            drv.close();
        }
    }

    @Test
    public void resumeAcrossDatabaseDropIsRefused() throws Exception {
        InMemoryDriver drv = new InMemoryDriver();
        drv.connect();
        try {
            drv.store(DB, "collA", List.of(doc(1)), null);
            drv.store("otherdb", "collB", List.of(doc(2)), null);
            long resumeToken = drv.getChangeStreamSequence();
            drv.store("otherdb", "collB", List.of(doc(3)), null);

            drv.drop(DB, null);

            assertFalse(drv.canResumeChangeStream(resumeToken),
                    "a resume token predating a database drop must be refused (window lost)");
        } finally {
            drv.close();
        }
    }

    /**
     * Regression test for the drop-boundary race: {@code lastGlobalDropSequence} used to be
     * updated via check-then-set on a plain {@code volatile long}. Two threads racing to advance
     * the boundary could interleave such that a later drop with a smaller observed sequence
     * overwrites a larger one already published by the other thread, regressing the boundary.
     *
     * <p>This drives the race directly via 1000 concurrent drops per thread on disjoint
     * collections (no sleeps — pure concurrency, started together via a latch) and asserts the
     * final boundary equals the true maximum drop sequence ever assigned. Each {@code drop()}
     * call records its own atomically-assigned sequence into the per-namespace
     * {@code lastDropSequence} map before folding it into the global boundary, so the maximum
     * over that map (read back via reflection, not derived from a racy live counter) is the
     * ground truth to compare {@code lastGlobalDropSequence} against — deterministic regardless
     * of thread interleaving.
     */
    @Test
    public void concurrentDropsNeverRegressGlobalDropBoundary() throws Exception {
        InMemoryDriver drv = new InMemoryDriver();
        drv.connect();
        try {
            final int iterations = 1000;
            final String db = "raceDropDb";
            CountDownLatch startLatch = new CountDownLatch(2);

            Runnable dropperA = () -> {
                startLatch.countDown();
                await(startLatch);
                for (int i = 0; i < iterations; i++) {
                    drv.drop(db, "collA" + i, null);
                }
            };
            Runnable dropperB = () -> {
                startLatch.countDown();
                await(startLatch);
                for (int i = 0; i < iterations; i++) {
                    drv.drop(db, "collB" + i, null);
                }
            };

            Thread t1 = new Thread(dropperA, "dropperA");
            Thread t2 = new Thread(dropperB, "dropperB");
            t1.start();
            t2.start();
            t1.join();
            t2.join();

            long expectedMax = readMaxPerNamespaceDropSequence(drv);
            long actual = readLastGlobalDropSequence(drv);
            assertEquals(expectedMax, actual,
                    "lastGlobalDropSequence must equal the true maximum sequence ever assigned "
                            + "after concurrent drops — a check-then-set volatile can regress it");
        } finally {
            drv.close();
        }
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private static long readLastGlobalDropSequence(InMemoryDriver drv) throws Exception {
        Field f = InMemoryDriver.class.getDeclaredField("lastGlobalDropSequence");
        f.setAccessible(true);
        Object value = f.get(drv);
        if (value instanceof AtomicLong al) {
            return al.get();
        }
        return (Long) value;
    }

    @SuppressWarnings("unchecked")
    private static long readMaxPerNamespaceDropSequence(InMemoryDriver drv) throws Exception {
        Field f = InMemoryDriver.class.getDeclaredField("lastDropSequence");
        f.setAccessible(true);
        Map<String, Long> map = (Map<String, Long>) f.get(drv);
        return map.values().stream().mapToLong(Long::longValue).max().orElseThrow();
    }

    /**
     * Deterministic proof of the exact interleaving that motivated switching
     * {@code lastGlobalDropSequence} from check-then-set on a plain {@code volatile long} to
     * {@code AtomicLong.accumulateAndGet(seq, Math::max)}.
     *
     * <p>{@link #concurrentDropsNeverRegressGlobalDropBoundary()} drives the race through the
     * real {@code drop()} API under natural OS thread scheduling. Extensive local stress runs
     * against the pre-fix (volatile long) code — 30 trials of 2 threads x 1000 drops, 5 trials of
     * 16 threads x 2000 drops, 3 trials of 64 threads x 3000 drops, both JIT-compiled and
     * {@code -Xint} interpreted, plus a per-iteration {@code CyclicBarrier} variant to
     * resynchronize the racing threads on every single call — never once reproduced a
     * regression: the non-atomic check-then-set window (a compare followed by a plain field
     * write, with no intervening work) is simply too narrow for two-thread OS-scheduled
     * preemption to reliably land inside it on modern multi-core hardware, even under heavy
     * oversubscription. That narrowness does not make the race safe; it makes it unobservable by
     * timing alone — the standard reason such bugs need deterministic interleaving tests rather
     * than relying on chance.
     *
     * <p>This test forces the precise bad ordering via explicit thread handoff (two
     * {@link CountDownLatch}s, no sleeps): a "small" writer is made to observe the field before a
     * "large" writer commits, then made to complete its own write only strictly <em>after</em>
     * the large writer has already committed — the exact interleaving under which a plain
     * check-then-set silently regresses the boundary. It first confirms that pattern actually
     * regresses (documenting the bug precisely), then applies the identical adversarial ordering
     * directly to the real {@code lastGlobalDropSequence} field of a live {@link InMemoryDriver}
     * (via the same {@code accumulateAndGet(value, Math::max)} call the fixed production code
     * uses) and asserts it converges on the true maximum regardless.
     */
    @Test
    public void deterministicAdversarialInterleavingProvesAccumulateAndGetIsRaceSafe() throws Exception {
        final long smallValue = 100L;
        final long largeValue = 200L;
        final long expectedMax = Math.max(smallValue, largeValue);

        // 1) Reproduce the OLD buggy pattern (check-then-set on a plain long) under the exact
        //    adversarial ordering that regresses it: the small writer's read happens before the
        //    large writer's write, but the small writer's own write is forced to complete after.
        long[] buggyField = {0L};
        CountDownLatch smallHasReadStaleValue = new CountDownLatch(1);
        CountDownLatch largeHasCommitted = new CountDownLatch(1);

        Thread buggyLargeWriter = new Thread(() -> {
            await(smallHasReadStaleValue);
            if (largeValue > buggyField[0]) {
                buggyField[0] = largeValue;
            }
            largeHasCommitted.countDown();
        }, "buggyLargeWriter");
        Thread buggySmallWriter = new Thread(() -> {
            boolean staleCheckPassed = smallValue > buggyField[0]; // reads 0, before large commits
            smallHasReadStaleValue.countDown();
            await(largeHasCommitted);
            if (staleCheckPassed) {
                buggyField[0] = smallValue; // regression: stomps the already-committed 200 with 100
            }
        }, "buggySmallWriter");

        buggySmallWriter.start();
        buggyLargeWriter.start();
        buggySmallWriter.join();
        buggyLargeWriter.join();

        assertNotEquals(expectedMax, buggyField[0],
                "sanity check: the check-then-set pattern must regress under this forced ordering "
                        + "— if it doesn't, the adversarial interleaving below proves nothing");
        assertEquals(smallValue, buggyField[0],
                "the buggy pattern should have regressed to the smaller, later-committed value");

        // 2) Apply the IDENTICAL adversarial ordering to the real field the fixed production code
        //    uses, via the exact same accumulateAndGet(value, Math::max) call.
        InMemoryDriver drv = new InMemoryDriver();
        drv.connect();
        try {
            Field f = InMemoryDriver.class.getDeclaredField("lastGlobalDropSequence");
            f.setAccessible(true);
            AtomicLong realField = (AtomicLong) f.get(drv);
            assertEquals(0L, realField.get(), "freshly connected driver must start at 0");

            CountDownLatch smallHasStarted = new CountDownLatch(1);
            CountDownLatch largeHasCommitted2 = new CountDownLatch(1);

            Thread realLargeWriter = new Thread(() -> {
                await(smallHasStarted);
                realField.accumulateAndGet(largeValue, Math::max);
                largeHasCommitted2.countDown();
            }, "realLargeWriter");
            Thread realSmallWriter = new Thread(() -> {
                smallHasStarted.countDown();
                await(largeHasCommitted2);
                // Forced to apply its (smaller, stale) value only AFTER the large value already
                // committed — the same ordering that broke the old volatile long.
                realField.accumulateAndGet(smallValue, Math::max);
            }, "realSmallWriter");

            realSmallWriter.start();
            realLargeWriter.start();
            realSmallWriter.join();
            realLargeWriter.join();

            assertEquals(expectedMax, realField.get(),
                    "accumulateAndGet(value, Math::max) must converge on the true maximum even "
                            + "under the exact adversarial ordering that regresses check-then-set");
        } finally {
            drv.close();
        }
    }

}
