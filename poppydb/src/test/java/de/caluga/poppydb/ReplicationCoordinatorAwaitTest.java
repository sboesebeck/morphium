package de.caluga.poppydb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import de.caluga.poppydb.ReplicationCoordinator.WaitOutcome;

/**
 * The non-blocking write-concern wait (#529): {@link ReplicationCoordinator#awaitReplication}
 * registers a waiter instead of blocking a thread, a progress report or one of the coordinator's
 * timers completes it, and the callback runs exactly once - whatever races. The blocking
 * {@code waitForReplication} overloads stay thin wrappers around it and keep their results.
 */
public class ReplicationCoordinatorAwaitTest {

    private static final long TIMEOUT_MS = 5_000;

    private ReplicationCoordinator coordinator;

    @AfterEach
    void tearDown() {
        if (coordinator != null) {
            coordinator.close();
            coordinator = null;
        }
    }

    /** Collects what a wait's callback reported; the callback may run on any thread. */
    private static final class Recorder implements Consumer<WaitOutcome> {
        private final CountDownLatch completed = new CountDownLatch(1);
        private final AtomicInteger calls = new AtomicInteger();
        private final List<WaitOutcome> outcomes = new CopyOnWriteArrayList<>();
        private final AtomicReference<Thread> callbackThread = new AtomicReference<>();

        @Override
        public void accept(WaitOutcome outcome) {
            calls.incrementAndGet();
            outcomes.add(outcome);
            callbackThread.set(Thread.currentThread());
            completed.countDown();
        }

        WaitOutcome await(long timeoutMs) throws InterruptedException {
            assertTrue(completed.await(timeoutMs, TimeUnit.MILLISECONDS),
                    "the wait must complete within " + timeoutMs + " ms");
            return outcomes.get(0);
        }

        boolean isDone() {
            return completed.getCount() == 0;
        }

        int calls() {
            return calls.get();
        }

        Thread callbackThread() {
            return callbackThread.get();
        }
    }

    private Recorder register(long sequenceNumber, int w, long timeoutMs, long startedAtMs) {
        Recorder recorder = new Recorder();
        coordinator.awaitReplication(sequenceNumber, w, timeoutMs, startedAtMs, recorder);
        return recorder;
    }

    // ---- the happy paths ----

    @Test
    @Timeout(20)
    public void aLaterReportAcknowledgesTheWait() throws Exception {
        coordinator = new ReplicationCoordinator(3, TIMEOUT_MS);
        coordinator.reportProgress("secondary-1:27017", 5);

        Recorder recorder = register(6, 2, TIMEOUT_MS, System.currentTimeMillis());
        assertFalse(recorder.isDone(), "sequence 6 is not reported yet - the wait must stay open");

        coordinator.reportProgress("secondary-1:27017", 6);

        assertEquals(WaitOutcome.ACKNOWLEDGED, recorder.await(2_000));
        assertEquals(1, recorder.calls(), "the callback must run exactly once");
    }

    @Test
    @Timeout(20)
    public void anAlreadyAcknowledgedSequenceCompletesOnTheCallingThread() {
        coordinator = new ReplicationCoordinator(3, TIMEOUT_MS);
        coordinator.reportProgress("secondary-1:27017", 10);

        Recorder recorder = register(10, 2, TIMEOUT_MS, System.currentTimeMillis());

        assertTrue(recorder.isDone(), "an already covered sequence must not register a wait at all");
        assertEquals(WaitOutcome.ACKNOWLEDGED, recorder.outcomes.get(0));
        assertSame(Thread.currentThread(), recorder.callbackThread(),
                "it must complete synchronously, on the thread that called awaitReplication");
    }

    @Test
    @Timeout(20)
    public void wOneNeverWaits() {
        coordinator = new ReplicationCoordinator(3, TIMEOUT_MS);

        Recorder recorder = register(1, 1, TIMEOUT_MS, System.currentTimeMillis());

        assertTrue(recorder.isDone());
        assertEquals(WaitOutcome.ACKNOWLEDGED, recorder.outcomes.get(0));
        assertEquals(0, coordinator.getWaitStats().get("waiting"));
    }

    // ---- how many secondaries a wait needs ----

    @Test
    @Timeout(20)
    public void wThreeNeedsTwoSecondaries() throws Exception {
        coordinator = new ReplicationCoordinator(3, TIMEOUT_MS);

        Recorder recorder = register(6, 3, TIMEOUT_MS, System.currentTimeMillis());

        coordinator.reportProgress("secondary-1:27017", 6);
        Thread.sleep(100);
        assertFalse(recorder.isDone(), "one secondary is not a w=3 concern on a three-node set");

        coordinator.reportProgress("secondary-2:27017", 6);

        assertEquals(WaitOutcome.ACKNOWLEDGED, recorder.await(2_000));
        assertEquals(1, recorder.calls());
    }

    @Test
    @Timeout(20)
    public void aStaleSecondaryDoesNotCount() throws Exception {
        coordinator = new ReplicationCoordinator(3, TIMEOUT_MS);
        // Reported 31s ago and nothing refreshed it since: past the coordinator's 30s liveness
        // window, like a secondary that vanished.
        coordinator.reportProgressAt("secondary-1:27017", 6, System.currentTimeMillis() - 31_000);

        Recorder recorder = register(6, 2, TIMEOUT_MS, System.currentTimeMillis());
        assertFalse(recorder.isDone(), "a stale secondary must not acknowledge a write");

        coordinator.reportProgress("secondary-1:27017", 6);

        assertEquals(WaitOutcome.ACKNOWLEDGED, recorder.await(2_000));
    }

    // ---- timeouts ----

    @Test
    @Timeout(20)
    public void timeoutFiresAtTheWritesDeadline() throws Exception {
        coordinator = new ReplicationCoordinator(3, TIMEOUT_MS);
        coordinator.reportProgress("secondary-1:27017", 1); // live, but stuck at sequence 1

        long startedAtMs = System.currentTimeMillis();
        Recorder recorder = register(2, 2, 300, startedAtMs);

        assertEquals(WaitOutcome.TIMEOUT, recorder.await(3_000));

        long elapsedMs = System.currentTimeMillis() - startedAtMs;
        assertTrue(elapsedMs >= 300, "the timeout must not fire early (took " + elapsedMs + " ms)");
        assertTrue(elapsedMs < 1_500, "the timeout must count from the write, not from a later point (took "
                + elapsedMs + " ms)");
        assertEquals(1, recorder.calls());
    }

    @Test
    @Timeout(20)
    public void anAlreadyExpiredWaitCompletesAtOnce() {
        coordinator = new ReplicationCoordinator(3, TIMEOUT_MS);
        coordinator.reportProgress("secondary-1:27017", 1);

        long startedAtMs = System.currentTimeMillis();
        Recorder recorder = register(2, 2, 300, startedAtMs - 301);

        assertTrue(recorder.isDone(), "a wait whose timeout was spent before it was registered must not wait");
        assertEquals(WaitOutcome.TIMEOUT, recorder.outcomes.get(0));
        assertTrue(System.currentTimeMillis() - startedAtMs < 200);
        assertEquals(0, coordinator.getWaitStats().get("waiting"));
    }

    @Test
    @Timeout(20)
    public void noSecondariesIsAnsweredAfterTheGraceNotAfterTheTimeout() throws Exception {
        coordinator = new ReplicationCoordinator(3, TIMEOUT_MS);

        long startedAtMs = System.currentTimeMillis();
        Recorder recorder = register(1, 2, TIMEOUT_MS, startedAtMs);

        assertEquals(WaitOutcome.NO_SECONDARIES, recorder.await(3_000));

        long elapsedMs = System.currentTimeMillis() - startedAtMs;
        assertTrue(elapsedMs >= 100, "the 100 ms grace must be given (" + elapsedMs + " ms)");
        assertTrue(elapsedMs < 2_000, "no secondaries must fail fast, not run into the full timeout ("
                + elapsedMs + " ms)");
        assertEquals(1L, coordinator.getWaitStats().get("noSecondaries"));
    }

    // ---- step-down ----

    @Test
    @Timeout(20)
    public void closeReleasesRegisteredWaits() throws Exception {
        coordinator = new ReplicationCoordinator(3, TIMEOUT_MS);
        coordinator.reportProgress("secondary-1:27017", 1); // live, so no NO_SECONDARIES grace

        List<Recorder> recorders = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            recorders.add(register(2 + i, 2, TIMEOUT_MS, System.currentTimeMillis()));
        }

        coordinator.close();

        for (Recorder recorder : recorders) {
            assertEquals(WaitOutcome.STEPPED_DOWN, recorder.await(1_000),
                    "a wait of a node that is no longer primary must end at once, not at its timeout");
            assertEquals(1, recorder.calls());
        }

        assertEquals(0, coordinator.getWaitStats().get("waiting"));
        assertEquals(5L, coordinator.getWaitStats().get("steppedDown"));
    }

    @Test
    @Timeout(20)
    public void registrationAfterCloseIsSteppedDownImmediately() {
        coordinator = new ReplicationCoordinator(3, TIMEOUT_MS);
        coordinator.reportProgress("secondary-1:27017", 1);
        coordinator.close();

        Recorder recorder = register(2, 2, TIMEOUT_MS, System.currentTimeMillis());

        assertTrue(recorder.isDone(), "there is no secondary left that could report to a closed coordinator");
        assertEquals(WaitOutcome.STEPPED_DOWN, recorder.outcomes.get(0));
    }

    /**
     * The callback hands the answer on (in the server: to the channel's event loop); it must never
     * run while the registry is locked, whichever path completes the wait.
     */
    @Test
    @Timeout(20)
    public void callbacksNeverRunUnderTheCoordinatorLock() throws Exception {
        coordinator = new ReplicationCoordinator(3, TIMEOUT_MS);
        coordinator.reportProgress("secondary-1:27017", 1);
        List<Boolean> lockHeld = new CopyOnWriteArrayList<>();
        CountDownLatch done = new CountDownLatch(4);
        Consumer<WaitOutcome> probe = outcome -> {
            lockHeld.add(coordinator.isWaitersLockHeldByCurrentThread());
            done.countDown();
        };

        coordinator.awaitReplication(1, 2, TIMEOUT_MS, System.currentTimeMillis(), probe); // immediate
        coordinator.awaitReplication(2, 2, TIMEOUT_MS, System.currentTimeMillis(), probe); // by report
        coordinator.awaitReplication(50, 2, 100, System.currentTimeMillis(), probe);       // by timeout
        coordinator.awaitReplication(60, 2, TIMEOUT_MS, System.currentTimeMillis(), probe); // by close
        coordinator.reportProgress("secondary-1:27017", 2);
        Thread.sleep(300);
        coordinator.close();

        assertTrue(done.await(2, TimeUnit.SECONDS), "all four waits must complete");
        assertEquals(List.of(false, false, false, false), lockHeld);
    }

    /**
     * A report's scan stops early once the covering count is below what the least demanding
     * waiter needs - it must not stop before a less demanding waiter at a later sequence.
     */
    @Test
    @Timeout(20)
    public void aLaterLessDemandingWaitIsAcknowledgedPastAnIncompleteOne() throws Exception {
        coordinator = new ReplicationCoordinator(3, TIMEOUT_MS);
        coordinator.reportProgress("secondary-1:27017", 1);

        Recorder needsTwo = register(5, 3, TIMEOUT_MS, System.currentTimeMillis());
        Recorder needsOne = register(6, 2, TIMEOUT_MS, System.currentTimeMillis());

        coordinator.reportProgress("secondary-1:27017", 6);

        assertEquals(WaitOutcome.ACKNOWLEDGED, needsOne.await(2_000));
        assertFalse(needsTwo.isDone(), "one secondary is not enough for w=3");
        assertEquals(1, coordinator.getWaitStats().get("waiting"));

        coordinator.reportProgress("secondary-2:27017", 6);
        assertEquals(WaitOutcome.ACKNOWLEDGED, needsTwo.await(2_000));
        assertEquals(0, coordinator.getWaitStats().get("waiting"));
    }

    /** One callback that throws must not cost the other released waits their answer. */
    @Test
    @Timeout(20)
    public void aThrowingCallbackDoesNotStopClose() throws Exception {
        coordinator = new ReplicationCoordinator(3, TIMEOUT_MS);
        coordinator.reportProgress("secondary-1:27017", 1);

        coordinator.awaitReplication(2, 2, TIMEOUT_MS, System.currentTimeMillis(), outcome -> {
            throw new Error("callback failure");
        });
        Recorder second = register(3, 2, TIMEOUT_MS, System.currentTimeMillis());

        coordinator.close();

        assertEquals(WaitOutcome.STEPPED_DOWN, second.await(1_000));
        assertEquals(2L, coordinator.getWaitStats().get("steppedDown"));
    }

    // ---- races and scale ----

    /**
     * Progress reports, timeouts and a step-down all complete the same waiters: every one of
     * them must report exactly once, never zero and never twice.
     */
    @Test
    @Timeout(60)
    public void exactlyOneCallbackUnderRaces() throws Exception {
        coordinator = new ReplicationCoordinator(3, TIMEOUT_MS);
        int waiterCount = 200;
        int reporterCount = 4;

        List<Recorder> recorders = new CopyOnWriteArrayList<>();
        ExecutorService pool = Executors.newFixedThreadPool(6);
        try {
            AtomicInteger registered = new AtomicInteger();
            for (int t = 0; t < 4; t++) {
                pool.submit(() -> {
                    for (int i = 0; i < waiterCount / 4; i++) {
                        long seq = registered.incrementAndGet();
                        Recorder recorder = new Recorder();
                        recorders.add(recorder);
                        // Short, jittered deadlines: reports, timeouts and the close below all
                        // race for the same waiters.
                        coordinator.awaitReplication(seq, 2,
                                ThreadLocalRandom.current().nextLong(20, 120),
                                System.currentTimeMillis(), recorder);
                    }
                });
            }

            for (int t = 0; t < reporterCount; t++) {
                final int reporter = t;
                pool.submit(() -> {
                    try {
                        for (long seq = 0; seq < waiterCount; seq += 3) {
                            coordinator.reportProgress("secondary-" + reporter + ":27017", seq);
                            Thread.sleep(1);
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });
            }

            Thread.sleep(150);
            pool.submit(coordinator::close);

            long deadline = System.currentTimeMillis() + 20_000;
            while (System.currentTimeMillis() < deadline
                    && recorders.stream().anyMatch(r -> !r.isDone())) {
                Thread.sleep(20);
            }

            assertEquals(waiterCount, recorders.size());
            for (Recorder recorder : recorders) {
                assertEquals(1, recorder.calls(), "every wait must complete exactly once, got "
                        + recorder.calls() + " callbacks with outcomes " + recorder.outcomes);
            }
        } finally {
            pool.shutdownNow();
        }
    }

    /**
     * The incident's shape in one assertion: 10,000 waits that cannot be acknowledged must cost
     * no threads, and an already covered sequence is still answered at once while they are all
     * registered.
     */
    @Test
    @Timeout(60)
    public void tenThousandWaitsCostNoThreadsAndDelayNobody() {
        coordinator = new ReplicationCoordinator(3, 10_000);
        // A live secondary that is stuck at sequence 5: the waits below are unacknowledgeable
        // but do not hit the "no secondaries" fast path, so they stay registered until close().
        coordinator.reportProgress("secondary-1:27017", 5);

        int threadsBefore = ManagementFactory.getThreadMXBean().getThreadCount();

        List<Recorder> recorders = new ArrayList<>(10_000);
        for (int i = 0; i < 10_000; i++) {
            Recorder recorder = new Recorder();
            coordinator.awaitReplication(6 + i, 2, 10_000, System.currentTimeMillis(), recorder);
            recorders.add(recorder);
        }

        int threadsAfter = ManagementFactory.getThreadMXBean().getThreadCount();
        assertTrue(threadsAfter - threadsBefore <= 2, "10,000 waiting writes must not cost threads (before "
                + threadsBefore + ", after " + threadsAfter + ")");
        assertEquals(10_000, coordinator.getWaitStats().get("waiting"));

        // A sequence the live secondary already covers is answered immediately - the registry
        // size must not delay anybody.
        long startedAtMs = System.currentTimeMillis();
        Recorder quick = register(3, 2, 10_000, startedAtMs);

        assertTrue(quick.isDone(), "an already acknowledged write must not wait behind 10,000 other waits");
        assertEquals(WaitOutcome.ACKNOWLEDGED, quick.outcomes.get(0));
        assertTrue(System.currentTimeMillis() - startedAtMs < 200,
                "answering it must not take measurable time");

        for (Recorder recorder : recorders) {
            assertFalse(recorder.isDone(), "the 10,000 unacknowledgeable waits must still be waiting");
        }
    }

    /**
     * The blocking overloads stay wrappers: {@code true} only for ACKNOWLEDGED, and a wait that
     * already spent its timeout returns at once (the hotfix behaviour).
     */
    @Test
    @Timeout(20)
    public void blockingWrappersKeepTheirResults() {
        coordinator = new ReplicationCoordinator(3, TIMEOUT_MS);
        coordinator.reportProgress("secondary-1:27017", 10);

        assertTrue(coordinator.waitForReplication(10, 2, TIMEOUT_MS));
        assertTrue(coordinator.waitForReplication(5, 1, TIMEOUT_MS), "w=1 never waits");

        long startedAtMs = System.currentTimeMillis();
        assertFalse(coordinator.waitForReplication(11, 2, 300, startedAtMs - 301));
        assertTrue(System.currentTimeMillis() - startedAtMs < 500, "an expired wait must return at once");

        assertFalse(coordinator.waitForReplication(11, 2, 200), "an unacknowledged write is not acknowledged");
    }
}
