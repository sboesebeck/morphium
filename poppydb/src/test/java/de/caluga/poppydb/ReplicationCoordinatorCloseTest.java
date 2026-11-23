package de.caluga.poppydb;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Prod bus, 2026-11-20 and 2026-11-23: after a leader change back to the same node every w=2
 * write on it timed out for minutes. The waits of the previous leadership still referenced the
 * dropped coordinator, nobody reported to it any more, and each held a thread of the fixed
 * command pool for a full timeout - the writes of the new leadership queued behind them.
 */
public class ReplicationCoordinatorCloseTest {

    private static final long TIMEOUT_MS = 5_000;

    /** The incident's shape: more dead waits than pool threads, then the node steps down. */
    @Test
    public void closeReleasesQueuedAndRunningWaits() throws Exception {
        ReplicationCoordinator coordinator = new ReplicationCoordinator(3, TIMEOUT_MS);
        // A secondary that followed this primary up to sequence 10 and then left.
        coordinator.reportProgress("secondary:27017", 10);

        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            List<Future<Boolean>> waits = new ArrayList<>();
            for (int i = 0; i < 20; i++) {
                long seq = 11 + i;
                waits.add(pool.submit(() -> coordinator.waitForReplication(seq, 2, TIMEOUT_MS, System.currentTimeMillis())));
            }

            Thread.sleep(200); // two waits are running, eighteen are queued
            long start = System.currentTimeMillis();
            coordinator.close();

            for (Future<Boolean> wait : waits) {
                assertFalse(wait.get(2, TimeUnit.SECONDS), "a wait on a closed coordinator is not acknowledged");
            }
            long tookMs = System.currentTimeMillis() - start;
            assertTrue(tookMs < 1_000, "20 dead waits on 2 threads must end at once, took " + tookMs
                    + " ms (10 timeouts in a row would be " + 10 * TIMEOUT_MS + " ms)");
        } finally {
            pool.shutdownNow();
        }
    }

    /** A wait that already spent its timeout in the pool's queue must not take a thread for another one. */
    @Test
    public void timeoutCountsFromTheWrite() {
        ReplicationCoordinator coordinator = new ReplicationCoordinator(3, TIMEOUT_MS);
        coordinator.reportProgress("secondary:27017", 10);

        long start = System.currentTimeMillis();
        boolean acknowledged = coordinator.waitForReplication(11, 2, TIMEOUT_MS, start - TIMEOUT_MS - 1);
        long tookMs = System.currentTimeMillis() - start;

        assertFalse(acknowledged);
        assertTrue(tookMs < 500, "an expired wait must return at once, took " + tookMs + " ms");
    }

    /** Closing must not turn an acknowledged write into a failure, nor break the normal path. */
    @Test
    public void acknowledgedWritesStayAcknowledged() {
        ReplicationCoordinator coordinator = new ReplicationCoordinator(3, TIMEOUT_MS);
        coordinator.reportProgress("secondary:27017", 10);

        assertTrue(coordinator.waitForReplication(10, 2, TIMEOUT_MS));
        assertTrue(coordinator.waitForReplication(5, 1, TIMEOUT_MS), "w=1 never waits");
    }
}
