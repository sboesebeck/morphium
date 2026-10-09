package de.caluga.poppydb;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.Test;

import de.caluga.morphium.driver.DriverTailableIterationCallback;

/**
 * The watch-staleness fix: a ReplicationManager watch must NOT be declared stale on a
 * quiet cluster. The staleness tracker (lastWatchResponseTime) is refreshed both by
 * incoming data AND by empty getMore batches (the liveness heartbeat a healthy-but-idle
 * stream answers every maxTimeMS). A reader blocked inside incomingData (byte-budget
 * backpressure) never receives that heartbeat, so its watch is still declared stale -
 * the dead-watch guard keeps working.
 */
public class ReplicationManagerStaleCheckTest {

    @Test
    public void idleBatchHookExistsAndIsDefaultNoOp() {
        DriverTailableIterationCallback cb = new DriverTailableIterationCallback() {
            @Override
            public void incomingData(Map<String, Object> data, long dur) {
            }

            @Override
            public boolean isContinued() {
                return true;
            }
        };
        // Must not throw and must not require an override (backwards compatible).
        cb.onIdleBatch();
    }

    @Test
    public void idleBatchRefreshesStalenessTrackerLikeIncomingData() {
        // Simulate the ReplicationManager's callback contract with a standalone harness:
        // both real events (incomingData) and empty batches (onIdleBatch) refresh the
        // staleness timer; a blocked reader (neither fired) goes stale after 30s.
        AtomicLong lastWatchResponseTime = new AtomicLong(0);
        DriverTailableIterationCallback cb = new DriverTailableIterationCallback() {
            @Override
            public void incomingData(Map<String, Object> data, long dur) {
                lastWatchResponseTime.set(System.currentTimeMillis());
            }

            @Override
            public void onIdleBatch() {
                lastWatchResponseTime.set(System.currentTimeMillis());
            }

            @Override
            public boolean isContinued() {
                long last = lastWatchResponseTime.get();
                return last == 0 || (System.currentTimeMillis() - last) <= 30_000;
            }
        };

        // No heartbeat for 31s -> stale (blocked reader / truly dead stream).
        lastWatchResponseTime.set(System.currentTimeMillis() - 31_000);
        assertFalse(cb.isContinued());

        // Empty-batch heartbeat refreshes the timer -> alive again.
        lastWatchResponseTime.set(System.currentTimeMillis() - 31_000);
        cb.onIdleBatch();
        assertTrue(cb.isContinued());

        // Real event refreshes it too.
        lastWatchResponseTime.set(System.currentTimeMillis() - 31_000);
        cb.incomingData(Map.of(), 0);
        assertTrue(cb.isContinued());
    }
}