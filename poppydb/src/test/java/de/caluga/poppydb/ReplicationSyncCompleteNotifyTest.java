package de.caluga.poppydb;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import de.caluga.morphium.driver.inmem.InMemoryDriver;

/**
 * #306 review follow-up on the initial-sync completion hook (the partial-restore guard
 * release): "initial sync complete" at the gate-opening moment only means the snapshot is
 * copied - the change events buffered in {@code eventQueue} during the snapshot (up to 100k)
 * are not applied yet. Firing the release right there hands candidacy back to a node that is
 * still measurably behind; if the primary dies inside that window, the node may win the
 * election and its stop()-time flush only processes a single batch - the remaining backlog is
 * lost. The notification is therefore only ARMED by the sync thread and FIRED by the batch
 * processor once the backlog has drained - and never by a manager that has been stopped
 * (a superseded manager's late sync thread can still arm it; nothing may fire it).
 */
@Tag("poppydb")
public class ReplicationSyncCompleteNotifyTest {

    private ReplicationManager manager() {
        return new ReplicationManager(new InMemoryDriver(), "localhost", 1);
    }

    @Test
    void releaseIsHeldBackWhileBufferedBacklogIsUnapplied() throws Exception {
        ReplicationManager rm = manager();
        AtomicInteger fired = new AtomicInteger();
        rm.setOnInitialSyncComplete(fired::incrementAndGet);

        rm.running.set(true);
        rm.initialSyncComplete.set(true);
        rm.syncCompleteNotifyPending.set(true);                    // sync thread armed it...
        rm.enqueueEventForTest(Map.of("operationType", "insert")); // ...but backlog remains

        rm.maybeFireSyncCompleteNotify();
        assertEquals(0, fired.get(),
                "the completion notification must not fire while buffered events are still "
                        + "unapplied - the node is measurably behind until the backlog drains");

        // Backlog applied - NOW the node really holds the authoritative state.
        rm.clearEventQueueForTest();
        rm.maybeFireSyncCompleteNotify();
        assertEquals(1, fired.get(), "once the backlog has drained the notification must fire");
    }

    @Test
    void stoppedManagerNeverFires() throws Exception {
        // A superseded ReplicationManager's sync thread can outlive stop() by design (stop()
        // joins it for at most 5s) and still arm the notification when its snapshot finally
        // completes - against a primary that may no longer be the leader. Firing then would
        // release the partial-restore guard on the strength of a stale sync.
        ReplicationManager rm = manager();
        AtomicInteger fired = new AtomicInteger();
        rm.setOnInitialSyncComplete(fired::incrementAndGet);

        rm.running.set(false);                    // stop() ran
        rm.initialSyncComplete.set(true);
        rm.syncCompleteNotifyPending.set(true);   // late sync thread armed it afterwards

        rm.maybeFireSyncCompleteNotify();
        assertEquals(0, fired.get(),
                "a stopped (superseded) manager must never fire the completion notification - "
                        + "its sync ran against a primary that may already be gone");
    }

    @Test
    void notificationFiresExactlyOnce() throws Exception {
        ReplicationManager rm = manager();
        AtomicInteger fired = new AtomicInteger();
        rm.setOnInitialSyncComplete(fired::incrementAndGet);

        rm.running.set(true);
        rm.initialSyncComplete.set(true);
        rm.syncCompleteNotifyPending.set(true);

        rm.maybeFireSyncCompleteNotify();
        rm.maybeFireSyncCompleteNotify();
        assertEquals(1, fired.get(), "the armed notification must fire exactly once per arming");
    }
}
