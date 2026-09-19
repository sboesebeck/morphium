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

    /** An event as the watch delivers it: the resume token carries the primary's sequence as hex. */
    private static Map<String, Object> eventWithSequence(long seq) {
        return Map.of("_id", Map.of("_data", Long.toHexString(seq)), "operationType", "insert");
    }

    /**
     * #370: "the queue is empty right now" samples a level where the guard wants a rate. Under
     * sustained load a tick rarely finds the queue empty, so a node whose sync has in fact
     * completed is never released - and since #352 that blocks candidacy, so a set whose
     * secondaries both resynced under load cannot elect a primary when the current one dies.
     * The release must instead be "everything that existed when the snapshot finished has been
     * applied", which is reached under load rather than in spite of it.
     */
    @Test
    void releaseIsReachedUnderSustainedLoadOnceTheHighWaterMarkIsApplied() throws Exception {
        ReplicationManager rm = manager();
        AtomicInteger fired = new AtomicInteger();
        rm.setOnInitialSyncComplete(fired::incrementAndGet);
        rm.running.set(true);

        // Snapshot in progress: the watch buffers two events behind the closed gate.
        rm.enqueueEventForTest(eventWithSequence(0x10));
        rm.enqueueEventForTest(eventWithSequence(0x20));
        // Snapshot finishes: the mark is the newest sequence seen so far.
        rm.captureSyncCompleteWatermark();
        assertEquals(0x20, rm.syncCompleteWatermark.get(),
                "the mark is the highest sequence the watch had delivered when the snapshot finished");
        rm.initialSyncComplete.set(true);
        rm.syncCompleteNotifyPending.set(true);

        // Load keeps coming: from here on no tick ever observes an empty queue.
        rm.enqueueEventForTest(eventWithSequence(0x30));

        rm.maybeFireSyncCompleteNotify();
        assertEquals(0, fired.get(), "nothing applied yet - the node is still behind the mark");

        rm.advanceLastAppliedSequenceForTest(0x10);
        rm.maybeFireSyncCompleteNotify();
        assertEquals(0, fired.get(), "half the buffered backlog applied - still behind the mark");

        rm.advanceLastAppliedSequenceForTest(0x20);
        rm.maybeFireSyncCompleteNotify();
        assertEquals(1, fired.get(),
                "everything that existed when the snapshot finished is applied: release, "
                        + "even though live events keep the queue non-empty");
    }

    /**
     * The empty-queue rule stays as the fallback. A sync with no sequence information has a mark
     * of 0, and on a quiet stream a trailing event that failed to apply never advances the
     * sequence past itself (the documented poison-skip trade-off) - in both cases only "nothing is
     * pending" can release, and it must.
     */
    @Test
    void anEmptyQueueStillReleasesWhenTheMarkCannotBeReached() throws Exception {
        ReplicationManager rm = manager();
        AtomicInteger fired = new AtomicInteger();
        rm.setOnInitialSyncComplete(fired::incrementAndGet);
        rm.running.set(true);
        rm.initialSyncComplete.set(true);
        rm.syncCompleteWatermark.set(0x20);   // never reached: lastAppliedSequence stays 0
        rm.syncCompleteNotifyPending.set(true);

        rm.maybeFireSyncCompleteNotify();
        assertEquals(1, fired.get(),
                "an empty queue means nothing is pending, whatever the mark says");
    }

    /** With no sequence information at all (mark 0) a non-empty queue must still hold the release. */
    @Test
    void aZeroMarkDoesNotReleaseOverABacklog() throws Exception {
        ReplicationManager rm = manager();
        AtomicInteger fired = new AtomicInteger();
        rm.setOnInitialSyncComplete(fired::incrementAndGet);
        rm.running.set(true);
        rm.initialSyncComplete.set(true);
        rm.syncCompleteNotifyPending.set(true);
        rm.enqueueEventForTest(Map.of("operationType", "insert"));   // no _id: sequence unknown

        rm.maybeFireSyncCompleteNotify();
        assertEquals(0, fired.get(),
                "0 >= 0 must not count as 'caught up' - a zero mark means no information, "
                        + "and the empty-queue rule alone decides");
    }

    /**
     * A discarded session must not leave its sequences in the mark (plan review A1): after a
     * resync the next snapshot's mark is built from that cycle's events, or a primary whose
     * counter regressed leaves an old, unreachable mark behind and the node is back to starving.
     */
    @Test
    void aResyncForgetsTheSequencesOfTheDiscardedSession() throws Exception {
        ReplicationManager rm = manager();
        rm.running.set(true);
        rm.enqueueEventForTest(eventWithSequence(0x500));

        rm.triggerResync(0x500);            // drains the queue, retires the session
        rm.enqueueEventForTest(eventWithSequence(0x10));
        rm.captureSyncCompleteWatermark();

        assertEquals(0x10, rm.syncCompleteWatermark.get(),
                "the mark must come from the new session only - the old high sequence is gone");
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
