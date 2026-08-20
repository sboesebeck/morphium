package de.caluga.poppydb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import de.caluga.morphium.driver.inmem.InMemoryDriver;

/**
 * Seam test for the byte budget of the secondary-side replication event queue.
 *
 * <p>The event queue was count-capped (100k events) but unbounded by bytes - 100k queued
 * ~300KB bulk-export events blow any heap. Unlike the replay buffer's byte budget (commit
 * 88acb76b0), overflow must NOT evict here: queued events are not yet applied, dropping one
 * is silent data loss. The budget therefore extends the existing count backpressure to bytes:
 * the producer (watch callback) blocks in {@code enqueueReplicationEvent()} until the
 * consumer's drain frees budget. An event larger than the whole budget must still be admitted
 * into an empty queue (never block forever), and a budget of 0 disables the byte bound.
 *
 * <p>Exercises the production enqueue/drain seams directly through package-private access -
 * the same methods the watch callback and processBatch() use.
 */
public class ReplicationEventQueueByteBudgetTest {

    /** How long we wait to conclude that a producer thread is (still) blocked. */
    private static final long BLOCKED_PROBE_MS = 400;

    private static Map<String, Object> event(int payloadChars) {
        Map<String, Object> doc = new HashMap<>();
        doc.put("_id", "id");
        doc.put("payload", "x".repeat(payloadChars));
        Map<String, Object> ev = new HashMap<>();
        ev.put("operationType", "insert");
        ev.put("fullDocument", doc);
        return ev;
    }

    /** Starts a daemon thread that enqueues {@code ev} and counts down when the enqueue returned. */
    private static Thread enqueueAsync(ReplicationManager mgr, Map<String, Object> ev, CountDownLatch done) {
        Thread t = new Thread(() -> {
            try {
                mgr.enqueueReplicationEvent(ev);
                done.countDown();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, "test-enqueue");
        t.setDaemon(true);
        t.start();
        return t;
    }

    @Test
    @Timeout(30)
    public void bytesAreTrackedOnEnqueueAndReleasedOnDrain() throws Exception {
        ReplicationManager mgr = new ReplicationManager(null, "localhost", 12345);
        mgr.running.set(true);

        Map<String, Object> ev = event(1000);
        long size = InMemoryDriver.estimateBsonSize(ev);
        assertTrue(size > 1000, "estimateBsonSize must reflect the payload, got " + size);

        mgr.enqueueReplicationEvent(ev);
        mgr.enqueueReplicationEvent(event(1000));
        mgr.enqueueReplicationEvent(event(1000));
        assertEquals(3 * size, mgr.getEventQueueBytes(),
                "queued bytes must be the sum of the enqueued events' estimated sizes");

        List<Map<String, Object>> batch = mgr.drainBatch(2);
        assertEquals(2, batch.size());
        assertEquals(size, mgr.getEventQueueBytes(), "drain must release exactly the drained events' bytes");

        assertEquals(1, mgr.drainBatch(10).size());
        assertEquals(0, mgr.getEventQueueBytes(), "empty queue must account 0 bytes");
    }

    @Test
    @Timeout(30)
    public void producerBlocksWhenBudgetExhaustedAndResumesAfterDrain() throws Exception {
        ReplicationManager mgr = new ReplicationManager(null, "localhost", 12345);
        mgr.running.set(true);

        long size = InMemoryDriver.estimateBsonSize(event(1000));
        mgr.setEventQueueByteBudget(2 * size + 10);

        // Two events fit into the budget without blocking.
        mgr.enqueueReplicationEvent(event(1000));
        mgr.enqueueReplicationEvent(event(1000));

        // The third would exceed the budget: the producer must block ...
        CountDownLatch done = new CountDownLatch(1);
        Thread producer = enqueueAsync(mgr, event(1000), done);
        try {
            assertFalse(done.await(BLOCKED_PROBE_MS, TimeUnit.MILLISECONDS),
                    "producer must block once the byte budget is exhausted");
            assertTrue(mgr.getEventQueueBytePressureCount() >= 1,
                    "byte-pressure counter must record the blocked enqueue, got "
                            + mgr.getEventQueueBytePressureCount());

            // ... and resume as soon as a drain frees budget.
            assertEquals(1, mgr.drainBatch(1).size());
            assertTrue(done.await(5, TimeUnit.SECONDS),
                    "producer must be woken when the consumer's drain frees byte budget");
            assertEquals(2 * size, mgr.getEventQueueBytes());
        } finally {
            producer.interrupt();
        }
    }

    @Test
    @Timeout(30)
    public void oversizedEventIsAdmittedIntoEmptyQueue() throws Exception {
        ReplicationManager mgr = new ReplicationManager(null, "localhost", 12345);
        mgr.running.set(true);
        mgr.setEventQueueByteBudget(100);

        // Larger than the whole budget, queue empty: must be admitted, never block forever.
        CountDownLatch first = new CountDownLatch(1);
        Thread p1 = enqueueAsync(mgr, event(10_000), first);
        try {
            assertTrue(first.await(5, TimeUnit.SECONDS),
                    "an event larger than the whole budget must be admitted into an empty queue");
        } finally {
            p1.interrupt();
        }

        // Queue no longer empty: the next oversized event must wait for a full drain.
        CountDownLatch second = new CountDownLatch(1);
        Thread p2 = enqueueAsync(mgr, event(10_000), second);
        try {
            assertFalse(second.await(BLOCKED_PROBE_MS, TimeUnit.MILLISECONDS),
                    "an oversized event must still respect the budget while the queue is non-empty");
            assertEquals(1, mgr.drainBatch(10).size());
            assertTrue(second.await(5, TimeUnit.SECONDS),
                    "draining the queue empty must admit the waiting oversized event");
        } finally {
            p2.interrupt();
        }
    }

    @Test
    @Timeout(30)
    public void zeroBudgetDisablesByteBound() throws Exception {
        ReplicationManager mgr = new ReplicationManager(null, "localhost", 12345);
        mgr.running.set(true);
        mgr.setEventQueueByteBudget(0);

        // Way beyond any reasonable byte bound - must never block with the budget off.
        CountDownLatch done = new CountDownLatch(1);
        Thread producer = new Thread(() -> {
            try {
                for (int i = 0; i < 20; i++) {
                    mgr.enqueueReplicationEvent(event(50_000));
                }
                done.countDown();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, "test-enqueue-unbounded");
        producer.setDaemon(true);
        producer.start();
        try {
            assertTrue(done.await(5, TimeUnit.SECONDS), "budget 0 must disable the byte bound entirely");
            assertEquals(0, mgr.getEventQueueBytePressureCount());
        } finally {
            producer.interrupt();
        }
    }

    @Test
    @Timeout(30)
    public void resyncDiscardReleasesBytesAndWakesBlockedProducer() throws Exception {
        ReplicationManager mgr = new ReplicationManager(null, "localhost", 12345);
        mgr.running.set(true);

        long size = InMemoryDriver.estimateBsonSize(event(1000));
        mgr.setEventQueueByteBudget(2 * size + 10);
        mgr.enqueueReplicationEvent(event(1000));
        mgr.enqueueReplicationEvent(event(1000));

        CountDownLatch done = new CountDownLatch(1);
        Thread producer = enqueueAsync(mgr, event(1000), done);
        try {
            assertFalse(done.await(BLOCKED_PROBE_MS, TimeUnit.MILLISECONDS), "precondition: producer blocked");

            // Resync discards the queue for the lost window - the byte accounting must follow
            // and the blocked producer must be woken. Its event is DROPPED, not admitted (#322):
            // it was received by the retired watch session, so admitting it after the discard
            // would apply stale pre-resync state over the fresh snapshot. (This test used to
            // expect the event to survive - that was the leak, not the contract.)
            mgr.triggerResync(0);
            assertTrue(done.await(5, TimeUnit.SECONDS),
                    "a resync's queue discard must free byte budget and wake the blocked producer");
            assertEquals(0L, mgr.getEventQueueBytes(),
                    "the retired session's event must be dropped, not accounted - admitting it "
                            + "would replay stale pre-resync state over the fresh snapshot");
        } finally {
            producer.interrupt();
        }
    }

    @Test
    public void statsReportByteBudgetState() throws Exception {
        ReplicationManager mgr = new ReplicationManager(null, "localhost", 12345);
        mgr.running.set(true);
        mgr.setEventQueueByteBudget(1234567);

        Map<String, Object> ev = event(1000);
        long size = InMemoryDriver.estimateBsonSize(ev);
        mgr.enqueueReplicationEvent(ev);

        Map<String, Object> stats = mgr.getStats();
        assertEquals(1234567L, stats.get("eventQueueByteBudget"), "stats must report the configured budget");
        assertEquals(size, stats.get("eventQueueBytes"), "stats must report the queued bytes");
        assertEquals(0L, stats.get("eventQueueBytePressureCount"), "stats must report the pressure counter");
    }

    @Test
    public void negativeBudgetIsRejected() {
        ReplicationManager mgr = new ReplicationManager(null, "localhost", 12345);
        assertThrows(IllegalArgumentException.class, () -> mgr.setEventQueueByteBudget(-1));
    }
}
