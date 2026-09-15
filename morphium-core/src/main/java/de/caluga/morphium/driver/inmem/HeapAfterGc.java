package de.caluga.morphium.driver.inmem;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.NotificationEmitter;
import javax.management.NotificationListener;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;
import java.util.Map;

/**
 * Heap occupancy as of the end of the most recent garbage collection, taken as one coherent
 * snapshot (#368).
 *
 * <p>The reading this replaced summed {@code MemoryPoolMXBean.getCollectionUsage()} over the heap
 * pools. That value is refreshed for a pool only when a collection touches <em>that</em> pool, and
 * under G1 a young collection does not touch the old generation - so the sum mixed a reading from
 * milliseconds ago with one that could be minutes old and described no moment in time. A GC
 * notification carries {@code GcInfo.getMemoryUsageAfterGc()}, which reports every pool as of the
 * end of one collection; summing <em>that</em> gives a number that belongs to a single, known
 * instant.
 *
 * <h2>What the number means, and what it cannot mean</h2>
 *
 * <p>Every after-collection reading is an <strong>upper bound on the live set</strong> at that
 * instant: what a collection leaves behind is the live data plus whatever garbage that collection
 * did not look at. After a young collection that is the whole old generation, garbage included;
 * after a mixed collection it is less; after a full collection it is nothing. The reading can
 * therefore be too high, never too low - and the newest one is the tightest bound available
 * without a marking cycle.
 *
 * <p>Only the <em>latest</em> reading is kept. Two things that look like improvements are not:
 *
 * <ul>
 * <li><em>Preferring the last collection whose action says "end of major GC"</em>: under G1 the
 * mixed collections that actually reclaim old-generation garbage are reported as minor, and on
 * JDKs before 20 the concurrent cycle's Remark/Cleanup pauses - which reclaim almost nothing - were
 * reported as major. Selecting on the collector's vocabulary picks the reading that has <em>not</em>
 * seen the garbage. Only an explicit full GC fits the naive reading.</li>
 * <li><em>Taking the lowest reading in a recent window</em>: a minimum over history is not a bound
 * on anything now. While the live set grows, every later reading is higher and the minimum keeps
 * reporting the old, low value for as long as the window lasts - a heap can go from below the
 * watermark to an OutOfMemoryError inside that window without the minimum ever moving. That is the
 * one failure the watermark exists to prevent.</li>
 * </ul>
 *
 * <p>The residual error of the latest reading is old-generation garbage that no collection has
 * reclaimed yet - after a TTL sweep or bulk delete, the heap can look full for as long as it takes
 * the collector to run a cycle over it. G1 starts one as soon as old-generation occupancy crosses
 * its initiating threshold, checked at every young pause, so under write load this resolves within
 * one marking cycle plus a few young pauses. This class deliberately does not shorten that with
 * {@code System.gc()}: a full collection on a large heap is seconds of stop-the-world on every
 * thread, including the ones a replica set uses to decide whether this node is alive, and it would
 * be issued from the write path at the moment the heap is under the most pressure. If that latency
 * matters for a deployment, {@code -XX:G1PeriodicGCInterval} lets the JVM run a concurrent cycle on
 * a schedule, and a lower {@code -XX:InitiatingHeapOccupancyPercent} bounds how much old-generation
 * garbage can accumulate in the first place.
 *
 * <p>JVM-wide state, so it is static: every driver in a process shares one heap and one listener.
 * Registration is best-effort - {@code com.sun.management.GarbageCollectionNotificationInfo} is not
 * required by the Java SE spec, and where it is missing callers fall back to the raw gauge.
 */
final class HeapAfterGc {
    private static final Logger log = LoggerFactory.getLogger(HeapAfterGc.class);

    /** One reading: heap bytes in use at the end of a collection, and when that was. */
    static final class Reading {
        final long usedBytes;
        final long atMs;

        Reading(long usedBytes, long atMs) {
            this.usedBytes = usedBytes;
            this.atMs = atMs;
        }

        long ageMs(long now) {
            return Math.max(0, now - atMs);
        }
    }

    private static volatile Reading latest;
    private static volatile boolean installed;
    private static volatile boolean unavailable;

    private HeapAfterGc() {
    }

    /**
     * Installs the notification listener once per JVM. Cheap to call on the write path - two
     * volatile reads once installed - and safe to call from anywhere. A JVM without the
     * notification API is remembered so the failure is logged once.
     */
    static void install() {
        if (installed || unavailable) {
            return;
        }

        installSlow();
    }

    private static synchronized void installSlow() {
        if (installed || unavailable) {
            return;
        }

        try {
            for (GarbageCollectorMXBean gc : ManagementFactory.getGarbageCollectorMXBeans()) {
                if (!(gc instanceof NotificationEmitter)) {
                    continue;
                }

                ((NotificationEmitter) gc).addNotificationListener(listener(), null, null);
            }

            installed = true;
        } catch (RuntimeException | LinkageError e) {
            unavailable = true;
            log.warn("GC notifications are not available on this JVM - heap watermarks fall back to "
                     + "raw occupancy, which counts collectable garbage: {}", e.toString());
        }
    }

    private static NotificationListener listener() {
        return (notification, handback) -> {
            if (!com.sun.management.GarbageCollectionNotificationInfo.GARBAGE_COLLECTION_NOTIFICATION
                    .equals(notification.getType())) {
                return;
            }

            try {
                var info = com.sun.management.GarbageCollectionNotificationInfo
                           .from((javax.management.openmbean.CompositeData) notification.getUserData());
                // The GC action is deliberately not consulted - see the class comment.
                latest = new Reading(heapUsedAfter(info.getGcInfo().getMemoryUsageAfterGc()),
                                     System.currentTimeMillis());
            } catch (RuntimeException ignored) {
                // A malformed notification must never take down whatever thread delivered it.
            }
        };
    }

    /** Sums the heap pools of one after-GC snapshot, skipping the non-heap ones by name. */
    private static long heapUsedAfter(Map<String, MemoryUsage> afterGc) {
        long used = 0;

        for (Map.Entry<String, MemoryUsage> e : afterGc.entrySet()) {
            String name = e.getKey();

            // The map carries every pool the collector reports on, including Metaspace, the
            // compressed class space and the CodeHeap segments - those are not heap and must not
            // inflate the reading.
            if (name.contains("Metaspace") || name.contains("Code") || name.contains("Compressed Class")) {
                continue;
            }

            used += e.getValue().getUsed();
        }

        return used;
    }

    /**
     * The most recent reading, or {@code null} when no collection has been observed since the
     * listener was installed. Carries the instant it was taken at; that is reported to operators
     * and does not change the decision - an old reading is still the newest bound there is.
     */
    static Reading latest() {
        return latest;
    }

    /** Test seam: feed a reading without provoking a real collection. */
    static void recordForTest(long usedBytes, long atMs) {
        latest = new Reading(usedBytes, atMs);
    }

    /** Test seam: forget the reading recorded so far. */
    static void resetForTest() {
        latest = null;
    }
}
