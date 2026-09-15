package de.caluga.morphium.driver.inmem;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.NotificationEmitter;
import javax.management.NotificationListener;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Map;

/**
 * Heap occupancy as of the end of a garbage collection, taken as coherent snapshots (#368).
 *
 * <p>The previous reading summed {@code MemoryPoolMXBean.getCollectionUsage()} over the heap pools.
 * That value is refreshed for a pool only when a collection touches <em>that</em> pool, and under G1
 * a young collection never touches the old generation - so the sum mixed a reading from milliseconds
 * ago with one that could be minutes old. It was not stale so much as incoherent: it described no
 * moment in time. Measured on a 12GB heap holding 7.2GB of live data, it reported 92.1% or 70.2%
 * for the identical dataset depending only on whether a full collection had just run.
 *
 * <p>A GC notification carries {@code GcInfo.getMemoryUsageAfterGc()}, which reports every pool as
 * of the end of one collection. Summing <em>that</em> gives a number belonging to a single instant,
 * and the instant is known.
 *
 * <h2>Why the minimum, and not "the last major collection"</h2>
 *
 * <p>The obvious approach - keep the reading from the last collection that reclaimed the old
 * generation - does not survive contact with G1, and an earlier version of this class got it
 * exactly backwards. G1 reports its concurrent-cycle end (Remark/Cleanup) as {@code "end of major
 * GC"}, and at that point almost nothing has been freed: marking only reclaims regions that are
 * entirely empty. The collections that actually reclaim old-generation garbage are G1's <em>mixed</em>
 * collections, and those are reported as {@code "end of minor GC"}. Preferring "major" therefore
 * systematically picks the reading that has <em>not</em> seen the garbage, and discards the lower
 * ones that have. Only an explicit full GC fits the naive reading - which is precisely why the
 * measurement that motivated this looked right.
 *
 * <p>So instead of trusting the collector's own vocabulary, this takes the <strong>lowest occupancy
 * observed in a recent window</strong>. The live set is a floor that every collection approaches
 * from above: a collection that frees a lot produces a low reading, one that frees little produces a
 * high one, and the lowest recent reading is the closest thing to the live set that can be had
 * without forcing a full GC. It is collector-agnostic, and it errs toward believing the heap has
 * room - the right direction, given that the failure being fixed was refusing writes on a heap that
 * was 30% free.
 *
 * <p>The window matters as much as the minimum: a reading from five minutes ago describes a heap
 * that no longer exists, so readings age out and every answer carries the age of the reading behind
 * it.
 *
 * <p>JVM-wide state, so it is static: every driver in a process shares one heap and one listener.
 * Registration is best-effort - {@code com.sun.management.GarbageCollectionNotificationInfo} is not
 * required by the Java SE spec, and where it is missing callers fall back to the raw gauge.
 */
final class HeapAfterGc {
    private static final Logger log = LoggerFactory.getLogger(HeapAfterGc.class);

    /** How far back a reading may be and still count toward the estimate. */
    static final long WINDOW_MS = 60_000;

    /** Plenty for a minute of collections; an allocation-heavy JVM does a few per second. */
    private static final int MAX_READINGS = 256;

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

    private static final Deque<Reading> READINGS = new ArrayDeque<>();
    private static volatile boolean installed;
    private static volatile boolean unavailable;

    private HeapAfterGc() {
    }

    /**
     * Installs the notification listener once per JVM. Safe to call from anywhere and as often as
     * you like; a JVM without the notification API is remembered so the failure is logged once.
     */
    static synchronized void install() {
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
                // The GC action is deliberately not consulted - see the class comment on why
                // "major" does not mean "reclaimed the old generation" under G1.
                record(heapUsedAfter(info.getGcInfo().getMemoryUsageAfterGc()), System.currentTimeMillis());
            } catch (RuntimeException ignored) {
                // A malformed notification must never take down whatever thread delivered it.
            }
        };
    }

    private static synchronized void record(long usedBytes, long atMs) {
        READINGS.addLast(new Reading(usedBytes, atMs));

        while (READINGS.size() > MAX_READINGS) {
            READINGS.removeFirst();
        }
    }

    /** Sums the heap pools of one after-GC snapshot, skipping the non-heap ones by name. */
    private static long heapUsedAfter(Map<String, MemoryUsage> afterGc) {
        long used = 0;

        for (Map.Entry<String, MemoryUsage> e : afterGc.entrySet()) {
            String name = e.getKey();

            // The map carries every pool the collector reports on, including Metaspace and the
            // code cache on some JVMs - those are not heap and must not inflate the reading.
            if (name.contains("Metaspace") || name.contains("Code") || name.contains("Compressed Class")) {
                continue;
            }

            used += e.getValue().getUsed();
        }

        return used;
    }

    /**
     * The lowest occupancy observed within {@link #WINDOW_MS}, or {@code null} when no collection
     * has been seen in that time. The returned reading carries the instant it was taken at, so a
     * caller can decide whether it is fresh enough to act on.
     */
    static synchronized Reading liveEstimate(long now) {
        Reading best = null;

        for (Reading r : READINGS) {
            if (now - r.atMs > WINDOW_MS) {
                continue;
            }

            if (best == null || r.usedBytes < best.usedBytes) {
                best = r;
            }
        }

        return best;
    }

    /** The most recent reading of any collection, regardless of age. Used only as a last resort. */
    static synchronized Reading mostRecent() {
        return READINGS.peekLast();
    }

    /** Test seam: feed readings without provoking real collections. */
    static void recordForTest(long usedBytes, long atMs) {
        record(usedBytes, atMs);
    }

    /** Test seam: forget everything recorded so far. */
    static synchronized void resetForTest() {
        READINGS.clear();
    }
}
