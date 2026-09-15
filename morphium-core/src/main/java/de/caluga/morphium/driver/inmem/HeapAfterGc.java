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
 * Heap occupancy as of the end of a garbage collection, taken as one coherent snapshot (#368).
 *
 * <p>The previous reading summed {@code MemoryPoolMXBean.getCollectionUsage()} over the heap pools.
 * That value is refreshed for a pool only when a collection touches <em>that</em> pool, and under G1
 * a young collection never touches the old generation - so the sum mixed a reading from milliseconds
 * ago with one that could be minutes old. It was not stale so much as incoherent: it described no
 * moment in time. Measured on a 12GB heap holding 7.2GB of live data, it reported 92.1% or 70.2%
 * for the identical dataset depending only on whether a full collection had just run, and it sat at
 * 71.8% right after a TTL sweep had freed nine tenths of the heap.
 *
 * <p>A GC notification carries {@code GcInfo.getMemoryUsageAfterGc()}, which reports every pool as
 * of the end of that one collection. Summing <em>that</em> gives a number that belongs to a single
 * instant, and the instant is known - so a consumer can tell a fresh reading from an old one instead
 * of being handed a blend.
 *
 * <p>Two readings are kept: the most recent collection of any kind, and the most recent one that
 * reclaimed the old generation. Only the latter approximates the live set; after a young-only
 * collection the number still counts every piece of old-generation garbage. Whoever acts on these
 * must decide how much staleness they can live with - see {@code InMemoryDriver.checkMemoryWatermark},
 * which asks for a collection rather than refusing writes on a reading that cannot see the garbage.
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

    private static volatile Reading lastAny;
    private static volatile Reading lastMajor;
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
                long used = heapUsedAfter(info.getGcInfo().getMemoryUsageAfterGc());
                Reading r = new Reading(used, System.currentTimeMillis());
                lastAny = r;

                // "end of major GC" is the documented action for a collection that covers the old
                // generation. G1 reports its concurrent-cycle end and every full GC this way; a G1
                // mixed collection does not, which is exactly why the major reading is kept apart
                // rather than assumed to be current.
                if (info.getGcAction() != null && info.getGcAction().contains("major")) {
                    lastMajor = r;
                }
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

            // The map carries every pool the collector reports on, including Metaspace and the
            // code cache on some JVMs - those are not heap and must not inflate the reading.
            if (name.contains("Metaspace") || name.contains("Code") || name.contains("Compressed Class")) {
                continue;
            }

            used += e.getValue().getUsed();
        }

        return used;
    }

    /** The most recent reading of any collection, or {@code null} when none has been seen. */
    static Reading lastAny() {
        return lastAny;
    }

    /**
     * The most recent reading from a collection that reclaimed the old generation - the only one
     * that approximates the live set. {@code null} when no such collection has happened yet.
     */
    static Reading lastMajor() {
        return lastMajor;
    }

    /** Test seam: feed readings without provoking real collections. */
    static void recordForTest(long usedBytes, long atMs, boolean major) {
        Reading r = new Reading(usedBytes, atMs);
        lastAny = r;

        if (major) {
            lastMajor = r;
        }
    }

    /** Test seam: forget everything recorded so far. */
    static void resetForTest() {
        lastAny = null;
        lastMajor = null;
    }
}
