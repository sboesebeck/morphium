package de.caluga.morphium.driver.inmem;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.commands.InsertMongoCommand;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Regression test for #368: the heap reading the memory watermarks decide on must belong to a
 * single moment in time, must say how old it is, and must not turn a write away while it is blind
 * to the garbage.
 *
 * <p>The old reading summed {@code MemoryPoolMXBean.getCollectionUsage()} across the heap pools.
 * A pool's value is only refreshed when a collection touches that pool, and under G1 a young
 * collection never touches the old generation - so the sum blended a reading from milliseconds ago
 * with one that could be minutes old. Measured on a 12GB heap holding 7.2GB: 92.1% or 70.2% for the
 * identical dataset depending only on whether a full collection had just run. At the default
 * {@code memory-reject} of 90 that first number refuses writes with ExceededMemoryLimit on a heap
 * that is 30% free.
 *
 * <p>The estimate is now the lowest occupancy seen in a recent window rather than the reading from
 * the last "major" collection. A review caught why that distinction cannot be trusted: G1 reports
 * its concurrent-cycle end as "end of major GC" before anything much is freed, while the mixed
 * collections that actually reclaim old-generation garbage report as "end of minor GC". Preferring
 * "major" picks the reading that has not seen the garbage and throws away the ones that have.
 */
@Tag("inmemory")
public class HeapReadingFreshnessTest {

    private InMemoryDriver drv;

    @AfterEach
    public void tearDown() {
        HeapAfterGc.resetForTest();

        if (drv != null) {
            drv.close();
        }
    }

    private static long percentOfHeap(double percent) {
        return (long) (Runtime.getRuntime().maxMemory() * percent / 100.0);
    }

    @Test
    public void theEstimateIsTheLowestOccupancySeenRecently() throws Exception {
        HeapAfterGc.resetForTest();
        drv = new InMemoryDriver();
        drv.connect();
        long now = System.currentTimeMillis();

        // A young collection leaves the old generation's garbage in place: high.
        HeapAfterGc.recordForTest(percentOfHeap(92), now - 3000);
        assertEquals(92.0, drv.heapUsedAfterGcPercent(), 0.5,
                "with one reading, that is the estimate");

        // A collection that reclaims produces a lower one. Under G1 this is a MIXED collection,
        // which the JVM reports as a minor GC - which is why the action is not consulted.
        HeapAfterGc.recordForTest(percentOfHeap(70), now - 2000);
        assertEquals(70.0, drv.heapUsedAfterGcPercent(), 0.5,
                "the lowest recent reading is the closest thing to the live set");

        // A later young collection must not push the estimate back up: it knows less than the one
        // that reclaimed, and the live set has not grown just because garbage accumulated again.
        HeapAfterGc.recordForTest(percentOfHeap(88), now - 1000);
        assertEquals(70.0, drv.heapUsedAfterGcPercent(), 0.5,
                "a later, higher reading must not override a lower one from the same window");
    }

    @Test
    public void readingsAgeOutOfTheWindow() throws Exception {
        HeapAfterGc.resetForTest();
        drv = new InMemoryDriver();
        drv.connect();
        long now = System.currentTimeMillis();

        // A very low reading from well outside the window describes a heap that no longer exists.
        HeapAfterGc.recordForTest(percentOfHeap(5), now - (HeapAfterGc.WINDOW_MS + 30_000));
        HeapAfterGc.recordForTest(percentOfHeap(80), now - 1000);

        assertEquals(80.0, drv.heapUsedAfterGcPercent(), 0.5,
                "an expired reading must not hold the estimate down - that was the other half of "
                + "the bug: a full GC at startup pinned the number at 5% while the heap filled up");
    }

    @Test
    public void anOldReadingIsReportedAsOldAndNotActedOn() throws Exception {
        HeapAfterGc.resetForTest();
        drv = new InMemoryDriver();
        drv.connect();

        assertEquals(-1, drv.heapUsedAfterGcAgeMs(),
                "with nothing recorded the age must say so rather than pretend to be current");

        HeapAfterGc.recordForTest(percentOfHeap(70), System.currentTimeMillis() - 45_000);
        assertTrue(drv.heapUsedAfterGcAgeMs() >= 45_000,
                "the age must be reported, got: " + drv.heapUsedAfterGcAgeMs());
        assertFalse(drv.heapReadingIsFresh(),
                "a 45s old reading is not a basis for refusing writes");
    }

    /**
     * The tests above drive the logic through {@code recordForTest}, which says nothing about
     * whether the JVM actually delivers what the logic expects. This one goes through the real
     * notification path: provoke a collection and require that a reading arrives, carries a sane
     * age, and is a plausible fraction of the heap.
     */
    @Test
    public void aRealCollectionPopulatesTheReadingThroughJmx() throws Exception {
        HeapAfterGc.resetForTest();
        drv = new InMemoryDriver();
        drv.connect();
        drv.heapUsedAfterGcPercent(); // installs the listener

        System.gc();

        long deadline = System.currentTimeMillis() + 10_000;

        while (HeapAfterGc.mostRecent() == null && System.currentTimeMillis() < deadline) {
            Thread.sleep(50);
            System.gc();
        }

        assertTrue(HeapAfterGc.mostRecent() != null,
                "a System.gc() must produce a reading through the GC notification listener - "
                + "without it every watermark decision falls back to the raw gauge");
        double pct = drv.heapUsedAfterGcPercent();
        assertTrue(pct > 0 && pct < 100,
                "the reading must be a plausible share of the heap, got: " + pct);
        assertTrue(drv.heapUsedAfterGcAgeMs() >= 0 && drv.heapUsedAfterGcAgeMs() < 60_000,
                "and carry a sane age, got: " + drv.heapUsedAfterGcAgeMs());
    }

    /**
     * M2 from the second review, and the reason the first version of this fix was dangerous: the
     * estimate is the lowest reading in the window, so while the live set GROWS every later reading
     * is higher and the minimum keeps reporting the old, low value. A reject decided on that never
     * fires - the process runs into an OOM instead of returning an error the caller can act on.
     * The trigger therefore has to be the most recent reading, which moves with the heap.
     */
    @Test
    public void aGrowingHeapIsNoticedEvenThoughTheMinimumLagsBehind() throws Exception {
        HeapAfterGc.resetForTest();
        drv = new InMemoryDriver();
        drv.connect();
        long now = System.currentTimeMillis();

        // A low reading, then a live set that climbs past the reject watermark. All within the
        // window, so the minimum stays at 70 the whole time.
        HeapAfterGc.recordForTest(percentOfHeap(70), now - 30_000);

        for (int pct : new int[] {80, 88, 93, 96}) {
            HeapAfterGc.recordForTest(percentOfHeap(pct), now);
        }

        assertEquals(70.0, drv.heapUsedAfterGcPercent(), 0.5,
                "the estimate does lag - that is what makes it safe for deciding, and unsafe for "
                + "noticing");
        assertEquals(96.0, drv.heapUsedAfterMostRecentGcPercent(), 0.5,
                "the most recent reading is the one that moves with the heap, and it is what the "
                + "reject stage triggers on");
    }

    @Test
    public void withNoReadingsAtAllTheMostRecentGaugeSaysSo() throws Exception {
        HeapAfterGc.resetForTest();
        drv = new InMemoryDriver();
        drv.connect();
        assertEquals(-1, drv.heapUsedAfterMostRecentGcPercent(), 0.001,
                "no reading must be reported as absent, not as zero occupancy");
    }

    /**
     * The H4 fix - waiting for the notification rather than racing it - had no test that ran it
     * against a real JVM; the one that looked like it stubbed requestFullGc() out entirely. This
     * calls the real thing.
     */
    @Test
    public void requestFullGcWaitsForTheReadingItAskedFor() throws Exception {
        HeapAfterGc.resetForTest();
        InMemoryDriver d = new InMemoryDriver();
        drv = d;
        d.connect();

        boolean got = d.requestFullGc();

        if (!got) {
            // -XX:+DisableExplicitGC and friends make this legitimately impossible. Say so rather
            // than failing, but do not let it pass silently either.
            System.out.println("requestFullGc() reported no new reading - explicit GC may be "
                    + "disabled on this JVM");
            return;
        }

        assertTrue(HeapAfterGc.mostRecent() != null,
                "a true return must mean a reading actually arrived");
        assertTrue(drv.heapReadingIsFresh(),
                "and it must be fresh enough to decide on - otherwise the wait bought nothing");
    }

    /** Counts collection requests instead of paying for a real full GC. */
    private static class GcCountingDriver extends InMemoryDriver {
        final AtomicInteger gcRequests = new AtomicInteger();
        volatile double raw;
        volatile double afterGcBeforeCollection;
        volatile double afterGcAfterCollection;
        volatile boolean collected;

        @Override
        public double heapUsedPercent() {
            return raw;
        }

        @Override
        public double heapUsedAfterGcPercent() {
            return collected ? afterGcAfterCollection : afterGcBeforeCollection;
        }

        @Override
        public boolean heapReadingIsFresh() {
            return collected;
        }

        @Override
        protected boolean requestFullGc() {
            gcRequests.incrementAndGet();
            collected = true;
            return true;
        }
    }

    @Test
    public void aBlindReadingProvokesACollectionInsteadOfRefusingTheWrite() throws Exception {
        GcCountingDriver d = new GcCountingDriver();
        drv = d;
        d.raw = 95;
        d.afterGcBeforeCollection = 92;  // the young reading: over the reject watermark
        d.afterGcAfterCollection = 70;   // what the heap actually holds
        d.connect();

        List<Map<String, Object>> docs = List.of(Doc.of("counter", 1));
        new InsertMongoCommand(d).setDb("freshdb").setColl("freshcoll").setDocuments(docs).execute();

        assertEquals(1, d.gcRequests.get(),
                "the driver must ask for a collection before refusing on a reading that cannot see "
                + "the garbage");
        assertEquals(1, d.find("freshdb", "freshcoll", Doc.of(), null, null, 0, 0).size(),
                "and then let the write through - the heap was 30% free the whole time");
    }

    @Test
    public void aReadingThatSawTheWholeHeapStillRejects() throws Exception {
        GcCountingDriver d = new GcCountingDriver();
        drv = d;
        d.raw = 95;
        d.collected = true;              // a major collection already ran ...
        d.afterGcAfterCollection = 93;   // ... and the heap really is that full
        d.connect();

        List<Map<String, Object>> docs = List.of(Doc.of("counter", 1));
        MorphiumDriverException ex = assertThrows(MorphiumDriverException.class,
                () -> new InsertMongoCommand(d).setDb("fulldb").setColl("fullcoll")
                      .setDocuments(docs).execute());
        assertEquals(146, ex.getMongoCode(),
                "a genuinely full heap must still be refused with ExceededMemoryLimit");
        assertEquals(0, d.gcRequests.get(),
                "and without asking for a collection - the reading had already seen the whole heap");
    }
}
