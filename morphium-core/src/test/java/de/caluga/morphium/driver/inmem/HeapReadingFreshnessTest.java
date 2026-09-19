package de.caluga.morphium.driver.inmem;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.commands.GenericCommand;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Regression tests for #368: which heap reading the memory watermark refuses a write on.
 *
 * <p>The reading is the heap occupancy at the end of the most recent collection, taken from that
 * collection's own {@code GcInfo} so it belongs to one instant. It is an upper bound on the live
 * set - a young collection leaves the old generation's garbage in place - and it is the newest
 * bound there is. The watermark refuses a document-creating write only when this reading AND the
 * raw gauge are over the line.
 *
 * <p>Every test here drives {@code checkMemoryWatermark()} through a real insert and asserts what
 * happens to the write. The raw gauge is overridden where a test needs it pinned; the after-GC
 * reading is the real {@link HeapAfterGc} state, fed through its test seam or by a real collection.
 */
@Tag("inmemory")
public class HeapReadingFreshnessTest {

    private static final String DB = "freshdb";

    private InMemoryDriver drv;

    @AfterEach
    public void tearDown() {
        HeapAfterGc.resetForTest();
        HeapAfterGc.ignoreRealNotificationsForTest(false);

        if (drv != null) {
            drv.close();
        }
    }

    private static long percentOfHeap(double percent) {
        return (long) (Runtime.getRuntime().maxMemory() * percent / 100.0);
    }

    /**
     * A driver whose raw gauge is pinned; the after-GC reading is whatever HeapAfterGc holds.
     *
     * <p>Real GC notifications are suspended for the duration. The listener stays registered for
     * the life of the JVM and the reading is a single field, so without this a young collection
     * landing between a test's seed and its assertion overwrites the value the test is about -
     * silently turning a genuine failure into a pass. Every seeded test in this class depends on
     * that suspension; the one test that wants the real listener turns it back on explicitly.
     */
    private InMemoryDriver driverWithRawGauge(double rawPercent) throws MorphiumDriverException {
        HeapAfterGc.ignoreRealNotificationsForTest(true);
        HeapAfterGc.resetForTest();
        InMemoryDriver d = new InMemoryDriver() {
            @Override
            public double heapUsedPercent() {
                return rawPercent;
            }
        };
        d.connect();
        d.setMemoryWatermarks(75, 90);
        drv = d;
        return d;
    }

    private static void insertOne(InMemoryDriver d, String coll) throws MorphiumDriverException {
        d.insert(DB, coll, List.of(Doc.of("v", 1)), null);
    }

    private static MorphiumDriverException assertRefused(InMemoryDriver d, String coll, String why) {
        MorphiumDriverException ex = assertThrows(MorphiumDriverException.class, () -> insertOne(d, coll), why);
        assertEquals(146, ex.getMongoCode(), "ExceededMemoryLimit expected: " + ex.getMessage());
        return ex;
    }

    private static int count(InMemoryDriver d, String coll) throws MorphiumDriverException {
        return d.find(DB, coll, Doc.of(), null, null, 0, 0).size();
    }

    /**
     * The attempt-2 failure: a minimum over a window reports an old low value while the live set
     * grows past the line, and the process runs into an OOM instead of returning an error. The
     * decision must follow the newest reading.
     */
    @Test
    public void aGrowingLiveSetIsRefusedOnTheLatestReadingNotOnAnEarlierLowOne() throws Exception {
        InMemoryDriver d = driverWithRawGauge(95);
        long now = System.currentTimeMillis();
        HeapAfterGc.recordForTest(percentOfHeap(70), now - 30_000);
        HeapAfterGc.recordForTest(percentOfHeap(96), now);

        assertRefused(d, "growing", "the newest reading is over the line; an earlier low one must not save the write");
        assertEquals(0, count(d, "growing"), "the refused document must not have been stored");
    }

    /**
     * The #368 failure in the other direction: after a collection that reclaimed the old
     * generation's garbage, the reading drops and the write must go through - even though the raw
     * gauge still counts everything allocated since.
     */
    @Test
    public void aCollectionThatReclaimedLowersTheReadingAndTheWriteGoesThrough() throws Exception {
        InMemoryDriver d = driverWithRawGauge(95);
        long now = System.currentTimeMillis();

        HeapAfterGc.recordForTest(percentOfHeap(92), now - 2000);
        assertRefused(d, "reclaimed", "a young reading over the line refuses (it may be counting garbage, "
                      + "and that is the recoverable direction)");

        HeapAfterGc.recordForTest(percentOfHeap(70), now - 1000);
        insertOne(d, "reclaimed");
        assertEquals(1, count(d, "reclaimed"), "the reading that saw the garbage go must let the write through");
    }

    /**
     * The hot path: below the raw gauge's threshold the reading is not consulted at all, because
     * the live set can never exceed raw occupancy. A stale high reading must not refuse a write on
     * a heap that is demonstrably not full.
     */
    @Test
    public void belowTheRawGaugeThresholdTheReadingIsNotConsulted() throws Exception {
        InMemoryDriver d = driverWithRawGauge(50);
        HeapAfterGc.recordForTest(percentOfHeap(99), System.currentTimeMillis());

        insertOne(d, "cheap");
        assertEquals(1, count(d, "cheap"), "raw occupancy 50% bounds the live set at 50%; no reading can override that");
    }

    /**
     * Age is reported, and it does not soften the decision. An old reading is still the newest
     * bound there is, and the raw gauge covers what happened since. (The previous design asked the
     * JVM for a full collection here - see HeapAfterGc for why it no longer does.)
     */
    @Test
    public void anOldReadingOverTheLineStillRefuses() throws Exception {
        InMemoryDriver d = driverWithRawGauge(95);
        HeapAfterGc.recordForTest(percentOfHeap(93), System.currentTimeMillis() - 45_000);

        assertTrue(d.heapUsedAfterGcAgeMs() >= 45_000, "age must be reported, got " + d.heapUsedAfterGcAgeMs());
        assertRefused(d, "old", "an old reading over the line, with raw occupancy over the line too, refuses");
        assertEquals(0, count(d, "old"));
    }

    /**
     * Before the first collection (or on a JVM without GC notifications) the raw gauge is all there
     * is, and it decides. The first reading then takes over.
     */
    @Test
    public void withNoReadingTheRawGaugeDecidesUntilTheFirstCollection() throws Exception {
        InMemoryDriver d = driverWithRawGauge(95);

        assertEquals(-1, d.heapUsedAfterGcAgeMs(), "no reading must be reported as absent, not as fresh");
        assertRefused(d, "noreading", "with nothing better to go on, raw occupancy over the line refuses");

        HeapAfterGc.recordForTest(percentOfHeap(40), System.currentTimeMillis());
        insertOne(d, "noreading");
        assertEquals(1, count(d, "noreading"), "the first reading shows the raw gauge was garbage - accept");
    }

    /**
     * The tests above feed readings through the test seam, which says nothing about whether the JVM
     * delivers what the logic expects. This one provokes a real collection and requires that a
     * reading arrives through the notification listener, is a plausible fraction of the heap, and
     * is not above the raw gauge - the invariant the fast path depends on.
     */
    @Test
    public void aRealCollectionPopulatesTheReadingThroughJmx() throws Exception {
        // This is the one test that wants the real listener, so it must undo the suspension the
        // seeded tests rely on - they run in the same JVM and the flag is static.
        HeapAfterGc.ignoreRealNotificationsForTest(false);
        HeapAfterGc.resetForTest();
        drv = new InMemoryDriver();
        // connect() installs the listener, so the collections of a startup restore are seen too
        // (#368, fourth review M2) - not only those after the first guarded write.
        drv.connect();
        assertTrue(awaitReading(System.currentTimeMillis() - 1, 10_000),
                   "a System.gc() must produce a reading through the GC notification listener - "
                   + "without it every watermark decision falls back to the raw gauge");

        double pct = drv.heapUsedAfterGcPercent();
        assertTrue(pct > 0 && pct < 100, "the reading must be a plausible share of the heap, got: " + pct);
        assertTrue(pct <= drv.heapUsedPercent() + 1.0,
                   "after-GC occupancy " + pct + "% cannot exceed raw occupancy " + drv.heapUsedPercent() + "%");
        assertTrue(drv.heapUsedAfterGcAgeMs() >= 0 && drv.heapUsedAfterGcAgeMs() < 60_000,
                   "and carry a sane age, got: " + drv.heapUsedAfterGcAgeMs());
    }

    /**
     * End to end on the real heap, no gauge overridden: fill the driver until a full collection
     * shows the data really is there, set the watermark just below that, and require that inserts
     * are refused while updates and deletes - the drain paths - still work; then drop the data and
     * require that inserts are accepted again once the collector has seen it go.
     */
    @Test
    public void theWatermarkFollowsTheRealHeapThroughFillAndDrain() throws Exception {
        HeapAfterGc.resetForTest();
        drv = new InMemoryDriver();
        drv.connect();
        drv.setMemoryWatermarks(100, 100); // off while filling

        double baseline = readingAfterFullGc();
        double target = Math.min(baseline + 20, 60);
        String payload = "x".repeat(2048);
        double reading = baseline;
        int id = 0;

        for (int round = 0; round < 400 && reading < target; round++) {
            List<Map<String, Object>> batch = new ArrayList<>();

            for (int i = 0; i < 2000; i++) {
                batch.add(Doc.of("_id", id++, "payload", payload));
            }

            drv.insert(DB, "fill", batch, null);

            if (round % 10 == 9) {
                reading = readingAfterFullGc();
            }
        }

        assertTrue(reading >= target, "could not fill the heap to " + target + "%, reached " + reading
                   + "% with " + id + " documents (baseline " + baseline + "%)");

        int reject = (int) Math.floor(reading) - 1;
        drv.setMemoryWatermarks(Math.max(1, reject - 5), reject);

        MorphiumDriverException ex = assertRefused(drv, "fill", "a full collection showed " + reading
                                                   + "% live with the watermark at " + reject + "%");
        assertTrue(ex.getMessage().contains("watermark"), ex.getMessage());
        assertEquals(id, count(drv, "fill"), "the refused document must not have been stored");

        // the drain paths must stay open: update ...
        Map<String, Object> upd = run(Doc.of("update", "fill", "updates",
            List.of(Doc.of("q", Doc.of("_id", 0), "u", Doc.of("$set", Doc.of("payload", "y")))), "$db", DB));
        assertEquals(1.0, upd.get("ok"), "updates must not be refused above the watermark: " + upd);
        assertEquals("y", drv.find(DB, "fill", Doc.of("_id", 0), null, null, 0, 0).get(0).get("payload"),
                     "the update must have taken effect");
        // ... and delete
        Map<String, Object> del = run(Doc.of("delete", "fill", "deletes",
            List.of(Doc.of("q", Doc.of("_id", 0), "limit", 1)), "$db", DB));
        assertEquals(1.0, del.get("ok"), "deletes must not be refused above the watermark: " + del);
        assertEquals(id - 1, count(drv, "fill"), "the delete must have taken effect");

        drv.drop(DB, "fill", null);
        double afterDrop = readingAfterFullGc();
        assertTrue(afterDrop < reject, "after dropping the data a full collection must read below the "
                   + "watermark, got " + afterDrop + "% (watermark " + reject + "%)");

        insertOne(drv, "fill");
        assertEquals(1, count(drv, "fill"), "with the data gone and collected, inserts are accepted again");
    }

    private Map<String, Object> run(Map<String, Object> cmdMap) throws Exception {
        GenericCommand cmd = new GenericCommand(drv);
        cmd.fromMap(cmdMap);
        Map<String, Object> res = drv.readSingleAnswer(drv.runCommand(cmd));
        assertNotNull(res);
        return res;
    }

    /** Asks for a full collection and returns the reading it produced, in percent of max heap. */
    private double readingAfterFullGc() throws Exception {
        long before = System.currentTimeMillis();
        Thread.sleep(2); // so the reading's timestamp is strictly newer than `before`
        System.gc();
        assertTrue(awaitReading(before, 10_000), "no GC notification arrived within 10s of System.gc() - "
                   + "is explicit GC disabled on this JVM?");
        return drv.heapUsedAfterGcPercent();
    }

    /**
     * Waits for a reading newer than {@code afterMs}, retrying System.gc() while none arrives.
     * Goes through the driver's public age, not the HeapAfterGc seam, so this test file compiles
     * against the previous design too and can be run against it as a negative control.
     */
    private boolean awaitReading(long afterMs, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;

        while (System.currentTimeMillis() < deadline) {
            long age = drv.heapUsedAfterGcAgeMs();

            if (age >= 0 && System.currentTimeMillis() - age > afterMs) {
                return true;
            }

            Thread.sleep(50);
            System.gc();
        }

        return false;
    }
}
