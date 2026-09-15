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
 * identical dataset depending only on whether a full collection had just run, and 71.8% right after
 * a TTL sweep had freed nine tenths of the heap. At the default {@code memory-reject} of 90 that
 * first number refuses writes with ExceededMemoryLimit on a heap that is 30% free.
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
    public void theReadingComesFromACollectionThatSawTheOldGeneration() throws Exception {
        HeapAfterGc.resetForTest();
        drv = new InMemoryDriver();
        drv.connect();

        // A young collection: high occupancy, because the old generation's garbage is still there.
        HeapAfterGc.recordForTest(percentOfHeap(92), System.currentTimeMillis(), false);
        assertEquals(92.0, drv.heapUsedAfterGcPercent(), 0.5,
                "with no major collection on record the young reading is all there is");
        assertFalse(drv.heapReadingSeesOldGeneration(),
                "a young-only reading must not claim to have seen the whole heap");

        // A major collection follows and finds most of it collectable.
        HeapAfterGc.recordForTest(percentOfHeap(70), System.currentTimeMillis(), true);
        assertEquals(70.0, drv.heapUsedAfterGcPercent(), 0.5,
                "the major collection's reading must win - it is the only one that saw the garbage");
        assertTrue(drv.heapReadingSeesOldGeneration());

        // Another young collection afterwards must not push the number back up: it knows less.
        HeapAfterGc.recordForTest(percentOfHeap(88), System.currentTimeMillis(), false);
        assertEquals(70.0, drv.heapUsedAfterGcPercent(), 0.5,
                "a later young reading must not override what a major collection established");
    }

    @Test
    public void anOldReadingIsReportedAsOld() throws Exception {
        HeapAfterGc.resetForTest();
        drv = new InMemoryDriver();
        drv.connect();

        assertEquals(-1, drv.heapUsedAfterGcAgeMs(),
                "with nothing recorded the age must say so rather than pretend to be current");

        HeapAfterGc.recordForTest(percentOfHeap(70), System.currentTimeMillis() - 45_000, true);
        assertTrue(drv.heapUsedAfterGcAgeMs() >= 45_000,
                "the age must be reported, got: " + drv.heapUsedAfterGcAgeMs());
        assertFalse(drv.heapReadingSeesOldGeneration(),
                "a 45s old major reading is not a basis for refusing writes");
    }

    /**
     * The tests above drive the logic through {@code recordForTest}, which says nothing about
     * whether the JVM actually delivers what the logic expects. This one goes through the real
     * notification path: provoke a collection and require that a reading arrives, that it comes
     * from something which saw the old generation, and that it is a plausible fraction of the heap.
     */
    @Test
    public void aRealCollectionPopulatesTheReadingThroughJmx() throws Exception {
        HeapAfterGc.resetForTest();
        drv = new InMemoryDriver();
        drv.connect();
        drv.heapUsedAfterGcPercent(); // installs the listener

        System.gc();

        long deadline = System.currentTimeMillis() + 10_000;

        while (HeapAfterGc.lastMajor() == null && System.currentTimeMillis() < deadline) {
            Thread.sleep(50);
            System.gc();
        }

        assertTrue(HeapAfterGc.lastMajor() != null,
                "a System.gc() must produce a major-collection reading through the GC notification "
                + "listener - without it every watermark decision falls back to the raw gauge");
        double pct = drv.heapUsedAfterGcPercent();
        assertTrue(pct > 0 && pct < 100,
                "the reading must be a plausible share of the heap, got: " + pct);
        assertTrue(drv.heapUsedAfterGcAgeMs() >= 0 && drv.heapUsedAfterGcAgeMs() < 60_000,
                "and carry a sane age, got: " + drv.heapUsedAfterGcAgeMs());
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
        public boolean heapReadingSeesOldGeneration() {
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
