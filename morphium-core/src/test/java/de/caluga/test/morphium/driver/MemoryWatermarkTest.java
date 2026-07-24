package de.caluga.test.morphium.driver;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.commands.GenericCommand;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Memory watermark: above the reject threshold, document-creating writes (insert/store)
 * are refused with ExceededMemoryLimit (146) instead of running into an OOM - which in a
 * replica set would kill every node, since replication copies the data volume everywhere.
 * Updates and deletes stay allowed (the drain paths - messaging processed-marks, lock
 * deletes, TTL - must keep working), and replication applies bypass the guard entirely.
 *
 * The decisions are based on the post-GC live set (heapUsedAfterGcPercent), NOT the raw
 * occupancy: with -Xms==-Xmx the JVM only collects when the heap is nearly full, so the
 * raw gauge sits above 90% under allocation-heavy load even when most of it is
 * collectable garbage. Both gauges are overridden here so the tests are deterministic
 * regardless of the surefire JVM's actual heap state.
 */
@Tag("inmemory")
public class MemoryWatermarkTest {

    private InMemoryDriver drv;
    private final String db = "watermark_test";
    private final String coll = "wm_coll";

    /** Driver whose heap gauges report fixed values instead of the real JVM heap. */
    private InMemoryDriver driverWithHeap(double rawPercent, double liveAfterGcPercent) throws Exception {
        InMemoryDriver d = new InMemoryDriver() {
            @Override
            public double heapUsedPercent() {
                return rawPercent;
            }

            @Override
            public double heapUsedAfterGcPercent() {
                return liveAfterGcPercent;
            }
        };
        d.connect();
        drv = d;
        return d;
    }

    @AfterEach
    public void tearDown() {
        if (drv != null) {
            drv.close();
        }
    }

    private Map<String, Object> run(Map<String, Object> cmdMap) throws Exception {
        GenericCommand cmd = new GenericCommand(drv);
        cmd.fromMap(cmdMap);
        int id = drv.runCommand(cmd);
        Map<String, Object> res = drv.readSingleAnswer(id);
        assertNotNull(res);
        return res;
    }

    @Test
    public void collectableGarbageDoesNotTriggerWarnOrReject() throws Exception {
        // raw occupancy way above both thresholds, but the post-GC live set is not:
        // the heap is full of garbage the next GC would clear - writes must go through
        InMemoryDriver d = driverWithHeap(95, 40);
        d.setMemoryWatermarks(75, 90);

        d.insert(db, coll, List.of(Doc.of("_id", 1)), null);

        assertFalse(d.isMemoryWarnActive(), "warn must not latch on collectable garbage");
    }

    @Test
    public void insertAndStoreAreRejectedAboveTheWatermark() throws Exception {
        InMemoryDriver d = driverWithHeap(95, 93); // live set really above the watermark
        d.setMemoryWatermarks(100, 100); // off while seeding
        d.insert(db, coll, List.of(Doc.of("_id", 1, "v", "before")), null);

        d.setMemoryWatermarks(75, 90);

        MorphiumDriverException insertEx = assertThrows(MorphiumDriverException.class,
            () -> d.insert(db, coll, List.of(Doc.of("_id", 2)), null));
        assertEquals(146, insertEx.getMongoCode(), "ExceededMemoryLimit: " + insertEx.getMessage());
        assertTrue(insertEx.getMessage().contains("watermark"), insertEx.getMessage());

        MorphiumDriverException storeEx = assertThrows(MorphiumDriverException.class,
            () -> d.store(db, coll, List.of(Doc.of("_id", 3)), null));
        assertEquals(146, storeEx.getMongoCode());
    }

    @Test
    public void updatesAndDeletesStayAllowedAboveTheWatermark() throws Exception {
        InMemoryDriver d = driverWithHeap(95, 93);
        d.setMemoryWatermarks(100, 100); // off while seeding
        d.insert(db, coll, List.of(Doc.of("_id", 1, "v", "before"), Doc.of("_id", 2, "v", "x")), null);

        d.setMemoryWatermarks(75, 90);

        // the drain paths must keep working, or the system could never get back under
        // the watermark: updates (messaging processed-marks) ...
        Map<String, Object> upd = run(Doc.of("update", coll, "updates",
            List.of(Doc.of("q", Doc.of("_id", 1), "u", Doc.of("$set", Doc.of("v", "after")))), "$db", db));
        assertEquals(1.0, upd.get("ok"), "update must not be rejected: " + upd);

        // ... and deletes (lock releases, manual cleanup)
        Map<String, Object> del = run(Doc.of("delete", coll, "deletes",
            List.of(Doc.of("q", Doc.of("_id", 2), "limit", 1)), "$db", db));
        assertEquals(1.0, del.get("ok"), "delete must not be rejected: " + del);
    }

    @Test
    public void bypassScopeAllowsReplicationApplies() throws Exception {
        InMemoryDriver d = driverWithHeap(95, 93);
        d.setMemoryWatermarks(75, 90);

        try (var ignored = d.bypassMemoryGuard()) {
            d.insert(db, coll, List.of(Doc.of("_id", "replicated")), null);
        }

        // bypass is scoped: after close, the guard is active again on this thread
        assertThrows(MorphiumDriverException.class,
            () -> d.insert(db, coll, List.of(Doc.of("_id", "client-write")), null));
    }

    @Test
    public void warnStageOnlyLogsAndKeepsAccepting() throws Exception {
        InMemoryDriver d = driverWithHeap(80, 78);
        d.setMemoryWatermarks(75, 100); // warn above 75, never reject

        assertFalse(d.isMemoryWarnActive());
        d.insert(db, coll, List.of(Doc.of("_id", 1)), null);

        assertTrue(d.isMemoryWarnActive(), "warn state must be latched after crossing");
        // still accepting
        d.insert(db, coll, List.of(Doc.of("_id", 2)), null);
    }

    @Test
    public void realGaugesAreConsistent() throws Exception {
        // on the real JVM the post-GC live set can never exceed the raw occupancy
        // (between collections, used memory only grows)
        drv = new InMemoryDriver();
        drv.connect();

        double raw = drv.heapUsedPercent();
        double live = drv.heapUsedAfterGcPercent();
        assertTrue(raw > 0 && raw <= 100, "raw gauge out of range: " + raw);
        assertTrue(live >= 0 && live <= 100, "live gauge out of range: " + live);
        assertTrue(live <= raw + 1.0, "live set (" + live + "%) must not exceed raw occupancy (" + raw + "%)");
    }

    @Test
    public void serverStatusReportsTheWatermarkState() throws Exception {
        drv = new InMemoryDriver();
        drv.connect();
        drv.setMemoryWatermarks(80, 95);

        Map<String, Object> res = run(Doc.of("serverStatus", 1, "$db", "admin"));

        @SuppressWarnings("unchecked")
        Map<String, Object> wm = (Map<String, Object>) res.get("memoryWatermark");
        assertNotNull(wm, "serverStatus must expose the watermark state: " + res);
        assertEquals(80, ((Number) wm.get("warnPercent")).intValue());
        assertEquals(95, ((Number) wm.get("rejectPercent")).intValue());
        assertTrue(((Number) wm.get("heapUsedPercent")).doubleValue() > 0);
        assertTrue(((Number) wm.get("heapUsedAfterGcPercent")).doubleValue() >= 0,
            "serverStatus must expose the live-set gauge: " + wm);
    }
}
