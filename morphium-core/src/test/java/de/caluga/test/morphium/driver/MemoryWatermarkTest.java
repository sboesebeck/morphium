package de.caluga.test.morphium.driver;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.commands.GenericCommand;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
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
 * Thresholds are set to 1% in these tests so any real heap occupancy triggers them -
 * no need to actually fill the heap.
 */
@Tag("inmemory")
public class MemoryWatermarkTest {

    private InMemoryDriver drv;
    private final String db = "watermark_test";
    private final String coll = "wm_coll";
    // holds >=2% of the max heap so the 1% thresholds are guaranteed to be exceeded,
    // independent of the surefire JVM's heap sizing
    private byte[] heapFiller;

    @BeforeEach
    public void setup() throws Exception {
        heapFiller = new byte[(int) Math.min(Integer.MAX_VALUE - 8, Runtime.getRuntime().maxMemory() / 50)];
        heapFiller[0] = 1;
        drv = new InMemoryDriver();
        drv.connect();
    }

    @AfterEach
    public void tearDown() {
        heapFiller = null;

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
    public void insertAndStoreAreRejectedAboveTheWatermark() throws Exception {
        drv.insert(db, coll, List.of(Doc.of("_id", 1, "v", "before")), null);

        drv.setMemoryWatermarks(1, 1); // any heap occupancy is above 1%

        MorphiumDriverException insertEx = assertThrows(MorphiumDriverException.class,
            () -> drv.insert(db, coll, List.of(Doc.of("_id", 2)), null));
        assertEquals(146, insertEx.getMongoCode(), "ExceededMemoryLimit: " + insertEx.getMessage());
        assertTrue(insertEx.getMessage().contains("watermark"), insertEx.getMessage());

        MorphiumDriverException storeEx = assertThrows(MorphiumDriverException.class,
            () -> drv.store(db, coll, List.of(Doc.of("_id", 3)), null));
        assertEquals(146, storeEx.getMongoCode());
    }

    @Test
    public void updatesAndDeletesStayAllowedAboveTheWatermark() throws Exception {
        drv.insert(db, coll, List.of(Doc.of("_id", 1, "v", "before"), Doc.of("_id", 2, "v", "x")), null);

        drv.setMemoryWatermarks(1, 1);

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
        drv.setMemoryWatermarks(1, 1);

        try (var ignored = drv.bypassMemoryGuard()) {
            drv.insert(db, coll, List.of(Doc.of("_id", "replicated")), null);
        }

        // bypass is scoped: after close, the guard is active again on this thread
        assertThrows(MorphiumDriverException.class,
            () -> drv.insert(db, coll, List.of(Doc.of("_id", "client-write")), null));
    }

    @Test
    public void warnStageOnlyLogsAndKeepsAccepting() throws Exception {
        drv.setMemoryWatermarks(1, 100); // warn on anything, never reject

        assertFalse(drv.isMemoryWarnActive());
        drv.insert(db, coll, List.of(Doc.of("_id", 1)), null);

        assertTrue(drv.isMemoryWarnActive(), "warn state must be latched after crossing");
        // still accepting
        drv.insert(db, coll, List.of(Doc.of("_id", 2)), null);
    }

    @Test
    public void serverStatusReportsTheWatermarkState() throws Exception {
        drv.setMemoryWatermarks(80, 95);

        Map<String, Object> res = run(Doc.of("serverStatus", 1, "$db", "admin"));

        @SuppressWarnings("unchecked")
        Map<String, Object> wm = (Map<String, Object>) res.get("memoryWatermark");
        assertNotNull(wm, "serverStatus must expose the watermark state: " + res);
        assertEquals(80, ((Number) wm.get("warnPercent")).intValue());
        assertEquals(95, ((Number) wm.get("rejectPercent")).intValue());
        assertTrue(((Number) wm.get("heapUsedPercent")).doubleValue() > 0);
    }
}
