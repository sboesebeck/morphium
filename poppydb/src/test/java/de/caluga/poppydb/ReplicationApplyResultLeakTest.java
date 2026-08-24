package de.caluga.poppydb;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumDriver.DriverStatsKey;
import de.caluga.morphium.driver.inmem.InMemoryDriver;

/**
 * Regression test for the secondary-side memory leak in the replication apply path.
 *
 * Every {@code InMemoryDriver.runCommand()} stores its result in an internal by-id map;
 * the entry is removed only when the caller fetches it ({@code readSingleAnswer} et al.).
 * The ReplicationManager apply path used to call {@code runCommand()} and throw the
 * returned message id away for every non-bulk-insert operation (update/replace, delete,
 * drop, dropDatabase, idempotent replay-insert), so on a PoppyDB secondary every single
 * replicated event of those types left one reply behind forever -- measured in production
 * as roughly 800 bytes per replicated update, i.e. unbounded heap growth until the node
 * hits the memory watermark. The primary never leaks because the netty handler fetches
 * every answer.
 *
 * The test drives {@link ReplicationManager#applyEventsInOrder(List)} -- the same
 * package-private seam ReplicationOrderingTest uses -- with non-insert events and asserts
 * that the driver's count of unfetched replies (DriverStatsKey.REPLY_IN_MEM, which
 * includes the by-id result map) does not grow at all.
 */
@Tag("server")
public class ReplicationApplyResultLeakTest {

    private static long seq = 1;

    private Map<String, Object> event(String opType, String db, String coll, Object id, Object value) {
        Map<String, Object> event = new LinkedHashMap<>();
        event.put("_id", Doc.of("_data", String.format(Locale.ROOT, "%016x", seq++)));
        event.put("operationType", opType);
        event.put("ns", Doc.of("db", db, "coll", coll));
        event.put("documentKey", Doc.of("_id", id));

        if (value != null) {
            Map<String, Object> doc = new LinkedHashMap<>();
            doc.put("_id", id);
            doc.put("value", value);
            event.put("fullDocument", doc);
        }

        return event;
    }

    private long pendingReplies(InMemoryDriver drv) {
        Double d = drv.getDriverStats().get(DriverStatsKey.REPLY_IN_MEM);
        return d == null ? 0 : d.longValue();
    }

    @Test
    public void applyingReplicatedEventsLeavesNoUnfetchedCommandResults() throws Exception {
        InMemoryDriver drv = new InMemoryDriver();
        drv.connect();

        try {
            ReplicationManager rm = new ReplicationManager(drv, "127.0.0.1", 1);
            final int n = 200;

            long before = pendingReplies(drv);

            // update/replace is the dominant op type on a live secondary; add deletes,
            // a drop and a dropDatabase so every non-insert apply site is covered.
            List<Map<String, Object>> batch = new ArrayList<>();

            for (int i = 0; i < n; i++) {
                batch.add(event("update", "testdb", "coll", i, "v" + i));
            }

            for (int i = 0; i < n; i++) {
                batch.add(event("delete", "testdb", "coll", i, null));
            }

            batch.add(event("drop", "testdb", "coll", 0, null));
            batch.add(event("dropDatabase", "testdb", null, 0, null));
            rm.applyEventsInOrder(batch);

            long after = pendingReplies(drv);
            assertEquals(before, after,
                "apply path must fetch every command result it produces - leaked "
                + (after - before) + " replies for " + batch.size() + " events");
        } finally {
            drv.close();
        }
    }
}
