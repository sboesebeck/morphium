package de.caluga.poppydb.netty;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import de.caluga.morphium.driver.MorphiumDriver.DriverStatsKey;
import de.caluga.morphium.driver.commands.WatchCommand;
import de.caluga.morphium.driver.inmem.InMemoryDriver;

/**
 * Same bug class as the ReplicationManager apply-path leak (see
 * ReplicationApplyResultLeakTest): {@code InMemoryDriver.runCommand()} stores its reply in a
 * by-id map that only the matching read clears. {@code createWatchCursor} used to call
 * {@code runCommand(wcmd)} and discard the returned id - the manager builds its own client
 * reply, so the driver's stub answer was never fetched and leaked one entry per created
 * change stream. Low volume compared to the per-event apply leak, but reconnect-looping
 * messaging clients create cursors all day.
 */
@Tag("poppydb")
public class WatchCursorResultLeakTest {

    private long pendingReplies(InMemoryDriver drv) {
        Double d = drv.getDriverStats().get(DriverStatsKey.REPLY_IN_MEM);
        return d == null ? 0 : d.longValue();
    }

    @Test
    public void creatingWatchCursorsLeavesNoUnfetchedCommandResults() throws Exception {
        InMemoryDriver drv = new InMemoryDriver();
        drv.connect();
        WatchCursorManager cursors = new WatchCursorManager();

        try {
            long before = pendingReplies(drv);

            for (int i = 0; i < 25; i++) {
                WatchCommand wcmd = new WatchCommand(drv).setDb("leak_db").setColl("coll")
                        .setMaxTimeMS(30000);
                cursors.createWatchCursor(drv, wcmd);
            }

            long after = pendingReplies(drv);
            assertEquals(before, after,
                "createWatchCursor must fetch the driver's stub reply - leaked " + (after - before)
                + " replies for 25 cursors");
        } finally {
            cursors.shutdown();
            drv.close();
        }
    }
}
