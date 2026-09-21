package de.caluga.poppydb;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.commands.WatchCommand;
import de.caluga.poppydb.netty.WatchCursorManager;

/**
 * #380, the trigger: the node's cleared-store notice - what {@code clearLocalDatabases()} raises
 * right before it drops the local databases - is the transition to RECOVERING, and it has to end
 * every client change stream the node still holds. The wire side of that (a parked getMore
 * answered with 286) is covered in {@code RecoveringChangeStreamTest}; this test pins that the
 * notice is what fires it, through the same seams {@code PartialRestoreGuardReleaseTest} uses.
 *
 * <p>Superseded managers are ignored here exactly as they are for the dump and election
 * consequences of the notice: a manager that was replaced by a later leader change must not be
 * able to cut the streams of a node that is not, in fact, about to be emptied.
 */
public class RecoveringNodeEndsChangeStreamsTest {

    private static final String DB = "recovering_node_db";
    private static final String COLL = "data";

    private PoppyDB db;

    @AfterEach
    public void tearDown() {
        if (db != null) {
            db.getCursorManagerForTest().shutdown();
            db.getDriver().close();
        }
    }

    private long registerStream(WatchCommand wcmd) {
        return db.getCursorManagerForTest().createWatchCursor(db.getDriver(), wcmd);
    }

    @Test
    public void clearedStoreNoticeEndsEveryClientStream() throws Exception {
        db = new PoppyDB(27017, "127.0.0.1", 100, 10);
        db.getDriver().insert(DB, COLL, List.of(Doc.of("_id", 1)), null, true);
        ReplicationManager rm = new ReplicationManager(null, "localhost", 12345);
        db.installReplicationManagerForTest(rm);

        WatchCommand first = new WatchCommand(db.getDriver()).setDb(DB).setColl(COLL).setMaxTimeMS(5000);
        WatchCommand second = new WatchCommand(db.getDriver()).setDb(DB).setColl("other").setMaxTimeMS(5000);
        long firstId = registerStream(first);
        long secondId = registerStream(second);
        WatchCursorManager cursors = db.getCursorManagerForTest();
        assertTrue(cursors.hasCursor(firstId) && cursors.hasCursor(secondId), "precondition: both streams are live");

        db.onLocalDataClearedForSyncForTest(rm);

        assertNotNull(first.getTerminalError(), "the first stream must be ended");
        assertNotNull(second.getTerminalError(), "the second stream must be ended");
        assertTrue(first.getTerminalError().contains("ChangeStreamHistoryLost"),
                "the reason carries the marker the clients key their restart on: " + first.getTerminalError());
        assertFalse(cursors.hasCursor(firstId), "an ended stream leaves no cursor behind");
        assertFalse(cursors.hasCursor(secondId), "an ended stream leaves no cursor behind");
    }

    @Test
    public void aSupersededManagerCannotEndTheStreams() throws Exception {
        db = new PoppyDB(27017, "127.0.0.1", 100, 10);
        db.getDriver().insert(DB, COLL, List.of(Doc.of("_id", 1)), null, true);
        ReplicationManager current = new ReplicationManager(null, "localhost", 12345);
        ReplicationManager superseded = new ReplicationManager(null, "localhost", 12346);
        db.installReplicationManagerForTest(current);

        WatchCommand stream = new WatchCommand(db.getDriver()).setDb(DB).setColl(COLL).setMaxTimeMS(5000);
        long cursorId = registerStream(stream);

        db.onLocalDataClearedForSyncForTest(superseded);

        assertNull(stream.getTerminalError(), "a stale manager's notice must not touch the streams");
        assertTrue(db.getCursorManagerForTest().hasCursor(cursorId));
    }
}
