package de.caluga.poppydb.netty;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.commands.WatchCommand;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A getMore that is already parked has to learn about an unservable stream immediately.
 *
 * <p>Checking the terminal state only when a NEW getMore arrives is not enough: the parked one
 * sits out its whole maxTimeMS and then answers with an empty batch - a successful-looking
 * reply for a stream already known to be dead - and only the request after that sees the error.
 * With a generous maxTimeMS that hides the failure for as long as the timeout lasts.
 */
@Tag("poppydb")
public class WatchCursorTerminalErrorTest {

    @Test
    public void parkedGetMoreFailsAsSoonAsTheWatchBecomesUnservable() throws Exception {
        InMemoryDriver drv = new InMemoryDriver();
        drv.connect();
        WatchCursorManager cursors = new WatchCursorManager();

        try {
            drv.insert("terminal_db", "coll", List.of(Doc.of("_id", 1, "counter", 1)), null, true);

            WatchCommand wcmd = new WatchCommand(drv).setDb("terminal_db").setColl("coll")
                    .setMaxTimeMS(30000);
            long cursorId = cursors.createWatchCursor(drv, wcmd);

            // Parked: nothing buffered, so this future waits - for 30s, if nobody wakes it.
            CompletableFuture<List<Map<String, Object>>> parked = cursors.getMore(cursorId, 30000);
            Thread.sleep(300);
            assertFalse(parked.isDone(), "precondition: the request must actually be waiting");

            long start = System.currentTimeMillis();
            wcmd.setTerminalError("resume window lost: no pre-image for the requested token");

            ExecutionException failure = assertThrows(ExecutionException.class,
                () -> parked.get(5, TimeUnit.SECONDS),
                "a parked getMore must fail when the stream becomes unservable, not wait out its timeout");

            long waited = System.currentTimeMillis() - start;
            assertTrue(waited < 5000,
                "the failure has to arrive promptly, not after maxTimeMS - waited " + waited + "ms");
            assertInstanceOf(WatchCursorManager.ChangeStreamHistoryLostException.class, failure.getCause(),
                "the cause has to be the history-lost signal the handler maps to code 286");
            assertTrue(failure.getCause().getMessage().contains("resume window lost"),
                "the reason must survive to the client");
        } finally {
            cursors.shutdown();
            drv.close();
        }
    }

    /**
     * Events that were legitimately produced before the stream broke still belong to the
     * consumer - the failure must not swallow them.
     */
    @Test
    public void bufferedEventsAreDeliveredBeforeTheFailure() throws Exception {
        InMemoryDriver drv = new InMemoryDriver();
        drv.connect();
        WatchCursorManager cursors = new WatchCursorManager();

        try {
            drv.insert("terminal_db2", "coll", List.of(Doc.of("_id", 1, "counter", 1)), null, true);

            WatchCommand wcmd = new WatchCommand(drv).setDb("terminal_db2").setColl("coll")
                    .setMaxTimeMS(30000);
            long cursorId = cursors.createWatchCursor(drv, wcmd);

            Thread.sleep(200);
            drv.insert("terminal_db2", "coll", List.of(Doc.of("_id", 2, "counter", 2)), null, true);
            Thread.sleep(300);

            wcmd.setTerminalError("resume window lost");

            List<Map<String, Object>> batch = cursors.getMore(cursorId, 1000).get(5, TimeUnit.SECONDS);
            assertFalse(batch.isEmpty(), "the event produced before the break must still be delivered");

            // ...and only then does the stream report why it ended.
            ExecutionException failure = assertThrows(ExecutionException.class,
                () -> cursors.getMore(cursorId, 1000).get(5, TimeUnit.SECONDS));
            assertInstanceOf(WatchCursorManager.ChangeStreamHistoryLostException.class, failure.getCause());
        } finally {
            cursors.shutdown();
            drv.close();
        }
    }
}
