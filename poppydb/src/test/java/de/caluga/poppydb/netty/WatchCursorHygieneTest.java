package de.caluga.poppydb.netty;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.commands.WatchCommand;
import de.caluga.morphium.driver.inmem.InMemoryDriver;

/**
 * #326: what a watch cursor leaves behind when it dies, and what a client is told when it never
 * lived.
 *
 * <p>Cursors are removed on five different paths (kill, buffer overflow, terminal error seen by
 * getMore, {@code failUnservable}, and a failed start). Two of them used to remove the cursor
 * without unregistering its messaging registration - and since nothing else ever removes an id
 * from {@code messagingCursors}, every terminated cursor stayed in that set forever, so the set
 * never emptied and its key was never removed either. Reconnect churn after
 * {@code ChangeStreamHistoryLost} - exactly what these paths exist for - grew it without bound.
 */
public class WatchCursorHygieneTest {

    private static final String DB = "hygiene";
    private static final String COLL = "msg";

    private InMemoryDriver drv;
    private WatchCursorManager cursors;

    @BeforeEach
    public void setUp() {
        drv = new InMemoryDriver();
        drv.connect();
        drv.setServerMode(true);
        cursors = new WatchCursorManager();
    }

    @AfterEach
    public void tearDown() {
        if (cursors != null) {
            cursors.shutdown();
        }

        if (drv != null) {
            drv.close();
        }
    }

    private WatchCommand wcmd;

    /** A registered messaging cursor; its WatchCommand is kept in {@link #wcmd}. */
    private long messagingCursor() {
        wcmd = new WatchCommand(drv).setDb(DB).setColl(COLL).setMaxTimeMS(30000);
        long cursorId = cursors.createWatchCursor(drv, wcmd);
        cursors.registerMessagingCursor(cursorId, DB, COLL, "subscriber-1");
        assertThat(cursors.hasMessagingCursors(DB, COLL))
            .as("precondition: the cursor is registered for fast-path delivery").isTrue();
        return cursorId;
    }

    /** Nothing buffered: setTerminalError -> failUnservable drops the cursor right away. */
    @Test
    public void failUnservableUnregistersTheMessagingCursor() {
        messagingCursor();

        wcmd.setTerminalError("resume window lost");

        assertThat(cursors.hasMessagingCursors(DB, COLL))
            .as("the dropped cursor must not stay registered for fast-path delivery").isFalse();
    }

    /**
     * Something still buffered: failUnservable deliberately KEEPS the cursor so the client can
     * collect what was legitimately delivered before the stream broke. The removal then happens
     * in getMore's terminal check - the second of the two paths that used to leak.
     */
    @Test
    public void aTerminalErrorSeenByGetMoreUnregistersTheMessagingCursor() throws Exception {
        long cursorId = messagingCursor();
        drv.store(DB, COLL, List.of(Doc.of("_id", 1, "v", "buffered")), null);

        // wait for the event to reach the cursor's queue WITHOUT draining it - the buffered
        // event is what makes failUnservable keep the cursor alive for this path
        for (int i = 0; i < 100 && cursors.bufferedEventCount(cursorId) < 1; i++) {
            Thread.sleep(20);
        }

        assertThat(cursors.bufferedEventCount(cursorId))
            .as("precondition: the cursor holds an undelivered event").isGreaterThan(0);

        wcmd.setTerminalError("resume window lost");
        assertThat(cursors.hasCursor(cursorId))
            .as("failUnservable must keep a cursor that still owes the client events").isTrue();

        // first getMore hands over what was legitimately delivered before the break...
        assertThat(cursors.getMore(cursorId, 100).get(5, TimeUnit.SECONDS))
            .as("buffered events must still reach the client").isNotEmpty();

        // ...and only the next one reports the stream as unservable
        CompletableFuture<List<Map<String, Object>>> f = cursors.getMore(cursorId, 100);

        assertThatThrownBy(() -> f.get(5, TimeUnit.SECONDS))
            .as("an unservable stream must be reported, not answered empty")
            .isInstanceOf(ExecutionException.class)
            .hasRootCauseInstanceOf(WatchCursorManager.ChangeStreamHistoryLostException.class);
        assertThat(cursors.hasMessagingCursors(DB, COLL))
            .as("the dead cursor must not stay registered for fast-path delivery").isFalse();
    }

    @Test
    public void aStreamThatDiesWhileTheGetMoreParksIsFailedInsteadOfSittingOutMaxTimeMs() throws Exception {
        long cursorId = messagingCursor();
        // The stream dies while nothing is parked yet - failUnservable drains an empty pending
        // queue and drops the cursor. A getMore arriving right after used to be orphaned and
        // answered by the timeout with an empty, successful-looking batch.
        wcmd.setTerminalError("resume window lost");
        cursors.killCursor(cursorId);

        long started = System.currentTimeMillis();
        CompletableFuture<List<Map<String, Object>>> f = cursors.getMore(cursorId, 30_000);
        f.handle((r, e) -> null).get(5, TimeUnit.SECONDS);

        assertThat(System.currentTimeMillis() - started)
            .as("the request must be answered right away, not after maxTimeMS")
            .isLessThan(5_000);
    }

    @Test
    public void aWatchThatCannotStartIsReportedInsteadOfHandingOutADeadCursor() {
        InMemoryDriver failing = new InMemoryDriver() {
            @Override
            public int runCommand(WatchCommand cmd) throws MorphiumDriverException {
                throw new MorphiumDriverException("watch cannot be established");
            }
        };
        failing.connect();

        try {
            WatchCommand failingWatch = new WatchCommand(failing).setDb(DB).setColl(COLL);

            assertThatThrownBy(() -> cursors.createWatchCursor(failing, failingWatch))
                .as("a stream that never started must not be handed back as a cursor id")
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("could not start change stream");
            assertThat(cursors.hasMessagingCursors(DB, COLL))
                .as("and nothing may stay registered for it").isFalse();
        } finally {
            failing.close();
        }
    }

    @Test
    public void registeringACursorThatNoLongerExistsIsIgnored() {
        long cursorId = messagingCursor();
        assertThat(cursors.killCursor(cursorId)).isTrue();
        assertThat(cursors.hasMessagingCursors(DB, COLL))
            .as("killCursor already unregisters").isFalse();

        // a late registration for a cursor that is gone must not resurrect the entry - nothing
        // would ever remove it again
        cursors.registerMessagingCursor(cursorId, DB, COLL, "subscriber-1");

        assertThat(cursors.hasMessagingCursors(DB, COLL))
            .as("a registration for a dead cursor id must be ignored").isFalse();
    }

    @Test
    public void bufferOverflowStillUnregisters() throws Exception {
        long cursorId = messagingCursor();

        // fill past MAX_CURSOR_QUEUE_SIZE so the cursor is killed by the overflow guard
        for (int i = 0; i < WatchCursorManager.MAX_CURSOR_QUEUE_SIZE + 5; i++) {
            drv.store(DB, COLL, List.of(Doc.of("_id", i, "v", "x")), null);
        }

        for (int i = 0; i < 100 && cursors.hasCursor(cursorId); i++) {
            Thread.sleep(50);
        }

        assertThat(cursors.hasCursor(cursorId)).as("the overflowing cursor must be killed").isFalse();
        assertThat(cursors.hasMessagingCursors(DB, COLL))
            .as("and its messaging registration must go with it").isFalse();
    }
}
