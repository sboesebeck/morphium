package de.caluga.morphium.driver.inmem;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.DriverTailableIterationCallback;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.commands.WatchCommand;
import de.caluga.morphium.driver.wire.MongoConnection;

/**
 * Regression tests for issue #320: the resume-window check used to be check-then-act — validated
 * (if at all) at registration time while the replay ran later, with eviction racing both. A
 * consumer could therefore receive a silently gapped stream: the surviving suffix plus the live
 * events, with the hole in the middle invisible.
 *
 * <p>The fix verifies the window <em>inside</em> {@code replayHistory}: after the replay it
 * checks that every token of the resume window was either observed in the buffer or already
 * delivered live to this subscription, and that no covering namespace was dropped past the
 * resume point. Any hole ends the stream with a visible {@code ChangeStreamHistoryLost}-style
 * terminal error instead of delivering a suffix. This also closes the previously entirely
 * ungated ordinary-resume path ({@code ChangeStreamMonitor} re-subscribing with
 * {@code resumeAfter} — i.e. messaging), not just PoppyDB's gated replication resume.
 */
@Tag("inmemory")
public class InMemReplayWindowLossTest {

    private static final String DB = "replay_window_loss";
    private static final String COLL = "events";

    private InMemoryDriver drv;

    @BeforeEach
    void setup() throws Exception {
        drv = new InMemoryDriver();
        drv.connect();
    }

    @AfterEach
    void tearDown() {
        drv.armReplayPauseForTest(null, null);
        drv.close();
    }

    private void insert(String coll, String id) throws Exception {
        drv.store(DB, coll, List.of(Doc.of("_id", id, "v", id)), null);
    }

    private static Map<String, Object> resumeToken(long token) {
        return Doc.of("_data", String.format(Locale.ROOT, "%016x", token));
    }

    /**
     * Runs a resumed watch on the current thread until {@code runForMs} elapsed (or the stream
     * dies). Returns the terminal exception, or null if the watch ended normally.
     */
    private MorphiumDriverException runWatch(String coll, Map<String, Object> resumeAfter,
            List<Object> deliveredIds, long runForMs) throws MorphiumDriverException {
        MongoConnection con = drv.getPrimaryConnection(null);
        long deadline = System.currentTimeMillis() + runForMs;
        WatchCommand cmd = new WatchCommand(con).setDb(DB).setColl(coll).setMaxTimeMS(100)
                .setResumeAfter(resumeAfter)
                .setCb(new DriverTailableIterationCallback() {
                    @Override
                    public void incomingData(Map<String, Object> data, long dur) {
                        @SuppressWarnings("unchecked")
                        Map<String, Object> key = (Map<String, Object>) data.get("documentKey");
                        deliveredIds.add(key == null ? null : key.get("_id"));
                    }

                    @Override
                    public boolean isContinued() {
                        return System.currentTimeMillis() < deadline;
                    }
                });

        try {
            cmd.watch();
            return null;
        } catch (MorphiumDriverException e) {
            return e;
        } finally {
            cmd.releaseConnection();
        }
    }

    /**
     * The window was already gone when the resume started: capacity eviction removed events
     * between the resume token and the oldest retained entry. The old behaviour delivered the
     * surviving suffix as if nothing happened; the consumer must instead get a visible
     * window-lost error.
     */
    @Test
    void resumeAfterEvictedWindowFailsWithHistoryLost() throws Exception {
        insert(COLL, "a");                                   // token 1
        long tokenOfA = drv.getChangeStreamSequence();

        for (int i = 0; i < 11; i++) {
            insert(COLL, "b" + i);                           // tokens 2..12
        }

        // Trim to the newest 5 events — tokens 2..7 are evicted, the window from tokenOfA is gone.
        drv.setChangeStreamHistoryLimit(5);

        List<Object> delivered = Collections.synchronizedList(new ArrayList<>());
        MorphiumDriverException err = runWatch(COLL, resumeToken(tokenOfA), delivered, 1500);

        assertNotNull(err, "resume over an evicted window must fail visibly, not deliver the "
                + "surviving suffix (delivered: " + delivered + ")");
        assertTrue(err.getMessage().contains("ChangeStreamHistoryLost"),
                "the error must carry the ChangeStreamHistoryLost marker so ChangeStreamMonitor "
                + "discards its resume token, got: " + err.getMessage());
    }

    /**
     * The check-then-act race itself: the window is intact when the resume starts, but eviction
     * strikes in the gap between registration and the replay actually running (in production the
     * exec-queue delay; here made deterministic via the replay pause seam).
     */
    @Test
    void evictionBetweenRegistrationAndReplayFailsWithHistoryLost() throws Exception {
        insert(COLL, "a");                                   // token 1
        long tokenOfA = drv.getChangeStreamSequence();

        for (int i = 0; i < 7; i++) {
            insert(COLL, "b" + i);                           // tokens 2..8
        }

        CountDownLatch reached = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        drv.armReplayPauseForTest(reached, release);

        List<Object> delivered = Collections.synchronizedList(new ArrayList<>());
        AtomicReference<MorphiumDriverException> err = new AtomicReference<>();
        Thread watcher = Thread.ofVirtual().start(() -> {
            try {
                err.set(runWatch(COLL, resumeToken(tokenOfA), delivered, 1500));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        assertTrue(reached.await(5, TimeUnit.SECONDS), "replay must reach the pause seam");
        // The replay is now parked right before iterating the buffer. Evict the resume window.
        drv.setChangeStreamHistoryLimit(2);                  // only tokens 7..8 survive
        release.countDown();
        watcher.join(5000);

        assertNotNull(err.get(), "a window evicted between registration and replay must fail "
                + "visibly, not deliver the surviving suffix (delivered: " + delivered + ")");
        assertTrue(err.get().getMessage().contains("ChangeStreamHistoryLost"),
                "expected ChangeStreamHistoryLost, got: " + err.get().getMessage());
    }

    /**
     * A resume whose token predates a drop of the watched collection must fail loud: the
     * consumer missed the drop and would otherwise keep serving the dropped collection forever.
     * The old behaviour silently skipped to the post-drop events.
     */
    @Test
    void resumeAcrossOwnCollectionDropFailsWithHistoryLost() throws Exception {
        insert(COLL, "a");                                   // token 1
        long tokenOfA = drv.getChangeStreamSequence();
        insert(COLL, "b");
        drv.drop(DB, COLL, null);

        List<Object> delivered = Collections.synchronizedList(new ArrayList<>());
        MorphiumDriverException err = runWatch(COLL, resumeToken(tokenOfA), delivered, 1500);

        assertNotNull(err, "a resume across the watched collection's drop must fail visibly");
        assertTrue(err.getMessage().contains("ChangeStreamHistoryLost"),
                "expected ChangeStreamHistoryLost, got: " + err.getMessage());
    }

    /**
     * A resume token beyond the driver's own sequence is from a foreign/reset sequence space
     * (primary restart, failover to another node). It must be rejected instead of silently
     * starting "from now" as if the resume had worked.
     */
    @Test
    void resumeFromForeignSequenceSpaceFailsWithHistoryLost() throws Exception {
        insert(COLL, "a");
        long foreign = drv.getChangeStreamSequence() + 1000;

        List<Object> delivered = Collections.synchronizedList(new ArrayList<>());
        MorphiumDriverException err = runWatch(COLL, resumeToken(foreign), delivered, 1500);

        assertNotNull(err, "a resume token beyond the driver's sequence must fail visibly");
        assertTrue(err.getMessage().contains("ChangeStreamHistoryLost"),
                "expected ChangeStreamHistoryLost, got: " + err.getMessage());
    }

    /**
     * Namespace fairness guard: a drop of an UNRELATED collection must not kill a
     * collection-scoped resume whose own window is fully intact. (The global drop boundary
     * rightly kills cluster-wide replication resumes — but a collection watch that lost
     * nothing has to keep working, otherwise every test-suite cleanup drop would force a
     * resync on every unrelated consumer.)
     */
    @Test
    void foreignCollectionDropDoesNotKillUnrelatedResume() throws Exception {
        insert(COLL, "a");                                   // token 1
        long tokenOfA = drv.getChangeStreamSequence();
        insert(COLL, "b");                                   // token 2 — in the resume window
        insert("other", "x");                                // token 3 — foreign collection
        drv.drop(DB, "other", null);                         // purges token 3, jumps the sequence

        List<Object> delivered = Collections.synchronizedList(new ArrayList<>());
        MorphiumDriverException err = runWatch(COLL, resumeToken(tokenOfA), delivered, 1500);

        assertNull(err, "an unrelated collection's drop must not kill an intact resume, got: "
                + (err == null ? "" : err.getMessage()));
        assertEquals(1, delivered.stream().filter("b"::equals).count(),
                "the intact window must be replayed exactly once: " + delivered);
    }
}
