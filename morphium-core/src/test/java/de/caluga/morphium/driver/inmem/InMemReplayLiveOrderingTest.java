package de.caluga.morphium.driver.inmem;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
 * Regression test for issue #319: on a resumed change stream the subscription is registered
 * first and the history replay runs afterwards, while live events are dispatched concurrently
 * into the same subscription. Without an ordering barrier a live event with token 105 reaches
 * the consumer before the replayed tokens 101-104 — out-of-order delivery that lets a replayed
 * older fullDocument overwrite a newer one on a PoppyDB secondary, or a replayed insert
 * resurrect a document a live delete already removed.
 *
 * <p>The fix stages live deliveries in a bounded per-subscription buffer while the replay runs
 * and flushes them in token order when the replay completes: strict per-stream ordering, no
 * duplicates, writers never block.
 */
@Tag("inmemory")
public class InMemReplayLiveOrderingTest {

    private static final String DB = "replay_live_ordering";
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

    private void insert(String id) throws Exception {
        drv.store(DB, COLL, List.of(Doc.of("_id", id, "v", id)), null);
    }

    private record Delivery(Object id, long token) {}

    /**
     * Makes the #319 race deterministic via the replay pause seam: the resumed watch is parked
     * between registration and the replay, live writes land in exactly that window (their
     * dispatch is what used to overtake the replay), then the replay runs. The consumer must
     * see strictly ascending tokens — replayed history first, then the raced live events — and
     * every event exactly once.
     */
    @Test
    void liveEventsDuringReplayAreDeliveredInTokenOrderExactlyOnce() throws Exception {
        insert("a");                                          // token 1 — the resume point
        long tokenOfA = drv.getChangeStreamSequence();
        insert("b");                                          // token 2 — history to replay
        insert("c");                                          // token 3 — history to replay

        CountDownLatch reached = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        drv.armReplayPauseForTest(reached, release);

        List<Delivery> deliveries = Collections.synchronizedList(new ArrayList<>());
        AtomicReference<MorphiumDriverException> err = new AtomicReference<>();
        long deadline = System.currentTimeMillis() + 3000;

        Thread watcher = Thread.ofVirtual().start(() -> {
            MongoConnection con;
            try {
                con = drv.getPrimaryConnection(null);
            } catch (MorphiumDriverException e) {
                throw new RuntimeException(e);
            }
            WatchCommand cmd = new WatchCommand(con).setDb(DB).setColl(COLL).setMaxTimeMS(100)
                    .setResumeAfter(Doc.of("_data", String.format(Locale.ROOT, "%016x", tokenOfA)))
                    .setCb(new DriverTailableIterationCallback() {
                        @Override
                        public void incomingData(Map<String, Object> data, long dur) {
                            @SuppressWarnings("unchecked")
                            Map<String, Object> key = (Map<String, Object>) data.get("documentKey");
                            @SuppressWarnings("unchecked")
                            Map<String, Object> token = (Map<String, Object>) data.get("_id");
                            deliveries.add(new Delivery(key == null ? null : key.get("_id"),
                                    Long.parseUnsignedLong((String) token.get("_data"), 16)));
                        }

                        @Override
                        public boolean isContinued() {
                            return System.currentTimeMillis() < deadline;
                        }
                    });

            try {
                cmd.watch();
            } catch (MorphiumDriverException e) {
                err.set(e);
            } finally {
                cmd.releaseConnection();
            }
        });

        assertTrue(reached.await(5, TimeUnit.SECONDS), "replay must reach the pause seam");

        // The watch is registered but its replay is parked — the exec-queue window of #319.
        // These two writes are dispatched live into the registered subscription NOW, while the
        // history events b and c have not been replayed yet.
        insert("d");                                          // token 4
        insert("e");                                          // token 5

        // Give the async client-mode dispatcher time to actually deliver (pre-fix) or stage
        // (post-fix) the live events before the replay is released — this is what makes the
        // pre-fix inversion deterministic instead of a scheduling coincidence.
        Thread.sleep(500);
        release.countDown();
        watcher.join(5000);

        assertNull(err.get(), "the watch must end normally, got: "
                + (err.get() == null ? "" : err.get().getMessage()));

        List<Delivery> got = new ArrayList<>(deliveries);
        for (String id : List.of("b", "c", "d", "e")) {
            assertEquals(1, got.stream().filter(x -> id.equals(x.id())).count(),
                    "event '" + id + "' must be delivered exactly once: " + got);
        }

        long prev = 0;
        for (Delivery x : got) {
            assertTrue(x.token() > prev, "tokens must be strictly ascending — a live event must "
                    + "not overtake the replayed history: " + got);
            prev = x.token();
        }
    }
}
