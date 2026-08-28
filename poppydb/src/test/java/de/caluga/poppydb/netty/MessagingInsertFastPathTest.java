package de.caluga.poppydb.netty;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.bson.MongoTimestamp;
import de.caluga.morphium.driver.commands.WatchCommand;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import de.caluga.poppydb.messaging.MessagingOptimizer;

/**
 * The messaging insert fast-path ({@code MessagingOptimizer.notifyMessageInsert}) builds a
 * synthetic change stream event, and that event must carry a resume token and a clusterTime
 * like every other event a client can receive.
 *
 * <p>The path is currently DORMANT: {@code notifyMessagingCursorsOnInsert} in
 * {@code MongoCommandHandler} has no caller (deliberately disabled to avoid duplicate delivery
 * next to the normal change stream chain). This test drives the optimizer directly anyway,
 * because whoever re-enables the fast-path would otherwise reproduce #347 on the first message
 * insert: spec-compliant drivers abort the whole stream on an event without {@code _id}
 * ("A change stream document has been received that lacks a resume token (_id)").
 *
 * <p>Token semantics under test: the insert is a real oplog operation whose real event (with
 * its own freshly allocated token) has already been emitted by the time the fast-path runs.
 * The synthetic duplicate therefore uses the CURRENT sequence - never a fresh one - so a client
 * resuming from it continues at the next real event without seeing the same insert twice, and
 * no phantom token is minted that no replay buffer entry ever matches.
 */
public class MessagingInsertFastPathTest {

    private static final String DB = "msgtest";
    private static final String COLL = "msg";
    private static final String LOCK_COLL = "msg_lck";

    private InMemoryDriver drv;
    private WatchCursorManager cursorManager;
    private MessagingOptimizer optimizer;

    @BeforeEach
    public void setUp() {
        drv = new InMemoryDriver();
        drv.connect();
        drv.setServerMode(true);
        cursorManager = new WatchCursorManager();
        optimizer = new MessagingOptimizer(drv);
        optimizer.setWatchCursorManager(cursorManager);
        optimizer.registerMessagingCollection(DB, COLL, LOCK_COLL, "subscriber-1");
    }

    @AfterEach
    public void tearDown() {
        if (cursorManager != null) {
            cursorManager.shutdown();
        }

        if (drv != null) {
            drv.close();
        }
    }

    /** A subscriber watching the messaging collection, with its getMore parked. */
    private CompletableFuture<List<Map<String, Object>>> parkedSubscriber() {
        WatchCommand wcmd = new WatchCommand(drv);
        wcmd.setDb(DB);
        wcmd.setColl(COLL);
        long cursorId = cursorManager.createWatchCursor(drv, wcmd);
        cursorManager.registerMessagingCursor(cursorId, DB, COLL, "subscriber-1");
        return cursorManager.getMore(cursorId, 5000);
    }

    @Test
    public void fastPathInsertEventCarriesAResumeTokenAndClusterTime() throws Exception {
        // A real insert first, as in production: by the time the fast-path fires, the driver
        // has already emitted the real event for it and advanced the sequence.
        Map<String, Object> document = Doc.of("_id", "m1", "sender", "other-sender", "name", "test");
        drv.store(DB, COLL, List.of(document), null);

        CompletableFuture<List<Map<String, Object>>> parked = parkedSubscriber();
        long seqBefore = drv.getChangeStreamSequence();

        boolean used = optimizer.notifyMessageInsert(DB, COLL, document);
        assertThat(used).as("the fast-path must fire for a registered messaging collection").isTrue();

        List<Map<String, Object>> events = parked.get(5, TimeUnit.SECONDS);
        assertThat(events).as("the fast-path must wake the parked getMore").isNotEmpty();
        Map<String, Object> evt = events.get(0);
        assertThat(evt.get("operationType")).as("event: " + evt).isEqualTo("insert");

        assertThat(evt.get("_id"))
            .as("every change stream event needs a resume token, or spec-compliant drivers abort (#347)")
            .isInstanceOf(Map.class);
        Object data = ((Map<?, ?>) evt.get("_id")).get("_data");
        assertThat(data).as("resume token must carry _data: " + evt.get("_id")).isInstanceOf(String.class);
        assertThat((String) data).as("token format must match the %016x shape of real tokens").hasSize(16);

        long tokenValue = Long.parseLong((String) data, 16);
        assertThat(tokenValue)
            .as("the synthetic event must reuse the CURRENT sequence, not allocate a fresh one")
            .isEqualTo(seqBefore);
        assertThat(drv.getChangeStreamSequence())
            .as("the fast-path must not consume a sequence number of its own")
            .isEqualTo(seqBefore);

        // clusterTime is set as plain millis by the optimizer and rewritten to a BSON timestamp
        // at the wire boundary (withWireClusterTime in offerWatchEvent) - the subscriber sees
        // the converted form, proving both that the field exists and that conversion applied.
        assertThat(evt.get("clusterTime"))
            .as("event must carry a wire-typed clusterTime: " + evt)
            .isInstanceOf(MongoTimestamp.class);
    }

    @Test
    public void fastPathFiltersTheSendersOwnMessage() throws Exception {
        Map<String, Object> document = Doc.of("_id", "m2", "sender", "subscriber-1", "name", "own");
        drv.store(DB, COLL, List.of(document), null);
        CompletableFuture<List<Map<String, Object>>> parked = parkedSubscriber();

        optimizer.notifyMessageInsert(DB, COLL, document);

        assertThat(parked.isDone())
            .as("a subscriber must not be woken by its own message")
            .isFalse();
        parked.cancel(true);
    }
}
