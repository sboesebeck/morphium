package de.caluga.poppydb.netty;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.commands.WatchCommand;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import de.caluga.morphium.driver.wireprotocol.OpMsg;
import de.caluga.poppydb.messaging.MessagingOptimizer;
import io.netty.channel.embedded.EmbeddedChannel;

/**
 * A released exclusive-message lock must wake the waiting subscribers.
 *
 * <p>On PoppyDB, {@code MultiCollectionMessaging} deliberately does NOT open its own lock-monitor
 * change stream ("server pushes lock_released events directly … 0 extra connections"). It
 * therefore depends entirely on the server emitting a synthetic {@code lock_released} event when
 * a lock document is deleted. The only producer of that event used to sit in the generic command
 * path - but direct dispatch took over {@code delete} in 16355e3c2, making that path unreachable
 * for deletes and silently killing the notification: releasing a lock woke nobody, and
 * redelivery of the freed message waited for the next poll round (the msg_lck stall shape).
 *
 * <p>This test drives the handler directly (no sockets) and parks a getMore on the messaging
 * collection's change stream, exactly like a waiting subscriber.
 */
public class MessagingLockReleaseFastPathTest {

    private static final String DB = "msgtest";
    private static final String COLL = "msg";
    private static final String LOCK_COLL = "msg_lck";

    private InMemoryDriver drv;
    private WatchCursorManager cursorManager;
    private MessagingOptimizer optimizer;
    private EmbeddedChannel ch;
    private final AtomicInteger msgId = new AtomicInteger(1);

    @BeforeEach
    public void setUp() {
        drv = new InMemoryDriver();
        drv.connect();
        drv.setServerMode(true);
        cursorManager = new WatchCursorManager();
        optimizer = new MessagingOptimizer(drv);
        optimizer.setWatchCursorManager(cursorManager);
        optimizer.registerMessagingCollection(DB, COLL, LOCK_COLL, "subscriber-1");
        ch = new EmbeddedChannel(new MongoCommandHandler(drv, cursorManager, new FindCursorRegistry(),
                optimizer, msgId, "0.0.0.0", 27017, "my-rs", List.of("localhost:27017"), true,
                "localhost:27017", 0, () -> null));
    }

    @AfterEach
    public void tearDown() {
        if (ch != null) {
            ch.finishAndReleaseAll();
        }

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

    private void sendDelete(String collection) {
        OpMsg msg = new OpMsg();
        msg.setMessageId(msgId.incrementAndGet());
        msg.setFirstDoc(Doc.of("delete", collection,
                "deletes", List.of(Doc.of("q", Doc.of("_id", "lock-1"), "limit", 1)),
                "$db", DB));
        ch.writeInbound(msg);
        ch.runPendingTasks();
    }

    @Test
    public void deletingALockWakesTheWaitingSubscribers() throws Exception {
        drv.store(DB, LOCK_COLL, List.of(Doc.of("_id", "lock-1", "msg_id", "m1")), null);
        CompletableFuture<List<Map<String, Object>>> parked = parkedSubscriber();
        assertThat(parked.isDone()).as("the subscriber must be waiting, not answered yet").isFalse();

        sendDelete(LOCK_COLL);

        List<Map<String, Object>> events = parked.get(5, TimeUnit.SECONDS);
        assertThat(events).as("the released lock must wake the parked getMore").isNotEmpty();
        assertThat(events.get(0).get("operationType"))
            .as("event: " + events.get(0)).isEqualTo("lock_released");
        assertThat(((Map<?, ?>) events.get(0).get("ns")).get("coll"))
            .as("the event belongs to the MESSAGING collection, not the lock collection")
            .isEqualTo(COLL);
    }

    /**
     * A change stream event without a resume token makes spec-compliant drivers abort the whole
     * stream ("A change stream document has been received that lacks a resume token (_id)") -
     * mongosh dies on the first {@code lock_released} it sees, so any third-party client watching
     * a messaging collection breaks as soon as a lock is released (#347).
     *
     * <p>The token must not run ahead of the real event sequence either: a client resuming from it
     * has to continue at the next real event rather than skip one.
     */
    @Test
    public void lockReleasedEventCarriesAResumeToken() throws Exception {
        drv.store(DB, LOCK_COLL, List.of(Doc.of("_id", "lock-1", "msg_id", "m1")), null);
        CompletableFuture<List<Map<String, Object>>> parked = parkedSubscriber();

        sendDelete(LOCK_COLL);

        List<Map<String, Object>> events = parked.get(5, TimeUnit.SECONDS);
        Map<String, Object> evt = events.get(0);
        assertThat(evt.get("operationType")).isEqualTo("lock_released");

        assertThat(evt.get("_id"))
            .as("every change stream event needs a resume token, or spec-compliant drivers abort")
            .isInstanceOf(Map.class);
        Object data = ((Map<?, ?>) evt.get("_id")).get("_data");
        assertThat(data).as("resume token must carry _data: " + evt.get("_id")).isInstanceOf(String.class);
        assertThat(Long.parseLong((String) data, 16))
            .as("the synthetic token must not run ahead of the real event sequence")
            .isLessThanOrEqualTo(drv.getChangeStreamSequence());
    }

    @Test
    public void deletingAnUnrelatedCollectionDoesNotWakeSubscribers() throws Exception {
        CompletableFuture<List<Map<String, Object>>> parked = parkedSubscriber();

        sendDelete("some_other_collection");

        assertThat(parked.isDone())
            .as("only deletes on a registered lock collection may produce lock_released")
            .isFalse();
        parked.cancel(true);
    }
}
