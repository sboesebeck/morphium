package de.caluga.poppydb.netty;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.bson.BsonEncoder;
import de.caluga.morphium.driver.bson.MongoTimestamp;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import de.caluga.morphium.driver.wireprotocol.OpMsg;
import de.caluga.poppydb.messaging.MessagingOptimizer;
import io.netty.channel.embedded.EmbeddedChannel;

/**
 * A change stream event's {@code clusterTime} must leave the server as a BSON timestamp (0x11),
 * not as an int64 (0x12).
 *
 * <p>The driver-internal events carry {@code clusterTime} as a plain Java long of epoch millis,
 * and BsonEncoder writes a Long as 0x12. Real MongoDB sends a timestamp there; the official Java
 * driver decodes {@code ChangeStreamDocument.clusterTime} typed as {@code BsonTimestamp} and
 * fails on the int64. mongosh and Node are untyped and swallow it, which is why this went
 * unnoticed. The server rewrites the field at the wire boundary
 * ({@code WatchCursorManager.withWireClusterTime}); this test fetches events over the real wire
 * path (aggregate + $changeStream, then getMore through the handler) and checks the actual BSON
 * TYPE of the encoded field, not just its presence.
 */
public class ChangeStreamClusterTimeWireTypeTest {

    private static final String DB = "cttest";
    private static final String COLL = "data";
    private static final String MSG_COLL = "msg";
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
        optimizer.registerMessagingCollection(DB, MSG_COLL, LOCK_COLL, "subscriber-1");
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

    /** Send one command document through the handler and return the reply document. */
    private Map<String, Object> sendCommand(Map<String, Object> cmd) {
        OpMsg msg = new OpMsg();
        msg.setMessageId(msgId.incrementAndGet());
        msg.setFirstDoc(cmd);
        ch.writeInbound(msg);
        ch.runPendingTasks();
        OpMsg reply = ch.readOutbound();
        assertThat(reply).as("the handler must answer the command: " + cmd.keySet()).isNotNull();
        return reply.getFirstDoc();
    }

    /** Open a change stream over the wire (aggregate + $changeStream) and return its cursor id. */
    @SuppressWarnings("unchecked")
    private long openChangeStream(String collection) {
        Map<String, Object> reply = sendCommand(Doc.of(
                "aggregate", collection,
                "pipeline", List.of(Doc.of("$changeStream", Doc.of())),
                "cursor", Doc.of(),
                "$db", DB));
        assertThat(reply.get("ok")).as("watch must succeed: " + reply).isEqualTo(1.0);
        long cursorId = ((Number) ((Map<String, Object>) reply.get("cursor")).get("id")).longValue();
        assertThat(cursorId).isNotZero();
        return cursorId;
    }

    /** The buffered events cannot be observed synchronously - delivery is async in the driver. */
    private void awaitBufferedEvents(long cursorId, int atLeast) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 5000;

        while (cursorManager.bufferedEventCount(cursorId) < atLeast) {
            assertThat(System.currentTimeMillis())
                .as("expected " + atLeast + " buffered events on cursor " + cursorId)
                .isLessThan(deadline);
            Thread.sleep(10);
        }
    }

    /** getMore through the real handler path, exactly like a wire client. */
    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> getMoreOverWire(long cursorId, String collection) {
        Map<String, Object> reply = sendCommand(Doc.of(
                "getMore", cursorId,
                "collection", collection,
                "maxTimeMS", 1000,
                "$db", DB));
        assertThat(reply.get("ok")).as("getMore must succeed: " + reply).isEqualTo(1.0);
        return (List<Map<String, Object>>) ((Map<String, Object>) reply.get("cursor")).get("nextBatch");
    }

    /**
     * The BSON element type byte the encoded document uses for a top-level field: in BSON every
     * element is [type byte]["name"\0][value], so the byte immediately before the field's
     * name-plus-NUL is its wire type.
     */
    private static byte bsonTypeOf(byte[] bson, String fieldName) {
        byte[] pattern = (fieldName + "\0").getBytes(StandardCharsets.US_ASCII);

        for (int i = 1; i <= bson.length - pattern.length; i++) {
            boolean match = true;

            for (int j = 0; j < pattern.length; j++) {
                if (bson[i + j] != pattern[j]) {
                    match = false;
                    break;
                }
            }

            if (match) {
                return bson[i - 1];
            }
        }

        throw new AssertionError("field " + fieldName + " not found in encoded document");
    }

    @Test
    public void clusterTimeGoesOnTheWireAsBsonTimestamp() throws Exception {
        long beforeSeconds = System.currentTimeMillis() / 1000L;
        long cursorId = openChangeStream(COLL);

        drv.store(DB, COLL, List.of(Doc.of("_id", "d1", "value", 42)), null);
        awaitBufferedEvents(cursorId, 1);

        List<Map<String, Object>> batch = getMoreOverWire(cursorId, COLL);
        assertThat(batch).isNotEmpty();
        Map<String, Object> event = batch.get(0);
        assertThat(event.get("operationType")).isEqualTo("insert");

        Object clusterTime = event.get("clusterTime");
        assertThat(clusterTime)
            .as("clusterTime must be a BSON timestamp on the wire - typed drivers decode it as "
                + "BsonTimestamp and fail on an int64. Got: "
                + (clusterTime == null ? "null" : clusterTime.getClass().getName()))
            .isInstanceOf(MongoTimestamp.class);

        MongoTimestamp ts = (MongoTimestamp) clusterTime;
        long afterSeconds = System.currentTimeMillis() / 1000L;
        assertThat((long) ts.getTime())
            .as("the timestamp's seconds part must be the event's wall-clock time, not raw millis: " + ts)
            .isBetween(beforeSeconds, afterSeconds);

        // The actual wire bytes: BsonEncoder writes MongoTimestamp as 0x11 and Long as 0x12,
        // so this pins the encoded TYPE, not just the Java class.
        byte type = bsonTypeOf(BsonEncoder.encodeDocument(event), "clusterTime");
        assertThat(type)
            .as("encoded BSON element type of clusterTime (0x11 = timestamp, 0x12 = int64)")
            .isEqualTo((byte) 0x11);
    }

    @Test
    public void clusterTimeIsNonDecreasingAcrossConsecutiveEvents() throws Exception {
        long cursorId = openChangeStream(COLL);

        drv.store(DB, COLL, List.of(Doc.of("_id", "d1")), null);
        drv.store(DB, COLL, List.of(Doc.of("_id", "d2")), null);
        awaitBufferedEvents(cursorId, 2);

        List<Map<String, Object>> batch = getMoreOverWire(cursorId, COLL);
        assertThat(batch.size()).isGreaterThanOrEqualTo(2);

        MongoTimestamp first = (MongoTimestamp) batch.get(0).get("clusterTime");
        MongoTimestamp second = (MongoTimestamp) batch.get(1).get("clusterTime");
        assertThat(first.compareTo(second))
            .as("clusterTime must not fall between consecutive events: " + first + " -> " + second)
            .isLessThanOrEqualTo(0);
    }

    /**
     * The synthetic {@code lock_released} event (#347) is built with plain-millis clusterTime in
     * MongoCommandHandler and must run through the same wire conversion as every real event.
     * Setup mirrors {@link MessagingLockReleaseFastPathTest}: a parked subscriber on the
     * messaging collection, woken by a lock delete through the handler.
     */
    @Test
    public void lockReleasedEventGetsTheSameConversion() throws Exception {
        drv.store(DB, LOCK_COLL, List.of(Doc.of("_id", "lock-1", "msg_id", "m1")), null);

        de.caluga.morphium.driver.commands.WatchCommand wcmd =
            new de.caluga.morphium.driver.commands.WatchCommand(drv);
        wcmd.setDb(DB);
        wcmd.setColl(MSG_COLL);
        long cursorId = cursorManager.createWatchCursor(drv, wcmd);
        cursorManager.registerMessagingCursor(cursorId, DB, MSG_COLL, "subscriber-1");
        CompletableFuture<List<Map<String, Object>>> parked = cursorManager.getMore(cursorId, 5000);

        OpMsg msg = new OpMsg();
        msg.setMessageId(msgId.incrementAndGet());
        msg.setFirstDoc(Doc.of("delete", LOCK_COLL,
                "deletes", List.of(Doc.of("q", Doc.of("_id", "lock-1"), "limit", 1)),
                "$db", DB));
        ch.writeInbound(msg);
        ch.runPendingTasks();

        List<Map<String, Object>> events = parked.get(5, TimeUnit.SECONDS);
        Map<String, Object> evt = events.get(0);
        assertThat(evt.get("operationType")).isEqualTo("lock_released");

        assertThat(evt.get("clusterTime"))
            .as("the synthetic event must get the same wire conversion as real events")
            .isInstanceOf(MongoTimestamp.class);
        MongoTimestamp ts = (MongoTimestamp) evt.get("clusterTime");
        assertThat((long) ts.getTime())
            .as("seconds part must be wall-clock seconds: " + ts)
            .isBetween(System.currentTimeMillis() / 1000L - 60, System.currentTimeMillis() / 1000L + 1);

        // The increment is derived from the sequence in the resume token, so the same event
        // always carries the same clusterTime, whichever cursor delivers it.
        long tokenSequence = Long.parseUnsignedLong((String) ((Map<?, ?>) evt.get("_id")).get("_data"), 16);
        assertThat((long) ts.getInc()).isEqualTo(tokenSequence & 0xFFFFFFFFL);
    }
}
