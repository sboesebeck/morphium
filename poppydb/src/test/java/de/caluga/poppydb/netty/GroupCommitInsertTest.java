package de.caluga.poppydb.netty;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import de.caluga.morphium.driver.wireprotocol.OpMsg;
import de.caluga.poppydb.messaging.MessagingOptimizer;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.embedded.EmbeddedChannel;

/**
 * Server-side group commit (#276): bursts of concurrent single-document insert commands must
 * be merged into one driver-level multi-insert, while every member keeps its own reply,
 * writeErrors and request id - indistinguishable from the inline path on the wire.
 *
 * <p>Netty's {@link EmbeddedChannel} fires {@code channelReadComplete} after EVERY inbound
 * message, while a real event loop fires it once per read cycle after all decoded messages.
 * A per-message drain can therefore never demonstrate grouping, so the grouping tests drive
 * the package-private queue path ({@code tryGroupCommitInsert} + {@code drainGroupedInserts})
 * directly: enqueue a burst, assert nothing was answered yet, drain once, assert one merged
 * driver call and per-member replies. The deferral EXCLUSIONS (multi-doc, transaction) are
 * exercised through the real dispatch path, which must answer inline.
 */
public class GroupCommitInsertTest {

    private static final String DB = "gctest";
    private static final String COLL = "msgs";

    private CountingDriver drv;
    private WatchCursorManager cursorManager;
    private MessagingOptimizer optimizer;
    private EmbeddedChannel ch;
    private MongoCommandHandler handler;
    private ChannelHandlerContext handlerCtx;
    private final AtomicInteger msgId = new AtomicInteger(1);

    @BeforeEach
    public void setUp() {
        MongoCommandHandler.setGroupCommitEnabledForTests(true);
        drv = new CountingDriver();
        drv.connect();
        drv.setServerMode(true);
        cursorManager = new WatchCursorManager();
        optimizer = new MessagingOptimizer(drv);
        optimizer.setWatchCursorManager(cursorManager);
        handler = new MongoCommandHandler(drv, cursorManager, new FindCursorRegistry(),
                optimizer, msgId, "0.0.0.0", 27017, "my-rs", List.of("localhost:27017"), true,
                "localhost:27017", 0, () -> null);
        ch = new EmbeddedChannel(handler);
        handlerCtx = ch.pipeline().context(handler);
    }

    @AfterEach
    public void tearDown() {
        MongoCommandHandler.setGroupCommitEnabledForTests(false);
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

    /** Counts driver-level insert batches; one entry per merged (or inline) call. */
    private static class CountingDriver extends InMemoryDriver {
        final List<List<Map<String, Object>>> insertBatches = new ArrayList<>();

        @Override
        public List<Map<String, Object>> insert(String db, String collection, List<Map<String, Object>> objs,
                                                Map<String, Object> wc, boolean ordered)
                throws MorphiumDriverException {
            insertBatches.add(new ArrayList<>(objs));
            return super.insert(db, collection, objs, wc, ordered);
        }
    }

    private void enqueueInsert(String collection, Object id) {
        Map<String, Object> doc = Doc.of("insert", collection,
                "documents", List.of(Doc.of("_id", id)),
                "$db", DB);
        assertThat(handler.tryGroupCommitInsert(handlerCtx, doc, msgId.incrementAndGet()))
                .as("single-doc insert in " + collection + " must be deferred")
                .isTrue();
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> readReply() {
        OpMsg reply = ch.readOutbound();
        assertThat(reply).as("a reply must have been written").isNotNull();
        return (Map<String, Object>) reply.getFirstDoc();
    }

    private void sendInbound(OpMsg msg) {
        ch.writeInbound(msg);
    }

    @Test
    public void burstOfSingleDocInsertsIsGroupedIntoOneWave() {
        for (int i = 1; i <= 8; i++) {
            enqueueInsert(COLL, i);
        }

        assertThat(ch.readOutbound() == null).as("deferred inserts must not answer before the drain")
                .isTrue();
        assertThat(drv.insertBatches).as("nothing must hit the driver before the drain").isEmpty();

        handler.drainGroupedInserts(handlerCtx);

        assertThat(drv.insertBatches).as("the whole burst must be one merged insert")
                .hasSize(1);
        assertThat(drv.insertBatches.get(0)).as("all 8 documents in the merged batch")
                .hasSize(8);
        for (int i = 1; i <= 8; i++) {
            Map<String, Object> reply = readReply();
            assertThat(reply.get("ok")).isEqualTo(1.0);
            assertThat(reply.get("n")).isEqualTo(1);
        }
    }

    @Test
    public void groupedMembersKeepTheirOwnWriteErrors() {
        drv.store(DB, COLL, List.of(Doc.of("_id", "dup")), null);

        enqueueInsert(COLL, "dup");
        enqueueInsert(COLL, "fresh-1");
        enqueueInsert(COLL, "fresh-2");
        handler.drainGroupedInserts(handlerCtx);

        assertThat(drv.insertBatches).as("one merged wave").hasSize(1);

        Map<String, Object> dup = readReply();
        assertThat(dup.get("n")).isEqualTo(0);
        List<Map<String, Object>> errors = (List<Map<String, Object>>) dup.get("writeErrors");
        assertThat(errors).as("the duplicate member must report its own error").hasSize(1);
        assertThat(errors.get(0).get("code")).isEqualTo(11000);
        assertThat(errors.get(0).get("index")).as("error index relative to the member")
                .isEqualTo(0);

        assertThat(readReply().get("n")).as("the following members must not be affected")
                .isEqualTo(1);
        assertThat(readReply().get("n")).isEqualTo(1);
    }

    @Test
    public void collectionBoundarySplitsWaves() {
        enqueueInsert(COLL, 1);
        enqueueInsert(COLL, 2);
        enqueueInsert("other", 1);
        enqueueInsert("other", 2);
        handler.drainGroupedInserts(handlerCtx);

        assertThat(drv.insertBatches).as("one wave per collection")
                .hasSize(2);
        assertThat(drv.insertBatches.get(0)).extracting(doc -> doc.get("_id")).containsExactly(1, 2);
        assertThat(drv.insertBatches.get(1)).extracting(doc -> doc.get("_id")).containsExactly(1, 2);
        for (int i = 0; i < 4; i++) {
            assertThat(readReply().get("ok")).isEqualTo(1.0);
        }
    }

    @Test
    public void repliesArriveInRequestOrder() {
        for (int i = 1; i <= 5; i++) {
            Map<String, Object> doc = Doc.of("insert", COLL,
                    "documents", List.of(Doc.of("_id", i)),
                    "$db", DB);
            assertThat(handler.tryGroupCommitInsert(handlerCtx, doc, msgId.incrementAndGet())).isTrue();
        }
        handler.drainGroupedInserts(handlerCtx);

        for (int i = 1; i <= 5; i++) {
            OpMsg reply = ch.readOutbound();
            assertThat(reply.getResponseTo())
                    .as("reply order must match request order")
                    .isEqualTo(i + 1);
        }
    }

    @Test
    public void multiDocInsertIsNotDeferred() {
        OpMsg msg = new OpMsg();
        msg.setMessageId(msgId.incrementAndGet());
        msg.setFirstDoc(Doc.of("insert", COLL,
                "documents", List.of(Doc.of("_id", 1), Doc.of("_id", 2)),
                "$db", DB));
        sendInbound(msg);

        assertThat(ch.readOutbound() == null).as("a multi-doc batch is already batching - answered inline")
                .isFalse();
        assertThat(drv.insertBatches).as("one inline driver call, never merged").hasSize(1);
        assertThat(drv.insertBatches.get(0)).hasSize(2);
    }

    @Test
    public void transactionInsertsAreNotDeferred() {
        Map<String, Object> lsid = Doc.of("id", "session-1");
        OpMsg start = new OpMsg();
        start.setMessageId(msgId.incrementAndGet());
        Map<String, Object> startDoc = Doc.of("insert", COLL, "documents", List.of(Doc.of("_id", "tx-1")),
                "$db", DB, "startTransaction", true, "txnNumber", 1L, "autocommit", false);
        startDoc.put("lsid", lsid);
        start.setFirstDoc(startDoc);
        sendInbound(start);
        assertThat(ch.readOutbound() == null).as("transaction member must be answered inline")
                .isFalse();

        OpMsg join = new OpMsg();
        join.setMessageId(msgId.incrementAndGet());
        Map<String, Object> joinDoc = Doc.of("insert", COLL, "documents", List.of(Doc.of("_id", "tx-2")),
                "$db", DB, "txnNumber", 1L, "autocommit", false);
        joinDoc.put("lsid", lsid);
        join.setFirstDoc(joinDoc);
        sendInbound(join);
        assertThat(ch.readOutbound() == null).as("transaction member must be answered inline")
                .isFalse();

        assertThat(drv.insertBatches).as("each transaction insert goes to the driver separately")
                .hasSize(2);

        OpMsg abort = new OpMsg();
        abort.setMessageId(msgId.incrementAndGet());
        abort.setFirstDoc(Doc.of("abortTransaction", 1, "$db", DB, "lsid", lsid));
        sendInbound(abort);
    }

    @Test
    public void singleInsertThroughRealDispatchIsAnsweredByTheDrain() {
        // EmbeddedChannel fires channelReadComplete per message, so a real dispatch of ONE
        // insert is enqueued and answered by the readComplete drain - proving the deferral
        // integrates end to end (no double reply from the inline path).
        OpMsg msg = new OpMsg();
        msg.setMessageId(msgId.incrementAndGet());
        msg.setFirstDoc(Doc.of("insert", COLL,
                "documents", List.of(Doc.of("_id", "wire-1")),
                "$db", DB));
        sendInbound(msg);

        assertThat(drv.insertBatches).as("one merged driver call, not the inline path")
                .hasSize(1);
        Map<String, Object> reply = readReply();
        assertThat(reply.get("ok")).isEqualTo(1.0);
        assertThat(reply.get("n")).isEqualTo(1);
    }
}