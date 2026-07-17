package de.caluga.test.poppydb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.commands.InsertMongoCommand;
import de.caluga.morphium.driver.wire.MongoConnection;
import de.caluga.morphium.driver.wire.SingleMongoConnectDriver;
import de.caluga.morphium.driver.wireprotocol.OpMsg;
import de.caluga.morphium.driver.wireprotocol.WireProtocolMessage;
import de.caluga.poppydb.PoppyDB;
import de.caluga.poppydb.netty.MongoCommandHandler;

/**
 * Verifies that server-side find cursors are cleaned up when a client disconnects
 * without exhausting the cursor. Regression test for the leak where {@code findCursors}
 * entries survived {@code channelInactive}.
 *
 * The find is issued over a raw socket (not the driver) on purpose: the high-level
 * driver auto-drains a batched cursor via getMore/killCursors, which would clean the
 * server cursor up before we can observe the leak. A raw OP_MSG lets us read exactly
 * one reply and then drop the connection with the cursor still populated.
 */
@Tag("server")
@Disabled("Disabled by default - runs local PoppyDB which is flaky with parallel tests. Run manually or with --include-tags server")
public class CursorLifecycleTest {
    private static final Logger log = LoggerFactory.getLogger(CursorLifecycleTest.class);
    private static final AtomicInteger PORT = new AtomicInteger(18500);

    private int nextPort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        } catch (Exception e) {
            return PORT.incrementAndGet();
        }
    }

    private void startServer(PoppyDB srv, int port) throws Exception {
        srv.start();
        long deadline = System.currentTimeMillis() + 10_000;
        while (true) {
            try (Socket s = new Socket()) {
                s.connect(new InetSocketAddress("localhost", port), 250);
                return;
            } catch (Exception e) {
                if (System.currentTimeMillis() > deadline) {
                    throw e;
                }
                Thread.sleep(50);
            }
        }
    }

    @Test
    public void findCursorRemovedOnDisconnect() throws Exception {
        int port = nextPort();
        var srv = new PoppyDB(port, "localhost", 20, 60);
        startServer(srv, port);

        int baseline = MongoCommandHandler.openFindCursors();

        // Insert 10 documents via the driver, then close it. The data lives in the
        // server's shared in-memory database independent of the connection.
        SingleMongoConnectDriver drv = new SingleMongoConnectDriver();
        drv.setHostSeed("localhost:" + port);
        drv.setMaxConnections(5);
        drv.setHeartbeatFrequency(1000);
        drv.setMaxWaitTime(0);
        drv.connect();
        try {
            MongoConnection con = drv.getConnection();
            List<Map<String, Object>> docs = new ArrayList<>();
            for (int i = 0; i < 10; i++) {
                docs.add(Doc.of("_id", i, "value", "v" + i));
            }
            new InsertMongoCommand(con).setDb("test").setColl("cursorleak").setDocuments(docs).execute();
        } finally {
            drv.close();
        }

        // Raw find with batchSize:1 -> server keeps a cursor with the remaining 9 docs.
        Socket raw = new Socket();
        raw.connect(new InetSocketAddress("localhost", port), 2000);
        raw.setSoTimeout(5000);
        try {
            OpMsg find = new OpMsg();
            find.setMessageId(42);
            find.setFlags(0);
            find.setFirstDoc(Doc.of("find", "cursorleak", "filter", Doc.of(),
                    "batchSize", 1, "$db", "test"));
            raw.getOutputStream().write(find.bytes());
            raw.getOutputStream().flush();

            OpMsg reply = (OpMsg) WireProtocolMessage.parseFromStream(raw.getInputStream());
            @SuppressWarnings("unchecked")
            Map<String, Object> cursor = (Map<String, Object>) reply.getFirstDoc().get("cursor");
            assertNotEquals(null, cursor, "find reply must contain a cursor document");
            long cursorId = ((Number) cursor.get("id")).longValue();
            assertNotEquals(0L, cursorId, "batchSize:1 over 10 docs must leave an open server cursor");

            assertEquals(baseline + 1, MongoCommandHandler.openFindCursors(),
                    "server should hold exactly one extra open find cursor after batched find");
        } finally {
            raw.close();
        }

        // Client disconnected without exhausting / killing the cursor; channelInactive
        // must drop the leaked cursor back to baseline.
        long deadline = System.currentTimeMillis() + 5_000;
        while (MongoCommandHandler.openFindCursors() > baseline
                && System.currentTimeMillis() < deadline) {
            Thread.sleep(50);
        }
        assertEquals(baseline, MongoCommandHandler.openFindCursors(),
                "find cursor must be removed on client disconnect");

        srv.shutdown();
    }

    /**
     * Regression/spec test for bounded find-cursor retention (Task 5, B2d). A server-side find
     * cursor must never retain more than {@code maxRetainedBatches × batchSize} documents in
     * memory at once — the rest is re-fetched on demand via getMore (see
     * {@code MongoCommandHandler#refillFindCursorWindow}) instead of being held as one giant
     * pre-fetched remainder. This test drives a 50k-document find with batchSize 10 entirely
     * over getMore and verifies both the memory bound and that paging is exact: every document
     * is returned exactly once, in stable sort order, with no gaps or duplicates at the window
     * boundaries where a re-execution happens.
     */
    @Test
    public void findCursorBoundedRetentionAndFullPaging() throws Exception {
        int port = nextPort();
        var srv = new PoppyDB(port, "localhost", 20, 60);
        startServer(srv, port);

        final int totalDocs = 50_000;
        final int batchSize = 10;
        final int maxRetainedBatches = 10; // mirrors MongoCommandHandler.MAX_RETAINED_BATCHES
        final int maxRetainedDocs = maxRetainedBatches * batchSize;

        // Insert 50k docs with a monotonically increasing "seq" field, in chunks (a single
        // 50k-document OP_MSG insert would be an unnecessarily large single message).
        SingleMongoConnectDriver drv = new SingleMongoConnectDriver();
        drv.setHostSeed("localhost:" + port);
        drv.setMaxConnections(5);
        drv.setHeartbeatFrequency(1000);
        drv.setMaxWaitTime(0);
        drv.connect();
        try {
            MongoConnection con = drv.getConnection();
            int chunk = 2000;
            for (int start = 0; start < totalDocs; start += chunk) {
                int end = Math.min(start + chunk, totalDocs);
                List<Map<String, Object>> docs = new ArrayList<>();
                for (int i = start; i < end; i++) {
                    docs.add(Doc.of("_id", i, "seq", i));
                }
                new InsertMongoCommand(con).setDb("test").setColl("cursorpaging").setDocuments(docs).execute();
            }
        } finally {
            drv.close();
        }

        Socket raw = new Socket();
        raw.connect(new InetSocketAddress("localhost", port), 2000);
        raw.setSoTimeout(20_000);
        AtomicInteger msgId = new AtomicInteger(1000);
        try {
            OpMsg find = new OpMsg();
            find.setMessageId(msgId.incrementAndGet());
            find.setFlags(0);
            find.setFirstDoc(Doc.of("find", "cursorpaging", "filter", Doc.of(), "sort", Doc.of("seq", 1),
                    "batchSize", batchSize, "$db", "test"));
            raw.getOutputStream().write(find.bytes());
            raw.getOutputStream().flush();

            OpMsg reply = (OpMsg) WireProtocolMessage.parseFromStream(raw.getInputStream());
            @SuppressWarnings("unchecked")
            Map<String, Object> cursor = (Map<String, Object>) reply.getFirstDoc().get("cursor");
            assertNotEquals(null, cursor, "find reply must contain a cursor document");
            long cursorId = ((Number) cursor.get("id")).longValue();
            assertNotEquals(0L, cursorId, "batchSize:10 over 50k docs must leave an open server cursor");

            @SuppressWarnings("unchecked")
            List<Map<String, Object>> firstBatch = (List<Map<String, Object>>) cursor.get("firstBatch");
            assertEquals(batchSize, firstBatch.size(), "firstBatch must contain exactly batchSize documents");

            List<Integer> seenSeq = new ArrayList<>();
            for (Map<String, Object> d : firstBatch) {
                seenSeq.add(((Number) d.get("seq")).intValue());
            }

            int retained = MongoCommandHandler.retainedFindCursorDocs(cursorId);
            assertTrue(retained >= 0 && retained <= maxRetainedDocs,
                    "cursor must retain at most maxRetainedBatches*batchSize=" + maxRetainedDocs
                            + " docs right after find, was " + retained);

            while (cursorId != 0L) {
                OpMsg getMore = new OpMsg();
                getMore.setMessageId(msgId.incrementAndGet());
                getMore.setFlags(0);
                getMore.setFirstDoc(Doc.of("getMore", cursorId, "collection", "cursorpaging", "$db", "test"));
                raw.getOutputStream().write(getMore.bytes());
                raw.getOutputStream().flush();

                OpMsg gmReply = (OpMsg) WireProtocolMessage.parseFromStream(raw.getInputStream());
                @SuppressWarnings("unchecked")
                Map<String, Object> gmCursor = (Map<String, Object>) gmReply.getFirstDoc().get("cursor");
                assertNotEquals(null, gmCursor, "getMore reply must contain a cursor document");
                @SuppressWarnings("unchecked")
                List<Map<String, Object>> nextBatch = (List<Map<String, Object>>) gmCursor.get("nextBatch");
                for (Map<String, Object> d : nextBatch) {
                    seenSeq.add(((Number) d.get("seq")).intValue());
                }

                cursorId = ((Number) gmCursor.get("id")).longValue();
                if (cursorId != 0L) {
                    int r = MongoCommandHandler.retainedFindCursorDocs(cursorId);
                    assertTrue(r >= 0 && r <= maxRetainedDocs,
                            "cursor must retain at most maxRetainedBatches*batchSize=" + maxRetainedDocs
                                    + " docs at all times, was " + r);
                }
            }

            assertEquals(totalDocs, seenSeq.size(), "getMore must page through every document exactly once");
            assertEquals(totalDocs, new HashSet<>(seenSeq).size(), "no duplicate documents across the paged cursor");
            for (int i = 0; i < totalDocs; i++) {
                assertEquals(i, seenSeq.get(i).intValue(),
                        "documents must be returned in stable sort order across window re-executions (mismatch at index " + i + ")");
            }
        } finally {
            raw.close();
        }

        srv.shutdown();
    }
}
