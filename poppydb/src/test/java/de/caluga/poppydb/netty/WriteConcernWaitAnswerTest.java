package de.caluga.poppydb.netty;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import de.caluga.morphium.driver.wireprotocol.OpMsg;
import de.caluga.poppydb.ReplicationCoordinator;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.embedded.EmbeddedChannel;

/**
 * What {@code postWrite} answers for a w&gt;1 write that cannot be acknowledged (#529). Every
 * outcome keeps {@code ok: 1}: the write WAS executed locally, so the answer is a failed write
 * concern, never a top-level not-primary error a driver would re-send.
 *
 * <ul>
 *   <li>a timeout (or no secondaries) is {@code writeConcernError} code 64 (WriteConcernFailed) as before,</li>
 *   <li>a step-down while waiting is code 189 (PrimarySteppedDown) - distinguishable from a
 *       timeout, and answered at once instead of at the end of the timeout.</li>
 * </ul>
 *
 * <p>The EmbeddedChannel is only ever touched from the test thread: the end-to-end cases complete
 * the wait from here (a report, {@code close()}), never from the coordinator's timer thread. The
 * timeout outcome is pinned through the mapping itself.
 */
public class WriteConcernWaitAnswerTest {

    private InMemoryDriver drv;
    private EmbeddedChannel ch;
    private MongoCommandHandler handler;
    private ReplicationCoordinator coordinator;
    private final AtomicInteger msgId = new AtomicInteger(1);

    @BeforeEach
    public void setUp() throws Exception {
        drv = new InMemoryDriver();
        drv.connect();
        drv.setServerMode(true);
        coordinator = new ReplicationCoordinator(3, 500);
        handler = new MongoCommandHandler(drv, null, null, null, msgId,
                "localhost", 27017, "rs0",
                List.of("localhost:27017", "localhost:27018", "localhost:27019"),
                true, "localhost:27017", 0, () -> coordinator);
        ch = new EmbeddedChannel(handler);
    }

    @AfterEach
    public void tearDown() {
        if (coordinator != null) {
            coordinator.close();
        }
        if (ch != null) {
            ch.finishAndReleaseAll();
        }
        if (drv != null) {
            drv.close();
        }
    }

    private ChannelHandlerContext ctx() {
        ChannelHandlerContext ctx = ch.pipeline().context(handler);
        assertNotNull(ctx, "the handler must be in the channel pipeline");
        return ctx;
    }

    /** The reply the channel produced; everything runs on the test thread. */
    private Map<String, Object> reply() {
        ch.runPendingTasks();
        OpMsg out = ch.readOutbound();
        assertNotNull(out, "the wait was completed on this thread - its reply must be queued");
        return out.getFirstDoc();
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> writeConcernError(Map<String, Object> reply) {
        return (Map<String, Object>) reply.get("writeConcernError");
    }

    @Test
    public void timeoutAndNoSecondariesAreWriteConcernFailed() {
        for (ReplicationCoordinator.WaitOutcome outcome : List.of(ReplicationCoordinator.WaitOutcome.TIMEOUT,
                ReplicationCoordinator.WaitOutcome.NO_SECONDARIES)) {
            Map<String, Object> answer = Doc.of("ok", 1.0, "n", 1);
            MongoCommandHandler.addWriteConcernOutcome(answer, outcome);

            assertEquals(1.0, answer.get("ok"), "the write itself succeeded - only the concern failed");
            Map<String, Object> wce = writeConcernError(answer);
            assertNotNull(wce, outcome + " must be reported");
            assertEquals(64, wce.get("code"));
            assertEquals("WriteConcernFailed", wce.get("codeName"));
        }
    }

    @Test
    public void acknowledgedAddsNothing() {
        Map<String, Object> answer = Doc.of("ok", 1.0, "n", 1);
        MongoCommandHandler.addWriteConcernOutcome(answer, ReplicationCoordinator.WaitOutcome.ACKNOWLEDGED);
        assertNull(answer.get("writeConcernError"));
    }

    @Test
    @Timeout(30)
    public void acknowledgedWriteIsAnsweredWithoutError() {
        // Live, but behind any sequence the write can have (nothing was written, it is 0).
        coordinator.reportProgress("secondary-1:27017", -1);

        boolean async = handler.postWrite(ctx(),
                Doc.of("$db", "wc_test", "insert", "docs", "writeConcern", Doc.of("w", 2, "wtimeout", 60_000)),
                "insert", Doc.of("ok", 1.0, "n", 1), 41);
        assertTrue(async, "a w>1 write on a current primary with a coordinator must be answered asynchronously");
        assertNull(ch.readOutbound(), "the write is not acknowledged yet");

        coordinator.reportProgress("secondary-1:27017", drv.getChangeStreamSequence() + 1);

        Map<String, Object> reply = reply();
        assertEquals(1.0, ((Number) reply.get("ok")).doubleValue());
        assertNull(reply.get("writeConcernError"), "an acknowledged write carries no error: " + reply);
    }

    @Test
    @Timeout(30)
    public void stepDownIsPrimarySteppedDownAndAnsweredAtOnce() {
        coordinator.reportProgress("secondary-1:27017", 1);

        boolean async = handler.postWrite(ctx(),
                Doc.of("$db", "wc_test", "insert", "docs", "writeConcern", Doc.of("w", 3, "wtimeout", 60_000)),
                "insert", Doc.of("ok", 1.0, "n", 1), 43);
        assertTrue(async);
        assertNull(ch.readOutbound(), "the write is waiting for its concern");

        // The node stops being primary while the write waits - PoppyDB closes the coordinator.
        coordinator.close();

        Map<String, Object> reply = reply();
        assertEquals(1.0, ((Number) reply.get("ok")).doubleValue(),
                "the write WAS executed - a top-level not-primary error would make a driver re-send it: "
                        + reply);
        Map<String, Object> wce = writeConcernError(reply);
        assertNotNull(wce, "a stepped-down wait must be reported: " + reply);
        assertEquals(189, ((Number) wce.get("code")).intValue());
        assertEquals("PrimarySteppedDown", wce.get("codeName"));
        assertNull(reply.get("code"), "the answer must not carry a top-level error code: " + reply);
    }
}
