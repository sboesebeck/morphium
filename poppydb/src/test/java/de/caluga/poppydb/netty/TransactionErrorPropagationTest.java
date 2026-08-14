package de.caluga.poppydb.netty;

import de.caluga.morphium.driver.MorphiumTransactionContext;
import de.caluga.morphium.driver.inmem.InMemTransactionContext;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.AttributeKey;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * commitTransaction/abortTransaction answered ok:1 unconditionally - a commit that threw was
 * only logged, the client believed its transaction was committed. Failures must surface as a
 * mongo-shaped error response.
 */
public class TransactionErrorPropagationTest {

    private static final AttributeKey<MorphiumTransactionContext> TX_KEY = AttributeKey.valueOf("txContext");

    private InMemoryDriver drv;
    private MongoCommandHandler handler;
    private EmbeddedChannel channel;
    private ChannelHandlerContext hctx;

    @BeforeEach
    public void setup() throws Exception {
        drv = new InMemoryDriver();
        drv.connect();
        handler = new MongoCommandHandler(drv, null, null, null, new AtomicInteger(1),
                "localhost", 17017, "rs0", List.of("localhost:17017"), true, "localhost:17017",
                0, () -> null);
        channel = new EmbeddedChannel(handler);
        hctx = channel.pipeline().context(MongoCommandHandler.class);
    }

    @AfterEach
    public void tearDown() {
        channel.finishAndReleaseAll();
        if (drv != null) {
            drv.close();
        }
    }

    @Test
    public void commitFailureIsReportedToTheClient() {
        // A touched-collections key without the db/collection separator makes the commit's
        // merge loop throw - stands in for any internal commit failure.
        InMemTransactionContext poisoned = new InMemTransactionContext();
        poisoned.setDatabase(new HashMap<>());
        poisoned.getTouchedCollections().add("no-separator-key");
        channel.attr(TX_KEY).set(poisoned);

        Map<String, Object> answer = handler.handleCommitTransaction(hctx);

        assertEquals(0.0, ((Number) answer.get("ok")).doubleValue(),
            "a commit that threw must not be acknowledged with ok:1");
        assertNotNull(answer.get("errmsg"), "the client needs the failure reason");
        assertNotNull(answer.get("code"), "mongo-shaped errors carry a code");
    }

    @Test
    public void successfulCommitStillAnswersOk() {
        MorphiumTransactionContext tx = drv.startTransaction(false);
        channel.attr(TX_KEY).set(tx);

        Map<String, Object> answer = handler.handleCommitTransaction(hctx);

        assertEquals(1.0, ((Number) answer.get("ok")).doubleValue());
    }

    @Test
    public void commitWithoutTransactionStaysLenient() {
        Map<String, Object> answer = handler.handleCommitTransaction(hctx);
        assertEquals(1.0, ((Number) answer.get("ok")).doubleValue(),
            "no-transaction commit stays a lenient no-op (full session state machine is out of scope)");
    }

    @Test
    public void abortWithoutTransactionStaysLenient() {
        Map<String, Object> answer = handler.handleAbortTransaction(hctx);
        assertEquals(1.0, ((Number) answer.get("ok")).doubleValue());
    }
}
