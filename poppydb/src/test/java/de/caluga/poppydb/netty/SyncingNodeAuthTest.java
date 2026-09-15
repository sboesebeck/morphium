package de.caluga.poppydb.netty;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import de.caluga.morphium.driver.wireprotocol.OpMsg;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Authentication is control plane and must reach a node that refuses data-plane traffic.
 *
 * <p>With {@code --auth} the election client SCRAM-authenticates against its peers before any
 * leader exists, and since #371 every member with peers starts out answering data-plane commands
 * with 13436 until its first sync completes or it leads. {@code saslStart} used to go through that
 * same guard: no member could authenticate against any other, so no vote could be requested, so
 * no leader was ever elected - the RS-internal auth bootstrap deadlocked on the very state that
 * was meant to keep clients honest. {@code RsInternalAuthTlsTest} hung on it; this is the fast,
 * handler-level version of the same contract.
 */
@Tag("server")
public class SyncingNodeAuthTest {

    private InMemoryDriver drv;
    private final AtomicInteger msgId = new AtomicInteger(1);

    @BeforeEach
    public void setup() throws Exception {
        drv = new InMemoryDriver();
        drv.connect();
    }

    @AfterEach
    public void tearDown() {
        if (drv != null) {
            drv.close();
        }
    }

    /** One command against a handler whose node reports "not serving data-plane traffic". */
    private Map<String, Object> onSyncingNode(Map<String, Object> cmd) throws Exception {
        MongoCommandHandler h = new MongoCommandHandler(drv, null, null, null, new AtomicInteger(1),
                "0.0.0.0", 27018, "my-rs", List.of("localhost:27017", "localhost:27018"),
                false, "localhost:27017", 0, () -> null, null, () -> true);
        EmbeddedChannel ch = new EmbeddedChannel(h);
        OpMsg msg = new OpMsg();
        msg.setMessageId(msgId.incrementAndGet());
        msg.setFirstDoc(cmd);
        ch.writeInbound(msg);

        // The generic command path answers from an executor; give the embedded loop a chance
        // to run the write back onto the channel.
        OpMsg reply = null;
        for (int i = 0; i < 100 && reply == null; i++) {
            ch.runPendingTasks();
            reply = ch.readOutbound();
            if (reply == null) {
                Thread.sleep(50);
            }
        }

        assertThat(reply).as("no reply for " + cmd.keySet().iterator().next()).isNotNull();
        return reply.getFirstDoc();
    }

    @Test
    public void saslStartIsNotRefusedAsRecovering() throws Exception {
        // SCRAM-SHA-256 client-first message for a user that does not exist: the handshake is
        // expected to fail on the credentials, never on the node's replication state.
        byte[] clientFirst = "n,,n=nobody,r=Y2xpZW50bm9uY2U=".getBytes(StandardCharsets.UTF_8);
        Map<String, Object> reply = onSyncingNode(Doc.of(
                "saslStart", 1, "mechanism", "SCRAM-SHA-256", "payload", clientFirst,
                "$db", "admin"));

        assertThat(reply.get("code"))
                .as("authentication must reach a node that refuses data-plane traffic: " + reply)
                .isNotEqualTo(13436);
    }

    @Test
    public void aDataPlaneReadIsStillRefused() throws Exception {
        Map<String, Object> reply = onSyncingNode(Doc.of(
                "find", "c", "filter", Doc.of(), "$db", "somedb"));

        assertThat(reply.get("code"))
                .as("negative control: the guard itself is intact")
                .isEqualTo(13436);
    }
}
