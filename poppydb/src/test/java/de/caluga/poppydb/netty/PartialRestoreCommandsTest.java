package de.caluga.poppydb.netty;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import de.caluga.morphium.driver.wireprotocol.OpMsg;
import de.caluga.poppydb.election.ElectionConfig;
import de.caluga.poppydb.election.ElectionManager;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * #391: the wire side of the cluster-wide partial restore - the {@code poppyRestoreStatus}
 * probe peers answer with their finding, and the operator's {@code poppyAcceptPartialRestore}.
 */
@Tag("poppydb")
public class PartialRestoreCommandsTest {

    private static final List<String> FILES = List.of("db_broken.morphium.gz");

    private InMemoryDriver drv;
    private ElectionManager election;
    private EmbeddedChannel ch;
    private final AtomicInteger msgId = new AtomicInteger(1);

    @BeforeEach
    public void setup() throws Exception {
        drv = new InMemoryDriver();
        drv.connect();
        election = new ElectionManager("localhost:27017", List.of("localhost:27017", "localhost:27018"),
                new ElectionConfig().setElectionPriority(80));
        election.setDataComplete(false);
        election.setFailedRestoreFiles(FILES);
        ch = new EmbeddedChannel(new MongoCommandHandler(drv, null, null, null, new AtomicInteger(1),
                "0.0.0.0", 27017, "my-rs", List.of("localhost:27017", "localhost:27018"), false,
                null, 0, () -> null, election).setOpRegistry(new OpRegistry()));
    }

    @AfterEach
    public void tearDown() {
        if (drv != null) {
            drv.close();
        }
    }

    private Map<String, Object> send(Map<String, Object> cmd) {
        OpMsg msg = new OpMsg();
        msg.setMessageId(msgId.incrementAndGet());
        msg.setFirstDoc(cmd);
        ch.writeInbound(msg);
        OpMsg reply = ch.readOutbound();
        assertThat(reply).as("no reply for " + cmd.keySet().iterator().next()).isNotNull();
        return reply.getFirstDoc();
    }

    @Test
    public void restoreStatusAnswersWithTheNodesFinding() {
        Map<String, Object> reply = send(Doc.of("poppyRestoreStatus", 1, "senderId", "localhost:27018", "$db", "admin"));

        assertThat(reply.get("ok")).as("reply: " + reply).isEqualTo(1.0);
        assertThat(reply.get("nodeId")).isEqualTo("localhost:27017");
        assertThat(reply.get("dataComplete")).isEqualTo(false);
        assertThat(reply.get("failedFiles")).isEqualTo(FILES);
        assertThat(reply.get("priority")).isEqualTo(80);
        assertThat(reply.get("leaderKnown")).isEqualTo(false);
    }

    @Test
    public void acceptPartialRestoreLiftsTheGuardAndReportsTheFiles() {
        Map<String, Object> reply = send(Doc.of("poppyAcceptPartialRestore", 1, "$db", "admin"));

        assertThat(reply.get("ok")).as("reply: " + reply).isEqualTo(1.0);
        assertThat(reply.get("lifted")).isEqualTo(true);
        assertThat(reply.get("failedFiles")).isEqualTo(FILES);
        assertThat(reply.get("dataComplete")).isEqualTo(true);
        assertThat(election.isDataComplete()).isTrue();

        // idempotent: nothing left to lift
        Map<String, Object> again = send(Doc.of("poppyAcceptPartialRestore", 1, "$db", "admin"));
        assertThat(again.get("ok")).isEqualTo(1.0);
        assertThat(again.get("lifted")).isEqualTo(false);
        assertThat(again.get("dataComplete")).isEqualTo(true);
    }

    @Test
    public void bothCommandsAreAdvertised() {
        Map<String, Object> reply = send(Doc.of("listCommands", 1, "$db", "admin"));

        @SuppressWarnings("unchecked")
        Map<String, Object> commands = (Map<String, Object>) reply.get("commands");
        assertThat(commands).containsKeys("poppyRestoreStatus", "poppyAcceptPartialRestore");
    }

    @Test
    public void withoutElectionBothCommandsAreRefused() throws Exception {
        try (InMemoryDriver standalone = new InMemoryDriver()) {
            standalone.connect();
            EmbeddedChannel plain = new EmbeddedChannel(new MongoCommandHandler(standalone, null, null, null,
                    new AtomicInteger(1), "0.0.0.0", 27017, "", List.of("localhost:27017"), true,
                    "localhost:27017", 0, () -> null).setOpRegistry(new OpRegistry()));
            for (String cmd : List.of("poppyRestoreStatus", "poppyAcceptPartialRestore")) {
                OpMsg msg = new OpMsg();
                msg.setMessageId(msgId.incrementAndGet());
                msg.setFirstDoc(Doc.of(cmd, 1, "$db", "admin"));
                plain.writeInbound(msg);
                OpMsg reply = plain.readOutbound();
                assertThat(reply.getFirstDoc().get("ok")).as(cmd).isEqualTo(0.0);
                assertThat(reply.getFirstDoc().get("code")).as(cmd).isEqualTo(76);
            }
        }
    }
}
