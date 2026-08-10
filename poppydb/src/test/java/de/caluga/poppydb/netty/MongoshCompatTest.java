package de.caluga.poppydb.netty;

import de.caluga.morphium.MorphiumVersion;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import de.caluga.morphium.driver.wireprotocol.OpMsg;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Wire-level compatibility with the mongosh connect handshake. mongosh probes
 * {@code atlasVersion} on every connect to detect Atlas and expects the mongod-shaped
 * CommandNotFound error (code 59) for it - not a server-side exception. It also prints
 * "Using MongoDB: &lt;buildInfo.version&gt;"; PoppyDB releases in lockstep with morphium,
 * so every version report (buildInfo, serverStatus, hello msg) must carry the real
 * product version from the Maven build, not a hardcoded placeholder.
 *
 * Uses an EmbeddedChannel around the real MongoCommandHandler, so the full wire dispatch
 * runs against the InMemoryDriver - no network needed.
 */
public class MongoshCompatTest {

    private InMemoryDriver drv;
    private EmbeddedChannel ch;
    private final AtomicInteger msgId = new AtomicInteger(1);

    @BeforeEach
    public void setup() throws Exception {
        drv = new InMemoryDriver();
        drv.connect();
        ch = new EmbeddedChannel(new MongoCommandHandler(drv, null, null, null, new AtomicInteger(1),
                "localhost", 17017, "rs0", List.of("localhost:17017"), true, "localhost:17017",
                0, () -> null));
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
    public void unknownCommandAnsweredWithCommandNotFound() {
        Map<String, Object> reply = send(Doc.of("atlasVersion", 1, "$db", "admin"));

        assertThat(reply.get("ok")).as("reply: " + reply).isEqualTo(0.0);
        assertThat(((Number) reply.get("code")).intValue()).as("reply: " + reply).isEqualTo(59);
        assertThat(reply.get("codeName")).isEqualTo("CommandNotFound");
        assertThat(reply.get("errmsg").toString()).contains("atlasVersion");
    }

    @Test
    public void versionReportsMatchTheProjectVersion() {
        String expected = MorphiumVersion.getVersion();
        assertThat(expected).as("Maven resource filtering must provide the project version")
                .matches("\\d+\\.\\d+\\.\\d+.*");
        assertThat(InMemoryDriver.REPORTED_SERVER_VERSION).isEqualTo(expected);

        Map<String, Object> buildInfo = send(Doc.of("buildInfo", 1, "$db", "admin"));
        assertThat(buildInfo.get("ok")).isEqualTo(1.0);
        assertThat(buildInfo.get("version")).isEqualTo(expected);

        Map<String, Object> serverStatus = send(Doc.of("serverStatus", 1, "$db", "admin"));
        assertThat(serverStatus.get("ok")).isEqualTo(1.0);
        assertThat(serverStatus.get("version")).isEqualTo(expected);

        Map<String, Object> hello = send(Doc.of("hello", 1, "$db", "admin"));
        assertThat(hello.get("msg").toString()).as("hello msg: " + hello.get("msg")).contains(expected);
    }
}
