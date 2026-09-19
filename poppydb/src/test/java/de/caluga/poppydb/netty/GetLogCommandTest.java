package de.caluga.poppydb.netty;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import de.caluga.morphium.driver.wireprotocol.OpMsg;
import de.caluga.poppydb.LogRingBuffer;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * #356: {@code getLog} answered an empty list for startupWarnings and "unknown log" for
 * everything else. {@code db.adminCommand({getLog: "global"})} is what you type when you can
 * reach the database but not the host - it now answers from the in-memory ring buffer, in
 * mongod's shape, with {@code "*"} listing the names. Its companions: {@code logRotate} says
 * plainly that rotation is Logback's job, and {@code setParameter: {logLevel: N}} raises a node
 * to DEBUG without a restart.
 */
public class GetLogCommandTest {

    private InMemoryDriver drv;
    private final AtomicInteger msgId = new AtomicInteger(1);
    private Level originalRootLevel;

    @BeforeEach
    public void setup() throws Exception {
        drv = new InMemoryDriver();
        drv.connect();
        originalRootLevel = root().getLevel();
    }

    @AfterEach
    public void tearDown() {
        root().setLevel(originalRootLevel);

        if (drv != null) {
            drv.close();
        }
    }

    private static ch.qos.logback.classic.Logger root() {
        return ((LoggerContext) LoggerFactory.getILoggerFactory()).getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
    }

    private MongoCommandHandler handler(boolean syncing) {
        return new MongoCommandHandler(drv, null, null, null, new AtomicInteger(1),
                "0.0.0.0", 27017, "my-rs", List.of("localhost:27017", "localhost:27018"),
                !syncing, "localhost:27017", 0, () -> null, null, () -> syncing);
    }

    private Map<String, Object> send(EmbeddedChannel ch, Map<String, Object> cmd) {
        OpMsg msg = new OpMsg();
        msg.setMessageId(msgId.incrementAndGet());
        msg.setFirstDoc(cmd);
        ch.writeInbound(msg);
        OpMsg reply = ch.readOutbound();
        assertThat(reply).as("no reply for " + cmd.keySet().iterator().next()).isNotNull();
        return reply.getFirstDoc();
    }

    @SuppressWarnings("unchecked")
    private static List<String> strings(Object o) {
        return (List<String>) o;
    }

    // ---- getLog ---------------------------------------------------------------------------

    @Test
    public void starListsTheAvailableLogs() {
        Map<String, Object> reply = send(new EmbeddedChannel(handler(false)), Doc.of("getLog", "*", "$db", "admin"));

        assertThat(reply.get("ok")).as("reply: " + reply).isEqualTo(1.0);
        assertThat(strings(reply.get("names"))).contains("global", "startupWarnings");
    }

    @Test
    public void globalAnswersTheRingBufferInMongodShape() {
        LogRingBuffer buffer = new LogRingBuffer(16);
        buffer.start();
        ch.qos.logback.classic.Logger logger = root().getLoggerContext().getLogger("getlog.test");

        for (int i = 0; i < 3; i++) {
            buffer.doAppend(new ch.qos.logback.classic.spi.LoggingEvent("fqcn", logger, Level.INFO,
                    "buffered line " + i, null, null));
        }

        EmbeddedChannel ch = new EmbeddedChannel(handler(false).setLogBuffer(buffer));
        Map<String, Object> reply = send(ch, Doc.of("getLog", "global", "$db", "admin"));

        assertThat(reply.get("ok")).as("reply: " + reply).isEqualTo(1.0);
        assertThat(((Number) reply.get("totalLinesWritten")).longValue()).isEqualTo(3);
        List<String> log = strings(reply.get("log"));
        assertThat(log).hasSize(3);
        assertThat(log.get(2)).endsWith("buffered line 2");
    }

    @Test
    public void globalWithoutABufferAnswersAnEmptyLogNotAnError() {
        Map<String, Object> reply = send(new EmbeddedChannel(handler(false)), Doc.of("getLog", "global", "$db", "admin"));

        assertThat(reply.get("ok")).isEqualTo(1.0);
        assertThat(strings(reply.get("log"))).isEmpty();
        assertThat(((Number) reply.get("totalLinesWritten")).longValue()).isZero();
    }

    @Test
    public void startupWarningsCarryTheRealOnes() {
        EmbeddedChannel ch = new EmbeddedChannel(handler(false)
                .setStartupWarningsSupplier(() -> List.of("Access control is not enabled", "no dump directory")));

        Map<String, Object> reply = send(ch, Doc.of("getLog", "startupWarnings", "$db", "admin"));

        assertThat(reply.get("ok")).isEqualTo(1.0);
        assertThat(strings(reply.get("log")))
                .hasSize(2)
                .anySatisfy(l -> assertThat(l).contains("Access control"));
        assertThat(((Number) reply.get("totalLinesWritten")).longValue()).isEqualTo(2);
    }

    @Test
    public void unknownLogNameIsRefusedLikeMongod() {
        Map<String, Object> reply = send(new EmbeddedChannel(handler(false)), Doc.of("getLog", "nope", "$db", "admin"));

        assertThat(reply.get("ok")).isEqualTo(0.0);
        assertThat(reply.get("errmsg").toString()).contains("no RamLog named: nope");
    }

    @Test
    public void nonStringArgumentIsATypeMismatch() {
        Map<String, Object> reply = send(new EmbeddedChannel(handler(false)), Doc.of("getLog", 1, "$db", "admin"));

        assertThat(reply.get("ok")).isEqualTo(0.0);
        assertThat(((Number) reply.get("code")).intValue()).as("reply: " + reply).isEqualTo(14);
    }

    @Test
    public void getLogWorksOnARecoveringNode() {
        Map<String, Object> reply = send(new EmbeddedChannel(handler(true)), Doc.of("getLog", "*", "$db", "admin"));

        assertThat(reply.get("ok")).as("diagnostics must survive RECOVERING: " + reply).isEqualTo(1.0);
    }

    // ---- logRotate ------------------------------------------------------------------------

    @Test
    public void logRotateSaysWhoRotates() {
        Map<String, Object> reply = send(new EmbeddedChannel(handler(false)), Doc.of("logRotate", 1, "$db", "admin"));

        assertThat(reply.get("ok")).isEqualTo(0.0);
        assertThat(((Number) reply.get("code")).intValue()).as("reply: " + reply).isEqualTo(115);
        assertThat(reply.get("errmsg").toString()).contains("Logback");
    }

    // ---- getParameter / setParameter logLevel ---------------------------------------------

    @Test
    public void logLevelCanBeRaisedAndReadBackAtRuntime() {
        EmbeddedChannel ch = new EmbeddedChannel(handler(false));
        root().setLevel(Level.INFO);

        Map<String, Object> before = send(ch, Doc.of("getParameter", 1, "logLevel", 1, "$db", "admin"));
        assertThat(before.get("ok")).as("reply: " + before).isEqualTo(1.0);
        assertThat(((Number) before.get("logLevel")).intValue()).isEqualTo(0);

        Map<String, Object> set = send(ch, Doc.of("setParameter", 1, "logLevel", 2, "$db", "admin"));
        assertThat(set.get("ok")).as("reply: " + set).isEqualTo(1.0);
        assertThat(((Number) set.get("was")).intValue()).as("mongod reports the previous value as 'was'").isEqualTo(0);
        assertThat(root().getLevel()).as("the root logger really moved").isEqualTo(Level.DEBUG);

        Map<String, Object> after = send(ch, Doc.of("getParameter", 1, "logLevel", 1, "$db", "admin"));
        assertThat(((Number) after.get("logLevel")).intValue()).isEqualTo(1);
    }

    @Test
    public void logLevelOutOfRangeIsRejectedWithoutChangingAnything() {
        root().setLevel(Level.INFO);
        Map<String, Object> reply = send(new EmbeddedChannel(handler(false)),
                Doc.of("setParameter", 1, "logLevel", 9, "$db", "admin"));

        assertThat(reply.get("ok")).isEqualTo(0.0);
        assertThat(((Number) reply.get("code")).intValue()).as("BadValue: " + reply).isEqualTo(2);
        assertThat(root().getLevel()).isEqualTo(Level.INFO);
    }

    @Test
    public void unknownParametersAreRefusedByName() {
        EmbeddedChannel ch = new EmbeddedChannel(handler(false));

        Map<String, Object> set = send(ch, Doc.of("setParameter", 1, "syncdelay", 30, "$db", "admin"));
        assertThat(set.get("ok")).isEqualTo(0.0);
        assertThat(set.get("errmsg").toString()).contains("syncdelay");

        Map<String, Object> get = send(ch, Doc.of("getParameter", 1, "syncdelay", 1, "$db", "admin"));
        assertThat(get.get("ok")).isEqualTo(0.0);
        assertThat(get.get("errmsg").toString()).contains("syncdelay");
    }

    @Test
    public void getParameterStarListsEverythingItKnows() {
        Map<String, Object> reply = send(new EmbeddedChannel(handler(false)),
                Doc.of("getParameter", "*", "$db", "admin"));

        assertThat(reply.get("ok")).as("reply: " + reply).isEqualTo(1.0);
        assertThat(reply).containsKeys("featureCompatibilityVersion", "logLevel");
    }

    @Test
    public void featureCompatibilityVersionStillAnswers() {
        Map<String, Object> reply = send(new EmbeddedChannel(handler(false)),
                Doc.of("getParameter", 1, "featureCompatibilityVersion", 1, "$db", "admin"));

        assertThat(reply.get("ok")).as("negative control for the rewrite: " + reply).isEqualTo(1.0);
        assertThat(reply.get("featureCompatibilityVersion")).isNotNull();
    }
}
