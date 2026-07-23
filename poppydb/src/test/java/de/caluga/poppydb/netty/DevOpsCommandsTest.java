package de.caluga.poppydb.netty;

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
 * DevOps command surface: currentOp (command and mongosh's $currentOp aggregation),
 * killOp, listCommands, hostInfo, connectionStatus, whatsmyuri, replSetGetConfig and
 * real connection gauges in serverStatus. Wire-level via EmbeddedChannel around the
 * real MongoCommandHandler.
 */
public class DevOpsCommandsTest {

    private InMemoryDriver drv;
    private OpRegistry registry;
    private EmbeddedChannel ch;
    private final AtomicInteger msgId = new AtomicInteger(1);

    @BeforeEach
    public void setup() throws Exception {
        drv = new InMemoryDriver();
        drv.connect();
        registry = new OpRegistry();
        ch = new EmbeddedChannel(new MongoCommandHandler(drv, null, null, null, new AtomicInteger(1),
                "0.0.0.0", 27017, "my-rs", List.of("localhost:27017", "localhost:27018"), true,
                "localhost:27017", 0, () -> null)
                .setOpRegistry(registry)
                .setConnectionCounters(() -> 42, () -> 99L)
                .setRsPriorities(Map.of("localhost:27017", 100, "localhost:27018", 75)));
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

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> inprog(Map<String, Object> reply) {
        return (List<Map<String, Object>>) reply.get("inprog");
    }

    // ---- currentOp / $currentOp / killOp ------------------------------------------------

    @Test
    public void currentOpCommandShowsTheRunningOp() {
        Map<String, Object> reply = send(Doc.of("currentOp", 1, "$db", "admin"));

        assertThat(reply.get("ok")).isEqualTo(1.0);
        // the currentOp op itself is registered while it executes
        assertThat(inprog(reply)).as("reply: " + reply).hasSize(1);
        Map<String, Object> op = inprog(reply).get(0);
        assertThat(op.get("active")).isEqualTo(true);
        assertThat(((Number) op.get("opid")).longValue()).isPositive();
        assertThat(op.get("op")).isEqualTo("command");
        assertThat(op.get("killPending")).isEqualTo(false);
        assertThat(op).containsKeys("ns", "command", "secs_running", "client");
    }

    @Test
    public void currentOpAggregationAnswersMongoshShape() {
        Map<String, Object> reply = send(Doc.of("aggregate", 1,
                "pipeline", List.of(Doc.of("$currentOp", Doc.of())),
                "cursor", Doc.of(), "$db", "admin"));

        assertThat(reply.get("ok")).as("reply: " + reply).isEqualTo(1.0);
        @SuppressWarnings("unchecked")
        Map<String, Object> cursor = (Map<String, Object>) reply.get("cursor");
        assertThat(cursor).isNotNull();
        assertThat(((Number) cursor.get("id")).longValue()).isZero();
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> batch = (List<Map<String, Object>>) cursor.get("firstBatch");
        assertThat(batch).as("the aggregation op itself must be visible").hasSize(1);
        assertThat(batch.get(0).get("op")).isEqualTo("command");
    }

    @Test
    public void currentOpAggregationAppliesMatchStages() {
        Map<String, Object> none = send(Doc.of("aggregate", 1,
                "pipeline", List.of(Doc.of("$currentOp", Doc.of()), Doc.of("$match", Doc.of("opid", -1L))),
                "cursor", Doc.of(), "$db", "admin"));
        @SuppressWarnings("unchecked")
        Map<String, Object> cursor = (Map<String, Object>) none.get("cursor");
        assertThat((List<?>) cursor.get("firstBatch")).isEmpty();

        Map<String, Object> refused = send(Doc.of("aggregate", 1,
                "pipeline", List.of(Doc.of("$currentOp", Doc.of()), Doc.of("$limit", 1)),
                "cursor", Doc.of(), "$db", "admin"));
        assertThat(refused.get("ok")).isEqualTo(0.0);
        assertThat(refused.get("errmsg").toString()).contains("$match");
    }

    @Test
    public void killOpFlagsARegisteredOpAndRejectsUnknownIds() {
        OpRegistry.OpEntry entry = registry.register("find", Doc.of("find", "coll", "$db", "tst"), "test-client");

        Map<String, Object> killed = send(Doc.of("killOp", 1, "op", entry.getOpid(), "$db", "admin"));
        assertThat(killed.get("ok")).as("reply: " + killed).isEqualTo(1.0);
        assertThat(entry.isKillPending()).isTrue();

        registry.deregister(entry);
        Map<String, Object> unknown = send(Doc.of("killOp", 1, "op", 999999, "$db", "admin"));
        assertThat(unknown.get("ok")).isEqualTo(0.0);
        assertThat(unknown.get("errmsg").toString()).contains("no such opid");
    }

    // ---- discovery / host / connection info ---------------------------------------------

    @Test
    public void listCommandsNamesTheCommandSurface() {
        Map<String, Object> reply = send(Doc.of("listCommands", 1, "$db", "admin"));

        assertThat(reply.get("ok")).as("reply: " + reply).isEqualTo(1.0);
        @SuppressWarnings("unchecked")
        Map<String, Object> commands = (Map<String, Object>) reply.get("commands");
        assertThat(commands).containsKeys("find", "insert", "aggregate", "currentOp", "killOp",
                "listCommands", "hostInfo", "replSetGetStatus", "serverStatus");
    }

    @Test
    public void hostInfoReportsSystemBasics() {
        Map<String, Object> reply = send(Doc.of("hostInfo", 1, "$db", "admin"));

        assertThat(reply.get("ok")).isEqualTo(1.0);
        @SuppressWarnings("unchecked")
        Map<String, Object> system = (Map<String, Object>) reply.get("system");
        assertThat(system.get("hostname").toString()).isNotEmpty();
        assertThat(((Number) system.get("numCores")).intValue()).isPositive();
        assertThat(((Number) system.get("memSizeMB")).longValue()).isPositive();
        assertThat(reply).containsKeys("os", "extra");
    }

    @Test
    public void connectionStatusReportsNoUsersWhenUnauthenticated() {
        Map<String, Object> reply = send(Doc.of("connectionStatus", 1, "$db", "admin"));

        assertThat(reply.get("ok")).isEqualTo(1.0);
        @SuppressWarnings("unchecked")
        Map<String, Object> authInfo = (Map<String, Object>) reply.get("authInfo");
        assertThat((List<?>) authInfo.get("authenticatedUsers")).isEmpty();
        assertThat((List<?>) authInfo.get("authenticatedUserRoles")).isEmpty();
    }

    @Test
    public void whatsmyuriAnswersTheClientAddress() {
        Map<String, Object> reply = send(Doc.of("whatsmyuri", 1, "$db", "admin"));

        assertThat(reply.get("ok")).isEqualTo(1.0);
        assertThat(reply.get("you").toString()).isNotEmpty();
    }

    @Test
    public void serverStatusCarriesRealConnectionGauges() {
        Map<String, Object> reply = send(Doc.of("serverStatus", 1, "$db", "admin"));

        assertThat(reply.get("ok")).isEqualTo(1.0);
        @SuppressWarnings("unchecked")
        Map<String, Object> connections = (Map<String, Object>) reply.get("connections");
        assertThat(((Number) connections.get("current")).intValue())
            .as("must be the Netty channel count from the supplier, not driver borrows").isEqualTo(42);
        assertThat(((Number) connections.get("totalCreated")).longValue()).isEqualTo(99L);
    }

    // ---- dbHash / validate / top ---------------------------------------------------------

    @Test
    public void dbHashAndValidateWorkOverTheWire() throws Exception {
        send(Doc.of("insert", "c1", "documents", List.of(Doc.of("_id", 1, "v", "x")), "$db", "hashdb"));

        Map<String, Object> hash = send(Doc.of("dbHash", 1, "$db", "hashdb"));
        assertThat(hash.get("ok")).as("reply: " + hash).isEqualTo(1.0);
        @SuppressWarnings("unchecked")
        Map<String, Object> colls = (Map<String, Object>) hash.get("collections");
        assertThat(colls).containsKey("c1");
        assertThat(hash.get("md5").toString()).hasSize(32);

        Map<String, Object> validate = send(Doc.of("validate", "c1", "$db", "hashdb"));
        assertThat(validate.get("ok")).as("reply: " + validate).isEqualTo(1.0);
        assertThat(validate.get("valid")).isEqualTo(true);
        assertThat(((Number) validate.get("nrecords")).longValue()).isEqualTo(1L);
    }

    @Test
    public void insertOverTheWireIsRejectedAboveTheMemoryWatermark() {
        // hold >=2% of the max heap so a 1% threshold is guaranteed to be exceeded,
        // independent of the surefire JVM's heap sizing
        byte[] filler = new byte[(int) Math.min(Integer.MAX_VALUE - 8, Runtime.getRuntime().maxMemory() / 50)];
        filler[0] = 1;
        drv.setMemoryWatermarks(1, 1);

        try {
            Map<String, Object> reply = send(Doc.of("insert", "c1",
                    "documents", List.of(Doc.of("_id", 1)), "$db", "wmdb"));

            @SuppressWarnings("unchecked")
            List<Map<String, Object>> writeErrors = (List<Map<String, Object>>) reply.get("writeErrors");
            assertThat(writeErrors).as("reply: " + reply).isNotNull().isNotEmpty();
            assertThat(((Number) writeErrors.get(0).get("code")).intValue())
                .as("ExceededMemoryLimit, not a mislabelled duplicate key: " + reply).isEqualTo(146);
        } finally {
            drv.setMemoryWatermarks(75, 90);
        }

        // keep the filler reachable until the assertions ran
        assertThat(filler.length).isPositive();
    }

    @Test
    public void topAnswersExplicitCommandNotSupported() {
        Map<String, Object> reply = send(Doc.of("top", 1, "$db", "admin"));

        assertThat(reply.get("ok")).isEqualTo(0.0);
        assertThat(((Number) reply.get("code")).intValue()).as("reply: " + reply).isEqualTo(115);
        assertThat(reply.get("errmsg").toString()).contains("not supported");
    }

    // ---- dumpNow -------------------------------------------------------------------------

    /** dumpNow answers asynchronously (the dump runs off the I/O thread) - pump the
     * EmbeddedChannel's task queue until the reply arrives. */
    private Map<String, Object> sendAsync(Map<String, Object> cmd) throws Exception {
        OpMsg msg = new OpMsg();
        msg.setMessageId(msgId.incrementAndGet());
        msg.setFirstDoc(cmd);
        ch.writeInbound(msg);

        for (int i = 0; i < 500; i++) {
            ch.runPendingTasks();
            OpMsg reply = ch.readOutbound();

            if (reply != null) {
                return reply.getFirstDoc();
            }

            Thread.sleep(10);
        }

        throw new AssertionError("no reply for " + cmd.keySet().iterator().next());
    }

    @Test
    public void dumpNowWithoutDumpDirectoryAnswersInvalidOptions() throws Exception {
        Map<String, Object> reply = sendAsync(Doc.of("dumpNow", 1, "$db", "admin"));

        assertThat(reply.get("ok")).as("reply: " + reply).isEqualTo(0.0);
        assertThat(((Number) reply.get("code")).intValue()).isEqualTo(72);
        assertThat(reply.get("errmsg").toString()).contains("--dump-dir");
    }

    @Test
    public void dumpNowTriggersTheConfiguredDumpAndReportsTheCount() throws Exception {
        java.util.concurrent.atomic.AtomicInteger calls = new java.util.concurrent.atomic.AtomicInteger();
        EmbeddedChannel dumpCh = new EmbeddedChannel(new MongoCommandHandler(drv, null, null, null,
                new AtomicInteger(1), "0.0.0.0", 27017, "my-rs", List.of("localhost:27017"), true,
                "localhost:27017", 0, () -> null)
                .setDumpNowAction(() -> { calls.incrementAndGet(); return 3; }));
        OpMsg msg = new OpMsg();
        msg.setMessageId(msgId.incrementAndGet());
        msg.setFirstDoc(Doc.of("dumpNow", 1, "$db", "admin"));
        dumpCh.writeInbound(msg);
        OpMsg reply = null;

        for (int i = 0; i < 500 && reply == null; i++) {
            dumpCh.runPendingTasks();
            reply = dumpCh.readOutbound();

            if (reply == null) {
                Thread.sleep(10);
            }
        }

        assertThat(reply).isNotNull();
        assertThat(reply.getFirstDoc().get("ok")).as("reply: " + reply.getFirstDoc()).isEqualTo(1.0);
        assertThat(((Number) reply.getFirstDoc().get("databases")).intValue()).isEqualTo(3);
        assertThat(calls.get()).isEqualTo(1);
    }

    @Test
    public void dumpNowReportsAFailedDumpAsError() throws Exception {
        EmbeddedChannel dumpCh = new EmbeddedChannel(new MongoCommandHandler(drv, null, null, null,
                new AtomicInteger(1), "0.0.0.0", 27017, "my-rs", List.of("localhost:27017"), true,
                "localhost:27017", 0, () -> null)
                .setDumpNowAction(() -> { throw new java.io.IOException("disk full"); }));
        OpMsg msg = new OpMsg();
        msg.setMessageId(msgId.incrementAndGet());
        msg.setFirstDoc(Doc.of("dumpNow", 1, "$db", "admin"));
        dumpCh.writeInbound(msg);
        OpMsg reply = null;

        for (int i = 0; i < 500 && reply == null; i++) {
            dumpCh.runPendingTasks();
            reply = dumpCh.readOutbound();

            if (reply == null) {
                Thread.sleep(10);
            }
        }

        assertThat(reply).isNotNull();
        assertThat(reply.getFirstDoc().get("ok")).isEqualTo(0.0);
        assertThat(reply.getFirstDoc().get("errmsg").toString()).contains("disk full");
    }

    @Test
    public void listCommandsIncludesDumpNow() {
        Map<String, Object> reply = send(Doc.of("listCommands", 1, "$db", "admin"));

        @SuppressWarnings("unchecked")
        Map<String, Object> commands = (Map<String, Object>) reply.get("commands");
        assertThat(commands).containsKeys("dumpNow", "dumpStatus");
    }

    @Test
    public void dumpStatusWithoutPersistenceAnswersDisabled() {
        Map<String, Object> reply = send(Doc.of("dumpStatus", 1, "$db", "admin"));

        assertThat(reply.get("ok")).as("reply: " + reply).isEqualTo(1.0);
        assertThat(reply.get("enabled")).isEqualTo(false);
    }

    @Test
    public void dumpStatusReportsTheWiredPersistenceInfo() {
        EmbeddedChannel statusCh = new EmbeddedChannel(new MongoCommandHandler(drv, null, null, null,
                new AtomicInteger(1), "0.0.0.0", 27017, "my-rs", List.of("localhost:27017"), true,
                "localhost:27017", 0, () -> null)
                .setDumpStatusSupplier(() -> Doc.of("enabled", true, "dir", "/data/dumps",
                        "intervalMs", 60000L, "schedulerRunning", true, "lastDumpMs", 12345L)));
        OpMsg msg = new OpMsg();
        msg.setMessageId(msgId.incrementAndGet());
        msg.setFirstDoc(Doc.of("dumpStatus", 1, "$db", "admin"));
        statusCh.writeInbound(msg);
        OpMsg reply = statusCh.readOutbound();

        assertThat(reply).isNotNull();
        Map<String, Object> doc = reply.getFirstDoc();
        assertThat(doc.get("ok")).as("reply: " + doc).isEqualTo(1.0);
        assertThat(doc.get("enabled")).isEqualTo(true);
        assertThat(doc.get("dir")).isEqualTo("/data/dumps");
        assertThat(((Number) doc.get("intervalMs")).longValue()).isEqualTo(60000L);
        assertThat(doc.get("schedulerRunning")).isEqualTo(true);
        assertThat(((Number) doc.get("lastDumpMs")).longValue()).isEqualTo(12345L);
    }

    // ---- replSetGetConfig ----------------------------------------------------------------

    @Test
    public void replSetGetConfigReconstructsTheConfig() {
        Map<String, Object> reply = send(Doc.of("replSetGetConfig", 1, "$db", "admin"));

        assertThat(reply.get("ok")).as("reply: " + reply).isEqualTo(1.0);
        @SuppressWarnings("unchecked")
        Map<String, Object> config = (Map<String, Object>) reply.get("config");
        assertThat(config.get("_id")).isEqualTo("my-rs");
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> members = (List<Map<String, Object>>) config.get("members");
        assertThat(members).hasSize(2);
        assertThat(members.get(0).get("host")).isEqualTo("localhost:27017");
        assertThat(((Number) members.get(0).get("priority")).intValue()).isEqualTo(100);
        assertThat(((Number) members.get(1).get("priority")).intValue()).isEqualTo(75);
    }

    @Test
    public void replSetGetConfigWithoutReplSetAnswersError() {
        EmbeddedChannel standalone = new EmbeddedChannel(new MongoCommandHandler(drv, null, null, null,
                new AtomicInteger(1), "localhost", 27017, "", List.of(), true, "localhost:27017",
                0, () -> null));
        OpMsg msg = new OpMsg();
        msg.setMessageId(msgId.incrementAndGet());
        msg.setFirstDoc(Doc.of("replSetGetConfig", 1, "$db", "admin"));
        standalone.writeInbound(msg);
        OpMsg reply = standalone.readOutbound();

        assertThat(reply).isNotNull();
        assertThat(reply.getFirstDoc().get("ok")).isEqualTo(0.0);
        assertThat(((Number) reply.getFirstDoc().get("code")).intValue()).isEqualTo(76);
    }
}
