package de.caluga.poppydb.netty;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.commands.InsertMongoCommand;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import de.caluga.morphium.driver.wireprotocol.OpMsg;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * #356: {@code replSetGetStatus} is the command an operator types to decide whether a node is
 * usable, and it has to answer that question honestly.
 *
 * <p>A secondary re-running its initial sync rejects every data-plane command with 13436 and
 * already advertises {@code secondary:false} in hello - but this command reported
 * {@code stateStr: "SECONDARY"} regardless. A rolling restart driven off that answer walks from
 * node to node while each one in turn holds nothing, and each one keeps saying it is fine. That is
 * the shape of an outage where every check was green.
 *
 * <p>The status document also carried no numbers at all - no {@code date}, no health, no
 * heartbeats, no replication progress - although {@code ReplicationCoordinator.getStats()} has
 * computed the progress all along and the secondaries push it themselves via
 * {@code replSetProgress}. Without {@code date} even mongosh's own lag column is decoration, since
 * it computes lag as {@code date - optimeDate}.
 */
public class ReplSetGetStatusSyncStateTest {

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

    /** Status of a static-config secondary whose initial sync is or is not still running. */
    private Map<String, Object> statusOfSecondary(BooleanSupplier syncing) {
        MongoCommandHandler h = new MongoCommandHandler(drv, null, null, null, new AtomicInteger(1),
                "0.0.0.0", 27018, "my-rs", List.of("localhost:27017", "localhost:27018"),
                false, "localhost:27017", 0, () -> null, null, syncing);
        EmbeddedChannel ch = new EmbeddedChannel(h);
        OpMsg msg = new OpMsg();
        msg.setMessageId(msgId.incrementAndGet());
        msg.setFirstDoc(Doc.of("replSetGetStatus", 1, "$db", "admin"));
        ch.writeInbound(msg);
        OpMsg reply = ch.readOutbound();
        assertThat(reply).as("no reply for replSetGetStatus").isNotNull();
        return reply.getFirstDoc();
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> self(Map<String, Object> status) {
        for (Map<String, Object> m : (List<Map<String, Object>>) status.get("members")) {
            if (Boolean.TRUE.equals(m.get("self"))) {
                return m;
            }
        }

        throw new AssertionError("no self member in " + status);
    }

    @Test
    public void aSyncingNodeReportsRecoveringNotSecondary() {
        Map<String, Object> status = statusOfSecondary(() -> true);

        assertThat(status.get("myState"))
                .as("a node that answers 13436 to every read must not report itself usable")
                .isEqualTo(3);
        assertThat(self(status).get("stateStr")).isEqualTo("RECOVERING");
    }

    /**
     * The state must not depend on how far the copy has got. An earlier version decided STARTUP2
     * versus RECOVERING by asking whether the node currently held data, which inverts mid-resync:
     * clearLocalDatabases() empties the store first, so the node would have claimed STARTUP2 while
     * empty and RECOVERING once the data arrived - backwards, and changing under an operator who
     * is watching it.
     */
    @Test
    public void theSyncingStateDoesNotFlipAsTheCopyArrives() throws Exception {
        Map<String, Object> emptied = statusOfSecondary(() -> true);

        new InsertMongoCommand(drv).setDb("payload").setColl("c")
                .setDocuments(List.of(Doc.of("_id", "d1"))).execute();
        Map<String, Object> partiallyCopied = statusOfSecondary(() -> true);

        assertThat(partiallyCopied.get("myState"))
                .as("same node, same sync, one collection further along - the state must not move")
                .isEqualTo(emptied.get("myState"));
        assertThat(self(partiallyCopied).get("stateStr")).isEqualTo("RECOVERING");
    }

    @Test
    public void aSyncedSecondaryStillReportsSecondary() {
        Map<String, Object> status = statusOfSecondary(() -> false);

        assertThat(status.get("myState"))
                .as("negative control: the guard must not swallow the normal case")
                .isEqualTo(2);
        assertThat(self(status).get("stateStr")).isEqualTo("SECONDARY");
    }

    @Test
    public void theStatusCarriesTheNumbersAMonitorNeeds() {
        Map<String, Object> status = statusOfSecondary(() -> false);

        assertThat(status.get("date"))
                .as("mongosh computes lag as date - optimeDate; without date the output is decoration")
                .isInstanceOf(Date.class);

        Map<String, Object> self = self(status);
        assertThat(self.get("health")).isEqualTo(1.0);
        assertThat(self.get("uptime"))
                .as("uptime in seconds, as mongod reports it")
                .isInstanceOf(Long.class);
    }
}
