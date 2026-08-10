package de.caluga.poppydb.netty;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import de.caluga.morphium.driver.wireprotocol.OpMsg;
import de.caluga.poppydb.PoppyDB;
import de.caluga.poppydb.election.ElectionConfig;
import de.caluga.poppydb.election.ElectionManager;
import de.caluga.poppydb.election.ElectionState;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * rs.status (replSetGetStatus) must speak MongoDB, not Raft: the self member of an elected
 * leader reports stateStr PRIMARY - not the internal election enum name LEADER, which
 * clients and monitoring tools cannot interpret. And a node bound to a wildcard address
 * (--bind 0.0.0.0) must identify itself by its seed-list name, so it does not show up
 * twice in the member list (once as 0.0.0.0:port, once as its configured name marked
 * SECONDARY) and does not treat itself as a votable peer.
 */
public class ReplSetGetStatusTest {

    private InMemoryDriver drv;
    private ElectionManager em;
    private final AtomicInteger msgId = new AtomicInteger(1);

    @BeforeEach
    public void setup() throws Exception {
        drv = new InMemoryDriver();
        drv.connect();
    }

    @AfterEach
    public void tearDown() {
        if (em != null) {
            em.stop();
            em = null;
        }
        if (drv != null) {
            drv.close();
        }
    }

    private Map<String, Object> rsStatus(ElectionManager manager) {
        MongoCommandHandler h = new MongoCommandHandler(drv, null, null, null, new AtomicInteger(1),
                "0.0.0.0", 27017, "my-rs", List.of("localhost:27017"), true, "localhost:27017",
                0, () -> null, manager);
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
    private List<Map<String, Object>> members(Map<String, Object> status) {
        return (List<Map<String, Object>>) status.get("members");
    }

    @Test
    public void leaderReportsPrimaryStateStr() throws Exception {
        em = new ElectionManager("localhost:27017", List.of("localhost:27017"),
                new ElectionConfig().setElectionTimeoutMinMs(50).setElectionTimeoutMaxMs(100));
        em.start();
        long deadline = System.currentTimeMillis() + 5000;
        while (em.getState() != ElectionState.LEADER && System.currentTimeMillis() < deadline) {
            Thread.sleep(10);
        }
        assertThat(em.getState()).isEqualTo(ElectionState.LEADER);

        Map<String, Object> status = rsStatus(em);

        assertThat(status.get("myState")).isEqualTo(1);
        assertThat(members(status)).hasSize(1);
        assertThat(members(status).get(0).get("stateStr")).isEqualTo("PRIMARY");
        assertThat(members(status).get(0).get("state")).isEqualTo(1);
    }

    @Test
    public void followerReportsSecondaryStateStr() {
        // not started - a fresh ElectionManager is a FOLLOWER
        em = new ElectionManager("localhost:27017", List.of("localhost:27017", "localhost:27018"),
                new ElectionConfig());

        Map<String, Object> status = rsStatus(em);

        assertThat(status.get("myState")).isEqualTo(2);
        assertThat(members(status).get(0).get("stateStr")).isEqualTo("SECONDARY");
    }

    @Test
    public void staticBranchMarksSelfBySeedIdentity() {
        // no election manager - the static rs.status branch must also recognize itself
        // by its seed-list name even when bound to a wildcard address
        Map<String, Object> status = rsStatus(null);

        assertThat(members(status)).hasSize(1);
        assertThat(members(status).get(0).get("name")).isEqualTo("localhost:27017");
        assertThat(members(status).get(0).get("self")).isEqualTo(true);
    }

    @Test
    public void wildcardBindNodeIdentifiesItselfBySeedName() {
        PoppyDB srv = new PoppyDB(27017, "0.0.0.0", 100, 30);
        try {
            srv.configureReplicaSet("my-rs",
                    List.of("localhost:27017", "localhost:27018", "localhost:27019"),
                    null, true, null);
            ElectionManager manager = srv.getElectionManager();
            assertThat(manager.getMyAddress()).isEqualTo("localhost:27017");
            assertThat(manager.getPeerAddresses())
                    .containsExactlyInAnyOrder("localhost:27018", "localhost:27019");
        } finally {
            srv.shutdown();
        }
    }
}
