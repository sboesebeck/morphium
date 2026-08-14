package de.caluga.poppydb;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.commands.InsertMongoCommand;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import de.caluga.poppydb.election.ElectionManager;
import de.caluga.poppydb.election.ElectionConfig;

/**
 * Integration-level regression test for the "freshly-synced but silent" leg of the
 * empty-node-wipe bug: a node that just completed an initial sync (full snapshot or consistency
 * shortcut - both converge on the same "success: open the gate" block in the replication loop,
 * see that block's comment in {@link ReplicationManager}) must report its real, non-zero
 * replication position to {@link ElectionManager} immediately, even if it goes on to apply ZERO
 * live events afterward (a quiet primary). Before this fix, only {@code processBatch()}'s
 * per-applied-batch call fed {@code onLogIndexUpdate}, so a freshly-synced-then-silent node kept
 * reporting index 0 - wrongly granting votes to genuinely empty candidates as voter (reopening
 * the wipe), and wrongly getting denied as candidate.
 *
 * <p>Uses the same lightweight harness as {@link InitialSyncChangeStreamSilenceTest}: a real
 * standalone {@link PoppyDB} primary plus a bare {@link ReplicationManager} pointed directly at
 * it (no multi-node election machinery, no {@code @Disabled} - this stays fast and always-on).
 * The {@code ElectionManager} here is not attached to a live election (no peers, never
 * started/stopped) - it exists purely as the production wiring target for
 * {@code setOnLogIndexUpdate}, exactly as {@link PoppyDB#startReplicationToLeader} wires it.
 */
@Tag("server")
public class InitialSyncElectionSeedTest {

    private PoppyDB leader;
    private ReplicationManager rm;
    private InMemoryDriver local;

    @AfterEach
    public void tearDown() {
        if (rm != null) {
            try {
                rm.stop();
            } catch (Exception ignored) {
            }
        }
        if (local != null) {
            try {
                local.close();
            } catch (Exception ignored) {
            }
        }
        if (leader != null) {
            try {
                leader.shutdown();
            } catch (Exception ignored) {
            }
        }
    }

    private int nextPort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
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
    public void freshlySyncedNodeReportsNonZeroIndexWithoutAnyLiveEvent() throws Exception {
        int port = nextPort();
        leader = new PoppyDB(port, "localhost", 20, 5);
        startServer(leader, port);
        assertTrue(leader.isPrimary(), "standalone PoppyDB must act as primary");

        // Give the primary real, pre-existing data BEFORE the secondary ever connects, so its
        // change-stream sequence is genuinely non-zero and the secondary's initial-sync seed
        // (recordPrimarySequenceAtRegistration) has something real to seed from.
        new InsertMongoCommand(leader.getDriver()).setDb("datadb").setColl("docs")
            .setDocuments(List.of(Doc.of("_id", 1, "v", "fresh")))
            .execute();

        local = new InMemoryDriver();
        local.connect();

        ElectionConfig config = new ElectionConfig()
                .setElectionTimeoutMinMs(60_000)
                .setElectionTimeoutMaxMs(60_000);
        ElectionManager electionManager = new ElectionManager(
                "localhost:test-secondary", List.of("localhost:test-secondary"), config);
        // Not started: this test only exercises updateLogIndex() as a wiring target, not the
        // election protocol itself (that's ElectionLogRecencyTest's job).

        rm = new ReplicationManager(local, "localhost", port);
        rm.setMyAddress("localhost:test-secondary");
        // Exactly the wiring PoppyDB#startReplicationToLeader installs in production.
        rm.setOnLogIndexUpdate((index, term) ->
                electionManager.updateLogIndex(index, electionManager.getCurrentTerm()));
        rm.start();
        assertTrue(rm.waitForInitialSync(30, TimeUnit.SECONDS), "initial sync must complete within 30s");

        // No write happens on the primary after this point in this test - the secondary applies
        // zero live events. Before this fix, ElectionManager's lastLogIndex would still be 0 here.
        assertTrue(electionManager.getLastLogIndex() > 0,
                "a freshly-synced node must report a non-zero replication position to "
                        + "ElectionManager even without applying any live event afterward "
                        + "(got lastLogIndex=" + electionManager.getLastLogIndex() + ", "
                        + "ReplicationManager lastAppliedSequence=" + rm.getLastAppliedSequence() + ")");
    }
}
