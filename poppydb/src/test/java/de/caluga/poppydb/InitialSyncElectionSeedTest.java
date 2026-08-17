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
import de.caluga.morphium.driver.commands.WatchCommand;
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

    /**
     * Deterministic reproduction of the CI-only failure of the test above: the initial-sync/
     * registration-seed race, from the losing side. In production the watch's registration
     * callback flips {@code watchLive} and only THEN records the primary's sequence
     * ({@code recordPrimarySequenceAtRegistration}); the sync thread is released by
     * {@code watchLive} alone, so on a loaded host it can complete its entire (shortcut-fast)
     * cycle inside that gap - its one-shot election report then reads
     * {@code lastAppliedSequence == 0} and is skipped. When the registration seed finally lands
     * ({@code compareAndSet(0, primarySeq)} succeeds), the position is known but - before the
     * fix - nobody reported it anymore: {@code processBatch()} only reports when live events
     * are actually drained, so ElectionManager stayed at 0 forever and the #306 empty-candidate
     * restraint permanently barred a fully-synced node from every election.
     *
     * <p>This test drives exactly that interleaving without any scheduling luck: it puts a bare
     * (never started) ReplicationManager into the precise post-race state - sync declared
     * complete, {@code lastAppliedSequence} still 0 - and then delivers the registration seed.
     * The fix makes the seed itself push the now-known position to the election layer.
     */
    @Test
    public void registrationSeedLandingAfterSyncCompletionStillReachesElection() throws Exception {
        local = new InMemoryDriver();
        local.connect();

        ElectionConfig config = new ElectionConfig()
                .setElectionTimeoutMinMs(60_000)
                .setElectionTimeoutMaxMs(60_000);
        ElectionManager electionManager = new ElectionManager(
                "localhost:test-secondary", List.of("localhost:test-secondary"), config);

        // Never started: state is driven directly, no primary involved.
        rm = new ReplicationManager(local, "localhost", 1);
        rm.setMyAddress("localhost:test-secondary");
        rm.setOnLogIndexUpdate((index, term) ->
                electionManager.updateLogIndex(index, electionManager.getCurrentTerm()));

        // The losing interleaving's post-sync state: the sync loop's success block already ran
        // (gate open, its one-shot report saw lastAppliedSequence == 0 and skipped), the
        // registration seed has NOT landed yet.
        rm.initialSyncComplete.set(true);

        // Now the registration seed lands - after sync completion, as on the loaded CI runner.
        WatchCommand watchCmd = new WatchCommand(null);
        watchCmd.setMetaData("poppyPrimarySequence", 42L);
        rm.recordPrimarySequenceAtRegistration(watchCmd);

        assertTrue(electionManager.getLastLogIndex() == 42,
                "a registration seed that lands AFTER the initial sync already declared success "
                        + "must still reach ElectionManager - otherwise the node reports index 0 "
                        + "forever and the #306 empty-candidate restraint permanently bars a "
                        + "fully-synced node from every election "
                        + "(got lastLogIndex=" + electionManager.getLastLogIndex() + ", "
                        + "ReplicationManager lastAppliedSequence=" + rm.getLastAppliedSequence() + ")");
    }

    /**
     * Guard for the opposite direction of the same fix: the ordinary, non-racy case is that the
     * registration seed lands at the START of a sync cycle, long before the node actually holds
     * the primary's dataset. The catch-up report must NOT fire then - a node claiming a non-zero
     * replication position while (possibly still empty and) mid-sync would recreate, from the
     * other side, exactly the hazard the #306 empty-candidate restraint closed.
     */
    @Test
    public void registrationSeedBeforeSyncCompletionDoesNotClaimAPosition() throws Exception {
        local = new InMemoryDriver();
        local.connect();

        ElectionConfig config = new ElectionConfig()
                .setElectionTimeoutMinMs(60_000)
                .setElectionTimeoutMaxMs(60_000);
        ElectionManager electionManager = new ElectionManager(
                "localhost:test-secondary", List.of("localhost:test-secondary"), config);

        rm = new ReplicationManager(local, "localhost", 1);
        rm.setMyAddress("localhost:test-secondary");
        rm.setOnLogIndexUpdate((index, term) ->
                electionManager.updateLogIndex(index, electionManager.getCurrentTerm()));

        // Normal cycle start: initial sync NOT complete yet when the registration seed lands.
        WatchCommand watchCmd = new WatchCommand(null);
        watchCmd.setMetaData("poppyPrimarySequence", 42L);
        rm.recordPrimarySequenceAtRegistration(watchCmd);

        assertTrue(electionManager.getLastLogIndex() == 0,
                "a registration seed arriving BEFORE the initial sync completed must not claim a "
                        + "replication position - the node does not hold the primary's dataset yet "
                        + "(got lastLogIndex=" + electionManager.getLastLogIndex() + ")");
    }
}
