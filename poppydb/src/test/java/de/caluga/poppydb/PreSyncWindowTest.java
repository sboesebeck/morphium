package de.caluga.poppydb;

import de.caluga.poppydb.election.ElectionConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.net.ServerSocket;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * #371: between the moment a node starts accepting connections and the moment its
 * ReplicationManager begins its initial sync, the node reported SECONDARY and answered data
 * reads successfully - with nothing in them. Both wire guards (preDispatch's 13436, and
 * RECOVERING in replSetGetStatus) key on {@code isSecondarySyncing()}, which was
 * "manager exists and is syncing": before the manager exists, both were open. The missing
 * state is "this node has not yet established that its data is authoritative", which is true
 * from process start until a sync completes or the node becomes primary - not merely while a
 * sync is running.
 *
 * <p>The tests drive the same supplier the wire layer reads, without binding a port: the
 * startup decision is a package-private method, and leadership / sync completion are driven
 * through the seams the neighbouring tests already use.
 */
@Tag("server")
public class PreSyncWindowTest {

    private PoppyDB db;

    private static int freePort() throws Exception {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    @AfterEach
    public void tearDown() {
        if (db != null) {
            try {
                db.shutdown();
            } catch (Exception ignored) {
                // never started
            }
        }
    }

    /** A three-member election-mode node, configured but not started - the state at bind time. */
    private PoppyDB electionMember() throws Exception {
        int port = freePort();
        db = new PoppyDB(port, "127.0.0.1", 10, 10);
        List<String> hosts = List.of("127.0.0.1:" + port, "127.0.0.1:1", "127.0.0.1:2");
        Map<String, Integer> prio = Map.of("127.0.0.1:" + port, 100, "127.0.0.1:1", 50, "127.0.0.1:2", 25);
        db.configureReplicaSet("rsPreSync", hosts, prio, true, new ElectionConfig());
        return db;
    }

    @Test
    public void aMemberWithPeersIsUnavailableBeforeItsFirstSyncBegins() throws Exception {
        electionMember();
        assertFalse(db.isSecondarySyncing(), "sanity: nothing is set before start()");

        db.enterAwaitingFirstSyncIfReplicating();

        assertTrue(db.isSecondarySyncing(),
                "no manager yet, no data confirmed: the node must answer 13436 and report "
                        + "RECOVERING, not serve an empty store as SECONDARY");
    }

    @Test
    public void aStandaloneNodeIsNeverCaught() throws Exception {
        db = new PoppyDB(freePort(), "127.0.0.1", 10, 10);

        db.enterAwaitingFirstSyncIfReplicating();

        assertFalse(db.isSecondarySyncing(),
                "a node with no peers can never have a sync to wait for - the same exception the "
                        + "candidacy guard makes");
    }

    @Test
    public void aSingleMemberSetIsNeverCaught() throws Exception {
        int port = freePort();
        db = new PoppyDB(port, "127.0.0.1", 10, 10);
        db.configureReplicaSet("rsSolo", List.of("127.0.0.1:" + port),
                Map.of("127.0.0.1:" + port, 100), true, new ElectionConfig());

        db.enterAwaitingFirstSyncIfReplicating();

        assertFalse(db.isSecondarySyncing(), "a one-member set elects itself; there is nobody to sync from");
    }

    @Test
    public void becomingPrimaryLiftsIt() throws Exception {
        electionMember();
        db.enterAwaitingFirstSyncIfReplicating();
        assertTrue(db.isSecondarySyncing());

        // The real transition: flag flip, then the synchronized body (as ElectionManager does).
        long epoch = db.applyLeadershipFlip(true);
        db.onLeadershipChangeSynchronized(true, epoch);
        assertFalse(db.isSecondarySyncing(), "a primary's data is authoritative by definition");

        // And the state must not come back when the node later steps down: it has led, so
        // whatever it holds is what the set replicated.
        db.applyLeadershipFlip(false);
        assertFalse(db.isSecondarySyncing(),
                "a demoted leader between managers holds the set's data - it serves like today");
    }

    @Test
    public void aCompletedSyncLiftsIt() throws Exception {
        electionMember();
        db.enterAwaitingFirstSyncIfReplicating();
        ReplicationManager rm = new ReplicationManager(db.getDriver(), "127.0.0.1", 1);
        db.installReplicationManagerForTest(rm);
        assertTrue(db.isSecondarySyncing(),
                "a manager that has not opened its gate changes nothing - still unavailable");

        db.releaseDataCompleteAfterSyncForTest(rm);

        assertFalse(db.isSecondarySyncing(), "a completed sync is the confirmation the state waits for");
    }

    @Test
    public void theOpenGateServesReadsBeforeTheBacklogDrains() throws Exception {
        electionMember();
        db.enterAwaitingFirstSyncIfReplicating();
        ReplicationManager rm = new ReplicationManager(db.getDriver(), "127.0.0.1", 1);
        db.installReplicationManagerForTest(rm);

        rm.initialSyncComplete.set(true);   // snapshot copied, backlog still applying

        assertFalse(db.isSecondarySyncing(),
                "a lagging secondary is a secondary: reads are served from the gate opening, as "
                        + "before - the pre-sync state must not hold them back until the drain");
    }

    @Test
    public void aRunningSyncStillReportsSyncing() throws Exception {
        electionMember();
        db.enterAwaitingFirstSyncIfReplicating();
        ReplicationManager rm = new ReplicationManager(db.getDriver(), "127.0.0.1", 1);
        db.installReplicationManagerForTest(rm);
        db.releaseDataCompleteAfterSyncForTest(rm);   // first sync done, node served for a while
        assertFalse(db.isSecondarySyncing());

        rm.running.set(true);                          // a resync: gate closed again
        rm.initialSyncComplete.set(false);

        assertTrue(db.isSecondarySyncing(), "negative control: the existing rule still applies");
    }
}
