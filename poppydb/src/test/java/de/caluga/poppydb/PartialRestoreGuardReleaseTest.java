package de.caluga.poppydb;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.driver.Doc;
import de.caluga.poppydb.election.ElectionConfig;
import de.caluga.test.mongo.suite.data.UncachedObject;

/**
 * #306 P1-2 follow-up: the partial-restore candidacy guard must be RELEASED again once an
 * authoritative initial sync has completed - in the ELECTION replication path, not just the
 * static one. The release used to live only in {@code PoppyDB#startReplication()}, which runs
 * exclusively in static (non-election) mode; election-mode replication goes through
 * {@code startReplicationToLeader()}, which had no initial-sync-completion hook at all. Net
 * effect: a node that started with an incomplete restore synced fine, but stayed
 * {@code localDataComplete=false} forever - and when the primary later died, it refused every
 * candidacy and the cluster stayed without a primary despite holding a full, authoritative
 * copy of the data.
 *
 * <p>Setup mirrors that scenario: a 3-node RS where the guarded node is the only one allowed
 * to succeed the primary (the third member has priority 0 - it votes but never leads), so a
 * stuck guard is directly observable as "no primary ever again".
 */
@Tag("server")
public class PartialRestoreGuardReleaseTest {

    private static final Logger log = LoggerFactory.getLogger(PartialRestoreGuardReleaseTest.class);

    private static final String DB = "partialrestorerelease";
    private static final String COLL = "objs";
    private static final int DOCS = 100;

    private final List<PoppyDB> nodes = new ArrayList<>();

    @AfterEach
    public void tearDown() {
        for (int i = nodes.size() - 1; i >= 0; i--) {
            try {
                nodes.get(i).shutdown();
            } catch (Exception ignored) {
            }
        }
        nodes.clear();
    }

    private int nextPort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private void startServer(PoppyDB srv, int port) throws Exception {
        nodes.add(srv);
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

    private boolean poll(long timeoutMs, Callable<Boolean> condition) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (Boolean.TRUE.equals(condition.call())) {
                return true;
            }
            Thread.sleep(100);
        }
        return Boolean.TRUE.equals(condition.call());
    }

    @Test
    @Timeout(240)
    public void completedInitialSyncReleasesThePartialRestoreGuardInElectionMode() throws Exception {
        int port1 = nextPort();
        int port2 = nextPort();
        int port3 = nextPort();
        PoppyDB intact = new PoppyDB(port1, "localhost", 20, 5);
        PoppyDB guarded = new PoppyDB(port2, "localhost", 20, 5);
        PoppyDB voterOnly = new PoppyDB(port3, "localhost", 20, 5);
        List<String> hosts = List.of("localhost:" + port1, "localhost:" + port2, "localhost:" + port3);
        // Only the guarded node may succeed the intact primary: the third member has priority
        // 0 (votes, never leads), so a permanently stuck guard shows up as "no primary at all".
        Map<String, Integer> prio = Map.of(
                "localhost:" + port1, 100,
                "localhost:" + port2, 90,
                "localhost:" + port3, 0);
        // One fresh ElectionConfig per node - configureReplicaSet mutates the instance in place.
        intact.configureReplicaSet("rsPartialRelease", hosts, prio, true, new ElectionConfig());
        guarded.configureReplicaSet("rsPartialRelease", hosts, prio, true, new ElectionConfig());
        voterOnly.configureReplicaSet("rsPartialRelease", hosts, prio, true, new ElectionConfig());

        // What PoppyDBCLI does after a partial restore: hold this node out of candidacy until
        // an authoritative sync replaces its (incomplete) local state.
        guarded.setLocalDataComplete(false);

        // All three come up together - a lone node cannot reach the 2-of-3 vote majority. The
        // guarded node still VOTES while its guard is set, so the intact (highest-priority)
        // node wins the initial election.
        startServer(intact, port1);
        startServer(guarded, port2);
        startServer(voterOnly, port3);
        assertTrue(poll(30_000, intact::isPrimary), "the intact node must become primary");

        try (Morphium writer = writerFor(port1)) {
            List<UncachedObject> batch = new ArrayList<>(DOCS);
            for (int i = 0; i < DOCS; i++) {
                batch.add(new UncachedObject("doc-" + i, i));
            }
            writer.storeList(batch, COLL);
        }

        // The guarded node runs as a secondary and receives the authoritative dataset over the
        // ELECTION replication path (leader discovery -> startReplicationToLeader).
        assertTrue(poll(60_000, () ->
                guarded.getDriver().count(DB, COLL, Doc.of(), null, null) == DOCS),
                "the guarded secondary must receive the full dataset via initial sync");

        // The actual fix: completing that sync must lift the guard in the election path, not
        // only in static mode's startReplication().
        assertTrue(poll(30_000, guarded::isLocalDataComplete),
                "a completed initial sync must release the partial-restore guard in election mode - "
                        + "the node now holds an authoritative copy and may stand for election again");

        // And the release must be effective end-to-end: when the primary dies, the recovered
        // node must be able to WIN the election instead of refusing candidacy forever.
        log.info("Shutting down the intact primary - the recovered node must take over");
        intact.shutdown();
        nodes.remove(intact);

        assertTrue(poll(60_000, guarded::isPrimary),
                "after the primary's death the synced (previously guarded) node must become primary - "
                        + "a permanently stuck guard leaves the cluster without any primary");
        assertFalse(voterOnly.isPrimary(), "the priority-0 member must never lead");
    }

    private Morphium writerFor(int port) {
        MorphiumConfig cfg = new MorphiumConfig();
        cfg.clusterSettings().setHostSeed("localhost:" + port);
        cfg.connectionSettings().setDatabase(DB);
        cfg.connectionSettings().setMaxConnections(10);
        cfg.cacheSettings().setBufferedWritesEnabled(false);
        return new Morphium(cfg);
    }
}
