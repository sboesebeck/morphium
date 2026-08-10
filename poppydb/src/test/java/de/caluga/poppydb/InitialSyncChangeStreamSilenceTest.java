package de.caluga.poppydb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.DriverTailableIterationCallback;
import de.caluga.morphium.driver.commands.InsertMongoCommand;
import de.caluga.morphium.driver.commands.WatchCommand;
import de.caluga.morphium.driver.commands.auth.CreateUserAdminCommand;
import de.caluga.morphium.driver.inmem.InMemoryDriver;

/**
 * Regression test for the {@link StepdownReplicationTest} flake (post-stepdown user never
 * reaching node3): a secondary's initial sync used to be OBSERVABLE via its own change stream.
 * {@code clearLocalDatabases()} wipes the local data with regular drop/dropDatabase commands,
 * and those emitted live change-stream events - including {@code drop admin.system.users}.
 *
 * <p>During a leadership transition that is catastrophic: the demoted ex-primary immediately
 * starts re-syncing toward the presumed new leader, and each (re)try of its snapshot wipes its
 * local databases. The OTHER nodes' old ReplicationManagers are still watching the demoted node
 * (they tear down only once their own ElectionManager delivers the leader change) and faithfully
 * apply the wipe's drop events to their own data - observed in the ambient logs as a storm of
 * {@code admin.system.users} drops ricocheting around all three nodes, with even the freshly
 * promoted primary applying the demoted node's wipe-drop right at its own promotion (its
 * stopping ReplicationManager flushes the queued stale drops). Whether
 * StepdownReplicationTest's post-stepdown user survived was then pure timing: if a stale drop
 * reached a node after that node had already picked up the user (via snapshot or stream), the
 * user was destroyed there with nothing left to re-deliver it - the ~40% flake.
 *
 * <p>Contract pinned here (mirrors MongoDB, where initial-sync writes are never oplogged): the
 * initial sync - both the {@code clearLocalDatabases()} wipe and the snapshot copy - must not
 * emit ANY change-stream events on the syncing node. A watcher subscribed to the syncing node
 * (standing in for another node's stale ReplicationManager) must observe nothing.
 */
@Tag("server")
public class InitialSyncChangeStreamSilenceTest {

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

    private void createUser(InMemoryDriver drv, String user, String pwd) throws Exception {
        CreateUserAdminCommand cmd = new CreateUserAdminCommand(null).setUserName(user).setPwd(pwd);
        cmd.setDb("admin");
        Map<String, Object> result = drv.readSingleAnswer(drv.runCommand(cmd));
        assertEquals(1.0, result.get("ok"), "createUser must succeed: " + result);
    }

    /** Collects every event delivered to a cluster-level watch on the given driver. */
    private static class ClusterWatch {
        final List<Map<String, Object>> events = Collections.synchronizedList(new ArrayList<>());
        final AtomicBoolean running = new AtomicBoolean(true);
        final CountDownLatch registered = new CountDownLatch(1);
        Thread thread;

        void stop() throws InterruptedException {
            running.set(false);
            thread.join(5000);
        }
    }

    /**
     * Subscribes to the local driver's change stream exactly the way another PoppyDB node's
     * ReplicationManager would (db "admin" = cluster level, empty pipeline) - this watcher plays
     * the role of a stale RM still pointed at the demoted/syncing node.
     */
    private ClusterWatch subscribeClusterWatch(InMemoryDriver drv) throws Exception {
        ClusterWatch cw = new ClusterWatch();
        var con = drv.getPrimaryConnection(null);
        WatchCommand watch = new WatchCommand(con)
            .setDb("admin")
            .setMaxTimeMS(200)
            .setFullDocument(WatchCommand.FullDocumentEnum.updateLookup)
            .setPipeline(List.of())
            .setRegistrationCallback(cw.registered::countDown)
            .setCb(new DriverTailableIterationCallback() {
                @Override
                public void incomingData(Map<String, Object> data, long dur) {
                    cw.events.add(data);
                }

                @Override
                public boolean isContinued() {
                    return cw.running.get();
                }
            });
        cw.thread = Thread.ofVirtual().start(() -> {
            try {
                watch.watch();
            } catch (Exception e) {
                // stream torn down on stop - nothing to do
            } finally {
                watch.releaseConnection();
            }
        });
        assertTrue(cw.registered.await(5, TimeUnit.SECONDS), "watch never registered");
        return cw;
    }

    private static String describe(Map<String, Object> event) {
        return event.get("operationType") + " on " + event.get("ns");
    }

    @Test
    public void initialSyncEmitsNoChangeStreamEvents() throws Exception {
        int port = nextPort();
        leader = new PoppyDB(port, "localhost", 20, 5);
        startServer(leader, port);
        assertTrue(leader.isPrimary(), "standalone PoppyDB must act as primary");

        // The primary's authoritative state: one user, one data collection.
        createUser(leader.getDriver(), "leader-user", "leader-pw");
        new InsertMongoCommand(leader.getDriver()).setDb("datadb").setColl("docs")
            .setDocuments(List.of(Doc.of("_id", 1, "v", "fresh")))
            .execute();

        // The syncing node's STALE local state - both a stale user (so the wipe's
        // drop("admin","system.users") acts on a non-empty collection) and a stale database
        // (so clearLocalDatabases has a dropDatabase to do). The divergence also guarantees
        // the consistency shortcut fails and the full wipe + snapshot path runs.
        local = new InMemoryDriver();
        local.connect();
        createUser(local, "stale-user", "stale-pw");
        new InsertMongoCommand(local).setDb("staledb").setColl("old")
            .setDocuments(List.of(Doc.of("_id", 1, "v", "stale")))
            .execute();

        // Stale-RM stand-in: watch the syncing node BEFORE its initial sync starts.
        ClusterWatch cw = subscribeClusterWatch(local);
        try {
            rm = new ReplicationManager(local, "localhost", port);
            rm.setMyAddress("localhost:test-secondary");
            rm.start();
            assertTrue(rm.waitForInitialSync(30, TimeUnit.SECONDS),
                "initial sync must complete within 30s");

            // Sanity: the full path (wipe + snapshot) actually ran - a shortcut sync would
            // trivially emit nothing and pin the wrong thing.
            assertFalse(rm.wasLastSyncShortcut(), "test must exercise the full wipe + snapshot path");
            assertTrue(rm.getClearLocalDatabasesInvocationsForTest() >= 1,
                "clearLocalDatabases must have run");

            // Sanity: the sync itself worked - the primary's state replaced the stale state.
            assertEquals(1, local.findByFieldValue("admin", "system.users", "_id", "admin.leader-user").size(),
                "the primary's user must have been copied");
            assertTrue(local.findByFieldValue("admin", "system.users", "_id", "admin.stale-user").isEmpty(),
                "the stale local user must be gone after the sync");

            // Grace period for any late asynchronous dispatch before asserting silence.
            Thread.sleep(500);
        } finally {
            cw.stop();
        }

        List<String> destructive;
        List<String> all;
        synchronized (cw.events) {
            destructive = cw.events.stream()
                .filter(e -> "drop".equals(e.get("operationType")) || "dropDatabase".equals(e.get("operationType")))
                .map(InitialSyncChangeStreamSilenceTest::describe).toList();
            all = cw.events.stream().map(InitialSyncChangeStreamSilenceTest::describe).toList();
        }

        // THE regression assertion: the wipe must not be observable. Pre-fix this collected
        // "drop on {db=admin, coll=system.users}" and "dropDatabase on {db=staledb}" - the very
        // events that, applied by other nodes' stale ReplicationManagers, destroyed
        // admin.system.users cluster-wide during the stepdown transition.
        assertTrue(destructive.isEmpty(),
            "a node's initial-sync wipe must not emit change-stream events (stale watchers of a "
            + "demoted node would apply them and destroy their own data), but got: " + destructive);

        // And the snapshot copy must be equally silent (MongoDB: initial sync is not oplogged).
        assertTrue(all.isEmpty(),
            "the initial sync (wipe + snapshot copy) must emit NO change-stream events at all, but got: " + all);
    }
}
