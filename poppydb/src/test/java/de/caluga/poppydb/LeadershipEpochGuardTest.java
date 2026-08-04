package de.caluga.poppydb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import de.caluga.poppydb.election.ElectionConfig;

/**
 * Unit-level regression test for Finding A of the leadership-hardening task: ElectionManager
 * dispatches {@code onLeadershipChange} from a 3-thread pool, serialized on the PoppyDB monitor
 * but NOT ordered - two rapid dispatches can acquire the monitor inverted, letting a stale body
 * run its full bookkeeping AFTER a newer one already did (e.g. a stale follower body clearing
 * {@code replicationCoordinatorRef} right after a fresh leader body just populated it).
 *
 * <p>Drives {@link PoppyDB#onLeadershipChangeSynchronized(boolean, long)} directly - a
 * package-private seam added specifically for this test, not a private implementation detail
 * reached via reflection - with an epoch value it controls, instead of going through the private
 * {@code onLeadershipChange} wrapper (which would require a live ElectionManager dispatch to
 * reach). No Netty server is started; {@code configureReplicaSet} alone is enough to make the
 * body's leader branch exercise the same replicationCoordinatorRef bookkeeping the finding is
 * about.
 */
public class LeadershipEpochGuardTest {

    private PoppyDB db;

    @AfterEach
    void tearDown() {
        if (db != null) {
            try {
                db.shutdown();
            } catch (Exception ignored) {
            }
        }
    }

    private PoppyDB electionModeNode() {
        List<String> hosts = List.of("localhost:27201", "localhost:27202", "localhost:27203");
        Map<String, Integer> prio = Map.of("localhost:27201", 100, "localhost:27202", 50, "localhost:27203", 50);
        PoppyDB node = new PoppyDB(27201, "localhost", 10, 5);
        node.configureReplicaSet("rsEpoch", hosts, prio, true, new ElectionConfig());
        return node;
    }

    /**
     * Control case: a call carrying the CURRENT epoch (the field starts at 0 and is only ever
     * incremented by the private wrapper, which this test never invokes, so 0 is current) must
     * run its body normally. Establishes that the guard doesn't just no-op everything.
     */
    @Test
    void currentEpochRunsTheBody() {
        db = electionModeNode();

        db.onLeadershipChangeSynchronized(true, 0);

        assertNotNull(db.getReplicationCoordinator(), "leader body must have created the coordinator");
        assertEquals("localhost:27201", db.getPrimaryHost());
    }

    /**
     * The guard itself: a call carrying a STALE epoch (anything other than the field's current
     * value, which is 0 here since this test never invokes the incrementing wrapper) must no-op
     * before touching any state - not just before the coordinator swap, the WHOLE body.
     */
    @Test
    void staleEpochNoOps() {
        db = electionModeNode();
        assertNull(db.getPrimaryHost(), "sanity: nothing has run yet");

        db.onLeadershipChangeSynchronized(true, 999);

        assertNull(db.getReplicationCoordinator(), "stale body must not have created the coordinator");
        assertNull(db.getPrimaryHost(), "stale body must not have touched primaryHost either");
    }

    /**
     * The exact Finding A scenario: a stale FOLLOWER body must not clear a coordinator that a
     * newer LEADER body already set up - reproduced here by running the (current-epoch) leader
     * body first, then a (stale-epoch) follower body second, and asserting the coordinator
     * survives. This is the scenario the brief calls out explicitly: "two rapid false->true
     * dispatches can acquire the monitor inverted... the stale follower body runs last - it
     * clears replicationCoordinatorRef".
     */
    @Test
    void staleFollowerBodyCannotClearANewerLeaderBodysCoordinator() {
        db = electionModeNode();

        // Current epoch (0): the "newer" transition's body actually runs, becoming leader.
        db.onLeadershipChangeSynchronized(true, 0);
        Object coordinatorAfterLeaderBody = db.getReplicationCoordinator();
        assertNotNull(coordinatorAfterLeaderBody, "leader body must have created the coordinator");

        // Stale epoch: a follower body dispatched BEFORE the leader transition above, but whose
        // execution was delayed until after it - must be a complete no-op, not just skip the
        // ReplicationManager teardown.
        db.onLeadershipChangeSynchronized(false, -1);

        assertSame(coordinatorAfterLeaderBody, db.getReplicationCoordinator(),
                "stale follower body must not clear the coordinator a newer leader body just set up");
    }
}
