package de.caluga.test.poppydb.election;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import de.caluga.poppydb.election.ElectionConfig;
import de.caluga.poppydb.election.ElectionManager;
import de.caluga.poppydb.election.ElectionState;

/**
 * A node that steps down must stop naming itself as the leader. {@code stepDown} went to
 * FOLLOWER via {@code becomeFollower(term, null)}, and a null leader keeps the previous value -
 * the node's own address. Until the next leader's first heartbeat arrived, {@code hello} on the
 * stepped-down node therefore answered {@code isWritablePrimary:false} together with
 * {@code primary: <itself>}: the driver took that as "the primary is ... this node", retried the
 * same node and got 13436 from its recovering gate, in a loop. Seen on the test runner on
 * 2026-09-21 22:20: a priority-takeover yield three seconds before AdvancedMessagingTests
 * connected ("Primary failover? 17018 -> 17018 (re-resolved from 17018's own hello)" x7, then
 * "Error: 13436 - node is recovering"). mongod reports no primary in that window.
 */
public class StepDownClearsLeaderTest {

    private ElectionManager manager;

    @AfterEach
    public void tearDown() {
        if (manager != null) {
            manager.stop();
        }
    }

    @Test
    public void aSteppedDownLeaderNoLongerNamesItselfAsLeader() throws Exception {
        ElectionConfig config = new ElectionConfig()
                .setElectionTimeoutMinMs(50)
                .setElectionTimeoutMaxMs(100);
        manager = new ElectionManager("localhost:27017", List.of("localhost:27017"), config);

        CountDownLatch leaderLatch = new CountDownLatch(1);
        manager.setOnLeadershipChange(isLeader -> {
            if (isLeader) {
                leaderLatch.countDown();
            }
        });
        manager.start();
        assertTrue(leaderLatch.await(2, TimeUnit.SECONDS), "single node must elect itself");
        assertEquals("localhost:27017", manager.getCurrentLeader());

        assertTrue(manager.stepDown(60, 0, true), "stepdown must succeed");

        assertEquals(ElectionState.FOLLOWER, manager.getState());
        assertNull(manager.getCurrentLeader(),
                "a stepped-down node knows no leader until the next one's heartbeat arrives - "
                + "it must not keep advertising itself");
    }
}
