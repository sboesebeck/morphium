package de.caluga.test.poppydb.election;

import de.caluga.poppydb.election.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A node that restored only part of its databases must not become primary. Restoring does not
 * advance the change-stream sequence, so after a full cluster restart every node reports index
 * 0 - which makes the ordinary candidacy restraint (empty while a peer holds data) blind: it
 * only triggers when some peer reports a higher index. A partially restored node could
 * therefore win the election and then push its incomplete state onto the intact nodes through
 * their initial sync. This is a data-loss path, so such a node stays out of elections until an
 * authoritative sync has given it a complete copy.
 */
@Tag("poppydb")
public class PartialRestoreElectionGuardTest {

    private final List<ElectionManager> managers = new ArrayList<>();

    @AfterEach
    public void tearDown() {
        for (ElectionManager m : managers) {
            try {
                m.stop();
            } catch (Exception e) {
                // ignore
            }
        }
        managers.clear();
    }

    private ElectionManager node(String address, int timeoutMs) {
        ElectionConfig config = new ElectionConfig()
                .setElectionTimeoutMinMs(timeoutMs)
                .setElectionTimeoutMaxMs(timeoutMs);
        ElectionManager manager = new ElectionManager(address,
                List.of(address, "peer-a:27017", "peer-b:27017"), config);
        managers.add(manager);
        return manager;
    }

    @Test
    @Timeout(30)
    public void nodeWithIncompleteDataNeverCampaigns() throws Exception {
        ElectionManager manager = node("incomplete:27017", 150);
        AtomicInteger requestsSent = new AtomicInteger();
        manager.setSendVoteRequest((peer, request) -> requestsSent.incrementAndGet());

        // What a partial restore reports: some databases came back, others did not.
        manager.setDataComplete(false);
        manager.start();

        long termBefore = manager.getCurrentTerm();
        Thread.sleep(2000);   // many election timeouts

        assertEquals(0, requestsSent.get(),
                "a node whose restore was incomplete must not campaign - not even a PreVote probe, "
                        + "or it can win a cluster-wide restart where every node reports index 0 "
                        + "and then overwrite the intact nodes via their initial sync");
        assertEquals(ElectionState.FOLLOWER, manager.getState(), "it must stay a follower");
        assertEquals(termBefore, manager.getCurrentTerm(), "and must not inflate the term");
    }

    @Test
    @Timeout(30)
    public void nodeWithCompleteDataCampaignsNormally() throws Exception {
        ElectionManager manager = node("complete:27017", 150);
        AtomicInteger requestsSent = new AtomicInteger();
        manager.setSendVoteRequest((peer, request) -> requestsSent.incrementAndGet());

        manager.setDataComplete(true);
        manager.start();

        long until = System.currentTimeMillis() + 5000;

        while (requestsSent.get() == 0 && System.currentTimeMillis() < until) {
            Thread.sleep(25);
        }

        assertTrue(requestsSent.get() > 0,
                "a node with complete data must still campaign - the guard must not block normal operation");
    }

    @Test
    @Timeout(30)
    public void completingASyncReleasesTheGuard() throws Exception {
        ElectionManager manager = node("recovering:27017", 150);
        AtomicInteger requestsSent = new AtomicInteger();
        manager.setSendVoteRequest((peer, request) -> requestsSent.incrementAndGet());

        manager.setDataComplete(false);
        manager.start();
        Thread.sleep(1000);
        assertEquals(0, requestsSent.get(), "still incomplete - must not campaign yet");

        // An authoritative initial sync gave us a complete copy.
        manager.setDataComplete(true);

        long until = System.currentTimeMillis() + 5000;

        while (requestsSent.get() == 0 && System.currentTimeMillis() < until) {
            Thread.sleep(25);
        }

        assertTrue(requestsSent.get() > 0, "once the data is complete the node must take part again");
    }

    @Test
    @Timeout(30)
    public void anIncompleteNodeStillVotes() throws Exception {
        // Denying votes as well would deadlock a cluster restart: the intact nodes need this
        // node's vote to reach a majority. It must only refrain from winning itself.
        ElectionManager manager = node("voter:27017", 60_000);
        manager.setDataComplete(false);
        manager.start();

        VoteResponse response = manager.handleVoteRequest(
                new VoteRequest(manager.getCurrentTerm() + 1, "peer-a:27017", 5, 1));

        assertTrue(response.isVoteGranted(),
                "an incomplete node must still grant votes, otherwise a cluster restart cannot elect anyone");
        assertFalse(manager.getState() == ElectionState.LEADER, "but it must not be leader itself");
    }
}
