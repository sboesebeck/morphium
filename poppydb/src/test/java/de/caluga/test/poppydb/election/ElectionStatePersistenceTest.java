package de.caluga.test.poppydb.election;

import de.caluga.poppydb.election.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * #306 point 5: Raft requires currentTerm and votedFor to survive restarts. During the ACC
 * incident a node came back at term 0 (no persistence) and contributed to the term churn.
 * The state lives in a small properties file (conventionally next to the dump directory),
 * written atomically on every term/votedFor change; a node WITHOUT a state file (first start,
 * or persistence newly enabled) must still start cleanly at term 0.
 *
 * <p>#306 P1-1 sharpens the durability contract in two ways covered below:
 * <ul>
 *   <li>a vote (and a candidacy's self-vote) may only be confirmed once the state has actually
 *       been written - a swallowed persist failure lets a crashed node forget its vote and vote
 *       a second time in the same term, i.e. two leaders in one term;</li>
 *   <li>a state file that EXISTS but cannot be read is not the same as a missing one: the node
 *       may have voted at any term, so it must stay out of elections entirely instead of
 *       resetting to term 0 (PreVote protects against term inflation, not double voting).</li>
 * </ul>
 */
public class ElectionStatePersistenceTest {

    private final List<ElectionManager> managers = new ArrayList<>();

    @TempDir
    Path tempDir;

    @AfterEach
    void cleanup() {
        for (ElectionManager manager : managers) {
            try {
                manager.stop();
            } catch (Exception e) {
                // ignore
            }
        }
        managers.clear();
    }

    private ElectionConfig persistingConfig(Path stateFile) {
        return new ElectionConfig()
                .setElectionTimeoutMinMs(60_000)   // never campaigns during the test
                .setElectionTimeoutMaxMs(60_000)
                .setPersistState(true)
                .setStatePersistencePath(stateFile.toString());
    }

    private ElectionManager node(ElectionConfig config) {
        ElectionManager manager = new ElectionManager("persist-node:27017",
                List.of("persist-node:27017", "candidate:27017", "other:27017"), config);
        managers.add(manager);
        manager.start();
        return manager;
    }

    @Test
    void termAndVotedForSurviveRestart() throws Exception {
        Path stateFile = tempDir.resolve("election-state.properties");

        ElectionManager first = node(persistingConfig(stateFile));
        // Grant a real vote at term 7 - persists both the adopted term and votedFor
        VoteResponse granted = first.handleVoteRequest(new VoteRequest(7, "candidate:27017", 0, 0));
        assertTrue(granted.isVoteGranted());
        assertEquals(7, first.getCurrentTerm());
        first.stop();

        assertTrue(Files.exists(stateFile), "state file must have been written");

        // "Restart": a fresh manager with the same persistence path
        ElectionManager restarted = node(persistingConfig(stateFile));
        assertEquals(7, restarted.getCurrentTerm(),
                "currentTerm must survive the restart (came back at term 0 during the #306 incident)");

        // votedFor must survive too: a DIFFERENT candidate asking again for term 7 after the
        // restart must be denied - the vote for term 7 was already given away before it.
        VoteResponse denied = restarted.handleVoteRequest(new VoteRequest(7, "other:27017", 0, 0));
        assertFalse(denied.isVoteGranted(),
                "the restored votedFor must prevent double-voting in the same term across a restart");

        // ... while the ORIGINAL candidate may still be re-granted (Raft: same candidateId)
        VoteResponse regranted = restarted.handleVoteRequest(new VoteRequest(7, "candidate:27017", 0, 0));
        assertTrue(regranted.isVoteGranted(), "the restored votedFor must still allow the original candidate");
    }

    @Test
    void nodeWithoutStateFileStartsCleanly() throws Exception {
        Path stateFile = tempDir.resolve("never-written.properties");

        ElectionManager manager = node(persistingConfig(stateFile));

        assertEquals(0, manager.getCurrentTerm(), "no state file means a clean start at term 0");
        assertEquals(ElectionState.FOLLOWER, manager.getState());

        // The missing-file case is the harmless one (first start, persistence newly enabled):
        // the node has provably never voted, so it takes part in elections normally - unlike
        // the existing-but-unreadable case below.
        VoteResponse granted = manager.handleVoteRequest(new VoteRequest(7, "candidate:27017", 0, 0));
        assertTrue(granted.isVoteGranted(), "a first-start node (no state file) must vote normally");
    }

    @Test
    void unreadableStateFileBlocksElectionParticipation() throws Exception {
        // The file EXISTS but cannot be parsed - this node may have voted at ANY term, so
        // "reset to term 0 and carry on" reopens the double-voting hole persistence exists to
        // close (#306 P1-1: PreVote covers term inflation, not a second vote in the same term).
        // The node must start (serving data is fine) but stay out of elections entirely.
        Path stateFile = tempDir.resolve("corrupt.properties");
        Files.writeString(stateFile, "currentTerm=not-a-number\n");

        ElectionManager manager = node(persistingConfig(stateFile));
        assertEquals(ElectionState.FOLLOWER, manager.getState(), "the node itself must still start");

        VoteResponse real = manager.handleVoteRequest(new VoteRequest(7, "candidate:27017", 0, 0));
        assertFalse(real.isVoteGranted(),
                "a node whose existing election state file is unreadable may already have voted in this "
                        + "term - it must not vote (again) until an operator resolves the file");

        VoteResponse pre = manager.handleVoteRequest(
                new VoteRequest(7, "candidate:27017", 0, 0).setPreVote(true));
        assertFalse(pre.isVoteGranted(), "PreVote probes must be denied for the same reason");
    }

    @Test
    void emptyStateFileIsQuarantinedNotTreatedAsTermZero() throws Exception {
        // Properties.load() happily parses an EMPTY (e.g. truncated-to-nothing) file, and a
        // getProperty default would turn it into "term 0, never voted" - the exact reset the
        // unreadable-file quarantine exists to prevent. An existing file that lost its content
        // is as suspicious as one that cannot be parsed.
        Path stateFile = tempDir.resolve("empty.properties");
        Files.writeString(stateFile, "");

        ElectionManager manager = node(persistingConfig(stateFile));

        VoteResponse response = manager.handleVoteRequest(new VoteRequest(7, "candidate:27017", 0, 0));
        assertFalse(response.isVoteGranted(),
                "an existing but EMPTY state file must be quarantined like an unparsable one - "
                        + "treating it as term 0 reopens the double-voting hole");
    }

    @Test
    void truncatedStateFileMissingVotedForIsQuarantined() throws Exception {
        // A partially preserved file that kept its currentTerm line but lost the votedFor line
        // parses fine - but the node may have voted in that very term. The persisted schema is
        // mandatory: every write contains currentTerm, votedFor (explicitly empty when null)
        // and a checksum, so a file missing any of them is not OUR complete write.
        Path stateFile = tempDir.resolve("truncated.properties");
        Files.writeString(stateFile, "currentTerm=7\n");

        ElectionManager manager = node(persistingConfig(stateFile));

        VoteResponse response = manager.handleVoteRequest(new VoteRequest(7, "other:27017", 0, 0));
        assertFalse(response.isVoteGranted(),
                "a state file with currentTerm but without votedFor/checksum is an incomplete write "
                        + "- the vote for term 7 may already be gone, it must not be re-granted");
    }

    @Test
    void tamperedStateFileFailsChecksumAndIsQuarantined() throws Exception {
        // Full roundtrip, then flip the persisted term: the checksum no longer matches, so the
        // file must be treated as unreadable instead of trusted.
        Path stateFile = tempDir.resolve("tampered.properties");

        ElectionManager first = node(persistingConfig(stateFile));
        assertTrue(first.handleVoteRequest(new VoteRequest(7, "candidate:27017", 0, 0)).isVoteGranted());
        first.stop();

        String content = Files.readString(stateFile);
        Files.writeString(stateFile, content.replace("currentTerm=7", "currentTerm=3"));

        ElectionManager restarted = node(persistingConfig(stateFile));
        VoteResponse response = restarted.handleVoteRequest(new VoteRequest(7, "other:27017", 0, 0));
        assertFalse(response.isVoteGranted(),
                "a state file whose checksum does not match its content must be quarantined");
    }

    @Test
    void heartbeatMustNotOverwriteAnUnreadableStateFile() throws Exception {
        // The quarantine must not be self-defeating: after an unreadable file is detected, a
        // heartbeat with a higher term used to run becomeFollower() -> persistElectionState(),
        // OVERWRITING the broken file with the made-up in-memory state. After the next restart
        // the file reads fine, the node re-enters elections automatically - and the unknown
        // earlier vote is gone. While quarantined, the file must be left untouched (it is the
        // operator's evidence) and the quarantine must survive restarts.
        Path stateFile = tempDir.resolve("corrupt-quarantine.properties");
        String corruptContent = "currentTerm=not-a-number\n";
        Files.writeString(stateFile, corruptContent);

        ElectionManager manager = node(persistingConfig(stateFile));

        // Heartbeats themselves stay welcome (the node keeps following and serving data)...
        manager.handleAppendEntries(AppendEntriesRequest.heartbeat(5, "leader:27017", 10, 1, 10));
        assertEquals(5, manager.getCurrentTerm(), "the node still follows the leader in memory");

        // ...but the term adoption they trigger must not have touched the quarantined file.
        assertEquals(corruptContent, Files.readString(stateFile),
                "a quarantined (unreadable) state file must never be overwritten with made-up "
                        + "in-memory state - that silently lifts the quarantine on the next restart");

        manager.stop();
        ElectionManager restarted = node(persistingConfig(stateFile));
        VoteResponse response = restarted.handleVoteRequest(new VoteRequest(9, "candidate:27017", 0, 0));
        assertFalse(response.isVoteGranted(),
                "the quarantine must still hold after a restart - only the operator resolves it");
    }

    @Test
    void unreadableStateFileBlocksCandidacy() throws Exception {
        Path stateFile = tempDir.resolve("corrupt-candidate.properties");
        Files.writeString(stateFile, "currentTerm=not-a-number\n");

        ElectionConfig config = new ElectionConfig()
                .setElectionTimeoutMinMs(100)
                .setElectionTimeoutMaxMs(150)
                .setPersistState(true)
                .setStatePersistencePath(stateFile.toString());
        ElectionManager manager = new ElectionManager("persist-node:27017",
                List.of("persist-node:27017", "candidate:27017", "other:27017"), config);
        managers.add(manager);

        AtomicInteger requestsSent = new AtomicInteger();
        manager.setSendVoteRequest((peer, request) -> requestsSent.incrementAndGet());
        manager.start();

        Thread.sleep(1000);   // many election timeouts

        assertEquals(0, requestsSent.get(),
                "a node whose existing election state file is unreadable must not campaign either - "
                        + "its own currentTerm is unknown, so any campaign runs on made-up state");
    }

    @Test
    void voteIsDeniedWhenVotedForCannotBePersisted() throws Exception {
        // "Raft: votedFor must be durable before the response leaves" - so when the write
        // fails, the response must be a denial, not a grant backed by nothing (#306 P1-1: a
        // crash after a swallowed persist failure lets this node vote twice in the same term).
        // The parent of the state path is a regular FILE, so every write attempt fails.
        Path blocker = tempDir.resolve("blocker");
        Files.writeString(blocker, "not a directory");
        Path stateFile = blocker.resolve("state.properties");

        ElectionManager manager = node(persistingConfig(stateFile));

        VoteResponse response = manager.handleVoteRequest(new VoteRequest(7, "candidate:27017", 0, 0));
        assertFalse(response.isVoteGranted(),
                "a vote whose votedFor could not be made durable must be denied - a granted-but-"
                        + "unpersisted vote is forgotten on crash and enables double voting in the same term");
    }

    @Test
    void candidacyIsAbortedWhenStateCannotBePersisted() throws Exception {
        // The same durability rule applies to the candidate's own self-vote: without a durable
        // (term, votedFor=self) record, a crashed candidate can grant its vote to somebody else
        // in the very term it campaigned in.
        Path blocker = tempDir.resolve("blocker-candidate");
        Files.writeString(blocker, "not a directory");
        Path stateFile = blocker.resolve("state.properties");

        ElectionConfig config = new ElectionConfig()
                .setElectionTimeoutMinMs(100)
                .setElectionTimeoutMaxMs(150)
                .setPersistState(true)
                .setStatePersistencePath(stateFile.toString());
        ElectionManager manager = new ElectionManager("persist-node:27017",
                List.of("persist-node:27017", "candidate:27017", "other:27017"), config);
        managers.add(manager);

        CountDownLatch becameLeader = new CountDownLatch(1);
        manager.setOnLeadershipChange(isLeader -> {
            if (isLeader) {
                becameLeader.countDown();
            }
        });
        // Every peer would grant everything - only the persist failure stands between this
        // node and leadership.
        manager.setSendVoteRequest((peer, request) -> manager.handleVoteResponse(peer, request,
                new VoteResponse(request.getTerm(), true, peer)));
        manager.start();

        assertFalse(becameLeader.await(1500, TimeUnit.MILLISECONDS),
                "a node that cannot persist its self-vote must not win an election");
        assertEquals(0, manager.getCurrentTerm(),
                "the aborted candidacy must also roll the un-persistable term increment back");
    }

    @Test
    void persistenceDisabledWritesNothing() throws Exception {
        Path stateFile = tempDir.resolve("disabled.properties");
        ElectionConfig config = new ElectionConfig()
                .setElectionTimeoutMinMs(60_000)
                .setElectionTimeoutMaxMs(60_000)
                .setPersistState(false)
                .setStatePersistencePath(stateFile.toString());

        ElectionManager manager = node(config);
        manager.handleVoteRequest(new VoteRequest(7, "candidate:27017", 0, 0));

        assertFalse(Files.exists(stateFile), "persistState=false must not write any state file");
    }
}
