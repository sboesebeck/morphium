package de.caluga.test.poppydb.election;

import de.caluga.poppydb.election.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * #306 point 5: Raft requires currentTerm and votedFor to survive restarts. During the ACC
 * incident a node came back at term 0 (no persistence) and contributed to the term churn.
 * The state lives in a small properties file (conventionally next to the dump directory),
 * written atomically on every term/votedFor change; a node WITHOUT a state file - or with a
 * corrupt one - must still start cleanly at term 0 (PreVote makes that non-disruptive).
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
    void nodeWithoutStateFileStartsCleanly() {
        Path stateFile = tempDir.resolve("never-written.properties");

        ElectionManager manager = node(persistingConfig(stateFile));

        assertEquals(0, manager.getCurrentTerm(), "no state file means a clean start at term 0");
        assertEquals(ElectionState.FOLLOWER, manager.getState());
    }

    @Test
    void corruptStateFileStartsCleanly() throws Exception {
        Path stateFile = tempDir.resolve("corrupt.properties");
        Files.writeString(stateFile, "currentTerm=not-a-number\n");

        ElectionManager manager = node(persistingConfig(stateFile));

        assertEquals(0, manager.getCurrentTerm(), "a corrupt state file must not prevent a clean start");
        assertEquals(ElectionState.FOLLOWER, manager.getState());
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
