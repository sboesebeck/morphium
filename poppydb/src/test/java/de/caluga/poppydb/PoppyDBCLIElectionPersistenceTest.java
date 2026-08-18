package de.caluga.poppydb;

import de.caluga.poppydb.election.ElectionManager;
import de.caluga.poppydb.election.VoteRequest;
import de.caluga.poppydb.election.VoteResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * #306: the CLI used to call {@code setDumpDirectory()} AFTER {@code configureReplicaSet()},
 * but configureReplicaSet() is where the election-state file path is derived from the dump
 * directory and put into the ElectionConfig - so on the customer environment election-state
 * persistence was silently inactive (no election-state.properties, no log line, neither
 * persisting nor loading ever ran). This test wires a server
 * exactly the way the CLI does and proves that a node with a configured dump directory keeps
 * its term across a "restart" (a second, freshly built server on the same dump directory).
 * The servers are never start()ed: no ports are bound, the election manager itself is enough.
 */
public class PoppyDBCLIElectionPersistenceTest {

    private final List<PoppyDB> servers = new ArrayList<>();

    @AfterEach
    void cleanup() {
        for (PoppyDB srv : servers) {
            try {
                if (srv.getElectionManager() != null) {
                    srv.getElectionManager().stop();
                }
            } catch (Exception e) {
                // ignore
            }
        }
        servers.clear();
    }

    /**
     * Builds a server through the CLI with whatever extra arguments the test needs, so a case
     * can leave out --dump-dir entirely (never started, ports stay unbound).
     */
    private PoppyDB buildViaCli(String... extra) throws Exception {
        List<String> args = new ArrayList<>(List.of(
            "--no-config",
            "--port", "27393",
            "--bind", "localhost",
            "--rs-name", "persistRs",
            "--rs-seed", "localhost:27393,localhost:27394"));
        args.addAll(List.of(extra));
        PoppyDB srv = PoppyDBCLI.configureServer(args.toArray(new String[0]));
        servers.add(srv);
        return srv;
    }

    /**
     * Raft needs currentTerm/votedFor to be durable, and a server that keeps no dumps must be
     * able to have that too (#316): the path used to be derivable only from the dump directory,
     * so a dump-less replica set silently ran without the guarantee.
     */
    @Test
    public void electionStatePathCanBeConfiguredWithoutADumpDirectory(@TempDir Path stateDir) throws Exception {
        File stateFile = new File(stateDir.toFile(), "election-state.properties");
        PoppyDB srv = buildViaCli("--election-state-path", stateFile.getAbsolutePath());

        assertNotNull(srv.getElectionConfigForTest(), "election config must exist for a two-member seed");
        assertTrue(srv.getElectionConfigForTest().isPersistState(),
            "an explicitly configured state path must switch persistence on");
        assertEquals(stateFile.getAbsolutePath(), srv.getElectionConfigForTest().getStatePersistencePath());
    }

    /**
     * An explicit path is an operator decision and must win over the dump-directory default.
     */
    @Test
    public void explicitElectionStatePathWinsOverTheDumpDirectory(@TempDir Path dumpDir, @TempDir Path stateDir) throws Exception {
        File stateFile = new File(stateDir.toFile(), "somewhere-else.properties");
        PoppyDB srv = buildViaCli("--dump-dir", dumpDir.toString(),
                                  "--election-state-path", stateFile.getAbsolutePath());

        assertEquals(stateFile.getAbsolutePath(), srv.getElectionConfigForTest().getStatePersistencePath(),
            "the configured path must not be overwritten by the dump-directory derivation");
    }

    /**
     * Documents the remaining gap rather than hiding it: with neither a dump directory nor an
     * explicit path there is still no persistence - but it is now announced at WARN instead of
     * being inferable only from a missing INFO line.
     */
    @Test
    public void withoutAnyPathPersistenceStaysOffAndIsNotSilent() throws Exception {
        PoppyDB srv = buildViaCli();

        assertNull(srv.getElectionConfigForTest().getStatePersistencePath());
        assertFalse(srv.getElectionConfigForTest().isPersistState());
    }

    private PoppyDB buildViaCliWithDumpDir(Path dumpDir) throws Exception {
        // never started - ports are not bound, so fixed test ports cannot collide
        PoppyDB srv = PoppyDBCLI.configureServer(new String[] {
            "--no-config",
            "--port", "27391",
            "--bind", "localhost",
            "--rs-name", "persistRs",
            "--rs-seed", "localhost:27391,localhost:27392",
            "--dump-dir", dumpDir.toString(),
        });
        servers.add(srv);
        return srv;
    }

    @Test
    public void termConfiguredViaCliSurvivesRestart(@TempDir Path dumpDir) throws Exception {
        PoppyDB first = buildViaCliWithDumpDir(dumpDir);
        ElectionManager em = first.getElectionManager();
        assertNotNull(em, "multi-node RS via CLI args must enable election");
        em.start(); // persisted state is loaded in start(); PreVote keeps the isolated node from bumping its term

        // Adopt term 7 by granting a vote - Raft requires this to be durable immediately
        VoteResponse granted = em.handleVoteRequest(new VoteRequest(7, "localhost:27392", 0, 0));
        assertTrue(granted.isVoteGranted());
        assertEquals(7, em.getCurrentTerm());

        File stateFile = new File(dumpDir.toFile(), "election-state.properties");
        assertTrue(stateFile.exists(),
                "the CLI wiring must persist election state next to the dumps - this is the file "
                + "that silently never appeared on the customer environment (#306)");
        em.stop();

        // "Restart": build a second server through the very same CLI path
        PoppyDB restarted = buildViaCliWithDumpDir(dumpDir);
        assertNotNull(restarted.getElectionManager());
        restarted.getElectionManager().start();
        assertEquals(7, restarted.getElectionManager().getCurrentTerm(),
                "a node with a configured dump directory must keep its term across a restart "
                + "(it came back at term 0 during the #306 ACC incident)");
    }
}
