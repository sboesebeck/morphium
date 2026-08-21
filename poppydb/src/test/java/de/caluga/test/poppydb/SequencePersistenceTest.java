package de.caluga.test.poppydb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.poppydb.PoppyDB;
import de.caluga.test.mongo.suite.data.UncachedObject;

/**
 * #329 follow-up: the change-stream sequence counter must survive a restart. Without this a
 * restarted server issues tokens from 0 again ("foreign or reset sequence space"): every
 * client's resume token is suddenly beyond newest, and sequence comparisons between peers
 * (the destructive-resync guard) become meaningless - a healthy restarted primary is
 * indistinguishable from a stale one, which livelocked the ACC secondaries on 2026-08-21.
 * Persisted next to the dumps (sequence-state.properties), restored monotonically with
 * headroom for increments a crash may have left unpersisted.
 */
@Tag("server")
public class SequencePersistenceTest {

    private int freePort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private void startServer(PoppyDB srv, int port) throws Exception {
        long deadline = System.currentTimeMillis() + 10_000;
        while (true) {
            try {
                srv.start();
                break;
            } catch (Exception e) {
                if (System.currentTimeMillis() > deadline) {
                    throw e;
                }
                Thread.sleep(100);
            }
        }
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

    private Morphium connect(int port) {
        MorphiumConfig cfg = new MorphiumConfig();
        cfg.clusterSettings().setHostSeed("localhost:" + port);
        cfg.connectionSettings().setDatabase("seq_persist_test");
        cfg.connectionSettings().setMaxConnections(10);
        return new Morphium(cfg);
    }

    @Test
    public void sequenceSurvivesOrderlyRestart(@TempDir File dumpDir) throws Exception {
        int port = freePort();
        PoppyDB srv = new PoppyDB(port, "localhost", 100, 1);
        srv.setDumpDirectory(dumpDir);
        startServer(srv, port);

        long seqBefore;
        try (Morphium m = connect(port)) {
            var bulk = new ArrayList<UncachedObject>();
            for (int i = 0; i < 200; i++) {
                bulk.add(new UncachedObject("seq", i));
            }
            m.storeList(bulk);
            seqBefore = srv.getDriver().getChangeStreamSequence();
            assertTrue(seqBefore > 0, "writes must have advanced the change-stream sequence");
        } finally {
            srv.shutdown(); // final dump persists databases AND the sequence state
        }

        assertTrue(new File(dumpDir, "sequence-state.properties").exists(),
            "shutdown's final dump must persist the change-stream sequence next to the dumps");

        PoppyDB restarted = new PoppyDB(port, "localhost", 100, 1);
        restarted.setDumpDirectory(dumpDir);
        try {
            restarted.restoreFromDump();
            long seqAfterRestore = restarted.getDriver().getChangeStreamSequence();
            assertTrue(seqAfterRestore >= seqBefore,
                "restored sequence (" + seqAfterRestore + ") must not fall below the pre-restart value ("
                + seqBefore + ") - a reset sequence space invalidates every client's resume token (#329)");
        } finally {
            restarted.shutdown();
        }
    }

    @Test
    public void restoreWithoutStateFileKeepsSequenceUntouched(@TempDir File dumpDir) throws Exception {
        // backward compatibility: dumps from a version without sequence persistence
        int port = freePort();
        PoppyDB srv = new PoppyDB(port, "localhost", 100, 1);
        srv.setDumpDirectory(dumpDir);
        try {
            srv.restoreFromDump(); // empty directory, no state file - must not throw
            assertEquals(0, srv.getDriver().getChangeStreamSequence(),
                "no persisted state - the sequence starts at 0 like before");
        } finally {
            srv.shutdown();
        }
    }
}
