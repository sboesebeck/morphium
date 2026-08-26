package de.caluga.test.poppydb;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.commands.CreateIndexesCommand;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import de.caluga.poppydb.PoppyDB;
import de.caluga.test.mongo.suite.data.UncachedObject;

/**
 * #340 at server level: the full-cluster-restart case. Every node writes its final dump on
 * shutdown; when ALL nodes go down, the restarted nodes have no peer left to get their indexes
 * from via initial sync - the dump is the only source. Before #340 the dump carried no index
 * definitions at all: data came back, {@code getIndexes()} showed only {@code _id_1}, TTL
 * indexes silently stopped working (the ACC {@code jef_servacc} collections grew unbounded).
 *
 * <p>Models exactly the reproduction from the issue: create a TTL index over the wire, stop
 * the server (final dump), bring up a NEW server instance on the same dump directory, restore.
 */
@Tag("server")
public class IndexDumpPersistenceTest {

    private static final String DB = "index_dump_test";

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
        cfg.connectionSettings().setDatabase(DB);
        cfg.connectionSettings().setMaxConnections(10);
        return new Morphium(cfg);
    }

    private static Map<String, Object> indexByName(List<Map<String, Object>> indexes, String name) {
        for (Map<String, Object> idx : indexes) {
            Map<?, ?> opts = (Map<?, ?>) idx.get("$options");
            if (opts != null && name.equals(opts.get("name"))) {
                return idx;
            }
        }
        return null;
    }

    @Test
    public void indexesSurviveFullRestartViaDump(@TempDir File dumpDir) throws Exception {
        int port = freePort();
        PoppyDB srv = new PoppyDB(port, "localhost", 100, 1);
        srv.setDumpDirectory(dumpDir);
        startServer(srv, port);

        String coll;
        try (Morphium m = connect(port)) {
            m.store(new UncachedObject("indexed", 1));
            coll = m.getMapper().getCollectionName(UncachedObject.class);

            // TTL + unique index over the wire, like any client would create them
            var con = m.getDriver().getPrimaryConnection(null);
            CreateIndexesCommand cmd = new CreateIndexesCommand(con).setDb(DB).setColl(coll)
                    .setIndexes(List.of(
                        Doc.of("key", Doc.of("counter", 1), "name", "ttl_counter", "expireAfterSeconds", 3600),
                        Doc.of("key", Doc.of("str_value", 1), "name", "uniq_value", "unique", true)));
            try {
                cmd.execute();
            } finally {
                cmd.releaseConnection();
            }

            List<Map<String, Object>> before = srv.getDriver().getIndexes(DB, coll);
            assertNotNull(indexByName(before, "ttl_counter"), "sanity: TTL index must exist before shutdown");
        } finally {
            srv.shutdown(); // writes the final dump
        }

        // full restart: a brand-new server instance, no peer to initial-sync indexes from
        PoppyDB restarted = new PoppyDB(port, "localhost", 100, 1);
        restarted.setDumpDirectory(dumpDir);
        try {
            InMemoryDriver.DirectoryRestoreResult result = restarted.restoreFromDump();
            assertTrue(result.isComplete(), "restore must be complete");
            assertFalse(result.hasIndexFailures(), "no index failures expected: " + result.getFailedIndexes());

            List<Map<String, Object>> after = restarted.getDriver().getIndexes(DB, coll);
            Map<String, Object> ttl = indexByName(after, "ttl_counter");
            Map<String, Object> uniq = indexByName(after, "uniq_value");
            assertNotNull(ttl, "#340: the TTL index must survive a full restart via the dump - got only " + after);
            assertNotNull(uniq, "#340: the unique index must survive a full restart via the dump");
            assertTrue(Boolean.TRUE.equals(((Map<?, ?>) uniq.get("$options")).get("unique")),
                    "unique option must survive the dump round trip");
        } finally {
            restarted.shutdown();
        }
    }
}
