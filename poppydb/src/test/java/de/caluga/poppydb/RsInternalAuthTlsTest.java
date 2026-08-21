package de.caluga.poppydb;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.io.File;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import javax.net.ssl.SSLContext;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.commands.InsertMongoCommand;
import de.caluga.morphium.driver.wire.PooledDriver;
import de.caluga.morphium.driver.wire.SingleMongoConnection;
import de.caluga.morphium.driver.wire.SslHelper;

/**
 * Reproduces and fixes the RS-internal auth/TLS gap (see
 * docs/superpowers/specs/2026-08-05-poppydb-rs-internal-auth-tls-design.md): with --auth and/or
 * --ssl enabled, {@code ElectionNetworkClient}/{@code ReplicationManager} connected to peers as a
 * plain, unauthenticated client, so no leader could ever be elected on a multi-node RS. Bootstrap
 * pattern follows {@link FastResyncTest}/{@link AuthTlsWireE2ETest} (keytool-generated self-signed
 * cert, reused here as both the servers' identity AND - per the fix - the internal client's
 * pinned trust anchor).
 */
@Tag("server")
public class RsInternalAuthTlsTest {

    private static final String ROOT_USER = "root";
    private static final String ROOT_PASSWORD = "rootpw";

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

    // JVM-wide: ports handed out to ANY test in this fork. new ServerSocket(0) can return the
    // same free port twice in a row (nothing bound it in between) - Map.of over the host list
    // then dies with "duplicate key" (seen on the loaded CI runner in FastResyncTest).
    private static final java.util.Set<Integer> handedOutPorts =
            java.util.concurrent.ConcurrentHashMap.newKeySet();

    private int nextPort() throws Exception {
        for (int i = 0; i < 100; i++) {
            try (ServerSocket socket = new ServerSocket(0)) {
                int p = socket.getLocalPort();

                if (handedOutPorts.add(p)) {
                    return p;
                }
            }
        }

        throw new IllegalStateException("could not allocate a fresh, distinct port");
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

    /** Poll a condition with generous timeout - election/replication is asynchronous. */
    private boolean poll(long timeoutMs, Callable<Boolean> condition) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (Boolean.TRUE.equals(condition.call())) {
                return true;
            }
            Thread.sleep(200);
        }
        return false;
    }

    /** First node (by iteration order) currently reporting isPrimary(), or null on timeout. */
    private PoppyDB waitForAnyPrimary(List<PoppyDB> candidates, long timeoutMs) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            for (PoppyDB node : candidates) {
                if (node.isPrimary()) {
                    return node;
                }
            }
            Thread.sleep(200);
        }
        return null;
    }

    /** Authenticated (+ TLS, if clientSsl != null) client connection as the configured root user. */
    private SingleMongoConnection connectAsRoot(int port, SSLContext clientSsl) throws Exception {
        PooledDriver carrier = new PooledDriver();
        carrier.setConnectionTimeout(3000);
        if (clientSsl != null) {
            carrier.setUseSSL(true);
            carrier.setSslContext(clientSsl);
            carrier.setSslInvalidHostNameAllowed(true); // self-signed test cert, CN mismatch is fine
        }
        SingleMongoConnection con = new SingleMongoConnection();
        con.setCredentials("admin", ROOT_USER, ROOT_PASSWORD);
        con.connect(carrier, "localhost", port);
        return con;
    }

    /** Unauthenticated (+ TLS, if clientSsl != null) client connection for servers with auth disabled. */
    private SingleMongoConnection connectUnauthenticated(int port, SSLContext clientSsl) throws Exception {
        PooledDriver carrier = new PooledDriver();
        carrier.setConnectionTimeout(3000);
        if (clientSsl != null) {
            carrier.setUseSSL(true);
            carrier.setSslContext(clientSsl);
            carrier.setSslInvalidHostNameAllowed(true); // self-signed test cert, CN mismatch is fine
        }
        SingleMongoConnection con = new SingleMongoConnection();
        con.connect(carrier, "localhost", port);
        return con;
    }

    /** Build a self-signed keystore via the JDK's keytool (pattern of AuthTlsWireE2ETest). */
    private static File buildKeystore(Path dir) throws Exception {
        File keystore = dir.resolve("cluster.p12").toFile();
        String keytool = System.getProperty("java.home") + File.separator + "bin" + File.separator + "keytool";
        run(keytool, "-genkeypair", "-alias", "poppy", "-keyalg", "RSA", "-keysize", "2048",
            "-storetype", "PKCS12", "-keystore", keystore.getAbsolutePath(),
            "-storepass", "changeit", "-keypass", "changeit",
            "-dname", "CN=localhost", "-validity", "1");
        return keystore;
    }

    private static void run(String... cmd) throws Exception {
        Process p = new ProcessBuilder(cmd).redirectErrorStream(true).start();
        String out = new String(p.getInputStream().readAllBytes());
        if (p.waitFor() != 0) {
            throw new IllegalStateException("keytool failed: " + String.join(" ", cmd) + "\n" + out);
        }
    }

    private record Cluster(PoppyDB node1, PoppyDB node2, PoppyDB node3,
                           int port1, int port2, int port3) {
        List<PoppyDB> all() {
            return List.of(node1, node2, node3);
        }
    }

    /**
     * Start a 3-node election RS (node1 wins by priority) with --auth and, if {@code serverSsl}
     * is non-null, --ssl. Deliberately does NOT wait for a leader - that is what
     * {@link #leaderElectedWithAuthAndSsl} tests.
     */
    /**
     * Retries a whole cluster build when a node start dies with "Address already in use".
     * {@link #nextPort()} is inherently check-then-act: it binds port 0, reads the assigned
     * port and CLOSES the socket again - under the loaded CI runner (five phases plus module
     * tests in parallel) another process grabs that port between the probe and the node's
     * actual bind often enough to flake. The ports are baked into every node's replica-set
     * config, so a retry has to rebuild the whole cluster with fresh ports - a per-node retry
     * cannot work. Half-started nodes of the failed attempt are shut down first.
     */
    private <T> T retryOnBindException(java.util.concurrent.Callable<T> clusterBuild) throws Exception {
        for (int attempt = 1; ; attempt++) {
            try {
                return clusterBuild.call();
            } catch (Exception e) {
                boolean bind = false;

                for (Throwable t = e; t != null; t = t.getCause()) {
                    if (t instanceof java.net.BindException
                            || (t.getMessage() != null && t.getMessage().contains("Address already in use"))) {
                        bind = true;
                        break;
                    }
                }

                if (!bind || attempt >= 3) {
                    throw e;
                }

                for (int i = nodes.size() - 1; i >= 0; i--) {
                    try {
                        nodes.get(i).shutdown();
                    } catch (Exception ignored) {
                    }
                }

                nodes.clear();
                Thread.sleep(250);
            }
        }
    }

    private Cluster bootstrapCluster(SSLContext serverSsl, SSLContext internalSsl) throws Exception {
        return retryOnBindException(() -> bootstrapClusterOnce(serverSsl, internalSsl));
    }

    private Cluster bootstrapClusterOnce(SSLContext serverSsl, SSLContext internalSsl) throws Exception {
        int port1 = nextPort();
        int port2 = nextPort();
        int port3 = nextPort();
        PoppyDB node1 = new PoppyDB(port1, "localhost", 20, 5);
        PoppyDB node2 = new PoppyDB(port2, "localhost", 20, 5);
        PoppyDB node3 = new PoppyDB(port3, "localhost", 20, 5);
        var hosts = List.of("localhost:" + port1, "localhost:" + port2, "localhost:" + port3);
        var prio = Map.of("localhost:" + port1, 100,
                          "localhost:" + port2, 75,
                          "localhost:" + port3, 50);

        for (PoppyDB node : List.of(node1, node2, node3)) {
            node.setAuthRequired(true);
            node.setRootUser(ROOT_USER, ROOT_PASSWORD);
            if (serverSsl != null) {
                node.setSslContext(serverSsl);
                node.setSslEnabled(true);
                node.setInternalSslContext(internalSsl);
            }
            node.configureReplicaSet("rsAuthTls", hosts, prio, true, null);
        }

        startServer(node1, port1);
        startServer(node2, port2);
        startServer(node3, port3);

        return new Cluster(node1, node2, node3, port1, port2, port3);
    }

    @Test
    public void leaderElectedWithAuthAndSsl(@TempDir Path dir) throws Exception {
        File keystore = buildKeystore(dir);
        SSLContext serverSsl = SslHelper.createServerSslContext(keystore.getAbsolutePath(), "changeit");

        SSLContext internalSsl = SslHelper.createClientSslContext(keystore.getAbsolutePath(), "changeit");
        Cluster c = bootstrapCluster(serverSsl, internalSsl);

        PoppyDB leader = waitForAnyPrimary(c.all(), 20_000);

        assertNotNull(leader, "a leader must be elected even with --auth and --ssl enabled");
    }

    @Test
    public void dataAndLoginWorkWithAuthAndSsl(@TempDir Path dir) throws Exception {
        File keystore = buildKeystore(dir);
        SSLContext serverSsl = SslHelper.createServerSslContext(keystore.getAbsolutePath(), "changeit");
        SSLContext internalSsl = SslHelper.createClientSslContext(keystore.getAbsolutePath(), "changeit");
        Cluster c = bootstrapCluster(serverSsl, internalSsl);

        PoppyDB leader = waitForAnyPrimary(c.all(), 20_000);
        assertNotNull(leader, "a leader must be elected even with --auth and --ssl enabled");

        // Write as the authenticated root user over TLS - the ordinary client-facing path,
        // unaffected by this fix, used here only to get data onto the leader.
        SingleMongoConnection con = connectAsRoot(leader.getPort(), internalSsl);
        try {
            InsertMongoCommand insert = new InsertMongoCommand(con);
            insert.setDb("authtlsdb").setColl("docs");
            insert.setDocuments(List.of(Doc.of("_id", "doc-1", "marker", "auth-tls-rs")));
            Map<String, Object> result = insert.execute();
            assertEquals(1, ((Number) result.get("n")).intValue(), "insert on the leader must succeed: " + result);
        } finally {
            con.close();
        }

        assertTrue(poll(30_000, () -> c.all().stream().allMatch(n ->
                n.getDriver().count("authtlsdb", "docs", Doc.of(), null, null) == 1)),
                "the write on the leader must replicate to every node, primary included");

        // The client-facing SCRAM+TLS login path itself must be completely unaffected by this fix.
        for (PoppyDB node : c.all()) {
            SingleMongoConnection loginCheck = connectAsRoot(node.getPort(), internalSsl);
            loginCheck.close(); // reaching here without an exception IS the assertion
        }
    }

    @Test
    public void authOnlyConverges() throws Exception {
        Cluster c = bootstrapCluster(null, null); // no SSL at all, auth still on inside bootstrapCluster

        PoppyDB leader = waitForAnyPrimary(c.all(), 20_000);
        assertNotNull(leader, "--auth alone must not prevent election");

        SingleMongoConnection con = connectAsRoot(leader.getPort(), null);
        try {
            InsertMongoCommand insert = new InsertMongoCommand(con);
            insert.setDb("authonlydb").setColl("docs");
            insert.setDocuments(List.of(Doc.of("_id", "doc-1")));
            Map<String, Object> result = insert.execute();
            assertEquals(1, ((Number) result.get("n")).intValue(), "insert on the leader must succeed: " + result);
        } finally {
            con.close();
        }

        assertTrue(poll(30_000, () -> c.all().stream().allMatch(n ->
                n.getDriver().count("authonlydb", "docs", Doc.of(), null, null) == 1)),
                "--auth alone must not prevent replication");
    }

    @Test
    public void sslOnlyConverges(@TempDir Path dir) throws Exception {
        // bootstrapCluster always turns auth on when serverSsl != null in this harness (see
        // Task 2), so exercise "ssl only" via a second, auth-free bootstrap helper here instead
        // of reusing bootstrapCluster - this stays a small, self-contained test method.
        File keystore = buildKeystore(dir);
        SSLContext serverSsl = SslHelper.createServerSslContext(keystore.getAbsolutePath(), "changeit");
        SSLContext internalSsl = SslHelper.createClientSslContext(keystore.getAbsolutePath(), "changeit");

        List<PoppyDB> all = retryOnBindException(() -> {
            int port1 = nextPort();
            int port2 = nextPort();
            int port3 = nextPort();
            PoppyDB node1 = new PoppyDB(port1, "localhost", 20, 5);
            PoppyDB node2 = new PoppyDB(port2, "localhost", 20, 5);
            PoppyDB node3 = new PoppyDB(port3, "localhost", 20, 5);
            var hosts = List.of("localhost:" + port1, "localhost:" + port2, "localhost:" + port3);
            var prio = Map.of("localhost:" + port1, 100, "localhost:" + port2, 75, "localhost:" + port3, 50);
            for (PoppyDB node : List.of(node1, node2, node3)) {
                node.setSslContext(serverSsl);
                node.setSslEnabled(true);
                node.setInternalSslContext(internalSsl);
                node.configureReplicaSet("rsSslOnly", hosts, prio, true, null);
            }
            startServer(node1, port1);
            startServer(node2, port2);
            startServer(node3, port3);
            return List.of(node1, node2, node3);
        });

        PoppyDB leader = waitForAnyPrimary(all, 20_000);
        assertNotNull(leader, "--ssl alone must not prevent election");

        SingleMongoConnection con = connectUnauthenticated(leader.getPort(), internalSsl);
        try {
            InsertMongoCommand insert = new InsertMongoCommand(con);
            insert.setDb("sslonlydb").setColl("docs");
            insert.setDocuments(List.of(Doc.of("_id", "doc-1")));
            insert.execute();
        } finally {
            con.close();
        }

        assertTrue(poll(30_000, () -> all.stream().allMatch(n ->
                n.getDriver().count("sslonlydb", "docs", Doc.of(), null, null) == 1)),
                "--ssl alone must not prevent replication");
    }

    @Test
    public void wrongRootPasswordNeverJoinsCluster(@TempDir Path dir) throws Exception {
        File keystore = buildKeystore(dir);
        SSLContext serverSsl = SslHelper.createServerSslContext(keystore.getAbsolutePath(), "changeit");
        SSLContext internalSsl = SslHelper.createClientSslContext(keystore.getAbsolutePath(), "changeit");

        List<PoppyDB> cluster = retryOnBindException(() -> {
            int port1 = nextPort();
            int port2 = nextPort();
            int port3 = nextPort();
            PoppyDB n1 = new PoppyDB(port1, "localhost", 20, 5); // priority 100, wins the election
            PoppyDB n2 = new PoppyDB(port2, "localhost", 20, 5); // priority 75, correctly configured
            PoppyDB n3 = new PoppyDB(port3, "localhost", 20, 5); // priority 50, WRONG password
            var hosts = List.of("localhost:" + port1, "localhost:" + port2, "localhost:" + port3);
            var prio = Map.of("localhost:" + port1, 100, "localhost:" + port2, 75, "localhost:" + port3, 50);

            for (PoppyDB node : List.of(n1, n2)) {
                node.setAuthRequired(true);
                node.setRootUser(ROOT_USER, ROOT_PASSWORD);
                node.setSslContext(serverSsl);
                node.setSslEnabled(true);
                node.setInternalSslContext(internalSsl);
                node.configureReplicaSet("rsWrongPw", hosts, prio, true, null);
            }
            n3.setAuthRequired(true);
            n3.setRootUser(ROOT_USER, "totally-different-password"); // config drift
            n3.setSslContext(serverSsl);
            n3.setSslEnabled(true);
            n3.setInternalSslContext(internalSsl);
            n3.configureReplicaSet("rsWrongPw", hosts, prio, true, null);

            startServer(n1, port1);
            startServer(n2, port2);
            startServer(n3, port3);
            return List.of(n1, n2, n3);
        });
        PoppyDB node1 = cluster.get(0);
        PoppyDB node2 = cluster.get(1);
        PoppyDB node3 = cluster.get(2);

        long deadline = System.currentTimeMillis() + 20_000;
        while (!node1.isPrimary() && System.currentTimeMillis() < deadline) {
            Thread.sleep(200);
        }
        assertTrue(node1.isPrimary(), "the highest-priority, correctly-configured node must win the election");

        SingleMongoConnection con = connectAsRoot(node1.getPort(), internalSsl);
        try {
            InsertMongoCommand insert = new InsertMongoCommand(con);
            insert.setDb("wrongpwdb").setColl("docs");
            insert.setDocuments(List.of(Doc.of("_id", "doc-1")));
            insert.execute();
        } finally {
            con.close();
        }

        assertTrue(poll(30_000, () -> node2.getDriver().count("wrongpwdb", "docs", Doc.of(), null, null) == 1),
                "the correctly-configured secondary must still receive replicated data");

        // Give node3 a generous window to (wrongly) catch up, then assert it never did: its
        // internal client can't authenticate against node1/node2 with the wrong password, so it
        // must stay isolated rather than silently joining unauthenticated.
        Thread.sleep(5_000);
        assertEquals(0, node3.getDriver().count("wrongpwdb", "docs", Doc.of(), null, null),
                "a node with the wrong root password must never receive replicated data");
        assertFalse(node3.isPrimary(), "a node with the wrong root password must never become primary");
    }
}
