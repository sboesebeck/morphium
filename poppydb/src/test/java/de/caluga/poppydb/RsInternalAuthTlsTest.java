package de.caluga.poppydb;

import static org.junit.jupiter.api.Assertions.assertNotNull;

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

    private int nextPort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
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
    private Cluster bootstrapCluster(SSLContext serverSsl, SSLContext internalSsl) throws Exception {
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
}
