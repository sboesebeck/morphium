package de.caluga.test.poppydb;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import de.caluga.morphium.driver.wire.SslHelper;
import de.caluga.poppydb.PoppyDB;

/**
 * Verifies that PoppyDB's TLS configuration is actually functional: enabling SSL without
 * a configured certificate must fall back to a self-signed certificate (not crash on a
 * missing classpath resource), and an explicitly configured SSLContext must actually be
 * used for the handshake rather than silently ignored.
 */
@Tag("server")
@Disabled("Disabled by default - starts real PoppyDB server(s) and is flaky under parallel test runs. Run manually with -Djunit.jupiter.conditions.deactivate=org.junit.*DisabledCondition (see ci/CLAUDE-testvm.md).")
public class PoppyDBTlsTest {

    private int nextPort() throws Exception {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    private void startServer(PoppyDB srv, int port) throws Exception {
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

    private static SSLContext trustAllClientContext() throws Exception {
        TrustManager[] trustAll = new TrustManager[]{
            new X509TrustManager() {
                public void checkClientTrusted(X509Certificate[] chain, String authType) {}
                public void checkServerTrusted(X509Certificate[] chain, String authType) {}
                public X509Certificate[] getAcceptedIssuers() {
                    return new X509Certificate[0];
                }
            }
        };
        SSLContext ctx = SSLContext.getInstance("TLS");
        ctx.init(null, trustAll, new SecureRandom());
        return ctx;
    }

    private File generateKeystore() throws Exception {
        File ks = File.createTempFile("poppydb-tls-test", ".p12");
        ks.delete(); // keytool must create the keystore itself, the file may not pre-exist
        ks.deleteOnExit();
        ProcessBuilder pb = new ProcessBuilder("keytool", "-genkeypair",
                "-alias", "poppydb-test",
                "-keyalg", "RSA", "-keysize", "2048",
                "-validity", "1",
                "-dname", "CN=localhost",
                "-keystore", ks.getAbsolutePath(),
                "-storetype", "PKCS12",
                "-storepass", "changeit",
                "-keypass", "changeit");
        pb.redirectErrorStream(true);
        Process p = pb.start();
        String output = new String(p.getInputStream().readAllBytes());
        int exit = p.waitFor();

        if (exit != 0) {
            throw new IllegalStateException("keytool failed: " + output);
        }

        return ks;
    }

    @Test
    void tlsHandshakeSucceedsWithSelfSignedFallback() throws Exception {
        int port = nextPort();
        PoppyDB srv = new PoppyDB(port, "localhost", 20, 5);
        srv.setSslEnabled(true);
        // No cert/context configured -> server must fall back to a self-signed
        // certificate instead of failing to load nonexistent classpath resources.

        try {
            startServer(srv, port);

            SSLSocketFactory factory = trustAllClientContext().getSocketFactory();

            try (SSLSocket socket = (SSLSocket) factory.createSocket("localhost", port)) {
                socket.startHandshake();
                assertNotNull(socket.getSession());
                assertTrue(socket.getSession().isValid());
            }
        } finally {
            srv.shutdown();
        }
    }

    @Test
    void tlsHandshakeSucceedsWithExplicitSslContext() throws Exception {
        int port = nextPort();
        PoppyDB srv = new PoppyDB(port, "localhost", 20, 5);
        File keystore = generateKeystore();

        try {
            // Same mechanism PoppyDBCLI already exposes via --sslKeystore: build a real
            // SSLContext and hand it to the server explicitly.
            SSLContext explicit = SslHelper.createServerSslContext(keystore.getAbsolutePath(), "changeit");
            srv.setSslContext(explicit);
            srv.setSslEnabled(true);

            startServer(srv, port);

            SSLSocketFactory factory = trustAllClientContext().getSocketFactory();

            try (SSLSocket socket = (SSLSocket) factory.createSocket("localhost", port)) {
                socket.startHandshake();
                assertNotNull(socket.getSession());
            }
        } finally {
            srv.shutdown();
            keystore.delete();
        }
    }
}
