package de.caluga.poppydb;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.commands.FindCommand;
import de.caluga.morphium.driver.commands.GenericCommand;
import de.caluga.morphium.driver.commands.InsertMongoCommand;
import de.caluga.morphium.driver.wire.PooledDriver;
import de.caluga.morphium.driver.wire.SingleMongoConnection;
import de.caluga.morphium.driver.wire.SslHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Full wire-level end-to-end coverage for PoppyDB's --auth and --ssl features: a real PoppyDB
 * server (in-JVM, random port) and morphium's real production client (SingleMongoConnection
 * over TCP - the same code path every pooled connection uses, including its automatic SCRAM
 * handshake on connect). This is the automated version of the manual mongosh verification.
 */
public class AuthTlsWireE2ETest {

    private PoppyDB server;
    private SingleMongoConnection connection;

    @AfterEach
    public void tearDown() {
        if (connection != null) {
            try {
                connection.close();
            } catch (Exception ignored) {
            }
        }
        if (server != null) {
            try {
                server.shutdown();
            } catch (Exception ignored) {
            }
        }
    }

    private static int freePort() throws Exception {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    private int startServer(boolean auth, javax.net.ssl.SSLContext serverSsl) throws Exception {
        int port = freePort();
        server = new PoppyDB(port, "127.0.0.1", 100, 60);
        if (auth) {
            server.setAuthRequired(true);
            server.setRootUser("root", "rootpw");
        }
        if (serverSsl != null) {
            server.setSslContext(serverSsl);
            server.setSslEnabled(true);
        }
        server.start();
        return port;
    }

    private SingleMongoConnection connect(int port, String user, String password,
                                          javax.net.ssl.SSLContext clientSsl) throws Exception {
        PooledDriver carrier = new PooledDriver();
        carrier.setConnectionTimeout(3000);
        if (clientSsl != null) {
            carrier.setUseSSL(true);
            carrier.setSslContext(clientSsl);
            carrier.setSslInvalidHostNameAllowed(true); // self-signed test cert, CN mismatch is fine
        }

        connection = new SingleMongoConnection();
        if (user != null) {
            connection.setCredentials("admin", user, password);
        }
        connection.connect(carrier, "127.0.0.1", port);
        return connection;
    }

    private void assertCrudWorks(SingleMongoConnection con) throws Exception {
        InsertMongoCommand insert = new InsertMongoCommand(con);
        insert.setDb("e2edb").setColl("e2ecoll");
        insert.setDocuments(List.of(Doc.of("marker", "auth-tls-e2e")));
        Map<String, Object> insertResult = insert.execute();
        assertThat(insertResult.get("n")).as("insert: " + insertResult).isEqualTo(1);

        FindCommand find = new FindCommand(con);
        find.setDb("e2edb").setColl("e2ecoll").setFilter(Doc.of("marker", "auth-tls-e2e"));
        List<Map<String, Object>> found = find.execute();
        assertThat(found).hasSize(1);
    }

    /** Build a self-signed keystore + matching truststore via the JDK's keytool. */
    private static javax.net.ssl.SSLContext[] buildSslContexts(Path dir) throws Exception {
        File keystore = dir.resolve("server.p12").toFile();
        File cert = dir.resolve("server.crt").toFile();
        File truststore = dir.resolve("client-trust.p12").toFile();
        String keytool = System.getProperty("java.home") + File.separator + "bin" + File.separator + "keytool";

        run(keytool, "-genkeypair", "-alias", "poppy", "-keyalg", "RSA", "-keysize", "2048",
            "-storetype", "PKCS12", "-keystore", keystore.getAbsolutePath(),
            "-storepass", "changeit", "-keypass", "changeit",
            "-dname", "CN=localhost", "-validity", "1");
        run(keytool, "-exportcert", "-alias", "poppy", "-keystore", keystore.getAbsolutePath(),
            "-storepass", "changeit", "-file", cert.getAbsolutePath());
        run(keytool, "-importcert", "-alias", "poppy", "-noprompt", "-storetype", "PKCS12",
            "-keystore", truststore.getAbsolutePath(), "-storepass", "changeit",
            "-file", cert.getAbsolutePath());

        return new javax.net.ssl.SSLContext[] {
            SslHelper.createServerSslContext(keystore.getAbsolutePath(), "changeit"),
            SslHelper.createClientSslContext(truststore.getAbsolutePath(), "changeit")
        };
    }

    private static void run(String... cmd) throws Exception {
        Process p = new ProcessBuilder(cmd).redirectErrorStream(true).start();
        String out = new String(p.getInputStream().readAllBytes());
        if (p.waitFor() != 0) {
            throw new IllegalStateException("keytool failed: " + String.join(" ", cmd) + "\n" + out);
        }
    }

    // ---- auth ----------------------------------------------------------------------------

    @Test
    public void authenticatedClientCanWork() throws Exception {
        int port = startServer(true, null);

        assertCrudWorks(connect(port, "root", "rootpw", null));
    }

    @Test
    public void unauthenticatedClientIsRejected() throws Exception {
        int port = startServer(true, null);
        SingleMongoConnection con = connect(port, null, null, null); // handshake is pre-auth, connect succeeds

        GenericCommand find = new GenericCommand(con);
        find.setCommandName("find");
        find.setCmdData(Doc.of("find", "e2ecoll", "filter", Doc.of()));
        find.setDb("e2edb");
        int id = con.sendCommand(find);
        Throwable t = catchThrowable(() -> con.readSingleAnswer(id));

        assertThat(t).isInstanceOf(MorphiumDriverException.class);
        assertThat(t.getMessage()).contains("13").contains("requires authentication");
    }

    @Test
    public void wrongPasswordFailsTheConnect() throws Exception {
        int port = startServer(true, null);

        Throwable t = catchThrowable(() -> connect(port, "root", "WRONG", null));

        assertThat(t).as("SCRAM happens during connect - wrong password must fail there")
                .isInstanceOf(MorphiumDriverException.class);
        assertThat(t.getMessage()).contains("Authenticating");
    }

    // ---- TLS -----------------------------------------------------------------------------

    @Test
    public void tlsEncryptedConnectionWorks(@TempDir Path dir) throws Exception {
        javax.net.ssl.SSLContext[] ctx = buildSslContexts(dir);
        int port = startServer(false, ctx[0]);

        assertCrudWorks(connect(port, null, null, ctx[1]));
    }

    @Test
    public void tlsPlusAuthWorksTogether(@TempDir Path dir) throws Exception {
        javax.net.ssl.SSLContext[] ctx = buildSslContexts(dir);
        int port = startServer(true, ctx[0]);

        assertCrudWorks(connect(port, "root", "rootpw", ctx[1]));
    }
}
