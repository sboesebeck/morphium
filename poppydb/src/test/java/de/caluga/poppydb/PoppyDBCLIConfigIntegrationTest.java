package de.caluga.poppydb;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.commands.FindCommand;
import de.caluga.morphium.driver.commands.InsertMongoCommand;
import de.caluga.morphium.driver.wire.PooledDriver;
import de.caluga.morphium.driver.wire.SingleMongoConnection;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end coverage for the config-file feature through the real code path: writes a temp
 * config file, builds the "config tokens then real CLI args" array exactly like
 * {@link PoppyDBCLI#main(String[])} does, and calls {@link PoppyDBCLI#configureServer(String[])}
 * - the same argument-parsing/wiring switch statement the CLI uses - rather than duplicating
 * that logic. This verifies port/bind actually came from the file (not the built-in defaults)
 * and that a real CLI argument overrides a config file value end-to-end.
 * <p>
 * {@code main()} itself is not invoked here: it blocks forever in a "keep alive" loop and can
 * call {@code System.exit}, neither of which is testable in-process - {@code configureServer}
 * is the extracted, side-effect-free seam that returns a configured-but-unstarted server so this
 * test can start it, connect, assert, and shut it down cleanly (mirrors AuthTlsWireE2ETest).
 */
public class PoppyDBCLIConfigIntegrationTest {

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

    /** Mirrors PoppyDBCLI.main()'s "config tokens first, real CLI args after" construction. */
    private static String[] effectiveArgs(List<String> configTokens, String... realArgs) {
        List<String> all = new ArrayList<>(configTokens);
        for (String a : realArgs) {
            all.add(a);
        }
        return all.toArray(new String[0]);
    }

    private void assertCrudWorks(int port) throws Exception {
        PooledDriver carrier = new PooledDriver();
        carrier.setConnectionTimeout(3000);
        connection = new SingleMongoConnection();
        connection.connect(carrier, "127.0.0.1", port);

        InsertMongoCommand insert = new InsertMongoCommand(connection);
        insert.setDb("cfgtestdb").setColl("cfgtestcoll");
        insert.setDocuments(List.of(Doc.of("marker", "cli-config-integration")));
        Map<String, Object> insertResult = insert.execute();
        assertThat(insertResult.get("n")).as("insert: " + insertResult).isEqualTo(1);

        FindCommand find = new FindCommand(connection);
        find.setDb("cfgtestdb").setColl("cfgtestcoll").setFilter(Doc.of("marker", "cli-config-integration"));
        List<Map<String, Object>> found = find.execute();
        assertThat(found).hasSize(1);
    }

    @Test
    void portAndBindComeFromConfigFileNotDefaults(@TempDir Path dir) throws Exception {
        int port = freePort();
        Path cfg = dir.resolve("config");
        Files.writeString(cfg, "port=" + port + "\nbind=127.0.0.1\nmax-connections=42\n",
                StandardCharsets.UTF_8);

        ConfigLoaderPipeline pipeline = ConfigLoaderPipeline.run(cfg);
        server = PoppyDBCLI.configureServer(effectiveArgs(pipeline.tokens));
        server.start();

        assertThat(server.getPort()).isEqualTo(port);
        assertThat(server.getHost()).isEqualTo("127.0.0.1");
        assertThat(server.getStats().get("maxConnections")).isEqualTo(42);
        assertCrudWorks(port);
    }

    @Test
    void realCliArgumentOverridesConfigFileValue(@TempDir Path dir) throws Exception {
        int configPort = freePort();
        int cliPort = freePort();
        Path cfg = dir.resolve("config");
        Files.writeString(cfg, "port=" + configPort + "\nbind=127.0.0.1\n", StandardCharsets.UTF_8);

        ConfigLoaderPipeline pipeline = ConfigLoaderPipeline.run(cfg);
        // Real CLI arg for --port must win over the config file's port.
        server = PoppyDBCLI.configureServer(effectiveArgs(pipeline.tokens, "--port", String.valueOf(cliPort)));
        server.start();

        assertCrudWorks(cliPort);
    }

    @Test
    void noSslCliFlagOverridesConfigSslTrue(@TempDir Path dir) throws Exception {
        int port = freePort();
        Path cfg = dir.resolve("config");
        Files.writeString(cfg, "port=" + port + "\nbind=127.0.0.1\nssl=true\n", StandardCharsets.UTF_8);

        ConfigLoaderPipeline pipeline = ConfigLoaderPipeline.run(cfg);
        server = PoppyDBCLI.configureServer(effectiveArgs(pipeline.tokens, "--no-ssl"));
        server.start();

        // No getter exposes sslEnabled directly - proof by behavior instead: a plaintext wire
        // handshake only succeeds if SSL really got switched back off by --no-ssl overriding the
        // config file's ssl=true (an SSL-enabled port would reject/hang on a plaintext connect,
        // as PoppyDBTlsTest demonstrates from the other direction).
        assertCrudWorks(port);
    }

    /** Small helper bundling ConfigLoader's real pipeline (load -> permissions -> file-refs -> toArgs). */
    private static final class ConfigLoaderPipeline {
        final List<String> tokens;

        private ConfigLoaderPipeline(List<String> tokens) {
            this.tokens = tokens;
        }

        static ConfigLoaderPipeline run(Path cfg) {
            de.caluga.poppydb.config.ConfigLoader loader = new de.caluga.poppydb.config.ConfigLoader();
            java.util.Properties props = loader.load(cfg);
            loader.checkPermissions(cfg, props);
            props = loader.resolveFileRefs(props);
            return new ConfigLoaderPipeline(loader.toArgs(props));
        }
    }
}
