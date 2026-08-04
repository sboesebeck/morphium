package de.caluga.poppydb;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/** --print-config rendering: source annotations, secret redaction, and the round-trip property. */
public class ConfigPrintTest {

    private static List<String> nonCommentLines(String rendered) {
        List<String> lines = new ArrayList<>();
        for (String line : rendered.split("\n")) {
            String t = line.strip();
            if (!t.isEmpty() && !t.startsWith("#")) {
                lines.add(t);
            }
        }
        return lines;
    }

    @Test
    void defaultsRenderWithDefaultAnnotationAndAllKnownKeys() {
        String out = ConfigInspector.render(PoppyDBCLI.parse(new String[0], 0), null);
        assertThat(out).contains("# Config file: none");
        assertThat(out).contains("# port  (default)\nport=17017");
        assertThat(out).contains("# bind  (default)\nbind=localhost");
        assertThat(out).contains("# log-level  (default)\nlog-level=INFO");
        assertThat(out).contains("compressor=none");
        assertThat(out).contains("ssl=false");
        assertThat(out).contains("auth=false");
        assertThat(out).contains("max-connections=500");
        assertThat(out).contains("socket-timeout=300");
        assertThat(out).contains("dump-interval=0");
        // unset keys show up commented out, usable as a template
        assertThat(out).contains("# rs-name  (unset)");
        assertThat(out).contains("# dump-dir  (unset)");
    }

    @Test
    void sourceAnnotationsDistinguishConfigFileAndCli() {
        // First 2 tokens simulate config-file origin, the rest is "real" CLI.
        ServerOptions opts = PoppyDBCLI.parse(
            new String[] {"--port", "27018", "--bind", "0.0.0.0"}, 2);
        String out = ConfigInspector.render(opts, Path.of("/etc/poppydb/config"));
        assertThat(out).contains("# Config file: /etc/poppydb/config");
        assertThat(out).contains("# port  (from config file)\nport=27018");
        assertThat(out).contains("# bind  (from command line)\nbind=0.0.0.0");
    }

    @Test
    void secretsAreNeverPrintedButMarkedAsSet() {
        ServerOptions opts = PoppyDBCLI.parse(
            new String[] {"--rootUser", "admin", "--rootPassword", "S3cretValue"}, 0);
        String out = ConfigInspector.render(opts, null);
        assertThat(out).doesNotContain("S3cretValue");
        assertThat(out).contains("# root-password  (set, from command line) - value not shown");
        assertThat(out).contains("# root-password=***");
        assertThat(out).contains("root-user=admin");
    }

    @Test
    void renderedOutputIsItselfALoadableConfigFile(@TempDir Path dir) throws Exception {
        ServerOptions original = PoppyDBCLI.parse(new String[] {
            "--port", "27018", "--bind", "0.0.0.0", "--compressor", "zlib",
            "--rs-name", "myrs", "--rs-seed", "a:1,b:2", "--rs-priorities", "100,50",
            "--max-connections", "77", "--dump-dir", "/tmp/poppy-dumps", "--dump-interval", "120",
            "--ssl", "--auth"
        }, 0);
        String rendered = ConfigInspector.render(original, null);

        Path cfg = dir.resolve("printed.conf");
        Files.writeString(cfg, rendered, StandardCharsets.UTF_8);

        de.caluga.poppydb.config.ConfigLoader loader = new de.caluga.poppydb.config.ConfigLoader();
        Properties props = loader.load(cfg);
        props = loader.resolveFileRefs(props);
        List<String> tokens = loader.toArgs(props);
        ServerOptions reloaded = PoppyDBCLI.parse(tokens.toArray(new String[0]), tokens.size());

        assertThat(reloaded.port).isEqualTo(original.port);
        assertThat(reloaded.bind).isEqualTo(original.bind);
        assertThat(reloaded.compressor).isEqualTo(original.compressor);
        assertThat(reloaded.rsName).isEqualTo(original.rsName);
        assertThat(reloaded.rsSeed).isEqualTo(original.rsSeed);
        assertThat(reloaded.rsPriorities).isEqualTo(original.rsPriorities);
        assertThat(reloaded.maxConnections).isEqualTo(original.maxConnections);
        assertThat(reloaded.dumpDir).isEqualTo(original.dumpDir);
        assertThat(reloaded.dumpIntervalSec).isEqualTo(original.dumpIntervalSec);
        assertThat(reloaded.ssl).isEqualTo(original.ssl);
        assertThat(reloaded.auth).isEqualTo(original.auth);

        // Printing the reloaded options again must produce the same key=value lines
        // (annotations/comments may differ - everything came from the config file now).
        String reRendered = ConfigInspector.render(reloaded, cfg);
        assertThat(nonCommentLines(reRendered)).isEqualTo(nonCommentLines(rendered));
    }

    @Test
    void newlineInValueCannotSmuggleExtraKeys(@TempDir Path dir) throws Exception {
        ServerOptions original = PoppyDBCLI.parse(
            new String[] {"--rs-name", "x\nport=9999", "--port", "27018"}, 0);
        String rendered = ConfigInspector.render(original, null);
        Path cfg = dir.resolve("printed.conf");
        Files.writeString(cfg, rendered, StandardCharsets.UTF_8);
        de.caluga.poppydb.config.ConfigLoader loader = new de.caluga.poppydb.config.ConfigLoader();
        java.util.Properties props = loader.resolveFileRefs(loader.load(cfg));
        java.util.List<String> tokens = loader.toArgs(props);
        ServerOptions reloaded = PoppyDBCLI.parse(tokens.toArray(new String[0]), tokens.size());
        assertThat(reloaded.port).isEqualTo(27018);
        assertThat(reloaded.rsName).isEqualTo("x\nport=9999");
    }

    @Test
    void leadingSpaceInValueSurvivesRoundTrip(@TempDir Path dir) throws Exception {
        ServerOptions original = PoppyDBCLI.parse(new String[] {"--rs-name", " padded"}, 0);
        String rendered = ConfigInspector.render(original, null);
        Path cfg = dir.resolve("printed.conf");
        Files.writeString(cfg, rendered, StandardCharsets.UTF_8);
        de.caluga.poppydb.config.ConfigLoader loader = new de.caluga.poppydb.config.ConfigLoader();
        java.util.List<String> tokens = loader.toArgs(loader.resolveFileRefs(loader.load(cfg)));
        ServerOptions reloaded = PoppyDBCLI.parse(tokens.toArray(new String[0]), tokens.size());
        assertThat(reloaded.rsName).isEqualTo(" padded");
    }
}
