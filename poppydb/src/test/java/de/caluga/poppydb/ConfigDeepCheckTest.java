package de.caluga.poppydb;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/** Deep (filesystem/crypto) checks behind --check-config - the errors that otherwise only
 *  surface when the server actually starts. */
public class ConfigDeepCheckTest {

    private static ServerOptions opts(String... args) {
        return PoppyDBCLI.parse(args, 0);
    }

    @Test
    void noKeystoreAndNoDumpDirMeansNothingToCheck() {
        ConfigInspector.Result r = ConfigInspector.deepCheck(opts());
        assertThat(r.errors()).isEmpty();
        assertThat(r.warnings()).isEmpty();
    }

    @Test
    void missingKeystoreIsAnError(@TempDir Path dir) {
        ConfigInspector.Result r = ConfigInspector.deepCheck(
            opts("--ssl", "--sslKeystore", dir.resolve("nope.jks").toString()));
        assertThat(r.errors()).anySatisfy(e -> assertThat(e).contains("not found"));
    }

    @Test
    void garbageKeystoreIsAnError(@TempDir Path dir) throws Exception {
        Path ks = dir.resolve("garbage.jks");
        Files.writeString(ks, "this is not a keystore", StandardCharsets.UTF_8);
        ConfigInspector.Result r = ConfigInspector.deepCheck(
            opts("--ssl", "--sslKeystore", ks.toString(), "--sslKeystorePassword", "changeit"));
        assertThat(r.errors()).anySatisfy(e -> assertThat(e).contains("Cannot load SSL keystore"));
    }

    @Test
    void dumpDirThatIsAFileIsAnError(@TempDir Path dir) throws Exception {
        Path f = dir.resolve("dumpfile");
        Files.writeString(f, "x", StandardCharsets.UTF_8);
        ConfigInspector.Result r = ConfigInspector.deepCheck(opts("--dump-dir", f.toString()));
        assertThat(r.errors()).anySatisfy(e -> assertThat(e).contains("not a directory"));
    }

    @Test
    void missingDumpDirIsOnlyAWarning(@TempDir Path dir) {
        ConfigInspector.Result r = ConfigInspector.deepCheck(
            opts("--dump-dir", dir.resolve("not-there-yet").toString()));
        assertThat(r.errors()).isEmpty();
        assertThat(r.warnings()).anySatisfy(w -> assertThat(w).contains("does not exist"));
    }

    @Test
    void writableDumpDirIsFine(@TempDir Path dir) {
        ConfigInspector.Result r = ConfigInspector.deepCheck(opts("--dump-dir", dir.toString()));
        assertThat(r.errors()).isEmpty();
        assertThat(r.warnings()).isEmpty();
    }
}
