package de.caluga.poppydb;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/** Exit-code level tests for --print-config/--check-config via the runInspection seam
 *  (main() itself calls System.exit and blocks, so it stays untested here - a jar-level
 *  smoke test covers it manually). */
public class PoppyDBCLIInspectionTest {

    private final ByteArrayOutputStream outBytes = new ByteArrayOutputStream();
    private final ByteArrayOutputStream errBytes = new ByteArrayOutputStream();
    private final PrintStream out = new PrintStream(outBytes, true, StandardCharsets.UTF_8);
    private final PrintStream err = new PrintStream(errBytes, true, StandardCharsets.UTF_8);

    private String out() { return outBytes.toString(StandardCharsets.UTF_8); }
    private String err() { return errBytes.toString(StandardCharsets.UTF_8); }

    @Test
    void printConfigDumpsEffectiveConfigAndReturnsZero() {
        int rc = PoppyDBCLI.runInspection(
            new String[] {"--port", "27018"}, 0, null, true, false, out, err);
        assertThat(rc).isZero();
        assertThat(out()).contains("port=27018").contains("# Config file: none");
    }

    @Test
    void checkConfigOkPrintsOkAndReturnsZero(@TempDir Path dir) throws Exception {
        Path cfg = dir.resolve("config");
        Files.writeString(cfg, "port=27018\n", StandardCharsets.UTF_8);
        int rc = PoppyDBCLI.runInspection(
            new String[] {"--port", "27018"}, 2, cfg, false, true, out, err);
        assertThat(rc).isZero();
        assertThat(out()).contains("Configuration OK (" + cfg + ")");
    }

    @Test
    void checkConfigReportsAllSemanticErrorsAndReturnsOne() {
        int rc = PoppyDBCLI.runInspection(
            new String[] {"--port", "0", "--memory-warn", "0"}, 0, null, false, true, out, err);
        assertThat(rc).isEqualTo(1);
        assertThat(err()).contains("Configuration check FAILED:");
        assertThat(err()).contains("port must be between");
        assertThat(err()).contains("memory-warn must be between");
    }

    @Test
    void checkConfigSurfacesDeepErrors(@TempDir Path dir) throws Exception {
        Path ks = dir.resolve("garbage.jks");
        Files.writeString(ks, "not a keystore", StandardCharsets.UTF_8);
        int rc = PoppyDBCLI.runInspection(
            new String[] {"--ssl", "--sslKeystore", ks.toString(), "--sslKeystorePassword", "x"},
            0, null, false, true, out, err);
        assertThat(rc).isEqualTo(1);
        assertThat(err()).contains("Cannot load SSL keystore");
    }

    @Test
    void checkConfigPrintsWarningsButStillReturnsZero() {
        int rc = PoppyDBCLI.runInspection(new String[] {"--ssl"}, 0, null, false, true, out, err);
        assertThat(rc).isZero();
        assertThat(err()).contains("WARNING:").contains("ssl-keystore");
        assertThat(out()).contains("Configuration OK (no config file)");
    }

    @Test
    void parseErrorReturnsOneWithMessage() {
        int rc = PoppyDBCLI.runInspection(new String[] {"--bogus"}, 0, null, false, true, out, err);
        assertThat(rc).isEqualTo(1);
        assertThat(err()).contains("--bogus");
    }

    @Test
    void parserToleratesTheNewFlags() {
        // main() strips nothing - parse must skip the inspection flags like --cfg/--no-config.
        ServerOptions opts = PoppyDBCLI.parse(
            new String[] {"--print-config", "--check-config", "--port", "1234"}, 0);
        assertThat(opts.port).isEqualTo(1234);
    }

    @Test
    void checkConfigOkWithValidUsersFile(@TempDir Path dir) throws Exception {
        Path usersFile = dir.resolve("users.json");
        Files.writeString(usersFile, "[{\"user\": \"app\", \"pwd\": \"s3cret\"}]", StandardCharsets.UTF_8);
        Files.setPosixFilePermissions(usersFile,
            java.nio.file.attribute.PosixFilePermissions.fromString("rw-------"));
        int rc = PoppyDBCLI.runInspection(
            new String[] {"--users-file", usersFile.toString()}, 0, null, false, true, out, err);
        assertThat(rc).isZero();
        assertThat(out()).contains("Configuration OK");
    }

    @Test
    void checkConfigFailsWithBrokenUsersFile(@TempDir Path dir) throws Exception {
        Path usersFile = dir.resolve("users.json");
        Files.writeString(usersFile, "{ this is not json", StandardCharsets.UTF_8);
        Files.setPosixFilePermissions(usersFile,
            java.nio.file.attribute.PosixFilePermissions.fromString("rw-------"));
        int rc = PoppyDBCLI.runInspection(
            new String[] {"--users-file", usersFile.toString()}, 0, null, false, true, out, err);
        assertThat(rc).isEqualTo(1);
        assertThat(err()).contains("Configuration check FAILED:");
    }

    @Test
    void printAndCheckTogetherAreRejected() {
        int rc = PoppyDBCLI.runInspection(
            new String[] {"--port", "27018"}, 0, null, true, true, out, err);
        assertThat(rc).isEqualTo(1);
        assertThat(err()).contains("cannot be combined");
        assertThat(out()).isEmpty();
    }
}
