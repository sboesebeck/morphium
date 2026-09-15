package de.caluga.poppydb;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.FileOutputStream;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.zip.GZIPOutputStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * #366: a restore that fails with an {@link Error} rather than an {@link Exception} must still land
 * in the startup guard, not walk out of {@code main}.
 *
 * <p>The design is already right: a dump is an optimisation that saves a full sync, so
 * {@code buildServer} catches a failed restore, logs it, and calls
 * {@code setLocalDataComplete(false)} - the node comes up without the data and refuses to stand for
 * election until an authoritative sync has completed. What it caught was {@code Exception}, and an
 * {@code OutOfMemoryError} is not one. It walked through {@code buildServer},
 * {@code configureServer} and out of {@code main}, which only catches {@code ConfigException} - so
 * no log line, no guard, and the process stayed alive on its non-daemon threads with nothing
 * listening. To an init system that is a healthy service.
 *
 * <p>Reproduced here with a {@code StackOverflowError} instead of an OOM: a dump nested thousands
 * of levels deep exhausts the stack in the recursive dump-value conversion. Same class of failure,
 * same catch, and it needs neither a 12GB heap nor a gigabyte of data. Such a dump is not
 * hypothetical either - a corrupted or hand-written file gets there.
 */
public class RestoreErrorStartupGuardTest {

    private PoppyDB server;

    @AfterEach
    public void tearDown() {
        if (server != null) {
            try {
                server.shutdown();
            } catch (Exception ignored) {
                // nothing to do - the test only ever configures the server, it never starts it
            }
        }
    }

    private static int freePort() throws Exception {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    /** A dump whose documents nest deep enough to exhaust the stack while being converted. */
    private void writeDeeplyNestedDump(Path dir, int depth) throws Exception {
        StringBuilder json = new StringBuilder();
        json.append("{ \"_id\" : 1723891234567, \"db\" : \"deep\", \"data\" : { \"coll\" : [ { \"_id\" : \"doc1\", \"v\" : ");
        json.append("[".repeat(depth)).append("1").append("]".repeat(depth));
        json.append(" } ] } }");

        try (FileOutputStream fos = new FileOutputStream(dir.resolve("deep.morphium.gz").toFile());
            GZIPOutputStream gz = new GZIPOutputStream(fos)) {
            gz.write(json.toString().getBytes(StandardCharsets.UTF_8));
        }
    }

    @Test
    void anErrorDuringRestoreLeavesAGuardedServerInsteadOfEscaping(@TempDir Path dir) throws Exception {
        Path dumps = Files.createDirectories(dir.resolve("dumps"));
        writeDeeplyNestedDump(dumps, 100_000);

        // configureServer() is the seam the CLI integration tests use: it wires everything up,
        // including the restore, and hands back a configured-but-unstarted server.
        server = PoppyDBCLI.configureServer(new String[] {
            "--port", String.valueOf(freePort()), "--bind", "127.0.0.1", "--dump-dir", dumps.toString()
        });

        assertThat(server)
                .as("a failed restore must not take the process with it - the node comes up without "
                    + "the data and syncs it from a peer")
                .isNotNull();
        assertThat(server.isLocalDataComplete())
                .as("the node must be barred from standing for election until an authoritative sync "
                    + "has completed, or it could win one and overwrite intact peers with nothing")
                .isFalse();
        assertThat(server.getDriver().listDatabases())
                .as("the database from the broken dump must not be half-present")
                .doesNotContain("deep");
    }

    @Test
    void anIntactDumpStillLeavesTheNodeComplete(@TempDir Path dir) throws Exception {
        Path dumps = Files.createDirectories(dir.resolve("dumps"));
        String json = "{ \"_id\" : 1723891234567, \"db\" : \"shallow\", \"data\" : "
                + "{ \"coll\" : [ { \"_id\" : \"doc1\", \"v\" : 42 } ] } }";

        try (FileOutputStream fos = new FileOutputStream(dumps.resolve("shallow.morphium.gz").toFile());
            GZIPOutputStream gz = new GZIPOutputStream(fos)) {
            gz.write(json.getBytes(StandardCharsets.UTF_8));
        }

        server = PoppyDBCLI.configureServer(new String[] {
            "--port", String.valueOf(freePort()), "--bind", "127.0.0.1", "--dump-dir", dumps.toString()
        });

        assertThat(server.isLocalDataComplete())
                .as("negative control: a dump that restores cleanly must leave the guard alone")
                .isTrue();
        assertThat(server.getDriver().listDatabases()).contains("shallow");
    }
}
