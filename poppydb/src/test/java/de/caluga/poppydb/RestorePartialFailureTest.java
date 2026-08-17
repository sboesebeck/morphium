package de.caluga.poppydb;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Regression tests for #306: a node whose restore-on-startup fails on one dump file must still
 * restore all the other databases, and neither PoppyDB nor the CLI may treat the partial
 * restore as success - the incompleteness has to be loud (WARN with restored/total) in the
 * startup log.
 */
public class RestorePartialFailureTest {

    private PoppyDB server;
    private ListAppender<ILoggingEvent> cliLogWatcher;

    @AfterEach
    public void tearDown() {
        if (server != null) {
            try {
                server.shutdown();
            } catch (Exception ignored) {
            }
            server = null;
        }
        if (cliLogWatcher != null) {
            ((Logger) LoggerFactory.getLogger(PoppyDBCLI.class)).detachAppender(cliLogWatcher);
            cliLogWatcher = null;
        }
    }

    private static int freePort() throws Exception {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    private static void createDump(File dir, String dbName) throws Exception {
        InMemoryDriver src = new InMemoryDriver();
        Map<String, List<Map<String, Object>>> db = new HashMap<>();
        List<Map<String, Object>> docs = new ArrayList<>();
        docs.add(Doc.of("_id", dbName + "-doc1", "value", 42));
        db.put("test_coll", docs);
        src.setDatabase(dbName, db);
        src.dumpToFile(dbName, new File(dir, dbName + ".morphium.gz"));
    }

    private static void createGarbageDump(File dir, String name) throws Exception {
        try (FileOutputStream fos = new FileOutputStream(new File(dir, name + ".morphium.gz"))) {
            fos.write("this is definitely not gzip".getBytes(StandardCharsets.UTF_8));
        }
    }

    @Test
    public void restoreFromDumpSurvivesBrokenFileAndReportsPartialResult(@TempDir Path tmp) throws Exception {
        File dir = tmp.toFile();
        createDump(dir, "db_alpha");
        createDump(dir, "db_omega");
        createGarbageDump(dir, "db_broken");

        server = new PoppyDB(freePort(), "127.0.0.1", 20, 60);
        server.setDumpDirectory(dir);

        InMemoryDriver.DirectoryRestoreResult result = server.restoreFromDump();

        assertFalse(result.isComplete(), "a broken dump file must be reported as a partial restore");
        assertTrue(result.getRestored() == 2 && result.getTotal() == 3,
                "expected 2/3 restored, got " + result.getRestored() + "/" + result.getTotal());
        assertTrue(server.getDriver().listDatabases().contains("db_alpha"),
                "intact dumps must be restored despite the broken one");
        assertTrue(server.getDriver().listDatabases().contains("db_omega"),
                "intact dumps must be restored despite the broken one");
    }

    @Test
    public void cliStartupLogsUnmissableWarnOnPartialRestore(@TempDir Path tmp) throws Exception {
        File dir = tmp.toFile();
        createDump(dir, "db_alpha");
        createDump(dir, "db_omega");
        createGarbageDump(dir, "db_broken");

        cliLogWatcher = new ListAppender<>();
        cliLogWatcher.start();
        ((Logger) LoggerFactory.getLogger(PoppyDBCLI.class)).addAppender(cliLogWatcher);

        // same seam as PoppyDBCLIConfigIntegrationTest: configureServer is the real CLI wiring
        // path (including restore-on-startup), minus main()'s keep-alive loop / System.exit.
        server = PoppyDBCLI.configureServer(new String[] {
            "-p", String.valueOf(freePort()),
            "-b", "127.0.0.1",
            "--dump-dir", dir.getAbsolutePath()
        });

        // the intact databases must be there - startup must not have aborted the restore
        assertTrue(server.getDriver().listDatabases().contains("db_alpha"));
        assertTrue(server.getDriver().listDatabases().contains("db_omega"));

        // and the CLI itself must flag the partial restore on WARN with restored/total counts
        boolean warned = cliLogWatcher.list.stream()
            .anyMatch(ev -> ev.getLevel().isGreaterOrEqual(Level.WARN)
                && ev.getFormattedMessage().contains("2")
                && ev.getFormattedMessage().contains("3"));
        List<String> warnings = cliLogWatcher.list.stream()
            .filter(ev -> ev.getLevel().isGreaterOrEqual(Level.WARN))
            .map(ILoggingEvent::getFormattedMessage).toList();
        assertTrue(warned, "CLI startup must WARN about the partial restore (2 of 3), got: " + warnings);
    }

    @Test
    public void cliStartupStaysQuietOnCompleteRestore(@TempDir Path tmp) throws Exception {
        File dir = tmp.toFile();
        createDump(dir, "db_alpha");
        createDump(dir, "db_omega");

        cliLogWatcher = new ListAppender<>();
        cliLogWatcher.start();
        ((Logger) LoggerFactory.getLogger(PoppyDBCLI.class)).addAppender(cliLogWatcher);

        server = PoppyDBCLI.configureServer(new String[] {
            "-p", String.valueOf(freePort()),
            "-b", "127.0.0.1",
            "--dump-dir", dir.getAbsolutePath()
        });

        assertTrue(server.getDriver().listDatabases().contains("db_alpha"));
        assertTrue(server.getDriver().listDatabases().contains("db_omega"));

        boolean restoreWarn = cliLogWatcher.list.stream()
            .anyMatch(ev -> ev.getLevel().isGreaterOrEqual(Level.WARN)
                && ev.getFormattedMessage().toLowerCase().contains("restore"));
        assertFalse(restoreWarn, "a complete restore must not produce restore warnings");
    }
}
