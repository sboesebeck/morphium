package de.caluga.test.mongo.suite.inmem;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import ch.qos.logback.classic.Level;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPOutputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Regression tests for #306: restore-on-startup aborted silently after the first broken dump
 * file, leaving all remaining databases unrestored without any error or summary line. A broken
 * dump file must be logged (with stack trace) and skipped; all other dumps must still be
 * restored, and a summary line (INFO when complete, WARN when partial) must always be emitted.
 */
@Tag("inmemory")
public class InMemRestoreDirectoryTest {

    private ListAppender<ILoggingEvent> logWatcher;

    @BeforeEach
    public void attachLogWatcher() {
        logWatcher = new ListAppender<>();
        // ListAppender.list is a plain ArrayList and the appender stays attached for the whole
        // test, including while the events are read - a background thread of the driver logging
        // at that moment would break the stream with a ConcurrentModificationException.
        // Copy-on-write reads a snapshot.
        logWatcher.list = new java.util.concurrent.CopyOnWriteArrayList<>();
        logWatcher.start();
        ((Logger) LoggerFactory.getLogger(InMemoryDriver.class)).addAppender(logWatcher);
    }

    @AfterEach
    public void detachLogWatcher() {
        ((Logger) LoggerFactory.getLogger(InMemoryDriver.class)).detachAppender(logWatcher);
    }

    private static void createDump(File dir, String dbName) throws Exception {
        InMemoryDriver src = new InMemoryDriver();
        Map<String, List<Map<String, Object>>> db = new HashMap<>();
        List<Map<String, Object>> docs = new ArrayList<>();
        docs.add(Doc.of("_id", dbName + "-doc1", "value", 42));
        docs.add(Doc.of("_id", dbName + "-doc2", "value", 43));
        db.put("test_coll", docs);
        src.setDatabase(dbName, db);
        src.dumpToFile(dbName, new File(dir, dbName + ".morphium.gz"));
    }

    /** A file that is not even valid gzip. */
    private static void createGarbageDump(File dir, String name) throws Exception {
        try (FileOutputStream fos = new FileOutputStream(new File(dir, name + ".morphium.gz"))) {
            fos.write("this is not a gzip file".getBytes(StandardCharsets.UTF_8));
        }
    }

    /** Valid gzip, but the content is not a parseable dump. */
    private static void createCorruptJsonDump(File dir, String name) throws Exception {
        try (FileOutputStream fos = new FileOutputStream(new File(dir, name + ".morphium.gz"));
            GZIPOutputStream gz = new GZIPOutputStream(fos)) {
            gz.write("{ definitely not valid json ]]".getBytes(StandardCharsets.UTF_8));
        }
    }

    private List<String> messagesAtLeast(Level level) {
        List<String> ret = new ArrayList<>();
        for (ILoggingEvent ev : logWatcher.list) {
            if (ev.getLevel().isGreaterOrEqual(level)) {
                ret.add(ev.getFormattedMessage());
            }
        }
        return ret;
    }

    @Test
    public void brokenDumpFileDoesNotAbortRestoreOfRemainingDatabases(@TempDir Path tmp) throws Exception {
        File dir = tmp.toFile();
        createDump(dir, "db_alpha");
        createDump(dir, "db_omega");
        createGarbageDump(dir, "db_broken_gzip");
        createCorruptJsonDump(dir, "db_broken_json");

        InMemoryDriver target = new InMemoryDriver();
        int restored = target.restoreAllFromDirectory(dir);

        assertEquals(2, restored, "both intact dumps must be restored despite the broken ones");
        assertTrue(target.listDatabases().contains("db_alpha"), "db_alpha must be restored");
        assertTrue(target.listDatabases().contains("db_omega"), "db_omega must be restored");
        assertEquals(2, target.getDatabase("db_alpha").get("test_coll").size());
        assertEquals(2, target.getDatabase("db_omega").get("test_coll").size());
    }

    @Test
    public void partialRestoreReportsFailedFilesAndWarns(@TempDir Path tmp) throws Exception {
        File dir = tmp.toFile();
        createDump(dir, "db_alpha");
        createDump(dir, "db_omega");
        createGarbageDump(dir, "db_broken");

        InMemoryDriver target = new InMemoryDriver();
        InMemoryDriver.DirectoryRestoreResult result = target.restoreAllFromDirectoryResult(dir);

        assertEquals(3, result.getTotal());
        assertEquals(2, result.getRestored());
        assertFalse(result.isComplete());
        assertEquals(List.of("db_broken.morphium.gz"), result.getFailedFiles());

        // each broken file must be logged as ERROR with a stack trace
        boolean errorWithStacktrace = logWatcher.list.stream()
            .anyMatch(ev -> ev.getLevel() == Level.ERROR
                && ev.getFormattedMessage().contains("db_broken.morphium.gz")
                && ev.getThrowableProxy() != null);
        assertTrue(errorWithStacktrace, "broken dump file must be logged on ERROR with stack trace, got: "
            + messagesAtLeast(Level.ERROR));

        // and the summary must be an unmissable WARN naming restored/total
        boolean warnSummary = logWatcher.list.stream()
            .anyMatch(ev -> ev.getLevel() == Level.WARN
                && ev.getFormattedMessage().contains("2")
                && ev.getFormattedMessage().contains("3"));
        assertTrue(warnSummary, "partial restore must emit a WARN summary with restored/total, got: "
            + messagesAtLeast(Level.WARN));
    }

    @Test
    public void completeRestoreEmitsInfoSummary(@TempDir Path tmp) throws Exception {
        File dir = tmp.toFile();
        createDump(dir, "db_one");
        createDump(dir, "db_two");

        InMemoryDriver target = new InMemoryDriver();
        InMemoryDriver.DirectoryRestoreResult result = target.restoreAllFromDirectoryResult(dir);

        assertEquals(2, result.getTotal());
        assertEquals(2, result.getRestored());
        assertTrue(result.isComplete());
        assertTrue(result.getFailedFiles().isEmpty());

        boolean infoSummary = logWatcher.list.stream()
            .anyMatch(ev -> ev.getLevel() == Level.INFO
                && ev.getFormattedMessage().contains("Restored 2 of 2"));
        assertTrue(infoSummary, "complete restore must emit an INFO summary, got: "
            + messagesAtLeast(Level.INFO));

        boolean anyWarn = logWatcher.list.stream().anyMatch(ev -> ev.getLevel().isGreaterOrEqual(Level.WARN));
        assertFalse(anyWarn, "complete restore must not warn, got: " + messagesAtLeast(Level.WARN));
    }
}
