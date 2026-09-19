package de.caluga.morphium.driver.inmem;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.commands.InsertMongoCommand;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Regression test for #366: a dump that has outgrown what an older reader can restore must say so
 * while it is being written.
 *
 * <p>Before the streaming restore, {@code restoreInternal} decoded a whole dump into one
 * {@code String}, so a database stopped being restorable once its serialized JSON crossed the JVM's
 * String limit - and the write side streamed, so it produced such files without a word. Observed on
 * PoppyDB 6.3.8: a 1.07M document database dumped to 72MB gz / 1.94GB of JSON every hour for days,
 * and the node came back dead on the first restart.
 *
 * <p>This version reads such a dump fine. What the warning protects now is the rollback: dropping
 * the process back to a jar from before the streaming restore is a normal recovery step, and a dump
 * past the old ceiling would be unreadable there.
 *
 * <p>The dump is written either way - refusing it would turn a restore problem into immediate data
 * loss at shutdown.
 *
 * <p>Lives in the driver's own package to reach {@code dumpRollbackLimitChars}, the same kind of
 * threshold seam as {@link InMemoryDriver#setSlowQueryThresholdMillis(long)} - making the condition
 * reachable without serializing an actual gigabyte.
 */
@Tag("inmemory")
public class DumpSizeWarningTest {
    private final String db = "dumpsizedb";
    private final String coll = "dumpsizecoll";

    private InMemoryDriver driverWithDocuments(int count) throws Exception {
        InMemoryDriver drv = new InMemoryDriver();
        drv.connect();
        List<Map<String, Object>> docs = new ArrayList<>();

        for (int i = 0; i < count; i++) {
            docs.add(Doc.of("counter", i, "text", "some payload for document " + i));
        }

        new InsertMongoCommand(drv).setDb(db).setColl(coll).setDocuments(docs).execute();
        return drv;
    }

    private List<ILoggingEvent> captureDumpWarns(ThrowingRunnable body) throws Exception {
        ch.qos.logback.classic.Logger logger =
                (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(InMemoryDriver.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        // Same copy-on-write reasoning as InMemoryDriverSlowQueryTest: the appender is still
        // attached while the events are read, and driver background threads may log meanwhile.
        appender.list = new java.util.concurrent.CopyOnWriteArrayList<>();
        appender.start();
        logger.addAppender(appender);

        try {
            body.run();
            return appender.list.stream()
                    .filter(ev -> ev.getLevel() == Level.WARN)
                    .filter(ev -> ev.getFormattedMessage().contains("rolling this process back"))
                    .collect(Collectors.toList());
        } finally {
            logger.detachAppender(appender);
        }
    }

    @FunctionalInterface
    private interface ThrowingRunnable {
        void run() throws Exception;
    }

    @Test
    void anOversizedDumpWarnsAndNamesTheDatabaseAndItsSize() throws Exception {
        InMemoryDriver drv = driverWithDocuments(20);
        drv.dumpRollbackLimitChars = 100; // far below what 20 documents serialize to
        File f = File.createTempFile("dumpsize-over", ".morphium.gz");
        f.deleteOnExit();
        List<ILoggingEvent> warns = captureDumpWarns(() -> drv.dumpToFile(db, f));
        assertEquals(1, warns.size(), "an oversized dump must warn exactly once");
        String msg = warns.get(0).getFormattedMessage();
        assertTrue(msg.contains(db), "the warning must name the database: " + msg);
        assertTrue(msg.contains("100"), "the warning must name the limit it crossed: " + msg);
        assertTrue(Files.size(f.toPath()) > 0,
                "the dump must still be written - warning about a file beats not having one");
    }

    @Test
    void anOrdinaryDumpStaysSilent() throws Exception {
        InMemoryDriver drv = driverWithDocuments(20);
        File f = File.createTempFile("dumpsize-under", ".morphium.gz");
        f.deleteOnExit();
        List<ILoggingEvent> warns = captureDumpWarns(() -> drv.dumpToFile(db, f));
        assertTrue(warns.isEmpty(),
                "a dump below the rollback limit must not warn: " + warns.stream()
                        .map(ILoggingEvent::getFormattedMessage).collect(Collectors.joining("; ")));
    }

    @Test
    void theWarningThresholdDefaultsToTheStringLimitOlderReadersHit() {
        InMemoryDriver drv = new InMemoryDriver();
        assertEquals(Integer.MAX_VALUE >> 1, drv.dumpRollbackLimitChars,
                "the default must be the UTF16 String limit a pre-streaming restoreInternal ran into");
    }
}
