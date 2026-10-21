package de.caluga.poppydb.netty;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.commands.WatchCommand;
import de.caluga.morphium.driver.inmem.InMemoryDriver;

/**
 * A cursor-kill log line has to name the consumer. "Change stream cursor 1174 exceeded max buffer
 * size 10000 - killing cursor (slow/absent consumer)" was all the production primary said, 488
 * times on 21.10.2026, and it took an hour of cross-referencing connection tables and client logs
 * to find the one bus participant in the kill/resume livelock. With namespace and client address
 * in the line the same question is answered by the server log alone.
 */
public class WatchCursorKillLogTest {

    private static final String DB = "killlog_db";
    private static final String COLL = "data";

    private InMemoryDriver drv;
    private WatchCursorManager cursorManager;
    private Logger managerLog;
    private ListAppender<ILoggingEvent> appender;
    private Level savedLevel;

    @BeforeEach
    public void setUp() {
        drv = new InMemoryDriver();
        drv.connect();
        drv.setServerMode(true);
        cursorManager = new WatchCursorManager();

        managerLog = (Logger) LoggerFactory.getLogger(WatchCursorManager.class);
        appender = new ListAppender<>();
        appender.start();
        savedLevel = managerLog.getLevel();
        managerLog.setLevel(Level.WARN);
        managerLog.addAppender(appender);
    }

    @AfterEach
    public void tearDown() {
        managerLog.detachAppender(appender);
        managerLog.setLevel(savedLevel);
        cursorManager.shutdown();
        drv.close();
    }

    private String killLine() {
        return appender.list.stream()
                .filter(e -> e.getLevel() == Level.WARN)
                .map(ILoggingEvent::getFormattedMessage)
                .filter(m -> m.contains("killing"))
                .findFirst()
                .orElse("");
    }

    @Test
    public void countCapKillNamesNamespaceAndClient() throws Exception {
        cursorManager.setMaxCursorQueueSizeForTest(2);
        WatchCommand wcmd = new WatchCommand(drv).setDb(DB).setColl(COLL).setPipeline(List.of());
        long cursorId = cursorManager.createWatchCursor(drv, wcmd, false, null, "10.101.6.13:51736");

        for (int i = 0; i < 6; i++) {
            drv.store(DB, COLL, List.of(Doc.of("_id", i, "v", i)), null);
        }

        String line = killLine();
        assertThat(line).as("the count-cap kill must be logged").contains("exceeded max buffer size");
        assertThat(line).contains("cursor " + cursorId + " (" + DB + "." + COLL + " client 10.101.6.13:51736)");
        assertThat(cursorManager.hasCursor(cursorId)).as("the cursor was killed").isFalse();
    }

    @Test
    public void byteBudgetKillNamesNamespaceAndClient() throws Exception {
        cursorManager.setCursorQueueByteBudget(512);
        WatchCommand wcmd = new WatchCommand(drv).setDb(DB).setColl(COLL).setPipeline(List.of());
        long cursorId = cursorManager.createWatchCursor(drv, wcmd, false, null, "10.101.6.29:40000");

        for (int i = 0; i < 4; i++) {
            drv.store(DB, COLL, List.of(Doc.of("_id", i, "payload", "x".repeat(400))), null);
        }

        String line = killLine();
        assertThat(line).as("the byte-budget kill must be logged").contains("exceeded its byte budget");
        assertThat(line).contains("(" + DB + "." + COLL + " client 10.101.6.29:40000)");
    }

    @Test
    public void describeNamesTheMemberForAReplicationWatchAndFallsBackToUnknownClient() {
        assertThat(WatchCursorManager.describe(new WatchCursorManager.WatchCursorState(7, DB, COLL, 10,
                true, "secondary-under-test:17018", "10.1.1.1:1")))
                .isEqualTo(DB + "." + COLL + " replication watch of secondary-under-test:17018");
        assertThat(WatchCursorManager.describe(new WatchCursorManager.WatchCursorState(8, DB, COLL, 10,
                false, null, null)))
                .isEqualTo(DB + "." + COLL + " client unknown");
    }
}
