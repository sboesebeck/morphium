package de.caluga.test.morphium.driver;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumCursor;
import de.caluga.morphium.driver.MorphiumDriver;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.ReadPreference;
import de.caluga.morphium.driver.SingleBatchCursor;
import de.caluga.morphium.driver.WriteConcern;
import de.caluga.morphium.driver.commands.ListCollectionsCommand;
import de.caluga.morphium.driver.commands.MongoCommand;
import de.caluga.morphium.driver.commands.ReadMongoCommand;
import de.caluga.morphium.driver.wire.MongoConnection;
import de.caluga.test.ConnectionMock;
import de.caluga.test.DriverMock;

/**
 * #393: a read that targets the primary and is answered with a not-primary/stepdown error
 * (13435 NotPrimaryNoSecondaryOk, 10107 NotWritablePrimary, 189 PrimarySteppedDown, 11602
 * InterruptedDueToReplStateChange, 91/11600 shutdown, 13436 NotPrimaryOrSecondary) must be
 * retried on the re-resolved primary, exactly as {@link WriteCommandStepDownRetryTest} pins it
 * for the write path. A read that did not ask for the primary keeps the error - there it is
 * the caller's configuration, not a failover.
 */
@Tag("core")
public class ReadCommandStepDownRetryTest {

    /** the answer the real primary gives */
    private static final List<Map<String, Object>> COLLECTIONS =
        List.of(Doc.of("name", "uncached_object"), Doc.of("name", "cached_object"));

    /** connection that answers every read with the given exception */
    private static class FailingConnection extends ConnectionMock {
        private final MorphiumDriver drv;
        private final MorphiumDriverException toThrow;
        final AtomicInteger reads = new AtomicInteger();

        FailingConnection(MorphiumDriver drv, MorphiumDriverException toThrow, ReadPreference effective) {
            this.drv = drv;
            this.toThrow = toThrow;
            setEffectiveReadPreference(effective);
        }

        @Override
        public MorphiumDriver getDriver() {
            return drv;
        }

        @Override
        public boolean isConnected() {
            return true;
        }

        @Override
        public String getConnectedTo() {
            return "oldprimary:27017";
        }

        @Override
        public int sendCommand(MongoCommand cmd) {
            reads.incrementAndGet();
            return 1;
        }

        @Override
        public MorphiumCursor getAnswerFor(int queryId, int batchsize) throws MorphiumDriverException {
            throw toThrow;
        }
    }

    /** connection to the new primary - answers the real result */
    private static class GoodConnection extends ConnectionMock {
        private final MorphiumDriver drv;
        final AtomicInteger reads = new AtomicInteger();

        GoodConnection(MorphiumDriver drv) {
            this.drv = drv;
            setEffectiveReadPreference(ReadPreference.primary());
        }

        @Override
        public MorphiumDriver getDriver() {
            return drv;
        }

        @Override
        public boolean isConnected() {
            return true;
        }

        @Override
        public String getConnectedTo() {
            return "newprimary:27017";
        }

        @Override
        public int sendCommand(MongoCommand cmd) {
            reads.incrementAndGet();
            return 1;
        }

        @Override
        public MorphiumCursor getAnswerFor(int queryId, int batchsize) {
            return new SingleBatchCursor(COLLECTIONS);
        }
    }

    /** driver whose getPrimaryConnection hands out the new primary */
    private static class FailoverDriverMock extends DriverMock {
        GoodConnection newPrimary;
        final AtomicInteger primaryRequests = new AtomicInteger();
        final AtomicInteger released = new AtomicInteger();
        final AtomicInteger refreshRequests = new AtomicInteger();

        @Override
        public void refreshPrimaryAfterStepDown(MongoConnection rejectedBy) {
            refreshRequests.incrementAndGet();
        }

        @Override
        public MongoConnection getPrimaryConnection(WriteConcern wc) {
            primaryRequests.incrementAndGet();
            if (newPrimary == null) {
                newPrimary = new GoodConnection(this);
            }
            return newPrimary;
        }

        @Override
        public void releaseConnection(MongoConnection con) {
            released.incrementAndGet();
        }

        @Override
        public int getRetriesOnNetworkError() {
            return 3;
        }

        @Override
        public int getSleepBetweenErrorRetries() {
            return 10;
        }

        @Override
        public int getHeartbeatFrequency() {
            return 10;
        }
    }

    private static MorphiumDriverException serverError(int code, String errmsg) {
        MorphiumDriverException e = new MorphiumDriverException("Error: " + code + " - " + errmsg);
        e.setMongoCode(code);
        return e;
    }

    private static ListCollectionsCommand listCollectionsOn(MongoConnection con) {
        ListCollectionsCommand cmd = new ListCollectionsCommand(con);
        cmd.setDb("testdb").setNameOnly(true);
        return cmd;
    }

    private static ListAppender<ILoggingEvent> captureLog() {
        Logger logger = (Logger) LoggerFactory.getLogger(ReadMongoCommand.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        logger.addAppender(appender);
        return appender;
    }

    @Test
    public void primaryReadAnsweredNotPrimaryIsRetriedOnReResolvedPrimary() throws Exception {
        FailoverDriverMock drv = new FailoverDriverMock();
        FailingConnection oldPrimary = new FailingConnection(drv,
            serverError(13435, "not primary and secondaryOk=false"), ReadPreference.primary());
        ListAppender<ILoggingEvent> log = captureLog();

        List<Map<String, Object>> result = listCollectionsOn(oldPrimary).execute();

        assertEquals(COLLECTIONS, result, "the retry must deliver the real answer");
        assertEquals(1, oldPrimary.reads.get(), "the stale primary is asked exactly once");
        assertEquals(1, drv.newPrimary.reads.get(), "exactly one retry on the new primary");
        assertEquals(1, drv.primaryRequests.get(), "the primary is re-resolved exactly once");
        assertEquals(1, drv.refreshRequests.get(), "the driver is asked to refresh its primary from the rejecting node");
        assertTrue(drv.released.get() >= 1, "the rejected connection must be given back to the pool");
        long warns = log.list.stream()
            .filter(e -> e.getLevel() == Level.WARN && e.getFormattedMessage().contains("re-resolving primary"))
            .count();
        assertEquals(1, warns, "one WARN per retry, naming the re-resolution: " + log.list);
    }

    @Test
    public void everyCodeOfTheStepDownFamilyIsRetried() throws Exception {
        int[] codes = {10107, 189, 91, 11600, 11602, 13435, 13436};
        for (int code : codes) {
            FailoverDriverMock drv = new FailoverDriverMock();
            FailingConnection oldPrimary = new FailingConnection(drv, serverError(code, "stepdown family " + code),
                ReadPreference.primary());

            List<Map<String, Object>> result = listCollectionsOn(oldPrimary).execute();

            assertEquals(COLLECTIONS, result, "code " + code + " must be retried");
            assertEquals(1, drv.newPrimary.reads.get(), "code " + code + ": one retry on the new primary");
        }
    }

    @Test
    public void primaryPreferredReadIsRetriedToo() throws Exception {
        FailoverDriverMock drv = new FailoverDriverMock();
        FailingConnection oldPrimary = new FailingConnection(drv,
            serverError(11602, "operation was interrupted"), ReadPreference.primaryPreferred());

        List<Map<String, Object>> result = listCollectionsOn(oldPrimary).execute();

        assertEquals(COLLECTIONS, result);
        assertEquals(1, drv.newPrimary.reads.get());
    }

    @Test
    public void executeIterableIsRetriedForTheFirstBatch() throws Exception {
        FailoverDriverMock drv = new FailoverDriverMock();
        FailingConnection oldPrimary = new FailingConnection(drv,
            serverError(13435, "not primary and secondaryOk=false"), ReadPreference.primary());

        MorphiumCursor crs = listCollectionsOn(oldPrimary).executeIterable(100);

        assertEquals(COLLECTIONS, crs.getAll(), "the cursor must come from the re-resolved primary");
        assertEquals(1, drv.newPrimary.reads.get());
    }

    @Test
    public void secondaryPreferredReadKeepsTheNotPrimaryError() {
        FailoverDriverMock drv = new FailoverDriverMock();
        FailingConnection secondary = new FailingConnection(drv,
            serverError(13435, "not primary and secondaryOk=false"), ReadPreference.secondaryPreferred());

        MorphiumDriverException e = assertThrows(MorphiumDriverException.class,
            () -> listCollectionsOn(secondary).execute());

        assertEquals(13435, e.getMongoCode(), "the caller's own error must reach the caller");
        assertEquals(1, secondary.reads.get(), "a read that did not ask for the primary is not retried");
        assertEquals(0, drv.primaryRequests.get(), "and the primary is not re-resolved for it");
    }

    @Test
    public void ordinaryErrorIsNotRetried() {
        FailoverDriverMock drv = new FailoverDriverMock();
        FailingConnection primary = new FailingConnection(drv,
            serverError(2, "BadValue: unknown operator"), ReadPreference.primary());

        assertThrows(MorphiumDriverException.class, () -> listCollectionsOn(primary).execute());

        assertEquals(1, primary.reads.get());
        assertEquals(0, drv.primaryRequests.get());
    }

    @Test
    public void givesUpAfterRetriesOnNetworkErrorIfNoPrimaryComesBack() {
        FailoverDriverMock drv = new FailoverDriverMock() {
            @Override
            public MongoConnection getPrimaryConnection(WriteConcern wc) {
                primaryRequests.incrementAndGet();
                // the failover never completes: every re-resolved "primary" rejects the read as well
                return new FailingConnection(this, serverError(13435, "not primary and secondaryOk=false"),
                    ReadPreference.primary());
            }
        };
        FailingConnection first = new FailingConnection(drv,
            serverError(13435, "not primary and secondaryOk=false"), ReadPreference.primary());

        MorphiumDriverException e = assertThrows(MorphiumDriverException.class,
            () -> listCollectionsOn(first).execute());

        assertEquals(13435, e.getMongoCode(), "the last stepdown error is what the caller gets");
        assertEquals(drv.getRetriesOnNetworkError(), drv.primaryRequests.get(),
            "retries are bounded by retriesOnNetworkError");
    }
}
