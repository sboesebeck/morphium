package de.caluga.test.morphium.driver;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumDriver;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.MorphiumDriverNetworkException;
import de.caluga.morphium.driver.WriteConcern;
import de.caluga.morphium.driver.commands.InsertMongoCommand;
import de.caluga.morphium.driver.commands.MongoCommand;
import de.caluga.morphium.driver.wire.MongoConnection;
import de.caluga.test.ConnectionMock;
import de.caluga.test.DriverMock;

/**
 * Failover behavior of the write path: a primary step-down
 * (e.g. "Error: 10107 - not primary") must be retried on a freshly
 * resolved primary connection instead of failing the write.
 */
public class WriteCommandStepDownRetryTest {

    /** connection that always succeeds and records the writes it received */
    private static class GoodConnection extends ConnectionMock {
        private final MorphiumDriver drv;
        final AtomicInteger writes = new AtomicInteger();

        GoodConnection(MorphiumDriver drv) {
            this.drv = drv;
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
            return "goodhost:27017";
        }

        @Override
        public int sendCommand(MongoCommand cmd) throws MorphiumDriverException {
            return 1;
        }

        @Override
        public Map<String, Object> readSingleAnswer(int id) throws MorphiumDriverException {
            writes.incrementAndGet();
            return Doc.of("ok", 1.0, "n", 1);
        }
    }

    /** connection that fails each attempt with the given exception */
    private static class FailingConnection extends ConnectionMock {
        private final MorphiumDriver drv;
        private final MorphiumDriverException toThrow;
        final AtomicInteger attempts = new AtomicInteger();

        FailingConnection(MorphiumDriver drv, MorphiumDriverException toThrow) {
            this.drv = drv;
            this.toThrow = toThrow;
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
        public int sendCommand(MongoCommand cmd) throws MorphiumDriverException {
            return 1;
        }

        @Override
        public Map<String, Object> readSingleAnswer(int id) throws MorphiumDriverException {
            attempts.incrementAndGet();
            throw toThrow;
        }
    }

    /** driver whose getPrimaryConnection returns the given (new primary) connection */
    private static class FailoverDriverMock extends DriverMock {
        GoodConnection newPrimary;
        final AtomicInteger primaryRequests = new AtomicInteger();

        @Override
        public MongoConnection getPrimaryConnection(WriteConcern wc) {
            primaryRequests.incrementAndGet();
            if (newPrimary == null) {
                newPrimary = new GoodConnection(this);
            }
            return newPrimary;
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
            return 10; // keep step-down wait short in tests
        }
    }

    private static MorphiumDriverException stepDown(int code, String errmsg) {
        MorphiumDriverException e = new MorphiumDriverException("Error: " + code + " - " + errmsg);
        e.setMongoCode(code);
        return e;
    }

    private InsertMongoCommand insertOn(MongoConnection con) {
        InsertMongoCommand ins = new InsertMongoCommand(con);
        ins.setDb("testdb").setColl("testcoll");
        ins.setDocuments(List.of(Doc.of("value", 42)));
        return ins;
    }

    @Test
    public void retriesWriteOnNotWritablePrimary10107() throws Exception {
        FailoverDriverMock drv = new FailoverDriverMock();
        FailingConnection oldPrimary = new FailingConnection(drv, stepDown(10107, "not primary"));

        var result = insertOn(oldPrimary).execute();

        assertEquals(1.0, result.get("ok"));
        assertTrue(drv.primaryRequests.get() >= 1, "must re-resolve primary after step-down");
        assertEquals(1, drv.newPrimary.writes.get(), "write must be retried on new primary");
    }

    @Test
    public void retriesWriteOnPrimarySteppedDown189() throws Exception {
        FailoverDriverMock drv = new FailoverDriverMock();
        FailingConnection oldPrimary = new FailingConnection(drv, stepDown(189, "The server is in quiesce mode and will shut down"));

        var result = insertOn(oldPrimary).execute();

        assertEquals(1.0, result.get("ok"));
        assertEquals(1, drv.newPrimary.writes.get());
    }

    @Test
    public void retriesWriteOnInterruptedDueToReplStateChange11602() throws Exception {
        FailoverDriverMock drv = new FailoverDriverMock();
        FailingConnection oldPrimary = new FailingConnection(drv, stepDown(11602, "operation was interrupted"));

        var result = insertOn(oldPrimary).execute();

        assertEquals(1.0, result.get("ok"));
        assertEquals(1, drv.newPrimary.writes.get());
    }

    @Test
    public void retriesWriteOnNetworkErrorWhenRetryWritesEnabled() throws Exception {
        FailoverDriverMock drv = new FailoverDriverMock();
        MorphiumDriverNetworkException netErr = new MorphiumDriverNetworkException("Error sending Request: Broken pipe");
        FailingConnection oldPrimary = new FailingConnection(drv, netErr);

        var result = insertOn(oldPrimary).execute();

        assertEquals(1.0, result.get("ok"));
        assertEquals(1, drv.newPrimary.writes.get(), "network error during failover must be retried on new primary");
    }

    /** connection that answers null (reply timeout) on every attempt */
    private static class NullAnswerConnection extends ConnectionMock {
        private final MorphiumDriver drv;
        boolean closed = false;

        NullAnswerConnection(MorphiumDriver drv) {
            this.drv = drv;
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
            return "slowhost:27017";
        }

        @Override
        public int sendCommand(MongoCommand cmd) throws MorphiumDriverException {
            return 1;
        }

        @Override
        public Map<String, Object> readSingleAnswer(int id) throws MorphiumDriverException {
            return null; // reply never arrived within maxWaitTime
        }

        @Override
        public void close() {
            closed = true;
        }
    }

    @Test
    public void retriesWriteWhenNoReplyArrives() throws Exception {
        FailoverDriverMock drv = new FailoverDriverMock();
        NullAnswerConnection slow = new NullAnswerConnection(drv);

        var result = insertOn(slow).execute();

        assertEquals(1.0, result.get("ok"));
        assertEquals(1, drv.newPrimary.writes.get(), "write must be retried on a fresh connection");
        assertTrue(slow.closed, "connection with pending unanswered request must be closed, not reused");
    }

    @Test
    public void doesNotRetryOnOrdinaryWriteError() {
        FailoverDriverMock drv = new FailoverDriverMock();
        // e.g. duplicate key - must NOT be retried
        MorphiumDriverException dup = stepDown(11000, "E11000 duplicate key error");
        FailingConnection con = new FailingConnection(drv, dup);

        assertThrows(MorphiumDriverException.class, () -> insertOn(con).execute());
        assertEquals(1, con.attempts.get(), "non-transient errors must not be retried");
        assertEquals(0, drv.primaryRequests.get());
    }

    @Test
    public void givesUpAfterMaxRetriesIfStepDownPersists() {
        FailoverDriverMock drv = new FailoverDriverMock() {
            @Override
            public MongoConnection getPrimaryConnection(WriteConcern wc) {
                primaryRequests.incrementAndGet();
                // failover never completes: always hand out another failing connection
                return new FailingConnection(this, stepDown(10107, "not primary"));
            }
        };
        FailingConnection con = new FailingConnection(drv, stepDown(10107, "not primary"));

        assertThrows(MorphiumDriverException.class, () -> insertOn(con).execute());
    }
}
