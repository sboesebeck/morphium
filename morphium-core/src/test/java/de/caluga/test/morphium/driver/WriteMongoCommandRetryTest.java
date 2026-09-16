package de.caluga.test.morphium.driver;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumDriver;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.commands.WriteMongoCommand;
import de.caluga.morphium.driver.wire.MongoConnection;
import de.caluga.test.ConnectionMock;
import de.caluga.test.DriverMock;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for the WriteConflict (Error 112) retry logic in
 * {@link WriteMongoCommand#execute()}.
 *
 * All tests are purely in-memory — no MongoDB required.
 */
@Tag("inmemory")
class WriteMongoCommandRetryTest {

    // --------------------------------------------------------------------- //
    //  Minimal concrete WriteMongoCommand subclass for testing               //
    // --------------------------------------------------------------------- //

    /** A trivial concrete subclass that requires no additional setup. */
    static class TestWriteCommand extends WriteMongoCommand<TestWriteCommand> {
        TestWriteCommand(MongoConnection con) {
            super(con);
        }

        @Override
        public String getCommandName() {
            return "testWrite";
        }
    }

    // --------------------------------------------------------------------- //
    //  Connection stub that throws Error 112 a configurable number of times  //
    // --------------------------------------------------------------------- //

    /**
     * A {@link ConnectionMock} that reports {@code isConnected() == true},
     * accepts {@code sendCommand()} calls (returning a fixed message id),
     * and throws a {@link MorphiumDriverException} with mongo code 112 on
     * the first {@code failTimes} calls to {@link #readSingleAnswer(int)},
     * then returns a success map.
     */
    static class Error112Connection extends ConnectionMock {

        private final int failTimes;
        private final AtomicInteger readCallCount = new AtomicInteger(0);
        private final MorphiumDriver driver;

        Error112Connection(int failTimes, MorphiumDriver driver) {
            this.failTimes = failTimes;
            this.driver = driver;
        }

        @Override
        public boolean isConnected() {
            return true;
        }

        @Override
        public String getConnectedTo() {
            return "localhost:27017";
        }

        @Override
        public int sendCommand(de.caluga.morphium.driver.commands.MongoCommand cmd)
                throws MorphiumDriverException {
            return 1;
        }

        @Override
        public Map<String, Object> readSingleAnswer(int id) throws MorphiumDriverException {
            int call = readCallCount.incrementAndGet();
            if (call <= failTimes) {
                MorphiumDriverException ex = new MorphiumDriverException("WriteConflict");
                ex.setMongoCode(112);
                throw ex;
            }
            // Success: return a minimal write-result map
            return Doc.of("ok", 1.0, "n", 1);
        }

        @Override
        public MorphiumDriver getDriver() {
            return driver;
        }

        int getReadCallCount() {
            return readCallCount.get();
        }
    }

    // --------------------------------------------------------------------- //
    //  Connection stub for ExceededMemoryLimit (146) in its three shapes     //
    // --------------------------------------------------------------------- //

    /** How the heap-watermark refuse reaches the client. */
    enum Shape146 {
        /** ok:0 / code:146 command failure, thrown by checkForError from readSingleAnswer. */
        THROWN_ON_READ,
        /** InMemoryDriver as client: runCommand executes synchronously inside sendCommand. */
        THROWN_ON_SEND,
        /** PoppyDB fast path (processInsertDirect): ok:1, n:0, writeErrors:[{code:146}]. */
        WRITE_ERROR_RESULT
    }

    /**
     * A {@link ConnectionMock} that reports the memory-watermark refuse (mongo code 146)
     * in the given shape for the first {@code failTimes} attempts, then succeeds.
     * Attempts are counted on sendCommand, since the THROWN_ON_SEND shape never reaches
     * readSingleAnswer.
     */
    static class Error146Connection extends ConnectionMock {

        private final int failTimes;
        private final Shape146 shape;
        private final AtomicInteger sendCallCount = new AtomicInteger(0);
        private final AtomicInteger readCallCount = new AtomicInteger(0);
        private final MorphiumDriver driver;

        Error146Connection(int failTimes, Shape146 shape, MorphiumDriver driver) {
            this.failTimes = failTimes;
            this.shape = shape;
            this.driver = driver;
        }

        private static MorphiumDriverException refuse() {
            MorphiumDriverException ex = new MorphiumDriverException(
                "heap occupancy after the last collection 93% is above the memory watermark (90%) - refusing to create new documents");
            ex.setMongoCode(146);
            return ex;
        }

        @Override
        public boolean isConnected() {
            return true;
        }

        @Override
        public String getConnectedTo() {
            return "localhost:27017";
        }

        @Override
        public int sendCommand(de.caluga.morphium.driver.commands.MongoCommand cmd)
                throws MorphiumDriverException {
            int call = sendCallCount.incrementAndGet();
            if (shape == Shape146.THROWN_ON_SEND && call <= failTimes) {
                throw refuse();
            }
            return call;
        }

        @Override
        public Map<String, Object> readSingleAnswer(int id) throws MorphiumDriverException {
            readCallCount.incrementAndGet();
            if (id <= failTimes) {
                switch (shape) {
                    case THROWN_ON_READ:
                        throw refuse();
                    case WRITE_ERROR_RESULT:
                        return Doc.of("ok", 1.0, "n", 0, "writeErrors",
                            List.of(Doc.of("index", 0, "code", 146, "errmsg", refuse().getMessage())));
                    default:
                        break;
                }
            }
            return Doc.of("ok", 1.0, "n", 1);
        }

        @Override
        public MorphiumDriver getDriver() {
            return driver;
        }

        int getSendCallCount() {
            return sendCallCount.get();
        }

        int getReadCallCount() {
            return readCallCount.get();
        }
    }

    // --------------------------------------------------------------------- //
    //  A DriverMock with fast sleep so tests don't take seconds              //
    // --------------------------------------------------------------------- //

    static class FastDriverMock extends DriverMock {
        private final int retriesOnNetworkError;
        private MongoConnection freshConnection;

        FastDriverMock(int retriesOnNetworkError, MongoConnection freshConnection) {
            this.retriesOnNetworkError = retriesOnNetworkError;
            this.freshConnection = freshConnection;
        }

        @Override
        public int getSleepBetweenErrorRetries() {
            // Minimum sleep so tests run fast
            return 1;
        }

        @Override
        public int getRetriesOnNetworkError() {
            return retriesOnNetworkError;
        }

        @Override
        public MongoConnection getPrimaryConnection(de.caluga.morphium.driver.WriteConcern wc) {
            // Return the same connection for retry scenarios
            return freshConnection;
        }
    }

    // --------------------------------------------------------------------- //
    //  DriverMock variant that reports an active transaction                 //
    // --------------------------------------------------------------------- //

    static class TransactionActiveDriverMock extends FastDriverMock {
        TransactionActiveDriverMock(int retriesOnNetworkError, MongoConnection freshConnection) {
            super(retriesOnNetworkError, freshConnection);
        }

        @Override
        public boolean isTransactionInProgress() {
            return true;
        }
    }

    // ===================================================================== //
    //  Tests                                                                 //
    // ===================================================================== //

    /**
     * When a single Error-112 is thrown and maxAttempts allows at least one
     * retry, execute() must succeed and call readSingleAnswer exactly twice.
     */
    @Test
    void error112_singleFailure_retriesAndSucceeds() throws MorphiumDriverException {
        // maxAttempts = max(0, retriesOnNetworkError) + 5 = 0 + 5 = 5
        // failTimes = 1 → should retry once and succeed
        FastDriverMock drv = new FastDriverMock(0, null /* not needed here */);
        Error112Connection con = new Error112Connection(1, drv);
        // The driver's getPrimaryConnection is not called for Error 112 (no connection refresh)
        drv = new FastDriverMock(0, con);

        TestWriteCommand cmd = new TestWriteCommand(con)
                .setDb("test_db")
                .setColl("test_coll");

        Map<String, Object> result = cmd.execute();

        assertNotNull(result);
        assertEquals(1.0, result.get("ok"));
        // readSingleAnswer must have been called twice: once throwing, once succeeding
        assertEquals(2, con.getReadCallCount(),
                "readSingleAnswer should be called twice (1 failure + 1 success)");
    }

    /**
     * When Error 112 is thrown more times than maxAttempts allows,
     * execute() must eventually rethrow the exception.
     */
    @Test
    void error112_exceedsMaxAttempts_throwsException() {
        // maxAttempts = max(0, 0) + 5 = 5
        // We want to exhaust all retries: fail 6 times (attempts 0..5 each increment attempts)
        // The condition is "attempts++ < maxAttempts": attempts starts at 0, maxAttempts = 5.
        // It retries while attempts (before increment) < 5: i.e. for calls 1,2,3,4,5 → 5 retries.
        // Total readSingleAnswer calls before throw = 6 (5 retries + 1 final failure).
        int failTimes = 6;
        FastDriverMock drv = new FastDriverMock(0, null);
        Error112Connection con = new Error112Connection(failTimes, drv);
        drv = new FastDriverMock(0, con);

        TestWriteCommand cmd = new TestWriteCommand(con)
                .setDb("test_db")
                .setColl("test_coll");

        MorphiumDriverException thrown = assertThrows(MorphiumDriverException.class, cmd::execute);
        assertEquals(112, ((Number) thrown.getMongoCode()).intValue(),
                "The rethrown exception must carry mongo code 112");
        assertEquals(failTimes, con.getReadCallCount(),
                "readSingleAnswer should be called exactly " + failTimes + " times before giving up");
    }

    /**
     * When Error 112 is thrown exactly maxAttempts times, execute() retries
     * all of them and succeeds on the next call.
     */
    @Test
    void error112_exactlyMaxAttempts_retriesAndSucceeds() throws MorphiumDriverException {
        // maxAttempts = 5 → fail 5 times, succeed on 6th call
        int failTimes = 5;
        FastDriverMock drv = new FastDriverMock(0, null);
        Error112Connection con = new Error112Connection(failTimes, drv);
        drv = new FastDriverMock(0, con);

        TestWriteCommand cmd = new TestWriteCommand(con)
                .setDb("test_db")
                .setColl("test_coll");

        Map<String, Object> result = cmd.execute();

        assertNotNull(result);
        assertEquals(1.0, result.get("ok"));
        assertEquals(failTimes + 1, con.getReadCallCount(),
                "readSingleAnswer should be called " + (failTimes + 1) + " times (" + failTimes + " failures + 1 success)");
    }

    /**
     * When a transaction is in progress, Error 112 must NOT be retried at the
     * write-command level — it must propagate immediately so the transaction
     * interceptor can abort and restart the entire transaction.
     */
    @Test
    void error112_insideTransaction_propagatesImmediately() {
        TransactionActiveDriverMock drv = new TransactionActiveDriverMock(0, null);
        Error112Connection con = new Error112Connection(1, drv);
        drv = new TransactionActiveDriverMock(0, con);

        // Re-create connection with the final driver instance
        con = new Error112Connection(1, drv);

        TestWriteCommand cmd = new TestWriteCommand(con)
                .setDb("test_db")
                .setColl("test_coll");

        MorphiumDriverException thrown = assertThrows(MorphiumDriverException.class, cmd::execute);
        assertEquals(112, ((Number) thrown.getMongoCode()).intValue());
        // Must have been called exactly once — no retry
        assertEquals(1, con.getReadCallCount(),
                "Error 112 inside a transaction must NOT be retried at write level");
    }

    /**
     * Non-112 errors must NOT be retried by the Error-112 handler — they
     * must be rethrown immediately.
     */
    @Test
    void nonWriteConflictError_isNotRetried() {
        // A connection that always throws Error 11000 (DuplicateKey) — should never retry
        ConnectionMock con = new ConnectionMock() {
            private final MorphiumDriver drv = new FastDriverMock(0, this);

            @Override
            public boolean isConnected() { return true; }

            @Override
            public String getConnectedTo() { return "localhost:27017"; }

            @Override
            public MorphiumDriver getDriver() { return drv; }

            @Override
            public int sendCommand(de.caluga.morphium.driver.commands.MongoCommand cmd)
                    throws MorphiumDriverException { return 1; }

            @Override
            public Map<String, Object> readSingleAnswer(int id) throws MorphiumDriverException {
                MorphiumDriverException ex = new MorphiumDriverException("DuplicateKey");
                ex.setMongoCode(11000);
                throw ex;
            }
        };

        TestWriteCommand cmd = new TestWriteCommand(con)
                .setDb("test_db")
                .setColl("test_coll");

        MorphiumDriverException thrown = assertThrows(MorphiumDriverException.class, cmd::execute);
        assertEquals(11000, ((Number) thrown.getMongoCode()).intValue(),
                "Non-112 errors must be rethrown as-is");
    }

    // ===================================================================== //
    //  ExceededMemoryLimit (146) - the heap watermark refuse is retryable    //
    // ===================================================================== //

    private static TestWriteCommand command146(Error146Connection con) {
        return new TestWriteCommand(con).setDb("test_db").setColl("test_coll");
    }

    /**
     * ok:0 / code:146 thrown from readSingleAnswer twice, then success: the write must
     * succeed on the third attempt. The refuse happens before any document is created,
     * so re-sending the whole command cannot double-insert.
     */
    @Test
    void error146_thrownOnRead_retriesAndSucceeds() throws MorphiumDriverException {
        FastDriverMock drv = new FastDriverMock(0, null);
        Error146Connection con = new Error146Connection(2, Shape146.THROWN_ON_READ, drv);

        Map<String, Object> result = command146(con).execute();

        assertEquals(1.0, result.get("ok"));
        assertNull(result.get("writeErrors"));
        assertEquals(3, con.getReadCallCount(), "2 refusals + 1 success");
    }

    /**
     * A heap that stays full must surface: after maxAttempts (= retriesOnNetworkError + 5)
     * retries the exception is rethrown with its mongo code intact, never retried forever.
     */
    @Test
    void error146_thrownOnRead_exceedsMaxAttempts_throwsWithCode146() {
        int failTimes = 6; // maxAttempts = 5 -> 5 retries + the final refusal
        FastDriverMock drv = new FastDriverMock(0, null);
        Error146Connection con = new Error146Connection(failTimes, Shape146.THROWN_ON_READ, drv);

        MorphiumDriverException thrown = assertThrows(MorphiumDriverException.class, command146(con)::execute);

        assertEquals(146, ((Number) thrown.getMongoCode()).intValue());
        assertEquals(failTimes, con.getReadCallCount());
    }

    /**
     * PoppyDB answers a refused insert on its direct-dispatch path with ok:1 and a
     * writeErrors entry carrying code 146 (MongoCommandHandler.processInsertDirect) - this
     * is the shape the poppydb_rs test phase sees, so it must be retried as well.
     */
    @Test
    void error146_asWriteErrorResult_retriesAndSucceeds() throws MorphiumDriverException {
        FastDriverMock drv = new FastDriverMock(0, null);
        Error146Connection con = new Error146Connection(2, Shape146.WRITE_ERROR_RESULT, drv);

        Map<String, Object> result = command146(con).execute();

        assertEquals(1.0, result.get("ok"));
        assertNull(result.get("writeErrors"), "the successful retry's answer must be returned");
        assertEquals(1, result.get("n"));
        assertEquals(3, con.getReadCallCount(), "2 refusals + 1 success");
    }

    /**
     * writeErrors-shaped refuse beyond maxAttempts: the last answer is returned as the
     * server sent it, so the command-level writeErrors handling (InsertMongoCommand)
     * surfaces the full heap to the caller.
     */
    @Test
    void error146_asWriteErrorResult_exceedsMaxAttempts_returnsRefusal() throws MorphiumDriverException {
        int failTimes = 6;
        FastDriverMock drv = new FastDriverMock(0, null);
        Error146Connection con = new Error146Connection(failTimes, Shape146.WRITE_ERROR_RESULT, drv);

        Map<String, Object> result = command146(con).execute();

        List<?> writeErrors = (List<?>) result.get("writeErrors");
        assertNotNull(writeErrors);
        assertEquals(146, ((Number) ((Map<?, ?>) writeErrors.get(0)).get("code")).intValue());
        assertEquals(failTimes, con.getReadCallCount(), "5 retries + the final refusal, then give up");
    }

    /**
     * InMemoryDriver as the client's driver runs the command synchronously inside
     * sendCommand, so the refuse is thrown there rather than from readSingleAnswer.
     */
    @Test
    void error146_thrownOnSend_retriesAndSucceeds() throws MorphiumDriverException {
        FastDriverMock drv = new FastDriverMock(0, null);
        Error146Connection con = new Error146Connection(2, Shape146.THROWN_ON_SEND, drv);

        Map<String, Object> result = command146(con).execute();

        assertEquals(1.0, result.get("ok"));
        assertEquals(3, con.getSendCallCount(), "2 refusals on send + 1 success");
        assertEquals(1, con.getReadCallCount(), "only the successful send is read");
    }

    @Test
    void error146_thrownOnSend_exceedsMaxAttempts_throwsWithCode146() {
        int failTimes = 6;
        FastDriverMock drv = new FastDriverMock(0, null);
        Error146Connection con = new Error146Connection(failTimes, Shape146.THROWN_ON_SEND, drv);

        MorphiumDriverException thrown = assertThrows(MorphiumDriverException.class, command146(con)::execute);

        assertEquals(146, ((Number) thrown.getMongoCode()).intValue());
        assertEquals(failTimes, con.getSendCallCount());
        assertEquals(0, con.getReadCallCount());
    }

    /**
     * Unlike WriteConflict (112), a watermark refuse does not abort the server-side
     * transaction - the write simply did not happen - so it is retried inside one too.
     */
    @Test
    void error146_insideTransaction_isRetried() throws MorphiumDriverException {
        TransactionActiveDriverMock drv = new TransactionActiveDriverMock(0, null);
        Error146Connection con = new Error146Connection(1, Shape146.THROWN_ON_READ, drv);

        Map<String, Object> result = command146(con).execute();

        assertEquals(1.0, result.get("ok"));
        assertEquals(2, con.getReadCallCount(), "146 inside a transaction must still be retried");
    }

    /**
     * Only an answer whose writeErrors ALL carry code 146 is a watermark refuse - that is
     * the only shape the atomic pre-write check can produce. Any other writeErrors answer
     * (here a duplicate key next to a 146) may describe documents that did land and must
     * be returned untouched, not re-sent.
     */
    @Test
    void writeErrors_notAllCode146_areNotRetried() throws MorphiumDriverException {
        AtomicInteger reads = new AtomicInteger();
        ConnectionMock con = new ConnectionMock() {
            private final MorphiumDriver drv = new FastDriverMock(0, this);

            @Override
            public boolean isConnected() { return true; }

            @Override
            public String getConnectedTo() { return "localhost:27017"; }

            @Override
            public MorphiumDriver getDriver() { return drv; }

            @Override
            public int sendCommand(de.caluga.morphium.driver.commands.MongoCommand cmd)
                    throws MorphiumDriverException { return 1; }

            @Override
            public Map<String, Object> readSingleAnswer(int id) {
                reads.incrementAndGet();
                return Doc.of("ok", 1.0, "n", 1, "writeErrors", List.of(
                    Doc.of("index", 0, "code", 11000, "errmsg", "E11000 duplicate key"),
                    Doc.of("index", 1, "code", 146, "errmsg", "above the memory watermark")));
            }
        };

        Map<String, Object> result = new TestWriteCommand(con).setDb("test_db").setColl("test_coll").execute();

        assertEquals(2, ((List<?>) result.get("writeErrors")).size(), "answer must be returned as sent");
        assertEquals(1, reads.get(), "a mixed writeErrors answer must not be retried");
    }
}
