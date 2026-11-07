package de.caluga.test.morphium;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.annotations.caching.WriteBuffer;
import de.caluga.morphium.async.AsyncOperationCallback;
import de.caluga.morphium.async.AsyncOperationType;
import de.caluga.morphium.driver.MorphiumDriver;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.MorphiumDriverNetworkException;
import de.caluga.morphium.driver.WriteConcern;
import de.caluga.morphium.driver.commands.InsertMongoCommand;
import de.caluga.morphium.driver.bulk.BulkRequestContext;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import de.caluga.morphium.driver.wire.MongoConnection;
import de.caluga.morphium.driver.wire.PooledDriver;
import de.caluga.morphium.query.Query;
import de.caluga.morphium.writer.BufferedMorphiumWriterImpl;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Tag("inmemory")
class BufferedWriterCallbackTest {

    @Entity(collectionName = "buffered_callback_entity")
    @WriteBuffer(size = 100, strategy = WriteBuffer.STRATEGY.JUST_WARN)
    public static class BufferedEntity {
        @Id
        public String id;
        public String value;
    }

    @Entity(collectionName = "buffered_callback_invalid_entity")
    @WriteBuffer(size = 100, strategy = WriteBuffer.STRATEGY.JUST_WARN)
    public static class InvalidIdEntity {
        @Id
        public Integer id;
        public String value;
    }

    @Entity(collectionName = "buffered_wait_entity")
    @WriteBuffer(size = 1, strategy = WriteBuffer.STRATEGY.WAIT, timeout = 1000)
    public static class WaitEntity {
        @Id
        public Integer id;
    }

    @Entity(collectionName = "buffered_write_old_entity")
    @WriteBuffer(size = 1, strategy = WriteBuffer.STRATEGY.WRITE_OLD)
    public static class WriteOldEntity {
        @Id
        public Integer id;
    }

    private static class FailingAfterInsertDriver extends InMemoryDriver {
        private int attempts;

        @Override
        public List<Map<String, Object>> insert(String db, String collection, List<Map<String, Object>> objects,
                                                Map<String, Object> wc) {
            attempts++;
            super.insert(db, collection, objects, wc);
            throw new MorphiumDriverException("reply lost after insert");
        }
    }

    private static class LostReplyDriver extends InMemoryDriver {
        private int replies;

        @Override
        public Map<String, Object> readSingleAnswer(int id) {
            replies++;
            throw new MorphiumDriverNetworkException("lost reply");
        }
    }

    private Morphium morphium;

    @AfterEach
    void tearDown() {
        if (morphium != null) {
            morphium.close();
        }
    }

    private void initializeMorphium() {
        MorphiumConfig config = new MorphiumConfig("buffered_callback_test_db", 10, 10_000, 10_000);
        config.driverSettings().setDriverName(InMemoryDriver.driverName);
        config.writerSettings().setWriteBufferTimeGranularity(300_000);
        morphium = new Morphium(config);
    }

    @Test
    void successIsReportedOnlyAfterBulkPersistence() {
        initializeMorphium();
        BufferedMorphiumWriterImpl writer = (BufferedMorphiumWriterImpl)
                morphium.getConfig().writerSettings().getBufferedWriter();
        AtomicInteger successes = new AtomicInteger();
        AsyncOperationCallback<BufferedEntity> callback = callback(successes, new AtomicInteger());
        BufferedEntity entity = new BufferedEntity();
        entity.id = "persisted";

        writer.insert(entity, null, callback);
        assertThat(successes).hasValue(0);

        writer.flush();

        assertThat(successes).hasValue(1);
        assertThat(morphium.createQueryFor(BufferedEntity.class).countAll()).isEqualTo(1);
    }

    @Test
    void enqueueFailureIsQuarantinedWithoutAutomaticRetry() {
        initializeMorphium();
        BufferedMorphiumWriterImpl writer = (BufferedMorphiumWriterImpl)
                morphium.getConfig().writerSettings().getBufferedWriter();
        AtomicInteger successes = new AtomicInteger();
        AtomicInteger errors = new AtomicInteger();

        writer.insert(new InvalidIdEntity(), null, callback(successes, errors));
        writer.flush();

        assertThat(successes).hasValue(0);
        assertThat(errors).hasValue(1);
        assertThat(writer.writeBufferCount()).isZero();
        assertThat(writer.getUnresolvedWrites()).hasSize(1);

        writer.flush();

        assertThat(errors).hasValue(1);
        assertThat(writer.getUnresolvedWrites()).hasSize(1);
    }

    @Test
    void driverFailureAfterPartialExecutionIsQuarantinedWithoutRetry() throws Exception {
        initializeMorphium();
        BufferedMorphiumWriterImpl writer = (BufferedMorphiumWriterImpl)
                morphium.getConfig().writerSettings().getBufferedWriter();
        MorphiumDriver original = morphium.getDriver();
        FailingAfterInsertDriver failing = new FailingAfterInsertDriver();
        failing.connect();
        morphium.setDriver(failing);
        AtomicInteger successes = new AtomicInteger();
        AtomicInteger errors = new AtomicInteger();
        AsyncOperationCallback<BufferedEntity> callerCallback = callback(successes, errors);

        try {
            BufferedEntity entity = new BufferedEntity();
            entity.id = "possibly-written";
            writer.insert(entity, null, callerCallback);
            writer.flush();
            writer.flush();

            assertThat(failing.attempts).isEqualTo(1);
            assertThat(failing.count("buffered_callback_test_db", "buffered_callback_entity", Map.of(), null, null)).isEqualTo(1);
            assertThat(successes).hasValue(0);
            assertThat(errors).hasValue(1);
            assertThat(writer.writeBufferCount()).isZero();
            var unresolved = writer.getUnresolvedWrites();
            assertThat(unresolved).hasSize(1);
            assertThat(unresolved.get(0).getFailure()).hasMessageContaining("reply lost");
            assertThat(unresolved.get(0).getCollectionName()).isEqualTo("buffered_callback_entity");
            assertThat(unresolved.get(0).getCallback()).isSameAs(callerCallback);
            assertThat(writer.discardUnresolvedWrite(unresolved.get(0))).isTrue();
            assertThat(writer.getUnresolvedWrites()).isEmpty();
        } finally {
            morphium.setDriver(original);
            failing.close();
        }
    }

    @Test
    void quarantinedFailureDoesNotBlockWaitBufferCapacity() {
        initializeMorphium();
        BufferedMorphiumWriterImpl writer = (BufferedMorphiumWriterImpl)
                morphium.getConfig().writerSettings().getBufferedWriter();
        AtomicInteger errors = new AtomicInteger();
        writer.insert(new WaitEntity(), null, callback(new AtomicInteger(), errors));
        writer.flush();
        assertThat(errors).hasValue(1);
        assertThat(writer.getUnresolvedWrites()).hasSize(1);

        WaitEntity next = new WaitEntity();
        next.id = 42;
        AtomicInteger successes = new AtomicInteger();
        assertTimeoutPreemptively(Duration.ofSeconds(2), () -> writer.insert(next, null, callback(successes, errors)));
        writer.flush();
        assertThat(successes).hasValue(1);
        assertThat(morphium.createQueryFor(WaitEntity.class).countAll()).isEqualTo(1);
    }

    @Test
    void writeOldOverflowQuarantinesFailureAndKeepsNewEntry() {
        initializeMorphium();
        BufferedMorphiumWriterImpl writer = (BufferedMorphiumWriterImpl)
                morphium.getConfig().writerSettings().getBufferedWriter();
        AtomicInteger successes = new AtomicInteger();
        AtomicInteger errors = new AtomicInteger();
        writer.insert(new WriteOldEntity(), null, callback(successes, errors));
        WriteOldEntity next = new WriteOldEntity();
        next.id = 42;
        writer.insert(next, null, callback(successes, errors));
        assertThat(errors).hasValue(1);
        assertThat(writer.getUnresolvedWrites()).hasSize(1);
        assertThat(writer.writeBufferCount()).isEqualTo(1);
        writer.flush();
        assertThat(successes).hasValue(1);
    }

    @Test
    void inMemoryDuplicateInsertIsNotReportedAsSuccess() {
        initializeMorphium();
        BufferedMorphiumWriterImpl writer = (BufferedMorphiumWriterImpl)
                morphium.getConfig().writerSettings().getBufferedWriter();
        BufferedEntity first = new BufferedEntity();
        first.id = "duplicate";
        writer.insert(first, null, callback(new AtomicInteger(), new AtomicInteger()));
        writer.flush();
        AtomicInteger successes = new AtomicInteger();
        AtomicInteger errors = new AtomicInteger();
        BufferedEntity duplicate = new BufferedEntity();
        duplicate.id = first.id;
        writer.insert(duplicate, null, callback(successes, errors));
        writer.flush();
        assertThat(successes).hasValue(0);
        assertThat(errors).hasValue(1);
        assertThat(writer.getUnresolvedWrites()).hasSize(1);
    }

    @Test
    void pooledDriverFailureIsReportedByBufferedWriter() {
        initializeMorphium();
        BufferedMorphiumWriterImpl writer = (BufferedMorphiumWriterImpl)
                morphium.getConfig().writerSettings().getBufferedWriter();
        MorphiumDriver original = morphium.getDriver();
        PooledDriver failing = new PooledDriver() {
            @Override
            public MongoConnection getPrimaryConnection(WriteConcern wc) {
                throw new MorphiumDriverException("connection failed");
            }
        };
        morphium.setDriver(failing);
        AtomicInteger successes = new AtomicInteger();
        AtomicInteger errors = new AtomicInteger();
        try {
            writer.insert(new BufferedEntity(), null, callback(successes, errors));
            writer.flush();
            assertThat(successes).hasValue(0);
            assertThat(errors).hasValue(1);
            assertThat(writer.getUnresolvedWrites()).hasSize(1);
            assertThat(writer.getUnresolvedWrites().get(0).getFailure()).hasMessageContaining("connection failed");
        } finally {
            morphium.setDriver(original);
        }
    }

    @Test
    void pooledBufferedWriteDoesNotRetryAnAmbiguousLostReply() throws Exception {
        initializeMorphium();
        BufferedMorphiumWriterImpl writer = (BufferedMorphiumWriterImpl)
                morphium.getConfig().writerSettings().getBufferedWriter();
        MorphiumDriver original = morphium.getDriver();
        LostReplyDriver connection = new LostReplyDriver();
        connection.connect();
        PooledDriver driver = new PooledDriver() {
            @Override
            public MongoConnection getPrimaryConnection(WriteConcern wc) {
                return connection;
            }
        };
        morphium.setDriver(driver);
        AtomicInteger successes = new AtomicInteger();
        AtomicInteger errors = new AtomicInteger();
        try {
            BufferedEntity entity = new BufferedEntity();
            entity.id = "lost-reply";
            writer.insert(entity, null, callback(successes, errors));
            writer.flush();
            writer.flush();
            assertThat(connection.replies).isEqualTo(1);
            assertThat(successes).hasValue(0);
            assertThat(errors).hasValue(1);
            assertThat(writer.getUnresolvedWrites()).hasSize(1);
        } finally {
            morphium.setDriver(original);
            connection.close();
        }
    }

    @Test
    void bulkWireCommandDoesNotRetryLostReply() throws Exception {
        LostReplyDriver driver = new LostReplyDriver();
        driver.connect();
        try {
            InsertMongoCommand command = new InsertMongoCommand(driver);
            command.setRetryNetworkErrors(false);
            command.setDb("lost_reply_db").setColl("items").setDocuments(List.of(new HashMap<>(Map.of("_id", "one"))));
            assertThatThrownBy(command::execute).isInstanceOf(MorphiumDriverNetworkException.class);
            assertThat(driver.replies).isEqualTo(1);
            assertThat(command.asMap()).doesNotContainKey("retryNetworkErrors");
        } finally {
            driver.close();
        }
    }

    @Test
    void pooledBulkRejectsWriteConcernErrorInSuccessfulCommandReply() throws Exception {
        InMemoryDriver connection = new InMemoryDriver() {
            @Override
            public Map<String, Object> readSingleAnswer(int id) {
                return Map.of("ok", 1, "n", 1, "writeConcernError", Map.of("errmsg", "not replicated"));
            }
        };
        connection.connect();
        PooledDriver driver = new PooledDriver() {
            @Override
            public MongoConnection getPrimaryConnection(WriteConcern wc) {
                return connection;
            }
        };
        try {
            BulkRequestContext context = driver.createBulkContext(null, "test", "items", true, null);
            context.addInsertBulkRequest(List.of(new HashMap<>(Map.of("_id", "one"))));
            assertThatThrownBy(context::execute)
                    .isInstanceOf(MorphiumDriverException.class)
                    .hasMessageContaining("not replicated");
        } finally {
            connection.close();
        }
    }

    private static <T> AsyncOperationCallback<T> callback(AtomicInteger successes, AtomicInteger errors) {
        return new AsyncOperationCallback<>() {
            @Override
            public void onOperationSucceeded(AsyncOperationType type, Query<T> query, long duration,
                                             List<T> result, T entity, Object... param) {
                successes.incrementAndGet();
            }

            @Override
            public void onOperationError(AsyncOperationType type, Query<T> query, long duration,
                                         String error, Throwable throwable, T entity, Object... param) {
                errors.incrementAndGet();
            }
        };
    }
}
