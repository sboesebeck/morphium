package de.caluga.morphium.config;

import de.caluga.morphium.annotations.Embedded;
import de.caluga.morphium.annotations.Transient;
import de.caluga.morphium.writer.AsyncWriterImpl;
import de.caluga.morphium.writer.BufferedMorphiumWriterImpl;
import de.caluga.morphium.writer.MorphiumWriter;
import de.caluga.morphium.writer.MorphiumWriterImpl;

@Embedded
public class WriterSettings extends Settings {
    @Transient
    private MorphiumWriter writer;
    @Transient
    private MorphiumWriter bufferedWriter;
    @Transient
    private MorphiumWriter asyncWriter;
    // maximum number of tries to queue a write operation
    private int maximumRetriesBufferedWriter = 10;
    private int maximumRetriesWriter = 10;
    private int maximumRetriesAsyncWriter = 10;
    // wait bewteen tries
    private int retryWaitTimeBufferedWriter = 200;
    private int retryWaitTimeWriter = 200;
    private int retryWaitTimeAsyncWriter = 200;
    // default time for write buffer to be filled
    private int writeBufferTime = 1000;
    // ms for the pause of the main thread
    private int writeBufferTimeGranularity = 100;
    private int threadConnectionMultiplier = 5;
    // MOR-059: a rejected write (the writeErrors of an update reply) raises a MorphiumDriverException
    private boolean raiseOnWriteErrors = true;


    public MorphiumWriter getWriter() {
        if (writer == null) {
            writer = new MorphiumWriterImpl();
        }

        return writer;
    }
    public WriterSettings setWriter(MorphiumWriter writer) {
        this.writer = writer;
        return this;
    }
    public MorphiumWriter getBufferedWriter() {
        if (bufferedWriter == null) {
            bufferedWriter = new BufferedMorphiumWriterImpl();
        }

        return bufferedWriter;
    }
    public WriterSettings setBufferedWriter(MorphiumWriter bufferedWriter) {
        this.bufferedWriter = bufferedWriter;
        return this;
    }
    public MorphiumWriter getAsyncWriter() {
        if (asyncWriter == null) {
            asyncWriter = new AsyncWriterImpl();
        }

        return asyncWriter;
    }
    public WriterSettings setAsyncWriter(MorphiumWriter asyncWriter) {
        this.asyncWriter = asyncWriter;
        return this;
    }
    public int getMaximumRetriesBufferedWriter() {
        return maximumRetriesBufferedWriter;
    }
    public WriterSettings setMaximumRetriesBufferedWriter(int maximumRetriesBufferedWriter) {
        this.maximumRetriesBufferedWriter = maximumRetriesBufferedWriter;
        return this;
    }
    public int getMaximumRetriesWriter() {
        return maximumRetriesWriter;
    }
    public WriterSettings setMaximumRetriesWriter(int maximumRetriesWriter) {
        this.maximumRetriesWriter = maximumRetriesWriter;
        return this;
    }
    public int getMaximumRetriesAsyncWriter() {
        return maximumRetriesAsyncWriter;
    }
    public WriterSettings setMaximumRetriesAsyncWriter(int maximumRetriesAsyncWriter) {
        this.maximumRetriesAsyncWriter = maximumRetriesAsyncWriter;
        return this;
    }
    public int getRetryWaitTimeBufferedWriter() {
        return retryWaitTimeBufferedWriter;
    }
    public WriterSettings setRetryWaitTimeBufferedWriter(int retryWaitTimeBufferedWriter) {
        this.retryWaitTimeBufferedWriter = retryWaitTimeBufferedWriter;
        return this;
    }
    public int getRetryWaitTimeWriter() {
        return retryWaitTimeWriter;
    }
    public WriterSettings setRetryWaitTimeWriter(int retryWaitTimeWriter) {
        this.retryWaitTimeWriter = retryWaitTimeWriter;
        return this;
    }
    public int getRetryWaitTimeAsyncWriter() {
        return retryWaitTimeAsyncWriter;
    }
    public WriterSettings setRetryWaitTimeAsyncWriter(int retryWaitTimeAsyncWriter) {
        this.retryWaitTimeAsyncWriter = retryWaitTimeAsyncWriter;
        return this;
    }
    public int getWriteBufferTime() {
        return writeBufferTime;
    }
    public WriterSettings setWriteBufferTime(int writeBufferTime) {
        this.writeBufferTime = writeBufferTime;
        return this;
    }
    public int getWriteBufferTimeGranularity() {
        return writeBufferTimeGranularity;
    }
    public WriterSettings setWriteBufferTimeGranularity(int writeBufferTimeGranularity) {
        this.writeBufferTimeGranularity = writeBufferTimeGranularity;
        return this;
    }
    public int getThreadConnectionMultiplier() {
        return threadConnectionMultiplier;
    }
    public WriterSettings setThreadConnectionMultiplier(int threadConnectionMultiplier) {
        this.threadConnectionMultiplier = threadConnectionMultiplier;
        return this;
    }

    /**
     * MOR-059: the {@code update} command reports a statement that the server rejects (duplicate key, a path below a scalar, ...) in its reply and does not fail.
     * With {@code true} (the default) the write methods of {@code Query} and {@code Morphium} (set, unset, inc, dec, push, pull, addToSet, currentDate, the entity
     * variants and the bulk context) raise a {@code MorphiumDriverException} for it, and a {@code WriteConcernNotSatisfiedException} if the write was applied but the
     * write concern was not satisfied. With {@code false} the old behaviour is restored: the reply is returned and nobody looks at it.
     * <p>
     * A temporary switch for the upgrade, deprecated and removed in 7.0. Overridable per query, see {@code Query.setRaiseOnWriteErrors}.
     *
     * @deprecated only for the migration, will be removed in 7.0
     */
    @Deprecated
    public boolean isRaiseOnWriteErrors() {
        return raiseOnWriteErrors;
    }

    @Deprecated
    public WriterSettings setRaiseOnWriteErrors(boolean raiseOnWriteErrors) {
        this.raiseOnWriteErrors = raiseOnWriteErrors;
        return this;
    }

    @Deprecated
    public WriterSettings enableRaiseOnWriteErrors() {
        raiseOnWriteErrors = true;
        return this;
    }

    @Deprecated
    public WriterSettings disableRaiseOnWriteErrors() {
        raiseOnWriteErrors = false;
        return this;
    }
}
