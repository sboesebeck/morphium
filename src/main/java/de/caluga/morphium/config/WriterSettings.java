package de.caluga.morphium.config;

import de.caluga.morphium.annotations.Embedded;
import de.caluga.morphium.annotations.Transient;
import de.caluga.morphium.writer.AsyncWriterImpl;
import de.caluga.morphium.writer.BufferedMorphiumWriterImpl;
import de.caluga.morphium.writer.MorphiumWriter;
import de.caluga.morphium.writer.MorphiumWriterImpl;

@Embedded
public class WriterSettings {
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
}
