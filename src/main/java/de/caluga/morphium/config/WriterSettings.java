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
    public void setWriter(MorphiumWriter writer) {
        this.writer = writer;
    }
    public MorphiumWriter getBufferedWriter() {
        if (bufferedWriter == null) {
            bufferedWriter = new BufferedMorphiumWriterImpl();
        }

        return bufferedWriter;
    }
    public void setBufferedWriter(MorphiumWriter bufferedWriter) {
        this.bufferedWriter = bufferedWriter;
    }
    public MorphiumWriter getAsyncWriter() {
        if (asyncWriter == null) {
            asyncWriter = new AsyncWriterImpl();
        }

        return asyncWriter;
    }
    public void setAsyncWriter(MorphiumWriter asyncWriter) {
        this.asyncWriter = asyncWriter;
    }
    public int getMaximumRetriesBufferedWriter() {
        return maximumRetriesBufferedWriter;
    }
    public void setMaximumRetriesBufferedWriter(int maximumRetriesBufferedWriter) {
        this.maximumRetriesBufferedWriter = maximumRetriesBufferedWriter;
    }
    public int getMaximumRetriesWriter() {
        return maximumRetriesWriter;
    }
    public void setMaximumRetriesWriter(int maximumRetriesWriter) {
        this.maximumRetriesWriter = maximumRetriesWriter;
    }
    public int getMaximumRetriesAsyncWriter() {
        return maximumRetriesAsyncWriter;
    }
    public void setMaximumRetriesAsyncWriter(int maximumRetriesAsyncWriter) {
        this.maximumRetriesAsyncWriter = maximumRetriesAsyncWriter;
    }
    public int getRetryWaitTimeBufferedWriter() {
        return retryWaitTimeBufferedWriter;
    }
    public void setRetryWaitTimeBufferedWriter(int retryWaitTimeBufferedWriter) {
        this.retryWaitTimeBufferedWriter = retryWaitTimeBufferedWriter;
    }
    public int getRetryWaitTimeWriter() {
        return retryWaitTimeWriter;
    }
    public void setRetryWaitTimeWriter(int retryWaitTimeWriter) {
        this.retryWaitTimeWriter = retryWaitTimeWriter;
    }
    public int getRetryWaitTimeAsyncWriter() {
        return retryWaitTimeAsyncWriter;
    }
    public void setRetryWaitTimeAsyncWriter(int retryWaitTimeAsyncWriter) {
        this.retryWaitTimeAsyncWriter = retryWaitTimeAsyncWriter;
    }
    public int getWriteBufferTime() {
        return writeBufferTime;
    }
    public void setWriteBufferTime(int writeBufferTime) {
        this.writeBufferTime = writeBufferTime;
    }
    public int getWriteBufferTimeGranularity() {
        return writeBufferTimeGranularity;
    }
    public void setWriteBufferTimeGranularity(int writeBufferTimeGranularity) {
        this.writeBufferTimeGranularity = writeBufferTimeGranularity;
    }
    public int getThreadConnectionMultiplier() {
        return threadConnectionMultiplier;
    }
    public void setThreadConnectionMultiplier(int threadConnectionMultiplier) {
        this.threadConnectionMultiplier = threadConnectionMultiplier;
    }
}
