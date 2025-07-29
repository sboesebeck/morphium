package de.caluga.morphium.config;

import de.caluga.morphium.annotations.Embedded;
import de.caluga.morphium.annotations.Transient;
import de.caluga.morphium.writer.MorphiumWriter;

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
}
