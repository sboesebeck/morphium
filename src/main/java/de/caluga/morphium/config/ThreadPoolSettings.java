package de.caluga.morphium.config;

import de.caluga.morphium.annotations.Embedded;

@Embedded
public class ThreadPoolSettings {

    private int threadPoolAsyncOpCoreSize = 1;
    private int threadPoolAsyncOpMaxSize = 1000;
    private long threadPoolAsyncOpKeepAliveTime = 1000;
    public int getThreadPoolAsyncOpCoreSize() {
        return threadPoolAsyncOpCoreSize;
    }
    public ThreadPoolSettings setThreadPoolAsyncOpCoreSize(int threadPoolAsyncOpCoreSize) {
        this.threadPoolAsyncOpCoreSize = threadPoolAsyncOpCoreSize;
        return this;
    }
    public int getThreadPoolAsyncOpMaxSize() {
        return threadPoolAsyncOpMaxSize;
    }
    public ThreadPoolSettings setThreadPoolAsyncOpMaxSize(int threadPoolAsyncOpMaxSize) {
        this.threadPoolAsyncOpMaxSize = threadPoolAsyncOpMaxSize;
        return this;
    }
    public long getThreadPoolAsyncOpKeepAliveTime() {
        return threadPoolAsyncOpKeepAliveTime;
    }
    public ThreadPoolSettings setThreadPoolAsyncOpKeepAliveTime(long threadPoolAsyncOpKeepAliveTime) {
        this.threadPoolAsyncOpKeepAliveTime = threadPoolAsyncOpKeepAliveTime;
        return this;
    }
}
