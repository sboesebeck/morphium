package de.caluga.morphium.config;

import de.caluga.morphium.annotations.Embedded;

@Embedded
public class ThreadPoolSettings {

    private int threadPoolAsyncOpCoreSize = 1;
    private int threadPoolAsyncOpMaxSize = 1000;
    private long threadPoolAsyncOpKeepAliveTime = 1000;
}
