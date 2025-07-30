package de.caluga.morphium.config;

import de.caluga.morphium.annotations.Embedded;
import de.caluga.morphium.annotations.Transient;
import de.caluga.morphium.cache.MorphiumCache;

@Embedded
public class CacheSettings {
    @Transient
    private MorphiumCache cache;
    private int globalCacheValidTime = 5000;
    private int writeCacheTimeout = 5000;
    private boolean readCacheEnabled = true;
    private boolean asyncWritesEnabled = true;
    private boolean bufferedWritesEnabled = true;
    private int housekeepingTimeout;
    public int getHousekeepingTimeout() {
        return housekeepingTimeout;
    }
    public void setHousekeepingTimeout(int housekeepingTimeout) {
        this.housekeepingTimeout = housekeepingTimeout;
    }
    public MorphiumCache getCache() {
        return cache;
    }
    public void setCache(MorphiumCache cache) {
        this.cache = cache;
    }
    public int getGlobalCacheValidTime() {
        return globalCacheValidTime;
    }
    public void setGlobalCacheValidTime(int globalCacheValidTime) {
        this.globalCacheValidTime = globalCacheValidTime;
    }
    public int getWriteCacheTimeout() {
        return writeCacheTimeout;
    }
    public void setWriteCacheTimeout(int writeCacheTimeout) {
        this.writeCacheTimeout = writeCacheTimeout;
    }
    public boolean isReadCacheEnabled() {
        return readCacheEnabled;
    }
    public void setReadCacheEnabled(boolean readCacheEnabled) {
        this.readCacheEnabled = readCacheEnabled;
    }
    public boolean isAsyncWritesEnabled() {
        return asyncWritesEnabled;
    }
    public void setAsyncWritesEnabled(boolean asyncWritesEnabled) {
        this.asyncWritesEnabled = asyncWritesEnabled;
    }
    public boolean isBufferedWritesEnabled() {
        return bufferedWritesEnabled;
    }
    public void setBufferedWritesEnabled(boolean bufferedWritesEnabled) {
        this.bufferedWritesEnabled = bufferedWritesEnabled;
    }
}
