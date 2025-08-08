package de.caluga.morphium.config;

import de.caluga.morphium.annotations.Embedded;
import de.caluga.morphium.annotations.Transient;
import de.caluga.morphium.cache.MorphiumCache;

@Embedded
public class CacheSettings extends Settings {
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
    public CacheSettings setHousekeepingTimeout(int housekeepingTimeout) {
        this.housekeepingTimeout = housekeepingTimeout;
        return this;
    }
    public MorphiumCache getCache() {
        return cache;
    }
    public CacheSettings setCache(MorphiumCache cache) {
        this.cache = cache;
        return this;
    }
    public int getGlobalCacheValidTime() {
        return globalCacheValidTime;
    }
    public CacheSettings setGlobalCacheValidTime(int globalCacheValidTime) {
        this.globalCacheValidTime = globalCacheValidTime;
        return this;
    }
    public int getWriteCacheTimeout() {
        return writeCacheTimeout;
    }
    public CacheSettings setWriteCacheTimeout(int writeCacheTimeout) {
        this.writeCacheTimeout = writeCacheTimeout;
        return this;
    }
    public boolean isReadCacheEnabled() {
        return readCacheEnabled;
    }
    public CacheSettings setReadCacheEnabled(boolean readCacheEnabled) {
        this.readCacheEnabled = readCacheEnabled;
        return this;
    }
    public boolean isAsyncWritesEnabled() {
        return asyncWritesEnabled;
    }
    public CacheSettings setAsyncWritesEnabled(boolean asyncWritesEnabled) {
        this.asyncWritesEnabled = asyncWritesEnabled;
        return this;
    }
    public boolean isBufferedWritesEnabled() {
        return bufferedWritesEnabled;
    }
    public CacheSettings setBufferedWritesEnabled(boolean bufferedWritesEnabled) {
        this.bufferedWritesEnabled = bufferedWritesEnabled;
        return this;
    }
}
