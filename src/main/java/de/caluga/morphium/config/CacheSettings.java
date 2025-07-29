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
}
