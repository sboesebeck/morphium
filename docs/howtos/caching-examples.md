# Caching Examples

Morphium provides entity‑level caching via `@Cache` and supports cluster synchronization and JCache.

1) Basic entity caching
```java
import de.caluga.morphium.annotations.caching.Cache;

@Entity
@Cache(
  timeout = 30_000,              // 30s TTL
  clearOnWrite = true,           // clear type cache on writes
  strategy = Cache.ClearStrategy.LRU,
  readCache = true,
  syncCache = Cache.SyncCacheStrategy.CLEAR_TYPE_CACHE
)
public class Product { ... }
```

2) Per‑type TTL override
```java
// Set a longer TTL for Product results at runtime
morphium.getCache().setValidCacheTime(Product.class, 120_000);
```

3) Cross‑node cache synchronization
```java
// Initialize messaging and start it
StdMessaging messaging = new StdMessaging();
messaging.init(morphium);
messaging.start();

// Attach synchronizer: clears caches on other nodes when writes occur
MessagingCacheSynchronizer sync = new MessagingCacheSynchronizer(messaging, morphium);

// Optional: send a manual clear‑all
sync.sendClearAllMessage("maintenance");
```

4) Switch to JCache implementation
```java
// Use javax.cache‑based cache impl
MorphiumCacheJCacheImpl jcache = new MorphiumCacheJCacheImpl();
cfg.cacheSettings().setCache(jcache);

// Optional: plug in a custom CacheManager
// jcache.setCacheManager(myCacheManager);
```

5) Manual cache operations
```java
// Clear one type
morphium.clearCachefor(Product.class);

// Reset all caches
morphium.getCache().resetCache();

// Inspect an object by ID from cache (if present)
Product p = morphium.getCache().getFromIDCache(Product.class, productId);
```

6) Cache listeners (observe additions/removals)
```java
morphium.getCache().addCacheListener(new CacheListener() {
  @Override public CacheEntry wouldAddToCache(String key, CacheEntry ce, boolean update) { return ce; }
  @Override public boolean wouldClearCache(Class<?> type) { return true; }
  @Override public boolean wouldRemoveEntryFromCache(String key, CacheEntry ce, boolean expired) { return true; }
});
```

Notes
- `@Cache.syncCache` controls how remote nodes update their caches:
  - `CLEAR_TYPE_CACHE`: clear entire type cache on write
  - `REMOVE_ENTRY_FROM_TYPE_CACHE`: remove single entry by ID
  - `UPDATE_ENTRY`: re‑read updated entries
- Ensure messaging is running on all nodes if you want cluster cache synchronization.
See also: Cache Patterns, Developer Guide (Caching)
