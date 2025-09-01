# Cache Patterns

This page outlines common caching patterns with Morphium and when to use them.

1) Read‑through entity cache (default)
- Use `@Cache` on entities to enable read cache for queries and ID cache
- Choose a clear strategy that matches your write rate and consistency needs
```java
@Cache(timeout = 30_000, clearOnWrite = true, strategy = Cache.ClearStrategy.LRU, syncCache = Cache.SyncCacheStrategy.CLEAR_TYPE_CACHE)
public class Product { ... }
```
When to use
- Most read‑heavy entities, where occasional stale reads for up to the TTL are acceptable

2) Precise invalidation per entity (remove or update)
- Reduce blast radius on writes by removing/updating only affected IDs instead of clearing the whole type cache
```java
@Cache(timeout = 60_000, clearOnWrite = true, syncCache = Cache.SyncCacheStrategy.REMOVE_ENTRY_FROM_TYPE_CACHE)
public class User { ... }
```
or
```java
@Cache(timeout = 60_000, clearOnWrite = true, syncCache = Cache.SyncCacheStrategy.UPDATE_ENTRY)
public class User { ... }
```
Notes
- With `UPDATE_ENTRY`, other nodes re‑read the updated document (slightly higher cost, but fewer cache misses)

3) Cluster‑wide synchronization via messaging
- Ensure every node runs messaging and attach a `MessagingCacheSynchronizer`
```java
StdMessaging messaging = new StdMessaging();
messaging.init(morphium);
messaging.start();
new MessagingCacheSynchronizer(messaging, morphium);
```
When to use
- Multi‑node deployments that need consistent caches after writes

4) TTL tuning and hot‑set sizing
- Keep `@Cache.timeout` small enough to minimize staleness, large enough to reduce DB load
- Use `maxEntries` and `strategy` to control memory use and eviction behavior
```java
@Cache(timeout = 15_000, maxEntries = 50_000, strategy = Cache.ClearStrategy.LRU)
public class Article { ... }
```

5) JCache integration and layering
- Use `MorphiumCacheJCacheImpl` to adopt javax.cache
- Optionally back it with an external provider for cross‑JVM cache layers
```java
MorphiumCacheJCacheImpl jcache = new MorphiumCacheJCacheImpl();
cfg.cacheSettings().setCache(jcache);
```

6) Avoiding cache stampede
- For heavy queries, consider:
  - Pre‑warming popular keys at startup
  - Adding small jitter to TTL at application level if you implement explicit invalidation
  - Using `UPDATE_ENTRY` to keep hot IDs fresh without clearing type cache

7) Eventual vs strong consistency
- Read‑through caching is eventually consistent between nodes
- To minimize staleness:
  - Use smaller TTLs
  - Prefer `REMOVE_ENTRY_FROM_TYPE_CACHE` or `UPDATE_ENTRY`
  - Ensure messaging is available and responsive (replica set + change streams is preferred)

8) Batching and write buffering
- Global write buffering and async writes reduce DB pressure under load
```java
cfg.cacheSettings().setAsyncWritesEnabled(true);
cfg.cacheSettings().setBufferedWritesEnabled(true);
```
Trade‑off
- Buffering can delay writes; choose TTLs and cache sync strategies accordingly

9) Manual controls for maintenance
```java
// Clear one type
morphium.clearCachefor(Product.class);
// Clear all types on all nodes
new MessagingCacheSynchronizer(messaging, morphium).sendClearAllMessage("maintenance");
```

10) Query‑result cache keys
- Morphium computes a deterministic key for queries (criteria + sort + projection + paging)
- Prefer projections to keep cached result documents small

See also
- [Caching Examples](./caching-examples.md)
- [Developer Guide](../developer-guide.md)
