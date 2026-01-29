# Performance Comparison: v5.1.x vs v6.x

*Benchmarked on MongoDB 8.2, 3-node replica set, January 2026*

---

## Executive Summary

| Aspect | v5.1.x → v6.x | Improvement |
|--------|---------------|-------------|
| **Connection Pool** | Global lock → Per-host locking | **+38%** throughput |
| **Messaging** | Improved threading & lock handling | Better under load |
| **$in Queries** | Same (MongoDB indexed) | ~8% faster |
| **SSL/TLS** | Not available → Full support | ✅ New feature |

---

## Real Benchmark Results

### v5.1.9 vs v6.x (MongoDB Cluster)

| Benchmark | v5.1.9 | v6.x | Improvement |
|-----------|--------|------|-------------|
| **Connection Pool** (20 threads × 100 ops) | 22,869 ops/sec | 31,642 ops/sec | **+38%** |
| **Messaging** (500 msgs, default settings) | 10 msgs/sec* | 21 msgs/sec | +110% |
| **$in Query** (500 values, indexed) | 3.40 ms | 3.14 ms | +8% |
| **Bulk Writes** (10K docs) | 43,544 docs/sec | 38,219 docs/sec | -12%** |

*\*v5 messaging hit timeout (345/500 received) — may indicate stability issues under load.*
*\*\*Bulk write difference under investigation.*

### Messaging Performance by Backend

| Backend | Throughput | Latency | vs MongoDB |
|---------|------------|---------|------------|
| **MongoDB** (3-node replica set) | 89 msgs/sec | 11.28 ms | 1x |
| **MorphiumServer** | 223 msgs/sec | 4.47 ms | **2.5x faster** |
| **InMemory Driver** (direct) | 281 msgs/sec | 3.56 ms | **3.2x faster** |

> **Key insight:** MorphiumServer is 2.5x faster than real MongoDB for messaging tests!

### $in Query: Indexed vs Non-Indexed

| Field | MongoDB | InMemory |
|-------|---------|----------|
| Indexed (counter, 500 values) | 5.52 ms | 81.30 ms |
| Non-indexed (category, 50 values) | 10.39 ms | 16.60 ms |

MongoDB indexes make a huge difference. InMemory shows O(n×m) behavior without indexes.

---

## Architecture Improvements

### PooledDriver: Per-Host Locking

**v5.1.x:**
```java
// Global synchronized blocks - all hosts blocked
private synchronized MongoConnection borrowConnection(String host) {
    synchronized (connectionPool) { ... }
}
```

**v6.x:**
```java
// Per-host isolation with modern concurrency
private final Map<String, Host> hosts = new ConcurrentHashMap<>();

class Host {
    private final BlockingQueue<ConnectionContainer> pool;
    private final AtomicInteger borrowedConnections;
}
```

**Result:** Operations on different hosts don't block each other.

### Messaging Improvements

Both v5 and v6 use ChangeStream, but v6 has:

- **Better thread pool management** — Configurable core/max sizes
- **Improved resume token handling** — More reliable after disconnects  
- **Lock optimizations** — Less contention in message processing
- **Java 21 threading** — Ready for virtual threads

**Result:** Better throughput with optimized settings.

---

## MorphiumServer for Testing

MorphiumServer provides a MongoDB-compatible server backed by InMemoryDriver:

| Feature | Benefit |
|---------|---------|
| **No MongoDB required** | CI/CD without Docker |
| **2.5x faster messaging** | Faster test suites |
| **Full wire protocol** | Drop-in replacement |
| **Clustering support** | Test replica set scenarios |

### Quick Start

```bash
# Start server
mvn exec:java -Dexec.mainClass="de.caluga.morphium.server.MorphiumServerCLI" \
    -Dexec.args="-p 17017"

# Connect Morphium
MorphiumConfig cfg = new MorphiumConfig();
cfg.addHostToSeed("localhost:17017");
cfg.setDatabase("test");
Morphium m = new Morphium(cfg);
```

---

## InMemory vs MongoDB: When to Use What

| Use Case | Recommendation |
|----------|----------------|
| Unit tests | InMemory Driver (fastest) |
| Integration tests | MorphiumServer (realistic + fast) |
| Load testing | Real MongoDB (production-like) |
| CI/CD pipelines | MorphiumServer (no dependencies) |

### InMemory Driver Limitations

- No real indexes (full collection scan)
- $in queries are O(n×m) not O(n+m)
- No persistence across restarts

---

## Tuning Messaging Performance

Default settings are conservative. For high-throughput:

```java
// Optimized settings
SingleCollectionMessaging messaging = new SingleCollectionMessaging(
    morphium,
    10,      // pause: 10ms (default: 100ms)
    true,    // multithreaded
    100      // windowSize (default: 10)
);
```

| Setting | Default | Optimized | Effect |
|---------|---------|-----------|--------|
| pause | 100ms | 10ms | Lower latency |
| multithreaded | false | true | Parallel processing |
| windowSize | 10 | 100 | Batch efficiency |

---

## Migration Checklist

Upgrading from v5.1.x to v6.x:

- [ ] Java 21 required
- [ ] Update `Messaging` → `SingleCollectionMessaging`
- [ ] Review messaging settings for optimal performance
- [ ] Enable SSL/TLS for production (new in v6!)
- [ ] Consider MorphiumServer for tests

See [Migration Guide v5→v6](./howtos/migration-v5-to-v6.md) for details.

---

*Benchmarks run on Mac Studio M2 Ultra, MongoDB 8.2.4, Morphium 6.1.8-SNAPSHOT*
