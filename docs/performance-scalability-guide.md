# Performance & Scalability Guide

Optimizing Morphium for different scale scenarios, from small applications to systems with hundreds of millions of documents.

## Scale Categories

### Small Scale (< 1M documents, < 10 concurrent users)
**Characteristics**: Single server, low concurrency, simple queries

**Recommended Configuration:**
```java
MorphiumConfig cfg = new MorphiumConfig();
cfg.connectionSettings().setMaxConnectionsPerHost(10);
cfg.connectionSettings().setMinConnectionsPerHost(2);
cfg.cacheSettings().setGlobalCacheValidTime(60000); // 1 minute cache
```

### Medium Scale (1M-100M documents, 10-100 concurrent users)
**Characteristics**: Replica set, moderate concurrency, mixed read/write workloads

**Recommended Configuration:**
```java
MorphiumConfig cfg = new MorphiumConfig();
cfg.connectionSettings().setMaxConnectionsPerHost(50);
cfg.connectionSettings().setMinConnectionsPerHost(10);
cfg.connectionSettings().setMaxWaitTime(5000);
cfg.cacheSettings().setGlobalCacheValidTime(300000); // 5 minute cache
cfg.messagingSettings().setMessagingWindowSize(200);
cfg.messagingSettings().setMessagingMultithreadded(true);
```

### Large Scale (100M+ documents, 100+ concurrent users)
**Characteristics**: Sharded clusters, high concurrency, complex workloads

**Recommended Configuration:**
```java
MorphiumConfig cfg = new MorphiumConfig();
cfg.connectionSettings().setMaxConnectionsPerHost(200);
cfg.connectionSettings().setMinConnectionsPerHost(50);
cfg.connectionSettings().setMaxWaitTime(3000); // Fail fast
cfg.connectionSettings().setRetriesOnNetworkError(10);
cfg.connectionSettings().setSleepBetweenErrorRetries(100);
cfg.cacheSettings().setGlobalCacheValidTime(600000); // 10 minute cache
cfg.messagingSettings().setMessagingWindowSize(500);
cfg.messagingSettings().setMessagingMultithreadded(true);
cfg.messagingSettings().setPollPauseTime(50); // Reduce latency
```

## Connection Pool Optimization

### Pool Sizing Guidelines

**Formula for connection pool sizing:**
```
MaxConnections = (Peak Concurrent Operations × Average Operation Time) / 1000ms
```

**Examples:**
- **100 ops/sec, 50ms avg operation time**: 100 × 50 / 1000 = 5 connections
- **1000 ops/sec, 100ms avg operation time**: 1000 × 100 / 1000 = 100 connections

### Monitor Pool Health
```java
// Regular monitoring
Map<DriverStatsKey, Double> stats = morphium.getDriver().getDriverStats();
double utilization = stats.get(DriverStatsKey.CONNECTIONS_IN_USE) / 
                    stats.get(DriverStatsKey.CONNECTIONS_IN_POOL);

if (utilization > 0.8) {
    // Consider increasing pool size
    System.out.println("WARNING: High connection pool utilization: " + utilization);
}

double waitingThreads = stats.get(DriverStatsKey.THREADS_WAITING_FOR_CONNECTION);
if (waitingThreads > 0) {
    System.out.println("WARNING: Threads waiting for connections: " + waitingThreads);
}
```

### Connection Lifecycle Tuning
```java
// For high-throughput systems
cfg.connectionSettings().setMaxConnectionIdleTime(300000); // 5 minutes
cfg.connectionSettings().setMaxConnectionLifetime(1800000); // 30 minutes

// For low-latency systems
cfg.connectionSettings().setMaxConnectionIdleTime(60000);  // 1 minute
cfg.connectionSettings().setMaxConnectionLifetime(600000); // 10 minutes
```

## Query Performance Optimization

### Index Strategy

**1. Single Field Indexes**
```java
@Entity
public class User {
    @Index
    private String email;      // Unique lookups
    
    @Index
    private String status;     // Filter queries
    
    @Index
    private Date lastLogin;    // Range queries
}
```

**2. Compound Indexes**
```java
@Entity
@Index({"status", "lastLogin"})        // Filter + sort
@Index({"userId", "timestamp"})        // User timeline queries
@Index({"category", "priority", "-created"}) // Multi-field filtering
public class Task {
    // Fields...
}
```

**3. Index for Large Collections**
For collections with 100M+ documents:
```java
@Entity
@Index({"partitionKey", "queryField"})  // Partition-aware indexing
@Index({"timestamp:1"})                 // Time-based partitioning
public class LogEntry {
    private String partitionKey; // e.g., date-based partition
    private Date timestamp;
    // Other fields...
}
```

### Query Patterns

**Efficient Queries:**
```java
// Good: Uses index, specific criteria
Query<User> q = morphium.createQueryFor(User.class)
    .f("status").eq("active")
    .f("lastLogin").gte(cutoffDate)
    .limit(100);

// Good: Projection reduces data transfer
Query<User> q = morphium.createQueryFor(User.class)
    .f("email").eq("user@example.com")
    .project("name", "email", "status");
```

**Avoid These Patterns:**
```java
// Bad: No index usage
Query<User> q = morphium.createQueryFor(User.class)
    .f("description").matches(".*keyword.*"); // Regex without index

// Bad: Large result sets without pagination
List<User> allUsers = morphium.createQueryFor(User.class).asList(); // Millions of records
```

### Large Result Set Handling
```java
// Use iterator for large datasets
MorphiumIterator<LogEntry> iterator = morphium.createQueryFor(LogEntry.class)
    .f("timestamp").gte(startDate)
    .f("timestamp").lt(endDate)
    .asIterable(1000); // Process in batches of 1000

while (iterator.hasNext()) {
    LogEntry entry = iterator.next();
    processEntry(entry); // Process one at a time
    
    // Optional: Track progress
    if (iterator.getCursor() % 10000 == 0) {
        System.out.println("Processed: " + iterator.getCursor() + "/" + iterator.getCount());
    }
}
```

## Caching Strategy

### Cache Configuration by Use Case

**High-Read, Low-Write (Reference Data):**
```java
@Entity
@Cache(timeout = 3600000, maxEntries = 50000, // 1 hour cache, large capacity
       strategy = Cache.ClearStrategy.LRU,
       syncCache = Cache.SyncCacheStrategy.CLEAR_TYPE_CACHE)
public class Country {
    // Rarely changing reference data
}
```

**Medium-Read, Medium-Write (User Data):**
```java
@Entity
@Cache(timeout = 300000, maxEntries = 10000,  // 5 minute cache
       strategy = Cache.ClearStrategy.LRU,
       syncCache = Cache.SyncCacheStrategy.UPDATE_ENTRY)
public class UserProfile {
    // User profile data with moderate changes
}
```

**High-Write, Low-Read (Logging Data):**
```java
@Entity
@NoCache // No caching for write-heavy data
@WriteBuffer(size = 5000, timeout = 10000,
             strategy = WriteBuffer.STRATEGY.WRITE_OLD)
public class AuditLog {
    // High-frequency writes, infrequent reads
}
```

### Cache Monitoring
```java
// Monitor cache effectiveness
MorphiumCache cache = morphium.getCache();
CacheStats stats = cache.getStats();

double hitRatio = stats.getHits() / (double)(stats.getHits() + stats.getMisses());
System.out.println("Cache hit ratio: " + hitRatio);

if (hitRatio < 0.7) {
    System.out.println("WARNING: Low cache hit ratio, consider tuning cache settings");
}
```

## Messaging System Performance

### High-Throughput Messaging
```java
// Configuration for high message volume
cfg.messagingSettings().setMessagingWindowSize(1000);     // Large batch size
cfg.messagingSettings().setMessagingMultithreadded(true); // Parallel processing
cfg.messagingSettings().setPollPauseTime(10);             // Aggressive polling
cfg.messagingSettings().setUseChangeStream(true);         // Real-time processing
```

### Message Processing Patterns

**Batch Processing:**
```java
messaging.addListenerForTopic("bulk-process", (morphiumMessaging, msg) -> {
    // Process multiple items in one message
    List<String> itemIds = (List<String>) msg.getMapValue().get("itemIds");
    processBatch(itemIds);
    return null;
});
```

**Load Balancing:**
```java
// Exclusive processing for load distribution
Msg msg = new Msg("work-queue", "process-item", itemId);
msg.setExclusive(true); // Only one worker processes this message
messaging.sendMessage(msg);
```

## Memory Management

### JVM Configuration for Different Scales

**Small Scale:**
```bash
-Xms512m -Xmx2g
-XX:+UseG1GC
```

**Medium Scale:**
```bash
-Xms2g -Xmx8g
-XX:+UseG1GC
-XX:MaxGCPauseMillis=200
-XX:G1HeapRegionSize=16m
```

**Large Scale:**
```bash
-Xms8g -Xmx32g
-XX:+UseG1GC
-XX:MaxGCPauseMillis=100
-XX:G1HeapRegionSize=32m
-XX:+UnlockExperimentalVMOptions
-XX:+UseJVMCICompiler
```

### Memory-Efficient Patterns
```java
// Use projections to reduce memory usage
Query<User> q = morphium.createQueryFor(User.class)
    .project("id", "name", "email"); // Only load needed fields

// Process large datasets with iterators
MorphiumIterator<Record> iter = query.asIterable(500, 5); // 500 per batch, 5 batches ahead
```

## Monitoring and Metrics

### Key Performance Indicators

**System Level:**
```java
// Monitor regularly (every minute)
Map<DriverStatsKey, Double> stats = morphium.getDriver().getDriverStats();

// Connection health
double connectionUtilization = stats.get(DriverStatsKey.CONNECTIONS_IN_USE) / 
                              stats.get(DriverStatsKey.CONNECTIONS_IN_POOL);

// Error rates
double errorRate = stats.get(DriverStatsKey.ERRORS) / 
                  stats.get(DriverStatsKey.CONNECTIONS_OPENED);

// System capacity
double waitingThreads = stats.get(DriverStatsKey.THREADS_WAITING_FOR_CONNECTION);
```

### Performance Benchmarking

**Simple Benchmark Template:**
```java
public class MorphiumBenchmark {
    public void benchmarkWrites(int count) {
        long start = System.currentTimeMillis();
        
        List<TestEntity> batch = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            TestEntity entity = new TestEntity();
            entity.setValue("Test value " + i);
            batch.add(entity);
            
            if (batch.size() == 1000) {
                morphium.storeList(batch);
                batch.clear();
            }
        }
        
        if (!batch.isEmpty()) {
            morphium.storeList(batch);
        }
        
        long duration = System.currentTimeMillis() - start;
        System.out.println("Wrote " + count + " records in " + duration + "ms");
        System.out.println("Rate: " + (count * 1000 / duration) + " records/sec");
    }
}
```

## Scaling Strategies

### Horizontal Scaling

**1. Collection Partitioning**
```java
// Time-based partitioning
@Entity(nameProvider = MonthlyPartitionProvider.class)
public class LogEntry {
    private Date timestamp;
    // Stored in collections like: log_entry_202412, log_entry_202501, etc.
}

// Hash-based partitioning
@Entity(nameProvider = HashPartitionProvider.class)
public class UserData {
    private String userId; // Partition key
    // Distributed across multiple collections based on hash
}
```

**2. Read Replicas**
```java
// Configure read preference for scaling reads
Query<User> readQuery = morphium.createQueryFor(User.class)
    .setReadPreferenceLevel(ReadPreferenceType.SECONDARY_PREFERRED);
```

### Vertical Scaling

**Connection Pool Scaling:**
```java
// Scale with server capacity
int cpuCores = Runtime.getRuntime().availableProcessors();
int maxConnections = Math.max(50, cpuCores * 10); // 10 connections per core

cfg.connectionSettings().setMaxConnectionsPerHost(maxConnections);
cfg.connectionSettings().setMinConnectionsPerHost(maxConnections / 4);
```

## Performance Testing

### Load Testing Template
```java
public class LoadTest {
    private final ExecutorService executor = Executors.newFixedThreadPool(20);
    
    public void runLoadTest(int totalOperations, int concurrency) {
        CountDownLatch latch = new CountDownLatch(concurrency);
        AtomicInteger operations = new AtomicInteger(totalOperations);
        
        for (int i = 0; i < concurrency; i++) {
            executor.submit(() -> {
                try {
                    while (operations.decrementAndGet() >= 0) {
                        // Perform operation (read/write/query)
                        performOperation();
                    }
                } finally {
                    latch.countDown();
                }
            });
        }
        
        latch.await();
    }
}
```

### Performance Regression Testing
```java
// Include in CI/CD pipeline
@Test
public void performanceRegressionTest() {
    long start = System.currentTimeMillis();
    
    // Run standard operations
    runStandardWorkload();
    
    long duration = System.currentTimeMillis() - start;
    
    // Assert performance hasn't degraded
    assertTrue("Performance regression detected", duration < BASELINE_TIME * 1.2);
}
```

This guide provides concrete strategies for scaling Morphium from small applications to systems handling hundreds of millions of documents, with specific configurations and monitoring approaches for each scale.