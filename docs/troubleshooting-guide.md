# Troubleshooting Guide

Common issues and their solutions when using Morphium.

## Connection Issues

### Cannot Connect to MongoDB

**Symptoms:**
- `MorphiumDriverException: Could not get connection to [host] in time [timeout]ms`
- Application startup fails with connection timeout

**Diagnosis:**
```xml
<!-- Enable debug logging for driver in log4j2.xml -->
<Logger name="de.caluga.morphium.driver.wire.PooledDriver" level="DEBUG" additivity="false">
    <AppenderRef ref="Console"/>
</Logger>
```

**Common Causes & Solutions:**

1. **Incorrect host/port configuration**
   ```java
   // Check your host configuration
   cfg.clusterSettings().addHostToSeed("correct-hostname", 27017);
   ```

2. **Network connectivity issues**
   - Verify MongoDB is running: `mongosh --host hostname:port`
   - Check firewall rules and network policies
   - Test connectivity: `telnet hostname 27017`

3. **Authentication failures**
   ```java
   // Verify credentials
   cfg.authSettings().setMongoLogin("correct-username");
   cfg.authSettings().setMongoPassword("correct-password");
   ```

4. **Connection pool exhaustion**
   ```java
   // Increase connection limits
   cfg.connectionSettings().setMaxConnectionsPerHost(200);
   cfg.connectionSettings().setMaxWaitTime(10000);
   ```

### Replica Set Connection Issues

**Symptoms:**
- `MorphiumDriverException: No primary node defined - not connected yet?`
- Intermittent connection failures

**Solutions:**
1. **Verify replica set configuration**
   ```javascript
   // In MongoDB shell
   rs.status()
   rs.conf()
   ```

2. **Check replica set name**
   ```java
   cfg.clusterSettings().setReplicaSetName("correct-replica-set-name");
   ```

3. **Add all replica set members to seed list**
   ```java
   cfg.clusterSettings().addHostToSeed("mongo1", 27017);
   cfg.clusterSettings().addHostToSeed("mongo2", 27017);
   cfg.clusterSettings().addHostToSeed("mongo3", 27017);
   ```

## Performance Issues

### Slow Query Performance

**Symptoms:**
- High response times
- CPU usage spikes
- Memory growth

**Diagnosis:**
```xml
<!-- Enable query logging in log4j2.xml -->
<Logger name="de.caluga.morphium" level="DEBUG" additivity="false">
    <AppenderRef ref="Console"/>
</Logger>
```

```java
// Monitor driver statistics
Map<DriverStatsKey, Double> stats = morphium.getDriver().getDriverStats();
System.out.println("Connections in use: " + stats.get(DriverStatsKey.CONNECTIONS_IN_USE));
System.out.println("Connections in pool: " + stats.get(DriverStatsKey.CONNECTIONS_IN_POOL));
```

**Solutions:**

1. **Add missing indexes**
   ```java
   @Entity
   @Index({"fieldName", "composite,field"}) // Add compound indexes
   public class MyEntity {
       @Index
       private String frequentlyQueriedField;
   }
   
   // Create indexes manually
   morphium.ensureIndicesFor(MyEntity.class);
   ```

2. **Optimize query patterns**
   ```java
   // Use projection to limit returned fields
   Query<MyEntity> q = morphium.createQueryFor(MyEntity.class)
       .f("status").eq("active")
       .project("name", "email"); // Only return specific fields
   
   // Use pagination for large result sets
   List<MyEntity> results = q.skip(offset).limit(pageSize).asList();
   ```

3. **Review caching configuration**
   ```java
   @Entity
   @Cache(timeout = 60000, maxEntries = 10000) // Adjust cache settings
   public class MyEntity { /* ... */ }
   ```

### Memory Issues

**Symptoms:**
- `OutOfMemoryError`
- Gradual memory increase
- Long GC pauses

**Diagnosis:**
```java
// Monitor connection pool stats
Map<DriverStatsKey, Double> stats = morphium.getDriver().getDriverStats();
Double connectionsInUse = stats.get(DriverStatsKey.CONNECTIONS_IN_USE);
Double connectionsInPool = stats.get(DriverStatsKey.CONNECTIONS_IN_POOL);

System.out.println("Memory usage - Connections: " + connectionsInUse + "/" + connectionsInPool);
```

**Solutions:**

1. **Connection pool sizing**
   ```java
   // Adjust pool size based on actual usage
   cfg.connectionSettings().setMaxConnectionsPerHost(50); // Reduce if too high
   cfg.connectionSettings().setMaxConnectionIdleTime(60000); // Close idle connections sooner
   cfg.connectionSettings().setMaxConnectionLifetime(300000); // Shorter connection lifetime
   ```

2. **Cache tuning**
   ```java
   @Entity
   @Cache(maxEntries = 1000, timeout = 30000, strategy = Cache.ClearStrategy.LRU)
   public class MyEntity { /* ... */ }
   ```

3. **Large result set handling**
   ```java
   // Use iterator for large datasets
   MorphiumIterator<MyEntity> iterator = query.asIterable(100); // Process in batches
   while (iterator.hasNext()) {
       MyEntity entity = iterator.next();
       // Process entity
   }
   ```

## Messaging Issues

### Messages Not Being Processed

**Symptoms:**
- Messages accumulating in queue
- Listeners not receiving messages
- High message latency

**Diagnosis:**
```java
// Check messaging statistics
MessagingStatsKey stats = messaging.getStats();
System.out.println("Messages processed: " + stats.getMessagesProcessed());
System.out.println("Messages failed: " + stats.getMessagesFailed());
```

**Solutions:**

1. **Check listener registration**
   ```java
   // Ensure listener is properly registered
   messaging.addListenerForTopic("mytopic", (morphiumMessaging, msg) -> {
       System.out.println("Processing: " + msg.getMsg());
       return null; // or return response message
   });
   
   // Verify messaging is started
   messaging.start();
   ```

2. **Increase processing capacity**
   ```java
   // Configure for higher throughput
   cfg.messagingSettings().setMessagingWindowSize(200);
   cfg.messagingSettings().setMessagingMultithreadded(true);
   cfg.messagingSettings().setPollPauseTime(100); // Reduce pause time
   ```

3. **Check for exceptions in listeners**
   ```java
   messaging.addListenerForTopic("mytopic", (morphiumMessaging, msg) -> {
       try {
           // Your processing logic
           return processMessage(msg);
       } catch (Exception e) {
           log.error("Message processing failed", e);
           // Decide whether to return error response or null
           return null;
       }
   });
   ```

### Message Ordering Issues

**Symptoms:**
- Messages processed out of order
- Race conditions in message processing

**Solutions:**

1. **Use exclusive messaging for ordering**
   ```java
   Msg msg = new Msg("ordered-topic", "content", "value");
   msg.setExclusive(true); // Ensure only one listener processes at a time
   messaging.sendMessage(msg);
   ```

2. **Single-threaded processing for critical topics**
   ```java
   // Configure single-threaded processing for specific topics
   cfg.messagingSettings().setMessagingMultithreadded(false);
   ```

## Logging and Debugging

### Enable Debug Logging

```xml
<!-- Configure logging in log4j2.xml -->
<Configuration>
    <Appenders>
        <Console name="Console">
            <PatternLayout pattern="%d{HH:mm:ss.SSS} [%t] %-5level %logger{36} - %msg%n"/>
        </Console>
        <File name="MorphiumLog" fileName="/var/log/morphium/app.log">
            <PatternLayout pattern="%d{yyyy-MM-dd HH:mm:ss.SSS} [%t] %-5level %logger{36} - %msg%n"/>
        </File>
    </Appenders>
    <Loggers>
        <!-- Global debug logging for all Morphium components -->
        <Logger name="de.caluga.morphium" level="DEBUG" additivity="false">
            <AppenderRef ref="Console"/>
            <AppenderRef ref="MorphiumLog"/>
        </Logger>
        
        <!-- Class-specific logging -->
        <Logger name="de.caluga.morphium.driver.wire.PooledDriver" level="DEBUG" additivity="false">
            <AppenderRef ref="MorphiumLog"/>
        </Logger>
        <Logger name="de.caluga.morphium.messaging.StdMessaging" level="DEBUG" additivity="false">
            <AppenderRef ref="MorphiumLog"/>
        </Logger>
        
        <!-- Package-level logging -->
        <Logger name="de.caluga.morphium.driver" level="DEBUG" additivity="false">
            <AppenderRef ref="MorphiumLog"/>
        </Logger>
    </Loggers>
</Configuration>
```

### Key Log Patterns to Watch

**Connection Issues:**
```
ERROR: Could not get connection to [host] in time [timeout]ms
WARN: Connection timeout
ERROR: Could not create connection to host [host]
```

**Performance Issues:**
```
WARN: Connection pool exhausted
DEBUG: Query took [time]ms
WARN: Cache hit ratio low: [ratio]
```

**Messaging Issues:**
```
ERROR: Message processing failed
WARN: Message queue size growing: [size]
DEBUG: Message processed in [time]ms
```

## Monitoring Key Metrics

### Connection Pool Metrics
```java
Map<DriverStatsKey, Double> stats = morphium.getDriver().getDriverStats();

// Monitor these key metrics:
Double connectionsInUse = stats.get(DriverStatsKey.CONNECTIONS_IN_USE);
Double connectionsInPool = stats.get(DriverStatsKey.CONNECTIONS_IN_POOL);
Double connectionsOpened = stats.get(DriverStatsKey.CONNECTIONS_OPENED);
Double connectionsClosed = stats.get(DriverStatsKey.CONNECTIONS_CLOSED);
Double errors = stats.get(DriverStatsKey.ERRORS);
Double threadsWaiting = stats.get(DriverStatsKey.THREADS_WAITING_FOR_CONNECTION);
```

### Performance Alerts

Set up monitoring for these conditions:

1. **Connection pool exhaustion**
   - `connectionsInUse / maxConnectionsPerHost > 0.8`

2. **High error rate**
   - `errors / (connectionsOpened + errors) > 0.05`

3. **Thread starvation**
   - `threadsWaiting > 0` for extended periods

4. **Connection churn**
   - High `connectionsOpened` and `connectionsClosed` rates

## Common Configuration Mistakes

### 1. Undersized Connection Pool
```java
// Problem: Too few connections for load
cfg.connectionSettings().setMaxConnectionsPerHost(5); // Too small

// Solution: Size based on concurrent operations
cfg.connectionSettings().setMaxConnectionsPerHost(50);
```

### 2. Excessive Connection Timeout
```java
// Problem: Long timeouts hide connection issues
cfg.connectionSettings().setMaxWaitTime(60000); // Too long

// Solution: Fail fast to detect issues
cfg.connectionSettings().setMaxWaitTime(5000);
```

### 3. Missing Indexes
```java
// Problem: No indexes on frequently queried fields
@Entity
public class User {
    private String email; // Frequently queried but not indexed
}

// Solution: Add appropriate indexes
@Entity
@Index({"email", "status,email"}) // Single and compound indexes
public class User {
    @Index
    private String email;
    private String status;
}
```

### 4. Inefficient Cache Configuration
```java
// Problem: Cache settings don't match usage patterns
@Cache(timeout = 1000, maxEntries = 100) // Too short timeout, too small cache

// Solution: Match cache to data access patterns
@Cache(timeout = 300000, maxEntries = 10000, strategy = Cache.ClearStrategy.LRU)
```

## Getting Help

When reporting issues, include:

1. **Configuration details** (sanitized)
2. **Error logs** with timestamps
3. **Driver statistics** output
4. **MongoDB version** and topology
5. **Java version** and JVM settings
6. **Morphium version**
7. **Steps to reproduce** the issue

```java
// Collect diagnostic information
System.out.println("Morphium version: " + Morphium.getVersion());
System.out.println("Java version: " + System.getProperty("java.version"));
System.out.println("Driver stats: " + morphium.getDriver().getDriverStats());
System.out.println("Configuration: " + morphium.getConfig().toString());
```