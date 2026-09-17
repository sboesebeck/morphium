# Monitoring & Metrics Guide

Comprehensive guide for monitoring Morphium applications with focus on DriverStats and performance metrics.

## DriverStats - Core Monitoring Foundation

DriverStats are **essential for monitoring driver performance**, especially **connection pool health** and **issue detection**. These metrics provide real-time insights into Morphium's internal state.

### Accessing DriverStats

```java
// Get current driver statistics
Map<DriverStatsKey, Double> stats = morphium.getDriver().getDriverStats();

// All metrics are cumulative counters or current values
Double connectionsInUse = stats.get(DriverStatsKey.CONNECTIONS_IN_USE);
Double connectionsInPool = stats.get(DriverStatsKey.CONNECTIONS_IN_POOL);
Double errors = stats.get(DriverStatsKey.ERRORS);
```

## Complete DriverStats Reference

### Connection Pool Metrics

| Metric | Type | Description | Healthy Range |
|--------|------|-------------|---------------|
| `CONNECTIONS_IN_USE` | Current | Active connections currently borrowed | < 80% of max pool size |
| `CONNECTIONS_IN_POOL` | Current | Available connections in pool | > 10% of max pool size |
| `CONNECTIONS_OPENED` | Counter | Total connections opened since start | Monotonic increasing |
| `CONNECTIONS_CLOSED` | Counter | Total connections closed since start | Should be < OPENED |
| `CONNECTIONS_RELEASED` | Counter | Connections returned to pool | Should match borrowing patterns |

### Performance Metrics

| Metric | Type | Description | Monitoring Goal |
|--------|------|-------------|-----------------|
| `THREADS_WAITING_FOR_CONNECTION` | Current | Threads waiting for available connection | Should be 0 |
| `ERRORS` | Counter | Total driver errors encountered | Low and stable |
| `NETWORK_ERRORS` | Counter | Network-related errors | Temporary spikes only |

### Example Monitoring Implementation

```java
@Component
@Scheduled(fixedDelay = 60000) // Every minute
public class MorphiumMonitor {
    
    private final Morphium morphium;
    private final MeterRegistry meterRegistry; // Micrometer for metrics export
    
    public void collectDriverStats() {
        Map<DriverStatsKey, Double> stats = morphium.getDriver().getDriverStats();
        
        // Export all stats to monitoring system
        stats.forEach((key, value) -> {
            Gauge.builder("morphium.driver." + key.name().toLowerCase())
                 .register(meterRegistry, () -> value);
        });
        
        // Calculate derived metrics
        double connectionUtilization = calculateConnectionUtilization(stats);
        double errorRate = calculateErrorRate(stats);
        
        // Export derived metrics
        Gauge.builder("morphium.connection.utilization")
             .register(meterRegistry, () -> connectionUtilization);
        
        Gauge.builder("morphium.connection.error_rate")
             .register(meterRegistry, () -> errorRate);
    }
    
    private double calculateConnectionUtilization(Map<DriverStatsKey, Double> stats) {
        Double inUse = stats.get(DriverStatsKey.CONNECTIONS_IN_USE);
        Double inPool = stats.get(DriverStatsKey.CONNECTIONS_IN_POOL);
        
        if (inPool == null || inPool == 0) return 0.0;
        return inUse / inPool;
    }
    
    private double calculateErrorRate(Map<DriverStatsKey, Double> stats) {
        Double errors = stats.get(DriverStatsKey.ERRORS);
        Double opened = stats.get(DriverStatsKey.CONNECTIONS_OPENED);
        
        if (opened == null || opened == 0) return 0.0;
        return errors / opened;
    }
}
```

## Connection Pool Health Monitoring

### Critical Health Indicators

**1. Pool Utilization**
```java
// Monitor connection pool utilization
public void checkPoolHealth() {
    Map<DriverStatsKey, Double> stats = morphium.getDriver().getDriverStats();
    
    double utilization = stats.get(DriverStatsKey.CONNECTIONS_IN_USE) / 
                        stats.get(DriverStatsKey.CONNECTIONS_IN_POOL);
    
    if (utilization > 0.8) {
        // CRITICAL: High utilization - may need pool scaling
        logger.warn("High connection pool utilization: {}%", utilization * 100);
        alertManager.send(Alert.HIGH_POOL_UTILIZATION, utilization);
    } else if (utilization > 0.6) {
        // WARNING: Moderate utilization - monitor closely
        logger.info("Moderate connection pool utilization: {}%", utilization * 100);
    }
}
```

**2. Thread Starvation Detection**
```java
public void checkThreadStarvation() {
    Map<DriverStatsKey, Double> stats = morphium.getDriver().getDriverStats();
    
    Double waitingThreads = stats.get(DriverStatsKey.THREADS_WAITING_FOR_CONNECTION);
    
    if (waitingThreads > 0) {
        // CRITICAL: Threads waiting for connections
        logger.error("Thread starvation detected: {} threads waiting", waitingThreads);
        alertManager.send(Alert.THREAD_STARVATION, waitingThreads);
        
        // Additional diagnostics
        double utilization = stats.get(DriverStatsKey.CONNECTIONS_IN_USE) / 
                           stats.get(DriverStatsKey.CONNECTIONS_IN_POOL);
        logger.error("Current pool utilization: {}%", utilization * 100);
    }
}
```

**3. Connection Churn Analysis**
```java
public void analyzeConnectionChurn() {
    Map<DriverStatsKey, Double> stats = morphium.getDriver().getDriverStats();
    
    Double opened = stats.get(DriverStatsKey.CONNECTIONS_OPENED);
    Double closed = stats.get(DriverStatsKey.CONNECTIONS_CLOSED);
    
    // High churn might indicate configuration issues
    double churnRate = closed / opened;
    
    if (churnRate > 0.5) {
        logger.warn("High connection churn rate: {}% of connections are closed", 
                   churnRate * 100);
        // May indicate:
        // - MaxConnectionLifetime too short
        // - MaxConnectionIdleTime too short
        // - Network instability
    }
}
```

## Real-Time Monitoring Dashboard

### Grafana Dashboard Configuration

**DriverStats Panel Queries (Prometheus):**
```promql
# Connection pool utilization
morphium_driver_connections_in_use / morphium_driver_connections_in_pool * 100

# Error rate
rate(morphium_driver_errors[5m])

# Threads waiting for connections
morphium_driver_threads_waiting_for_connection

# Connection opening rate
rate(morphium_driver_connections_opened[5m])

# Connection closing rate  
rate(morphium_driver_connections_closed[5m])
```

### Alert Rules Configuration

```yaml
# alerting-rules.yml
groups:
- name: morphium.rules
  rules:
  - alert: MorphiumHighConnectionUtilization
    expr: morphium_connection_utilization > 0.8
    for: 2m
    labels:
      severity: warning
    annotations:
      summary: "Morphium connection pool utilization high"
      description: "Connection pool is {{ $value }}% utilized"

  - alert: MorphiumThreadStarvation
    expr: morphium_driver_threads_waiting_for_connection > 0
    for: 30s
    labels:
      severity: critical
    annotations:
      summary: "Morphium threads waiting for connections"
      description: "{{ $value }} threads are waiting for connections"

  - alert: MorphiumHighErrorRate
    expr: rate(morphium_driver_errors[5m]) > 0.1
    for: 1m
    labels:
      severity: warning
    annotations:
      summary: "High Morphium driver error rate"
      description: "Error rate is {{ $value }} errors/second"
```

## Application-Level Metrics

### Query Performance Monitoring

```java
@Component
public class QueryMetricsCollector {
    
    private final MeterRegistry meterRegistry;
    private final Timer queryTimer;
    
    public QueryMetricsCollector(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
        this.queryTimer = Timer.builder("morphium.query.duration")
                              .description("Query execution time")
                              .register(meterRegistry);
    }
    
    // Wrap queries with timing
    public <T> List<T> timedQuery(Query<T> query) {
        return queryTimer.recordCallable(() -> query.asList());
    }
    
    // Monitor different query types
    public <T> List<T> timedQueryWithTags(Query<T> query, String collection, String operation) {
        return Timer.builder("morphium.query.duration")
                   .tag("collection", collection)
                   .tag("operation", operation)
                   .register(meterRegistry)
                   .recordCallable(() -> query.asList());
    }
}
```

### Cache Performance Monitoring

```java
@Component
public class CacheMetricsCollector {
    
    public void collectCacheMetrics() {
        // Note: Cache metrics depend on cache implementation
        // This is conceptual - actual implementation may vary
        
        MorphiumCache cache = morphium.getCache();
        
        if (cache instanceof MorphiumCacheImpl) {
            MorphiumCacheImpl cacheImpl = (MorphiumCacheImpl) cache;
            
            // Collect cache statistics
            long hits = cacheImpl.getHits();
            long misses = cacheImpl.getMisses();
            long evictions = cacheImpl.getEvictions();
            
            double hitRatio = (double) hits / (hits + misses);
            
            Gauge.builder("morphium.cache.hit_ratio")
                 .register(meterRegistry, () -> hitRatio);
                 
            Counter.builder("morphium.cache.hits")
                   .register(meterRegistry)
                   .increment(hits);
                   
            Counter.builder("morphium.cache.misses")
                   .register(meterRegistry)
                   .increment(misses);
        }
    }
}
```

## Messaging System Monitoring

### Message Queue Metrics

```java
@Component
public class MessagingMetricsCollector {
    
    private final Messaging messaging;
    
    @Scheduled(fixedDelay = 30000)
    public void collectMessagingMetrics() {
        // Monitor message processing
        // Note: Exact metrics depend on messaging implementation
        
        // Queue depth monitoring
        for (String topic : getActiveTopics()) {
            long queueDepth = getQueueDepth(topic);
            
            Gauge.builder("morphium.messaging.queue_depth")
                 .tag("topic", topic)
                 .register(meterRegistry, () -> queueDepth);
                 
            if (queueDepth > 1000) {
                logger.warn("High queue depth for topic {}: {}", topic, queueDepth);
            }
        }
    }
    
    // Message processing rate tracking
    public void trackMessageProcessed(String topic, boolean success, long processingTime) {
        Counter.builder("morphium.messaging.messages_processed")
               .tag("topic", topic)
               .tag("status", success ? "success" : "error")
               .register(meterRegistry)
               .increment();
               
        Timer.builder("morphium.messaging.processing_duration")
             .tag("topic", topic)
             .register(meterRegistry)
             .record(processingTime, TimeUnit.MILLISECONDS);
    }
}
```

## Health Checks and Diagnostics

### Comprehensive Health Check

```java
@Component
public class MorphiumHealthIndicator implements HealthIndicator {
    
    private final Morphium morphium;
    
    @Override
    public Health health() {
        Health.Builder builder = new Health.Builder();
        
        try {
            // Test basic connectivity
            long start = System.currentTimeMillis();
            morphium.createQueryFor(User.class).limit(1).asList();
            long responseTime = System.currentTimeMillis() - start;
            
            // Get driver statistics for detailed health info
            Map<DriverStatsKey, Double> stats = morphium.getDriver().getDriverStats();
            
            // Analyze connection pool health
            double utilization = stats.get(DriverStatsKey.CONNECTIONS_IN_USE) / 
                               stats.get(DriverStatsKey.CONNECTIONS_IN_POOL);
            double waitingThreads = stats.get(DriverStatsKey.THREADS_WAITING_FOR_CONNECTION);
            
            // Determine overall health
            if (utilization < 0.8 && waitingThreads == 0 && responseTime < 1000) {
                builder.status(Status.UP);
            } else if (utilization < 0.9 && waitingThreads == 0 && responseTime < 3000) {
                builder.status("DEGRADED");
            } else {
                builder.status(Status.DOWN);
            }
            
            // Add detailed metrics
            builder.withDetail("database", Map.of(
                "responseTime", responseTime + "ms",
                "status", responseTime < 3000 ? "UP" : "SLOW"
            ));
            
            builder.withDetail("connectionPool", Map.of(
                "utilization", String.format("%.1f%%", utilization * 100),
                "connectionsInUse", stats.get(DriverStatsKey.CONNECTIONS_IN_USE).intValue(),
                "connectionsInPool", stats.get(DriverStatsKey.CONNECTIONS_IN_POOL).intValue(),
                "threadsWaiting", waitingThreads.intValue(),
                "totalErrors", stats.get(DriverStatsKey.ERRORS).intValue()
            ));
            
            return builder.build();
            
        } catch (Exception e) {
            return builder.status(Status.DOWN)
                         .withException(e)
                         .build();
        }
    }
}
```

### Diagnostic Information Collection

```java
@RestController
@RequestMapping("/admin/morphium")
public class MorphiumDiagnosticsController {
    
    @GetMapping("/stats")
    public ResponseEntity<Map<String, Object>> getDriverStats() {
        Map<DriverStatsKey, Double> rawStats = morphium.getDriver().getDriverStats();
        
        Map<String, Object> response = new HashMap<>();
        rawStats.forEach((key, value) -> 
            response.put(key.name().toLowerCase(), value));
        
        // Add derived metrics
        response.put("connection_utilization", 
            rawStats.get(DriverStatsKey.CONNECTIONS_IN_USE) / 
            rawStats.get(DriverStatsKey.CONNECTIONS_IN_POOL));
            
        return ResponseEntity.ok(response);
    }
    
    @GetMapping("/config")
    public ResponseEntity<Map<String, Object>> getConfiguration() {
        MorphiumConfig config = morphium.getConfig();
        
        Map<String, Object> response = new HashMap<>();
        response.put("maxConnectionsPerHost", config.connectionSettings().getMaxConnectionsPerHost());
        response.put("minConnectionsPerHost", config.connectionSettings().getMinConnectionsPerHost());
        response.put("maxWaitTime", config.connectionSettings().getMaxWaitTime());
        response.put("database", config.connectionSettings().getDatabase());
        response.put("driverName", config.driverSettings().getDriverName());
        
        return ResponseEntity.ok(response);
    }
    
    @PostMapping("/connection-pool/analyze")
    public ResponseEntity<Map<String, Object>> analyzeConnectionPool() {
        Map<DriverStatsKey, Double> stats = morphium.getDriver().getDriverStats();
        
        Map<String, Object> analysis = new HashMap<>();
        
        // Connection pool analysis
        double utilization = stats.get(DriverStatsKey.CONNECTIONS_IN_USE) / 
                           stats.get(DriverStatsKey.CONNECTIONS_IN_POOL);
        analysis.put("utilization", utilization);
        
        // Health assessment
        List<String> issues = new ArrayList<>();
        List<String> recommendations = new ArrayList<>();
        
        if (utilization > 0.8) {
            issues.add("High connection pool utilization (" + (utilization * 100) + "%)");
            recommendations.add("Consider increasing maxConnectionsPerHost");
        }
        
        if (stats.get(DriverStatsKey.THREADS_WAITING_FOR_CONNECTION) > 0) {
            issues.add("Threads waiting for connections");
            recommendations.add("Increase connection pool size or optimize query performance");
        }
        
        double errorRate = stats.get(DriverStatsKey.ERRORS) / 
                          stats.get(DriverStatsKey.CONNECTIONS_OPENED);
        if (errorRate > 0.05) {
            issues.add("High error rate (" + (errorRate * 100) + "%)");
            recommendations.add("Check network stability and MongoDB health");
        }
        
        analysis.put("issues", issues);
        analysis.put("recommendations", recommendations);
        analysis.put("healthScore", calculateHealthScore(stats));
        
        return ResponseEntity.ok(analysis);
    }
    
    private int calculateHealthScore(Map<DriverStatsKey, Double> stats) {
        double utilization = stats.get(DriverStatsKey.CONNECTIONS_IN_USE) / 
                           stats.get(DriverStatsKey.CONNECTIONS_IN_POOL);
        double waitingThreads = stats.get(DriverStatsKey.THREADS_WAITING_FOR_CONNECTION);
        double errorRate = stats.get(DriverStatsKey.ERRORS) / 
                          stats.get(DriverStatsKey.CONNECTIONS_OPENED);
        
        int score = 100;
        
        if (utilization > 0.9) score -= 30;
        else if (utilization > 0.8) score -= 15;
        else if (utilization > 0.6) score -= 5;
        
        if (waitingThreads > 0) score -= 40;
        
        if (errorRate > 0.1) score -= 25;
        else if (errorRate > 0.05) score -= 10;
        
        return Math.max(0, score);
    }
}
```

## Monitoring Best Practices

### 1. Baseline Establishment
- Monitor DriverStats for 1-2 weeks to establish baseline performance
- Document normal operating ranges for each metric
- Set alert thresholds based on observed patterns

### 2. Proactive Monitoring
- **Connection utilization** > 60%: Monitor closely
- **Connection utilization** > 80%: Plan capacity increase
- **Threads waiting** > 0: Immediate investigation required
- **Error rate** > 5%: Check MongoDB and network health

### 3. Regular Health Checks
- Automated health checks every minute
- Deep diagnostic analysis every hour
- Weekly performance trend analysis

### 4. Incident Response
- **Level 1**: Connection utilization > 90% - Scale immediately
- **Level 2**: Threads waiting for connections - Emergency response
- **Level 3**: Error rate > 10% - Full system investigation

This monitoring guide provides comprehensive coverage of Morphium's DriverStats and ensures optimal connection pool performance and early issue detection.