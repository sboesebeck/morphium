# Production Deployment Guide

Comprehensive guide for deploying Morphium applications in production environments.

## Performance Baseline

**Morphium Performance Advantage:**
- **At least 10% faster** than standard MongoDB Java driver with mapping
- Performance advantage comes from:
  - Custom wire protocol implementation optimized for Morphium's mapping
  - Reduced object creation overhead
  - Optimized BSON serialization/deserialization
  - Connection pool efficiency

## Pre-Deployment Checklist

### 1. Configuration Review

**Connection Pool Sizing:**
```java
// Standard recommendation: 10-100 connections for most scenarios
cfg.connectionSettings().setMaxConnectionsPerHost(50);  // Sweet spot for most loads
cfg.connectionSettings().setMinConnectionsPerHost(10);  // Maintain baseline
```

**Performance Testing Validation:**
```java
// Verify your specific load requirements
MorphiumConfig cfg = new MorphiumConfig();

// Start conservative, monitor, and adjust
cfg.connectionSettings().setMaxConnectionsPerHost(25);
cfg.connectionSettings().setMinConnectionsPerHost(5);
cfg.connectionSettings().setMaxWaitTime(5000); // Fail fast to detect issues

// Monitor and adjust based on actual load:
// - CPU utilization < 70%
// - No threads waiting for connections
// - Response times within SLA
```

### 2. Security Configuration

**Authentication Setup:**
```java
cfg.authSettings().setMongoLogin(System.getenv("MONGO_USERNAME"));
cfg.authSettings().setMongoPassword(System.getenv("MONGO_PASSWORD"));

// For replica set operations (if needed)
cfg.authSettings().setMongoAdminUser(System.getenv("MONGO_ADMIN_USER"));
cfg.authSettings().setMongoAdminPwd(System.getenv("MONGO_ADMIN_PWD"));
```

**Network Security:**
```java
// Note: Wire protocol driver has limitations
// - No MongoDB Atlas support
// - No SSL/TLS connections
// Deploy in trusted network environments or use network-level encryption
```

### 3. Environment-Specific Configurations

#### Development Environment
```java
MorphiumConfig createDevConfig() {
    MorphiumConfig cfg = new MorphiumConfig();
    cfg.connectionSettings().setDatabase("myapp_dev");
    cfg.clusterSettings().addHostToSeed("dev-mongo", 27017);
    cfg.connectionSettings().setMaxConnectionsPerHost(10);  // Lower for dev
    cfg.cacheSettings().setGlobalCacheValidTime(30000);     // Shorter cache for dev
    cfg.setGlobalLogLevel(5); // Debug logging
    return cfg;
}
```

#### Staging Environment
```java
MorphiumConfig createStagingConfig() {
    MorphiumConfig cfg = new MorphiumConfig();
    cfg.connectionSettings().setDatabase("myapp_staging");
    cfg.clusterSettings().addHostToSeed("staging-mongo1", 27017);
    cfg.clusterSettings().addHostToSeed("staging-mongo2", 27017);
    cfg.connectionSettings().setMaxConnectionsPerHost(25);  // Moderate load
    cfg.connectionSettings().setRetriesOnNetworkError(5);
    cfg.authSettings().setMongoLogin(System.getenv("MONGO_USERNAME"));
    cfg.authSettings().setMongoPassword(System.getenv("MONGO_PASSWORD"));
    return cfg;
}
```

#### Production Environment
```java
MorphiumConfig createProductionConfig() {
    MorphiumConfig cfg = new MorphiumConfig();
    cfg.connectionSettings().setDatabase("myapp_prod");
    
    // Full replica set configuration
    cfg.clusterSettings().addHostToSeed("prod-mongo1", 27017);
    cfg.clusterSettings().addHostToSeed("prod-mongo2", 27017);
    cfg.clusterSettings().addHostToSeed("prod-mongo3", 27017);
    cfg.clusterSettings().setReplicaSetName("prod-replica-set");
    
    // Production-tuned connection pool
    cfg.connectionSettings().setMaxConnectionsPerHost(50);
    cfg.connectionSettings().setMinConnectionsPerHost(10);
    cfg.connectionSettings().setMaxWaitTime(3000); // Fail fast
    cfg.connectionSettings().setRetriesOnNetworkError(10);
    cfg.connectionSettings().setSleepBetweenErrorRetries(200);
    
    // Longer connection lifecycles for stability
    cfg.connectionSettings().setMaxConnectionIdleTime(300000);  // 5 minutes
    cfg.connectionSettings().setMaxConnectionLifetime(1800000); // 30 minutes
    
    // Production cache settings
    cfg.cacheSettings().setGlobalCacheValidTime(300000); // 5 minute default cache
    
    // Messaging optimization
    cfg.messagingSettings().setMessagingWindowSize(200);
    cfg.messagingSettings().setMessagingMultithreadded(true);
    cfg.messagingSettings().setUseChangeStream(true);
    
    // Security
    cfg.authSettings().setMongoLogin(System.getenv("MONGO_USERNAME"));
    cfg.authSettings().setMongoPassword(System.getenv("MONGO_PASSWORD"));
    
    // Production logging (reduce verbosity)
    cfg.setGlobalLogLevel(3); // WARN level
    cfg.setGlobalLogFile("/var/log/myapp/morphium.log");
    
    return cfg;
}
```

## Deployment Strategies

### 1. Container Deployment (Docker)

**Dockerfile:**
```dockerfile
FROM openjdk:21-jdk-slim

# Application setup
WORKDIR /app
COPY target/myapp.jar app.jar
COPY config/morphium-prod.properties morphium.properties

# JVM optimization for containers
ENV JAVA_OPTS="-Xms2g -Xmx4g -XX:+UseG1GC -XX:MaxGCPauseMillis=200"

# Health check endpoint
HEALTHCHECK --interval=30s --timeout=3s --start-period=60s --retries=3 \
  CMD curl -f http://localhost:8080/health || exit 1

EXPOSE 8080
CMD java $JAVA_OPTS -jar app.jar
```

**Docker Compose with MongoDB:**
```yaml
version: '3.8'
services:
  mongodb:
    image: mongo:7.0
    restart: always
    environment:
      MONGO_INITDB_ROOT_USERNAME: admin
      MONGO_INITDB_ROOT_PASSWORD: ${MONGO_ADMIN_PASSWORD}
    volumes:
      - mongodb_data:/data/db
    ports:
      - "27017:27017"
    
  app:
    build: .
    restart: always
    environment:
      MONGO_USERNAME: ${MONGO_USERNAME}
      MONGO_PASSWORD: ${MONGO_PASSWORD}
      JAVA_OPTS: "-Xms2g -Xmx4g -XX:+UseG1GC"
    ports:
      - "8080:8080"
    depends_on:
      - mongodb
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8080/health"]
      interval: 30s
      timeout: 10s
      retries: 3

volumes:
  mongodb_data:
```

### 2. Kubernetes Deployment

**ConfigMap for Configuration:**
```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: morphium-config
data:
  morphium.properties: |
    database=myapp_prod
    hosts=mongo-0.mongo,mongo-1.mongo,mongo-2.mongo
    replica_set_name=rs0
    max_connections_per_host=50
    min_connections_per_host=10
    max_wait_time=3000
    retries_on_network_error=10
    global_cache_valid_time=300000
```

**Deployment:**
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: myapp
spec:
  replicas: 3
  selector:
    matchLabels:
      app: myapp
  template:
    metadata:
      labels:
        app: myapp
    spec:
      containers:
      - name: myapp
        image: myapp:latest
        ports:
        - containerPort: 8080
        env:
        - name: MONGO_USERNAME
          valueFrom:
            secretKeyRef:
              name: mongo-secret
              key: username
        - name: MONGO_PASSWORD
          valueFrom:
            secretKeyRef:
              name: mongo-secret
              key: password
        - name: JAVA_OPTS
          value: "-Xms2g -Xmx4g -XX:+UseG1GC -XX:MaxGCPauseMillis=200"
        volumeMounts:
        - name: config
          mountPath: /app/config
        resources:
          limits:
            memory: "5Gi"
            cpu: "2000m"
          requests:
            memory: "2Gi"
            cpu: "1000m"
        livenessProbe:
          httpGet:
            path: /health
            port: 8080
          initialDelaySeconds: 60
          periodSeconds: 30
        readinessProbe:
          httpGet:
            path: /ready
            port: 8080
          initialDelaySeconds: 30
          periodSeconds: 10
      volumes:
      - name: config
        configMap:
          name: morphium-config
```

## Monitoring and Observability

### 1. Application Metrics

**Health Check Implementation:**
```java
@RestController
public class HealthController {
    
    @Autowired
    private Morphium morphium;
    
    @GetMapping("/health")
    public ResponseEntity<Map<String, Object>> health() {
        Map<String, Object> health = new HashMap<>();
        
        try {
            // Test database connectivity
            long start = System.currentTimeMillis();
            morphium.createQueryFor(User.class).limit(1).asList();
            long responseTime = System.currentTimeMillis() - start;
            
            // Get driver statistics
            Map<DriverStatsKey, Double> stats = morphium.getDriver().getDriverStats();
            
            health.put("status", "UP");
            health.put("database", Map.of(
                "status", "UP",
                "responseTime", responseTime + "ms"
            ));
            health.put("connectionPool", Map.of(
                "connectionsInUse", stats.get(DriverStatsKey.CONNECTIONS_IN_USE).intValue(),
                "connectionsInPool", stats.get(DriverStatsKey.CONNECTIONS_IN_POOL).intValue(),
                "threadsWaiting", stats.get(DriverStatsKey.THREADS_WAITING_FOR_CONNECTION).intValue(),
                "errors", stats.get(DriverStatsKey.ERRORS).intValue()
            ));
            
            return ResponseEntity.ok(health);
            
        } catch (Exception e) {
            health.put("status", "DOWN");
            health.put("error", e.getMessage());
            return ResponseEntity.status(503).body(health);
        }
    }
    
    @GetMapping("/metrics")
    public ResponseEntity<Map<String, Object>> metrics() {
        Map<DriverStatsKey, Double> stats = morphium.getDriver().getDriverStats();
        
        Map<String, Object> metrics = new HashMap<>();
        stats.forEach((key, value) -> 
            metrics.put(key.name().toLowerCase(), value));
        
        return ResponseEntity.ok(metrics);
    }
}
```

### 2. Logging Configuration

**Production Logging:**
```java
// Configure structured logging
cfg.setGlobalLogLevel(3); // WARN level for production
cfg.setGlobalLogFile("/var/log/myapp/morphium.log");
cfg.setGlobalLogSynced(false); // Async logging for performance

// Class-specific logging for troubleshooting
cfg.setLogLevelForClass(PooledDriver.class, 4); // INFO level for connection issues
cfg.setLogLevelForClass(StdMessaging.class, 4);  // INFO level for messaging issues
```

**Log Rotation Configuration (logrotate):**
```bash
/var/log/myapp/morphium.log {
    daily
    rotate 30
    compress
    delaycompress
    missingok
    create 0644 app app
    postrotate
        systemctl reload myapp || true
    endscript
}
```

### 3. Alerting Rules

**Connection Pool Alerts:**
```java
// Monitor connection pool utilization
Map<DriverStatsKey, Double> stats = morphium.getDriver().getDriverStats();
double utilization = stats.get(DriverStatsKey.CONNECTIONS_IN_USE) / 
                    stats.get(DriverStatsKey.CONNECTIONS_IN_POOL);

if (utilization > 0.8) {
    // Alert: High connection pool utilization
    alertManager.send("HIGH_CONNECTION_POOL_UTILIZATION", 
                     "Connection pool " + (utilization * 100) + "% full");
}

if (stats.get(DriverStatsKey.THREADS_WAITING_FOR_CONNECTION) > 0) {
    // Alert: Thread starvation
    alertManager.send("THREAD_STARVATION", 
                     "Threads waiting for connections: " + 
                     stats.get(DriverStatsKey.THREADS_WAITING_FOR_CONNECTION));
}

double errorRate = stats.get(DriverStatsKey.ERRORS) / 
                  stats.get(DriverStatsKey.CONNECTIONS_OPENED);
if (errorRate > 0.05) {
    // Alert: High error rate
    alertManager.send("HIGH_ERROR_RATE", 
                     "Connection error rate: " + (errorRate * 100) + "%");
}
```

## Backup and Recovery

### 1. Database Backup Strategy

**MongoDB Backup (using mongodump):**
```bash
#!/bin/bash
# backup-script.sh

DATE=$(date +%Y%m%d_%H%M%S)
BACKUP_DIR="/backups/mongodb"
DATABASE="myapp_prod"

# Create backup
mongodump --host rs0/mongo1:27017,mongo2:27017,mongo3:27017 \
          --db $DATABASE \
          --out $BACKUP_DIR/$DATE \
          --username $MONGO_USERNAME \
          --password $MONGO_PASSWORD

# Compress backup
tar -czf $BACKUP_DIR/backup_${DATABASE}_${DATE}.tar.gz -C $BACKUP_DIR $DATE
rm -rf $BACKUP_DIR/$DATE

# Cleanup old backups (keep 30 days)
find $BACKUP_DIR -name "backup_${DATABASE}_*.tar.gz" -mtime +30 -delete

echo "Backup completed: backup_${DATABASE}_${DATE}.tar.gz"
```

### 2. Message Queue Backup

**Since messaging uses MongoDB collections:**
```bash
#!/bin/bash
# Include message collections in backup

mongodump --host rs0/mongo1:27017,mongo2:27017,mongo3:27017 \
          --db myapp_prod \
          --collection msg_queue \
          --collection msg_queue_locks \
          --out /backups/messaging/$(date +%Y%m%d_%H%M%S)
```

### 3. Disaster Recovery Procedures

**Recovery Checklist:**
1. **Restore MongoDB data** from latest backup
2. **Verify replica set status** and elect primary
3. **Update application configuration** if needed
4. **Start application services**
5. **Verify connectivity** and basic operations
6. **Monitor for any issues**

**Recovery Script:**
```bash
#!/bin/bash
# disaster-recovery.sh

BACKUP_FILE=$1
DATABASE="myapp_prod"

if [ -z "$BACKUP_FILE" ]; then
    echo "Usage: $0 <backup_file.tar.gz>"
    exit 1
fi

# Extract backup
TEMP_DIR=$(mktemp -d)
tar -xzf $BACKUP_FILE -C $TEMP_DIR

# Stop application
kubectl scale deployment myapp --replicas=0

# Restore database
mongorestore --host rs0/mongo1:27017,mongo2:27017,mongo3:27017 \
             --db $DATABASE \
             --drop \
             --dir $TEMP_DIR/myapp_prod \
             --username $MONGO_USERNAME \
             --password $MONGO_PASSWORD

# Verify replica set status
mongo --host mongo1:27017 --eval "rs.status()"

# Restart application
kubectl scale deployment myapp --replicas=3

# Cleanup
rm -rf $TEMP_DIR

echo "Recovery completed. Monitor application logs for issues."
```

## Performance Monitoring

### 1. Key Metrics to Track

**Application Level:**
```java
// Connection pool metrics
- connections_in_use / connections_in_pool (target: < 0.8)
- threads_waiting_for_connection (target: 0)
- connection_errors (target: < 5% of total)

// Performance metrics
- average_response_time (target: < SLA requirements)
- throughput (ops/second)
- cache_hit_ratio (target: > 0.7 for cached entities)
```

**System Level:**
```bash
# Monitor these system metrics
- CPU utilization (target: < 70%)
- Memory usage (target: < 80%)
- Network I/O
- Disk I/O and space
```

### 2. Performance Testing in Production

**Canary Deployment Testing:**
```java
// Gradual rollout with performance monitoring
// Deploy to 5% of traffic, monitor metrics, then expand

@Component
public class PerformanceMonitor {
    
    private final MeterRegistry meterRegistry;
    
    @EventListener
    public void onQueryExecution(QueryExecutionEvent event) {
        Timer.Sample sample = Timer.start(meterRegistry);
        sample.stop(Timer.builder("morphium.query.duration")
                   .tag("collection", event.getCollection())
                   .register(meterRegistry));
    }
}
```

## Troubleshooting Common Production Issues

### Issue: Connection Pool Exhaustion
**Symptoms:** Timeouts, high response times
**Solution:**
```java
// Immediate: Increase pool size
cfg.connectionSettings().setMaxConnectionsPerHost(100);

// Long-term: Optimize query patterns and add caching
```

### Issue: Memory Leaks
**Symptoms:** Gradual memory increase, OOM errors
**Solution:**
```java
// Add connection lifecycle limits
cfg.connectionSettings().setMaxConnectionLifetime(600000); // 10 minutes
cfg.connectionSettings().setMaxConnectionIdleTime(180000); // 3 minutes

// Review cache sizes
@Cache(maxEntries = 5000, timeout = 300000) // Bounded cache
```

### Issue: Network Partitions
**Symptoms:** Intermittent connection failures
**Solution:**
```java
// Increase retry tolerance
cfg.connectionSettings().setRetriesOnNetworkError(15);
cfg.connectionSettings().setSleepBetweenErrorRetries(500);
cfg.clusterSettings().setServerSelectionTimeout(10000);
```

This comprehensive deployment guide covers the essential aspects of running Morphium in production, from initial configuration to ongoing monitoring and maintenance.