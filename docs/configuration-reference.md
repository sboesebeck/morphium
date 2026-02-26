# Configuration Reference

Complete reference for all Morphium configuration options.

## Overview

`MorphiumConfig` uses nested settings objects for different subsystems:

```java
MorphiumConfig cfg = new MorphiumConfig();

// Access nested settings
cfg.connectionSettings().setDatabase("myapp");
cfg.clusterSettings().addHostToSeed("localhost", 27017);
cfg.driverSettings().setDriverName("PooledDriver");
// ... other settings
```

## Connection Settings

Configure database connection parameters.

```java
cfg.connectionSettings()
```

| Method | Default | Description |
|--------|---------|-------------|
| `setDatabase(String)` | required | Database name to connect to |
| `setConnectionTimeout(int)` | 0 | Connection timeout in milliseconds |
| `setMaxConnectionIdleTime(int)` | 30000 | Max time connection can be idle (ms) |
| `setMaxConnectionLifetime(int)` | 600000 | Max connection lifetime (ms) |
| `setMinConnections(int)` | 1 | Minimum connections |
| `setMaxConnections(int)` | 250 | Maximum connections |
| `setMaxWaitTime(int)` | 2000 | Max wait time for connection from pool (ms) |
| `setHeartbeatFrequency(int)` | 1000 | Heartbeat frequency in milliseconds |
| `setRetriesOnNetworkError(int)` | 1 | Number of retries on network error |
| `setSleepBetweenNetworkErrorRetries(int)` | 1000 | Sleep between network error retries (ms) |
| `setUseSSL(boolean)` | false | Enable SSL/TLS connections |
| `setSslContext(SSLContext)` | null | Custom SSL context |
| `setSslInvalidHostNameAllowed(boolean)` | false | Allow invalid host names (self-signed certs) |

## Cluster Settings

Configure MongoDB cluster/replica set connection.

```java
cfg.clusterSettings()
```

| Method | Default | Description |
|--------|---------|-------------|
| `addHostToSeed(String, int)` | required | Add host:port to seed list |
| `setRequiredReplicaSetName(String)` | null | Replica set name (auto-detected if null) |
| `setHeartbeatFrequency(int)` | 0 | Heartbeat frequency in milliseconds |
| `setReplicaset(boolean)` | true | Enable replica set monitoring |
| `setReplicaSetMonitoringTimeout(int)` | 5000 | Monitoring timeout (ms) |

### Example
```java
// Single host
cfg.clusterSettings().addHostToSeed("localhost", 27017);

// Replica set
cfg.clusterSettings().addHostToSeed("mongo1", 27017);
cfg.clusterSettings().addHostToSeed("mongo2", 27017);
cfg.clusterSettings().addHostToSeed("mongo3", 27017);
cfg.clusterSettings().setRequiredReplicaSetName("myReplicaSet");
```

> ⚠️ **IPv6 Limitation**: The current hostname normalization assumes IPv4 addresses or DNS hostnames. IPv6 addresses (e.g., `[2001:db8::1]:27017`) are **not fully supported** and may cause connection pool issues. Use DNS hostnames instead of raw IPv6 addresses when configuring MongoDB connections.

## Driver Settings

Configure the MongoDB driver implementation.

```java
cfg.driverSettings()
```

| Method | Default | Description |
|--------|---------|-------------|
| `setDriverName(String)` | "PooledDriver" | Driver implementation to use |
| `setIdleSleepTime(int)` | 20 | Sleep time between idle checks (ms) |
| `setSharedConnectionPool(boolean)` | false | Share driver/connection pool across Morphium instances with same hosts+database |
| `setInMemorySharedDatabases(boolean)` | false | Share InMemoryDriver instance across Morphium instances with same database |
| `setServerSelectionTimeout(int)` | 30000 | Server selection timeout (ms) |
| `setHeartbeatFrequency(int)` | 1000 | Heartbeat frequency (ms) |
| `setRetryReads(boolean)` | false | Retry read operations |
| `setRetryWrites(boolean)` | false | Retry write operations |
| `setLocalThreshold(int)` | 15 | Local threshold for server selection (ms) |
| `setMaxConnectionIdleTime(int)` | 30000 | Max connection idle time (ms) |
| `setMaxConnectionLifeTime(int)` | 600000 | Max connection lifetime (ms) |
| `setCursorBatchSize(int)` | 1000 | Default batch size for cursors |

### Available Drivers
- **`PooledDriver`** (default): Connection pooling with replica set support
- **`SingleMongoConnectDriver`**: Single connection driver
- **`InMemDriver`**: In-memory driver for testing

## Authentication Settings

Configure MongoDB authentication.

```java
cfg.authSettings()
```

| Method | Default | Description |
|--------|---------|-------------|
| `setMongoLogin(String)` | null | MongoDB username |
| `setMongoPassword(String)` | null | MongoDB password |
| `setMongoAdminUser(String)` | null | Admin user for replica set operations |
| `setMongoAdminPwd(String)` | null | Admin password for replica set operations |
| `setAuthMechanism(String)` | null | Auth mechanism: `"MONGODB-X509"` for certificate-based auth (requires SSL) |

### Example — password authentication
```java
cfg.authSettings().setMongoLogin("appuser");
cfg.authSettings().setMongoPassword("secret123");

// Admin user for replica set status (optional)
cfg.authSettings().setMongoAdminUser("admin");
cfg.authSettings().setMongoAdminPwd("adminsecret");
```

### Example — MONGODB-X509 (certificate-based, no password)
```java
// SSL + mTLS must be configured (see docs/ssl-tls.md)
cfg.authSettings().setAuthMechanism("MONGODB-X509");
// no setMongoLogin / setMongoPassword needed
```

## Cache Settings

Configure the global caching behavior.

```java
cfg.cacheSettings()
```

| Method | Default | Description |
|--------|---------|-------------|
| `setGlobalCacheValidTime(int)` | 5000 | Global cache TTL in milliseconds |
| `setHousekeepingTimeout(int)` | 5000 | Cache housekeeping interval (ms) |

## Messaging Settings

Configure the messaging system.

```java
cfg.messagingSettings()
```

| Method | Default | Description |
|--------|---------|-------------|
| `setMessageQueueName(String)` | "msg" | Base name for message collections |
| `setMessagingWindowSize(int)` | 100 | Number of messages to process per batch |
| `setMessagingMultithreadded(boolean)` | true | Enable multithreaded message processing |
| `setUseChangeStream(boolean)` | true | Use MongoDB Change Streams for messaging |
| `setMessagingPollPause(int)` | 250 | Pause between message polls (ms) |
| `setProcessMultiple(boolean)` | true | Process multiple messages if available |
| `setAutoAnswer(boolean)` | false | Automatically answer messages |
| `setMessagingImplementation(String)` | "StandardMessaging" | Implementation class alias |
| `setThreadPoolMessagingCoreSize(int)` | 0 | Thread pool core size |
| `setThreadPoolMessagingMaxSize(int)` | 100 | Thread pool max size |
| `setThreadPoolMessagingKeepAliveTime(long)` | 2000 | Thread keep-alive time (ms) |

## Thread Pool Settings

Configure async operation thread pools.

```java
cfg.threadPoolSettings()
```

| Method | Default | Description |
|--------|---------|-------------|
| `setThreadPoolAsyncOpCoreSize(int)` | 1 | Core thread pool size |
| `setThreadPoolAsyncOpMaxSize(int)` | 1000 | Maximum thread pool size |
| `setThreadPoolAsyncOpKeepAliveTime(long)` | 1000 | Thread keep-alive time (ms) |

## Writer Settings

Configure write operation behavior.

```java
cfg.writerSettings()
```

| Method | Default | Description |
|--------|---------|-------------|
| `setWriteBufferTime(int)` | 1000 | Buffer flush timeout (ms) |
| `setWriteBufferTimeGranularity(int)` | 100 | Time granularity for buffer checks (ms) |
| `setMaximumRetriesWriter(int)` | 10 | Max retries for direct writer |
| `setMaximumRetriesBufferedWriter(int)` | 10 | Max retries for buffered writer |
| `setMaximumRetriesAsyncWriter(int)` | 10 | Max retries for async writer |
| `setRetryWaitTimeWriter(int)` | 200 | Wait between writer retries (ms) |
| `setRetryWaitTimeBufferedWriter(int)` | 200 | Wait between buffered writer retries (ms) |
| `setRetryWaitTimeAsyncWriter(int)` | 200 | Wait between async writer retries (ms) |
| `setThreadConnectionMultiplier(int)` | 5 | Multiplier for connections per thread |

## Object Mapping Settings

Configure object mapping behavior.

```java
cfg.objectMappingSettings()
```

| Method | Default | Description |
|--------|---------|-------------|
| `setCheckForNew(boolean)` | true | Check if entity is new before store |
| `setAutoValues(boolean)` | true | Enable automatic value generation (@LastChange, etc.) |
| `setObjectSerializationEnabled(boolean)` | true | Enable object serialization |
| `setCamelCaseConversionEnabled(boolean)` | true | Convert camelCase to snake_case in MongoDB |
| `setWarnOnNoEntitySerialization(boolean)` | false | Warn if no entity serialization is available |

## Error Handling Settings

Configure retry and error handling behavior.

```java
cfg.connectionSettings()
```

| Method | Default | Description |
|--------|---------|-------------|
| `setRetriesOnNetworkError(int)` | 5 | Number of retries on network errors |
| `setSleepBetweenErrorRetries(int)` | 100 | Sleep between error retries (ms) |

## Configuration Sources

Morphium supports multiple configuration sources:

### 1. Programmatic Configuration
```java
MorphiumConfig cfg = new MorphiumConfig();
cfg.connectionSettings().setDatabase("myapp");
// ... other settings
```

### 2. Properties File
```java
Properties props = new Properties();
props.load(new FileInputStream("morphium.properties"));
MorphiumConfig cfg = new MorphiumConfig(props);
```

Example `morphium.properties`:
```properties
database=myapp
hosts=mongo1:27017,mongo2:27017,mongo3:27017
replica_set_name=myReplicaSet
driver_name=PooledDriver
max_connections_per_host=50
mongo_login=appuser
mongo_password=secret123
```

### 3. JSON Configuration
```java
String json = Files.readString(Paths.get("morphium-config.json"));
MorphiumConfig cfg = MorphiumConfig.createFromJson(json);
```

Example JSON:
```json
{
  "database": "myapp",
  "hosts": ["mongo1:27017", "mongo2:27017", "mongo3:27017"],
  "replicaSetName": "myReplicaSet",
  "driverName": "PooledDriver",
  "maxConnectionsPerHost": 50,
  "mongoLogin": "appuser",
  "mongoPassword": "secret123"
}
```

## Environment-Specific Configurations

### Development
```java
MorphiumConfig cfg = new MorphiumConfig();
cfg.connectionSettings().setDatabase("myapp_dev");
cfg.clusterSettings().addHostToSeed("localhost", 27017);
cfg.driverSettings().setDriverName("PooledDriver");
cfg.connectionSettings().setMaxConnectionsPerHost(10); // Lower for dev
```

### Testing
```java
MorphiumConfig cfg = new MorphiumConfig();
cfg.connectionSettings().setDatabase("myapp_test");
cfg.driverSettings().setDriverName("InMemDriver"); // In-memory for tests
```

### Production
```java
MorphiumConfig cfg = new MorphiumConfig();
cfg.connectionSettings().setDatabase("myapp_prod");
cfg.clusterSettings().addHostToSeed("mongo1", 27017);
cfg.clusterSettings().addHostToSeed("mongo2", 27017);
cfg.clusterSettings().addHostToSeed("mongo3", 27017);
cfg.connectionSettings().setMaxConnectionsPerHost(100);
cfg.connectionSettings().setMaxWaitTime(10000);
cfg.authSettings().setMongoLogin("produser");
cfg.authSettings().setMongoPassword(System.getenv("MONGO_PASSWORD"));
```

## Common Configuration Patterns

### High Throughput
```java
cfg.connectionSettings().setMaxConnectionsPerHost(200);
cfg.connectionSettings().setMaxWaitTime(5000);
cfg.messagingSettings().setMessagingWindowSize(500);
cfg.messagingSettings().setMessagingMultithreadded(true);
```

### Low Latency
```java
cfg.connectionSettings().setMinConnectionsPerHost(20);
cfg.connectionSettings().setConnectionTimeout(500);
cfg.connectionSettings().setMaxWaitTime(1000);
cfg.clusterSettings().setHeartbeatFrequency(1000);
```

### High Availability
```java
cfg.connectionSettings().setRetriesOnNetworkError(10);
cfg.connectionSettings().setSleepBetweenErrorRetries(200);
cfg.clusterSettings().setServerSelectionTimeout(5000);
cfg.connectionSettings().setMaxConnectionIdleTime(300000);
```