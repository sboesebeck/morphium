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
| `setConnectionTimeout(int)` | 1000 | Connection timeout in milliseconds |
| `setSocketTimeout(int)` | 0 | Socket timeout in milliseconds (0 = no timeout) |
| `setMaxConnectionIdleTime(int)` | 100000 | Max time connection can be idle (ms) |
| `setMaxConnectionLifetime(int)` | 600000 | Max connection lifetime (ms) |
| `setMinConnectionsPerHost(int)` | 1 | Minimum connections per host |
| `setMaxConnectionsPerHost(int)` | 100 | Maximum connections per host |
| `setMaxWaitTime(int)` | 30000 | Max wait time for connection from pool (ms) |

## Cluster Settings

Configure MongoDB cluster/replica set connection.

```java
cfg.clusterSettings()
```

| Method | Default | Description |
|--------|---------|-------------|
| `addHostToSeed(String, int)` | required | Add host:port to seed list |
| `setReplicaSetName(String)` | null | Replica set name (auto-detected if null) |
| `setHeartbeatFrequency(int)` | 2000 | Heartbeat frequency in milliseconds |
| `setServerSelectionTimeout(int)` | 2000 | Server selection timeout (ms) |

### Example
```java
// Single host
cfg.clusterSettings().addHostToSeed("localhost", 27017);

// Replica set
cfg.clusterSettings().addHostToSeed("mongo1", 27017);
cfg.clusterSettings().addHostToSeed("mongo2", 27017);
cfg.clusterSettings().addHostToSeed("mongo3", 27017);
cfg.clusterSettings().setReplicaSetName("myReplicaSet");
```

## Driver Settings

Configure the MongoDB driver implementation.

```java
cfg.driverSettings()
```

| Method | Default | Description |
|--------|---------|-------------|
| `setDriverName(String)` | "PooledDriver" | Driver implementation to use |
| `setIdleSleepTime(int)` | 5 | Sleep time between idle checks (ms) |
| `setSharedConnectionPool(boolean)` | false | Share driver/connection pool across Morphium instances with same hosts+database |
| `setInMemorySharedDatabases(boolean)` | false | Share InMemoryDriver instance across Morphium instances with same database |

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

### Example
```java
// Basic authentication
cfg.authSettings().setMongoLogin("appuser");
cfg.authSettings().setMongoPassword("secret123");

// Admin user for replica set status (optional)
cfg.authSettings().setMongoAdminUser("admin");
cfg.authSettings().setMongoAdminPwd("adminsecret");
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
| `setPollPauseTime(int)` | 500 | Pause between message polls (ms) |

## Thread Pool Settings

Configure async operation thread pools.

```java
cfg.threadPoolSettings()
```

| Method | Default | Description |
|--------|---------|-------------|
| `setCorePoolSize(int)` | 0 | Core thread pool size |
| `setMaxPoolSize(int)` | Integer.MAX_VALUE | Maximum thread pool size |
| `setKeepAliveTime(int)` | 60000 | Thread keep-alive time (ms) |

## Writer Settings

Configure write operation behavior.

```java
cfg.writerSettings()
```

| Method | Default | Description |
|--------|---------|-------------|
| `setBufferedWriterBufferSize(int)` | 1000 | Buffer size for buffered writes |
| `setBufferedWriterTimeout(int)` | 5000 | Buffer flush timeout (ms) |

## Object Mapping Settings

Configure object mapping behavior.

```java
cfg.objectMappingSettings()
```

| Method | Default | Description |
|--------|---------|-------------|
| `setTreatCollectionAsUncapped(boolean)` | false | Treat all collections as uncapped |
| `setObjectMapper(String)` | "ObjectMapperImpl" | Object mapper implementation |

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