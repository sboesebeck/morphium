# Architecture Overview

Understanding Morphium's internal architecture and component relationships.

## High-Level Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Application   │    │    Messaging    │    │      Cache      │
│                 │    │    System       │    │   Management    │
└─────────┬───────┘    └─────────┬───────┘    └─────────┬───────┘
          │                      │                      │
          └──────────────────────┼──────────────────────┘
                                 │
              ┌─────────────────────────────────────┐
              │           Morphium Core             │
              │  ┌─────────────┐ ┌─────────────┐    │
              │  │   Query     │ │   Object    │    │
              │  │   System    │ │   Mapper    │    │
              │  └─────────────┘ └─────────────┘    │
              └─────────────────┬───────────────────┘
                                │
              ┌─────────────────────────────────────┐
              │         Driver Layer               │
              │  ┌─────────────┐ ┌─────────────┐    │
              │  │  Pooled     │ │  Single     │    │
              │  │  Driver     │ │  Driver     │    │
              │  └─────────────┘ └─────────────┘    │
              └─────────────────┬───────────────────┘
                                │
              ┌─────────────────────────────────────┐
              │         Wire Protocol              │
              │    MongoDB Communication Layer      │
              └─────────────────┬───────────────────┘
                                │
              ┌─────────────────────────────────────┐
              │            MongoDB                 │
              │      (Replica Set / Cluster)       │
              └─────────────────────────────────────┘
```

## Core Components

### 1. Morphium Core (`Morphium.java`)

**Central facade** providing all database operations and configuration.

**Key Responsibilities:**
- Configuration management
- Driver lifecycle
- Transaction coordination
- Entity lifecycle management

**Main APIs:**
```java
// CRUD operations
morphium.store(entity);
morphium.delete(entity);
Query<T> query = morphium.createQueryFor(Class.class);

// Transaction support
morphium.beginTransaction();
morphium.commitTransaction();
morphium.abortTransaction();

// Configuration access
MorphiumConfig config = morphium.getConfig();
```

### 2. Driver Architecture

Three driver implementations for different use cases:

#### PooledDriver (Default - Production)
- **Connection pooling** with configurable min/max connections per host
- **Replica set support** with automatic failover
- **Health monitoring** with heartbeat checks
- **Load balancing** across replica set members

```java
// Internal architecture
PooledDriver {
    Map<String, BlockingQueue<Connection>> connectionPool;
    Map<Integer, Connection> borrowedConnections;
    Map<String, AtomicInteger> waitCounter;
    ScheduledExecutorService heartbeat;
}
```

#### SingleMongoConnectDriver
- **Single connection** per operation
- **Simpler implementation** for low-concurrency scenarios
- **No connection pooling overhead**

#### InMemoryDriver (Testing)
- **Full in-memory MongoDB emulation**
- **No network communication**
- **Supports most MongoDB operations** including aggregation
- **Perfect for unit testing**

### 3. Object Mapping System

#### ObjectMapperImpl
**Bidirectional mapping** between Java objects and BSON documents.

**Features:**
- Annotation-driven mapping (`@Entity`, `@Embedded`, `@Property`)
- Type conversion for Java → BSON → Java
- Reference resolution (lazy and eager)
- Polymorphism support with class name storage

**Mapping Flow:**
```
Java Object → ObjectMapper → BSON Document → MongoDB
MongoDB → BSON Document → ObjectMapper → Java Object
```

#### Type Mappers
Specialized mappers for different Java types:
- `BigDecimalTypeMapper`
- `BigIntegerTypeMapper`
- `DateTypeMapper`
- `EnumTypeMapper`
- Collections and arrays

### 4. Query System

#### Query Interface
**Fluent API** for building MongoDB queries:

```java
Query<User> q = morphium.createQueryFor(User.class)
    .f("status").eq("active")
    .f("age").gte(18)
    .sort("-created")
    .limit(100);
```

#### MorphiumIterator
**Paged iteration** for large result sets:
- Configurable window size
- Prefetching support
- Thread-safe access
- Memory-efficient processing

#### Aggregation Framework
**MongoDB aggregation pipeline** support:
```java
Aggregator<Order, OrderStats> agg = morphium.createAggregator(Order.class, OrderStats.class)
    .match(morphium.createQueryFor(Order.class).f("status").eq("completed"))
    .group("customerId").sum("total", "$amount").avg("avgAmount", "$amount").end()
    .sort("total");
```

### 5. Caching System

#### Multi-Level Caching
1. **Query Result Cache** - Caches query results
2. **ID Cache** - Caches individual entities by ID  
3. **Write Cache** - Buffers writes for performance

#### Cache Synchronization
**Cluster-aware caching** with synchronization:
- **WatchingCacheSynchronizer** - Uses MongoDB Change Streams
- **MessagingCacheSynchronizer** - Uses Morphium messaging
- **Manual cache control** for custom strategies

#### Cache Strategies
```java
@Cache(
    timeout = 60000,           // 60-second TTL
    maxEntries = 10000,        // LRU eviction limit
    strategy = Cache.ClearStrategy.LRU,
    syncCache = Cache.SyncCacheStrategy.CLEAR_TYPE_CACHE
)
```

### 6. Messaging System

#### MongoDB-Based Message Queue
**Unique approach**: Uses MongoDB collections as message queues.

**Key Features:**
- **Topic-based routing**
- **Exclusive vs broadcast** delivery
- **Request/response patterns**
- **Message persistence** and durability
- **Change Stream integration** for real-time processing

#### Message Processing Flow
```
Sender → MongoDB Collection → Change Stream/Polling → Listener
                 ↓
          Message Persistence
```

#### Implementation Variants
- **StdMessaging** - Standard implementation
- **AdvancedSplitCollectionMessaging** - Scalable variant with collection splitting

### 7. Writer System

#### Three Writer Types

1. **Synchronous Writer** (`MorphiumWriter`)
   - Immediate writes
   - Strong consistency
   - Simple error handling

2. **Asynchronous Writer** (`AsyncWriterImpl`)
   - Non-blocking writes
   - Callback-based completion
   - Higher throughput

3. **Buffered Writer** (`BufferedMorphiumWriterImpl`)
   - Write batching for performance
   - Configurable buffer size and timeout
   - Risk of data loss on system failure

```java
@Entity
@WriteBuffer(size = 1000, timeout = 5000, 
             strategy = WriteBuffer.STRATEGY.WRITE_OLD)
public class LogEntry {
    // High-frequency writes use buffering
}
```

## Threading Model

### Virtual Threads (JDK 21+)
Morphium leverages **virtual threads** for better concurrency:

```java
// Connection pool uses virtual thread factory
ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(5,
    Thread.ofVirtual().name("MCon-", 0).factory());

// Individual operations use virtual threads
Thread.ofVirtual().name("HeartbeatCheck-" + host).start(() -> {
    // Heartbeat logic
});
```

### Concurrency Patterns

1. **Connection Pool Management**
   - Lock-free connection borrowing/returning
   - Atomic counters for statistics
   - ConcurrentHashMap for connection storage

2. **Cache Management**
   - Lock-free cache access
   - Atomic cache statistics
   - Background cache cleanup

3. **Message Processing**
   - Configurable single/multi-threaded processing
   - Thread-safe message listeners
   - Concurrent message polling

## Configuration Architecture

### Modular Configuration System
```java
MorphiumConfig {
    ConnectionSettings connectionSettings;
    ClusterSettings clusterSettings;
    DriverSettings driverSettings;
    MessagingSettings messagingSettings;
    CacheSettings cacheSettings;
    ThreadPoolSettings threadPoolSettings;
    WriterSettings writerSettings;
    // ... other settings
}
```

### Configuration Sources
1. **Programmatic** - Direct Java configuration
2. **Properties Files** - Standard Java properties
3. **JSON** - Structured JSON configuration
4. **Environment Variables** - System environment

## Extension Points

### Plugin Architecture
Morphium provides multiple extension points:

1. **Custom Drivers** - Implement `MorphiumDriver` interface
2. **Name Providers** - Custom collection naming strategies
3. **Object Mappers** - Custom serialization logic
4. **Cache Implementations** - JCache integration support
5. **Encryption Providers** - Field-level encryption
6. **Lifecycle Listeners** - Storage event hooks

### Example Extension
```java
// Custom name provider for time-based collections
public class TimeBasedNameProvider implements NameProvider {
    @Override
    public String getCollectionName(Class<?> type, ObjectMapper om, 
                                   boolean translateCamelCase, boolean useFQN, 
                                   String specifiedName, Morphium morphium) {
        SimpleDateFormat df = new SimpleDateFormat("yyyyMM");
        String date = df.format(new Date());
        return specifiedName + "_" + date;
    }
}

@Entity(nameProvider = TimeBasedNameProvider.class)
public class LogEntry {
    // Will be stored in collections like "log_entry_202412"
}
```

## Performance Architecture

### Key Performance Features

1. **Connection Pooling**
   - Reduces connection overhead
   - Configurable pool sizes per host
   - Connection health monitoring

2. **Query Optimization**
   - Index-aware query building
   - Result caching at multiple levels
   - Projection support to limit data transfer

3. **Batching Support**
   - Bulk write operations
   - Message batching
   - Aggregation pipeline optimization

4. **Async Operations**
   - Non-blocking I/O where possible
   - Callback-based async API
   - Virtual thread utilization

### Memory Management
- **Bounded caches** with LRU eviction
- **Connection lifecycle** management
- **Large result set** streaming via iterators
- **Reference counting** for shared resources

## Design Principles

### 1. **Annotation-Driven Configuration**
Minimal XML/configuration files, maximum annotation-based setup.

### 2. **Fail-Fast Philosophy**
Early detection of configuration and runtime errors.

### 3. **Extensibility**
Plugin architecture for customization without core changes.

### 4. **Performance First**
Design decisions prioritize performance and scalability.

### 5. **MongoDB-Native**
Deep integration with MongoDB features and concepts.

### 6. **Type Safety**
Compile-time checking where possible, runtime validation where necessary.

This architecture provides **high performance**, **scalability**, and **flexibility** while maintaining **simplicity** for common use cases and **extensibility** for advanced scenarios.