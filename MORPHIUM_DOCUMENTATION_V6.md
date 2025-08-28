# Morphium Documentation v6.0

## Table of Contents
1. [Introduction](#introduction)
2. [Getting Started](#getting-started)
3. [Core Features](#core-features)
4. [Morphium Message Queueing](#morphium-message-queueing)
5. [Object Mapping & Querying](#object-mapping--querying)
6. [Caching System](#caching-system)
7. [How-To Guide](#how-to-guide)
8. [Implementation Details & Developer Notes](#implementation-details--developer-notes)
9. [Migration Guide](#migration-guide)

## Introduction

**Morphium** is a sophisticated MongoDB Object Document Mapper (ODM) and messaging system for Java 21+. Originally designed as a high-performance, feature-rich access layer for MongoDB, Morphium has evolved into a comprehensive database toolkit with unique messaging capabilities that set it apart from traditional solutions.

### What Makes Morphium Special

- **Integrated MongoDB-based Message Queue**: A unique, database-native messaging solution
- **Zero-Dependency Architecture**: Uses its own MongoDB wire protocol driver (since v5.0)
- **Cluster-Aware Caching**: Built-in distributed caching with automatic synchronization
- **Annotation-Driven Development**: Rich annotation system for rapid development
- **Production-Ready**: Used by enterprise applications like [Genios.de](https://www.genios.de)

### Version 6.0 Highlights

- **JDK 21 Requirement**: Modern Java features and performance improvements
- **Messaging System Rewrite**: Enhanced performance and reliability
- **Improved Driver Architecture**: Streamlined MongoDB connectivity
- **Enhanced Thread Safety**: Better concurrency support throughout

## Getting Started

### Prerequisites
- **JDK 21** or higher
- **MongoDB 5.0** or higher
- **Maven 3.6+** for dependency management

### Maven Dependency

```xml
<dependency>
    <groupId>de.caluga</groupId>
    <artifactId>morphium</artifactId>
    <version>6.0.0</version>
</dependency>
<dependency>
    <groupId>org.mongodb</groupId>
    <artifactId>bson</artifactId>
    <version>4.7.1</version>
</dependency>
```

### Basic Configuration

```java
// Simple configuration
MorphiumConfig cfg = new MorphiumConfig();
cfg.setDatabase("myapp");
cfg.addHostToSeed("localhost", 27017);

Morphium morphium = new Morphium(cfg);
```

```java
// Production configuration
MorphiumConfig cfg = new MorphiumConfig();
cfg.setDatabase("production_db");
cfg.addHostToSeed("mongo1.example.com", 27017);
cfg.addHostToSeed("mongo2.example.com", 27017);
cfg.addHostToSeed("mongo3.example.com", 27017);

// Enable authentication
cfg.setMongoLogin("app_user");
cfg.setMongoPassword("secure_password");

// Configure connection pool
cfg.setMaxConnections(20);
cfg.setMinConnections(5);

Morphium morphium = new Morphium(cfg);
```

### Your First Entity

```java
@Entity
@Cache(timeout = 30000) // Cache for 30 seconds
public class User {
    @Id
    private MorphiumId id;
    
    @Index // Create index on username
    private String username;
    
    private String email;
    
    @CreationTime
    private long created;
    
    @LastChange
    private long lastModified;
    
    // Getters and setters...
}
```

### Basic Operations

```java
// Create
User user = new User();
user.setUsername("alice");
user.setEmail("alice@example.com");
morphium.store(user);

// Read
User found = morphium.createQueryFor(User.class)
    .f("username").eq("alice")
    .get();

// Update
found.setEmail("alice.new@example.com");
morphium.store(found);

// Delete
morphium.delete(found);
```

## Core Features

### 1. Object Mapping
- **POJO to Document**: Seamless mapping between Java objects and MongoDB documents
- **Type Safety**: Strong typing with compile-time checks
- **Inheritance Support**: Polymorphic entities with automatic type handling
- **Custom Mappers**: Extensible type mapping system

### 2. Query System
- **Fluent Interface**: Intuitive query building
- **MongoDB Aggregation**: Full aggregation framework support
- **Geospatial Queries**: Built-in geo-spatial query support
- **Text Search**: MongoDB text search integration

### 3. Caching
- **Multi-Level Cache**: In-memory and distributed caching
- **Cluster Synchronization**: Automatic cache sync across nodes
- **JCache Integration**: Standard Java caching API support
- **Configurable Strategies**: LRU, FIFO, and custom eviction policies

### 4. Performance Optimization
- **Connection Pooling**: Efficient connection management
- **Batch Operations**: Bulk insert/update support
- **Lazy Loading**: On-demand object loading
- **Write Buffering**: Asynchronous write operations

### 5. Pluggable Architecture
- **Custom Cache Implementations**: Implement `MorphiumCache` interface for Redis, Hazelcast, or any caching solution
- **Custom Writers**: Implement `MorphiumWriter` interface for specialized write operations, encryption, or audit trails
- **Custom Messaging Systems**: Implement `MorphiumMessaging` interface to replace MongoDB-based messaging with RabbitMQ, Kafka, or proprietary solutions
- **Custom Object Mappers**: Implement `MorphiumTypeMapper` interface for specialized type conversions (e.g., GIS data, custom formats, legacy data structures)
- **Custom Name Providers**: Implement `NameProvider` interface for dynamic collection naming (e.g., daily/monthly collections for statistics, tenant-based collections, load balancing)
- **Custom Drivers**: Extend driver support for different databases or specialized MongoDB configurations
- **Storage Listeners**: Hook into all CRUD operations for auditing, validation, or cross-cutting concerns

## Morphium Message Queueing

### Why Choose Morphium Messaging?

Morphium's messaging system offers unique advantages over traditional message brokers:

**🔍 Database-Native Transparency**
- Messages are stored as MongoDB documents - fully inspectable and searchable
- Use standard MongoDB tools (`mongosh`, MongoDB Compass) to monitor queues
- Complex queries and aggregations on message data
- Easy debugging and troubleshooting

**🛠️ Operational Simplicity**
- No additional infrastructure required - uses existing MongoDB
- No separate message broker to maintain, monitor, or scale
- Leverages MongoDB's reliability, replication, and backup features
- Unified data store for both application data and messages

**⚡ MongoDB Change Streams Integration**
- Push-based message delivery (MongoDB 3.6+)
- Near real-time message processing
- Efficient resource utilization compared to polling

**🎯 Advanced Message Processing**
- Exclusive messages (broadcast to all, processed by one)
- Group-based message targeting
- Message priorities and scheduling
- Automatic retry mechanisms with configurable strategies

**🔒 Enterprise Features**
- Transaction support for message operations
- Message persistence and durability guarantees
- Built-in dead letter queue functionality
- Comprehensive monitoring and metrics

### Comparison with Traditional Solutions

| Feature | Morphium Messaging | RabbitMQ | Apache Kafka | ActiveMQ |
|---------|-------------------|----------|--------------|----------|
| **Infrastructure** | Uses existing MongoDB | Separate broker cluster | Separate cluster | Separate broker |
| **Message Inspection** | Native MongoDB queries | Management UI/CLI | CLI tools | Management console |
| **Persistence** | MongoDB durability | Configurable | Log-based | Configurable |
| **Scaling** | MongoDB scaling | Cluster scaling | Partition scaling | Broker scaling |
| **Query Messages** | Full MongoDB query power | Limited | Limited | Limited |
| **Transaction Support** | MongoDB transactions | AMQP transactions | Limited | JTA support |
| **Setup Complexity** | Minimal (if MongoDB exists) | Medium | High | Medium |

### Basic Messaging Usage

#### Simple Message Producer/Consumer

```java
// Initialize messaging
Morphium morphium = new Morphium(config);
StdMessaging messaging = new StdMessaging();
messaging.init(morphium);
messaging.start();

// Consumer - listen to all messages
messaging.addMessageListener((msg, message) -> {
    log.info("Received: {} - {}", message.getTopic(), message.getMsg());
    return null; // No response
});

// Producer - send message
Msg message = new Msg("user.created", "User alice created", "user_id:123");
messaging.sendMessage(message);
```

#### Topic-Based Messaging

```java
// Topic-specific consumer
messaging.addListenerForTopic("payment.processed", (msg, message) -> {
    PaymentInfo payment = message.getMapValue().get("payment");
    processPayment(payment);
    return null;
});

// Send to specific topic
Msg paymentMsg = new Msg("payment.processed", "Payment completed");
paymentMsg.getMapValue().put("payment", paymentInfo);
paymentMsg.getMapValue().put("amount", 99.99);
messaging.sendMessage(paymentMsg);
```

#### Request-Response Pattern

```java
// Consumer that responds
messaging.addListenerForTopic("user.lookup", (msg, message) -> {
    String userId = message.getValue();
    User user = findUser(userId);
    
    Msg response = new Msg();
    response.setMapValue(Map.of("user", user, "found", user != null));
    return response;
});

// Request with response waiting
Msg request = new Msg("user.lookup", "Find user", "user123");
Msg response = messaging.sendAndAwaitFirstAnswer(request, 5000); // 5 second timeout

if (response != null) {
    User user = (User) response.getMapValue().get("user");
}
```

### Advanced Messaging Features

#### Exclusive Messages (Broadcast Processing)

```java
// Create exclusive message (processed by all listeners, but only one processes)
Msg broadcastMsg = new Msg("cache.invalidate", "Clear user cache");
broadcastMsg.setExclusive(true);
broadcastMsg.setProcessedBy(List.of("ALL")); // Special marker
messaging.sendMessage(broadcastMsg);

// Multiple listeners can receive, but only one processes
messaging.addListenerForTopic("cache.invalidate", (msg, message) -> {
    cacheService.invalidateUserCache();
    return null;
});
```

#### Message Priorities and Scheduling

```java
// High priority message
Msg urgentMsg = new Msg("system.alert", "Critical system error");
urgentMsg.setPriority(10); // Higher number = higher priority
messaging.sendMessage(urgentMsg);

// Scheduled message
Msg scheduledMsg = new Msg("reminder.send", "Daily report reminder");
scheduledMsg.setTimestamp(System.currentTimeMillis() + 3600000); // 1 hour from now
messaging.sendMessage(scheduledMsg);
```

#### Custom Message Types

```java
@Entity
@Messaging(name = "OrderMessage")
public class OrderMessage extends Msg {
    private String orderId;
    private String customerId;
    private BigDecimal amount;
    private OrderStatus status;
    
    // Constructors, getters, setters...
}

// Usage
OrderMessage orderMsg = new OrderMessage();
orderMsg.setOrderId("ORD-123");
orderMsg.setCustomerId("CUST-456");
orderMsg.setAmount(new BigDecimal("99.99"));
orderMsg.setTopic("order.created");
messaging.sendMessage(orderMsg);
```

#### Message Persistence and Durability

```java
// Configure messaging for high durability
MessagingSettings settings = new MessagingSettings();
settings.setUseChangeStream(true);  // Use change streams for push delivery
settings.setMultithreaded(true);    // Enable concurrent processing
settings.setWindowSize(50);         // Process 50 messages per batch
settings.setPause(100);             // 100ms pause between polls

StdMessaging messaging = new StdMessaging();
messaging.init(morphium, settings);
```

### Enterprise Messaging Patterns

#### Dead Letter Queue

```java
messaging.addListenerForTopic("order.process", (msg, message) -> {
    try {
        processOrder(message);
        return null;
    } catch (Exception e) {
        // Increment retry count
        int retries = message.getMapValue().getOrDefault("retries", 0);
        if (retries >= 3) {
            // Move to dead letter queue
            Msg dlqMessage = new Msg("dlq.order.process", message.getMsg());
            dlqMessage.setMapValue(message.getMapValue());
            dlqMessage.getMapValue().put("original_topic", "order.process");
            dlqMessage.getMapValue().put("error", e.getMessage());
            messaging.sendMessage(dlqMessage);
            return null;
        } else {
            // Retry
            message.getMapValue().put("retries", retries + 1);
            message.setTimestamp(System.currentTimeMillis() + 5000); // Retry in 5 seconds
            messaging.sendMessage(message);
            return null;
        }
    }
});
```

#### Circuit Breaker Pattern

```java
public class CircuitBreakerMessageHandler {
    private CircuitBreaker circuitBreaker = new CircuitBreaker();
    
    public Msg handleMessage(MorphiumMessaging messaging, Msg message) {
        if (circuitBreaker.isOpen()) {
            // Circuit is open, queue message for later
            message.setTimestamp(System.currentTimeMillis() + 60000); // Retry in 1 minute
            messaging.sendMessage(message);
            return null;
        }
        
        try {
            processMessage(message);
            circuitBreaker.recordSuccess();
            return null;
        } catch (Exception e) {
            circuitBreaker.recordFailure();
            throw e;
        }
    }
}
```

#### Saga Pattern Implementation

```java
@Entity
public class SagaMessage extends Msg {
    private String sagaId;
    private String step;
    private Map<String, Object> sagaData;
    private List<String> completedSteps;
    
    // Saga orchestration logic...
}

// Saga coordinator
messaging.addListenerForTopic("saga.*", (msg, message) -> {
    SagaMessage sagaMsg = (SagaMessage) message;
    return sagaOrchestrator.processStep(sagaMsg);
});
```

## Object Mapping & Querying

### Advanced Entity Configuration

```java
@Entity(translateCamelCase = true, polymorph = true)
@Cache(timeout = 600000, strategy = Cache.ClearStrategy.LRU)
@Index({"username", "email:1", "location:2d"})
public class User {
    @Id
    private MorphiumId id;
    
    @Property(fieldName = "user_name")
    @Index(options = {"unique:true"})
    private String username;
    
    @Reference(lazyLoading = true)
    private List<Order> orders;
    
    @Embedded
    private Address homeAddress;
    
    @Encrypted // Field-level encryption
    private String ssn;
    
    @CreationTime
    private long createdAt;
    
    @LastChange
    private long updatedAt;
    
    @LastAccess
    private long lastAccess;
}
```

### Lazy Loading References

Morphium provides powerful lazy loading capabilities for referenced objects, which can significantly improve performance by loading related data only when needed:

```java
@Entity
public class User {
    @Id
    private MorphiumId id;
    
    private String username;
    
    // Lazy loaded - only fetched when accessed
    @Reference(lazyLoading = true)
    private List<Order> orders;
    
    // Lazy loaded single reference
    @Reference(lazyLoading = true)
    private Profile profile;
    
    // Eagerly loaded by default
    @Reference
    private Company company;
}

@Entity
public class Order {
    @Id
    private MorphiumId id;
    
    private BigDecimal amount;
    private Date orderDate;
    
    // Back-reference to user (lazy loaded)
    @Reference(lazyLoading = true)
    private User customer;
}
```

#### How Lazy Loading Works

```java
// User is loaded immediately, but orders are not
User user = morphium.findById(User.class, userId);
System.out.println(user.getUsername()); // No additional queries

// This triggers the lazy loading of orders
List<Order> orders = user.getOrders(); // Query executed here
System.out.println("User has " + orders.size() + " orders");

// Subsequent access uses cached data
for (Order order : user.getOrders()) { // No additional queries
    System.out.println("Order: " + order.getAmount());
}
```

#### Lazy Loading Best Practices

```java
// 1. Use lazy loading for collections that might be large
@Entity
public class Category {
    @Reference(lazyLoading = true)
    private List<Product> products; // Could be thousands of products
}

// 2. Avoid lazy loading in loops to prevent N+1 queries
// BAD: This will cause N queries
for (User user : users) {
    System.out.println(user.getOrders().size()); // Each access loads orders
}

// GOOD: Pre-load when needed
Map<MorphiumId, List<Order>> ordersByUser = morphium.createQueryFor(Order.class)
    .f("customer").in(users)
    .asMap("customer");

// 3. Use projection when you only need specific fields
List<Order> recentOrders = morphium.createQueryFor(Order.class)
    .f("orderDate").gte(lastMonth)
    .project("amount", "orderDate") // Only load these fields
    .asList();
```

#### Proxy Behavior

Lazy-loaded references are implemented using proxies:

```java
User user = morphium.findById(User.class, userId);

// The orders field contains a proxy until first access
List<Order> orders = user.getOrders();
boolean isProxy = orders instanceof LazyDeReferencingProxy;

// After first access, it behaves like a normal list
orders.add(new Order()); // Works normally
```

### Complex Queries

```java
// Complex query with multiple conditions
Query<User> query = morphium.createQueryFor(User.class)
    .f("status").eq("active")
    .f("lastLogin").gt(System.currentTimeMillis() - 86400000) // Last 24 hours
    .f("location").near(40.7128, -74.0060, 10000) // Within 10km of NYC
    .sort("lastLogin", -1)
    .limit(50);

List<User> activeUsers = query.asList();

// Using field enums for type safety
List<User> users = morphium.createQueryFor(User.class)
    .f(User.Fields.status).eq("premium")
    .f(User.Fields.subscriptionEnd).gt(System.currentTimeMillis())
    .asList();
```

### Aggregation Pipeline

```java
// Complex aggregation example
Aggregator<User, UserStats> aggregator = morphium.createAggregator(User.class, UserStats.class);

List<UserStats> stats = aggregator
    .match(morphium.createQueryFor(User.class).f("status").eq("active"))
    .group("$country")
        .sum("totalUsers", 1)
        .avg("avgAge", "$age")
        .max("maxLoginTime", "$lastLogin")
        .end()
    .sort("totalUsers", -1)
    .limit(10)
    .aggregate();
```

### Geospatial Queries

```java
@Entity
public class Restaurant {
    @Id private MorphiumId id;
    private String name;
    
    @Index(options = {"2dsphere:true"})
    private Point location; // GeoJSON Point
    
    private String cuisine;
    private double rating;
}

// Find restaurants near a location
List<Restaurant> nearby = morphium.createQueryFor(Restaurant.class)
    .f("location").near(40.7128, -74.0060, 5000) // 5km radius
    .f("rating").gte(4.0)
    .f("cuisine").eq("Italian")
    .sort("rating", -1)
    .asList();

// Find restaurants within a polygon
Polygon searchArea = new Polygon(Arrays.asList(
    Arrays.asList(-74.1, 40.7),
    Arrays.asList(-74.0, 40.7),
    Arrays.asList(-74.0, 40.8),
    Arrays.asList(-74.1, 40.8),
    Arrays.asList(-74.1, 40.7)
));

List<Restaurant> inArea = morphium.createQueryFor(Restaurant.class)
    .f("location").geoWithin(searchArea)
    .asList();
```

## Caching System

### Cache Configuration

```java
@Entity
@Cache(
    timeout = 300000,           // 5 minutes
    maxEntries = 10000,         // Maximum cache entries
    strategy = Cache.ClearStrategy.LRU,
    syncCache = Cache.SyncCacheStrategy.CLEAR_TYPE_CACHE
)
public class Product {
    // Entity definition...
}
```

### Distributed Cache Synchronization

```java
// Setup cache synchronization via messaging
Morphium morphium = new Morphium(config);
StdMessaging messaging = new StdMessaging();
messaging.init(morphium);

// Enable cache synchronization
CacheSynchronizer cacheSynchronizer = new CacheSynchronizer(messaging, morphium);
// Cache will automatically sync across cluster nodes when data changes
```

### Custom Cache Implementation

```java
// Custom cache implementation
public class RedisMorphiumCache implements MorphiumCache {
    private RedisTemplate<String, Object> redisTemplate;
    
    @Override
    public void store(Class<?> type, String key, Object value, long validUntil) {
        redisTemplate.opsForValue().set(
            getCacheKey(type, key), 
            value, 
            Duration.ofMillis(validUntil - System.currentTimeMillis())
        );
    }
    
    // Implement other cache methods...
}

// Use custom cache
MorphiumConfig config = new MorphiumConfig();
config.setCache(new RedisMorphiumCache());
```

## How-To Guide

### Common Use Cases

#### 1. Building a REST API with Caching

```java
@RestController
@RequestMapping("/api/users")
public class UserController {
    @Autowired
    private Morphium morphium;
    
    @GetMapping("/{id}")
    public ResponseEntity<User> getUser(@PathVariable String id) {
        // Automatic caching if User entity has @Cache annotation
        User user = morphium.findById(User.class, new MorphiumId(id));
        return user != null ? ResponseEntity.ok(user) : ResponseEntity.notFound().build();
    }
    
    @PostMapping
    public ResponseEntity<User> createUser(@RequestBody User user) {
        morphium.store(user); // Cache will be updated automatically
        return ResponseEntity.ok(user);
    }
}
```

#### 2. Event-Driven Architecture with Messaging

```java
// Order service
@Service
public class OrderService {
    @Autowired
    private MorphiumMessaging messaging;
    
    public void createOrder(Order order) {
        morphium.store(order);
        
        // Publish order created event
        Msg event = new Msg("order.created", "New order created");
        event.getMapValue().put("orderId", order.getId().toString());
        event.getMapValue().put("customerId", order.getCustomerId());
        event.getMapValue().put("amount", order.getAmount());
        messaging.sendMessage(event);
    }
}

// Inventory service listener
@Component
public class InventoryService {
    @PostConstruct
    public void initializeListeners() {
        messaging.addListenerForTopic("order.created", this::handleOrderCreated);
    }
    
    private Msg handleOrderCreated(MorphiumMessaging messaging, Msg message) {
        String orderId = (String) message.getMapValue().get("orderId");
        reserveInventory(orderId);
        
        // Send confirmation
        Msg response = new Msg("inventory.reserved", "Inventory reserved for order");
        response.getMapValue().put("orderId", orderId);
        return response;
    }
}
```

#### 3. Microservices Communication

```java
// User service - publishes user events
@Service
public class UserEventPublisher {
    private final MorphiumMessaging messaging;
    
    public void publishUserRegistered(User user) {
        UserEvent event = new UserEvent();
        event.setTopic("user.registered");
        event.setUserId(user.getId().toString());
        event.setUsername(user.getUsername());
        event.setEmail(user.getEmail());
        messaging.sendMessage(event);
    }
}

// Email service - subscribes to user events
@Service
public class EmailService {
    @PostConstruct
    public void setupListeners() {
        messaging.addListenerForTopic("user.registered", this::sendWelcomeEmail);
        messaging.addListenerForTopic("user.password_reset", this::sendPasswordResetEmail);
    }
    
    private Msg sendWelcomeEmail(MorphiumMessaging messaging, Msg message) {
        UserEvent event = (UserEvent) message;
        emailClient.sendWelcomeEmail(event.getEmail(), event.getUsername());
        return null;
    }
}
```

#### 4. Background Job Processing

```java
// Job queue using messaging
@Service
public class JobProcessor {
    private final MorphiumMessaging messaging;
    
    @PostConstruct
    public void startProcessing() {
        messaging.addListenerForTopic("job.image_resize", this::processImageResize);
        messaging.addListenerForTopic("job.email_batch", this::processBatchEmail);
        messaging.addListenerForTopic("job.report_generation", this::generateReport);
    }
    
    public void scheduleJob(String jobType, Map<String, Object> params) {
        Msg job = new Msg(jobType, "Background job");
        job.setMapValue(params);
        job.setPriority(params.getOrDefault("priority", 5));
        messaging.sendMessage(job);
    }
    
    private Msg processImageResize(MorphiumMessaging messaging, Msg job) {
        String imageId = (String) job.getMapValue().get("imageId");
        List<String> sizes = (List<String>) job.getMapValue().get("sizes");
        
        try {
            imageService.resizeImage(imageId, sizes);
            
            // Send completion notification
            Msg completion = new Msg("job.completed", "Job completed successfully");
            completion.getMapValue().put("originalJob", job.getMsgId().toString());
            completion.getMapValue().put("result", "success");
            messaging.sendMessage(completion);
            
        } catch (Exception e) {
            // Send failure notification
            Msg failure = new Msg("job.failed", "Job failed");
            failure.getMapValue().put("originalJob", job.getMsgId().toString());
            failure.getMapValue().put("error", e.getMessage());
            messaging.sendMessage(failure);
        }
        
        return null;
    }
}
```

#### 5. Data Synchronization Between Systems

```java
// Sync data changes to external systems
@Component
public class DataSyncHandler {
    
    @PostConstruct
    public void setupSync() {
        // Listen for all entity changes
        messaging.addListenerForTopic("entity.*", this::handleEntityChange);
    }
    
    private Msg handleEntityChange(MorphiumMessaging messaging, Msg message) {
        String topic = message.getTopic();
        String[] parts = topic.split("\\.");
        String entityType = parts[1]; // entity.User.created -> "User"
        String operation = parts[2];  // created/updated/deleted
        
        switch (entityType) {
            case "User":
                syncUserToExternalSystem(message, operation);
                break;
            case "Order":
                syncOrderToWarehouse(message, operation);
                break;
        }
        
        return null;
    }
    
    private void syncUserToExternalSystem(Msg message, String operation) {
        Map<String, Object> userData = message.getMapValue();
        externalApiClient.syncUser(userData, operation);
    }
}

// Trigger sync when entities change
@Component
public class EntityChangeListener implements MorphiumStorageListener<Object> {
    
    @Override
    public void postStore(Object entity, boolean isNew) {
        String operation = isNew ? "created" : "updated";
        String entityType = entity.getClass().getSimpleName();
        
        Msg syncMessage = new Msg("entity." + entityType + "." + operation, "Entity changed");
        syncMessage.setMapValue(morphium.getMapper().marshall(entity).toMap());
        messaging.sendMessage(syncMessage);
    }
    
    @Override
    public void postRemove(Query<Object> query) {
        // Handle deletions...
    }
}
```

### Performance Optimization Tips

#### 1. Efficient Queries
```java
// Use projection to limit returned fields
List<User> users = morphium.createQueryFor(User.class)
    .f("status").eq("active")
    .project("username", "email", "lastLogin") // Only return these fields
    .asList();

// Use explain to understand query performance
String explanation = morphium.createQueryFor(User.class)
    .f("username").eq("alice")
    .explain();
```

#### 2. Batch Operations
```java
// Bulk insert
List<User> users = Arrays.asList(/* large list of users */);
morphium.storeList(users); // More efficient than individual stores

// Bulk update
morphium.set(
    morphium.createQueryFor(User.class).f("status").eq("inactive"),
    "status", "archived"
); // Update multiple documents in one operation
```

#### 3. Connection Pool Tuning
```java
MorphiumConfig config = new MorphiumConfig();
config.setMaxConnections(50);        // Pool size based on concurrent load
config.setMinConnections(10);        // Keep minimum connections open
config.setMaxConnectionLifetime(300000); // 5 minutes max lifetime
config.setMaxConnectionIdleTime(60000);  // 1 minute idle timeout
```

### Testing Strategies

#### 1. Unit Testing with InMemory Driver
```java
@TestConfiguration
public class TestMorphiumConfig {
    
    @Bean
    @Primary
    public Morphium testMorphium() {
        MorphiumConfig config = new MorphiumConfig();
        config.setDriverName("InMemDriver"); // Use in-memory driver for tests
        config.setDatabase("test_db");
        return new Morphium(config);
    }
}

@SpringBootTest
class UserServiceTest {
    @Autowired
    private Morphium morphium;
    
    @Test
    void testUserCreation() {
        User user = new User();
        user.setUsername("testuser");
        morphium.store(user);
        
        User found = morphium.createQueryFor(User.class)
            .f("username").eq("testuser")
            .get();
            
        assertNotNull(found);
        assertEquals("testuser", found.getUsername());
    }
}
```

#### 2. Integration Testing with TestContainers
```java
@SpringBootTest
@Testcontainers
class IntegrationTest {
    
    @Container
    static MongoDBContainer mongoContainer = new MongoDBContainer("mongo:6.0")
        .withExposedPorts(27017);
    
    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        registry.add("morphium.hosts", () -> 
            mongoContainer.getHost() + ":" + mongoContainer.getFirstMappedPort());
    }
    
    @Test
    void testCompleteWorkflow() {
        // Test with real MongoDB instance
    }
}
```

## Implementation Details & Developer Notes

### Architecture Deep Dive

#### Driver Architecture
Morphium v6.0 uses a custom MongoDB wire protocol driver that provides:

- **Lightweight Implementation**: Only features needed by Morphium
- **Better Error Handling**: Morphium-specific error messages and handling
- **Performance Optimization**: Optimized for Morphium's usage patterns
- **Native Features**: Direct support for Morphium's caching and messaging

```java
// Driver interface allows pluggable implementations
public interface MorphiumDriver {
    void connect() throws MorphiumDriverException;
    MorphiumCursor find(String database, String collection, Doc query, Doc projection, Doc sort, int limit, int skip);
    List<Map<String, Object>> find(FindCommand settings) throws MorphiumDriverException;
    // ... other driver methods
}
```

#### Object Mapper Implementation
The object mapper handles the complex task of converting between Java objects and MongoDB documents:

```java
// Core mapping logic
public class ObjectMapperImpl implements MorphiumObjectMapper {
    
    public Doc marshall(Object o) {
        // Handle polymorphism
        // Process annotations
        // Convert Java types to BSON types
        // Handle embedded objects and references
    }
    
    public <T> T unmarshall(Class<T> cls, Doc doc) {
        // Create instance
        // Set fields from document
        // Handle lazy loading setup
        // Process lifecycle callbacks
    }
}
```

#### Caching Implementation
Multi-level caching with cluster synchronization:

```java
// Cache implementation details
public class MorphiumCacheImpl implements MorphiumCache {
    private final ConcurrentHashMap<String, CacheElement> cache = new ConcurrentHashMap<>();
    private final ScheduledExecutorService housekeeper = Executors.newScheduledThreadPool(1);
    
    // Cache operations with TTL handling
    // LRU eviction logic
    // Memory usage optimization
}
```

### Messaging System Internals

#### Change Stream Integration
```java
// Change stream monitoring for push-based messaging
public class ChangeStreamMonitor {
    private ChangeStreamIterable<Doc> changeStream;
    
    public void startMonitoring() {
        changeStream = morphiumDriver.watch(database, collection)
            .fullDocument(FullDocument.UPDATE_LOOKUP);
            
        changeStream.forEach(change -> {
            processChangeEvent(change);
        });
    }
    
    private void processChangeEvent(ChangeStreamEvent event) {
        // Convert change event to message
        // Notify registered listeners
        // Handle error scenarios
    }
}
```

#### Message Processing Pipeline
```java
// Message processing architecture
public class MessageProcessor {
    private final BlockingQueue<ProcessingQueueElement> processingQueue;
    private final ThreadPoolExecutor workerPool;
    
    public void processMessages() {
        while (running) {
            ProcessingQueueElement element = processingQueue.take();
            workerPool.submit(() -> processMessage(element));
        }
    }
    
    private void processMessage(ProcessingQueueElement element) {
        // Lock message for processing
        // Execute message listeners
        // Handle responses and errors
        // Update message status
        // Cleanup locks
    }
}
```

### Performance Considerations

#### Memory Management
- **Connection Pooling**: Efficient connection reuse
- **Cache Sizing**: Configurable memory limits
- **Object Lifecycle**: Proper cleanup of resources

#### Concurrency
- **Thread Safety**: All core components are thread-safe
- **Lock-Free Operations**: Where possible, avoid locks for better performance
- **Async Operations**: Non-blocking operations for high throughput

#### Monitoring and Metrics
```java
// Built-in metrics collection
public class Statistics {
    private final Map<StatisticKeys, StatisticValue> stats = new ConcurrentHashMap<>();
    
    public void recordQuery(long duration) {
        incrementStat(StatisticKeys.QUERIES_TOTAL);
        updateStat(StatisticKeys.QUERY_TIME_AVG, duration);
    }
    
    // Expose metrics for monitoring systems
    public Map<String, Object> getAllStats() {
        return stats.entrySet().stream()
            .collect(Collectors.toMap(
                e -> e.getKey().name(),
                e -> e.getValue().getValue()
            ));
    }
}
```

### Extension Points

#### Custom Type Mappers
```java
// Custom type mapper for special data types
public class GeometryTypeMapper implements MorphiumTypeMapper<Geometry> {
    @Override
    public Object marshall(Geometry geometry) {
        return geometryToGeoJSON(geometry);
    }
    
    @Override
    public Geometry unmarshall(Object value, Class<? extends Geometry> targetClass) {
        return geoJSONToGeometry((Map<String, Object>) value);
    }
}

// Register custom mapper
morphium.getObjectMapper().addTypeMapper(Geometry.class, new GeometryTypeMapper());
```

#### Storage Listeners
```java
// Global storage listener for auditing
public class AuditListener implements MorphiumStorageListener<Object> {
    @Override
    public void preStore(Object entity, boolean isNew) {
        if (entity instanceof Auditable) {
            Auditable auditable = (Auditable) entity;
            auditable.setModifiedBy(getCurrentUser());
            auditable.setModifiedAt(System.currentTimeMillis());
        }
    }
}

morphium.addListener(new AuditListener());
```

### Best Practices

#### Entity Design
- Keep entities focused and cohesive
- Use appropriate indexing strategies
- Consider document size limits (16MB)
- Design for query patterns

#### Messaging Design
- Use meaningful topic names
- Keep message payloads reasonable
- Implement proper error handling
- Consider message ordering requirements

#### Performance
- Monitor connection pool usage
- Use projection to limit data transfer
- Implement appropriate caching strategies
- Consider read preferences for replica sets

## Migration Guide

### From Morphium 5.x to 6.0

#### JDK Requirements
- **JDK 21 Required**: Update your runtime environment
- **Module System**: Ensure proper module path if using JPMS

#### Configuration Changes
```java
// Old v5 configuration
MorphiumConfig config = new MorphiumConfig();
config.setDriverClass(SingleMongoConnectDriver.class.getName());

// New v6 configuration
MorphiumConfig config = new MorphiumConfig();
config.setDriverName("SingleConnectDriver"); // String-based driver selection
```

#### Messaging API Changes
```java
// Old v5 messaging
Messaging messaging = new Messaging(morphium);
messaging.addMessageListener("topic", listener);

// New v6 messaging
StdMessaging messaging = new StdMessaging();
messaging.init(morphium);
messaging.addListenerForTopic("topic", listener);
```

#### Breaking Changes
1. **Driver Configuration**: Use driver names instead of class names
2. **Messaging Interface**: Updated method signatures
3. **Java 21 Features**: May use newer Java language features

### From MongoDB Java Driver to Morphium

#### Entity Mapping
```java
// MongoDB Java Driver
@Document
public class User {
    @BsonId
    private ObjectId id;
    
    @BsonProperty("user_name")
    private String username;
}

// Morphium equivalent
@Entity
public class User {
    @Id
    private MorphiumId id;
    
    @Property(fieldName = "user_name")
    private String username;
}
```

#### Query Migration
```java
// MongoDB Java Driver
FindIterable<Document> docs = collection.find(Filters.eq("status", "active"));

// Morphium equivalent
List<User> users = morphium.createQueryFor(User.class)
    .f("status").eq("active")
    .asList();
```

## Conclusion

Morphium v6.0 represents a mature, production-ready solution for Java applications requiring both sophisticated MongoDB access and reliable message queueing. Built on its own lightweight MongoDB wire protocol driver (since v5.0), Morphium offers superior performance and stability compared to solutions relying on the standard MongoDB Java driver. This custom implementation provides better error handling, reduced dependencies, and optimizations specifically tailored for Morphium's usage patterns.

The unique approach of leveraging MongoDB for messaging provides unmatched transparency, operational simplicity, and integration benefits that traditional message brokers cannot offer. Combined with the performance advantages of the custom driver implementation, Morphium delivers both efficiency and reliability.

Whether you're building microservices, implementing event-driven architectures, or need high-performance data access with intelligent caching, Morphium provides the tools and patterns to succeed. The combination of type-safe object mapping, powerful querying capabilities, MongoDB-native messaging, and optimized driver performance makes it an excellent choice for modern Java applications.

For support, contributions, and the latest updates, visit the [Morphium GitHub repository](https://github.com/sboesebeck/morphium).

*This documentation covers Morphium v6.0. For older versions, refer to the legacy documentation or consider upgrading to take advantage of the latest features and improvements.*