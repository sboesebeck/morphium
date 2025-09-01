# API Reference

Comprehensive reference for Morphium's core APIs, methods, and interfaces.

## Core Morphium API

### Morphium Class

The main entry point for all Morphium operations.

#### Constructor

```java
// Primary constructor
public Morphium(MorphiumConfig config)

// Convenience constructors (since V2.2.23)
public Morphium(String host, String database)
public Morphium(String host, int port, String database) 
```

#### Basic Operations

**Store Operations:**
```java
// Store single entity
public <T> void store(T entity)
public <T> void store(T entity, AsyncOperationCallback<T> callback)

// Store multiple entities
public <T> void storeList(List<T> entities)
public <T> void storeList(List<T> entities, AsyncOperationCallback<T> callback)

// Update using fields (partial updates)
public <T> void updateUsingFields(T entity, String... fields)
```

**Delete Operations:**
```java
// Delete single entity
public <T> void delete(T entity)
public <T> void delete(T entity, AsyncOperationCallback<T> callback)

// Delete by query
public <T> void delete(Query<T> query)
public <T> void delete(Query<T> query, AsyncOperationCallback<T> callback)
```

**Query Operations:**
```java
// Create query for type
public <T> Query<T> createQueryFor(Class<T> type)

// Get single entity by ID
public <T> T findById(Class<T> type, Object id)
```

#### Configuration and Lifecycle

```java
// Get configuration
public MorphiumConfig getConfig()

// Get driver
public MorphiumDriver getDriver()

// Close connection and cleanup
public void close()

// Check if closed
public boolean isClosed()
```

#### Index Management

```java
// Ensure all indexes for a type
public <T> void ensureIndicesFor(Class<T> type)

// Create index manually
public <T> void ensureIndex(Class<T> type, Map<String, Object> index)
public <T> void ensureIndex(Class<T> type, String... fields)
```

#### Transaction Support

```java
// Transaction management
public void beginTransaction()
public void commitTransaction() 
public void abortTransaction()

// Check transaction state
public boolean isTransactionInProgress()
public MorphiumTransactionContext getTransactionContext()
```

## Query API

### Query Interface

**Field Selection:**
```java
// Select field for operations
public Query<T> f(String field)
public Query<T> f(Enum field) // Using field enums

// Field operations
public Query<T> eq(Object value)           // Equal
public Query<T> ne(Object value)           // Not equal  
public Query<T> lt(Object value)           // Less than
public Query<T> lte(Object value)          // Less than or equal
public Query<T> gt(Object value)           // Greater than
public Query<T> gte(Object value)          // Greater than or equal
public Query<T> in(Collection<?> values)   // In array
public Query<T> nin(Collection<?> values)  // Not in array
public Query<T> matches(String regex)      // Regex match
public Query<T> exists()                   // Field exists
public Query<T> notExists()               // Field doesn't exist
```

**Query Modifiers:**
```java
// Sorting
public Query<T> sort(String field)        // Ascending
public Query<T> sort(Map<String, Integer> sort) // Custom sort

// Pagination
public Query<T> skip(int skip)
public Query<T> limit(int limit)

// Projection  
public Query<T> project(String... fields)
public Query<T> addProjection(String field, String projection)
```

**Query Execution:**
```java
// Get results
public List<T> asList()                    // All results as list
public T get()                             // Single result (first match)
public long countAll()                     // Count all matches

// Async execution
public void asList(AsyncOperationCallback<T> callback)
public void get(AsyncOperationCallback<T> callback)

// Iterator for large result sets
public MorphiumIterator<T> asIterable()
public MorphiumIterator<T> asIterable(int windowSize)
public MorphiumIterator<T> asIterable(int windowSize, int prefetch)
```

**Logical Operators:**
```java
// OR conditions
public Query<T> or(Query<T>... queries)

// NOR conditions  
public Query<T> nor(Query<T>... queries)

// Create sub-query
public Query<T> q() // New query instance
```

**Advanced Features:**
```java
// Text search
public Query<T> text(Query.TextSearchLanguages language, String... terms)
public Query<T> text(Query.TextSearchLanguages language, boolean caseSensitive, 
                     boolean diacriticSensitive, String... terms)

// Distinct values
public List<Object> distinct(String field)

// Complex query with raw MongoDB query
public List<T> complexQuery(Map<String, Object> query)
```

## Aggregation API

### Aggregator Interface

```java
// Create aggregator
public <T, R> Aggregator<T, R> createAggregator(Class<T> inputType, Class<R> resultType)
```

**Pipeline Operations:**
```java
// Match stage (filter)
public Aggregator<T, R> match(Query<T> query)

// Project stage (field selection/transformation)  
public Aggregator<T, R> project(String... fields)
public Aggregator<T, R> project(Map<String, Object> projection)

// Group stage
public Aggregator<T, R> group(String groupBy)
public Aggregator<T, R> group(Map<String, Object> groupBy)

// Group operations
public Aggregator<T, R> sum(String field, String source)
public Aggregator<T, R> avg(String field, String source)  
public Aggregator<T, R> min(String field, String source)
public Aggregator<T, R> max(String field, String source)
public Aggregator<T, R> first(String field, String source)
public Aggregator<T, R> last(String field, String source)
public Aggregator<T, R> count(String field)

// End group stage
public Aggregator<T, R> end()

// Sort stage
public Aggregator<T, R> sort(String... fields)
public Aggregator<T, R> sort(Map<String, Integer> sort)

// Skip/Limit stages
public Aggregator<T, R> skip(int skip)
public Aggregator<T, R> limit(int limit)
```

**Execution:**
```java
// Execute aggregation
public List<R> aggregate()

// Get raw aggregation list
public List<Map<String, Object>> toAggregationList()
```

## Messaging API

### Messaging Interface

**Setup and Lifecycle:**
```java
// Initialize messaging
public void init(Morphium morphium)
public void init(Morphium morphium, MessagingSettings settings)

// Start/stop messaging
public void start()
public void terminate()

// Check state
public boolean isAlive()
```

**Message Operations:**
```java
// Send message
public void sendMessage(Msg message)

// Send and wait for responses
public Msg sendAndAwaitFirstAnswer(Msg message, long timeout)
public List<Msg> sendAndAwaitAnswers(Msg message, int expectedAnswers, long timeout)

// Send message to specific listener
public void sendDirectMessage(Msg message, String host, String listenerId)
```

**Listener Management:**
```java
// Add topic listener
public void addListenerForTopic(String topic, MessageListener listener)
public void addListenerForTopic(String topic, MessageListener listener, boolean multithreaded)

// Remove listener
public boolean removeListenerForTopic(String topic, MessageListener listener)

// Get registered listeners
public List<MessageListener> getListenersForTopic(String topic)
```

**Message Listener Interface:**
```java
@FunctionalInterface
public interface MessageListener {
    /**
     * Process incoming message
     * @param messaging The messaging instance
     * @param message The received message
     * @return Response message (null if no response)
     */
    Msg onMessage(Morphium messaging, Msg message);
}
```

### Msg Class

**Constructor:**
```java
public Msg(String topic, String message, String value)
public Msg(String topic, String message, String value, long ttl)
```

**Properties:**
```java
// Basic properties
public String getTopic()
public void setTopic(String topic)

public String getMsg()
public void setMsg(String message) 

public String getValue()
public void setValue(String value)

// Timing properties
public long getTtl()
public void setTtl(long ttl)

public long getTimestamp()
public void setTimestamp(long timestamp)

// Message routing
public boolean isExclusive()
public void setExclusive(boolean exclusive)

public String getInAnswerTo()  
public void setInAnswerTo(String inAnswerTo)

public String getSender()
public void setSender(String sender)

public String getRecipient()
public void setRecipient(String recipient)
```

**Map Values (for complex data):**
```java
// Store/retrieve complex objects as map
public Map<String, Object> getMapValue()
public void setMapValue(Map<String, Object> mapValue)

// Convenience methods
public void addAdditional(String key, Object value)
public Object getAdditional(String key)
```

## Configuration API

### MorphiumConfig

**Main Settings Access:**
```java
// Get nested settings objects
public ConnectionSettings connectionSettings()
public ClusterSettings clusterSettings()
public DriverSettings driverSettings()
public MessagingSettings messagingSettings()
public CacheSettings cacheSettings()
public ThreadPoolSettings threadPoolSettings()
public WriterSettings writerSettings()
public ObjectMappingSettings objectMappingSettings()
public EncryptionSettings encryptionSettings()
public CollectionCheckSettings collectionCheckSettings()
public AuthSettings authSettings()
```

**Logging Configuration:**
```java
// Global logging
public void setGlobalLogLevel(int level)
public void setGlobalLogFile(String fileName)
public void setGlobalLogSynced(boolean synced)

// Class/package specific logging  
public void setLogLevelForClass(Class<?> cls, int level)
public void setLogLevelForPrefix(String prefix, int level)
public void setLogFileForClass(Class<?> cls, String fileName)
public void setLogFileForPrefix(String prefix, String fileName)
```

**Factory Methods:**
```java
// Create from different sources
public static MorphiumConfig createFromJson(String json)
public static MorphiumConfig fromProperties(Properties props)
```

## Annotation Reference

### Entity Annotations

**@Entity**
```java
@Entity(
    value = "collection_name",        // Custom collection name
    translateCamelCase = true,        // Convert camelCase to snake_case
    polymorph = false,                // Store class name for inheritance
    useFQN = false,                   // Use fully qualified class name
    nameProvider = NameProvider.class // Custom naming strategy
)
```

**@Embedded**
```java
@Embedded(
    polymorph = false,     // Store class name for polymorphism
    translateCamelCase = true // Convert field names
)
```

**@Id**
```java
@Id // Mark field as MongoDB _id field
```

**@Property**
```java
@Property(
    fieldName = "custom_field_name"  // Custom field name in MongoDB
)
```

**@Reference**
```java
@Reference(
    lazyLoading = false,    // Enable lazy loading
    fieldName = "ref_field" // Custom field name
)
```

### Index Annotations

**@Index (Field Level)**
```java
@Index(
    direction = IndexDirection.ASC,  // ASC or DESC
    options = {"unique:true"}        // MongoDB index options
)
```

**@Index (Class Level)**
```java
@Index({
    "field1",           // Simple ascending index
    "-field2",          // Descending index (note the minus)
    "field3,field4",    // Compound index
    "location:2d"       // Geospatial index
})
```

### Caching Annotations

**@Cache**
```java
@Cache(
    timeout = 60000,                    // Cache timeout in ms
    maxEntries = 10000,                 // Maximum cache entries
    strategy = Cache.ClearStrategy.LRU, // Eviction strategy
    syncCache = Cache.SyncCacheStrategy.CLEAR_TYPE_CACHE, // Cluster sync
    clearOnWrite = true                 // Clear cache on writes
)
```

**@NoCache**
```java
@NoCache // Disable caching for this entity
```

### Write Buffer Annotations

**@WriteBuffer**
```java
@WriteBuffer(
    size = 1000,                              // Buffer size
    timeout = 5000,                           // Flush timeout (ms)
    strategy = WriteBuffer.STRATEGY.WRITE_OLD // Strategy when full
)
```

**@AsyncWrites**
```java
@AsyncWrites // All writes are asynchronous
```

### Lifecycle Annotations

**@CreationTime**
```java
@CreationTime
private long createdAt; // Set on first save
```

**@LastChange**
```java
@LastChange  
private long lastModified; // Updated on each save
```

**@LastAccess**
```java
@LastAccess
private long lastAccessed; // Updated on each read
```

**Lifecycle Callbacks:**
```java
@PreStore
public void beforeStore() {
    // Called before storing to database
}

@PostStore  
public void afterStore() {
    // Called after successful store
}

@PreLoad
public void beforeLoad() {
    // Called before loading from database
}

@PostLoad
public void afterLoad() {
    // Called after loading from database
}
```

### Validation Annotations

Morphium supports standard javax.validation annotations:

```java
@Entity
public class User {
    @NotNull
    @Size(min = 3, max = 50)
    private String username;
    
    @Email
    private String email;
    
    @Min(18)
    private int age;
    
    @Pattern(regexp = "^[A-Za-z]+$")
    private String firstName;
}
```

## Exception Handling

### Common Exceptions

**MorphiumDriverException:**
```java
// Thrown for driver-level issues
try {
    morphium.store(entity);
} catch (MorphiumDriverException e) {
    // Handle connection/driver errors
    logger.error("Driver error: " + e.getMessage(), e);
}
```

**MorphiumAccessVetoException:**
```java
// Thrown when access is denied by security rules
try {
    List<User> users = query.asList();
} catch (MorphiumAccessVetoException e) {
    // Handle security violations
    logger.warn("Access denied: " + e.getMessage());
}
```

## Utility Classes

### MorphiumIterator

**Large Dataset Processing:**
```java
// Create iterator
MorphiumIterator<Entity> iterator = query.asIterable(1000, 5);

// Navigation
public boolean hasNext()
public T next()
public void remove()

// Position information  
public long getCount()         // Total number of results
public int getCursor()         // Current position
public void ahead(int steps)   // Jump ahead
public void back(int steps)    // Jump back

// Buffer information
public List<T> getCurrentBuffer()     // Current buffer contents
public int getCurrentBufferSize()     // Current buffer size

// Threading
public void setMultithreaddedAccess(boolean enable)
```

### ObjectMapper

**Manual Object Mapping:**
```java
ObjectMapper mapper = morphium.getMapper();

// Object to BSON
Map<String, Object> bson = mapper.marshall(entity);

// BSON to Object  
Entity entity = mapper.unmarshall(Entity.class, bson);

// JSON support
String json = mapper.marshall(entity).toString();
Entity entity = mapper.unmarshall(Entity.class, json);
```

This API reference provides comprehensive documentation of all major Morphium APIs, including method signatures, parameters, and usage examples.