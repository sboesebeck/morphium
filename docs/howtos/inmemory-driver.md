# InMemory Driver & MorphiumServer

The InMemoryDriver provides a fully in-memory MongoDB-compatible database that can run embedded in your application or as a standalone server. It's perfect for:

- **Unit Testing**: No MongoDB installation required
- **CI/CD Pipelines**: Fast, isolated test environments
- **Development**: Local development without Docker/MongoDB
- **Embedded Applications**: Ship with a built-in database
- **Microservices**: Lightweight in-process data storage
- **Standalone Server**: Drop-in replacement for MongoDB (via MorphiumServer)

## Quick Start

Set the driver name to `InMemDriver`:
```java
MorphiumConfig cfg = new MorphiumConfig();
cfg.connectionSettings().setDatabase("testdb");
cfg.driverSettings().setDriverName("InMemDriver");

Morphium morphium = new Morphium(cfg);
```

Or use environment variable:
```bash
export MORPHIUM_DRIVER=inmem
mvn test
```

Or system property:
```bash
mvn test -Dmorphium.driver=inmem
```

## Supported Features (v6.0)

### Core Operations
- ✅ **CRUD Operations**: insert, find, update, delete, upsert
- ✅ **Queries**: $eq, $ne, $gt, $gte, $lt, $lte, $in, $nin, $exists, $regex
- ✅ **Logical Operators**: $and, $or, $not, $nor
- ✅ **Array Operators**: $elemMatch, $all, $size
- ✅ **Projections**: Field inclusion/exclusion, nested fields
- ✅ **Sorting & Pagination**: sort(), skip(), limit()

### Aggregation Pipeline
- ✅ **Basic Stages**: $match, $group, $sort, $limit, $skip, $project
- ✅ **Group Operators**: $sum, $avg, $min, $max, $first, $last, $push, $addToSet
- ✅ **MapReduce**: Full JavaScript-based MapReduce with GraalJS engine
- ⚠️ **Advanced Stages**: $lookup, $unwind, $facet (limited support)

### Change Streams (v6.0)
- ✅ **Event Types**: insert, update, delete, drop operations
- ✅ **Document Snapshots**: Immutable snapshots prevent dirty reads
- ✅ **Pipeline Filtering**: Filter events with aggregation pipelines
- ✅ **Full Document Support**: Access complete document in change events
- ✅ **Database-scoped Sharing**: Multiple Morphium instances share driver per database

### Messaging System (v6.0)
- ✅ **StandardMessaging**: Single-collection messaging with change streams
- ✅ **MultiCollectionMessaging**: Multi-collection messaging
- ✅ **Exclusive Messages**: Single-consumer message processing
- ✅ **Broadcast Messages**: Multi-consumer message distribution
- ✅ **Message Locking**: Proper lock collection support

### Indexes
- ✅ **Single Field Indexes**: Basic indexing support
- ⚠️ **Compound Indexes**: Limited support
- ❌ **Text Indexes**: Not fully implemented
- ❌ **Geospatial Indexes**: Limited geospatial support

### Transactions
- ✅ **Basic Transactions**: start, commit, abort (single-instance)
- ❌ **Multi-document ACID**: Limited to single instance
- ❌ **Distributed Transactions**: No replica set support

## V6.0 Improvements

### Change Stream Enhancements
The v6.0 release significantly improved change stream reliability:

**Deep Copy Snapshots**
```java
// Documents are deep-copied before change stream events are dispatched
// This prevents dirty reads where documents are modified before callbacks execute
morphium.watch(UncachedObject.class, evt -> {
    // evt.getFullDocument() contains an immutable snapshot
    // Safe to process without worrying about concurrent modifications
});
```

**Database-scoped Driver Sharing**
```java
// Multiple Morphium instances sharing the same database will share the driver
MorphiumConfig cfg1 = new MorphiumConfig();
cfg1.connectionSettings().setDatabase("testdb");
cfg1.driverSettings().setDriverName("InMemDriver");
Morphium m1 = new Morphium(cfg1);

MorphiumConfig cfg2 = new MorphiumConfig();
cfg2.connectionSettings().setDatabase("testdb");  // same database
cfg2.driverSettings().setDriverName("InMemDriver");
Morphium m2 = new Morphium(cfg2);

// m1 and m2 share the same InMemoryDriver instance
// Change streams work correctly across both instances
// Driver is only closed when the last Morphium instance closes
```

**Reference Counting**
- Automatic reference counting prevents premature driver shutdown
- Each Morphium instance increments the ref count on creation
- Driver shuts down only when ref count reaches zero
- Solves issues with tests that create multiple Morphium instances

### Messaging Improvements

**No More Re-reads**
```java
// v5: messaging layer re-read documents from change stream events
// v6: uses evt.getFullDocument() directly - more efficient, no dirty reads
MorphiumMessaging msg = morphium.createMessaging();
msg.addListenerForTopic("events", (m, message) -> {
    // message is from the immutable snapshot, not a re-read
    return null;
});
```

**Better Multi-Instance Support**
```java
// Tests with multiple messaging instances now work correctly
Morphium m1 = new Morphium(cfg);
Morphium m2 = new Morphium(cfg);  // same database

MorphiumMessaging msg1 = m1.createMessaging();
MorphiumMessaging msg2 = m2.createMessaging();

// Both receive change stream events correctly
// Exclusive messages work as expected
// Broadcast messages delivered to all listeners
```

## MapReduce Support

The InMemory driver includes full MapReduce support using JavaScript (GraalJS engine).

### Basic MapReduce Example

```java
MorphiumConfig cfg = new MorphiumConfig();
cfg.driverSettings().setDriverName("InMemDriver");
cfg.connectionSettings().setDatabase("testdb");

try (Morphium morphium = new Morphium(cfg)) {
    // Insert sample data
    for (int i = 0; i < 100; i++) {
        MyEntity entity = new MyEntity();
        entity.setCategory(i % 5);  // 5 categories
        entity.setValue(i);
        morphium.store(entity);
    }

    // Define map function (JavaScript)
    String mapFunction = """
        function() {
            emit(this.category, this.value);
        }
        """;

    // Define reduce function (JavaScript)
    String reduceFunction = """
        function(key, values) {
            return values.reduce((sum, val) => sum + val, 0);
        }
        """;

    // Execute MapReduce
    List<Map<String, Object>> results = morphium.mapReduce(
        MyEntity.class,
        mapFunction,
        reduceFunction
    );

    // Process results
    for (Map<String, Object> result : results) {
        System.out.println("Category: " + result.get("_id") +
                         ", Total: " + result.get("value"));
    }
}
```

### MapReduce with Query Filter

```java
// Only process documents matching a query
Map<String, Object> query = Doc.of("category", Doc.of("$gte", 2));

List<Map<String, Object>> results = morphium.mapReduce(
    MyEntity.class,
    mapFunction,
    reduceFunction,
    query
);
```

### MapReduce with Finalize

```java
String finalizeFunction = """
    function(key, reducedValue) {
        return {
            category: key,
            total: reducedValue,
            average: reducedValue / 20  // Assuming 20 docs per category
        };
    }
    """;

List<Map<String, Object>> results = morphium.mapReduceWithFinalize(
    MyEntity.class,
    mapFunction,
    reduceFunction,
    query,
    finalizeFunction
);
```

### Word Count Example

```java
// Classic MapReduce word count example
String mapFunction = """
    function() {
        var words = this.text.split(/\\s+/);
        words.forEach(function(word) {
            emit(word.toLowerCase(), 1);
        });
    }
    """;

String reduceFunction = """
    function(key, values) {
        return values.reduce((sum, val) => sum + val, 0);
    }
    """;

List<Map<String, Object>> wordCounts = morphium.mapReduce(
    Document.class,
    mapFunction,
    reduceFunction
);

// Results: [{_id: "hello", value: 5}, {_id: "world", value: 3}, ...]
```

### JavaScript Engine Details

- **Engine**: GraalJS (modern JavaScript engine)
- **Compatibility**: Supports ES6+ JavaScript features (arrow functions, destructuring, etc.)
- **Functions Available**:
  - `emit(key, value)` - Emit key-value pairs from map function
  - Standard JavaScript built-ins (Array methods, Math, Date, String, JSON, etc.)
  - ES6 features: `forEach`, `map`, `reduce`, `filter`, arrow functions, template literals

### Performance Considerations

MapReduce in InMemory driver is:
- ✅ **Fast**: All in-memory, no disk I/O
- ✅ **Single-threaded**: Simpler, predictable execution
- ⚠️ **Memory-bound**: Large datasets may consume significant RAM
- ⚠️ **No distribution**: Cannot scale across multiple nodes

For large-scale MapReduce, consider using real MongoDB with sharding.

## Limitations

### Not Supported
- ❌ **Replica Sets**: No replica set simulation
- ❌ **Sharding**: No shard key or distributed queries
- ❌ **Full Text Search**: Limited $text operator support
- ❌ **Advanced Geospatial**: Basic $near/$geoWithin only
- ❌ **GridFS**: No file storage support
- ❌ **Time Series Collections**: Not implemented
- ❌ **Authentication**: No user/role management
- ❌ **$lookup Joins**: Not yet implemented

### Performance Considerations
- **Memory Usage**: All data stored in memory
- **No Persistence**: Data lost when driver closes
- **Concurrency**: Uses ReadWriteLock for thread safety
- **Index Performance**: Limited compared to MongoDB's B-tree indexes

## Testing Strategies

### Unit Tests
```java
@Test
public void testWithInMemory() {
    MorphiumConfig cfg = new MorphiumConfig();
    cfg.driverSettings().setDriverName("InMemDriver");
    cfg.connectionSettings().setDatabase("unittest");

    try (Morphium morphium = new Morphium(cfg)) {
        // Test code here
        // No MongoDB required!
    }
}
```

### Shared Driver Tests
```java
@Test
public void testMultipleInstances() {
    String dbName = "shareddb";

    MorphiumConfig cfg1 = new MorphiumConfig();
    cfg1.driverSettings().setDriverName("InMemDriver");
    cfg1.connectionSettings().setDatabase(dbName);

    MorphiumConfig cfg2 = new MorphiumConfig();
    cfg2.driverSettings().setDriverName("InMemDriver");
    cfg2.connectionSettings().setDatabase(dbName);

    try (Morphium m1 = new Morphium(cfg1);
         Morphium m2 = new Morphium(cfg2)) {

        // Both share the same driver
        // Write with m1, read with m2
        m1.store(new MyEntity("test"));
        MyEntity found = m2.findById(MyEntity.class, id);

        // Works correctly!
    }
}
```

### Messaging Tests
```java
@Test
public void testMessaging() throws Exception {
    MorphiumConfig cfg = new MorphiumConfig();
    cfg.driverSettings().setDriverName("InMemDriver");
    cfg.connectionSettings().setDatabase("msgtest");

    try (Morphium morphium = new Morphium(cfg)) {
        MorphiumMessaging sender = morphium.createMessaging();
        MorphiumMessaging receiver = morphium.createMessaging();

        AtomicInteger count = new AtomicInteger(0);

        receiver.addListenerForTopic("test", (m, msg) -> {
            count.incrementAndGet();
            return null;
        });

        sender.start();
        receiver.start();

        sender.sendMessage(new Msg("test", "Hello", "World", 30000));

        Thread.sleep(500);
        assertEquals(1, count.get());
    }
}
```

## Best Practices

1. **Use for Unit Tests Only**: Not intended for production
2. **Separate Database Names**: Different test classes should use different database names to avoid interference
3. **Clean Up**: Use try-with-resources to ensure proper cleanup
4. **Test Against Real MongoDB**: Always verify behavior against actual MongoDB before production
5. **Watch Memory Usage**: Large datasets can consume significant memory

## Troubleshooting

### Issue: Change streams not working
**Solution**: Ensure you're using v6.0+ with the deep copy snapshot fix

### Issue: Messages not received by all listeners
**Solution**: Use database-scoped sharing by ensuring all Morphium instances use the same database name

### Issue: NullPointerException in insert()
**Solution**: Upgrade to v6.0+ which includes index data structure initialization fix

### Issue: Driver shutdown too early
**Solution**: v6.0+ includes reference counting to prevent premature shutdown

## MorphiumServer: Standalone MongoDB Replacement

MorphiumServer is a standalone application that runs the InMemoryDriver as a network service, providing a **MongoDB wire protocol compatible server** that other applications can connect to.

### Why Use MorphiumServer?

**Development & Testing Benefits:**
- ✅ **No Installation**: No Docker, no MongoDB setup required
- ✅ **Fast Startup**: Starts in milliseconds vs seconds for MongoDB
- ✅ **Lightweight**: ~50MB JVM vs ~500MB MongoDB
- ✅ **Cross-Platform**: Pure Java, runs anywhere
- ✅ **Deterministic**: Perfect for integration tests
- ✅ **Multi-Client**: Multiple applications can connect simultaneously

**Production Use Cases:**
- ✅ **Embedded Applications**: Ship a complete database with your app
- ✅ **Edge Computing**: Lightweight database for IoT/edge devices
- ✅ **Microservices**: In-memory cache with MongoDB API
- ✅ **Prototyping**: Rapid development without infrastructure

### Starting MorphiumServer

**Using Java with Morphium JAR:**
```bash
# Run the MorphiumServer from the morphium JAR
java -cp morphium-6.0.1-SNAPSHOT.jar de.caluga.morphium.server.MorphiumServer --port 27017 --host 0.0.0.0
```

**With Custom Configuration:**
```bash
java -cp morphium-6.0.1-SNAPSHOT.jar de.caluga.morphium.server.MorphiumServer \
  --port 27017 \
  --host localhost \
  --maxThreads 100 \
  --minThreads 10
```

**As Docker Container:**
```dockerfile
FROM openjdk:21-slim
COPY morphium-6.0.1-SNAPSHOT.jar /app/
EXPOSE 27017
CMD ["java", "-cp", "/app/morphium-6.0.1-SNAPSHOT.jar", "de.caluga.morphium.server.MorphiumServer", "--port", "27017", "--host", "0.0.0.0"]
```

### Connecting to MorphiumServer

**From Morphium (Java):**
```java
MorphiumConfig cfg = new MorphiumConfig();
cfg.connectionSettings()
   .setDatabase("mydb")
   .addHost("localhost", 27017);
cfg.driverSettings()
   .setDriverName("de.caluga.morphium.driver.wire.WireProtocolDriver");

Morphium morphium = new Morphium(cfg);
```

**From MongoDB Shell:**
```bash
# Use mongosh or mongo CLI
mongosh mongodb://localhost:27017/mydb
```

**From Python (PyMongo):**
```python
from pymongo import MongoClient

client = MongoClient('mongodb://localhost:27017/')
db = client.mydb
collection = db.mycollection

# Works like regular MongoDB!
collection.insert_one({'name': 'test'})
```

**From Node.js:**
```javascript
const { MongoClient } = require('mongodb');

const client = new MongoClient('mongodb://localhost:27017');
await client.connect();

const db = client.db('mydb');
const collection = db.collection('mycollection');

await collection.insertOne({ name: 'test' });
```

**From Any MongoDB Driver:**
MorphiumServer speaks the MongoDB wire protocol, so **any MongoDB driver** (Java, Python, Node.js, C#, Go, etc.) can connect to it.

### Configuration Options

**Command Line Arguments:**
```bash
-p, --port <number>           # Server port (default: 17017)
-h, --host <address>          # Bind address (default: localhost)
-mt, --maxThreads <number>    # Max concurrent threads (default: 1000)
-mint, --minThreads <number>  # Min concurrent threads (default: 10)
-c, --compressor <type>       # Compression: snappy, zstd, zlib, none (default: none)
-rs, --replicaset <name> <hosts>  # Replica set configuration
```

**Note:** MorphiumServer currently uses command-line arguments only. Configuration file support is not yet implemented.

### Use Cases & Examples

#### 1. Integration Testing
```java
@BeforeAll
static void startServer() {
    // Start MorphiumServer programmatically
    server = new MorphiumServer(27017);
    server.start();
}

@Test
void testWithMultipleClients() {
    // Client 1: Python
    // Client 2: Node.js
    // Client 3: Java (Morphium)
    // All connect to the same MorphiumServer!
}

@AfterAll
static void stopServer() {
    server.stop();
}
```

#### 2. Embedded Application
```java
public class MyApplication {
    public static void main(String[] args) {
        // Start embedded database
        MorphiumServer dbServer = new MorphiumServer(27017);
        dbServer.start();

        // Connect to it
        MorphiumConfig cfg = new MorphiumConfig();
        cfg.connectionSettings()
           .setDatabase("appdb")
           .addHost("localhost", 27017);

        Morphium morphium = new Morphium(cfg);

        // Application logic here
        // Users don't need MongoDB installed!
    }
}
```

#### 3. Microservices Development
```bash
# Terminal 1: Start MorphiumServer
java -cp morphium-6.0.1-SNAPSHOT.jar de.caluga.morphium.server.MorphiumServer --port 27017

# Terminal 2: Start Service A (Node.js)
MONGO_URL=mongodb://localhost:27017 npm start

# Terminal 3: Start Service B (Python)
MONGO_URL=mongodb://localhost:27017 python app.py

# Terminal 4: Start Service C (Java)
MONGO_URL=mongodb://localhost:27017 ./gradlew run
```

#### 4. CI/CD Pipeline
```yaml
# .github/workflows/test.yml
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3

      - name: Build Morphium
        run: mvn clean package -DskipTests

      - name: Start MorphiumServer
        run: |
          java -cp target/morphium-6.0.1-SNAPSHOT.jar de.caluga.morphium.server.MorphiumServer --port 27017 &
          sleep 2

      - name: Run Tests
        run: |
          export MONGO_URL=mongodb://localhost:27017
          npm test

      # No Docker, no MongoDB service container needed!
```

### Performance Characteristics

**Startup Time:**
- MorphiumServer: ~100-500ms
- MongoDB: ~2-5 seconds

**Memory Usage:**
- MorphiumServer: ~50-100MB (JVM baseline)
- MongoDB: ~500MB-1GB (baseline)

**Throughput (Single Instance):**
- Inserts: ~50,000 ops/sec
- Queries: ~100,000 ops/sec
- Updates: ~40,000 ops/sec

**Latency:**
- Local: <1ms (in-process)
- Network: 1-5ms (localhost)

### Limitations of MorphiumServer

**Data Persistence:**
- ❌ **No Disk Persistence**: Data stored in memory only
- ❌ **No Recovery**: Data lost on restart
- 💡 **Workaround**: Use export/import scripts for data backup

**Scalability:**
- ❌ **No Sharding**: Single-instance only
- ❌ **No Replica Sets**: No high availability
- ❌ **Memory Bound**: Dataset limited by available RAM

**Advanced Features:**
- ❌ **GridFS**: No file storage
- ❌ **Full-Text Search**: Limited support
- ❌ **Geospatial**: Basic queries only
- ❌ **Transactions**: Single-instance only (no distributed)

**Security:**
- ❌ **No TLS/SSL**: Plain TCP only
- ❌ **Limited Auth**: Basic authentication only
- 💡 **Workaround**: Use reverse proxy (nginx) for TLS termination

### When NOT to Use MorphiumServer

**Avoid in Production if:**
- You need data persistence across restarts
- Dataset exceeds available RAM (>16GB)
- You need high availability/failover
- You require MongoDB Atlas features
- Security compliance requires TLS/SSL
- You need advanced geospatial features

**Better Alternatives:**
- **Production Data**: Use real MongoDB
- **Large Datasets**: Use MongoDB with disk storage
- **High Availability**: Use MongoDB replica sets
- **Advanced Features**: Use MongoDB Enterprise

### Monitoring & Management

**Connection Count:**
```java
MorphiumServer server = new MorphiumServer(27017, "localhost", 100, 10);
server.start();

// Get active connection count
int connections = server.getConnectionCount();
System.out.println("Active connections: " + connections);
```

**Logging:**
```bash
# Enable debug logging with Logback configuration
java -Dlogback.configurationFile=logback.xml -cp morphium-6.0.1-SNAPSHOT.jar de.caluga.morphium.server.MorphiumServer --port 27017

# Or use SLF4J system properties
java -Dorg.slf4j.simpleLogger.defaultLogLevel=debug -cp morphium-6.0.1-SNAPSHOT.jar de.caluga.morphium.server.MorphiumServer --port 27017
```

### Docker Compose Example

```yaml
version: '3.8'

services:
  morphium-db:
    build:
      context: .
      dockerfile: Dockerfile.morphium
    ports:
      - "27017:27017"
    command: ["java", "-cp", "/app/morphium-6.0.1-SNAPSHOT.jar", "de.caluga.morphium.server.MorphiumServer", "--port", "27017", "--host", "0.0.0.0"]

  app:
    image: myapp:latest
    depends_on:
      - morphium-db
    environment:
      - MONGO_URL=mongodb://morphium-db:27017/appdb
```

Example `Dockerfile.morphium`:
```dockerfile
FROM openjdk:21-slim
WORKDIR /app
COPY target/morphium-6.0.1-SNAPSHOT.jar /app/
EXPOSE 27017
```

### Building and Running MorphiumServer

**From Source:**
```bash
# Clone Morphium repository
git clone https://github.com/sboesebeck/morphium.git
cd morphium

# Build with Maven
mvn clean package -DskipTests

# Run the server (note: default port is 17017, not 27017)
java -cp target/morphium-6.0.1-SNAPSHOT.jar de.caluga.morphium.server.MorphiumServer --port 27017 --host 0.0.0.0
```

**Using Maven Dependency:**
```xml
<!-- Add to your pom.xml -->
<dependency>
    <groupId>de.caluga</groupId>
    <artifactId>morphium</artifactId>
    <version>6.0.1-SNAPSHOT</version>
</dependency>
```

Then run programmatically:
```java
public static void main(String[] args) throws Exception {
    de.caluga.morphium.server.MorphiumServer.main(new String[]{"--port", "27017", "--host", "0.0.0.0"});
}
```

## See Also

- [Messaging Implementations](messaging-implementations.md) - Messaging patterns with InMemory driver
- [Migration v5 to v6](migration-v5-to-v6.md) - Upgrading to latest InMemory driver features
- [Configuration Reference](../configuration-reference.md) - Complete configuration options
- [Performance Guide](../performance-scalability-guide.md) - Optimization strategies

