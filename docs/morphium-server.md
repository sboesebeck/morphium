# MorphiumServer: Standalone MongoDB Replacement

MorphiumServer runs the InMemoryDriver as a network service, providing a **MongoDB wire protocol compatible server** that any MongoDB client can connect to. It's part of the main Morphium project, not a separate application.

## Why Use MorphiumServer?

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

> **Important**: MorphiumServer is part of the main `morphium.jar` - there is no separate `morphium-server.jar`.

## Starting MorphiumServer

### From Command Line

**Basic Usage:**
```bash
# Build Morphium first
mvn clean package -DskipTests

# Run the server (default port is 17017)
java -cp target/morphium-6.0.1-SNAPSHOT.jar de.caluga.morphium.server.MorphiumServer --port 27017 --host 0.0.0.0
```

**With Custom Configuration:**
```bash
java -cp target/morphium-6.0.1-SNAPSHOT.jar de.caluga.morphium.server.MorphiumServer \
  --port 27017 \
  --host 0.0.0.0 \
  --maxThreads 100 \
  --minThreads 10
```

**With Compression:**
```bash
java -cp target/morphium-6.0.1-SNAPSHOT.jar de.caluga.morphium.server.MorphiumServer \
  --port 27017 \
  --host 0.0.0.0 \
  --compressor snappy
```

### Programmatically

**Embedded in Application:**
```java
public class MyApplication {
    public static void main(String[] args) throws Exception {
        // Start embedded database
        MorphiumServer dbServer = new MorphiumServer(27017, "localhost", 100, 10);
        dbServer.start();

        // Connect to it
        MorphiumConfig cfg = new MorphiumConfig();
        cfg.connectionSettings()
           .setDatabase("appdb")
           .addHost("localhost", 27017);
        cfg.driverSettings()
           .setDriverName("de.caluga.morphium.driver.wire.WireProtocolDriver");

        Morphium morphium = new Morphium(cfg);

        // Application logic here
        // Users don't need MongoDB installed!
    }
}
```

**In Test Setup:**
```java
@BeforeAll
static void startServer() throws Exception {
    // Start MorphiumServer programmatically
    server = new MorphiumServer(27017, "localhost", 100, 10);
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
    server.terminate();
}
```

## Configuration Options

### Command Line Arguments

```bash
-p, --port <number>           # Server port (default: 17017)
-h, --host <address>          # Bind address (default: localhost)
-mt, --maxThreads <number>    # Max concurrent threads (default: 1000)
-mint, --minThreads <number>  # Min concurrent threads (default: 10)
-c, --compressor <type>       # Compression: snappy, zstd, zlib, none (default: none)
-rs, --replicaset <name> <hosts>  # Replica set configuration (experimental)
```

### Constructor Options

```java
// Full constructor
MorphiumServer server = new MorphiumServer(
    int port,           // Server port
    String host,        // Bind address
    int maxThreads,     // Maximum thread pool size
    int minThreads      // Minimum thread pool size
);

// Default constructor (port 17017, localhost, 100 max threads, 10 min threads)
MorphiumServer server = new MorphiumServer();
```

## Connecting to MorphiumServer

MorphiumServer speaks the MongoDB wire protocol, so **any MongoDB driver** can connect to it.

### From Java (Morphium)

```java
MorphiumConfig cfg = new MorphiumConfig();
cfg.connectionSettings()
   .setDatabase("mydb")
   .addHost("localhost", 27017);
cfg.driverSettings()
   .setDriverName("de.caluga.morphium.driver.wire.WireProtocolDriver");

Morphium morphium = new Morphium(cfg);
```

### From MongoDB Shell

```bash
# Use mongosh or mongo CLI
mongosh mongodb://localhost:27017/mydb

# Test it
> db.test.insertOne({name: "hello"})
> db.test.find()
```

### From Python (PyMongo)

```python
from pymongo import MongoClient

client = MongoClient('mongodb://localhost:27017/')
db = client.mydb
collection = db.mycollection

# Works like regular MongoDB!
collection.insert_one({'name': 'test'})
result = collection.find_one({'name': 'test'})
print(result)
```

### From Node.js

```javascript
const { MongoClient } = require('mongodb');

const client = new MongoClient('mongodb://localhost:27017');
await client.connect();

const db = client.db('mydb');
const collection = db.collection('mycollection');

await collection.insertOne({ name: 'test' });
const doc = await collection.findOne({ name: 'test' });
console.log(doc);
```

### From Go

```go
import (
    "context"
    "go.mongodb.org/mongo-driver/mongo"
    "go.mongodb.org/mongo-driver/mongo/options"
)

client, err := mongo.Connect(context.TODO(), options.Client().ApplyURI("mongodb://localhost:27017"))
if err != nil {
    panic(err)
}

collection := client.Database("mydb").Collection("mycollection")
_, err = collection.InsertOne(context.TODO(), bson.D{{"name", "test"}})
```

## Use Cases & Examples

### 1. Microservices Development

```bash
# Terminal 1: Start MorphiumServer
java -cp target/morphium-6.0.1-SNAPSHOT.jar de.caluga.morphium.server.MorphiumServer --port 27017

# Terminal 2: Start Service A (Node.js)
MONGO_URL=mongodb://localhost:27017 npm start

# Terminal 3: Start Service B (Python)
MONGO_URL=mongodb://localhost:27017 python app.py

# Terminal 4: Start Service C (Java)
MONGO_URL=mongodb://localhost:27017 ./gradlew run
```

### 2. CI/CD Pipeline

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
          java -cp target/morphium-6.0.1-SNAPSHOT.jar de.caluga.morphium.server.MorphiumServer --port 27017 --host 0.0.0.0 &
          sleep 2

      - name: Run Tests
        run: |
          export MONGO_URL=mongodb://localhost:27017
          npm test

      # No Docker, no MongoDB service container needed!
```

### 3. Docker Deployment

**Dockerfile:**
```dockerfile
FROM openjdk:21-slim
WORKDIR /app

# Copy the Morphium JAR
COPY target/morphium-6.0.1-SNAPSHOT.jar /app/

EXPOSE 27017

# Run MorphiumServer
CMD ["java", "-cp", "/app/morphium-6.0.1-SNAPSHOT.jar", "de.caluga.morphium.server.MorphiumServer", "--port", "27017", "--host", "0.0.0.0"]
```

**Docker Compose:**
```yaml
version: '3.8'

services:
  morphium-db:
    build:
      context: .
      dockerfile: Dockerfile
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

**Build and Run:**
```bash
# Build the Docker image
docker build -t morphium-server .

# Run the container
docker run -p 27017:27017 morphium-server

# Or use Docker Compose
docker-compose up
```

### 4. Integration Testing with Multiple Languages

```java
// Java test
@BeforeAll
static void startServer() throws Exception {
    server = new MorphiumServer(27017, "0.0.0.0", 100, 10);
    server.start();
}

@Test
void testCrossLanguageCompatibility() throws Exception {
    // Insert from Java
    MorphiumConfig cfg = new MorphiumConfig();
    cfg.connectionSettings().setDatabase("test").addHost("localhost", 27017);
    cfg.driverSettings().setDriverName("de.caluga.morphium.driver.wire.WireProtocolDriver");

    try (Morphium morphium = new Morphium(cfg)) {
        MyEntity entity = new MyEntity();
        entity.setName("test");
        morphium.store(entity);
    }

    // Read from Python (via ProcessBuilder or testcontainers)
    // Read from Node.js
    // Verify all clients see the same data
}

@AfterAll
static void stopServer() {
    server.terminate();
}
```

## Performance Characteristics

### Startup Time
- **MorphiumServer**: ~100-500ms
- **MongoDB**: ~2-5 seconds

### Memory Usage
- **MorphiumServer**: ~50-100MB (JVM baseline)
- **MongoDB**: ~500MB-1GB (baseline)

### Throughput (Single Instance)
- **Inserts**: ~50,000 ops/sec
- **Queries**: ~100,000 ops/sec
- **Updates**: ~40,000 ops/sec

### Latency
- **In-process**: <1ms (embedded mode)
- **Network (localhost)**: 1-5ms

## Monitoring & Management

### Connection Count

```java
MorphiumServer server = new MorphiumServer(27017, "localhost", 100, 10);
server.start();

// Get active connection count
int connections = server.getConnectionCount();
System.out.println("Active connections: " + connections);
```

### Logging

**Enable Debug Logging:**
```bash
# Using Logback configuration
java -Dlogback.configurationFile=logback.xml \
     -cp morphium-6.0.1-SNAPSHOT.jar \
     de.caluga.morphium.server.MorphiumServer --port 27017

# Using SLF4J simple logger
java -Dorg.slf4j.simpleLogger.defaultLogLevel=debug \
     -cp morphium-6.0.1-SNAPSHOT.jar \
     de.caluga.morphium.server.MorphiumServer --port 27017
```

**Example logback.xml:**
```xml
<configuration>
    <appender name="STDOUT" class="ch.qos.logback.core.ConsoleAppender">
        <encoder>
            <pattern>%d{HH:mm:ss.SSS} [%thread] %-5level %logger{36} - %msg%n</pattern>
        </encoder>
    </appender>

    <logger name="de.caluga.morphium.server" level="DEBUG"/>
    <root level="INFO">
        <appender-ref ref="STDOUT" />
    </root>
</configuration>
```

## Limitations

### Data Persistence
- ❌ **No Disk Persistence**: Data stored in memory only
- ❌ **No Recovery**: Data lost on restart
- 💡 **Workaround**: Implement export/import scripts for data backup

### Scalability
- ❌ **No Sharding**: Single-instance only
- ❌ **No Replica Sets**: No high availability (experimental replicaset support exists)
- ❌ **Memory Bound**: Dataset limited by available RAM

### Advanced Features
- ❌ **GridFS**: No file storage
- ❌ **Full-Text Search**: Limited support
- ❌ **Geospatial**: Basic queries only
- ❌ **Transactions**: Single-instance only (no distributed)

### Security
- ❌ **No TLS/SSL**: Plain TCP only
- ❌ **Limited Auth**: No authentication implemented
- 💡 **Workaround**: Use reverse proxy (nginx, HAProxy) for TLS termination

## When NOT to Use MorphiumServer

**Avoid in Production if:**
- You need data persistence across restarts
- Dataset exceeds available RAM (>16GB)
- You need high availability/failover
- You require MongoDB Atlas features
- Security compliance requires TLS/SSL
- You need advanced geospatial features
- You need full-text search capabilities

**Better Alternatives:**
- **Production Data**: Use real MongoDB
- **Large Datasets**: Use MongoDB with disk storage
- **High Availability**: Use MongoDB replica sets
- **Advanced Features**: Use MongoDB Enterprise
- **Cloud Deployment**: Use MongoDB Atlas

## Building from Source

```bash
# Clone the repository
git clone https://github.com/sboesebeck/morphium.git
cd morphium

# Build with Maven
mvn clean package -DskipTests

# The MorphiumServer class is included in the main JAR
ls -lh target/morphium-6.0.1-SNAPSHOT.jar

# Run it
java -cp target/morphium-6.0.1-SNAPSHOT.jar de.caluga.morphium.server.MorphiumServer --port 27017 --host 0.0.0.0
```

## Using as Maven Dependency

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
    // Option 1: Call main method
    de.caluga.morphium.server.MorphiumServer.main(
        new String[]{"--port", "27017", "--host", "0.0.0.0"}
    );

    // Option 2: Create instance
    MorphiumServer server = new MorphiumServer(27017, "0.0.0.0", 100, 10);
    server.start();

    // Keep running
    while (true) {
        Thread.sleep(1000);
    }
}
```

## Comparison with MongoDB

| Feature | MorphiumServer | MongoDB |
|---------|---------------|---------|
| Startup Time | ~100-500ms | ~2-5 seconds |
| Memory Usage | ~50-100MB | ~500MB-1GB |
| Installation | None (pure Java) | Required |
| Data Persistence | No | Yes |
| Replica Sets | No (experimental) | Yes |
| Sharding | No | Yes |
| Wire Protocol | Compatible | Native |
| TLS/SSL | No | Yes |
| Authentication | No | Yes |
| GridFS | No | Yes |
| Change Streams | Yes | Yes |
| Aggregation | Basic | Full |
| Atlas Support | No | Yes |

## See Also

- [InMemory Driver](./inmemory-driver.md) - Embedded in-process driver usage
- [Messaging](./messaging.md) - Messaging with MorphiumServer
- [Configuration Reference](./configuration-reference.md) - Complete configuration options
- [Architecture Overview](./architecture-overview.md) - How MorphiumServer works internally
