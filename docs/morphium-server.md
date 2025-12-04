# MorphiumServer: Standalone MongoDB-Compatible Server

MorphiumServer is a standalone MongoDB wire protocol-compatible server built on the InMemoryDriver. It allows any MongoDB client (Java, Python, Node.js, Go, etc.) to connect and interact with an in-memory database. **Important:** MorphiumServer can be run as a standalone application from a dedicated executable JAR, or used programmatically as part of a Java application.

## Key Features

- ✅ **MongoDB Wire Protocol Compatible** - Works with any MongoDB client library
- ✅ **Multi-Language Support** - Connect from Java, Python, Node.js, Go, C#, etc.
- ✅ **Fast Startup** - Starts in ~100-500ms vs ~2-5 seconds for MongoDB
- ✅ **Lightweight** - ~50-100MB RAM vs ~500MB-1GB for MongoDB
- ✅ **No Installation** - Pure Java, runs anywhere
- ✅ **Perfect for CI/CD** - No Docker or MongoDB installation required
- ✅ **Integration Testing** - Test multi-language microservices together

## Quick Start

### Running from Command Line

After building the project, you can run the server directly using the `server-cli` JAR.

```bash
# Build the project first if you haven't
mvn clean package -DskipTests

# Run MorphiumServer with default settings (port 17017)
java -jar target/morphium-*-server-cli.jar

# Run on a different port
java -jar target/morphium-*-server-cli.jar --port 27017
```

### Running Programmatically

```java
import de.caluga.morphium.server.MorphiumServer;

public class MyApp {
    public static void main(String[] args) throws Exception {
        // Start embedded MongoDB-compatible server
        MorphiumServer server = new MorphiumServer(27017, "0.0.0.0", 100, 10);
        server.start();

        System.out.println("MorphiumServer running on port 27017");

        // Keep running
        while (true) {
            Thread.sleep(1000);
        }
    }
}
```

## Configuration

### Command Line Arguments

```bash
-p, --port <number>           # Server port (default: 17017)
-h, --host <address>          # Bind address (default: localhost)
-mt, --maxThreads <number>    # Max thread pool size (default: 1000)
-mint, --minThreads <number>  # Min thread pool size (default: 10)
-c, --compressor <type>       # Compression: snappy, zstd, zlib, none
-rs, --replicaset <name> <hosts>  # Replica set mode (experimental)
```

### Constructor Options

```java
// Full constructor
MorphiumServer server = new MorphiumServer(
    int port,           // Server port
    String host,        // Bind address
    int maxThreads,     // Maximum threads
    int minThreads      // Minimum threads
);

// Default constructor (port 17017, localhost, 100/10 threads)
MorphiumServer server = new MorphiumServer();
```

## Connecting Clients

### Java (Morphium)

```java
MorphiumConfig cfg = new MorphiumConfig();
cfg.connectionSettings()
   .setDatabase("mydb")
   .addHost("localhost", 27017);
cfg.driverSettings()
   .setDriverName("SingleMongoConnectDriver");

Morphium morphium = new Morphium(cfg);
```

### Python (PyMongo)

```python
from pymongo import MongoClient

client = MongoClient('mongodb://localhost:27017/')
db = client.mydb
collection = db.users

# Works like regular MongoDB!
collection.insert_one({'name': 'Alice', 'age': 30})
user = collection.find_one({'name': 'Alice'})
print(user)
```

### Node.js (mongodb driver)

```javascript
const { MongoClient } = require('mongodb');

async function main() {
    const client = new MongoClient('mongodb://localhost:27017');
    await client.connect();

    const db = client.db('mydb');
    const collection = db.collection('users');

    await collection.insertOne({ name: 'Bob', age: 25 });
    const user = await collection.findOne({ name: 'Bob' });
    console.log(user);
}

main();
```

### Go (mongo-driver)

```go
package main

import (
    "context"
    "go.mongodb.org/mongo-driver/bson"
    "go.mongodb.org/mongo-driver/mongo"
    "go.mongodb.org/mongo-driver/mongo/options"
)

func main() {
    client, _ := mongo.Connect(
        context.TODO(),
        options.Client().ApplyURI("mongodb://localhost:27017"),
    )

    collection := client.Database("mydb").Collection("users")
    collection.InsertOne(context.TODO(), bson.D{{"name", "Charlie"}})
}
```

### MongoDB Shell

```bash
mongosh mongodb://localhost:27017/mydb

# Test it
> db.users.insertOne({name: "Alice", age: 30})
> db.users.find()
```

## Use Cases

### 1. CI/CD Pipelines

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
          java -jar target/morphium-*-server-cli.jar \
               --port 27017 --host 0.0.0.0 &
          sleep 2

      - name: Run Integration Tests
        run: npm test
        env:
          MONGO_URL: mongodb://localhost:27017
```

### 2. Integration Testing (Multi-Language)

```java
@BeforeAll
static void startServer() throws Exception {
    server = new MorphiumServer(27017, "0.0.0.0", 100, 10);
    server.start();
    Thread.sleep(500); // Wait for server to be ready
}

@Test
void testCrossLanguageCompatibility() throws Exception {
    // Insert from Java
    MorphiumConfig cfg = new MorphiumConfig();
    cfg.connectionSettings().setDatabase("test").addHost("localhost", 27017);
    cfg.driverSettings().setDriverName("SingleMongoConnectDriver");

    Morphium morphium = new Morphium(cfg);
    MyEntity entity = new MyEntity();
    entity.setName("test-entity");
    morphium.store(entity);
    morphium.close();

    // Verify from Python script
    ProcessBuilder pb = new ProcessBuilder("python3", "test_read.py");
    pb.environment().put("MONGO_URL", "mongodb://localhost:27017/test");
    Process p = pb.start();
    assertEquals(0, p.waitFor());
}

@AfterAll
static void stopServer() {
    server.terminate();
}
```

### 3. Microservices Development

```bash
# Terminal 1: Start MorphiumServer
java -jar target/morphium-*-server-cli.jar --port 27017

# Terminal 2: Start Node.js service
MONGO_URL=mongodb://localhost:27017 npm start

# Terminal 3: Start Python service
MONGO_URL=mongodb://localhost:27017 python app.py

# Terminal 4: Start Java service
MONGO_URL=mongodb://localhost:27017 ./gradlew run
```

### 4. Docker Deployment

**Dockerfile:**
```dockerfile
FROM openjdk:21-slim
WORKDIR /app

# Copy the executable server JAR
COPY target/morphium-*-server-cli.jar /app/morphium-server.jar

EXPOSE 27017

CMD ["java", "-jar", "/app/morphium-server.jar", \
     "--port", "27017", "--host", "0.0.0.0"]
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

  app:
    image: myapp:latest
    depends_on:
      - morphium-db
    environment:
      - MONGO_URL=mongodb://morphium-db:27017/appdb
```

**Build and Run:**
```bash
docker build -t morphium-server .
docker run -p 27017:27017 morphium-server

# Or use docker-compose
docker-compose up
```

## Performance

| Metric | MorphiumServer | MongoDB |
|--------|---------------|---------|
| Startup Time | ~100-500ms | ~2-5 seconds |
| Memory (baseline) | ~50-100MB | ~500MB-1GB |
| Inserts/sec | ~50,000 | Varies |
| Queries/sec | ~100,000 | Varies |
| Updates/sec | ~40,000 | Varies |
| Latency (localhost) | 1-5ms | 1-10ms |

## Monitoring

### Built-in Status Monitoring

**All Morphium messaging instances automatically include status monitoring** via the `morphium_status` topic. This works with MorphiumServer and any Morphium messaging setup.

Quick example:
```java
MorphiumMessaging sender = morphium.createMessaging();
sender.start();

// Query all instances for status
List<Msg> responses = sender.sendAndAwaitAnswers(
    new Msg(sender.getStatusInfoListenerName(), "status", "ALL"),
    5,      // Wait for up to 5 responses
    2000    // 2 second timeout
);

// Process JVM, messaging, and driver metrics
for (Msg response : responses) {
    Map<String, Object> stats = response.getMapValue();
    System.out.println("Instance: " + response.getSender());
    System.out.println("  Heap Used: " + stats.get("jvm.heap.used"));
    System.out.println("  Messages Processing: " + stats.get("messaging.processing"));
}
```

**For complete documentation on status monitoring**, including:
- All available metrics (JVM, messaging, driver)
- Query levels (PING, MESSAGING_ONLY, MORPHIUM_ONLY, ALL)
- Cross-language monitoring (Python, Node.js, etc.)
- Health checks and periodic monitoring
- Enable/disable controls

See the **[Messaging - Built-in Status Monitoring](./messaging.md#built-in-status-monitoring)** section.

### Connection Count

```java
MorphiumServer server = new MorphiumServer(27017, "localhost", 100, 10);
server.start();

// Get active connections
int connections = server.getConnectionCount();
System.out.println("Active connections: " + connections);
```

### Logging

```bash
# Debug logging with Logback
java -Dlogback.configurationFile=logback.xml \
     -cp morphium.jar de.caluga.morphium.server.MorphiumServer \
     --port 27017

# Simple logger
java -Dorg.slf4j.simpleLogger.defaultLogLevel=debug \
     -cp morphium.jar de.caluga.morphium.server.MorphiumServer \
     --port 27017
```

## Limitations

### Data Persistence
- ❌ **No Disk Storage** - All data in memory, lost on restart
- ❌ **No Recovery** - No WAL or recovery mechanism
- 💡 **Workaround** - Implement periodic export/import

### Scalability
- ❌ **Single Instance** - No sharding support
- ❌ **No Replica Sets** - Experimental support only
- ❌ **Memory Bound** - Dataset limited by available RAM

### Features
- ❌ **GridFS** - No file storage
- ❌ **Full-Text Search** - Limited $text support
- ❌ **Advanced Geospatial** - Basic queries only
- ❌ **Distributed Transactions** - Single instance only

### Security
- ❌ **No TLS/SSL** - Plain TCP only
- ❌ **No Authentication** - Not implemented
- 💡 **Workaround** - Use reverse proxy for TLS

## When NOT to Use

**Avoid for:**
- Production data requiring persistence
- Datasets exceeding available RAM (>16GB)
- High availability requirements
- TLS/SSL compliance requirements
- MongoDB Atlas features
- Advanced search/geospatial features

**Use Instead:**
- **Production**: Real MongoDB with persistence
- **Large Datasets**: MongoDB with disk storage
- **High Availability**: MongoDB replica sets
- **Cloud**: MongoDB Atlas

## Building from Source

```bash
git clone https://github.com/sboesebeck/morphium.git
cd morphium
mvn clean package -DskipTests

# This creates two important artifacts in target/:
# 1. morphium-X.Y.Z.jar (the standard library)
# 2. morphium-X.Y.Z-server-cli.jar (the executable server)

# Run the server:
java -jar target/morphium-*-server-cli.jar --port 27017
```

## Maven Dependency

```xml
<dependency>
    <groupId>de.caluga</groupId>
    <artifactId>morphium</artifactId>
    <version>6.0.4-SNAPSHOT</version>
</dependency>
```

Then start programmatically:
```java
public static void main(String[] args) throws Exception {
    // Option 1: Call main from the CLI class
    de.caluga.morphium.server.MorphiumServerCLI.main(
        new String[]{"--port", "27017", "--host", "0.0.0.0"}
    );

    // Option 2: Create instance directly
    MorphiumServer server = new MorphiumServer(27017, "0.0.0.0", 100, 10);
    server.start();
}
```

## Comparison: MorphiumServer vs InMemory Driver

| Feature | MorphiumServer | InMemory Driver |
|---------|---------------|-----------------|
| **Network Access** | Yes (wire protocol) | No (embedded) |
| **Multi-Language** | Yes | No (Java only) |
| **Use Case** | Integration tests, microservices | Unit tests |
| **Overhead** | Network latency | In-process |
| **Setup** | Start server | Config setting |
| **Isolation** | Process-level | Per-JVM |

**When to use each:**
- **InMemory Driver**: Java unit tests, embedded apps
- **MorphiumServer**: Integration tests, CI/CD, multi-language services

## See Also

- [InMemory Driver](./howtos/inmemory-driver.md) - Embedded driver for unit tests
- [Messaging](./messaging.md) - Messaging with MorphiumServer
- [Configuration Reference](./configuration-reference.md) - All configuration options
- [Architecture Overview](./architecture-overview.md) - How it works internally
