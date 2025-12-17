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
You can configure the MorphiumServer using the following command-line arguments:

| Argument | Description | Default |
|---|---|---|
| `-p`, `--port <port>` | Port to listen on. | `17017` |
| `-b`, `--bind <host>` | Host to bind to. | `localhost` |
| `-mt`, `--maxThreads <threads>` | Maximum number of threads for handling client connections. | `1000` |
| `-mint`, `--minThreads <threads>` | Minimum number of threads to keep in the pool. | `10` |
| `-c`, `--compressor <type>` | Compressor to use for the wire protocol. Can be `none`, `snappy`, `zstd`, or `zlib`. | `none` |
| `--rs-name <name>` | Name of the replica set. | |
| `--rs-seed <hosts>` | Comma-separated list of hosts to seed the replica set. The first host in the list will have the highest priority. | |
| `--ssl`, `--tls` | Enable SSL/TLS encrypted connections. | disabled |
| `--sslKeystore <path>` | Path to JKS or PKCS12 keystore file containing server certificate. | |
| `--sslKeystorePassword <pw>` | Password for the keystore. | |
| `-d`, `--dump-dir <path>` | Directory for periodic database dumps. Enables persistence. | |
| `--dump-interval <seconds>` | Interval between periodic dumps. 0 = only dump on shutdown. | `0` |
| `-h`, `--help` | Print this help message and exit. | |

Example:
```bash
java -jar target/morphium-*-server-cli.jar -p 27018 -b 0.0.0.0 --rs-name my-rs --rs-seed host1:27017,host2:27018
```

### Replica Set Behavior (experimental)

MorphiumServer now performs a lightweight initial sync whenever you start an additional member with the same `--rs-name` / `--rs-seed`:

- The first node that starts without detecting peers becomes primary immediately.
- Any later node that can reach an existing peer demotes itself to secondary, runs an initial sync from the detected primary (or highest-priority reachable host), and only participates in elections after the sync finishes.
- Elections and automatic failover continue to respect the configured host priorities, but a node will not promote itself until it completed the initial copy of data.

Practical tips:

1. Always include all hosts in `--rs-seed` so nodes can find a sync source.
2. Start at least one node, write the test data you need, then bring additional members online—they will clone the existing data automatically.
3. Keep in mind that this is still meant for testing: persistence and durability are unchanged.

### Persistence (Periodic Snapshots)

MorphiumServer can periodically dump all databases to disk and restore them on startup. This provides basic persistence for development and testing scenarios.

**How it works:**
- On startup: If dump files exist in the configured directory, they are automatically restored
- During runtime: If `--dump-interval` is set, databases are dumped periodically
- On shutdown: A final dump is performed to capture all changes

**Quick Start with Persistence:**

```bash
# Start with persistence - dumps every 5 minutes
java -jar target/morphium-*-server-cli.jar -p 27017 \
  --dump-dir /var/morphium/data --dump-interval 300

# Start with persistence - dump only on shutdown
java -jar target/morphium-*-server-cli.jar -p 27017 \
  --dump-dir /var/morphium/data
```

**Programmatic Configuration:**
```java
import de.caluga.morphium.server.MorphiumServer;
import java.io.File;

MorphiumServer server = new MorphiumServer(27017, "localhost", 100, 10);

// Configure persistence
server.setDumpDirectory(new File("/var/morphium/data"));
server.setDumpIntervalMs(300000); // 5 minutes

// Restore previous state before starting
try {
    int restored = server.restoreFromDump();
    System.out.println("Restored " + restored + " databases");
} catch (Exception e) {
    System.out.println("Starting fresh: " + e.getMessage());
}

server.start();

// Manual dump if needed
server.dumpNow();
```

**Dump File Format:**
- Each database is saved as `<dbname>.morphium.gz` (gzip-compressed JSON)
- Files can be inspected with `zcat <file>.morphium.gz | jq .`

**Limitations:**
- Not a real-time persistence solution (no write-ahead log)
- Data between dump intervals may be lost on crash
- Suitable for development/testing, not production

### SSL/TLS Configuration

MorphiumServer supports SSL/TLS encrypted connections for secure communication.

**Quick Start with SSL:**

1. Generate a self-signed certificate:
```bash
keytool -genkeypair -alias morphium -keyalg RSA -keysize 2048 \
  -validity 365 -keystore server.jks -storepass changeit \
  -dname "CN=localhost"
```

2. Start the server with SSL enabled:
```bash
java -jar target/morphium-*-server-cli.jar -p 27018 \
  --ssl --sslKeystore server.jks --sslKeystorePassword changeit
```

3. Connect with mongosh:
```bash
# For self-signed certificates
mongosh "mongodb://localhost:27018" --tls --tlsAllowInvalidCertificates

# With proper certificate verification (export cert first)
keytool -exportcert -alias morphium -keystore server.jks \
  -storepass changeit -rfc -file server-cert.pem
mongosh "mongodb://localhost:27018" --tls --tlsCAFile server-cert.pem
```

**Programmatic SSL Configuration:**
```java
import de.caluga.morphium.server.MorphiumServer;
import de.caluga.morphium.driver.wire.SslHelper;

MorphiumServer server = new MorphiumServer(27018, "localhost", 100, 10);

// Load keystore and enable SSL
SSLContext sslContext = SslHelper.createServerSslContext(
    "server.jks", "changeit"
);
server.setSslContext(sslContext);
server.setSslEnabled(true);

server.start();
```

**SSL with Docker:**
```dockerfile
FROM openjdk:21-slim
WORKDIR /app

COPY target/morphium-*-server-cli.jar /app/morphium-server.jar
COPY server.jks /app/server.jks

EXPOSE 27018

CMD ["java", "-jar", "/app/morphium-server.jar", \
     "--port", "27018", "--host", "0.0.0.0", \
     "--ssl", "--sslKeystore", "/app/server.jks", \
     "--sslKeystorePassword", "changeit"]
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

## Supported Admin Commands

MorphiumServer implements the following MongoDB admin commands:

| Command | Description |
|---------|-------------|
| `ping` | Basic connectivity test |
| `hello` / `isMaster` / `ismaster` | Server status and topology information |
| `listDatabases` | List all databases with sizes |
| `buildInfo` | Server version information |
| `getCmdLineOpts` | Command line options |
| `getParameter` | Server parameters |
| `getLog` | Server logs |
| `replSetStepDown` | Step down from primary (for replica sets) |
| `startSession` / `endSessions` / `refreshSessions` | Session management |
| `getMore` | Cursor iteration for both regular queries and change streams |

### Standalone Server Behavior

When running MorphiumServer as a standalone server (without replica set configuration):

- The server always reports itself as primary (`isWritablePrimary: true`)
- `replSetStepDown` commands are acknowledged but the server immediately becomes primary again
- This ensures compatibility with clients and tests that issue replica set commands

### Change Stream Support

MorphiumServer fully supports change streams for real-time notifications:

- **Collection-level watches**: Watch changes on a specific collection
- **Database-level watches**: Watch all collections in a database
- **Cluster-level watches**: Watch all databases

Example with mongosh:
```javascript
// Watch a collection
db.users.watch().on('change', console.log);

// Watch entire database
db.watch().on('change', console.log);
```

## Limitations

### Data Persistence
- ✅ **Periodic Snapshots** - Dump/restore to disk (since v6.1.0)
- ❌ **No Real-time Persistence** - No WAL or journaling
- ❌ **Crash Risk** - Data between dumps may be lost on crash
- 💡 **Tip** - Use short dump intervals for important data

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
- ✅ **TLS/SSL Supported** - Encrypted connections available (since v6.1.0)
- ❌ **No Authentication** - Not implemented yet
- 💡 **Workaround** - Use reverse proxy for authentication

## When NOT to Use

**Avoid for:**
- Production data requiring persistence
- Datasets exceeding available RAM (>16GB)
- High availability requirements
- Authentication requirements (not yet implemented)
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
