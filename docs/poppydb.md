# PoppyDB: Standalone MongoDB-Compatible Server

PoppyDB is a standalone MongoDB wire protocol-compatible server built on the InMemoryDriver. Introduced in its mature form with **Morphium 6.1**, it allows any MongoDB client (Java, Python, Node.js, Go, etc.) to connect and interact with an in-memory database as a true **drop-in replacement** for MongoDB during development and testing.

**Important:** PoppyDB can be run as a standalone application from a dedicated executable JAR, or used programmatically as part of a Java application.

## Key Features

- ✅ **MongoDB Wire Protocol Compatible** - Works with any MongoDB client library
- ✅ **Multi-Language Support** - Connect from Java, Python, Node.js, Go, C#, etc.
- ✅ **3x Faster Than MongoDB** - Insert 0.74ms vs 4.48ms, Find 0.45ms vs 1.95ms, Update 0.66ms vs 5.19ms (local benchmarks)
- ✅ **Fast Startup** - Starts in ~100-500ms vs ~2-5 seconds for MongoDB
- ✅ **Lightweight** - ~50-100MB RAM vs ~500MB-1GB for MongoDB
- ✅ **No Installation** - Pure Java, runs anywhere
- ✅ **Perfect for CI/CD** - No Docker or MongoDB installation required
- ✅ **Integration Testing** - Test multi-language microservices together
- ✅ **Opt-in Authentication & TLS** - Real SCRAM-SHA-1/-256 auth (`--auth`) and SSL/TLS encrypted connections (`--ssl`)

## Quick Start

### Running from Command Line

After building the project, you can run the server directly using the PoppyDB CLI JAR.

```bash
# Build the project first if you haven't
mvn clean package -pl poppydb -am -Dmaven.test.skip=true

# Run PoppyDB with default settings (port 17017)
java -jar poppydb/target/poppydb-6.2.0-SNAPSHOT-cli.jar

# Run on a different port
java -jar poppydb/target/poppydb-6.2.0-SNAPSHOT-cli.jar --port 27017
```

### Running Programmatically

```java
import de.caluga.poppydb.PoppyDB;

public class MyApp {
    public static void main(String[] args) throws Exception {
        // Start embedded MongoDB-compatible server
        PoppyDB server = new PoppyDB(27017, "0.0.0.0", 100, 10);
        server.start();

        System.out.println("PoppyDB running on port 27017");

        // Keep running
        while (true) {
            Thread.sleep(1000);
        }
    }
}
```

## Configuration

### Command Line Arguments
You can configure the PoppyDB using the following command-line arguments:

| Argument | Description | Default |
|---|---|---|
| `-p`, `--port <port>` | Port to listen on. | `17017` |
| `-b`, `--bind <host>` | Host to bind to. | `localhost` |
| `--log-level <level>` | Log verbosity: `ERROR`, `WARN`, `INFO`, `DEBUG` or `TRACE`. See [Logging](#logging). | `INFO` |
| `--memory-warn <percent>` | Log a warning when heap occupancy crosses this percentage (100 = off). See [Memory Watermark](#memory-watermark). | `75` |
| `--memory-reject <percent>` | Reject document-creating writes above this heap percentage (100 = off). See [Memory Watermark](#memory-watermark). | `90` |
| `-mt`, `--maxThreads <threads>` | Maximum number of threads for handling client connections. | `1000` |
| `-mint`, `--minThreads <threads>` | Minimum number of threads to keep in the pool. | `10` |
| `-c`, `--compressor <type>` | Compressor to use for the wire protocol. Can be `none`, `snappy`, `zstd`, or `zlib`. | `none` |
| `--rs-name <name>` | Name of the replica set. | |
| `--rs-seed <hosts>` | Comma-separated list of hosts to seed the replica set. The first host in the list will have the highest priority. | |
| `--ssl`, `--tls` | Enable SSL/TLS encrypted connections. | disabled |
| `--sslKeystore <path>` | Path to JKS or PKCS12 keystore file containing server certificate. | |
| `--sslKeystorePassword <pw>` | Password for the keystore. | |
| `--auth` | Require SCRAM authentication (SCRAM-SHA-1 / SCRAM-SHA-256). Unauthenticated connections may only run the handshake/SASL/ping commands. | disabled |
| `--rootUser <name>` | Initial admin user, created at startup if absent. Required for a fresh `--auth` server — there is no localhost exception. | |
| `--rootPassword <pw>` | Password for the initial admin user. | |
| `-d`, `--dump-dir <path>` | Directory for periodic database dumps. Enables persistence. | |
| `--dump-interval <seconds>` | Interval between periodic dumps. 0 = only dump on shutdown. | `0` |
| `-h`, `--help` | Print this help message and exit. | |

Example:
```bash
java -jar poppydb/target/poppydb-6.2.0-SNAPSHOT-cli.jar -p 27018 -b 0.0.0.0 --rs-name my-rs --rs-seed host1:27017,host2:27018
```

### Replica Set Behavior (experimental)

PoppyDB now performs a lightweight initial sync whenever you start an additional member with the same `--rs-name` / `--rs-seed`:

- The first node that starts without detecting peers becomes primary immediately.
- Any later node that can reach an existing peer demotes itself to secondary, runs an initial sync from the detected primary (or highest-priority reachable host), and only participates in elections after the sync finishes.
- Elections and automatic failover continue to respect the configured host priorities, but a node will not promote itself until it completed the initial copy of data.

Practical tips:

1. Always include all hosts in `--rs-seed` so nodes can find a sync source.
2. Start at least one node, write the test data you need, then bring additional members online—they will clone the existing data automatically.
3. Keep in mind that this is still meant for testing: persistence and durability are unchanged.

### Programmatic Replica Set Configuration

You can configure a replica set programmatically using the `configureReplicaSet()` method:

```java
PoppyDB primary = new PoppyDB(27017, "localhost", 100, 10);

// Configure as a 2-node replica set with host priorities
var hosts = List.of("localhost:27017", "localhost:27018");
var priorities = Map.of("localhost:27017", 300, "localhost:27018", 100);
primary.configureReplicaSet("myReplicaSet", hosts, priorities);

primary.start();

// Start secondary later
PoppyDB secondary = new PoppyDB(27018, "localhost", 100, 10);
secondary.configureReplicaSet("myReplicaSet", hosts, priorities);
secondary.start();
```

**Write Concern Behavior with Partial Replica Sets:**

When using entities with `@WriteSafety(level = SafetyLevel.WAIT_FOR_ALL_SLAVES)` or explicit write concerns with `w > 1`, PoppyDB handles the case where not all secondaries are available:

- If no secondaries have connected yet, the server returns a `writeConcernError` after a brief grace period (100ms) instead of waiting for the full `wtimeout`
- This allows you to store documents on the primary before starting secondary nodes
- Once secondaries connect, writes will properly wait for replication acknowledgment

This is particularly useful for testing scenarios where you want to:
1. Start a primary node
2. Store initial test data
3. Start secondary nodes and verify data replication

### Index replication

Secondaries replicate **index definitions** as well as documents (since 6.3.0, #258): the initial
sync copies the primary's `listIndexes` output after the data snapshot, and a periodic diff (every
30s) converges afterwards — indexes created on the primary are created on the secondary with their
full options (unique, TTL, partial, sparse, ...), and indexes dropped on the primary are dropped
locally (the `_id` index is never touched). The periodic diff also covers changes the secondary
missed while disconnected. Change streams carry no index DDL, so index changes can lag up to one
diff interval behind; document replication is unaffected. After a failover, a promoted secondary
therefore enforces the same unique constraints and expires TTL documents like the old primary did.

### Replication buffer sizing

A secondary that falls behind (network partition, GC pause, slow disk) resumes from its
last-applied position once it reconnects — but only if the primary's replay buffer still covers
the gap. If too much has been written while the secondary was disconnected, the primary signals
"resume window lost" and the secondary falls back to a full initial re-sync instead of a cheap
incremental resume.

The rule of thumb: **the replay buffer must be sized to cover the sustainable write rate times the
worst-case sync/reconnect duration** — `buffer size >= write_rate × sync_duration`. A buffer sized
for average load will still force a full re-sync under a burst or a slow reconnect, and if
re-syncs then start overlapping with new bursts faster than they can complete, replication can
enter a state where it never catches up (each re-sync itself takes time, during which more writes
accumulate). This is exactly what `ReplicationManager` now watches for: if a resync happens more
than once within a 10-minute window, it logs a WARN —
*"replication cannot keep up — buffer sizes bound write rate × sync duration"* — because a single
isolated re-sync is a normal recovery from a transient outage, but back-to-back re-syncs are a sign
the buffer (or the sync speed) can no longer absorb the actual write rate.

Use `ReplicationManager.getStats()` (nested under `PoppyDB.getStats()`'s `"replication"` key) to
watch this before it becomes an incident:

| Key | Meaning |
|-----|---------|
| `resyncCount` | How many times this secondary has fallen back to a full re-sync. |
| `lastAppliedSequence` | The secondary's own last-applied change-stream sequence. |
| `eventQueueSize` / `eventQueueCapacity` | Current depth / configured bound of the secondary's local replication event queue — a queue that is consistently near capacity means the batch processor cannot keep up with incoming events. |
| `replicationLagEvents` | The primary's sequence at the most recent watch registration minus `lastAppliedSequence` — an approximation of how many events behind the secondary was at reconnect time. |
| `watchGeneration` | Bumped on every successful watch (re-)registration; a fast-climbing counter indicates a flapping connection. |

### Persistence (Periodic Snapshots)

PoppyDB can periodically dump all databases to disk and restore them on startup. This provides basic persistence for development and testing scenarios.

**How it works:**
- On startup: If dump files exist in the configured directory, they are automatically restored.
- During runtime: If `--dump-interval` is set, databases are dumped periodically.
- On shutdown: A final dump is performed to capture all changes.

**Quick Start with Persistence:**

```bash
# Start with persistence - dumps every 5 minutes
java -jar poppydb/target/poppydb-6.2.0-SNAPSHOT-cli.jar -p 27017 \
  --dump-dir /var/morphium/data --dump-interval 300

# Start with persistence - dump only on shutdown
java -jar poppydb/target/poppydb-6.2.0-SNAPSHOT-cli.jar -p 27017 \
  --dump-dir /var/morphium/data
```

**Manual Snapshots:**
You can trigger a manual dump at any time using the `dumpNow()` method programmatically (see below).

**Programmatic Configuration:**
```java
import de.caluga.poppydb.PoppyDB;
import java.io.File;

PoppyDB server = new PoppyDB(27017, "localhost", 100, 10);

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

### Memory Watermark

An in-memory store dies of OOM when producers outrun consumers — and a replica set dies
*completely*, because replication copies the data volume to every node. PoppyDB therefore
guards its heap with two watermarks (percent of the JVM's max heap):

- **Warn** (default 75%): a WARN log line when heap occupancy crosses the threshold
  (logged once per crossing, re-arms 5% below).
- **Reject** (default 90%): document-creating writes (`insert`, replace-style `store`) are
  refused with a mongod-shaped `ExceededMemoryLimit` error (code 146). **Updates, deletes
  and TTL expiry keep working** — the drain paths (messaging processed-marks, lock
  releases, cleanup) must stay open so the system can get back under the watermark
  instead of being stuck above it.

Replication applies and the initial sync bypass the guard: the primary is the gate, and a
secondary refusing to apply what the primary accepted would silently diverge. All nodes of
a replica set stop accepting new data at the same watermark instead of failing together.

Clients receive the rejection as a write error and should treat it as retryable
backpressure. The current state is visible in `db.serverStatus().memoryWatermark`
(`heapUsedPercent`, thresholds, warn state). The gauge is JVM heap occupancy — garbage
inflates it, so rejection near the limit errs on the conservative side, which beats an
OOM kill.

```bash
# defaults: warn at 75%, reject at 90%
java -jar poppydb-cli.jar --port 27017

# tighter bound, e.g. when sharing the JVM host with other services
java -Xmx2g -jar poppydb-cli.jar --port 27017 --memory-warn 60 --memory-reject 75

# disable entirely (test setups that intentionally fill the heap)
java -jar poppydb-cli.jar --port 27017 --memory-warn 100 --memory-reject 100
```

Programmatic: `poppyDb.setMemoryWatermarks(warnPercent, rejectPercent)` or
`InMemoryDriver.setMemoryWatermarks(...)` for embedded use.

### SSL/TLS Configuration

PoppyDB supports SSL/TLS encrypted connections for secure communication.

**Quick Start with SSL:**

1. Generate a self-signed certificate:
```bash
keytool -genkeypair -alias morphium -keyalg RSA -keysize 2048 \
  -validity 365 -keystore server.jks -storepass changeit \
  -dname "CN=localhost"
```

2. Start the server with SSL enabled:
```bash
java -jar poppydb/target/poppydb-6.2.0-SNAPSHOT-cli.jar -p 27018 \
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
import de.caluga.poppydb.PoppyDB;
import de.caluga.morphium.driver.wire.SslHelper;

PoppyDB server = new PoppyDB(27018, "localhost", 100, 10);

// Load keystore and enable SSL
SSLContext sslContext = SslHelper.createServerSslContext(
    "server.jks", "changeit"
);
server.setSslContext(sslContext);
server.setSslEnabled(true);

server.start();
```

### Authentication (`--auth`)

PoppyDB supports real SCRAM authentication (SCRAM-SHA-1 and SCRAM-SHA-256, RFC 5802/7677) with
users stored mongod-shaped in `admin.system.users`. Enforcement is **strictly opt-in**: without
`--auth` the server stays completely open (unchanged test/dev behavior). With `--auth`, a
connection may only run the handshake, SASL, `logout`, `ping` and `buildInfo` commands until it
completes a SCRAM exchange — everything else is rejected with code 13 `Unauthorized`.

**Quick Start with authentication:**

```bash
# There is no localhost exception - configure the initial admin user at startup:
java -jar poppydb-cli.jar -p 27018 \
  --auth --rootUser admin --rootPassword s3cr3t
```

```bash
# Unauthenticated access is rejected:
mongosh "mongodb://localhost:27018/test" --eval 'db.coll.find()'
# MongoServerError: command find requires authentication

# Standard clients authenticate as against real MongoDB:
mongosh "mongodb://admin:s3cr3t@localhost:27018/test?authSource=admin"

# Create additional users the normal way:
mongosh "mongodb://admin:s3cr3t@localhost:27018/admin?authSource=admin" \
  --eval 'db.createUser({user: "app", pwd: "apppass", roles: []})'
```

Morphium clients simply set credentials as usual (`authDb`/user/password in the connection
settings) — the driver performs the SCRAM handshake automatically on connect.

**Combine with TLS** for encrypted, authenticated deployments:
```bash
java -jar poppydb-cli.jar -p 27018 --auth --rootUser admin --rootPassword s3cr3t \
  --ssl --sslKeystore server.jks --sslKeystorePassword changeit
```

**Programmatic configuration:**
```java
PoppyDB server = new PoppyDB(27018, "localhost", 100, 10);
server.setAuthRequired(true);
server.setRootUser("admin", "s3cr3t");   // created at startup if absent
server.start();
```

**Current limitations:**
- Authorization is authentication-only: a logged-in user may run any command — roles are
  stored (`createUser`'s `roles` field, `createRole` is not implemented) but not evaluated.
- X.509 client-certificate authentication is not supported (fails honestly with code 18).
- Passwords travel SCRAM-hashed, never in the clear — but combine `--auth` with `--ssl` when
  crossing untrusted networks to also encrypt the data itself.

**SSL with Docker:**
```dockerfile
FROM openjdk:21-slim
WORKDIR /app

COPY poppydb/target/poppydb-6.2.0-SNAPSHOT-cli.jar /app/poppydb.jar
COPY server.jks /app/server.jks

EXPOSE 27018

CMD ["java", "-jar", "/app/poppydb.jar", \
     "--port", "27018", "--host", "0.0.0.0", \
     "--ssl", "--sslKeystore", "/app/server.jks", \
     "--sslKeystorePassword", "changeit"]
```

### Constructor Options

```java
// Full constructor
PoppyDB server = new PoppyDB(
    int port,           // Server port
    String host,        // Bind address
    int maxThreads,     // Maximum threads
    int minThreads      // Minimum threads
);

// Default constructor (port 17017, localhost, 100/10 threads)
PoppyDB server = new PoppyDB();
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
        run: mvn clean package -pl poppydb -am -Dmaven.test.skip=true

      - name: Start PoppyDB
        run: |
          java -jar poppydb/target/poppydb-6.2.0-SNAPSHOT-cli.jar \
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
    server = new PoppyDB(27017, "0.0.0.0", 100, 10);
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
# Terminal 1: Start PoppyDB
java -jar poppydb/target/poppydb-6.2.0-SNAPSHOT-cli.jar --port 27017

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
COPY poppydb/target/poppydb-6.2.0-SNAPSHOT-cli.jar /app/poppydb.jar

EXPOSE 27017

CMD ["java", "-jar", "/app/poppydb.jar", \
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
docker build -t poppydb .
docker run -p 27017:27017 poppydb

# Or use docker-compose
docker-compose up
```

### 5. Message Broker for Short-Lived Messages (production)

Morphium messaging runs natively against PoppyDB: TTL-based messages, exclusive locks,
answer semantics, and event-driven delivery via change streams. For **ephemeral messages**
— events, cache invalidation, job triggers with sender-side retry — a 3-node replica set
is a lightweight broker: node failures are covered by replication and Raft failover, and
the [memory watermark](#memory-watermark) turns overload into retryable backpressure
instead of an OOM.

**Know the loss model:** there is no write-ahead log. A cluster-wide outage loses all
messages since the last snapshot (if any). Use it where in-flight loss is acceptable and
senders can retry — not for guaranteed delivery.

### 6. Cache / Session Storage (production)

Cache semantics tolerate total loss by definition, which makes PoppyDB a candidate for
memcached/Redis-style roles with two twists neither offers out of the box:

- **Wire compatibility**: every language with a MongoDB driver is a client — no extra
  protocol or library.
- **Push invalidation**: change streams are built-in pub/sub — cache entries invalidate
  by event instead of polling.

Session storage is the textbook case: a TTL index on the last-access field expires
sessions automatically, replica-set failover keeps sessions alive across node restarts
(more than memcached offers), and off-the-shelf MongoDB session backends (e.g. Spring
Session) work unchanged. `$inc` + TTL also cover rate limiting and counters; tiny
config/feature-flag collections get instant propagation via change streams.

For all production use: enable [`--auth`](#authentication---auth) (note that roles are
not evaluated yet — isolate the network segment), size the heap deliberately, monitor
`db.serverStatus().memoryWatermark` and `db.stats()`, and read the loss model above.

## Performance

| Metric | PoppyDB | MongoDB |
|--------|---------------|---------|
| Startup Time | ~100-500ms | ~2-5 seconds |
| Memory (baseline) | ~50-100MB | ~500MB-1GB |
| Inserts/sec | ~50,000 | Varies |
| Queries/sec | ~100,000 | Varies |
| Updates/sec | ~40,000 | Varies |
| Latency (localhost) | 1-5ms | 1-10ms |

### Messaging Round-Trip Latency (measured)

Measured with [morpheus](https://github.com/sboesebeck/morpheus), the CLI tool for
morphium-driven projects: its latency harness sends morphium messages ping-pong style and
records the full round trip (send → receive → answer → answer received). Setup: dedicated
run on a single MacBook Pro, **both** systems as a 3-node replica set, identical morphium
messaging code on both sides.

| Metric (ms) | PoppyDB (3-node RS) | MongoDB (3-node RS) | Factor |
|---|---|---|---|
| avg round trip | 2.64 | 59.5 | ~22x |
| p50 | 2.43 | 59.1 | ~24x |
| p90 | 3.31 | 70.0 | ~21x |
| p99 | 6.70 | 79.8 | ~12x |
| min | 1.31 | 34.8 | ~26x |
| jitter | 0.66 | 6.48 | ~10x |
| message loss | 0% | 0% | — |

**Why the gap is structural, not tuning:** MongoDB change streams only emit
majority-committed events, so every messaging hop pays replication plus journal-commit
latency — that is MongoDB's ~35ms latency *floor* in this setup, and it buys durability.
PoppyDB's change streams emit directly from memory and pay nothing, because there is
nothing to persist — the same trade-off described in the
[use cases](#5-message-broker-for-short-lived-messages-production): you get the latency
because you accepted the loss model. Numbers vary with hardware; re-run the harness on
your target machine for real figures.

## Monitoring

### Built-in Status Monitoring

**All Morphium messaging instances automatically include status monitoring** via the `morphium_status` topic. This works with PoppyDB and any Morphium messaging setup.

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
PoppyDB server = new PoppyDB(27017, "localhost", 100, 10);
server.start();

// Get active connections
int connections = server.getConnectionCount();
System.out.println("Active connections: " + connections);
```

### Logging

The server CLI jar ships its own Logback configuration: root level `INFO`, Netty at `WARN`,
console output. Verbosity can be raised or lowered at startup — the three overrides, in
increasing order of control:

```bash
# CLI option (recommended)
java -jar poppydb-cli.jar --port 27017 --log-level DEBUG

# System property (read by the bundled logback.xml)
java -Dpoppydb.log.level=DEBUG -jar poppydb-cli.jar --port 27017

# Full control: replace the bundled configuration entirely
java -Dlogback.configurationFile=/path/to/my-logback.xml \
     -jar poppydb-cli.jar --port 27017
```

`--log-level` accepts `ERROR`, `WARN`, `INFO`, `DEBUG` and `TRACE`. Both the CLI option and
the system property change the root logger only — Netty stays at `WARN` either way; to see
Netty internals, replace the configuration via `-Dlogback.configurationFile`.

## Supported Admin Commands

PoppyDB implements the following MongoDB admin commands:

| Command | Description |
|---------|-------------|
| `ping` | Basic connectivity test |
| `hello` / `isMaster` / `ismaster` | Server status and topology information |
| `listDatabases` | List all databases with sizes |
| `buildInfo` | Server version information |
| `getCmdLineOpts` | Command line options |
| `getParameter` | Server parameters |
| `getLog` | Server logs |
| `listCommands` | Names of every command this server answers |
| `currentOp` / `$currentOp` stage | Live operations from the server's op registry — `db.currentOp()` works, including `$match` filters |
| `killOp` | Marks an op kill-pending; best-effort thread interrupt (never a Netty event loop — cooperative like mongod) |
| `serverStatus` | Includes real client connection gauges (`connections.current`/`totalCreated` from the Netty channel group) |
| `dbStats` / `collStats` | Real BSON data sizes, estimated index sizes |
| `hostInfo` | Host basics (hostname, cores, memory, OS, JVM) |
| `connectionStatus` | Authenticated user of this connection (empty without `--auth`) |
| `whatsmyuri` | Client address as the server sees it |
| `replSetGetStatus` / `replSetGetConfig` | `rs.status()` and `rs.conf()` (config reconstructed from seeds and priorities) |
| `dbHash` | MD5 per collection + combined hash in canonical document order — compare replica-set members with one command, works on secondaries |
| `validate` | Real data↔index consistency check against the collection's index store (stale/missing index entries, keysPerIndex) |
| `replSetStepDown` | Step down from primary (for replica sets) |
| `startSession` / `endSessions` / `refreshSessions` | Session management |
| `getMore` | Cursor iteration for both regular queries and change streams |

### Standalone Server Behavior

When running PoppyDB as a standalone server (without replica set configuration):

- The server always reports itself as primary (`isWritablePrimary: true`)
- `replSetStepDown` commands are acknowledged but the server immediately becomes primary again
- This ensures compatibility with clients and tests that issue replica set commands

### StepDown / Failover Behavior (Replica Set)

PoppyDB uses **Raft-based leader election**, which behaves differently from MongoDB:

| Behavior | MongoDB | PoppyDB |
|----------|---------|---------|
| StepDown | Old primary steps down, may become primary again after `timeToStepDown` | Old primary steps down, may reclaim leadership via priority takeover |
| New leader | Elected via priority/oplog, often original wins re-election | Elected via Raft, highest priority wins |
| Step-back | Common (original typically returns as primary) | Yes, if the original node has a higher priority (since 6.3) |
| Election time | ~2-10 seconds | ~2-5 seconds |

**Impact on tests/applications:** the original primary only returns if it was configured with a *higher priority* than the node that took over. In a cluster where all nodes share the default priority (50), the new leader keeps leadership — do not wait for a specific node to become primary, verify that *any* node did.

#### Priority Takeover

A leader periodically checks whether a peer with higher priority is available. It hands leadership over once that peer

- answers the leader's heartbeats (it is online and reachable), and
- has acknowledged everything the leader replicated during its term (it is caught up, see `priorityTakeoverMaxLag`).

The yielding leader then refuses re-election for `priorityTakeoverStepDownSecs`, so the higher-priority node — which uses a shorter, priority-adjusted election timeout — wins the resulting election.

| `ElectionConfig` setting | System property | Default | Meaning |
|--------------------------|-----------------|---------|---------|
| `priorityTakeoverEnabled` | `morphiumserver.priorityTakeoverEnabled` | `true` | Enable voluntary step-down to a higher-priority peer |
| `priorityTakeoverCheckIntervalMs` | `morphiumserver.priorityTakeoverCheckIntervalMs` | `2000` | How often the leader looks for a successor |
| `priorityTakeoverMinStabilityMs` | `morphiumserver.priorityTakeoverMinStabilityMs` | `30000` | How long a node must have been leader before it may yield (anti-flapping) |
| `priorityTakeoverMaxLag` | `morphiumserver.priorityTakeoverMaxLag` | `0` | Change stream events the successor may still lag behind |
| `priorityTakeoverStepDownSecs` | `morphiumserver.priorityTakeoverStepDownSecs` | `10` | How long the yielding leader refuses re-election |

Set `priorityTakeoverEnabled` to `false` to keep the pre-6.3 behavior, where a failover is permanent.

### Change Stream Support

PoppyDB fully supports change streams for real-time notifications:

- **Collection-level watches**: Watch changes on a specific collection
- **Database-level watches**: Watch all collections in a database
- **Cluster-level watches**: Watch all databases
- **Resume tokens**: Resume a change stream from a token after reconnection (since v6.2.2)
- **Tailable cursors**: Capped collection tailable cursors with proper `maxTimeMS` polling (since v6.2.2)

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
- ❌ **No Sharding** - Single instance only
- ✅ **Replica Sets** - Supported with Raft-based leader election (since v6.2.0)
- ❌ **Memory Bound** - Dataset limited by available RAM

### Features
- ✅ **Server-side Cursors** - Batched find queries with `batchSize` and `getMore` (since v6.2.2)
- ✅ **Tailable Cursors** - Capped collection tailable cursors with `maxTimeMS` polling (since v6.2.2)
- ✅ **Change Stream Resume** - Resume-after token support for reliable event delivery (since v6.2.2)
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
mvn clean package -pl poppydb -am -Dmaven.test.skip=true

# This creates the executable PoppyDB CLI JAR:
# poppydb/target/poppydb-X.Y.Z-cli.jar

# Run the server:
java -jar poppydb/target/poppydb-6.2.0-SNAPSHOT-cli.jar --port 27017
```

## Maven Dependency

```xml
<dependency>
    <groupId>de.caluga</groupId>
    <artifactId>poppydb</artifactId>
    <version>6.2.0</version>
</dependency>
```

Then start programmatically:
```java
public static void main(String[] args) throws Exception {
    // Option 1: Call main from the CLI class
    de.caluga.poppydb.PoppyDBCLI.main(
        new String[]{"--port", "27017", "--host", "0.0.0.0"}
    );

    // Option 2: Create instance directly
    PoppyDB server = new PoppyDB(27017, "0.0.0.0", 100, 10);
    server.start();
}
```

## Comparison: PoppyDB vs InMemory Driver

| Feature | PoppyDB | InMemory Driver |
|---------|---------------|-----------------|
| **Network Access** | Yes (wire protocol) | No (embedded) |
| **Multi-Language** | Yes | No (Java only) |
| **Use Case** | Integration tests, microservices | Unit tests |
| **Overhead** | Network latency | In-process |
| **Setup** | Start server | Config setting |
| **Isolation** | Process-level | Per-JVM |

**When to use each:**
- **InMemory Driver**: Java unit tests, embedded apps
- **PoppyDB (server)**: Integration tests, CI/CD, multi-language services

## See Also

- [InMemory Driver](./howtos/inmemory-driver.md) - Embedded driver for unit tests
- [Messaging](./messaging.md) - Messaging with Morphium / PoppyDB
- [Configuration Reference](./configuration-reference.md) - All configuration options
- [Architecture Overview](./architecture-overview.md) - How it works internally
