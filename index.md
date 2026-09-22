# Morphium

<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="branding/morphium-logo-dark.svg">
    <img src="branding/morphium-logo.svg" alt="Morphium" width="640">
  </picture>
</p>

**Feature-rich MongoDB ODM and messaging framework for Java 21+**

Available languages: English and [Deutsch](README.de.md)

- 🗄️ **High-performance object mapping** with annotation-driven configuration
- 📨 **Integrated message queue** backed by MongoDB (no extra infrastructure)
- ⚡ **Multi-level caching** with cluster-wide invalidation
- 🔌 **Custom MongoDB wire-protocol driver** tuned for Morphium
- 🧪 **In-memory driver** for fast tests (no MongoDB required)
- 🌱 **[PoppyDB](https://sboesebeck.github.io/morphium/poppydb/)** — MongoDB-compatible in-memory server: replica sets, auth/TLS, messaging backend
- 🎯 **JMS API (experimental)** for standards-based messaging
- 🚀 **Java 21+** — modern language baseline (pattern matching, sealed types)

[![Maven Central](https://img.shields.io/maven-central/v/de.caluga/morphium.svg)](https://search.maven.org/artifact/de.caluga/morphium)
[![Tests](https://img.shields.io/endpoint?url=https%3A%2F%2Fraw.githubusercontent.com%2Fsboesebeck%2Fmorphium%2Ftest-results%2Fbadges%2Ftests.json)](https://github.com/sboesebeck/morphium/releases)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

## 🎯 Why Morphium?

Morphium is the only Java ODM that ships a message queue living inside MongoDB. If you already run MongoDB, you can power persistence, messaging, caching, and change streams with a single component.

| Feature | Morphium | Morphium + PoppyDB | Spring Data + RabbitMQ | Kafka |
|---------|----------|--------------------|------------------------|-------|
| Infrastructure | MongoDB only | **None** — embedded Java server | MongoDB + RabbitMQ | MongoDB + Kafka |
| Setup complexity | ⭐ Very low | ⭐ Minimal (one dependency) | ⭐⭐⭐ Medium | ⭐⭐⭐⭐⭐ High |
| Message persistence | Built in | Snapshots (optional) | Optional | Built in |
| Message priority | ✅ Yes | ✅ Yes | ✅ Yes | ❌ No |
| Distributed locks | ✅ Yes | ✅ Yes | ❌ No | ❌ No |
| Throughput, one-way send→receive* | ~870–1,250 msg/s | ~770–4,900 msg/s | 10K–50K msg/s | 100K+ msg/s |
| Round-trip request→response (ping-pong)* | 89 msg/s | **223 msg/s (2.5×)** | — | — |
| Operations | ⭐ Very easy | ⭐ Trivial (single process) | ⭐⭐ Medium | ⭐⭐⭐⭐ Complex |

_* All numbers are indicative and depend heavily on hardware and workload; Morphium's are
[measured](docs/v5-vs-v6-performance.md), the RabbitMQ/Kafka columns quote typical vendor/
community figures. **One-way** (send→receipt, no reply) runs ~870–1,250 msg/s against a
3-node MongoDB replica set; PoppyDB runs in-process and scales with the host, from ~770
msg/s on a small CI box up to ~4,900 msg/s on an M1 Ultra desktop. **Round-trip**
(request→response) is where the tight PoppyDB/Morphium Messaging integration shows: 223
msg/s at 4.5 ms latency vs. 89 msg/s at 11.3 ms against MongoDB — 2.5× the throughput at
less than half the latency, confirmed by a symmetric re-measurement (2.34–2.49×, PoppyDB
p50 ~2.1 ms vs MongoDB p50 ~5.0 ms, with a much tighter p99 tail). Full methodology, the
Kafka comparison and per-message cost breakdown are in
[docs/v5-vs-v6-performance.md](docs/v5-vs-v6-performance.md)._

_**How does Kafka's 100K+ figure hold up?** That number is real for Kafka's normal batched,
async mode — but forced into Morphium's semantics (synchronous, individually-acknowledged
sends) on the same hardware, Kafka drops to ~8–10K msg/s vs. ~1,800 msg/s for
Morphium+PoppyDB — a 4–5× gap, not 100+. The difference is architecture, not
implementation quality: Kafka's headline throughput comes from batching, and Morphium
deliberately acknowledges every message individually. Client-side batching narrows that
gap for Morphium too (`sendMessages()`/`sendAnswers()`, ~4× over unbatched `sendMessage()`)
— details in [docs/v5-vs-v6-performance.md](docs/v5-vs-v6-performance.md)._

## 🌱 PoppyDB — MongoDB-Compatible In-Memory Server

<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="branding/poppydb-logo-dark.svg">
    <img src="branding/poppydb-logo.svg" alt="PoppyDB" width="560">
  </picture>
</p>

PoppyDB is Morphium's sibling product: an in-memory server that speaks the MongoDB wire
protocol. Any client connects — `mongosh`, Compass, PyMongo, the official drivers, and of
course Morphium. It starts in milliseconds and needs zero infrastructure: no Docker, no
Testcontainers, no MongoDB installation.

- Wire protocol, change streams, aggregation pipeline, indexes, transactions
- **Replica-set emulation** with real leader election and automatic failover
- **SCRAM authentication + TLS** (6.3.0) — `mongosh` logs in exactly as against real MongoDB
- **Declarative user provisioning** (6.3.0) via `--users-file` — idempotent, replicated, version-gated
- **Snapshot persistence** — periodic dumps, automatic restore on startup
- **Messaging backend** — server-side optimizations specifically for Morphium Messaging

### How-to: embedded test backend

```xml
<dependency>
    <groupId>de.caluga</groupId>
    <artifactId>poppydb</artifactId>
    <version>6.3.12</version>
    <scope>test</scope>
</dependency>
```

```java
PoppyDB server = new PoppyDB(27017, "localhost", 100, 10);
server.start();
// ... any MongoDB client can connect to localhost:27017 now ...
server.shutdown();
```

### How-to: the CLI — a throwaway MongoDB for ANY test suite

The embedded route above is Java-only; the CLI jar works for every stack. It is a single
self-contained jar from Maven Central (classifier `cli`) — your Python/Node/Go/Rust
integration tests get a MongoDB-compatible server in milliseconds, no Docker image, no
Testcontainers, nothing to install:

```bash
curl -O https://repo1.maven.org/maven2/de/caluga/poppydb/6.3.12/poppydb-6.3.12-cli.jar

# start for a test run: --no-config keeps it isolated from any stray
# ~/.config/poppydb/config on a developer machine - same flags, same behavior in CI
java -jar poppydb-6.3.12-cli.jar --port 27017 --no-config
```

Point your test suite at `mongodb://localhost:27017`, kill the process afterwards — state is
gone (unless you want persistence, see below). `--help` lists all options.

The CLI is not just a test tool, though: **as a messaging backend it is production-ready** —
that is exactly what PoppyDB's server-side messaging optimizations are for. Run it with
snapshot persistence, a replica set for HA, and auth/TLS (all below), and you have a
standing message broker with a single jar. It is a general-purpose MongoDB *replacement*
only for dev/test — but for Morphium Messaging it is the recommended dedicated backend, see
the [deployment playbook](docs/howtos/poppydb-deployment.md).

### How-to: standalone server with persistence

```bash
java -jar poppydb-6.3.12-cli.jar --port 27017 --dump-dir ./data --dump-interval 300
```

Snapshots every 5 minutes, final dump on shutdown, automatic restore on the next start.
Config can also live in a properties file: `--cfg /etc/poppydb/config` (validate it upfront
with `--check-config`, inspect the effective result with `--print-config`).

### How-to: 3-node replica set

One process per node, each with the same seed list — election picks the primary, failover is
automatic:

```bash
java -jar poppydb-6.3.12-cli.jar -p 17017 --rs-name myrs \
  --rs-seed host1:17017,host2:17017,host3:17017 --rs-priorities 100,50,50
```

Users (`admin.system.users`) replicate across the set, so logins survive failover.

### How-to: authentication + TLS (6.3.0)

```bash
java -jar poppydb-cli.jar -p 27018 --auth --rootUser admin --rootPassword s3cr3t \
  --ssl --sslKeystore server.jks --sslKeystorePassword changeit

mongosh "mongodb://admin:s3cr3t@localhost:27018/test?authSource=admin"
```

For provisioning a whole user set declaratively, point `--users-file` at a JSON file — applied
idempotently on every leadership change, protected against rollback by a version gate.

### How-to: message queue without MongoDB

Morphium Messaging runs on PoppyDB as its backend — a full message queue (topics, exclusive
delivery, request/response) with a single Java dependency. This is a production use case,
not a test trick: PoppyDB and Morphium Messaging are optimized for each other, and a
standalone PoppyDB (CLI, with persistence + replica set + auth/TLS) makes a dedicated
message broker without operating a MongoDB:

```java
PoppyDB server = new PoppyDB(27017, "localhost", 100, 10);
server.start();

try (Morphium morphium = new Morphium(cfg)) {          // cfg points at localhost:27017
    MorphiumMessaging messaging = morphium.createMessaging();
    messaging.addListenerForTopic("orders", (mq, msg) -> {
        System.out.println("new order: " + msg.getValue());
        return null;
    });
    messaging.start();
}
```

📖 **Deep dives:** [Online documentation](https://sboesebeck.github.io/morphium/poppydb/) ·
[PoppyDB guide](docs/poppydb.md) ·
[Production deployment playbook](docs/howtos/poppydb-deployment.md) ·
[Migrating from MongoDB](docs/howtos/migration-mongodb-to-poppydb.md)

## 📚 Documentation

### Quick access
- **[Documentation hub](docs/index.md)** – entry point for all guides
- **[Overview](docs/overview.md)** – core concepts, quick start, compatibility
- **[Upgrade v6.2→v6.3](docs/howtos/migration-v6_2-to-v6_3.md)** – what changes in 6.3.x
- **[Upgrade v6.1→v6.2](docs/howtos/migration-v6_1-to-v6_2.md)** – migration checklist for 6.2.x
- **[Migration v5→v6](docs/howtos/migration-v5-to-v6.md)** – step-by-step upgrade guide
- **[InMemory Driver Guide](docs/howtos/inmemory-driver.md)** – capabilities, caveats, testing tips
- **[PoppyDB Guide](docs/poppydb.md)** – the MongoDB-compatible in-memory server in depth
- **[PoppyDB Deployment Playbook](docs/howtos/poppydb-deployment.md)** – config file, replica sets, auth/TLS in production
- **[Optimistic Locking (`@Version`)](docs/howtos/optimistic-locking.md)** – prevent lost updates with `@Version`
- **[SSL/TLS & MONGODB-X509](docs/ssl-tls.md)** – encrypted connections and certificate-based authentication

### More resources
- Aggregation examples: `docs/howtos/aggregation-examples.md`
- Messaging implementations: `docs/howtos/messaging-implementations.md`
- Performance guide: `docs/performance-scalability-guide.md`
- Production deployment: `docs/production-deployment-guide.md`
- Monitoring & troubleshooting: `docs/monitoring-metrics-guide.md`
- Live monitoring & load testing: [Morpheus](https://github.com/sboesebeck/morpheus) — terminal UI/CLI for watching messages, topics and node health in real time, no code required

## 🚀 What’s New in v6.3

### About the 6.3.3 → 6.3.6 release storm (August 2026)

Four patch releases in one week is not our usual cadence: an AI-assisted deep-code-review of the
change-stream and replication path surfaced a class of load-only bugs (silent event loss on
resume, live events overtaking history replay, unbounded memory pinning) that no user had ever
reported — exactly the kind that doesn't file an issue, it just shows up months later as quietly
diverged data. **6.3.4** shipped those fixes but introduced a client-side resume-token loop
(#329), fixed the same day in **6.3.5**; **6.3.6** then fixed a related connection-pool topology
bug (#330) found on our own staging cluster. Full story per fix in the [CHANGELOG](CHANGELOG.md).

**If you are on any 6.3.x: upgrade straight to 6.3.6**, in the order below.

> ⚠️ **Upgrade order matters: clients first, then servers — and skip 6.3.4.**
>
> Every client up to and including 6.3.4 carries a resume-token bug
> ([#329](https://github.com/sboesebeck/morphium/issues/329)): when the server ends a stream
> with `ChangeStreamHistoryLost`, the monitor discards its resume token and immediately
> resurrects and retries it — forever, with no backoff. A PoppyDB restart (or a MongoDB
> consumer falling off the oplog) triggers this in every connected pre-6.3.5 client at once,
> effectively DDoSing the server until each client is restarted by hand.
>
> **Rollout order: 1)** upgrade all client applications to ≥ 6.3.5 first, **2)** only then
> restart the PoppyDB servers — a server deployed first arms the loop in any client not yet
> upgraded.
>
> From 6.3.5 on, a PoppyDB server with a dump directory also persists its change-stream
> sequence across restarts, so orderly restarts no longer invalidate resume tokens at all.

### Optional Integration Modules
`morphium-jakarta-data` implements [Jakarta Data 1.0](https://jakarta.ee/specifications/data/1.0/) on top of Morphium's query engine — `@Repository` interfaces with query derivation from method names, JDQL via `@Query` (including `GROUP BY`/`HAVING` compiled into an aggregation pipeline), offset and cursor/keyset pagination. Two framework modules build on it: `quarkus-morphium` for CDI integration (config mapping, `@MorphiumTransactional`, health checks, Dev Services, Dev UI, GraalVM native-image support, build-time repository generation via Gizmo), and `spring-boot-morphium` for Spring Boot (auto-configuration, type-safe `@ConfigurationProperties`, `@MorphiumTransactional`, an Actuator health indicator) — a Spring Boot app on Morphium, not a Spring Data MongoDB replacement. All three are optional — core has no dependency on any of them, and `-DskipExtensions` still produces a core-only build. See [Jakarta Data](docs/jakarta-data.md), [Quarkus Extension](docs/quarkus-extension.md) and [Spring Boot Starter](docs/spring-boot.md).

**Note:** the Quarkus extension moved from `io.quarkiverse.morphium:quarkus-morphium:1.2.0` to `de.caluga:quarkus-morphium:6.3.0`. Coordinates only — no package renames, no API changes.

### DualChannelMessaging (beta)
A third messaging implementation: the standard single collection and cursor for broadcast/topic traffic, plus a dedicated per-recipient collection with its own cursor and dispatcher thread for directed messages and answers. Select it with `cfg.messagingSettings().setMessagingImplementation("DualChannelMessaging")`. Beta on purpose — past saturation it trades a little throughput for markedly better tail latency. See `docs/howtos/messaging-implementations.md`.

> ⚠️ **All messaging participants on a queue must run the same implementation.** This has always been true for `SingleCollectionMessaging` and `MultiCollectionMessaging`, and it applies to `DualChannelMessaging` too: the implementations use different collection layouts and there is no bridge between them. A mismatch fails *silently* — a Standard node waiting for an answer from a Dual Channel responder times out forever, because the answer goes into the requester's DM collection, which Standard never reads. Switch every node together, and drain or pause request/reply traffic while you do.
>
> Since **6.3.1** a mismatch is *detected*: every instance announces its implementation in a layout-independent `<queue>_participants` collection and checks the other participants on startup — WARN by default; `cfg.messagingSettings().setMessagingImplementationCheck(ImplementationCheck.THROW)` makes a mismatched instance refuse to start instead (#280).

### Messaging Improvements (all implementations)
One database roundtrip less per non-exclusive message (processed straight from the change-stream `fullDocument`), event-driven delivery of requeued messages, configurable default TTL and fallback-poll cadence, change-stream liveness driving the fallback poll, and a processing decision trace for diagnosing answer timeouts.

### PoppyDB: Operable, Not Just Runnable
Real SCRAM-SHA-1/SCRAM-SHA-256 authentication with opt-in enforcement (`--auth`), declarative user provisioning from a file (`--users-file`) and users that replicate across the replica set instead of living on one node. Configuration files (`--cfg`, `--print-config`, `--check-config`) keep secrets off the command line, `--log-level` stops the DEBUG firehose, and a DevOps command surface adds live `currentOp`/`killOp`, `rs.conf()`, `listCommands`, `hostInfo`, `dbHash` and a `validate` that really walks the indexes.

### Memory Watermark and Honest Size Limits
Two heap watermarks (`--memory-warn` / `--memory-reject`, decided on the post-GC live set) reject document-creating writes with a retryable `ExceededMemoryLimit` before the heap dies, while updates, deletes and TTL expiry stay allowed so the system can drain. The 16MB BSON document limit is now enforced like mongod instead of merely advertised, and `maxMessageSizeBytes` is respected end-to-end with byte-aware write-batch splitting.

### InMemoryDriver: Closing the Gap to mongod
New aggregation stages (`$merge`, `$documents`, `$densify`, `$fill`, `$setWindowFields`, `$collStats`, `$listSessions`, and a real `$out`), ~40 additional expression operators, positional update operators `$`/`$[]`/`$[<identifier>]` with `arrayFilters`, and `$bit`. Plus a long list of correctness fixes — among them `$geoWithin` with `$center`/`$centerSphere`/`$polygon`, which matched *every* document, UTC-correct date operators with a 1-based `$month`, and `$project` inclusion mode actually restricting output.

### Replication and Failover Hardening
PoppyDB replication is now lossless, order-preserving and covers index definitions. Fixed: a re-syncing secondary broadcasting its initial-sync wipe as change-stream drop events (which could destroy `admin.system.users` cluster-wide during a stepdown), a demoted leader stuck at `primary == true`, `rs.status()` reporting a dead peer as SECONDARY forever, and a plaintext internal election/replication channel that made `--auth`/`--ssl` ineffective on a replica set. On the client side, the failover read path could throw a raw NPE past every retry.

### Performance
Insert's duplicate-`_id` pre-check is an O(1) index lookup instead of a full scan under the write lock, the change-stream before-image is no longer deep-copied twice per watched update, and the index-store rebuild ping-pong between an open transaction and concurrent readers is gone.

Upgrading is covered step by step in the [migration guide](docs/howtos/migration-v6_2-to-v6_3.md); see [CHANGELOG](CHANGELOG.md) for full details.

## 🚀 What’s New in v6.2

### Multi-Module Maven Build
Morphium is now a multi-module project: `morphium-parent` (BOM), `morphium` (core library), and `poppydb` (server). The core library `de.caluga:morphium` no longer drags in server dependencies (Netty, etc.) — 90% leaner for users who just need the ODM.

### PoppyDB – Standalone MongoDB-Compatible Server
The former MorphiumServer became an independent module `de.caluga:poppydb` in 6.2 — see the
[PoppyDB section above](#-poppydb--mongodb-compatible-in-memory-server) for what it does and
how to use it.

### MorphiumDriverException is now unchecked
`MorphiumDriverException` extends `RuntimeException` — consistent with the MongoDB Java driver. Eliminates 40+ boilerplate `catch-wrap-rethrow` blocks.

### @Reference Cascade Delete/Store
`@Reference` now supports `cascadeDelete` and `cascadeStore` for automatic lifecycle management of referenced entities.

### @AutoSequence
Annotation-driven auto-increment sequences — no manual counter management needed.

### @CreationTime Improvements
Works correctly with `store()` and `storeList()`, supports `@CreationTime` on `Date`, `long`, and `String` fields.

### CosmosDB Auto-Detection
Morphium detects Azure CosmosDB connections and automatically adjusts behavior for compatibility.

### Patch releases 6.2.1 – 6.2.10
The 6.2.x patch releases brought continuous improvements, among them: server-side recipient filtering and a liveness watchdog for messaging, a `defaultQueryTimeoutMS` setting, field-name translation in `Aggregator` and `Query.distinct()`, a dedicated `MorphiumDocumentTooLargeException`, and numerous PoppyDB/InMemoryDriver robustness fixes. The later patches (6.2.5–6.2.10) focused on production hardening of the wire path and messaging: mid-message read timeouts no longer desynchronize the wire stream, replies are verified against their request id (`responseTo`), change streams resume from the last token across restarts instead of silently skipping events, and exclusive messages can no longer be processed twice when their lock is lost mid-processing.

See [CHANGELOG](CHANGELOG.md) for full details.

## Upgrading from 6.1.x to 6.2.x

Two breaking changes: `MorphiumDriverException` now extends `RuntimeException` instead of
`Exception` (drop `MorphiumDriverException` from multi-catches and `throws` clauses), and the
embedded server was extracted into its own module and renamed `MorphiumServer` → `PoppyDB`
(`de.caluga:poppydb`, package `de.caluga.poppydb`, tag `@Tag("poppydb")`) — the wire handshake
still answers to both names, so mixed-version replica sets keep working. Config setters also
moved to typed sub-objects (`cfg.connectionSettings().setDatabase(...)`); the old flat setters
stay deprecated-but-functional through the whole 6.x line (see the
[Deprecation Policy](docs/deprecation-policy.md)). Full checklist and code samples in the
[migration guide](docs/howtos/migration-v6_1-to-v6_2.md).

## 🚀 What’s New in v6.1.x

### MONGODB-X509 Client-Certificate Authentication
- Connect to MongoDB instances that require mutual TLS / x.509 client certificates
- Configure via `AuthSettings.setAuthMechanism("MONGODB-X509")` together with the existing `SslHelper` mTLS setup

### `@Version` – Optimistic Locking
Prevents lost updates in concurrent environments without requiring pessimistic database locks. See `docs/howtos/optimistic-locking.md` for the full guide.

## 🚀 What’s New in v6.0

### Java 21 & Modern Language Features
- **Pattern matching** across driver and mapping layers
- **Records**: Not yet supported as `@Entity` or `@Embedded` types (see [#116](https://github.com/sboesebeck/morphium/issues/116))
- **Sealed class support** for cleaner domain models
- **Virtual threads** were introduced in this era but rolled back again in 6.2.x: JDK 21's `synchronized` pinning caused deadlocks under load. Morphium runs on platform threads throughout; virtual threads will be re-evaluated once JEP 491 (JDK 24+) is the baseline.

### Driver & Connectivity
- **SSL/TLS Support**: Secure connections to MongoDB instances (added in v6.0)

### Messaging Improvements
- **Fewer duplicates** thanks to refined message processing
- **Higher throughput** confirmed in internal benchmarking
- **Distributed locking** for coordinated multi-instance deployments

### In-Memory Driver Enhancements
- **No MongoDB required** for unit tests or CI pipelines
- **Significantly faster test cycles** in pure in-memory mode
- **~93% MongoDB feature coverage** including advanced operations
- **Full aggregation pipeline** with `$lookup`, `$graphLookup`, `$bucket`, `$mergeObjects`
- **MapReduce support** with JavaScript engine integration
- **Array operators** including `$pop`, `$push`, `$pull`, `$addToSet`
- **Change streams & transactions** available for integration testing
- **Drop-in replacement** for most development and testing scenarios

### Documentation Overhaul
- Complete rewrite of the guide set
- Practical examples and end-to-end use cases
- Dedicated migration playbook from 5.x to 6.x
- Architecture insights and best practices

## ✅ Requirements
- Java 21 or newer
- MongoDB 5.0+ for production deployments
- Maven

Maven dependencies:
```xml
<dependency>
  <groupId>de.caluga</groupId>
  <artifactId>morphium</artifactId>
  <version>[6.2.0,)</version>
</dependency>
<dependency>
  <groupId>org.mongodb</groupId>
  <artifactId>bson</artifactId>
  <version>4.7.1</version>
</dependency>
```

Migrating from v5? → `docs/howtos/migration-v5-to-v6.md`

## ⚡ Quick Start

### Maven dependency

```xml
<dependency>
  <groupId>de.caluga</groupId>
  <artifactId>morphium</artifactId>
  <version>6.3.12</version>
</dependency>
```

### Object mapping example

```java
import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.annotations.*;
import de.caluga.morphium.driver.MorphiumId;
import java.time.LocalDateTime;
import java.util.List;

// Entity definition
@Entity
public class User {
    @Id
    private MorphiumId id;
    private String name;
    private String email;
    private LocalDateTime createdAt;
    // getters/setters
}

// Configuration
MorphiumConfig cfg = new MorphiumConfig();
cfg.connectionSettings().setDatabase("myapp");
cfg.clusterSettings().addHostToSeed("localhost", 27017);
cfg.driverSettings().setDriverName("PooledDriver");

Morphium morphium = new Morphium(cfg);

// Store entity
User user = new User();
user.setName("John Doe");
user.setEmail("john@example.com");
user.setCreatedAt(LocalDateTime.now());
morphium.store(user);

// Query
List<User> users = morphium.createQueryFor(User.class)
    .f("email").matches(".*@example.com")
    .sort("createdAt")
    .asList();
```

### Messaging example

```java
import de.caluga.morphium.messaging.MorphiumMessaging;
import de.caluga.morphium.messaging.Msg;

// Messaging setup
MorphiumMessaging messaging = morphium.createMessaging();
messaging.setSenderId("my-app");
messaging.start();

// Send a message
Msg message = new Msg("orderQueue", "Process Order", "Order #12345");
message.setPriority(5);
message.setTtl(300000); // 5 minutes
messaging.sendMessage(message);

// Receive messages
messaging.addListenerForTopic("orderQueue", (m, msg) -> {
    // process order ...
    return null; // no reply
});
```

### Properties & environment configuration

```bash
# Environment variables
export MONGODB_URI='mongodb://user:pass@localhost:27017/app?replicaSet=rs0'
export MORPHIUM_DRIVER=inmem

# System properties
mvn -Dmorphium.uri='mongodb://localhost/mydb' test

# Properties file (morphium.properties)
morphium.hosts=mongo1.example.com:27017,mongo2.example.com:27017
morphium.database=myapp
morphium.replicaSet=myReplicaSet
```

## 🧪 Tests & Test Runner

### Maven
```bash
# All tests
mvn test

# Full build with checks
mvn clean verify

# Tagged test selection
mvn test -Dgroups="core,messaging"

# Run against a real MongoDB instance
mvn test -Dmorphium.driver=pooled -Dmorphium.uri=mongodb://localhost/testdb
```

### `./runtests.sh` helper
```bash
# Default: in-memory driver (fast, no MongoDB required)
./runtests.sh

# Run tagged suites
./runtests.sh --tags core,messaging

# Parallel runs
./runtests.sh --parallel 8 --tags core

# Retry only failed methods
./runtests.sh --rerunfailed
./runtests.sh --rerunfailed --retry 3

# Single test class
./runtests.sh CacheTests

# Statistics
./runtests.sh --stats
./getFailedTests.sh  # list failed methods
```

Run `./runtests.sh --help` to see every option.

### Multi-Backend Testing

Tests are parameterized to run against multiple drivers. Use `--driver` to select:

```bash
# InMemory only (fastest, default)
./runtests.sh --driver inmem

# Against external MongoDB with all drivers (pooled + single + inmem)
./runtests.sh --uri mongodb://mongo1,mongo2/testdb --driver all

# Against external MongoDB with pooled driver only
./runtests.sh --uri mongodb://mongo1,mongo2/testdb --driver pooled

# Against PoppyDB (auto-starts local server)
./runtests.sh --poppydb --driver pooled  # --morphium-server is a deprecated alias
```

**Complete test coverage** requires running against all backends:
```bash
# 1. Fast in-memory tests
./runtests.sh --driver inmem

# 2. Real MongoDB tests
./runtests.sh --uri mongodb://your-mongodb/testdb --driver all

# 3. PoppyDB tests
./runtests.sh --poppydb --driver pooled  # --morphium-server is a deprecated alias
```

Tests use a unified `MultiDriverTestBase` with parameterized drivers (each test declares which
drivers it supports via `@MethodSource`), isolated per parallel slot with unique databases, and
support method-level reruns (`--rerunfailed`) for fast, targeted retries.

### Test configuration precedence

`TestConfig` consolidates all test settings. Priority order:
1. System properties (`-Dmorphium.*`)
2. Environment variables (`MORPHIUM_*`, `MONGODB_URI`)
3. `src/test/resources/morphium-test.properties`
4. Defaults (localhost:27017)

## 🔧 InMemoryDriver

### MongoDB-free testing

The in-memory driver provides a largely MongoDB-compatible data store fully in memory:

**Features**
- ✅ Full CRUD operations
- ✅ Rich query operator coverage
- ✅ Aggregation stages such as `$match`, `$group`, `$project`
- ✅ Single-instance transactions
- ✅ Basic change streams
- ✅ JavaScript `$where` support

**Performance**
- Significantly faster than external MongoDB for tests
- No network latency
- No disk I/O
- Ideal for CI/CD pipelines

**Usage**
```bash
# All tests with the in-memory driver
./runtests.sh --driver inmem

# Specific tests
mvn test -Dmorphium.driver=inmem -Dtest="CacheTests"
```

See `docs/howtos/inmemory-driver.md` for feature coverage and limitations. For PoppyDB — the
standalone MongoDB-compatible server — see the
[🌱 PoppyDB section](#-poppydb--mongodb-compatible-in-memory-server) above.

## 🚀 Production Use Cases

Organizations run Morphium in production for:
- **E-commerce**: order processing with guaranteed delivery
- **Financial services**: coordinating transactions across microservices
- **Healthcare**: patient-data workflows with strict compliance
- **IoT platforms**: device state synchronization and command distribution
- **Content management**: document workflows and event notifications

## 🤝 Community & Contribution

### Stay in touch
- **Blog**: https://caluga.de
- **GitHub**: [sboesebeck/morphium](https://github.com/sboesebeck/morphium)
- **Issues**: Report bugs or request features on GitHub

### Showcase
Check out the **[Quarkus Morphium Showcase](https://morphium.kopp-cloud.de/)** by Heiko Kopp ([Bardioc1977](https://github.com/Bardioc1977)) — a live, interactive demo of Morphium with Quarkus covering CRUD, caching, aggregation pipelines, geospatial queries, messaging, transactions, Jakarta Data, and more. A great way to explore what Morphium can do before writing a single line of code.

### Contributing

We appreciate pull requests! Areas where help is especially welcome:
- **InMemoryDriver**: expanding MongoDB feature coverage
- **Documentation**: tutorials, examples, translations
- **Performance**: profiling and benchmarks
- **Tests**: broader scenarios and regression coverage

**How to contribute**
1. Fork the repository
2. Create a feature branch **from `develop`** (`git checkout -b feature/AmazingFeature develop`)
3. Commit your changes (`git commit -m 'Add AmazingFeature'`)
4. Push the branch (`git push origin feature/AmazingFeature`)
5. Open a pull request **against `develop`** (not `master`)

**Important:** `master` is only updated during releases. All PRs must target `develop`.

**Tips**
- Respect test tags (`@Tag("inmemory")`, `@Tag("poppydb")`)
- Run `./runtests.sh --tags core` before submitting
- Update documentation when you change APIs

## 📜 License

Apache License 2.0 – see [LICENSE](LICENSE) for details.

## 🙏 Thanks

Thanks to every contributor who helped ship the Morphium 6.2.x releases and to the MongoDB community for continuous feedback.

A special thank-you goes to **Heiko Kopp** ([Bardioc1977](https://github.com/Bardioc1977)) for countless contributions, real-world feedback from large-scale production deployments, and the excellent [Quarkus Morphium Showcase](https://morphium.kopp-cloud.de/).

---

**Questions?** Open an issue on [GitHub](https://github.com/sboesebeck/morphium/issues) or browse the [documentation](docs/index.md).

**Planning an upgrade?** Follow the [migration guide](docs/howtos/migration-v5-to-v6.md).

Enjoy Morphium! 🚀

*Stephan Bösebeck & the Morphium team*
