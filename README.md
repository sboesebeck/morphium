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
community figures. The two rows measure different things. **One-way** counts send→receipt
only (no processing, no reply): ~870–1,250 msg/s against a 3-node MongoDB replica set
(depending on the client host); PoppyDB
runs in-process and therefore scales with the host — ~770 msg/s on a small 4-core CI host,
~2,100 msg/s on an M1 Max laptop, ~4,300–4,900 msg/s on an M1 Ultra desktop — in-process,
it simply scales with the host. **Round-trip** measures complete ping-pongs (request out,
response received): 223 msg/s at 4.5 ms latency against PoppyDB vs. 89 msg/s at 11.3 ms
against the MongoDB replica set — 2.5× the throughput at less than half the latency, thanks
to PoppyDB and Morphium Messaging being optimized for each other (both sides detect the
counterpart). Re-measured 2026-08-07 with the Morpheus load generator (100 msg/s fixed
rate, 5 sender threads, Mac Studio client): median round-trip 2.4 ms against a local
PoppyDB replica set vs 5.7 ms against the MongoDB replica set — note that this run was
*not* like-for-like (PoppyDB local, MongoDB over the network), so part of that gap is
network, not broker. A **symmetric re-measurement on 2026-08-11** — client inside the
homelab network, both backends separate processes on dedicated hosts at equal distance —
confirms the ratio at **2.34–2.49×**: MongoDB p50 4.97/5.12 ms vs PoppyDB p50 2.13/2.06 ms
over two runs (3001 pings each, zero loss). The tail is where they really diverge: MongoDB
p99 42–129 ms at 100 msg/s on an idle cluster, PoppyDB below 7 ms, with 2.5–3× lower jitter.
A same-session A/B attributes 8–18 % lower median RTT to the 2026-08 messaging
optimizations (answers dispatched before the `processed_by` write, non-exclusive messages
processed straight from the change-stream `fullDocument`). PoppyDB's strength is latency,
not raw one-way throughput on constrained hardware. Persistence there is snapshot-based, see the
[PoppyDB section](#-poppydb--mongodb-compatible-in-memory-server) below._

_**How real is Kafka's 100K+ figure — and how big is the gap really?** We measured both on
one and the same laptop-class machine (Apple M1 Max, single-node Kafka 4.1, ~200-byte
payload, one consumer, end-to-end from first send to last receipt — the same setup as our
[one-way benchmark](poppydb/src/test/java/de/caluga/poppydb/MessagingOneWayThroughputBenchmark.java)).
In its normal operating mode — asynchronous sends, client-side batching — Kafka reached
~900K msg/s, so the 100K+ column is real and even conservative on modern hardware. But
forced into Morphium's semantics, where every message is sent synchronously and individually
acknowledged by the broker (4 sender threads, `acks=all`), Kafka drops to ~8–10K msg/s vs.
~1,800 msg/s for Morphium+PoppyDB on the same machine — a factor of 4–5, not 100+. Kafka's
headline throughput comes almost entirely from batching thousands of records into each
network round-trip (with no per-message broker ack and, by default, no per-message fsync —
durability comes from replication), not from faster per-message handling. Morphium Messaging
deliberately sends each message as an individually acknowledged insert; the remaining 4–5×
is the price of a full ODM insert (object mapping, wire protocol, change-stream dispatch)
per message._

_**Where exactly does Morphium's per-message cost go?** Decomposed on the same machine: a
raw `morphium.insert` of the very same Msg document into PoppyDB runs at ~4,600 docs/s —
0.33 ms per operation single-threaded, on par with Kafka's ~0.5 ms per-request latency, so
the wire protocol and server are not the problem. An active change-stream watcher brings
that to ~3,600 docs/s (fanout, ~20 %), and the full messaging layer (topic registry,
listener dispatch, processing queue) lands at ~2,500–2,800 msg/s once the JVM is warm — the
~1,800 msg/s above is a cold-start figure. The 2026-08 optimization round (duplicate-`_id`
insert pre-check is an O(1) index lookup instead of an O(N) collection scan, one dead
messaging index removed, non-exclusive messages processed straight from the change-stream
`fullDocument` with no per-message re-read) additionally made insert cost independent of
collection size — the former O(N) `_id` scan degraded to double-digit inserts/s on a
200K-document collection, the index lookup holds >200K inserts/s there (A/B-measured on an
M1 Ultra); its effect on the M1-Max figures in this paragraph has not been re-measured yet.
The real limiting
factor is write concurrency: PoppyDB's in-memory backend serializes writes per collection,
so raw throughput plateaus at ~4,600
inserts/s no matter how many sender threads you add (1 thread: ~3,100/s; 2+: ~4,300–4,600/s).
Per-message-acknowledged throughput on par with Kafka's synchronous mode (~8–10K msg/s) is
the realistic ceiling for future server-side concurrency work — not 100K+, which no system
reaches without batching._

_**Does client-side batching help Morphium the way it helps Kafka?** Yes, when it's a genuine
single-round-trip bulk insert — no annotation, no tuning: against MongoDB it roughly
quadruples end-to-end throughput over unbatched `sendMessage()` (1128 vs. 291 msg/s, chunks of
100). The `@WriteBuffer` annotation, tried as the "just annotate it" shortcut, turned out to
be the wrong tool — it predates Morphium Messaging and flushes on a polling housekeeping
thread, which becomes a throughput *ceiling*, not a booster, once producers outrun the poll
interval. Full numbers, and two real bugs this probe surfaced along the way (both already
fixed on `develop`), in the ["Batch Send Throughput"](docs/v5-vs-v6-performance.md#batch-send-throughput-writebuffer-vs-client-side-bulk-insert)
section._

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
    <version>6.3.6</version>
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
curl -O https://repo1.maven.org/maven2/de/caluga/poppydb/6.3.6/poppydb-6.3.6-cli.jar

# start for a test run: --no-config keeps it isolated from any stray
# ~/.config/poppydb/config on a developer machine - same flags, same behavior in CI
java -jar poppydb-6.3.6-cli.jar --port 27017 --no-config
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
java -jar poppydb-6.3.6-cli.jar --port 27017 --dump-dir ./data --dump-interval 300
```

Snapshots every 5 minutes, final dump on shutdown, automatic restore on the next start.
Config can also live in a properties file: `--cfg /etc/poppydb/config` (validate it upfront
with `--check-config`, inspect the effective result with `--print-config`).

### How-to: 3-node replica set

One process per node, each with the same seed list — election picks the primary, failover is
automatic:

```bash
java -jar poppydb-6.3.6-cli.jar -p 17017 --rs-name myrs \
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

## 🚀 What’s New in v6.3

### Two Optional Integration Modules
`morphium-jakarta-data` implements [Jakarta Data 1.0](https://jakarta.ee/specifications/data/1.0/) on top of Morphium's query engine — `@Repository` interfaces with query derivation from method names, JDQL via `@Query` (including `GROUP BY`/`HAVING` compiled into an aggregation pipeline), offset and cursor/keyset pagination. `quarkus-morphium` builds on it for CDI integration: config mapping, `@MorphiumTransactional`, health checks, Dev Services, Dev UI, GraalVM native-image support, and build-time repository generation via Gizmo. Both are optional — core has no dependency on either, and `-DskipExtensions` still produces a core-only build. See [Jakarta Data](docs/jakarta-data.md) and [Quarkus Extension](docs/quarkus-extension.md).

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

### Breaking: MorphiumDriverException is now unchecked

`MorphiumDriverException` extends `RuntimeException` instead of `Exception`. This eliminates boilerplate `catch-wrap-rethrow` blocks but requires attention in existing code:

```java
// Multi-catch — simplify (MorphiumDriverException IS a RuntimeException now)
// Before:
catch (RuntimeException | MorphiumDriverException e) { ... }
// After:
catch (RuntimeException e) { ... }

// throws declarations — can be removed (but still compile if left in)
// Before:
public void doStuff() throws MorphiumDriverException { ... }
// After:
public void doStuff() { ... }

// Standalone catch — works unchanged
catch (MorphiumDriverException e) { ... }  // still compiles
```

### Breaking: MorphiumServer → PoppyDB

The embedded MongoDB-compatible server was extracted to its own module and renamed:

| | 6.1.x | 6.2.x |
|---|---|---|
| Maven artifact | included in `morphium` | separate: `de.caluga:poppydb:6.3.6` |
| Package | `de.caluga.morphium.server` | `de.caluga.poppydb` |
| Main class | `MorphiumServer` | `PoppyDB` |
| CLI JAR | `morphium-*-server-cli.jar` | `poppydb-*-cli.jar` |
| Test tag | `@Tag("morphiumserver")` | `@Tag("poppydb")` |

If you use PoppyDB in tests, add the dependency:
```xml
<dependency>
    <groupId>de.caluga</groupId>
    <artifactId>poppydb</artifactId>
    <version>6.3.6</version>
    <scope>test</scope>
</dependency>
```

Wire-protocol compatibility is preserved — PoppyDB responds to both `poppyDB` and `morphiumServer` in the hello handshake.

### Deprecated: Direct config setters → sub-objects

`MorphiumConfig` now organizes settings into typed sub-objects. The old setters still work but are `@Deprecated`:

```java
// 6.1.x style (deprecated but functional)
cfg.setDatabase("mydb");
cfg.addHostToSeed("localhost", 27017);

// 6.2.x style (preferred)
cfg.connectionSettings().setDatabase("mydb");
cfg.clusterSettings().addHostToSeed("localhost", 27017);
cfg.driverSettings().setDriverName("PooledDriver");
```

Available sub-objects: `connectionSettings()`, `clusterSettings()`, `driverSettings()`, `messagingSettings()`, `cacheSettings()`, `authSettings()`, `threadPoolSettings()`, `objectMappingSettings()`, `writerSettings()`.

### New: Multi-Module Maven Structure

The `morphium` core artifact no longer bundles server dependencies (Netty, etc.). If you only use Morphium as ODM, your dependency tree is ~90% leaner — no changes to your pom needed.

### Migration checklist

1. **Search for `catch (RuntimeException | MorphiumDriverException`** — simplify to `catch (RuntimeException`
2. **Search for `import de.caluga.morphium.server`** — replace with `import de.caluga.poppydb`
3. **Search for `MorphiumServer`** — rename to `PoppyDB`
4. **Search for `@Tag("morphiumserver")`** — rename to `@Tag("poppydb")`
5. **Add `poppydb` dependency** if you use the embedded server in tests
6. **Optional:** migrate direct config setters to sub-object style
7. **Optional:** adopt new features (`@Reference(cascadeDelete)`, `@AutoSequence`, `@Version`)

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
  <version>6.3.6</version>
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

**New in v6.1**
- ✅ **Unified test base**: All tests now use `MultiDriverTestBase` with parameterized drivers
- ✅ **Driver selection**: Each test declares which drivers it supports via `@MethodSource`
- ✅ **Parallel safe**: Tests isolated per parallel slot with unique databases

**New in v6.0**
- ✅ **Method-level reruns**: `--rerunfailed` only re-executes failing methods
- ✅ **No more hangs**: known deadlocks resolved
- ✅ **Faster iteration**: noticeably quicker partial retries
- ✅ **Better filtering**: class-name filters now reliable

Run `./runtests.sh --help` to see every option.

### Test configuration precedence

`TestConfig` consolidates all test settings. Priority order:
1. System properties (`-Dmorphium.*`)
2. Environment variables (`MORPHIUM_*`, `MONGODB_URI`)
3. `src/test/resources/morphium-test.properties`
4. Defaults (localhost:27017)

## 🔧 PoppyDB & InMemoryDriver

### InMemoryDriver – MongoDB-free testing

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

See `docs/howtos/inmemory-driver.md` for feature coverage and limitations.

### PoppyDB – Standalone MongoDB replacement

PoppyDB (formerly MorphiumServer) runs the Morphium wire-protocol driver in a separate process, allowing it to act as a lightweight, in-memory MongoDB replacement.

**Maven dependency** (server module):
```xml
<dependency>
  <groupId>de.caluga</groupId>
  <artifactId>poppydb</artifactId>
  <version>6.3.6</version>
</dependency>
```

**Building the Server**

```bash
mvn clean package -pl poppydb -am -Dmaven.test.skip=true
```

This creates `poppydb/target/poppydb-6.3.6-cli.jar`.

**Running the Server**

```bash
# Start the server on the default port (17017)
java -jar poppydb/target/poppydb-6.3.6-cli.jar

# Start on a different port
java -jar poppydb/target/poppydb-6.3.6-cli.jar --port 8080

# Start with persistence (snapshots)
java -jar poppydb/target/poppydb-6.3.6-cli.jar --dump-dir ./data --dump-interval 300
```

**Replica Set Support (Experimental)**

PoppyDB supports basic replica set emulation. Start multiple instances with the same replica set name and seed list:

```bash
java -jar poppydb/target/poppydb-6.3.6-cli.jar --rs-name my-rs --rs-seed host1:17017,host2:17018
```

**Use cases**
- Local development without installing MongoDB
- CI environments
- Embedded database for desktop applications
- Smoke-testing MongoDB tooling (mongosh, Compass, mongodump, ...)

**Current limitations**
- No sharding support
- Some advanced aggregation operators and joins still missing

See `docs/poppydb.md` for more details on persistence and replica sets.

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
