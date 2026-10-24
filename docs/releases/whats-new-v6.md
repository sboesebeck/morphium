# What is new in Morphium 6.x

The release highlights of the 6.x line. The sections for 6.2 and older moved here from the README; the 6.3 highlights are also in the README. The complete list per release is in the
[CHANGELOG](../../CHANGELOG.md); upgrade steps are in the [migration guides](../howtos/migration-v6_2-to-v6_3.md).

## What’s new in v6.3

### About the 6.3.3 → 6.3.6 release storm (August 2026)

Four patch releases in one week is not our usual cadence: an AI-assisted deep-code-review of the
change-stream and replication path surfaced a class of load-only bugs (silent event loss on
resume, live events overtaking history replay, unbounded memory pinning) that no user had ever
reported — exactly the kind that doesn't file an issue, it just shows up months later as quietly
diverged data. **6.3.4** shipped those fixes but introduced a client-side resume-token loop
(#329), fixed the same day in **6.3.5**; **6.3.6** then fixed a related connection-pool topology
bug (#330) found on our own staging cluster. Full story per fix in the [CHANGELOG](../../CHANGELOG.md).

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
`morphium-jakarta-data` implements [Jakarta Data 1.0](https://jakarta.ee/specifications/data/1.0/) on top of Morphium's query engine — `@Repository` interfaces with query derivation from method names, JDQL via `@Query` (including `GROUP BY`/`HAVING` compiled into an aggregation pipeline), offset and cursor/keyset pagination. Two framework modules build on it: `quarkus-morphium` for CDI integration (config mapping, `@MorphiumTransactional`, health checks, Dev Services, Dev UI, GraalVM native-image support, build-time repository generation via Gizmo), and `spring-boot-morphium` for Spring Boot (auto-configuration, type-safe `@ConfigurationProperties`, `@MorphiumTransactional`, an Actuator health indicator) — a Spring Boot app on Morphium, not a Spring Data MongoDB replacement. All three are optional — core has no dependency on any of them, and `-DskipExtensions` still produces a core-only build. See [Jakarta Data](../jakarta-data.md), [Quarkus Extension](../quarkus-extension.md) and [Spring Boot Starter](../spring-boot.md).

**Note:** the Quarkus extension moved from `io.quarkiverse.morphium:quarkus-morphium:1.2.0` to `de.caluga:quarkus-morphium:6.3.0`. Coordinates only — no package renames, no API changes.

### DualChannelMessaging (beta)
A third messaging implementation: the standard single collection and cursor for broadcast/topic traffic, plus a dedicated per-recipient collection with its own cursor and dispatcher thread for directed messages and answers. Select it with `cfg.messagingSettings().setMessagingImplementation("DualChannelMessaging")`. Beta on purpose — past saturation it trades a little throughput for markedly better tail latency. See `../howtos/messaging-implementations.md`.

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

Upgrading is covered step by step in the [migration guide](../howtos/migration-v6_2-to-v6_3.md); see [CHANGELOG](../../CHANGELOG.md) for full details.

## What’s new in v6.2

### Multi-Module Maven Build
Morphium is now a multi-module project: `morphium-parent` (BOM), `morphium` (core library), and `poppydb` (server). The core library `de.caluga:morphium` no longer drags in server dependencies (Netty, etc.) — 90% leaner for users who just need the ODM.

### PoppyDB – Standalone MongoDB-Compatible Server
The former MorphiumServer became an independent module `de.caluga:poppydb` in 6.2 — see the
[PoppyDB section of the README](../../README.md#-poppydb--mongodb-compatible-in-memory-server) for what it does and
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

See [CHANGELOG](../../CHANGELOG.md) for full details.

## Upgrading from 6.1.x to 6.2.x

Two breaking changes: `MorphiumDriverException` now extends `RuntimeException` instead of
`Exception` (drop `MorphiumDriverException` from multi-catches and `throws` clauses), and the
embedded server was extracted into its own module and renamed `MorphiumServer` → `PoppyDB`
(`de.caluga:poppydb`, package `de.caluga.poppydb`, tag `@Tag("poppydb")`) — the wire handshake
still answers to both names, so mixed-version replica sets keep working. Config setters also
moved to typed sub-objects (`cfg.connectionSettings().setDatabase(...)`); the old flat setters
stay deprecated-but-functional through the whole 6.x line (see the
[Deprecation Policy](../deprecation-policy.md)). Full checklist and code samples in the
[migration guide](../howtos/migration-v6_1-to-v6_2.md).

## What’s new in v6.1.x

### MONGODB-X509 Client-Certificate Authentication
- Connect to MongoDB instances that require mutual TLS / x.509 client certificates
- Configure via `AuthSettings.setAuthMechanism("MONGODB-X509")` together with the existing `SslHelper` mTLS setup

### `@Version` – Optimistic Locking
Prevents lost updates in concurrent environments without requiring pessimistic database locks. See `../howtos/optimistic-locking.md` for the full guide.

## What’s new in v6.0

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

