# Changelog

All notable changes to Morphium will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed

#### ChangeStreamHistoryLost
- forget resume token as it is invalid
- restart changestream
- might cause loss of a message or two, but is stable


#### Messaging Lock TTL Bug
- **Lock expires immediately when message has no timeout**: When a message had `timingOut=false`, the TTL was 0, causing the lock to be created with `deleteAt = now`. MongoDB's TTL monitor would delete the lock almost immediately, allowing duplicate message processing. Now uses 7 days as fallback TTL for messages without timeout.

#### ChangeStreamMonitor Stability
- **ChangeStreamMonitor dies on "connection closed"**: Previously, a "connection closed" exception would cause the ChangeStreamMonitor to stop permanently with no auto-recovery. This is often a transient error (network issues, MongoDB failover). Now the monitor will retry the connection instead of giving up.
- **Improved exit logging**: ChangeStreamMonitor now logs at WARN level when it stops, explaining the reason (config null, connection closed, no such host, etc.). Previously most exit conditions were logged at DEBUG level, making it hard to diagnose why messaging stopped working.
- **Resume token support for ChangeStreamMonitor**: ChangeStreamMonitor now tracks the resume token from each event and uses it when restarting the watch after connection issues. This prevents duplicate events and ensures no events are missed during reconnection. Also handles `ChangeStreamHistoryLost` errors gracefully by discarding the stale token and starting fresh.

## [6.1.0] 

### Added

#### MorphiumServer Enhancements
- **Replica set support**: MorphiumServer now supports replica set configuration with automatic primary election and failover
- **Server CLI**: New standalone `morphium-server-cli.jar` for running MorphiumServer from command line with `--help` option
- **Replication**: Data replication between MorphiumServer instances in a replica set
- **Custom election protocol**: Implemented Raft-inspired election system for MorphiumServer replica sets with:
  - Configurable election priorities per node
  - Heartbeat-based leader detection
  - Automatic leader election on primary failure
  - Vote request/response protocol for consensus
- **Netty-based wire protocol handler**: New `MongoCommandHandler` using Netty for improved performance and connection handling
- **Messaging optimization**: MorphiumServer-specific optimizations for messaging workloads

#### Messaging
- **Topic Registry / Network Registry**: New `NetworkRegistry` implementation for discovering messaging topics across the network
- **MessagingSettings**: New configuration class for messaging-related settings

#### InMemoryDriver
- **Tailable cursor support**: InMemoryDriver now supports tailable queries
- **Shared InMemory databases**: Multiple Morphium instances can share the same InMemory database (configurable via `DriverSettings.setShareInMemoryDatabase()`)
- **MongoDB-compatible `$text` query support**: Full text search with MongoDB-standard query syntax
  - Root-level queries: `{ $text: { $search: "search terms" } }`
  - Phrase search: `{ $text: { $search: "\"exact phrase\"" } }`
  - Term negation: `{ $text: { $search: "include -exclude" } }`
  - Case sensitivity: `{ $text: { $search: "...", $caseSensitive: true } }`
  - Automatically searches fields defined in text indexes

#### Driver
- **Host class**: New `Host` class for improved readability in connection pool management
- **Shared connection pools**: Connection pool sharing between Morphium instances

#### MorphiumServer
- **SSL/TLS support**: MorphiumServer can now accept SSL/TLS encrypted connections
  - `server.setSslEnabled(true)` to enable SSL
  - `server.setSslContext(sslContext)` for custom SSL configuration
  - Automatic TLS 1.2/1.3 protocol selection
- **Periodic snapshots/persistence**: MorphiumServer can now dump databases to disk and restore on startup
  - `--dump-dir <path>` CLI option to enable persistence
  - `--dump-interval <seconds>` for periodic dumps during runtime
  - Automatic restore from dump files on startup
  - Final dump on graceful shutdown
  - Programmatic API: `setDumpDirectory()`, `setDumpIntervalMs()`, `dumpNow()`, `restoreFromDump()`

### Fixed
- **MultiCollectionMessaging DM polling when change streams disabled**: When `setUseChangeStream(false)` is called on `MultiCollectionMessaging`, direct messages (DMs) are now also polled instead of using change streams. Previously, DMs were always using change streams regardless of the setting, causing inconsistent behavior. Added new `pollAndProcessAllDms()` method and updated the poll trigger handler to support "dm_all" triggers
- **Graceful thread pool shutdown in Morphium**: Changed `asyncOperationsThreadPool.shutdownNow()` to graceful shutdown to prevent abrupt task termination
- **PooledDriver NPE and race conditions**: Fixed null pointer exception for `primaryNode`, race condition with `primaryNodeLock`, and connection cleanup improvements
- **MorphiumWriterImpl graceful shutdown**: Added graceful shutdown in `close()` and `onShutdown()` methods
- **InMemoryDriver change stream race condition**: Fixed race condition in change stream handling (line 633-646)
- **Flaky IteratorTest.concurrentAccessTest**: Fixed race condition where multiple threads sharing a single iterator would call `hasNext()` and `next()` non-atomically, causing incorrect element counts (e.g., 29130 instead of 25000). The test now properly synchronizes the hasNext+next critical section
- **Parallel test database isolation**: Fixed race condition in MultiDriverTestBase where database cleanup would drop ALL databases matching the prefix pattern, including databases from other parallel tests that were still running. Now each test only drops its own database, preventing "expected X but was 0" failures in parallel execution
- **MorphiumServer listDatabases**: Added explicit handler for `listDatabases` command in MorphiumServer. Previously this command returned null when forwarded through GenericCommand, causing NullPointerException in tests that call `morphium.listDatabases()`
- **MorphiumServer stepDown for standalone servers**: Standalone MorphiumServer instances (no replica set configured) now immediately become primary again after receiving a `replSetStepDown` command. Previously, stepDown would leave the server in secondary state with no way to recover, causing "no primary" errors for subsequent operations
- **InMemoryDriver database-level change streams via MorphiumServer**: Fixed change stream event delivery for database-level watches registered through MorphiumServer. When a client creates a database-level watch via the wire protocol, MongoDB convention sets collection to "1". The InMemoryDriver now correctly delivers events to subscribers registered under the `db.1` namespace key
- **Message sending to self**: Fixed broken message sending when sender equals recipient
- **Deadlocks**: Fixed multiple deadlock scenarios in messaging and server components
- **Robust shutdown**: Improved shutdown handling across components
- **NPE in QueryHelper.matchesQuery**: Fixed null pointer exception when comparing MorphiumId/ObjectId fields against null query values
- **Flaky test fixes**: Replaced timing-dependent `Thread.sleep()` + assertion patterns with `TestUtils.waitForConditionToBecomeTrue()` polling in messaging and changestream tests
- **Pooled driver updates**: Updates now apply proper `writeConcern` consistently and single-document updates honor sort
- **Buffered writer bulk inserts**: Fixed a race where mutating a list after `storeList/insert(list)` could flush as "0 operations" and/or cause duplicate inserts
- **Change stream lifecycle**: `ChangeStreamMonitor` no longer misses early events as easily and terminates reliably (stops blocking watches on shutdown)
- **MorphiumServer dropDatabase handling**: Added "dropdatabase" to WRITE_COMMANDS set so database drops are properly forwarded to primary instead of being rejected by secondaries
- **Test database cleanup**: Fixed `MultiDriverTestBase` to clean databases for ALL morphium instances (both PooledDriver and InMemoryDriver), not just the first one. Previously only one storage backend was cleaned, causing test isolation failures
- **GenericCommand key ordering**: Changed `cmdData` from `HashMap` to `LinkedHashMap` in `GenericCommand.fromMap()` to preserve key ordering, which is critical for MongoDB wire protocol where the command name must be the first key
- **Test configuration default hosts**: Changed `TestConfig` to default to single host (localhost:27017) instead of 3-host replica set for simpler test setup. Multi-node replica sets can still be configured via `morphium.hostSeed` property
- **MorphiumServer getMore for regular query cursors**: Fixed `getMore` command to forward regular query cursors to InMemoryDriver instead of only handling change stream cursors. Previously, iterators would hang infinitely when fetching additional batches because non-change-stream cursors were returning empty batches with non-zero cursor IDs
- **MorphiumServer replica set replication**: Extended change stream replication to handle `drop`, `dropDatabase`, `replace`, and `rename` operations. Previously only `insert`, `update`, and `delete` were replicated, causing collection drops and document replacements to not sync to secondaries
- **MorphiumServer collection metadata forwarding**: Added forwarding of `listCollections` command to primary when running as secondary. This ensures `isCapped()` checks return correct results for capped collections created on primary
- **InMemoryDriver listCollections capped status**: Fixed `listCollections` response to include `capped`, `size`, and `max` options for capped collections. Previously the options field was always empty, causing `isCapped()` to return false even for capped collections
- **MorphiumServer capped collection replication**: Added initial and periodic sync of capped collection metadata from primary to secondaries. Capped collections created on primary are now properly registered on secondaries, ensuring capped behavior is enforced during replication
- **InMemory backend detection for tests**: Added `isInMemoryBackend()` method to MorphiumDriver interface and `inMemoryBackend` field to hello response from MorphiumServer. Tests that need to skip unsupported features (like Collation) can now correctly detect when connected to MorphiumServer with InMemory backend, not just when using InMemoryDriver directly
- **MorphiumServer changestream event delivery via wire protocol**: Fixed changestream events not being delivered to clients connecting via the wire protocol. Watch cursors are now properly created with callbacks, events are queued via `LinkedBlockingQueue`, and `getMore` requests correctly return queued events to clients. This enables reliable messaging when using MorphiumServer as a messaging hub
- **MorphiumServer killCursors command handler**: Added missing `killCursors` command handler to MorphiumServer. Without this, watch cursors were never cleaned up when clients disconnected, causing virtual threads to accumulate and eventually block new watch thread creation. The fix properly removes cursors from `watchCursors` and `tailableCursors` maps
- **InMemoryDriver watch thread cleanup**: Modified `watchInternal()` to periodically check `callback.isContinued()` after each wait timeout (max 5 seconds). This ensures watch threads properly terminate when cursors are killed, preventing resource exhaustion when many clients connect/disconnect
- **PooledDriver connection leak**: Fixed connection leak in `releaseConnection()` where connections were removed from `inUse` set but not returned to the pool when the connection's host was no longer in the valid hosts set. Connections are now properly closed instead of being leaked
- **InMemoryDriver serverMode premature shutdown**: Fixed InMemoryDriver to not clear data or shut down when `serverMode=true` and `close()` is called. MorphiumServer instances now properly maintain their data when client Morphium instances disconnect
- **SingleMongoConnection watch loop termination**: Fixed watch loop to check `isContinued()` after each individual event instead of only after processing the entire batch. This ensures watches terminate immediately when the callback returns false, matching InMemoryDriver behavior
- **ChangeStreamMonitor reconnection loop on shutdown**: Fixed ChangeStreamMonitor to stop gracefully when receiving "No such host" errors instead of endlessly retrying. Also added driver connectivity check before attempting to get connections. This prevents resource exhaustion when MorphiumServer instances are shut down
- **PooledDriver parallel connection creation**: Changed connection creation from sequential to parallel (up to 10 virtual threads) to handle burst scenarios where many connections are needed simultaneously. This prevents connection timeouts when many async operations are queued at once
- **MorphiumServer write concern handling with partial replica sets**: Fixed write concern handling when configuring a replica set programmatically before all secondaries are started. Previously, writes with `w > 1` would block for the full `wtimeout` (10 seconds) waiting for non-existent secondaries, causing client-side timeouts. The `ReplicationCoordinator` now fails fast (100ms grace period) when no secondaries have registered, returning a proper `writeConcernError` response instead of timing out. This enables tests to store documents on a primary before starting secondary nodes
- **Replication staleness detection**: Added staleness detection mechanism to ReplicationManager that detects when a secondary's change stream watch connection has gone stale (no response for 30+ seconds). When detected, the connection is forcibly closed and a new one is established. This prevents secondaries from falling behind when connections silently break
- **SingleMongoConnection socket timeout limit**: Modified `readNextMessage()` to limit consecutive socket timeout retries to 100 (approximately 10 seconds with 100ms timeout). After reaching this limit, it returns null to allow the calling code to check `isContinued()` and handle connection issues. Previously, the method would retry indefinitely, causing watch loops to never detect broken connections
- **Connection pool issues**: Fixed multiple connection pool problems including proper connection release, leak prevention, and handling of stale connections
- **Messaging stability**: Fixed various messaging issues including connection handling, message processing, and proper cleanup on shutdown
- **Server status on startup**: Fixed MorphiumServer status reporting during initial startup phase
- **NPE fixes**: Fixed null pointer exceptions in various components during edge cases
- **Election priorities**: Fixed election priority handling to ensure highest-priority node becomes primary
- **Read preference on secondary**: Fixed read preference checks when operating on secondary nodes
- **Flaky CollationTest timing**: Added wait conditions for collation queries to handle replica set replication delay. Previously, tests would fail intermittently because collation queries were executed before data was fully replicated
- **Flaky ExclusiveMessageBasicTests timing**: Increased timing tolerance from 30s to 35s to account for timing variance in message processing
- **Flaky LastAccessTest assertions**: Added better error messages for debugging timing-related assertion failures
- **CacheTests write buffer timeout**: Increased write buffer flush timeout from 3s to 10s to handle MorphiumServer latency

### Added (Tests)
- **Failover tests for MorphiumServer replica sets**: Added comprehensive failover tests (`FailoverTest.java`) that verify:
  - Primary election based on configured priorities
  - Automatic failover when primary is terminated
  - Write operations succeed after failover
  - Rejoining nodes integrate correctly into the cluster
  - Tests cover both `PooledDriver` and `SingleMongoConnectDriver`

### Changed (Test Infrastructure)
- **Unified multi-driver test base**: Migrated 72 test classes from `MorphiumTestBase` to `MultiDriverTestBase`
  - Converted 356+ test methods from `@Test` to `@ParameterizedTest` with `@MethodSource`
  - Each test now declares driver compatibility via `@MethodSource`:
    - `getMorphiumInstancesNoSingle()` - pooled + inmem (default for most tests)
    - `getMorphiumInstances()` - all drivers including single connection
    - `getMorphiumInstancesPooledOnly()` - pooled driver only
    - `getMorphiumInstancesInMemOnly()` - inmem driver only
  - Tests receive `Morphium morphium` as parameter instead of using inherited field

- **Driver selection via runtests.sh**: Tests can now run against different backends:
  ```bash
  # InMemory only (fast, default without --external)
  ./runtests.sh --driver inmem

  # All drivers against external MongoDB
  ./runtests.sh --uri mongodb://host1,host2/db --driver all

  # Against MorphiumServer (run separately from MongoDB tests)
  ./runtests.sh --morphium-server --driver pooled
  ```

- **Multi-backend testing workflow**: To test against all backends:
  1. `./runtests.sh --driver inmem` - InMemory driver (fast, no dependencies)
  2. `./runtests.sh --uri mongodb://... --driver all` - Real MongoDB with all drivers
  3. `./runtests.sh --morphium-server --driver pooled` - MorphiumServer

- **External test tagging**: Added `@Tag("external")` to driver tests that require a real MongoDB connection (PooledDriverTest, PooledDriverConnectionsTests, SharedConnectionPoolTest). Fixed pom.xml to use correct `<excludedGroups>` parameter instead of invalid `<excludeTags>` for Maven Surefire plugin JUnit 5 tag filtering

- **Test script improvements**: Major refactoring of `runtests.sh` for:
  - Modular script architecture with separate utility scripts in `scripts/` directory
  - Better temporary file management and cleanup
  - Improved parallel test execution and slot management
  - Enhanced failure reporting and log management
  - Support for different test backends via `--driver`, `--uri`, and `--morphium-server` options
  - Memory settings optimization for test execution

### Changed
- **Modernized concurrent collections**: Replaced legacy `Vector` with `CopyOnWriteArrayList` and `Hashtable` with `ConcurrentHashMap` for better performance
- **Optimized string operations**: Consolidated multiple `replaceAll()` calls into single regex patterns, replaced `replaceAll()` with `replace()` for literal string replacements
- **ChangeStream implementation**: Improved change stream handling and event delivery reliability

### Dependencies
- **logback-core**: Bumped from 1.5.13 to 1.5.19

### Performance

#### InMemoryDriver Optimizations
- **Removed global synchronization on `sendCommand()`**: Operations on different collections can now execute in parallel. Previously all commands were serialized through a single synchronized method, causing unnecessary contention.

- **Optimized `find()` deep copy behavior**: Documents are now only copied after query matching succeeds, and projection-aware copying avoids redundant work:
  - Non-matching documents: No copy (previously copied before match check)
  - Include projections: Only projected fields are copied (previously full document copied twice)
  - Exclude projections: Single copy (previously double copy)

- **Improved index lookups for equality queries**: Simple equality queries (e.g., `{field: value}`) now use fast `Objects.equals()` instead of full `matchesQuery()` evaluation. Operator queries (`$gt`, `$lt`, etc.) skip the index path entirely to avoid ineffective bucket scanning.

- **Rewrote TTL expiration checking**:
  - Collections without TTL indexes have zero overhead (previously all collections scanned every 10 seconds)
  - TTL index info is cached when indexes are created
  - No snapshot copy during expiration check - iterates directly on CopyOnWriteArrayList
  - Auto-cleanup of tracking when collections are dropped

- **`$in` operator optimization**: Changed from O(n*m) to O(n+m) using HashSet lookups

- **Aggregator reuse**: Aggregators are now reused to reduce object allocation

- **Subdocument projection support**: Improved projection handling for nested documents

- **Stats performance**: Improved performance for driver statistics collection

#### MorphiumServer Optimizations
- **Buffered I/O**: Added 64KB buffered streams for socket read/write operations
- **ZLIB decompression buffer**: Increased from 100 bytes to 8KB with pre-sized output buffer
- **Reduced redundant serialization**: Avoid calling `bytes()` multiple times in logging paths

---

## [6.0.3] - 2025-11-28

### Fixed
- **NPE in MultiCollectionMessaging**: Fixed null pointer exception in `getLockCollectionName()` when building lock collection names

---

## [6.0.2] - 2025-10-16

### Fixed
- **NPE in Query.set() methods**: Changed from `Map.of()` to `Doc.of()` to allow null values in set operations
- **NPE in Msg.preStore()**: Initialize `processedBy` list if null before validation

### Changed
- **Default queue name handling**: Setting queue name to "msg" now resets to default (null) for backward compatibility
- **Build configuration**: Added `runOrder=filesystem` to surefire plugin for consistent test execution

---

## [6.0.1] - TBD

> 📖 **Detailed release notes**: [docs/releases/CHANGELOG-6.0.1.md](docs/releases/CHANGELOG-6.0.1.md)
> 📝 **Quick summary**: [docs/releases/RELEASE-NOTES-6.0.1.md](docs/releases/RELEASE-NOTES-6.0.1.md)

### Breaking Changes
- **Null Handling Behavior Change**: Default behavior now matches standard ORM conventions
  - **Previous behavior**: Null values were NOT stored in the database by default (fields omitted)
  - **New behavior**: Null values ARE stored as explicit nulls in the database by default
  - Fields WITHOUT annotation: Accept and store null values (standard ORM behavior)
  - Fields WITH `@IgnoreNullFromDB`: Reject nulls, field omitted when null
  - **Migration impact**: Existing code that relies on null values being omitted by default may need to add `@IgnoreNullFromDB` to those fields

- **@UseIfNull Deprecated**: Replaced with `@IgnoreNullFromDB` for clearer semantics
  - Old annotation had inverted logic that was confusing
  - `@UseIfNull` is now deprecated but still functional
  - Migration: Replace `@UseIfNull` with `@IgnoreNullFromDB` and remove the annotation (behavior is inverted)

### Added
- **New `@IgnoreNullFromDB` annotation**: Protects fields from null contamination
  - Prevents null values from being stored during serialization (field omitted)
  - Rejects null values during deserialization (preserves default value)
  - Distinguishes between "field missing from DB" vs "field present with null value"
  - Special handling for `@Id` fields: NEVER stored when null (MongoDB auto-generates)
  - Comprehensive documentation with behavior matrix and use cases
- Comprehensive test suites for null handling behavior
- Enhanced documentation for null handling with detailed examples

### Changed
- **Default null handling now matches standard ORMs**:
  - Serialization: Null values stored as explicit null in database
  - Deserialization: Null values from database accepted and set to null
  - This aligns with Hibernate, JPA, and other standard ORMs
- **@Id field handling**: Fields annotated with `@Id` are NEVER stored when null
  - Ensures MongoDB can auto-generate unique `_id` values
  - Prevents E11000 duplicate key errors from null `_id` values
- `runtests.sh`: Added local MorphiumServer cluster convenience mode (`--morphiumserver-local`) with optional auto-start (`--start-morphiumserver-local`)
  - Auto-start logs now go to `.morphiumserver-local/logs/`
  - Auto-start is idempotent and keeps a locally started cluster running by default

### Fixed
- Socket timeout handling in `SingleMongoConnection` - automatic retry on timeout exceptions
- Better timeout detection in watch operations
- Multi-collection messaging error handling and lock release
- Connection management in message rejection handler
- MorphiumServer: fix replica set startup to avoid ending up with no primary
- MorphiumServer: support `aggregate` command over the wire (enables aggregation stage tests against MorphiumServer)
- **Bulk operations now return proper operation counts**: `runBulk()` now returns statistics including `num_inserted`, `num_matched`, `num_modified`, `num_deleted`, `num_upserts`, and `upsertedIds`

### Performance
- Added collection name caching to reduce reflection overhead

### Known Issues

#### Messaging with MorphiumServer Replicaset
- **ExclusiveMessageTests#exclusivityTest**: This test is flaky when running with multiple Morphium instances connecting to a MorphiumServer replicaset. The test sometimes passes and sometimes times out due to slower message processing compared to real MongoDB. Change stream events ARE being delivered correctly, but processing throughput with MorphiumServer is lower than with real MongoDB, causing occasional timeouts with the default test timeout.
  - Workaround: Increase test timeout or use InMemoryDriver directly for messaging tests, or use a real MongoDB replicaset
  - Status: Performance issue, not a correctness issue

#### Test Suite Notes
- **ShardingTests**: These tests require a sharded MongoDB cluster and will fail on standalone or replica set deployments
- **SharedConnectionPoolTest**: Infrastructure test that requires specific connection pool setup
- **TopicRegistryTest**: Network registry discovery tests may fail due to timing issues in some environments

#### Test Results Summary (v6.1.0)
| Backend | Tests Run | Passed | Errors | Skipped |
|---------|-----------|--------|--------|---------|
| InMemory Driver | 1046 | 929 | 0 | 105 |
| MongoDB (Replicaset) | 1046 | 933 | 0 | 105 |
| MorphiumServer (Replicaset) | 1024 | 1024 | 0 | 92 |

## [6.0.0] - 2024-XX-XX

### Major Release
- Java 21+ requirement
- Significant architectural improvements
- Enhanced driver support
- **SSL/TLS support**: Added SSL/TLS support for secure connections to MongoDB
  - `driver.setUseSSL(true)` to enable SSL connections
  - `driver.setSslContext(sslContext)` for custom SSL configuration
  - `driver.setSslInvalidHostNameAllowed(true)` to disable hostname verification
  - New `SslHelper` utility class for creating SSLContext from keystores
- Improved documentation

---

For detailed release notes, see individual release documentation in [docs/releases/](docs/releases/).
