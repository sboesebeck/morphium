# Changelog

All notable changes to Morphium will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [6.1.0] 

### Added

#### MorphiumServer Enhancements
- **Replica set support**: MorphiumServer now supports replica set configuration with automatic primary election and failover
- **Server CLI**: New standalone `morphium-server-cli.jar` for running MorphiumServer from command line with `--help` option
- **Replication**: Data replication between MorphiumServer instances in a replica set

#### Messaging
- **Topic Registry / Network Registry**: New `NetworkRegistry` implementation for discovering messaging topics across the network
- **MessagingSettings**: New configuration class for messaging-related settings

#### InMemoryDriver
- **Tailable cursor support**: InMemoryDriver now supports tailable queries
- **Shared InMemory databases**: Multiple Morphium instances can share the same InMemory database (configurable via `DriverSettings.setShareInMemoryDatabase()`)

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

### Changed
- **Modernized concurrent collections**: Replaced legacy `Vector` with `CopyOnWriteArrayList` and `Hashtable` with `ConcurrentHashMap` for better performance
- **Optimized string operations**: Consolidated multiple `replaceAll()` calls into single regex patterns, replaced `replaceAll()` with `replace()` for literal string replacements

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

### Known Limitations
- **MorphiumServer replica set data synchronization**: When running multiple MorphiumServer instances as a replica set, each instance has its own isolated InMemoryDriver. Data written to the primary is not automatically replicated to secondaries. For testing purposes, use single-node mode or a real MongoDB replica set.

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
