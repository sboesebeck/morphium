# Changelog

All notable changes to Morphium will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [6.1.0] - TBD

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
- **SSL/TLS support**: Added SSL/TLS support for secure connections to MongoDB
  - `driver.setUseSSL(true)` to enable SSL connections
  - `driver.setSslContext(sslContext)` for custom SSL configuration
  - `driver.setSslInvalidHostNameAllowed(true)` to disable hostname verification
  - New `SslHelper` utility class for creating SSLContext from keystores

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
- **Message sending to self**: Fixed broken message sending when sender equals recipient
- **Deadlocks**: Fixed multiple deadlock scenarios in messaging and server components
- **Robust shutdown**: Improved shutdown handling across components
- **NPE in QueryHelper.matchesQuery**: Fixed null pointer exception when comparing MorphiumId/ObjectId fields against null query values
- **Flaky test fixes**: Replaced timing-dependent `Thread.sleep()` + assertion patterns with `TestUtils.waitForConditionToBecomeTrue()` polling in messaging and changestream tests

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

### Fixed
- Socket timeout handling in `SingleMongoConnection` - automatic retry on timeout exceptions
- Better timeout detection in watch operations
- Multi-collection messaging error handling and lock release
- Connection management in message rejection handler
- **Bulk operations now return proper operation counts**: `runBulk()` now returns statistics including `num_inserted`, `num_matched`, `num_modified`, `num_deleted`, `num_upserts`, and `upsertedIds`

### Performance
- Added collection name caching to reduce reflection overhead

## [6.0.0] - 2024-XX-XX

### Major Release
- Java 21+ requirement
- Significant architectural improvements
- Enhanced driver support
- Improved documentation

---

For detailed release notes, see individual release documentation in [docs/releases/](docs/releases/).
