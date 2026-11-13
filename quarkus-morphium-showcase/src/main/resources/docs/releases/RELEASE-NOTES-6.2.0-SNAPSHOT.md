# Morphium 6.2.0-SNAPSHOT - Unreleased Changes

## Quick Summary

Development version with significant new features: optimistic locking via `@Version`, automatic sequence numbers via `@AutoSequence`, bulk sequence allocation, X.509 certificate authentication, `MorphiumDriverException` made unchecked, MongoDB Atlas SRV connection support, Quarkus classloader compatibility, and standalone MongoDB hardening.

> **Note:** This document describes changes currently on the `master` branch that have not yet been included in an official release.

## Breaking Change

### MorphiumDriverException is now unchecked (`extends RuntimeException`)

`MorphiumDriverException` now extends `RuntimeException` instead of `Exception`, aligning with every major Java database framework (MongoDB Driver, JPA/Hibernate, Spring Data, jOOQ).

**Action required** if your code catches a `RuntimeException` and inspects the cause:

```java
// BROKEN after this change -- instanceof check now always returns false
catch (RuntimeException e) {
    if (e.getCause() instanceof MorphiumDriverException) { ... }
}

// Correct -- catch it directly
catch (MorphiumDriverException e) {
    handleDbError(e);
}
```

This is a **silent behavioral change** -- no compile error, the `instanceof` check simply returns `false`.

## New Features

### @Version Annotation -- Optimistic Locking

Morphium now supports optimistic locking via the `@Version` annotation. A version field is automatically incremented on each save and checked during updates to prevent lost updates in concurrent scenarios.

```java
@Entity
public class Product {
    @Id
    private MorphiumId id;

    @Version
    private Long version;

    private String name;
}

// First save: version set to 1
morphium.store(product);

// Concurrent update with stale version throws VersionMismatchException
```

- Version is initialized to `1L` on first insert
- Updates use atomic `$and` filter: `{_id: ..., version: currentVersion}`
- `VersionMismatchException` thrown on conflict (no silent overwrites)
- Works with all drivers (PooledDriver, SingleMongoConnectDriver, InMemoryDriver)

### @AutoSequence Annotation -- Automatic Sequence Numbers

New annotation for zero-boilerplate sequence number assignment. When a document is stored and the annotated field is unset, Morphium automatically assigns the next value from the named sequence.

```java
@Entity
public class ImportRecord {
    @Id
    private MorphiumId id;

    @AutoSequence(name = "import_number")
    private long importNumber;

    private String data;
}

// Single store: importNumber auto-assigned (e.g. 1, 2, 3, ...)
morphium.store(record);

// Batch store: all sequence numbers allocated in ONE round-trip
morphium.storeList(records);  // 1000 records = still just 1 lock cycle
```

- Supported field types: `long`, `Long`, `int`, `Integer`, `String`
- For primitive types, `0` is treated as "not yet assigned"
- Batch optimisation via `SequenceGenerator.getNextBatch()` -- O(1) instead of O(N) round-trips
- Configurable: `name`, `startValue` (default 1), `inc` (step size, default 1)

### SequenceGenerator.getNextBatch(int) -- Bulk Sequence Allocation

New method for reserving a contiguous block of sequence numbers in a single lock+increment+unlock round-trip:

```java
SequenceGenerator sg = new SequenceGenerator(morphium, "order_seq", 1, 1);
long[] batch = sg.getNextBatch(1000);
// returns [1, 2, 3, ..., 1000] -- atomically reserved, no gaps
```

Reduces MongoDB round-trips from 5 x N down to a constant 5 regardless of batch size.

### MONGODB-X509 Client Certificate Authentication

Native support for X.509 client certificate authentication:

```java
MorphiumConfig cfg = new MorphiumConfig();
cfg.connectionSettings().setUseSSL(true);
cfg.authSettings().setAuthMechanism("MONGODB-X509");

SSLContext sslContext = SslHelper.createMutualTlsContext(
    "/path/to/client-keystore.jks", "keystorePassword",
    "/path/to/truststore.jks", "truststorePassword"
);
cfg.connectionSettings().setSslContext(sslContext);
```

### MongoDB Atlas SRV Connection Support

Connect to MongoDB Atlas using `mongodb+srv://` connection strings with automatic DNS SRV lookup:

```java
MorphiumConfig cfg = MorphiumConfig.fromConnection(
    "mongodb+srv://user:password@cluster0.abc123.mongodb.net/mydb"
);
```

- Pure-Java DNS implementation via `DnsSrvResolver` -- no JNDI dependency, works in Quarkus native, Android, and Windows
- Automatic TCP fallback when UDP DNS response is truncated
- TLS enabled automatically for `mongodb+srv://` connections
- Windows-compatible: skips `/etc/resolv.conf`, falls back to public DNS (8.8.8.8, 1.1.1.1)

### Thread Context ClassLoader (Quarkus Compatibility)

All `Class.forName()` calls replaced with `AnnotationAndReflectionHelper.classForName()` which uses the thread context classloader. This is required for Quarkus and other frameworks with custom classloader hierarchies.

## Bug Fixes

### Concurrent Double-Write in BufferedMorphiumWriterImpl

Fixed a race condition where two threads calling `flush()` concurrently could both process the same write queue, causing E11000 duplicate key errors. The fix uses `ConcurrentHashMap.remove()` for atomic ownership transfer.

### Standalone MongoDB Improvements

- `@WriteSafety(level=MAJORITY)` automatically downgraded to `w:1` on standalone MongoDB (prevents timeout)
- `startTransaction()` warns when connected to standalone (transactions require replica set)
- Replica set detection uses actual driver state instead of config flag
- Write buffer executes immediately when no `@WriteBuffer` annotation is present

### Index and Collection Checks

- `setAutoIndexAndCappedCreationOnWrite()` now sets both IndexCheck and CappedCheck
- Missing indices no longer reported for collections that don't exist yet

## Dependency Updates

- `logback-core`: 1.5.24 -> 1.5.25
- `assertj-core`: 3.23.1 -> 3.27.7
- `slf4j-api`: 2.0.0 -> 2.0.17
- `bson`: 4.7.1 -> 4.11.5
- `netty-all`: 4.1.100.Final -> 4.2.9.Final

## Requirements

- Java 21+
- MongoDB 5.0+ (for production deployments)

---

For the full changelog, see [CHANGELOG.md](../../CHANGELOG.md).
