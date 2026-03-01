# Morphium 6.2.0-SNAPSHOT - Unreleased Changes

## Quick Summary

Development version with significant new features: optimistic locking via `@Version`, X.509 certificate authentication, `MorphiumDriverException` made unchecked, and MongoDB Atlas SRV connection support.

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

For the full commit history, see `git log v6.1.9..HEAD` on the `master` branch.
