# Migration v6.1.x → v6.2.0

This guide covers all breaking changes and new features when upgrading from Morphium 6.1.x to 6.2.0.

## Breaking Changes

### 1. MorphiumDriverException is now unchecked

`MorphiumDriverException` extends `RuntimeException` instead of `Exception`. This aligns with the MongoDB Java Driver (`MongoException`), Spring Data, JPA, and jOOQ conventions. It eliminates 40+ boilerplate `catch-wrap-rethrow` blocks throughout Morphium.

**What to change:**

```java
// Multi-catch: simplify (MorphiumDriverException IS a RuntimeException now)
// Before:
try {
    morphium.store(entity);
} catch (RuntimeException | MorphiumDriverException e) {
    handleError(e);
}
// After:
try {
    morphium.store(entity);
} catch (RuntimeException e) {
    handleError(e);
}

// throws declarations: can be removed (still compile if left in)
// Before:
public void doStuff() throws MorphiumDriverException { ... }
// After:
public void doStuff() { ... }

// Standalone catch: works unchanged
catch (MorphiumDriverException e) { ... }  // still valid
```

**Important:** If your code catches `Exception` broadly, `MorphiumDriverException` is now caught by `catch (RuntimeException e)` as well. Review your exception handling hierarchy.

### 2. MorphiumServer → PoppyDB (Module Extraction)

The embedded MongoDB-compatible server was extracted into a separate Maven module and renamed.

| | 6.1.x | 6.2.0 |
|---|---|---|
| Maven artifact | bundled in `de.caluga:morphium` | separate: `de.caluga:poppydb:6.2.0` |
| Package | `de.caluga.morphium.server` | `de.caluga.poppydb` |
| Main class | `MorphiumServer` | `PoppyDB` |
| CLI class | `MorphiumServerCLI` | `PoppyDBCLI` |
| CLI JAR | `morphium-*-server-cli.jar` | `poppydb-*-cli.jar` |
| Test tag | `@Tag("morphiumserver")` | `@Tag("poppydb")` |

**What to change:**

Add the PoppyDB dependency if you use the embedded server:
```xml
<dependency>
    <groupId>de.caluga</groupId>
    <artifactId>poppydb</artifactId>
    <version>6.2.0</version>
    <scope>test</scope> <!-- or remove scope for production use -->
</dependency>
```

Update imports and class names:
```java
// Before:
import de.caluga.morphium.server.MorphiumServer;
MorphiumServer server = new MorphiumServer(27017, "localhost", 100, 10);

// After:
import de.caluga.poppydb.PoppyDB;
PoppyDB server = new PoppyDB(27017, "localhost", 100, 10);
```

Wire-protocol compatibility is preserved — PoppyDB responds to both `poppyDB: true` and `morphiumServer: true` in the hello handshake, so existing driver detection code continues to work.

### 3. Multi-Module Maven Structure

Morphium is now a multi-module project:

```
morphium-parent (de.caluga:morphium-parent) — BOM/parent POM
├── morphium-core (de.caluga:morphium)      — core ODM library
└── poppydb (de.caluga:poppydb)             — MongoDB-compatible server
```

The `de.caluga:morphium` artifact no longer bundles Netty and other server dependencies. This makes the core library ~90% leaner. **No changes to your pom.xml needed** unless you use PoppyDB (see above).

## Deprecations

### Config setters moved to sub-objects

Some `MorphiumConfig` setters that were already delegating to sub-objects are now formally `@Deprecated`. They still work, but the sub-object API is preferred:

```java
// Deprecated (still works):
cfg.setMessagingStatusInfoListenerEnabled(true);

// Preferred:
cfg.messagingSettings().setMessagingStatusInfoListenerEnabled(true);
```

The core config methods (`setDatabase()`, `addHostToSeed()`, etc.) were already migrated to sub-objects in 6.0 and remain available as convenience delegates.

## New Features

### @Reference Cascade Delete/Store

`@Reference` now supports automatic lifecycle management:

```java
@Entity
public class Author {
    @Id private MorphiumId id;

    @Reference(cascadeDelete = true)
    private List<Book> books;  // books deleted when author is deleted

    @Reference(orphanRemoval = true)
    private List<Article> articles;  // removed refs auto-deleted
}
```

Circular references (A→B→A) are detected and handled safely.

### @AutoSequence

Annotation-driven auto-increment sequences:

```java
@Entity
public class Invoice {
    @Id private MorphiumId id;

    @AutoSequence(name = "invoice_number", startValue = 1000, inc = 1)
    private Long invoiceNumber;  // auto-assigned on store()
}
```

- Supports `long`, `Long`, `int`, `Integer`, `String`
- Only assigns when field is null/0 — explicit values are never overwritten
- Batch-optimized: `storeList()` fetches all sequence numbers in one round-trip

### @CreationTime Improvements

- **Field-only annotation** now sufficient (class-level `@CreationTime` no longer required)
- **`LocalDateTime` support** added (in addition to `Date`, `long`, `String`)
- Preset values are no longer overwritten

### CosmosDB Auto-Detection

Morphium detects Azure CosmosDB connections via the hello handshake and adjusts behavior automatically. Query via `morphium.isCosmosDB()`.

### PoppyDB & Morphium Messaging Optimization

PoppyDB and Morphium Messaging recognize each other and optimize their communication path. This results in lower latency and less overhead compared to using a real MongoDB as messaging backend.

## Dependency Updates

| Library | 6.1.x | 6.2.0 |
|---|---|---|
| io.netty:netty-all | 4.1.100.Final | 4.2.9.Final |
| org.mongodb:bson | 4.7.1 | 4.11.5 |
| org.slf4j:slf4j-api | 2.0.0 | 2.0.17 |
| ch.qos.logback | 1.5.24 | 1.5.25 |

If you depend on Netty or BSON directly, verify compatibility. Netty 4.2 is backward-compatible with 4.1 APIs.

## Migration Checklist

1. [ ] **Search `catch (RuntimeException | MorphiumDriverException`** — simplify to `catch (RuntimeException`
2. [ ] **Search `import de.caluga.morphium.server`** — replace with `import de.caluga.poppydb`
3. [ ] **Search `MorphiumServer`** — rename to `PoppyDB`
4. [ ] **Search `@Tag("morphiumserver")`** — rename to `@Tag("poppydb")`
5. [ ] **Add `poppydb` dependency** if you use the embedded server
6. [ ] **Review exception handling** — `MorphiumDriverException` now caught by `catch (RuntimeException e)`
7. [ ] **Update CLI scripts** — JAR name changed to `poppydb-*-cli.jar`
8. [ ] **Optional:** adopt `@Reference(cascadeDelete)`, `@AutoSequence`, `@Version`
9. [ ] **Optional:** migrate deprecated config setters to sub-object API
