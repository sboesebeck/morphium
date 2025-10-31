# Migration v5 → v6

## Requirements

- **Java 21+** (mandatory)
- **MongoDB 5.0+** for production deployments
- **BSON library**: Morphium 6 uses MongoDB's BSON library (version 4.7.1+)

## Breaking Changes

### 1. Configuration API Changes

**Old (v5):**
```java
MorphiumConfig cfg = new MorphiumConfig();
cfg.setDatabase("mydb");
cfg.setHosts("localhost:27017");
cfg.setDriverName("PooledDriver");
```

**New (v6):**
```java
MorphiumConfig cfg = new MorphiumConfig();
cfg.connectionSettings().setDatabase("mydb");
cfg.clusterSettings().addHostToSeed("localhost", 27017);
cfg.driverSettings().setDriverName("PooledDriver");
```

Prefer the new nested settings objects:
- `connectionSettings()` - database name, credentials, timeouts
- `clusterSettings()` - hosts, replica set configuration
- `driverSettings()` - driver selection and configuration
- `messagingSettings()` - messaging implementation and settings
- `cacheSettings()` - caching configuration
- `encryptionSettings()` - encryption keys

### 2. Driver Selection

- v5 and v6: select drivers by name via `MorphiumConfig.driverSettings().setDriverName(name)`.
- Built‑in driver names:
  - `PooledDriver` - Connection-pooled MongoDB driver (default for production)
  - `SingleMongoConnectDriver` - Single-connection driver
  - `InMemDriver` - In-memory driver for testing (no MongoDB required)
- Custom drivers must be annotated with `@de.caluga.morphium.annotations.Driver(name = "YourName")` to be discoverable.

### 3. Messaging API

**Old (v5):**
```java
Messaging m = new Messaging(morphium, 100, true);
m.setSenderId("myapp");
```

**New (v6):**
```java
// Use factory method for correct configuration
MorphiumMessaging m = morphium.createMessaging();
m.setSenderId("myapp");
m.start();
m.addListenerForTopic("topic", (mm, msg) -> {
    // process message
    return null;
});
```

- **Always use `Morphium.createMessaging()`** so configuration and implementation selection happen correctly
- The messaging implementation is chosen by name from `MorphiumConfig.messagingSettings().getMessagingImplementation()`
- Built-in implementations:
  - `StandardMessaging` (default) - Single-collection messaging
  - `MultiCollectionMessaging` - Multi-collection messaging for high-volume scenarios
- Custom implementations must be annotated with `@de.caluga.morphium.annotations.Messaging(name = "YourName")`

#### Message "name" field renamed to "topic"

In v6, the `Msg` class field and methods have been renamed from "name" to "topic" to align with standard messaging terminology:

**Old (v5):**
```java
Msg msg = new Msg("myMessageName", "command", "data");
String name = msg.getName();
msg.setName("newName");
```

**New (v6):**
```java
Msg msg = new Msg("myTopic", "command", "data");
String topic = msg.getTopic();
msg.setTopic("newTopic");
```

**Backward compatibility:**
- The MongoDB field is backward compatible via `@Aliases({"name"})` annotation
- V5 and V6 messaging instances can communicate if `SingleCollectionMessaging` (now called `StandardMessaging`) is used in V6
- Existing MongoDB documents with the "name" field will continue to work

**Removed deprecated methods:**
- `getName()` → use `getTopic()`
- `setName(String)` → use `setTopic(String)`
- `sendAnswer(Messaging, Msg)` → removed (use messaging instance methods instead)

### 4. Null Value Handling Changes

Morphium v6 changes how null values are handled during serialization and deserialization to be more consistent with MongoDB behavior and other ORMs.

#### New Default Behavior (v6)

**Field missing from MongoDB document:**
- Java field is NOT updated (preserves default values or existing values)
- This is consistent behavior regardless of annotations

**Field present in MongoDB with explicit null value:**
- By default, Java field IS set to null (overwrites default values)
- Use `@IgnoreNullFromDB` to protect specific fields from null contamination

**Old (v5):**
```java
@Entity
public class MyEntity {
    private Integer counter = 42;  // null from DB would be ignored (protected)

    @UseIfnull
    private String name;           // null from DB would be accepted
}
```

**New (v6):**
```java
@Entity
public class MyEntity {
    private Integer counter = 42;  // null from DB IS accepted (becomes null)

    @IgnoreNullFromDB
    private Integer protectedCounter = 99;  // null from DB is ignored (stays 99)
}
```

#### Migration Strategy

**The `@UseIfnull` annotation is now deprecated.** The behavior has been inverted:

**Old behavior (v5):**
- Fields WITHOUT `@UseIfnull`: ignored nulls from DB (protected)
- Fields WITH `@UseIfnull`: accepted nulls from DB

**New behavior (v6):**
- Fields WITHOUT `@IgnoreNullFromDB`: accept nulls from DB (standard behavior)
- Fields WITH `@IgnoreNullFromDB`: ignore nulls from DB (protected)

**To migrate:**
1. Remove `@UseIfnull` from fields that should accept nulls (this is now the default)
2. Add `@IgnoreNullFromDB` to fields that previously did NOT have `@UseIfnull` if you want to maintain the old protection behavior

#### Example: Field Missing vs. Explicit Null

```java
@Entity
public class Example {
    private String regularField = "default";

    @IgnoreNullFromDB
    private String protectedField = "default";
}
```

**Scenario 1: MongoDB document is `{}`** (field missing entirely)
- `regularField` stays "default"
- `protectedField` stays "default"

**Scenario 2: MongoDB document is `{ regularField: null, protectedField: null }`**
- `regularField` becomes null (default overwritten)
- `protectedField` stays "default" (protected from null)

This makes the behavior more consistent: if a field doesn't exist in MongoDB, the Java object is not modified.

### 5. Removed Deprecated Methods from Morphium Class

Many deprecated methods were removed from the `Morphium` class in v6. Most update operations (set, unset, inc, push, pull, etc.) should now be performed through the `Query` class instead.

#### Update Operations: Use Query Methods

**Removed methods (were deprecated in v5):**
- `morphium.set(entity, collection, values, upsert, callback)`
- `morphium.unset(entity, collection, field, callback)`
- `morphium.inc(fieldsToInc, query, upsert, multiple, callback)`

**Migration:**

**Old (v5):**
```java
// Setting values on entities matching a query
Map<Enum, Object> values = new HashMap<>();
values.put(MyEntity.Fields.status, "active");
morphium.set(entity, null, values, false, null);

// Unsetting a field
morphium.unset(entity, null, "fieldName", null);

// Incrementing values
Map<Enum, Number> increments = new HashMap<>();
increments.put(MyEntity.Fields.counter, 1);
morphium.inc(increments, query, false, true, null);
```

**New (v6):**
```java
// Use Query.set() instead
Query<MyEntity> query = morphium.createQueryFor(MyEntity.class);
query.f("_id").eq(entity.getId());
query.set("status", "active");

// Use Query.unset() instead
query.unset("fieldName");

// Use Query.inc() instead
query.inc(MyEntity.Fields.counter, 1);
```

**Note:** The methods `setInEntity()` and `unsetInEntity()` still exist for updating specific entities directly.

#### Query Creation: Unified Method

**Removed methods:**
- `createQueryFor(Class, String collectionName)` - two-parameter version removed
- `createQueryByTemplate(T template, String... fields)` - template-based queries removed
- `findByTemplate(T template, String... fields)` - template-based find removed

**Migration:**

**Old (v5):**
```java
// Creating query with custom collection name
Query<MyEntity> query = morphium.createQueryFor(MyEntity.class, "custom_collection");

// Template-based queries
MyEntity template = new MyEntity();
template.setStatus("active");
List<MyEntity> results = morphium.findByTemplate(template, "status");
```

**New (v6):**
```java
// Use single-parameter createQueryFor and specify collection on query
Query<MyEntity> query = morphium.createQueryFor(MyEntity.class);
query.setCollectionName("custom_collection");

// Template-based queries: use standard query syntax
Query<MyEntity> query = morphium.createQueryFor(MyEntity.class);
query.f("status").eq("active");
List<MyEntity> results = query.asList();
```

#### Other Removed Methods

**Read operations:**
- `readAll(Class)` - removed (use `createQueryFor(Class).asList()`)
- `findByField(Class, field, value)` - removed (use query with field filter)
- `findById(Class, id, collection)` - three-parameter version removed

**Index operations:**
- `ensureIndex(Class, callback, Enum... fields)` - removed (use `ensureIndices(Class)`)
- `getIndexesFromMongo(Class)` - removed (use driver-specific methods)

**Other operations:**
- `flush(Class)` - removed (use buffered writer methods directly)
- `createAggregator(Class, resultClass)` - removed (use `aggregate()` on Query)
- `mapReduce(Class, map, reduce)` - removed (MongoDB deprecated map-reduce in favor of aggregation)

**Migration approach:**
1. Replace `morphium.set/unset/inc/push/pull` with equivalent `query.set/unset/inc/push/pull` methods
2. Use `Query` for all update operations instead of direct `Morphium` methods
3. Use standard query syntax instead of template-based queries
4. For simple operations like `findById`, use `query.f(Query.ID_FIELD).eq(id).get()`

### 6. InMemoryDriver Improvements (v6)

The InMemoryDriver received major enhancements in v6.0:

#### Change Streams
- **Full change stream support** with document snapshots
- **Proper event isolation** preventing dirty reads
- **Database-scoped driver sharing** with reference counting
- Multiple Morphium instances can share the same InMemoryDriver when using the same database name

#### Message Queue Testing
```java
// Multiple messaging instances can now share InMemoryDriver correctly
MorphiumMessaging msg1 = morphium1.createMessaging();
MorphiumMessaging msg2 = morphium2.createMessaging();
// Both will receive change stream events from the shared driver
```

#### Known Limitations
- No replica set simulation (single-instance only)
- No sharding support
- Limited geospatial operations compared to MongoDB
- See `docs/howtos/inmemory-driver.md` for detailed feature coverage

## New Features in v6

### 1. Virtual Threads Support
Morphium 6 uses Java 21 virtual threads for:
- Change stream event dispatching
- Async operation handling
- Lightweight concurrent processing

### 2. Enhanced Configuration
- URI-based configuration: `MONGODB_URI` environment variable
- System properties: `-Dmorphium.driver=inmem`
- Properties file: `morphium-test.properties`

### 3. Test Infrastructure
- Tag-based test organization (`@Tag("core")`, `@Tag("messaging")`, `@Tag("inmemory")`)
- Improved test runner: `./runtests.sh` with retry logic and filtering
- Better isolation between tests with proper driver lifecycle management

## Migration Checklist

- [ ] Update to Java 21+
- [ ] Update pom.xml dependency to `6.0.0` or higher
- [ ] Replace flat config setters with nested settings objects
- [ ] Change messaging instantiation to `morphium.createMessaging()`
- [ ] Update messaging code: replace `getName()`/`setName()` with `getTopic()`/`setTopic()`
- [ ] Replace removed `Morphium` methods:
  - [ ] `morphium.set/unset/inc()` → use `query.set/unset/inc()` instead
  - [ ] `morphium.findByTemplate()` → use standard query syntax
  - [ ] `morphium.readAll()` → use `query.asList()`
  - [ ] `morphium.findByField()` → use query with field filter
  - [ ] `morphium.createQueryFor(Class, collection)` → use `query.setCollectionName()`
- [ ] Review null handling behavior: consider adding `@IgnoreNullFromDB` to fields that need protection from null values
- [ ] Remove deprecated `@UseIfnull` annotations (behavior is now inverted)
- [ ] Review custom driver/messaging implementations for annotation requirements
- [ ] Update test infrastructure to use tags if using parameterized tests
- [ ] Test with InMemoryDriver to verify change stream handling

## JMS Support

- The JMS classes provided are experimental/incomplete and should not be relied upon for full JMS compatibility
- May be completed in upcoming versions

## Getting Help

- Check `docs/troubleshooting-guide.md` for common issues
- Review `docs/howtos/` for specific use cases
- Join our Slack: https://join.slack.com/t/team-morphium/shared_invite/...
