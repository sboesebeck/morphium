# Developer Guide

This guide explains how to configure Morphium and use its core APIs.

Configuration model
- `MorphiumConfig` aggregates dedicated settings objects. Use these nested accessors:
  - `connectionSettings()` – database name, pool sizes, timeouts
  - `clusterSettings()` – host seed, replica set
  - `driverSettings()` – driver name (`PooledDriver`, `SingleMongoConnectDriver`, `InMemDriver`), idle sleeps
  - `messagingSettings()` – queue name, window size, multithreading, change streams, poll pause
  - `cacheSettings()` – global TTL, housekeeping, cache implementation
  - `threadPoolSettings()` – async operation thread pool
  - `writerSettings()` – write buffer behavior and writer implementation
  - `objectMappingSettings()` – camelCase conversion, lifecycle options
  - `encryptionSettings()` – value and credentials encryption providers/keys
  - `collectionCheckSettings()` – index/capped checks
  - `authSettings()` – MongoDB credentials

Example
```java
MorphiumConfig cfg = new MorphiumConfig();
cfg.connectionSettings().setDatabase("myapp");
cfg.clusterSettings().addHostToSeed("mongo1", 27017);
cfg.clusterSettings().addHostToSeed("mongo2", 27017);
cfg.driverSettings().setDriverName("PooledDriver"); // default

// Optional: messaging defaults
cfg.messagingSettings().setMessageQueueName("msg");
cfg.messagingSettings().setMessagingWindowSize(100);
cfg.messagingSettings().setMessagingMultithreadded(true);
cfg.messagingSettings().setUseChangeStream(true);
```

Object mapping
- Use annotations on POJOs: `@Entity`, `@Embedded`, `@Id`, `@Reference(lazyLoading=true)`, `@Cache`, `@Index`
- Field‑level encryption: `@Encrypted` (requires an encryption provider/key)

Example
```java
@Entity(translateCamelCase = true)
@Cache(timeout = 60_000)
public class Order {
  @Id private MorphiumId id;
  @Index private String customerId;
  private BigDecimal amount;

  @Reference(lazyLoading = true)
  private List<Item> items;

  @Embedded
  private Address shippingAddress;
}
```

Querying
```java
// Find one
Order o = morphium.createQueryFor(Order.class)
    .f("customerId").eq("C123")
    .get();

// Find many with projection/sort
List<Order> recent = morphium.createQueryFor(Order.class)
    .f("status").eq("OPEN")
    .sort("-created")
    .asList();
```

Field names (avoid string literals)
- Prefer enums over string field names to avoid typos and ease migrations/renames.
```java
@Entity(translateCamelCase = true)
public class User {
  @Id private MorphiumId id;
  private String userName; // stored as user_name

  public enum Fields { id, userName }
}

// Safer queries using enums
var q = morphium.createQueryFor(User.class)
    .f(User.Fields.userName).eq("alice");
```
- Alternative without codegen: use the lambda property extractor helper
```java
import static de.caluga.morphium.query.FieldNames.of;
var q2 = morphium.createQueryFor(User.class)
    .f(of(User::getUserName)).eq("alice");
```
- Enums/lambdas remain stable across refactors and camelCase translation changes.
- See How‑To: Field Names for more options (including annotation‑processor codegen).

Aggregation
```java
var agg = morphium.createAggregator(Order.class, Map.class);
agg.match(morphium.createQueryFor(Order.class).f("status").eq("OPEN"));
agg.group("$customerId").sum("total", "$amount").count("cnt").end();
agg.sort("-total");
List<Map> results = agg.aggregate();
```
See How‑To: Aggregation Examples for more pipelines.

Caching
- Add `@Cache` to entities to enable read cache; TTL, max entries, and clear strategy are configurable.
- Cluster‑wide cache synchronization uses Morphium’s messaging; see Messaging guide.
- A JCache adapter is available if you prefer standard javax.cache interfaces.
See How‑To: Caching Examples and How‑To: Cache Patterns for recipes and guidance.

Encryption
- Annotate sensitive fields with `@Encrypted` and configure providers/keys via `cfg.encryptionSettings()`.
```java
cfg.encryptionSettings().setCredentialsEncryptionKey("secret");
// Optional: custom providers
// cfg.encryptionSettings().setEncryptionKeyProviderClass(...);
// cfg.encryptionSettings().setValueEncryptionProviderClass(...);
```

Threading
- Async operations run on a dedicated thread pool (virtual threads by default) configured via `threadPoolSettings()`.
- Messaging has its own thread pool configuration in `messagingSettings()`.

Extension points
- NameProvider: dynamic collection naming
```java
morphium.setNameProviderForClass(MyEntity.class, (m, cls, def) -> def + "_2025");
```
- Storage listeners: audit/validation hooks
```java
morphium.addListener(new MorphiumStorageAdapter<Object>() {
  @Override public void preStore(Morphium m, Object entity, boolean isNew) {
    // audit or validation
  }
});
```
- Custom cache/writer/type mappers: implement the respective interfaces and register via config or Morphium API.

See also
- Messaging: topic listeners, exclusive vs broadcast, change streams vs polling
- How‑Tos for focused recipes
