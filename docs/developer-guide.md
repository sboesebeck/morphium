# Developer Guide

This guide explains how to configure Morphium and use its core APIs.

## Configuration Model

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

### Example
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

## Object Mapping
- Use annotations on POJOs: `@Entity`, `@Embedded`, `@Id`, `@Reference(lazyLoading=true)`, `@Cache`, `@Index`
- Field‑level encryption: `@Encrypted` (requires an encryption provider/key)

_hint_: All times and timeout settings are in milliseconds throughout whole Morphium

### Example
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

## Embedded vs Reference

- Embedded (`@Embedded`):
  - No MongoDB `_id` required; data is stored inline inside the parent document.
  - Ideal for value objects and nested structures (address, money, coords).
  - One read/write touches a single MongoDB document; deserialization splits into Java objects.
  - Supports `typeId`, `translateCamelCase`, and `polymorph` like `@Entity`.
- Reference (`@Reference`):
  - Stores only the target entity’s ID in the parent; the referenced entity lives in its own collection/document.
  - Reading references may incur N+1 queries: one for the parent plus one per referenced entity (unless `lazyLoading=true` defers loads until first access).
  - Use for large/independent aggregates or when the referenced object changes on its own lifecycle.

### Simple Example
```java
@Embedded(typeId = "Address")
public class Address {
  private String street;
  private String city;
}

@Entity(typeId = "Customer")
public class Customer {
  @Id private MorphiumId id;
  private String name;
}

@Entity(typeId = "Order")
public class Order {
  @Id private MorphiumId id;

  // Embedded: stored inline in the Order document
  @Embedded private Address shippingAddress;

  // Reference: only the Customer ID is stored in Order; Customer lives in its own collection
  @Reference(lazyLoading = true) private Customer customer;
}
```
#### Notes
- When reading an `Order`, Morphium returns `shippingAddress` from the same document. The `customer` is loaded on first access due to `lazyLoading` (otherwise N+1 reads if you eagerly access many references).
- The MongoDB `orders` collection holds full address fields inline; customers are separate documents in the `customer` collection.

### Further Examples in Tests
- `EmbeddedObject` used across tests: src/test/java/de/caluga/test/mongo/suite/data/EmbeddedObject.java
  - GitHub: https://github.com/sboesebeck/morphium/blob/develop/src/test/java/de/caluga/test/mongo/suite/data/EmbeddedObject.java
- `ComplexObject` demonstrates embedded lists and references: src/test/java/de/caluga/test/mongo/suite/data/ComplexObject.java
  - GitHub: https://github.com/sboesebeck/morphium/blob/develop/src/test/java/de/caluga/test/mongo/suite/data/ComplexObject.java
Note: these test classes are intentionally technical and cover edge cases.

## Stable Type Identification (recommended)

- Prefer specifying a stable type identifier on your classes to avoid coupling persisted data to Java class names. This makes refactors and package/class renames safer.
- Entities: set `@Entity(typeId = "Order")` (choose any stable string meaningful to your domain).
- Embedded types: set `@Embedded(typeId = "Address")` likewise.
- Morphium uses the `typeId` (when provided) instead of the Java class name to identify the target POJO for incoming data. This decouples stored documents from Java class names and eases migrations when packages or class names change.
```java
@Entity(typeId = "Order", translateCamelCase = true)
public class Order { /* ... */ }

@Embedded(typeId = "Address")
public class Address { /* ... */ }
```
#### Notes
- `typeId` is available on both `@Entity` and `@Embedded`.
- For heterogeneous collections/fields, you can also enable `polymorph = true` to include type information in the stored documents.
- Important: set a `typeId` from the beginning. If you first store documents without `typeId`, Morphium will persist the Java class name; after a rename or package move those documents may no longer deserialize. You can set `typeId` in the new version to the old fully‑qualified class name as a recovery step, but it is ugly and potentially confusing—prefer setting a stable `typeId` from day one.

### Renames and Schema Evolution

- Field renames: use `@Aliases({"oldName1", "oldName2"})` on the new field to accept legacy field names from MongoDB and in queries during migration.
```java
public class User {
  @Aliases({"name", "user_name"})
  private String userName;
}
```
- Additional/dynamic fields: add a catch‑all `Map<String,Object>` annotated with `@AdditionalData` to retain unknown fields that exist in MongoDB but not in your POJO.
```java
public class User {
  @AdditionalData(readOnly = true) // set false if you want to write them back
  private Map<String,Object> extras;
}
```
- Combine `typeId` with `@Aliases` and `@AdditionalData` for smoother migrations: keep deserialization working after refactors, accept legacy field names, and preserve unexpected fields.

### Example: Rename Class and Fields Safely

#### Version 1 (initial, best practice)
```java
// package com.example.v1;
@Entity(typeId = "User") // set a stable typeId from day one
public class User {
  @Id private MorphiumId id;
  private String name;            // old field name
  private int age;
}
```

#### Version 2 (after refactor/migration)
```java
// package com.example.accounts;    // class/package renamed
@Entity(typeId = "User")           // stable type identifier
public class AccountUser {          // class renamed
  @Id private MorphiumId id;

  // field renamed; accept legacy names from existing MongoDB docs
  @Aliases({"name", "user_name"})
  private String userName;

  private int age;

  // capture unknown/dynamic fields to avoid data loss during migration
  @AdditionalData(readOnly = true)
  private Map<String,Object> extras;
}
```

#### Recovery (if v1 had no `typeId`)
- If v1 stored the Java class name, set `@Entity(typeId = "com.example.v1.User")` in v2 so existing documents still deserialize. Then plan a data migration to switch to a clean, stable `typeId` later.

#### Notes
- Existing documents continue to deserialize because `typeId = "User"` no longer depends on the Java class name.
- Legacy documents with `name` (or `user_name` if camelCase translation changed) populate `userName` thanks to `@Aliases`.
- Any unexpected fields present in legacy documents are preserved in `extras`.

## Querying
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

## Field Names (avoid string literals)
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
- See How‑To: [Field Names](./howtos/field-names.md) for more options (including annotation‑processor codegen).

## Aggregation
```java
var agg = morphium.createAggregator(Order.class, Map.class);
agg.match(morphium.createQueryFor(Order.class).f("status").eq("OPEN"));
agg.group("$customerId").sum("total", "$amount").count("cnt").end();
agg.sort("-total");
List<Map> results = agg.aggregate();
```
See How‑To: [Aggregation Examples](./howtos/aggregation-examples.md) for more pipelines.

## Caching
- Add `@Cache` to entities to enable read cache; TTL, max entries, and clear strategy are configurable.
- Cluster‑wide cache synchronization uses Morphium’s messaging; see the [Messaging](./messaging.md) guide.
- A JCache adapter is available if you prefer standard javax.cache interfaces.
See How‑To: [Caching Examples](./howtos/caching-examples.md) and [Cache Patterns](./howtos/cache-patterns.md) for recipes and guidance.

### Cache Synchronization

- Purpose: keep caches consistent across nodes. Messaging was originally introduced to propagate cache change events in clusters.
- Mechanism: on writes, Morphium emits a cache message; other nodes apply a policy from `@Cache.syncCache`:
  - `CLEAR_TYPE_CACHE`: clear the entire type cache for the entity.
  - `REMOVE_ENTRY_FROM_TYPE_CACHE`: remove a single entry (by ID) from the cache.
  - `UPDATE_ENTRY`: re‑read and update the cached entity in place (may briefly expose stale data under concurrent reads—“dirty reads”).
- Requirements: ensure messaging is running on all nodes; change streams improve responsiveness and reduce polling (replica set required).
- Setup snippet:
```java
var messaging = morphium.createMessaging();
messaging.start();
new MessagingCacheSynchronizer(messaging, morphium); // attach synchronizer
```

## Encryption
- Annotate sensitive fields with `@Encrypted` and configure providers/keys via `cfg.encryptionSettings()`.
```java
cfg.encryptionSettings().setCredentialsEncryptionKey("secret");
// Optional: custom providers
// cfg.encryptionSettings().setEncryptionKeyProviderClass(...);
// cfg.encryptionSettings().setValueEncryptionProviderClass(...);
```

## Threading
- Async operations run on a dedicated thread pool (virtual threads by default) configured via `threadPoolSettings()`.
- Messaging has its own thread pool configuration in `messagingSettings()`.

## Extension Points
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

## See Also
- [Messaging](./messaging.md): topic listeners, exclusive vs broadcast, change streams vs polling
- How‑Tos for focused recipes: start at [Basic Setup](./howtos/basic-setup.md)
