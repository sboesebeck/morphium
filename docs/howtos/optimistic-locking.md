# Optimistic Locking with `@Version`

Morphium supports **optimistic concurrency control** via the `@Version` annotation. It prevents **lost updates** — a situation where two concurrent writes overwrite each other silently — without requiring database-level locks.

## How it works

When a field is annotated with `@Version`, Morphium manages it automatically:

| Event | Behaviour |
|-------|-----------|
| First `store()` (INSERT) | Field is set to `1L` |
| Every subsequent `store()` (UPDATE) | Field is incremented by `1L` |
| Concurrent write detected | `VersionMismatchException` is thrown |

The version value is checked server-side on every UPDATE. If the document in the database already has a higher version number, the update is rejected and the caller must reload and retry.

## Basic usage

```java
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.annotations.Version;
import de.caluga.morphium.driver.MorphiumId;

@Entity
public class Order {
    @Id
    private MorphiumId id;

    @Version
    private long version;       // managed by Morphium — do not set manually

    private String status;
    private double total;

    // getters / setters
}
```

```java
// First store — version is set to 1 automatically
Order order = new Order();
order.setStatus("PENDING");
order.setTotal(99.90);
morphium.store(order);  // order.getVersion() == 1 after this call

// Second store — version is incremented to 2
order.setStatus("CONFIRMED");
morphium.store(order);  // order.getVersion() == 2 after this call
```

## Custom field name

By default the MongoDB field name follows the camelCase convention (same rule as `@Property`). You can override it:

```java
@Version(fieldName = "v")   // stored as "v" in MongoDB
private long version;
```

## Handling conflicts

When a stale entity is stored, Morphium throws `VersionMismatchException`:

```java
import de.caluga.morphium.VersionMismatchException;

try {
    morphium.store(order);
} catch (VersionMismatchException e) {
    long staleVersion = e.getExpectedVersion(); // the version we tried to write with
    // reload from DB and apply changes again
    Order fresh = morphium.createQueryFor(Order.class)
        .f("_id").eq(order.getId())
        .get();
    fresh.setStatus(order.getStatus());
    morphium.store(fresh); // fresh version from DB → succeeds
}
```

## Inheritance

`@Version` can be placed on a base class. All subclasses inherit the optimistic-locking behaviour:

```java
@Entity
public abstract class BaseDocument {
    @Id private MorphiumId id;

    @Version
    private long version;
}

@Entity(collectionName = "orders")
public class Order extends BaseDocument {
    private String status;
}

@Entity(collectionName = "invoices")
public class Invoice extends BaseDocument {
    private double amount;
}
```

## Entities without `@Version`

Entities without a `@Version` field are **not affected**. Morphium falls back to the standard replace behaviour for those classes.

## Testing with InMemoryDriver

`@Version` is fully supported by the `InMemoryDriver`, so no real MongoDB is needed for tests:

```java
MorphiumConfig cfg = new MorphiumConfig();
cfg.setDriverName("InMemDriver");
cfg.connectionSettings().setDatabase("test");

try (Morphium morphium = new Morphium(cfg)) {
    Order o = new Order();
    morphium.store(o);
    assert o.getVersion() == 1L;

    Order copy = morphium.createQueryFor(Order.class).f("_id").eq(o.getId()).get();
    copy.setStatus("CONFIRMED");
    morphium.store(copy);   // copy.version → 2

    o.setStatus("CANCELLED");
    assertThrows(VersionMismatchException.class, () -> morphium.store(o)); // o still has version=1
}
```

## Comparison with JPA `@Version`

| | JPA `@Version` | Morphium `@Version` |
|---|---|---|
| Annotation | `javax.persistence.Version` | `de.caluga.morphium.annotations.Version` |
| Exception | `OptimisticLockException` | `VersionMismatchException` |
| Version init | `0` (first persist sets to `1`) | `1L` on first store |
| Type | `int`, `long`, `Timestamp`, … | `long` / `Long` |
| Backend | SQL `WHERE id=? AND version=?` | MongoDB `{$and:[{_id},{version}]}` filter |
| In-memory testing | Requires JPA provider setup | `InMemoryDriver` — zero infrastructure |
