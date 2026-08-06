# Quarkus Morphium Extension

A [Quarkus](https://quarkus.io) CDI extension for [Morphium](https://github.com/sboesebeck/morphium),
an actively maintained MongoDB ORM for Java — with full **Jakarta Data 1.0** support.

> **Module status:** this extension is now an optional module of the
> [Morphium](https://github.com/sboesebeck/morphium) reactor (`quarkus-morphium/`),
> built and released in lockstep with the Morphium core. The core does not depend on
> this module — building Morphium without extensions (`-DskipExtensions`) is unaffected.

### What's new in v1.2.0


- **`MorphiumId` serializes as a hex string by default** — entities with `@Id MorphiumId id`
  now emit `"id":"<hex>"` over REST (Jackson **and** JSON-B), and `MorphiumId` path/query/body
  parameters deserialize from the hex string. No serializer to write yourself.
  - **BREAKING (positive):** replaces the old internal struct
    `{"pid":..,"counter":..,"machineId":..,"bytes":"..","time":..}`, which was unusable as a
    row id on the consumer side (frontend grids got `"[object Object]"`, lost row identity, and
    crashed the renderer). Clients that parsed the old shape must update — none should.

### What's new in v1.1.1

- **Morphium 6.2.1** — now built against the upstream release (no longer requires fork SNAPSHOT)
- **JDQL `NOT BETWEEN`** — `WHERE NOT price BETWEEN :min AND :max`
- **JDQL `NOT (...)` groups** — `WHERE NOT (status = 'OPEN' OR status = 'PENDING')` with De Morgan transformation
- **JDQL error messages** — parse errors now include position and caret pointer
- **Optional health checks** — `quarkus-smallrye-health` is no longer forced on downstream apps
- **Dev UI fix** — external MongoDB connections now show actual host/database instead of `n/a`
- **deleteBy* fix** — uses `query.delete()` instead of loading all entities into memory
- **Write buffer in transactions** — `@MorphiumTransactional` disables write buffering automatically
- **Regex patterns extracted** — JDQL parser patterns compiled once as static fields

<details>
<summary>What was new in v1.1.0</summary>

- **JDQL Aggregation:** `COUNT`, `SUM`, `AVG`, `MIN`, `MAX` with `GROUP BY` (single + multi-field), `HAVING` (AND/OR), `COUNT(field)` NULL filtering
- **Stream:** `Stream<T>` return type with cursor-backed lazy loading
- **Async:** `CompletionStage<T>` for non-blocking repository methods
- **Keyset pagination:** `CursoredPage<T>` for efficient large-collection paging
- **JDQL SELECT projection:** `SELECT field1, field2 WHERE ...`
- **JDQL NOT + string literals:** `WHERE NOT status = 'CANCELLED'`
- **GROUP BY pagination:** `Page<Record>` return type for aggregated results
- **Jakarta Data exceptions:** `EmptyResultException`, `NonUniqueResultException`
- **New query operators:** Contains, Empty, Size, Matches, IgnoreCase, deleteBy*
- **223 integration tests** — all green
</details>

**[Documentation](docs/modules/ROOT/pages/index.adoc)** | **[Showcase Source](https://github.com/Bardioc1977/quarkus-morphium-showcase)**

---

## Jakarta Data 1.0 — Declarative Repositories for MongoDB

Define a `@Repository` interface, inject it, done. The extension generates the implementation
at **Quarkus build time** via Gizmo bytecode generation — no runtime reflection, no proxies,
GraalVM native-image compatible.

```java
@Repository
public interface ProductRepository extends CrudRepository<Product, MorphiumId> {

    List<Product> findByCategory(String category);

    @OrderBy("price")
    List<Product> findByPriceBetween(double min, double max);

    long countByCategory(String category);

    boolean existsByName(String name);

    Page<Product> findByCategory(String category, PageRequest page);

    @Find
    List<Product> search(@By("category") String cat,
                         @By("price") double minPrice,
                         Sort<Product> sort);

    @Query("WHERE category = :cat AND price > :minPrice ORDER BY price")
    List<Product> findExpensive(@Param("cat") String category,
                                @Param("minPrice") double minPrice);

    // GROUP BY with aggregates and HAVING
    @Query("SELECT category, COUNT(this), SUM(price) GROUP BY category HAVING COUNT(this) > :min")
    List<CategoryStats> categoriesAboveMin(@Param("min") long minCount);

    // Async query
    CompletionStage<List<Product>> findByCategoryAsync(String category);

    // Stream for large result sets
    Stream<Product> findByPriceGreaterThan(double minPrice);
}
```

```java
@ApplicationScoped
public class ProductService {

    @Inject ProductRepository products;

    public Page<Product> browse(int page, int size) {
        return products.findByCategory("electronics",
            PageRequest.ofPage(page, size, true));
    }
}
```

### What's supported

| Feature | Details |
|---------|---------|
| **CRUD** | `CrudRepository<T,K>`, `BasicRepository<T,K>`, `DataRepository<T,K>`, `MorphiumRepository<T,K>` — save, insert, update, delete, findById, findAll, existsById |
| **Query derivation** | `findBy`, `countBy`, `existsBy`, `deleteBy` with operators: Equals, Not, GreaterThan, LessThan, Between, In, NotIn, Like, StartsWith, EndsWith, Null, NotNull, True, False — combined with And/Or |
| **@Find + @By** | Explicit field binding via parameter annotations; each `@By`-bound parameter is applied as an equality condition |
| **@Query (JDQL)** | Jakarta Data Query Language with WHERE, ORDER BY, named parameters (`:param`), comparison operators, BETWEEN, IN, LIKE, IS NULL, NOT, string literals, GROUP BY (single + multi-field), HAVING (AND/OR), aggregate functions (COUNT/SUM/AVG/MIN/MAX) |
| **@OrderBy** | Static sort annotation on query methods |
| **Pagination** | `Page<T>`, `PageRequest` with total counts, `Limit`, `CursoredPage<T>` (keyset pagination), `Page<Record>` for GROUP BY results |
| **Sorting** | `Sort<T>`, `Order<T>` as method parameters |
| **Stream** | `Stream<T>` return type with cursor-backed lazy loading for memory-efficient large result sets |
| **Async** | `CompletionStage<T>` return type for non-blocking repository methods (query derivation, `@Find`, `@Query`) |
| **@StaticMetamodel** | Auto-generated `Entity_` classes with `Attribute`, `SortableAttribute`, `TextAttribute` fields — type-safe field references |
| **Build-time validation** | Entity fields, ID types, method signatures validated during `mvn compile` — fail fast, not at runtime |

> **Note:** `@By` currently only supports equality conditions. Jakarta Data's `@Is(Operator)`
> annotation for non-equality `@By` conditions (e.g. `@By("price") @Is(GreaterThanEqual)`)
> requires Jakarta Data 1.1, which is not yet finalized (latest available artifact as of this
> writing is the `1.1.0-M3` milestone) — this module targets the stable `jakarta.data-api:1.0.0`.
> Support for `@Is` is a natural candidate once Jakarta Data 1.1 ships as a final release; for
> non-equality conditions today, use query derivation (`findByPriceGreaterThan(...)`) or `@Query`
> (JDQL) instead.

All Morphium ORM features work transparently through generated repositories: `@Version`
(optimistic locking), `@CreationTime`/`@LastChange`, lifecycle callbacks (`@PreStore`,
`@PostLoad`), `@Cache`, `@WriteBuffer`, and `@Reference` (lazy/eager) — because the
generated implementation delegates to `morphium.store()`, `morphium.findById()` etc.

### MorphiumRepository — The Escape Hatch

`MorphiumRepository<T,K>` extends `CrudRepository` with Morphium-specific operations that
have no equivalent in Jakarta Data 1.0:

```java
@Repository
public interface ProductRepository extends MorphiumRepository<Product, MorphiumId> {

    List<Product> findByCategory(String category);  // Jakarta Data query derivation
}
```

```java
// Distinct values for a field
List<Object> categories = products.distinct("category");

// Direct access to Morphium API for aggregation, atomic updates, etc.
products.morphium().inc(product, "stock", 5);

// Create a typed Morphium Query for complex conditions
Query<Product> q = products.query();
q.f("price").gt(100).f("category").eq("electronics");
```

All standard Jakarta Data features work exactly the same as with `CrudRepository`.
The imperative Morphium API (`@Inject Morphium`) also remains fully available for
aggregation pipelines, bulk updates, and anything beyond standard CRUD.

---

## All Features

### CDI & Lifecycle
- **Zero-boilerplate CDI integration** — inject `Morphium` or any `@Repository` interface directly via `@Inject`
- **Declarative transactions** — `@MorphiumTransactional` with automatic commit/rollback and CDI lifecycle events (`BEFORE_COMMIT`, `AFTER_COMMIT`, `AFTER_ROLLBACK`)
- **Graceful shutdown** — `Morphium.close()` called automatically on application stop

### Developer Experience
- **`MorphiumId` JSON out of the box** — `@Id MorphiumId id` serializes to a flat hex string (`"id":"<hex>"`) and parses back from one, for both Jackson and JSON-B, with no user-written serializer
- **Type-safe configuration** — all settings under `quarkus.morphium.*` in `application.properties`
- **Dev Services** — automatic MongoDB container in dev/test mode via Testcontainers, with optional single-node replica set for transactions
- **Dev UI card** — MongoDB connection info in the Quarkus Dev UI at `/q/dev-ui/`
- **Test-friendly** — `quarkus.morphium.driver-name=InMemDriver` for fast, in-process tests without Docker
- **Blocking call detection** — warns when Morphium writes happen on the Vert.x event loop

### Production
- **Health checks** — MicroProfile liveness, readiness, and startup probes with connection pool metadata
- **SSL/TLS & X.509** — encrypted connections and client-certificate authentication via `quarkus.morphium.ssl.*`
- **GraalVM native ready** — all `@Entity` and `@Embedded` classes registered for reflection at build time
- **CosmosDB compatibility** — `@MorphiumTransactional` gracefully degrades on Azure CosmosDB (auto-detected); supports all Azure sovereign clouds

### Morphium ORM
- **@Reference cascade** — `cascadeDelete` and `orphanRemoval` with automatic cycle detection for bidirectional references
- **Built-in caching** — `@Cache` and `@WriteBuffer` annotations for read cache and async write batching
- **Lifecycle hooks** — `@PreStore`, `@PostStore`, `@PostLoad` etc. on `@Entity` classes
- **Optimistic locking** — `@Version` for concurrent modification detection
- **Schema evolution** — `@Aliases` for legacy field name compatibility

---

## Prerequisites

<!-- MAINTAINERS: these version numbers are duplicated by hand (Markdown has no
     shared-attribute mechanism like docs/modules/ROOT/pages/includes/attributes.adoc
     does for the AsciiDoc guide pages) -- update both this table AND the <version> in
     the snippet below whenever the reactor version in the root pom.xml changes. -->

| Dependency | Minimum version |
|---|---|
| Java | 21 |
| Quarkus | 3.32.3 |
| Morphium | 6.3.0-SNAPSHOT (built in lockstep as part of the [sboesebeck/morphium](https://github.com/sboesebeck/morphium) reactor) |

## Installation

This extension is a module of the Morphium reactor. Add it to your application's
`pom.xml`:

```xml
<dependency>
    <groupId>de.caluga</groupId>
    <artifactId>quarkus-morphium</artifactId>
    <version>6.3.0-SNAPSHOT</version> <!-- MAINTAINERS: keep in sync with the table above and the root pom.xml -->
</dependency>
```

### Migrating from the standalone `io.quarkiverse.morphium` extension

If you previously depended on the standalone Quarkiverse extension, update your
coordinates:

| | Before | After |
|---|---|---|
| groupId | `io.quarkiverse.morphium` | `de.caluga` |
| artifactId | `quarkus-morphium` | `quarkus-morphium` (unchanged) |
| version | `1.2.0` (or earlier) | `6.3.x` (tracks the Morphium core release) |

No package renames, no API changes — only the Maven coordinates move. All
`quarkus.morphium.*` configuration properties are unchanged.

## Quick Start

### 1. Configure

```properties
# Required
quarkus.morphium.database=my-database

# MongoDB hosts (default: localhost:27017)
quarkus.morphium.hosts=mongo1:27017,mongo2:27017

# Or use Dev Services — no config needed, MongoDB starts automatically
```

### 2. Define an entity

```java
@Entity(collectionName = "products")
@Data @NoArgsConstructor
public class Product {
    @Id private MorphiumId id;
    private String name;
    private double price;
    private String category;
    @Version private long version;
}
```

### 3. Create a repository

```java
@Repository
public interface ProductRepository extends CrudRepository<Product, MorphiumId> {

    List<Product> findByCategory(String category);

    @OrderBy("price")
    List<Product> findByPriceGreaterThan(double minPrice);
}
```

### 4. Use it

```java
@ApplicationScoped
public class ProductService {

    @Inject ProductRepository products;

    public Product create(String name, double price, String category) {
        var product = new Product();
        product.setName(name);
        product.setPrice(price);
        product.setCategory(category);
        return products.insert(product);
    }

    public List<Product> findExpensive(double minPrice) {
        return products.findByPriceGreaterThan(minPrice);
    }
}
```

### Imperative API (always available)

For complex queries, aggregations, or atomic operations, inject `Morphium` directly:

```java
@Inject Morphium morphium;

public List<Map<String, Object>> salesByCategory() {
    return morphium.createAggregator(Product.class, Map.class)
        .group("$category").sum("total", "$price").end()
        .sort("-total")
        .aggregateMap();
}
```

## Configuration Reference

| Property | Default | Description |
|---|---|---|
| `quarkus.morphium.database` | *(required)* | MongoDB database name |
| `quarkus.morphium.hosts` | `localhost:27017` | Comma-separated `host:port` list |
| `quarkus.morphium.username` | -- | MongoDB username |
| `quarkus.morphium.password` | -- | MongoDB password |
| `quarkus.morphium.auth-database` | `admin` | Authentication database |
| `quarkus.morphium.atlas-url` | -- | MongoDB Atlas SRV URL (overrides `hosts`) |
| `quarkus.morphium.read-preference` | `primary` | Read preference |
| `quarkus.morphium.index-check` | `create-on-startup` | Index creation strategy (`create-on-startup`, `warn-on-startup`, `create-on-write-new-col`, `no-check`) |
| `quarkus.morphium.max-connections` | `250` | Connection pool size |
| `quarkus.morphium.max-wait-time` | `2000` | Max wait (ms) for a pooled connection / driver-level timeout |
| `quarkus.morphium.default-query-timeout-ms` | `0` | Default server-side query time limit (ms); `0` disables it |
| `quarkus.morphium.replica-set-name` | -- | Replica set name; required for `@MorphiumTransactional` and change streams |
| `quarkus.morphium.connect-retries` | `5` | Connection attempts before giving up |
| `quarkus.morphium.driver-name` | `PooledDriver` | `PooledDriver` (production) or `InMemDriver` (tests) |
| `quarkus.morphium.cache.read-cache-enabled` | `true` | Enable query result cache |
| `quarkus.morphium.cache.global-valid-time` | `60000` | Cache TTL in milliseconds |
| `quarkus.morphium.local-date-time.use-bson-date` | `true` | Store `LocalDateTime` as BSON `ISODate` |
| `quarkus.morphium.ssl.enabled` | `false` | Enable TLS |
| `quarkus.morphium.ssl.auth-mechanism` | -- | `MONGODB-X509` for client-cert auth |
| `quarkus.morphium.ssl.keystore-path` | -- | Keystore path (JKS/PKCS12) |
| `quarkus.morphium.ssl.keystore-password` | -- | Keystore password |
| `quarkus.morphium.ssl.truststore-path` | -- | Truststore path |
| `quarkus.morphium.ssl.truststore-password` | -- | Truststore password |
| `quarkus.morphium.ssl.invalid-hostname-allowed` | `false` | Allow invalid hostnames (dev only) |
| `quarkus.morphium.ssl.x509-username` | -- | X.509 subject DN override |
| `quarkus.morphium.devservices.enabled` | `true` | Enable automatic MongoDB container |
| `quarkus.morphium.devservices.image-name` | `mongo:8` | Docker image for Dev Services |
| `quarkus.morphium.devservices.database-name` | `morphium-dev` | Database name in Dev Services |
| `quarkus.morphium.devservices.replica-set` | `true` | Start as replica set (enables transactions) |
| `quarkus.morphium.health.enabled` | `true` | Enable health checks |
| `quarkus.morphium.migration.migrate-at-start` | `false` | Run pending migrations automatically at startup |
| `quarkus.morphium.migration.change-log-collection` | `morphiumChangeLog` | Collection tracking executed migrations |
| `quarkus.morphium.migration.lock-collection` | `morphiumMigrationLock` | Collection used for the distributed migration lock |
| `quarkus.morphium.migration.lock-ttl-seconds` | `60` | Migration lock TTL in seconds (renewed between migrations) |
| `quarkus.morphium.migration.lock-wait-seconds` | `0` | Seconds to wait for a held migration lock before failing (`0` = fail immediately) |

For detailed descriptions, see the
[Configuration Reference](docs/modules/ROOT/pages/configuration.adoc).

## Transactions

```java
@ApplicationScoped
public class OrderService {

    @Inject Morphium morphium;

    @MorphiumTransactional
    public void placeOrder(Order order, Payment payment) {
        morphium.store(order);
        morphium.store(payment);
        // auto-commit on success, auto-rollback on exception
    }
}
```

React to transaction events via CDI:

```java
void afterCommit(@Observes @MorphiumTxPhase(AFTER_COMMIT) MorphiumTransactionEvent e) {
    // send confirmation, publish domain event, ...
}
```

## Testing

```properties
# src/test/resources/application.properties
%test.quarkus.morphium.driver-name=InMemDriver
%test.quarkus.morphium.database=test-db
```

```java
@QuarkusTest
class ProductRepositoryTest {

    @Inject ProductRepository repository;

    @Test
    void shouldFindByCategory() {
        var p = new Product();
        p.setName("Widget");
        p.setCategory("tools");
        p.setPrice(9.99);
        repository.save(p);

        var results = repository.findByCategory("tools");
        assertThat(results).hasSize(1);
        assertThat(results.get(0).getName()).isEqualTo("Widget");
    }
}
```

## Known Limitation: `sun.misc.Unsafe`

The Morphium ORM uses `sun.misc.Unsafe.allocateInstance()` to instantiate entity classes that
**do not have a no-arg constructor**. This is the de facto standard used by Spring, Jackson,
Gson, Kryo, Hibernate/Objenesis and others.

**To avoid it:** add a no-arg constructor (can be `private` or package-private) to your
`@Entity` classes. When present, Morphium uses it directly and `Unsafe` is never called.

`Unsafe.allocateInstance()` is **not** covered by [JEP 471](https://openjdk.org/jeps/471) (JDK 23).
Once a public replacement API exists, Morphium will migrate to it.

## Building from Source

This module is built as part of the Morphium reactor:

```bash
cd morphium
mvn -pl quarkus-morphium -am verify
```

`-am` also builds `morphium-core` and `morphium-jakarta-data`, this extension's direct
dependencies, in the same reactor run.

## Related Projects

- [quarkus-morphium-showcase](https://github.com/Bardioc1977/quarkus-morphium-showcase) — interactive demo source code
- [Morphium](https://github.com/sboesebeck/morphium) — the underlying MongoDB ORM
- [Quarkus](https://quarkus.io) — supersonic, subatomic Java framework
- [Jakarta Data 1.0](https://jakarta.ee/specifications/data/1.0/) — the specification

## Contributing

Contributions are welcome! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

This project follows the [Contributor Covenant Code of Conduct](CODE_OF_CONDUCT.md).

## License

[Apache License 2.0](LICENSE)
