# Spring Boot Starter: Auto-Configuration for Morphium

`morphium-spring-boot-*` is an **optional Morphium module** that integrates Morphium
into [Spring Boot](https://spring.io/projects/spring-boot) applications via
auto-configuration, type-safe `@ConfigurationProperties`, declarative transactions,
an Actuator health indicator, and Jakarta Data `@Repository` interfaces backed by JDK
dynamic proxies at runtime — no build-time bytecode generation, no annotation
processor for the repositories themselves. It pulls in
[`morphium-jakarta-data`](jakarta-data.md) for the entire query-derivation, JDQL, and
pagination runtime.

!!! note "Optional module — the Morphium core does not depend on it"
    `de.caluga:morphium` has zero compile- or runtime dependency on this module, on
    Spring, or on `jakarta.data-api`. You only need `morphium-spring-boot-starter` if
    you are building a Spring Boot application against MongoDB via Morphium.

## What it provides

- **Auto-configuration** — `MorphiumAutoConfiguration` creates the application's
  single `Morphium` bean from `morphium.*` properties, with connection retry on
  transient failures (linear backoff) and a best-effort classpath pre-scan for
  `@Entity`/`@Embedded` classes so Morphium can skip its own internal scan at startup.
- **Type-safe configuration** — every setting lives under `morphium.*` as
  `@ConfigurationProperties`, with `spring-boot-configuration-processor`-generated
  metadata for IDE autocompletion.
- **Jakarta Data repositories** — declare a `@Repository` interface extending
  `CrudRepository`/`MorphiumRepository` from `morphium-jakarta-data`; at Spring
  context-startup time, `MorphiumRepositoryRegistrar` scans for such interfaces and
  registers a `MorphiumRepositoryFactoryBean` for each, which creates a
  `java.lang.reflect.Proxy` implementing the interface — see
  [Proxy mechanism vs. Quarkus](#proxy-mechanism-vs-quarkus) below. See
  [Jakarta Data](jakarta-data.md) for the full query-derivation, JDQL, and pagination
  feature set — everything documented there works identically once wired through this
  module's proxies.
- **Declarative transactions** — `@MorphiumTransactional` on a Spring bean method
  wraps the method body in `startTransaction()`/`commitTransaction()`/
  `abortTransaction()` via an AspectJ `@Around` advice, active only when
  `spring-boot-starter-aop` is on the classpath.
- **Actuator health** — a `HealthIndicator` reporting live MongoDB connection status
  (database, driver, replica-set state) under `/actuator/health`, active only when
  `spring-boot-actuator` is present and a `Morphium` bean already exists.
- **Test support** — the companion `morphium-spring-boot-test` module provides
  `@MorphiumTest`, a composite annotation that wires `InMemDriver` (Morphium's
  in-memory MongoDB emulation) into a `@SpringBootTest`, so repository tests run
  without a MongoDB instance or container.

## Installation

```xml
<dependency>
  <groupId>de.caluga</groupId>
  <artifactId>morphium-spring-boot-starter</artifactId>
  <version>${project.version}</version>
</dependency>
```

In the Morphium reactor, `${project.version}` currently resolves to `6.3.0-SNAPSHOT`.
This module follows Morphium's regular release versioning — it is versioned and
released in lockstep with Morphium; there is no separate version line to track.

## Configuration Reference

All properties live under `morphium.*` (not `spring.morphium.*` — the `spring.*`
namespace is reserved for Spring Boot's own configuration keys). Every entry below is
verified directly against `MorphiumProperties.java` in the
`morphium-spring-boot-autoconfigure` module.

| Property | Default | Description | Source |
|---|---|---|---|
| `morphium.database` | *(required)* | MongoDB database name | `MorphiumProperties.java:46` |
| `morphium.hosts` | `localhost:27017` | Comma-separated `host:port` list; ignored if `morphium.atlas-url` is set | `MorphiumProperties.java:39` |
| `morphium.username` / `.password` | -- | Optional credentials, applied only when both are set | `MorphiumProperties.java:52,57` |
| `morphium.auth-database` | `admin` | Authentication database (`authSource`) | `MorphiumProperties.java:64` |
| `morphium.driver-name` | `PooledDriver` | `PooledDriver` (production) or `InMemDriver` (tests, no MongoDB needed) | `MorphiumProperties.java:71` |
| `morphium.read-preference` | `primary` | MongoDB read preference | `MorphiumProperties.java:77` |
| `morphium.max-connections` | `250` | Connection pool size | `MorphiumProperties.java:82` |
| `morphium.atlas-url` | -- | MongoDB Atlas SRV connection string (overrides `morphium.hosts` when set) | `MorphiumProperties.java:89` |
| `morphium.replica-set-name` | -- | Replica set name (required for transactions) | `MorphiumProperties.java:97` |
| `morphium.connect-retries` | `5` | Connection attempts before giving up on transient failures, linear backoff `attempt * 2000`ms | `MorphiumProperties.java:106` |
| `morphium.index-check` | `CREATE_ON_STARTUP` | `CREATE_ON_STARTUP`, `WARN_ON_STARTUP`, `CREATE_ON_WRITE_NEW_COL`, `NO_CHECK` | `MorphiumProperties.java:115` |
| `morphium.cache.global-valid-time` | `5000` | Cache TTL in milliseconds | `MorphiumProperties.java:361` |
| `morphium.cache.read-cache-enabled` | `true` | Enable query result cache | `MorphiumProperties.java:368` |
| `morphium.ssl.enabled` | `false` | Enable TLS | `MorphiumProperties.java:418` |
| `morphium.ssl.keystore-path` / `.keystore-password` | -- | Keystore (JKS/PKCS12) for client-certificate TLS | `MorphiumProperties.java:426,431` |

If `spring-boot-configuration-processor` is on the classpath (declared as an optional
dependency of `morphium-spring-boot-autoconfigure`), every property above also appears
in `META-INF/spring-configuration-metadata.json`, giving IDEs autocompletion and
validation for `morphium.*` keys.

## Quick Example

```java
@Entity(collectionName = "products")
public class Product {
    @Id private MorphiumId id;
    private String name;
    private double price;
    private String category;
    // getters/setters omitted
}

@Repository
public interface ProductRepository extends MorphiumRepository<Product, MorphiumId> {
    List<Product> findByCategory(String category);

    List<Product> findByPriceGreaterThan(double minPrice);
}

@SpringBootApplication
@EnableMorphiumRepositories
public class MyApplication {
    public static void main(String[] args) {
        SpringApplication.run(MyApplication.class, args);
    }
}

@Service
public class ProductService {
    @Autowired ProductRepository products;

    public List<Product> findExpensive(double minPrice) {
        return products.findByPriceGreaterThan(minPrice);
    }
}
```

```properties
morphium.database=my-app-db
morphium.hosts=localhost:27017
```

## Repository Usage

Annotate a `@SpringBootApplication` (or any `@Configuration` class) with
`@EnableMorphiumRepositories` to enable scanning. By default the scan covers the
annotated class's package and sub-packages; pass explicit packages via `value()`/
`basePackages()` to scan elsewhere.

Repository interfaces extend either `jakarta.data.repository.CrudRepository<T, K>`
(the plain Jakarta Data interface) or `de.caluga.morphium.data.MorphiumRepository<T,
K>`, which adds Morphium-specific escape hatches with no Jakarta Data equivalent:

```java
// Distinct values for a field
List<Object> categories = products.distinct("category");

// Direct access to the Morphium API
products.morphium().inc(product, "stock", 5);

// A typed Morphium Query, for anything beyond derived queries/JDQL/@Find
Query<Product> q = products.query();
q.f("price").gt(100).f("category").eq("electronics");
```

### Proxy mechanism vs. Quarkus

This module uses **JDK dynamic proxies at runtime** — the standard Spring Data
pattern — in contrast to the [Quarkus extension](quarkus-extension.md), which uses
**Gizmo bytecode generation at build time**.

Concretely: at Spring context-startup time, `MorphiumRepositoryRegistrar` (imported by
`@EnableMorphiumRepositories`) scans the configured base packages for `@Repository`
interfaces and registers a `MorphiumRepositoryFactoryBean` bean definition for each
one found. Each factory bean creates a `java.lang.reflect.Proxy` implementing the
repository interface, backed by a `MorphiumRepositoryInvocationHandler` that
dispatches every method call — derived queries, JDQL, `@Find`/`@Delete`, plain CRUD —
to the shared `morphium-jakarta-data` runtime. No implementation class is ever
generated or compiled; the proxy is synthesized by the JVM itself, once per repository
interface, the first time the bean is requested.

Quarkus's `quarkus-morphium` extension instead runs a build-time processor that emits
a real, compiled implementation class via Gizmo bytecode generation before the
application ever starts — no proxy or reflective dispatch exists at runtime there at
all. The trade-off is the classic one: this module's proxies need zero build-time
tooling and work with plain `javac`, at the cost of a small amount of per-call
reflective dispatch overhead and no build-time validation of query derivation;
Quarkus's build-time generation avoids that runtime cost and validates earlier, at the
cost of requiring its build-time augmentation phase. Both mechanisms delegate to the
exact same `morphium-jakarta-data` query engine — only *how* a repository interface is
wired to that engine differs.

## Transactions

Requires a MongoDB replica set or Atlas cluster (`morphium.replica-set-name`) — a
standalone MongoDB node rejects multi-document transactions.

```java
@Service
public class OrderService {
    @Autowired Morphium morphium;

    @MorphiumTransactional
    public void placeOrder(Order order, Payment payment) {
        morphium.store(order);
        morphium.store(payment);
        // committed automatically on return, rolled back automatically on exception
    }
}
```

`@MorphiumTransactional` is picked up by an AspectJ `@Around` advice
(`MorphiumTransactionAspect`) that is only active when `spring-boot-starter-aop` is on
the classpath and a `Morphium` bean exists in the context. It starts a transaction
before the advised method runs, commits on normal return, and aborts (rethrowing the
original exception unchanged) if the method throws.

## Health / Actuator

When `spring-boot-actuator` is on the classpath and a `Morphium` bean already exists,
`MorphiumHealthAutoConfiguration` registers a `HealthIndicator` under
`/actuator/health`:

```json
{
  "components": {
    "morphium": {
      "status": "UP",
      "details": {
        "database": "my-app-db",
        "driver": "PooledDriver",
        "replicaSet": true,
        "replicaSetName": "rs0"
      }
    }
  }
}
```

Disable it with `management.health.morphium.enabled=false`, or override it entirely
by defining your own `@Bean(name = "morphiumHealthIndicator") HealthIndicator` — the
auto-configured bean backs off via `@ConditionalOnMissingBean(name =
"morphiumHealthIndicator")`.

## Testing without a MongoDB instance

```properties
# src/test/resources/application-test.properties
morphium.database=test
morphium.driver-name=InMemDriver
```

```java
@SpringBootTest
@ActiveProfiles("test")
@EnableMorphiumRepositories
class ProductRepositoryTest {
    @Autowired ProductRepository repository;

    @Test
    void shouldFindByCategory() {
        repository.save(new Product("Widget", 9.99, "tools"));
        assertThat(repository.findByCategory("tools")).hasSize(1);
    }
}
```

The companion `morphium-spring-boot-test` module wraps the same properties into a
composite `@MorphiumTest` annotation:

```java
@MorphiumTest
@EnableMorphiumRepositories
class ProductRepositoryTest {
    @Autowired ProductRepository repository;
    // InMemDriver is auto-configured — no MongoDB instance or container needed
}
```

`InMemDriver` is Morphium's in-memory MongoDB emulation — tests run against it with no
container and no external MongoDB, exactly like the core Morphium test suite.

## Abgrenzung zu Spring Data MongoDB

This module is **not** a replacement for, or a re-implementation of, Spring Data
MongoDB, and does not aim to be API-compatible with it:

- It implements the **Jakarta Data 1.0** specification (`@Repository`,
  `CrudRepository`, `@Find`, `@Query`/JDQL, `Page`/`CursoredPage`, `Sort`/`Order`) — a
  vendor-neutral Jakarta EE specification — not Spring Data's own repository
  interfaces or query-method conventions.
- The underlying data access is always **Morphium**, not Spring Data MongoDB's
  `MongoTemplate`/`MongoOperations`. There is no `MongoTemplate` bean and no Spring
  Data MongoDB entity mapping; entities use Morphium's own annotations (`@Entity`,
  `@Id`, `@Reference`, etc.).
- Transactions are Morphium transactions wrapped by a small AOP aspect, not Spring's
  `PlatformTransactionManager`/`@Transactional` infrastructure.
- Query derivation, JDQL, and pagination/sorting behavior come from
  `morphium-jakarta-data`; the keyword set and grammar differ in detail from Spring
  Data's query-method conventions, even though simple method names
  (`findByCategory`, `countByStatus`, ...) often look similar.

If your application already uses Spring Data MongoDB and does not use Morphium, this
module has nothing to offer you. If you are building on Morphium and want a
Spring-managed, dependency-injected repository layer with Jakarta Data semantics, this
is the module for that.

## Full Documentation

This page is an overview. The complete module documentation — installation, the full
property reference, repository usage, transactions, testing, and the detailed
architecture comparison with Quarkus — lives in the module's own README:

[`morphium-spring-boot-starter/README.md`](https://github.com/sboesebeck/morphium/tree/develop/morphium-spring-boot-starter/README.md)

See also [Jakarta Data](jakarta-data.md) for the framework-agnostic repository runtime
this module builds on, and [Quarkus Extension](quarkus-extension.md) for the
build-time-bytecode alternative to this module's runtime JDK proxies.
