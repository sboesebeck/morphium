# Morphium Spring Boot Starter

[![Build](https://github.com/Bardioc1977/spring-boot-morphium/actions/workflows/build.yml/badge.svg)](https://github.com/Bardioc1977/spring-boot-morphium/actions/workflows/build.yml)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Spring Boot](https://img.shields.io/badge/Spring%20Boot-3.4.13-brightgreen)](https://spring.io/projects/spring-boot)
[![Java](https://img.shields.io/badge/Java-21%2B-orange)](https://adoptium.net)
[![Jakarta Data](https://img.shields.io/badge/Jakarta%20Data-1.0-green)](https://jakarta.ee/specifications/data/1.0/)

A [Spring Boot](https://spring.io/projects/spring-boot) auto-configuration for
[Morphium](https://github.com/sboesebeck/morphium), an actively maintained MongoDB ORM
for Java -- with full **Jakarta Data 1.0** repository support.

> **Part of the Morphium project.** This module is being integrated into the main
> [Morphium](https://github.com/sboesebeck/morphium) reactor and is versioned and
> released **in lockstep with Morphium** -- there is no separate release cadence or
> version line to track. Building the Morphium reactor builds this module against the
> exact Morphium core version in the same build. Maven coordinates are
> `morphium-spring-boot-starter` / `morphium-spring-boot-autoconfigure` /
> `morphium-spring-boot-test` (not `spring-boot-morphium-*` -- that naming was used
> before integration; see [MIGRATION-NOTES.md](MIGRATION-NOTES.md) for the full
> rename history).

> **Companion project:** See [quarkus-morphium](https://github.com/Bardioc1977/quarkus-morphium)
> for Quarkus integration with the same Jakarta Data feature set.

---

## Features

- **Auto-configuration** -- `Morphium` bean created from `morphium.*` properties
- **Jakarta Data repositories** -- `@Repository` interfaces with JDK dynamic proxies (runtime)
- **Query derivation** -- `findBy*`, `countBy*`, `existsBy*`, `deleteBy*` with And/Or, Between, In, Like, etc.
- **JDQL** -- `@Query("WHERE status = :s ORDER BY name")` Jakarta Data Query Language
- **@Find / @Delete** -- explicit field binding via `@By` parameters
- **Transactions** -- `@MorphiumTransactional` with AOP-based commit/rollback
- **Actuator health** -- Morphium connection status in `/actuator/health`
- **Test support** -- `@MorphiumTest` composite annotation with InMemDriver (no MongoDB needed)
- **MorphiumRepository** -- escape hatch for `distinct()`, `query()`, `morphium()` access

---

## Prerequisites

| Dependency | Minimum version |
|---|---|
| Java | 21 |
| Spring Boot | 3.4.x |
| Morphium | 6.2.2 ([sboesebeck/morphium](https://github.com/sboesebeck/morphium)) |

## Installation

Add the starter to your `pom.xml`:

```xml
<dependency>
    <groupId>de.caluga</groupId>
    <artifactId>morphium-spring-boot-starter</artifactId>
    <version>6.3.2-SNAPSHOT</version>
</dependency>
```

In the Morphium reactor, `${project.version}` currently resolves to `6.3.2-SNAPSHOT`.
This module follows Morphium's regular release versioning -- there is no independent
version to pin beyond the reactor version.

> **Note:** Until published to Maven Central, build the reactor locally:
> ```bash
> git clone https://github.com/sboesebeck/morphium.git
> cd morphium
> mvn install -DskipTests
> ```

## Quick Start

### 1. Configure

```properties
# application.properties
morphium.database=my-database
morphium.hosts=localhost:27017
```

### 2. Define an entity

```java
@Entity(collectionName = "products")
public class Product {
    @Id private MorphiumId id;
    private String name;
    private double price;
    private String category;

    // getters, setters, constructors
}
```

### 3. Create a repository

```java
@Repository
public interface ProductRepository extends MorphiumRepository<Product, MorphiumId> {

    List<Product> findByCategory(String category);

    List<Product> findByPriceGreaterThan(double minPrice);

    long countByCategory(String category);

    @Query("WHERE category = :cat AND price > :minPrice ORDER BY price")
    List<Product> findExpensive(@Param("cat") String category,
                                @Param("minPrice") double minPrice);
}
```

### 4. Enable and inject

```java
@SpringBootApplication
@EnableMorphiumRepositories
public class MyApplication {
    public static void main(String[] args) {
        SpringApplication.run(MyApplication.class, args);
    }
}
```

```java
@Service
public class ProductService {

    @Autowired ProductRepository products;

    public List<Product> findExpensive(double minPrice) {
        return products.findByPriceGreaterThan(minPrice);
    }
}
```

---

## Jakarta Data Repository Support

| Feature | Details |
|---------|---------|
| **CRUD** | `CrudRepository<T,K>`, `MorphiumRepository<T,K>` -- save, insert, update, delete, findById, findAll |
| **Query derivation** | `findBy`, `countBy`, `existsBy`, `deleteBy` with operators: Equals, Not, GreaterThan, LessThan, Between, In, NotIn, Like, StartsWith, EndsWith, Null, NotNull, True, False -- combined with And/Or |
| **@Find + @By** | Explicit field binding via parameter annotations |
| **@Query (JDQL)** | Jakarta Data Query Language with WHERE, ORDER BY, named parameters, BETWEEN, IN, LIKE, IS NULL, NOT, GROUP BY, HAVING, aggregates |
| **@OrderBy** | Static sort annotation on query methods |
| **Pagination** | `Page<T>`, `PageRequest`, `CursoredPage<T>` (keyset pagination) |
| **Sorting** | `Sort<T>`, `Order<T>` as method parameters |
| **Stream** | `Stream<T>` return type for large result sets |
| **Async** | `CompletionStage<T>` return type for non-blocking operations |

### MorphiumRepository -- The Escape Hatch

`MorphiumRepository<T,K>` extends `CrudRepository` with Morphium-specific operations:

```java
// Distinct values for a field
List<Object> categories = products.distinct("category");

// Direct access to the Morphium API
products.morphium().inc(product, "stock", 5);

// Create a typed Morphium Query
Query<Product> q = products.query();
q.f("price").gt(100).f("category").eq("electronics");
```

---

## Configuration Reference

Property prefix is `morphium` (not `spring.morphium`) -- the `spring.*` namespace is
reserved for Spring Boot's own configuration keys. Every property below is verified
directly against `MorphiumProperties.java`.

| Property | Default | Description | Source |
|---|---|---|---|
| `morphium.database` | *(required)* | MongoDB database name | `MorphiumProperties.java:46` |
| `morphium.hosts` | `localhost:27017` | Comma-separated `host:port` list; ignored if `morphium.atlas-url` is set | `MorphiumProperties.java:39` |
| `morphium.username` | -- | MongoDB username; only applied together with `morphium.password` | `MorphiumProperties.java:52` |
| `morphium.password` | -- | MongoDB password | `MorphiumProperties.java:57` |
| `morphium.auth-database` | `admin` | Authentication database (`authSource`) | `MorphiumProperties.java:64` |
| `morphium.driver-name` | `PooledDriver` | `PooledDriver` (production) or `InMemDriver` (tests, no MongoDB needed) | `MorphiumProperties.java:71` |
| `morphium.read-preference` | `primary` | MongoDB read preference | `MorphiumProperties.java:77` |
| `morphium.max-connections` | `250` | Connection pool size | `MorphiumProperties.java:82` |
| `morphium.atlas-url` | -- | MongoDB Atlas SRV URL (overrides `morphium.hosts` when set) | `MorphiumProperties.java:89` |
| `morphium.replica-set-name` | -- | Replica set name (required for transactions) | `MorphiumProperties.java:97` |
| `morphium.connect-retries` | `5` | Connection attempts before giving up on transient failures (linear backoff, `attempt * 2000`ms) | `MorphiumProperties.java:106` |
| `morphium.index-check` | `CREATE_ON_STARTUP` | `CREATE_ON_STARTUP`, `WARN_ON_STARTUP`, `CREATE_ON_WRITE_NEW_COL`, `NO_CHECK` | `MorphiumProperties.java:115` |
| `morphium.cache.global-valid-time` | `5000` | Cache TTL in milliseconds | `MorphiumProperties.java:361` |
| `morphium.cache.read-cache-enabled` | `true` | Enable query result cache | `MorphiumProperties.java:368` |
| `morphium.ssl.enabled` | `false` | Enable TLS | `MorphiumProperties.java:418` |
| `morphium.ssl.keystore-path` | -- | Keystore path (JKS/PKCS12) for client-certificate TLS | `MorphiumProperties.java:426` |
| `morphium.ssl.keystore-password` | -- | Keystore password | `MorphiumProperties.java:431` |

If `spring-boot-configuration-processor` is on the classpath (it is an optional
dependency of `morphium-spring-boot-autoconfigure`), every property above also appears
in `META-INF/spring-configuration-metadata.json`, giving IDEs autocompletion and
validation for `morphium.*` keys.

## Transactions

Requires a MongoDB replica set or Atlas.

```java
@Service
public class OrderService {

    @Autowired Morphium morphium;

    @MorphiumTransactional
    public void placeOrder(Order order, Payment payment) {
        morphium.store(order);
        morphium.store(payment);
        // auto-commit on success, auto-rollback on exception
    }
}
```

## Actuator Health

When `spring-boot-actuator` is on the classpath, a Morphium health indicator is
automatically registered at `/actuator/health`:

```json
{
  "status": "UP",
  "components": {
    "morphium": {
      "status": "UP",
      "details": {
        "database": "my-database",
        "driver": "PooledDriver",
        "replicaSet": true,
        "replicaSetName": "rs0"
      }
    }
  }
}
```

## Testing

### Option A: InMemDriver (no MongoDB required)

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

        var results = repository.findByCategory("tools");
        assertThat(results).hasSize(1);
        assertThat(results.get(0).getName()).isEqualTo("Widget");
    }
}
```

### Option B: @MorphiumTest annotation

The `morphium-spring-boot-test` module provides a composite annotation:

```xml
<dependency>
    <groupId>de.caluga</groupId>
    <artifactId>morphium-spring-boot-test</artifactId>
    <version>6.3.2-SNAPSHOT</version>
    <scope>test</scope>
</dependency>
```

```java
@MorphiumTest
@EnableMorphiumRepositories
class ProductRepositoryTest {

    @Autowired ProductRepository repository;

    @Test
    void shouldFindByCategory() {
        // InMemDriver is auto-configured
    }
}
```

## Module Structure

```
spring-boot-morphium/
  morphium-spring-boot-autoconfigure/   Auto-configuration, repository proxy, AOP, health
  morphium-spring-boot-starter/         Dependency-only POM (pull this in your app)
  morphium-spring-boot-test/            @MorphiumTest annotation for test support
```

## Architecture

This starter uses **JDK dynamic proxies** at runtime (the standard Spring Data pattern),
in contrast to the [quarkus-morphium](https://github.com/Bardioc1977/quarkus-morphium)
extension which uses **Gizmo bytecode generation** at build time. Concretely: a
repository interface annotated `@Repository` is discovered at Spring context-startup
time by `MorphiumRepositoryRegistrar`, which registers a `MorphiumRepositoryFactoryBean`
that creates a `java.lang.reflect.Proxy` implementing the interface -- no implementation
class is ever generated or compiled. Quarkus instead generates a real, compiled
implementation class via Gizmo bytecode generation before the application starts,
avoiding runtime reflection entirely at the cost of a build-time processing step.

Both share the same query engine via the
[morphium-jakarta-data](https://github.com/Bardioc1977/morphium-jakarta-data) module --
a framework-agnostic library containing all Jakarta Data query derivation, JDQL parsing,
pagination, and CRUD logic.

```
morphium (core ODM)
  └── morphium-jakarta-data (shared Jakarta Data runtime)
        ├── morphium-spring-boot-* (this project, JDK proxies)
        └── quarkus-morphium (Gizmo bytecode, build-time)
```

### Relationship to `morphium-jakarta-data`

`morphium-jakarta-data` contains the entire framework-agnostic Jakarta Data runtime:
`MethodNameParser`/`QueryExecutor` (query derivation from method names), `JdqlParser`/
`JdqlMethodBridge` (the `@Query` JDQL grammar), `FindMethodBridge` (`@Find`/`@Delete`
with `@By` parameter binding), pagination (`AbstractMorphiumRepository`'s offset and
cursor pagination), and sorting. None of that logic is duplicated here.

This module (`morphium-spring-boot-*`) adds exactly the Spring-specific wiring on top:
the `Morphium` bean and `MorphiumProperties` (`morphium.*` configuration,
`MorphiumAutoConfiguration`), the `@EnableMorphiumRepositories`/
`MorphiumRepositoryRegistrar`/`MorphiumRepositoryFactoryBean` JDK-proxy mechanism that
turns a `@Repository` interface into a Spring bean, the `@MorphiumTransactional` AOP
aspect, and the actuator health indicator. Every Jakarta Data feature documented for
`morphium-jakarta-data` (query derivation keywords, JDQL grammar, pagination types,
return-type handling) applies unchanged once wired through this module -- there is no
separate, Spring-specific feature set to learn.

### Distinction from Spring Data MongoDB

This module is **not** a replacement for or a re-implementation of Spring Data
MongoDB, and does not aim to be API-compatible with it:

- It implements the **Jakarta Data 1.0** specification (`@Repository`,
  `CrudRepository`, `@Find`, `@Query`/JDQL, `Page`/`CursoredPage`, `Sort`/`Order`), a
  vendor-neutral Jakarta EE specification -- not Spring Data's own repository
  interfaces (`MongoRepository`, `@Query` with a different string syntax, Spring
  Data's `Criteria`/`Aggregation` API, etc.).
- The underlying data access is always **Morphium**, not Spring Data MongoDB's own
  `MongoTemplate`/`MongoOperations`. There is no `MongoTemplate` bean and no
  Spring Data MongoDB entity mapping (`@Document`, Spring Data converters) --
  entities use Morphium's own annotations (`@Entity`, `@Id`, `@Reference`, etc.).
- Transactions here are Morphium transactions (`Morphium.startTransaction()`/
  `commitTransaction()`/`abortTransaction()`) wrapped by a small AOP aspect, not
  Spring's `PlatformTransactionManager`/`@Transactional` infrastructure.
- Query derivation, JDQL, and pagination/sorting behavior come from
  `morphium-jakarta-data`; the exact keyword set and grammar there differs in detail
  from Spring Data's query-method conventions (see that module's README/docs for the
  full grammar), even though many method names look similar in simple cases
  (`findByCategory`, `countByStatus`, ...).

If your application already uses Spring Data MongoDB and does not use Morphium, this
module has nothing to offer you. If you are building on Morphium and want a
Spring-managed, dependency-injected repository layer with Jakarta Data semantics, this
is the module for that.

## Building from Source

```bash
# Part of the Morphium reactor -- build from the reactor root, or standalone with
# morphium and morphium-jakarta-data already installed to your local repository.

mvn clean install

# Run tests only
mvn test -pl morphium-spring-boot-autoconfigure
```

## Related Projects

- [Morphium](https://github.com/sboesebeck/morphium) -- the underlying MongoDB ORM
- [morphium-jakarta-data](https://github.com/Bardioc1977/morphium-jakarta-data) -- shared Jakarta Data runtime
- [quarkus-morphium](https://github.com/Bardioc1977/quarkus-morphium) -- Quarkus CDI extension (same Jakarta Data features)
- [quarkus-morphium-showcase](https://github.com/Bardioc1977/quarkus-morphium-showcase) -- interactive demo
- [Jakarta Data 1.0](https://jakarta.ee/specifications/data/1.0/) -- the specification

## Contributing

Contributions are welcome! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

This project follows the [Contributor Covenant Code of Conduct](CODE_OF_CONDUCT.md).

## License

[Apache License 2.0](LICENSE)
