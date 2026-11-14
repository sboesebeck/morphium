# Quarkus Morphium Showcase

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](../LICENSE)
[![Java](https://img.shields.io/badge/Java-21%2B-orange)](https://adoptium.net)
[![Jakarta Data](https://img.shields.io/badge/Jakarta%20Data-1.0-green)](https://jakarta.ee/specifications/data/1.0/)

> **A module of the Morphium repository.** The showcase lives here since 7.0 (before: `Bardioc1977/quarkus-morphium-showcase`, whose history is kept).
> It is optional and not part of the default build; it always uses the Morphium and Quarkus versions of this checkout (`morphium-parent`).
>
> ```bash
> mvn -Pshowcase -pl quarkus-morphium/deployment,quarkus-morphium-showcase -am install -DskipTests    # build it with the modules it needs (Quarkus loads the deployment module of the extension, so it is named, too)
> mvn -Pshowcase -pl quarkus-morphium-showcase quarkus:dev                # run it (dev mode, MongoDB through Dev Services)
> mvn -Pshowcase -pl quarkus-morphium-showcase test                       # its tests
> docker build -f quarkus-morphium-showcase/Dockerfile -t quarkus-morphium-showcase:local .   # the image, context is the repository root
> ```

An interactive demo application showcasing [Morphium](https://github.com/sboesebeck/morphium) ORM
and **Jakarta Data 1.0** with [Quarkus](https://quarkus.io), [Qute](https://quarkus.io/guides/qute) templates
and [HTMX](https://htmx.org).

Built on the [quarkus-morphium](../quarkus-morphium) extension of this repository.

### What the showcase shows of 7.0

- **Writes that fail raise** -- a unique index violation or a `$pull` on a non-array is a `MorphiumDriverException` (`isDuplicateKey()`), no longer ignored
- **Update values are mapped like `store()`** -- embedded objects, enums and `java.time` in `Query.set/push`; no `Date.from(...)` workarounds
- **Atomic updates** -- `Query.modify()`, `@AtomicUpdate`, derived `updateBy...` repository methods, leases with `LeaseManager`
- **Jakarta Data** -- `findFirstBy`, `IgnoreCase`, `@Query("UPDATE ...")`, ordinal parameters, repositories without a primary entity
- **Records and enums** -- `@Embedded` records, `@Enumerated` (name or ordinal)
- **Docs hub** -- the documentation of this repository, including [Migrating from 6 to 7](/docs/howtos/migration-v6-to-v7)

Coming from Morphium 6: the [migration page](../docs/howtos/migration-v6-to-v7.md) lists what to look at.

---

## Jakarta Data -- Declarative Repositories for MongoDB

The showcase demonstrates how Jakarta Data `@Repository` interfaces replace imperative
Morphium API calls. Three repositories show different query styles side by side:

### ProductRepository (`MorphiumRepository`)

```java
@Repository
public interface ProductRepository extends MorphiumRepository<Product, MorphiumId> {

    // Query derivation -- method name becomes the query
    List<Product> findByName(String name);
    List<Product> findByPriceBetween(double min, double max);
    long countByStockLessThan(int threshold);
    boolean existsByName(String name);

    // @Find + @By -- explicit field binding with annotations
    @Find
    @OrderBy(value = "price", descending = true)
    List<Product> topByCategory(@By("category.name") String name, Limit limit);

    // @Query (JDQL) -- SQL-like syntax for complex queries
    @Query("WHERE name LIKE :pattern ORDER BY price ASC")
    List<Product> searchByNameLike(@Param("pattern") String pattern);

    @Query("WHERE price >= :min AND price <= :max ORDER BY price ASC")
    List<Product> queryByPriceRange(@Param("min") double min, @Param("max") double max);
}
```

`MorphiumRepository` extends `CrudRepository` with Morphium-specific operations:

```java
// Distinct values for a field -- no Jakarta Data equivalent
List<Object> categories = products.distinct("category.name");

// Direct access to Morphium for aggregation, atomic updates, etc.
products.morphium().inc(product, "stock", 5);

// Create a typed query for complex conditions
Query<Product> q = products.query();
q.f("price").gt(100).f("category.name").eq("electronics");
```

### BlogPostRepository (`BasicRepository`)

```java
@Repository
public interface BlogPostRepository extends BasicRepository<BlogPost, MorphiumId> {

    List<BlogPost> findByPublishedTrue();
    long countByPublishedTrue();

    @Query("WHERE published = true ORDER BY createdAt DESC")
    Page<BlogPost> findPublishedPaged(PageRequest pageRequest);

    @Query("WHERE title LIKE :pattern ORDER BY createdAt DESC")
    List<BlogPost> searchByTitle(@Param("pattern") String titlePattern);
}
```

### EmployeeRepository -- Operator Comparison

The `EmployeeRepository` mirrors every query from the imperative `QueryShowcaseService`
to demonstrate the Jakarta Data equivalent for each Morphium operator:

| Morphium API | Jakarta Data |
|---|---|
| `.f("department").eq(val).asList()` | `findByDepartment(val)` |
| `.f("salary").gt(val).asList()` | `findBySalaryGreaterThan(val)` |
| `.f("salary").gte(min).f("salary").lte(max)` | `findBySalaryBetween(min, max)` |
| `.f("active").eq(true).asList()` | `findByActiveTrue()` |
| `.f("department").eq(dept).countAll()` | `countByDepartment(dept)` |
| `.f("email").eq(e).countAll() > 0` | `existsByEmail(e)` |
| `.sort(Map.of("salary", -1)).limit(2)` | `@Find @OrderBy("salary" DESC) ... Limit.of(2)` |
| `.f("dept").eq(d).f("salary").gte(m)` | `@Query("WHERE department = :d AND salary >= :m")` |
| `agg.group("$dept").sum("count",1).end()` | `@Query("SELECT dept, COUNT(this) GROUP BY dept")` |
| pipeline + `$match` after `$group` | `HAVING COUNT(this) > :min OR SUM(salary) >= :x` |
| `CompletableFuture.supplyAsync(...)` | `CompletionStage<List<Employee>> findByDeptAsync(dept)` |
| `query.f(...).sort(...).asStream()` | `Stream<Employee> streamByDepartment(dept)` |

### Interactive Comparison (Jakarta Data Page)

The `/jakarta-data` page lets you run queries through **both** the Morphium API and Jakarta Data
side by side, showing the code and results for each approach. Includes demonstrations of:

- **Query Derivation** -- findBy, countBy, existsBy, Between, Like, True/False
- **@Find / @OrderBy** -- explicit field binding with Limit
- **@Query (JDQL)** -- WHERE, ORDER BY, GROUP BY, HAVING, aggregates
- **GROUP BY + Aggregation** -- COUNT, SUM with Record return types
- **HAVING (AND/OR)** -- filter aggregated results with flexible combinators
- **Stream** -- cursor-backed lazy loading for memory-efficient processing
- **Async** -- `CompletionStage<T>` for non-blocking repository methods
- **MorphiumRepository** -- distinct() escape hatch

### Morphium ORM features work transparently

`@Version` (optimistic locking), `@CreationTime`/`@LastChange`, lifecycle callbacks
(`@PreStore`, `@PostLoad`), `@Cache`, `@Reference` (lazy/eager) -- all work through
repository calls because the generated implementation delegates to `morphium.store()`,
`morphium.findById()` etc.

### When to use which

| Use Jakarta Data / MorphiumRepository for | Use Morphium API for |
|---|---|
| Standard CRUD (save, findById, delete) | Complex aggregation pipelines ($project, $unwind) |
| Queries (findBy, countBy, @Query JDQL) | Atomic field operations (inc, push, pull) |
| GROUP BY + HAVING aggregation | Bulk updates ($set, $unset) |
| Paginated results (Page, CursoredPage) | Change streams, messaging |
| Stream / async queries | Geospatial queries ($near, $geoWithin) |
| Distinct queries (via `MorphiumRepository`) | Multi-stage aggregation pipelines |
| Testable interfaces (easy to mock) | Map-reduce operations |

Both approaches work together. Use `MorphiumRepository` for the best of both worlds --
standard Jakarta Data CRUD plus `morphium()` and `query()` as the escape hatch.

---

## All Showcase Features

### Core
- **Product Catalog** -- CRUD, `@Entity`, `@Embedded`, `@Version` (optimistic locking), `@Index`
- **Blog System** -- `@Reference` between entities, `@CreationTime`, pagination, comments
- **Banking** -- `@MorphiumTransactional` declarative transactions, CDI transaction lifecycle events

### Extended
- **Analytics** -- Aggregation pipeline with `$group`, `$sort`, `$match`
- **Audit Log** -- `@PreStore` / `@PostStore` lifecycle callbacks
- **Store Locator** -- `2dsphere` index, `$near` geospatial queries
- **Messaging** -- Morphium's built-in messaging system with SSE live updates
- **Change Streams** -- realtime insert/update/delete notifications via `ChangeStreamMonitor`, streamed over SSE
- **Polymorphism** -- Inheritance mapping, custom type mappers

### Jakarta Data
- **Jakarta Data** -- Interactive side-by-side comparison of Morphium API vs `@Repository`
- **Query Builder** -- Fluent query API vs query derivation vs JDQL
- **GROUP BY + HAVING** -- JDQL aggregation with Record return types, AND/OR combinators
- **Stream + Async** -- Cursor-backed `Stream<T>` and `CompletionStage<T>` demos
- **Pagination** -- Offset `Page<T>` and keyset `CursoredPage<T>` side by side
- **`MorphiumId` JSON** -- a real JSON REST endpoint proving the `@Id` field serializes as a plain hex string

### Platform
- **Migration Runner** -- `@MorphiumChangeUnit`, changelog, distributed lock
- **Security & Caching** -- `@Encrypted` field-level encryption, JCache (JSR-107) integration

### Advanced
- **Bulk Import** -- `SequenceGenerator`, batch inserts, cursor-based iteration
- **Reference Cascade** -- `@Reference(cascadeDelete, orphanRemoval)`, cycle detection
- **CosmosDB** -- Auto-detection via `BackendType`; graceful degradation
- **Docs Hub** -- Integrated Morphium documentation rendered from Markdown

---

## Quick Start

From the root of the Morphium repository (the showcase is an optional module, profile `showcase`):

```bash
mvn -Pshowcase -pl quarkus-morphium-showcase -am install -DskipTests
mvn -Pshowcase -pl quarkus-morphium-showcase quarkus:dev      # Dev Services starts MongoDB automatically
```

Open [http://localhost:8080](http://localhost:8080) in your browser. The showcase is built against the Morphium and Quarkus versions of the checkout, not against a release.

## Docker

The image is built from the root of the repository and published by the workflow of the maintainer's fork; the server pulls `:latest` (Watchtower).

```bash
docker build -f quarkus-morphium-showcase/Dockerfile -t quarkus-morphium-showcase:local .
docker compose -f quarkus-morphium-showcase/docker-compose.yml up        # image from GHCR
docker compose -f quarkus-morphium-showcase/docker-compose.build.yml up --build   # build locally
```

## Prerequisites

| Dependency | Minimum version |
|---|---|
| Java | 21 |
| Maven | 3.9 |
| Docker | (for Dev Services) |

## Testing

Tests use Morphium's `InMemDriver` -- no Docker or MongoDB needed:

```bash
mvn -Pshowcase -pl quarkus-morphium-showcase test
```

Covers all repositories, services, query operators and the docs hub.

## Contributing

Changes go through pull requests to the Morphium repository, like any other module; see its `docs/contributing.md`.

## License

[Apache License 2.0](../LICENSE)

## Related Projects

- [sboesebeck/morphium](https://github.com/sboesebeck/morphium) -- Morphium ORM, including the
  `quarkus-morphium` and `morphium-jakarta-data` modules this showcase depends on
