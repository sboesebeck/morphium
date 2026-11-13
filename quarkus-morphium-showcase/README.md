# Quarkus Morphium Showcase

[![Build](https://github.com/Bardioc1977/quarkus-morphium-showcase/actions/workflows/build.yml/badge.svg)](https://github.com/Bardioc1977/quarkus-morphium-showcase/actions/workflows/build.yml)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Quarkus](https://img.shields.io/badge/Quarkus-3.32.3-blue)](https://quarkus.io)
[![Java](https://img.shields.io/badge/Java-21%2B-orange)](https://adoptium.net)
[![Jakarta Data](https://img.shields.io/badge/Jakarta%20Data-1.0-green)](https://jakarta.ee/specifications/data/1.0/)
[![quarkus-morphium](https://img.shields.io/badge/quarkus--morphium-6.3.1-blue)](https://central.sonatype.com/artifact/de.caluga/quarkus-morphium)

An interactive demo application showcasing [Morphium](https://github.com/sboesebeck/morphium) ORM
and **Jakarta Data 1.0** with [Quarkus](https://quarkus.io), [Qute](https://quarkus.io/guides/qute) templates
and [HTMX](https://htmx.org).

Built on the [quarkus-morphium](https://github.com/sboesebeck/morphium/tree/develop/quarkus-morphium)
extension, part of the main Morphium repository since its 6.3.0 modularization. Both
`de.caluga:quarkus-morphium` and `de.caluga:morphium` are published to Maven Central — no bundled
jars, no GitHub Packages credentials, no fork required to build this showcase.

### New in 6.3.1

- **Migration Runner** -- `@MorphiumChangeUnit`/`@Execution`/`@RollbackExecution`, a changelog,
  and a distributed lock so multiple instances never run the same migration concurrently
- **Field-Level Encryption** -- `@Encrypted` transparently encrypts individual entity fields
  before they hit MongoDB
- **JCache (JSR-107) Integration** -- `MorphiumCacheJCacheImpl` as a drop-in alternative to the
  default in-memory cache
- **Change Streams** -- realtime insert/update/delete notifications via `ChangeStreamMonitor`,
  streamed to the browser over Server-Sent Events
- **`CursoredPage<T>`** -- keyset (cursor) pagination alongside the existing offset `Page<T>`
- **`MorphiumId` JSON serialization** -- entities serialize their `@Id MorphiumId` field as a
  plain 24-character hex string over REST, no custom serializer needed
- **CDI transaction lifecycle events** -- `@MorphiumTxPhase(BEFORE_COMMIT/AFTER_COMMIT/AFTER_ROLLBACK)`
  observers alongside `@MorphiumTransactional`

<details>
<summary>Earlier releases</summary>

**quarkus-morphium v1.1.0** (pre-modularization fork, since merged into the main Morphium
repository):

- **GROUP BY + Aggregation** -- `SELECT department, COUNT(this), SUM(salary) GROUP BY department` with Java Record return types
- **HAVING (AND/OR)** -- `HAVING COUNT(this) > :min OR SUM(salary) >= :threshold`
- **Stream** -- cursor-backed `Stream<T>` for memory-efficient large result set processing
- **Async** -- `CompletionStage<T>` for non-blocking repository queries
</details>

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

```bash
# Clone and run (Dev Services starts MongoDB automatically)
git clone https://github.com/Bardioc1977/quarkus-morphium-showcase.git
cd quarkus-morphium-showcase
mvn quarkus:dev
```

Open [http://localhost:8080](http://localhost:8080) in your browser.

`quarkus-morphium` and `morphium` are resolved directly from Maven Central -- no extra
repository configuration, no credentials, no need to build the extension from source.

## Docker Quickstart

Run the showcase with a single command -- no JDK, Maven, or local MongoDB required:

```bash
docker compose up
```

Open [http://localhost:8080](http://localhost:8080) in your browser.

```bash
# Stop containers (keep data)
docker compose down

# Stop containers and remove data
docker compose down -v
```

### Local build

```bash
docker compose -f docker-compose.yml -f docker-compose.build.yml up --build
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
mvn test
```

92 tests covering all repositories, services, and query operators.

## Building

```bash
# Full build with tests
mvn verify

# Skip tests
mvn package -DskipTests
```

## Contributing

Contributions are welcome! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

This project follows the [Contributor Covenant Code of Conduct](CODE_OF_CONDUCT.md).

## License

[Apache License 2.0](LICENSE)

## Related Projects

- [sboesebeck/morphium](https://github.com/sboesebeck/morphium) -- Morphium ORM, including the
  `quarkus-morphium` and `morphium-jakarta-data` modules this showcase depends on
- [Bardioc1977/morphium](https://github.com/Bardioc1977/morphium) -- fork used for contributing
  PRs upstream
- [Quarkus](https://quarkus.io) -- supersonic, subatomic Java framework
- [Jakarta Data 1.0](https://jakarta.ee/specifications/data/1.0/) -- the specification
