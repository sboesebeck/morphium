# Quarkus Morphium Showcase

[![Build](https://github.com/Bardioc1977/quarkus-morphium-showcase/actions/workflows/build.yml/badge.svg)](https://github.com/Bardioc1977/quarkus-morphium-showcase/actions/workflows/build.yml)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Quarkus](https://img.shields.io/badge/Quarkus-3.32.1-blue)](https://quarkus.io)
[![Java](https://img.shields.io/badge/Java-21%2B-orange)](https://adoptium.net)
[![Jakarta Data](https://img.shields.io/badge/Jakarta%20Data-1.0-green)](https://jakarta.ee/specifications/data/1.0/)
[![Live Demo](https://img.shields.io/badge/demo-morphium.kopp--cloud.de-blueviolet)](https://morphium.kopp-cloud.de)

> **[Live Demo: morphium.kopp-cloud.de](https://morphium.kopp-cloud.de)**
>
> Built with **Morphium 6.2.1-SNAPSHOT** and **quarkus-morphium Extension 1.1.1-SNAPSHOT**.
> Fork improvements are being contributed back to the upstream [sboesebeck/morphium](https://github.com/sboesebeck/morphium)
> project via pull requests and are progressively merged.

An interactive demo application showcasing [Morphium](https://github.com/sboesebeck/morphium) ORM
and **Jakarta Data 1.0** with [Quarkus](https://quarkus.io), [Qute](https://quarkus.io/guides/qute) templates
and [HTMX](https://htmx.org).

Built on the [quarkus-morphium](https://github.com/Bardioc1977/quarkus-morphium) extension.

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
- **Banking** -- `@MorphiumTransactional` declarative transactions, CDI transaction events

### Extended
- **Analytics** -- Aggregation pipeline with `$group`, `$sort`, `$match`
- **Audit Log** -- `@PreStore` / `@PostStore` lifecycle callbacks
- **Store Locator** -- `2dsphere` index, `$near` geospatial queries
- **Messaging** -- Morphium's built-in messaging system with SSE live updates
- **Polymorphism** -- Inheritance mapping, custom type mappers

### Jakarta Data
- **Jakarta Data** -- Interactive side-by-side comparison of Morphium API vs `@Repository`
- **Query Builder** -- Fluent query API vs query derivation vs JDQL
- **GROUP BY + HAVING** -- JDQL aggregation with Record return types, AND/OR combinators
- **Stream + Async** -- Cursor-backed `Stream<T>` and `CompletionStage<T>` demos

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

> **Note:** The quarkus-morphium extension is resolved from
> [GitHub Packages](https://github.com/Bardioc1977/quarkus-morphium/packages).
> For local development, you can also build the extension from source:
> ```bash
> git clone https://github.com/Bardioc1977/quarkus-morphium.git
> cd quarkus-morphium && mvn install -DskipTests && cd ..
> ```

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
# Set GitHub credentials for Maven (quarkus-morphium SNAPSHOT resolution)
export GITHUB_ACTOR=your-github-username
export GITHUB_TOKEN=your-github-pat

# Build and start
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

67 tests covering all repositories, services, and query operators.

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

- **[Live Demo](https://morphium.kopp-cloud.de)** -- try the showcase online
- [quarkus-morphium](https://github.com/Bardioc1977/quarkus-morphium) -- Quarkus extension for Morphium (with Jakarta Data 1.0)
- [Bardioc1977/morphium](https://github.com/Bardioc1977/morphium) -- fork of Morphium used by this showcase
- [sboesebeck/morphium](https://github.com/sboesebeck/morphium) -- upstream Morphium ORM
- [Quarkus](https://quarkus.io) -- supersonic, subatomic Java framework
- [Jakarta Data 1.0](https://jakarta.ee/specifications/data/1.0/) -- the specification
