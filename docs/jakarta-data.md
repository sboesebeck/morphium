# Jakarta Data: Framework-Agnostic Repository Runtime

`morphium-jakarta-data` is an **optional Morphium module** that implements the
[Jakarta Data 1.0](https://jakarta.ee/specifications/data/1.0/) specification on top of
Morphium's core query engine. It gives Morphium a standard, `@Repository`-based data
access layer — query derivation from method names, JDQL, `@Find`/`@Delete` methods,
pagination, and sorting — without depending on any particular application framework.

## What is Jakarta Data?

Jakarta Data is a Jakarta EE specification that standardizes the repository pattern for
Java persistence: you declare an interface such as `CrudRepository<Product, String>`,
add derived-query methods like `findByCategory(String category)`, and a runtime
generates the implementation for you — comparable in spirit to Spring Data repositories,
but as a vendor-neutral Jakarta specification. It defines the core annotations
(`@Repository`, `@Find`, `@Query`, `@OrderBy`, `@By`), pagination types (`Page`,
`CursoredPage`, `PageRequest`), and sorting types (`Sort`, `Order`) that any compliant
provider implements against its own data store. `morphium-jakarta-data` is Morphium's
provider for this specification, translating Jakarta Data semantics into Morphium
`Query` calls against MongoDB (or PoppyDB/InMemoryDriver).

## Purpose and Scope

This module is **not** something most application code depends on directly. It contains
only the framework-agnostic runtime: parsing, query building, and result-type adaptation
as plain Java classes with zero dependencies on Quarkus, Spring, or any DI container.

!!! note "Applications typically don't add this module directly"
    Applications normally consume Jakarta Data through a full framework integration:
    **quarkus-morphium** (Gizmo bytecode generation at build time) or
    **spring-boot-morphium** (JDK dynamic proxies at runtime). Those modules pull in
    `morphium-jakarta-data` transitively and wire the generated/proxied repositories
    into their respective dependency-injection containers.

    `morphium-jakarta-data` is directly relevant to you if you are **building your own
    framework integration** — for a DI container or framework not already covered by
    the two integrations above. See [Building your own framework integration](#building-your-own-framework-integration)
    below.

## Dependency Direction

`morphium-jakarta-data` depends on Morphium core (`de.caluga:morphium`) and on the
Jakarta Data API (`jakarta.data:jakarta.data-api`). The dependency direction is strictly
one-way: **module → core, never core → module.** Morphium core has no knowledge of
Jakarta Data and no compile- or runtime dependency on this module.

!!! note "The core does not pull in Jakarta Data"
    If your application only declares a dependency on `de.caluga:morphium`, you do
    **not** get `jakarta.data-api` on your classpath, and none of the Jakarta Data
    annotations or types described on this page are available. You must add
    `de.caluga:morphium-jakarta-data` (or one of the framework integrations) explicitly
    to use any of this.

## Maven Coordinates

```xml
<dependency>
  <groupId>de.caluga</groupId>
  <artifactId>morphium-jakarta-data</artifactId>
  <version>${project.version}</version>
</dependency>
```

In the Morphium reactor, `${project.version}` resolves to whatever version the reactor is
currently on (see the root `pom.xml`). This module follows Morphium's regular release
versioning; there is no separate version line to track.

## Repository Interfaces

Two base interfaces are available:

- `jakarta.data.repository.CrudRepository<T, K>` — the standard Jakarta Data interface:
  `insert`, `insertAll`, `update`, `updateAll` (from `CrudRepository`), plus
  `save`, `saveAll`, `findById`, `existsById`, `findAll`, `delete`, `deleteAll`,
  `deleteById` (inherited from `BasicRepository`).
- `de.caluga.morphium.data.MorphiumRepository<T, K>` — extends `CrudRepository<T, K>`
  with Morphium-specific escape hatches that have no equivalent in the Jakarta Data 1.0
  specification: `distinct(String fieldName)` and `morphium()` (direct access to the
  underlying `Morphium` instance for aggregation pipelines, atomic field operations,
  change streams, and messaging), plus `query()` as a shortcut for
  `morphium().createQueryFor(entityClass)`.

All standard Jakarta Data features — query derivation, `@Find`, `@Query`/JDQL,
pagination, sorting — work identically on both interfaces. Morphium ORM annotations
(`@Version`, `@CreationTime`, `@PreStore`, `@Cache`, `@Reference`, `@Aliases`, ...)
work transparently on the entity because the generated implementation delegates to the
regular Morphium API underneath.

### Example

```java
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.data.MorphiumRepository;
import de.caluga.morphium.driver.MorphiumId;
import jakarta.data.repository.Repository;

import java.util.List;
import java.util.Optional;

@Entity
public class Product {
    @Id
    private MorphiumId id;
    private String category;
    private String name;
    private double price;
    private boolean active;

    // getters/setters omitted
}

@Repository
public interface ProductRepository extends MorphiumRepository<Product, MorphiumId> {

    List<Product> findByCategory(String category);

    Optional<Product> findByName(String name);

    long countByCategory(String category);
}
```

```java
// Using it via a framework integration (Quarkus/Spring inject the implementation):
List<Product> active = productRepository.findByCategory("electronics");

// Morphium-specific escape hatches from MorphiumRepository:
List<Object> categories = productRepository.distinct("category");
Morphium m = productRepository.morphium();
```

## Query Derivation

Repository method names are parsed by `MethodNameParser` into a `QueryDescriptor`,
which `QueryExecutor` then translates into a Morphium `Query`. The parser recognizes the
prefixes `find`, `count`, `exists`, `delete` followed by `By`, e.g. `findByStatus`,
`countByCategory`, `existsById`, `deleteByStatus`. A `By` with nothing after it
(`findBy()`, `countBy()`, ...) matches all entities.

The following table lists every keyword the parser supports, each verified directly
against `MethodNameParser.java`.

| Keyword | Example method | Resulting Morphium condition |
|---|---|---|
| `findBy` | `findByStatus(String status)` | `query.f("status").eq(status)` — implicit `Equals` when no operator suffix matches (`MethodNameParser.java:161-165`) |
| `countBy` | `countByCategory(String category)` | Prefix maps to `Prefix.COUNT`, executed as `query.countAll()` (`MethodNameParser.java:51`, `QueryExecutor.java:59`) |
| `existsBy` | `existsById(String id)` | Prefix maps to `Prefix.EXISTS`, executed as `query.countAll() > 0` (`MethodNameParser.java:52`, `QueryExecutor.java:60`) |
| `deleteBy` | `deleteByStatus(String status)` | Prefix maps to `Prefix.DELETE`, executed via `query.delete()` after counting matches (`MethodNameParser.java:53`, `QueryExecutor.java:61-69`) |
| `And` | `findByStatusAndCategory(String s, String c)` | Combinator `AND`: both conditions applied to the same query (`MethodNameParser.java:78-87`, `QueryExecutor.java:91-95`) |
| `Or` | `findByStatusOrCategory(String s, String c)` | Combinator `OR`: `query.or(...)` combining sub-queries (`MethodNameParser.java:82-84`, `QueryExecutor.java:82-90`) |
| `Between` | `findByPriceBetween(double min, double max)` | `{ price: { $gte: min, $lte: max } }` (`MethodNameParser.java:145-148,190`, `QueryExecutor.java:189-194`) |
| `In` | `findByStatusIn(List<String> statuses)` | `{ status: { $in: statuses } }` (`MethodNameParser.java:201`, `QueryExecutor.java:195`) |
| `Like` | `findByNameLike(String pattern)` | SQL-style `%`/`_` pattern converted to anchored `$regex` (`MethodNameParser.java:200`, `QueryExecutor.java:197-199`, `likeToRegex` at `QueryExecutor.java:255-274`) |
| `GreaterThan` | `findByPriceGreaterThan(double price)` | `{ price: { $gt: price } }` (`MethodNameParser.java:174`, `QueryExecutor.java:185`) |
| `LessThan` | `findByPriceLessThan(double price)` | `{ price: { $lt: price } }` (`MethodNameParser.java:175`, `QueryExecutor.java:187`) |
| `Not` | `findByStatusNot(String status)` | `{ status: { $ne: status } }` — matched last among suffixes to avoid shadowing `NotIn`/`NotNull`/etc. (`MethodNameParser.java:195`, `QueryExecutor.java:184`) |
| `OrderBy` | `findByStatusOrderByCreatedAtDesc(String status)` | `query.sort({ createdAt: -1 })` after applying conditions (`MethodNameParser.java:69-76,103-132`, `QueryExecutor.java:47-49,145-155`) |

Beyond the keywords requested for this table, the parser also supports (verified at the
same source locations, `MethodNameParser.java:172-202`): `GreaterThanEqual`,
`LessThanEqual`, `NotIn`, `StartsWith`, `EndsWith`, `Contains`, `NotContains`, `Matches`/
`Regex`, `IgnoreCase`, `IsNull`/`Null`, `IsNotNull`/`NotNull`, `IsEmpty`/`Empty`,
`IsNotEmpty`/`NotEmpty`, `IsTrue`/`True`, `IsFalse`/`False`, `Size`, and `Is`/`Equals` as
explicit equality suffixes.

Return type overrides — a single-entity return type (`T`), `Optional<T>`, or
`Stream<T>` — are detected by the build-time code generator and passed to the runtime
bridge (`QueryMethodBridge.executeQuery`), which adjusts the effective `ReturnType`
accordingly (`QueryMethodBridge.java:84-108`).

## JDQL via `@Query`

For queries that don't fit the method-name convention, annotate a method with
`@jakarta.data.repository.Query("...")` using JDQL (Jakarta Data Query Language).
`JdqlParser` parses the string into a `JdqlQuery`; `JdqlMethodBridge` executes it.

The supported grammar, verified against `JdqlParser.java`:

```
[SELECT field1, field2 [FROM EntityName]]
[WHERE condition [AND|OR condition ...]]
[GROUP BY field1, field2 [HAVING aggregateCondition [AND|OR ...]]]
[ORDER BY field [ASC|DESC] [, field [ASC|DESC] ...]]
```

Condition grammar (`JdqlParser.java:14-33`):

- `field = :param`, `field <> :param`, `field != :param`
- `field > :param` / `>=` / `<` / `<=`
- `field BETWEEN :min AND :max`
- `field IN :param`
- `field NOT IN :param`
- `field LIKE :param`
- `field IS NULL` / `field IS NOT NULL`
- Boolean literals: `field = true` / `field = false`
- Numeric literals: `field > 100`
- String literals: `field = 'value'`
- `NOT` prefix on any condition or parenthesized group: `NOT field = :param`,
  `NOT (cond1 OR cond2)`
- Parenthesized groups with `AND`/`OR` nesting: `field1 = :a AND (field2 IS NULL OR field2 = '')`

Not supported: JOINs, subqueries (documented explicitly in `JdqlParser.java:33`).

Aggregate/grouping support (`JdqlQuery.java:25-33`, `JdqlMethodBridge.java:443-620`):
`SELECT COUNT(this)`, `SUM(field)`, `AVG(field)`, `MIN(field)`, `MAX(field)` are
compiled into a Morphium aggregation pipeline (`$match` → `$group` → optional `$match`
for `HAVING` → optional `$sort`). `GROUP BY` results must be mapped into a Java `record`
whose canonical constructor matches the `SELECT` field order.

### Examples

```java
@Repository
public interface ProductRepository extends MorphiumRepository<Product, MorphiumId> {

    @Query("WHERE category = :cat AND price BETWEEN :min AND :max ORDER BY price")
    List<Product> searchInPriceRange(@Param("cat") String category,
                                      @Param("min") double min,
                                      @Param("max") double max);

    @Query("WHERE active = true AND (category = :cat OR category IS NULL)")
    List<Product> findActiveInCategoryOrUncategorized(@Param("cat") String category);

    record CategorySummary(String category, long count, double avgPrice) {}

    @Query("SELECT category, COUNT(this), AVG(price) FROM Product GROUP BY category HAVING COUNT(this) > :minCount")
    List<CategorySummary> summarizeByCategory(@Param("minCount") long minCount);
}
```

## `@Find` / `@Delete` with `@By` Parameter Binding

As an alternative to method-name derivation, annotate a method with
`@jakarta.data.repository.Find` or `@jakarta.data.repository.Delete` and bind each
parameter explicitly with `@By("fieldName")`. `FindMethodBridge` applies each `@By`
parameter as an equality condition (`FindMethodBridge.java:68-77`), then layers on
dynamic `Sort`/`Order`/`Limit`/`PageRequest` parameters if present.

```java
@Repository
public interface ProductRepository extends MorphiumRepository<Product, MorphiumId> {

    @Find
    List<Product> byCategoryAndActive(@By("category") String category,
                                       @By("active") boolean active);

    @Delete
    void removeByCategory(@By("category") String category);
}
```

`@Delete` with `@By` parameters loads matching entities and deletes them one by one via
`morphium.delete(entity)` (`FindMethodBridge.java:251-254`) — unlike derived
`deleteBy*` methods, which use a bulk `query.delete()`.

## Pagination

Three types cover pagination: `jakarta.data.page.Page<T>`,
`jakarta.data.page.CursoredPage<T>`, and `jakarta.data.page.PageRequest`.

- **Offset pagination** (`Page<T>`): pass a `PageRequest` (e.g.
  `PageRequest.ofPage(1, 20, true)`) to a repository method; the runtime computes
  `skip`/`limit` from `page()`/`size()` and, if `requestTotal()` is true, issues a
  separate `countAll()` query for the total (`AbstractMorphiumRepository.java:71-99`,
  `MorphiumPage.java`).
- **Cursor (keyset) pagination** (`CursoredPage<T>`): pass a `PageRequest` in one of the
  cursor modes; the runtime builds a keyset condition from the previous page's last
  sort-key values and fetches one extra row to determine `hasNext`/`hasPrevious`
  (`AbstractMorphiumRepository.java:102-153`, `CursorHelper.java`).

```java
// Offset pagination
PageRequest request = PageRequest.ofPage(1, 20, true);
Page<Product> page = productRepository.findAll(request, Order.by(Sort.asc("name")));
long total = page.totalElements();
Page<Product> next = productRepository.findAll(page.nextPageRequest(), Order.by(Sort.asc("name")));

// Cursor pagination via @Find + PageRequest parameter
@Find
@OrderBy("createdAt")
CursoredPage<Product> allOrderedByCreation(PageRequest pageRequest);
```

!!! note "When to prefer cursor pagination over offset pagination"
    Offset pagination (`Page<T>`, `skip`/`limit`) re-evaluates `skip` on every request,
    so results can shift or duplicate if documents are inserted or deleted between page
    requests, and `skip` on large offsets becomes expensive as MongoDB still has to walk
    past the skipped documents. Cursor pagination (`CursoredPage<T>`) anchors each page
    request to the sort-key values of the last row seen, so it stays stable and
    efficient under concurrent writes and for deep pagination. Prefer `CursoredPage<T>`
    whenever the underlying data can change between page fetches or the result set is
    large; keep `Page<T>` for small, mostly-static datasets or when you need
    `totalPages()`/direct page-number jumps.

## Sorting

Sorting is available through three complementary mechanisms:

- `jakarta.data.Sort<T>` / `jakarta.data.Order<T>` — pass a dynamic `Sort` or `Order`
  parameter to a `@Find`/`@Query` method; `SortMapper.apply(...)` (and the equivalent
  inline logic in `FindMethodBridge`/`JdqlMethodBridge`) resolves each `Sort.property()`
  to its MongoDB field name and applies ascending/descending order
  (`SortMapper.java:26-36`).
- `@jakarta.data.repository.OrderBy("field")` — a static, compile-time ordering
  annotation on the repository method, merged with any method-name-derived `OrderBy`
  clause (`QueryMethodBridge.java:70-80,143-175`).
- `OrderBy<Field>[Asc|Desc]` suffix on derived query method names, e.g.
  `findByStatusOrderByCreatedAtDesc` (`MethodNameParser.java:103-132`).

```java
@Find
@OrderBy(value = "price", descending = true)
List<Product> allSortedByPriceDesc();

// Dynamic sort parameter
List<Product> found = productRepository.query()
    .f("category").eq("electronics")
    .sort(Map.of("price", 1))
    .asList();
```

## Return Types

`QueryResultHelper` enforces Jakarta Data's single-result semantics for the two
single-entity helper methods it provides; the broader set of return types is handled by
the calling bridges (`QueryExecutor`, `FindMethodBridge`, `JdqlMethodBridge`), which
route to the right result shape.

| Return type | Behavior | Source |
|---|---|---|
| `T` (single entity) | `requireSingle`: throws `EmptyResultException` on zero results, `NonUniqueResultException` on more than one | `QueryResultHelper.java:34-44` |
| `Optional<T>` | `optionalSingle`: `Optional.empty()` on zero results, `Optional.of(entity)` on exactly one, `NonUniqueResultException` on more than one | `QueryResultHelper.java:53-63` |
| `List<T>` | `query.asList()` | `QueryExecutor.java:57`, `FindMethodBridge.java:163`, `JdqlMethodBridge.java:184` |
| `Stream<T>` | `query.stream()` | `QueryExecutor.java:56`, `FindMethodBridge.java:161`, `JdqlMethodBridge.java:182` |
| `Page<T>` | `MorphiumPage` built from `skip`/`limit` results plus optional total count | `AbstractMorphiumRepository.java:71-99`, `FindMethodBridge.java:150`, `JdqlMethodBridge.java:165` |
| `CursoredPage<T>` | `CursoredPageRecord` built via keyset lookup | `AbstractMorphiumRepository.java:102-153`, `FindMethodBridge.java:126-129`, `JdqlMethodBridge.java:148-151` |
| `long` (count) | `query.countAll()` | `QueryExecutor.java:59`, `JdqlMethodBridge.java:169-171` |
| `boolean` (exists) | `query.countAll() > 0` | `QueryExecutor.java:60`, `JdqlMethodBridge.java:172-174` |
| `CompletionStage<T>` (async) | Wraps any of the above in `CompletableFuture.supplyAsync(...)` on the Morphium async operations thread pool | `QueryMethodBridge.java:120-141`, `FindMethodBridge.java:257-274`, `JdqlMethodBridge.java:775-797`, `AbstractMorphiumRepository.java:246-284` |
| Scalar aggregate (`long`/`double`/boxed) | Single `COUNT`/`SUM`/`AVG`/`MIN`/`MAX` from JDQL, converted via `toNumber(...)` | `JdqlMethodBridge.java:605-615,764-773` |
| `Object[]` | Multiple aggregate functions in one `SELECT` (no `GROUP BY`) return one array slot per aggregate | `JdqlMethodBridge.java:596-614` |
| `List<Record>` | JDQL `GROUP BY` queries mapped into a caller-supplied Java `record` matching the `SELECT` clause | `JdqlMethodBridge.java:566-589,674-734` |

## Building Your Own Framework Integration

If neither `quarkus-morphium` nor `spring-boot-morphium` fits your target environment,
you can build your own thin adapter on top of `morphium-jakarta-data`. The key
extension point is `AbstractMorphiumRepository<T, K>`: it implements all CRUD logic as
plain `doXxx()` methods (`doFindById`, `doFindAll`, `doSave`, `doDelete`, ...) and
exposes a `protected void setMorphium(Morphium morphium)` setter that your framework
subclass or generated proxy must call to wire in a live `Morphium` instance before any
`doXxx()` method is used.

A minimal, framework-free example — implementing `MorphiumRepository<Product, MorphiumId>`
by hand, without any bytecode generation or dynamic proxy. `MorphiumRepository` extends
`CrudRepository` which extends `BasicRepository`, so a full implementation covers all
three interfaces' methods; every one of them delegates directly to a `doXxx()` method
already provided by `AbstractMorphiumRepository`:

```java
import de.caluga.morphium.Morphium;
import de.caluga.morphium.data.AbstractMorphiumRepository;
import de.caluga.morphium.data.MorphiumRepository;
import de.caluga.morphium.data.RepositoryMetadata;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.query.Query;
import jakarta.data.Order;
import jakarta.data.page.Page;
import jakarta.data.page.PageRequest;

import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

public class ProductRepositoryImpl
        extends AbstractMorphiumRepository<Product, MorphiumId>
        implements MorphiumRepository<Product, MorphiumId> {

    public ProductRepositoryImpl(Morphium morphium) {
        super(new RepositoryMetadata(Product.class, MorphiumId.class, "id"));
        setMorphium(morphium); // wires the Morphium instance for all doXxx() calls
    }

    // -- BasicRepository<Product, MorphiumId> --

    @Override
    public <S extends Product> S save(S entity) {
        return (S) doSave(entity);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <S extends Product> List<S> saveAll(List<S> entities) {
        return (List<S>) (List<?>) doSaveAll(entities);
    }

    @Override
    public Optional<Product> findById(MorphiumId id) {
        return doFindById(id);
    }

    @Override
    public Stream<Product> findAll() {
        return doFindAll();
    }

    @Override
    public Page<Product> findAll(PageRequest pageRequest, Order<Product> sortBy) {
        return doFindAllPaged(pageRequest, sortBy);
    }

    @Override
    public void deleteById(MorphiumId id) {
        doDeleteById(id);
    }

    @Override
    public void delete(Product entity) {
        doDelete(entity);
    }

    @Override
    public void deleteAll(List<? extends Product> entities) {
        doDeleteAll(entities);
    }

    // -- CrudRepository<Product, MorphiumId> --

    @Override
    public <S extends Product> S insert(S entity) {
        return (S) doInsert(entity);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <S extends Product> List<S> insertAll(List<S> entities) {
        return (List<S>) (List<?>) doInsertAll(entities);
    }

    @Override
    public <S extends Product> S update(S entity) {
        return (S) doUpdate(entity);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <S extends Product> List<S> updateAll(List<S> entities) {
        return (List<S>) (List<?>) doUpdateAll(entities);
    }

    // -- MorphiumRepository<Product, MorphiumId> extensions --

    @Override
    public List<Object> distinct(String fieldName) {
        return doDistinct(fieldName);
    }

    @Override
    public Morphium morphium() {
        return doMorphium();
    }

    @Override
    public Query<Product> query() {
        return doQuery();
    }

    // -- A hand-written derived query, without any code generation --

    public List<Product> findByCategory(String category) {
        return morphium().createQueryFor(Product.class)
                .f("category").eq(category)
                .asList();
    }
}
```

Quarkus and Spring Boot differ only in *how* they call `setMorphium(...)` and how they
generate the repository interface implementation:

- **Quarkus**: build-time Gizmo bytecode generation produces a concrete subclass of
  `AbstractMorphiumRepository`; the Quarkus extension injects the `Morphium` instance
  via `@Inject` and a `@PostConstruct` callback that calls `setMorphium(...)`.
- **Spring Boot**: a JDK dynamic proxy backed by an `AbstractMorphiumRepository`
  instance is created by a `FactoryBean`; `setMorphium(...)` is invoked from the
  factory once the `Morphium` bean is available.

Any integration you write follows the same shape: construct or generate a repository
implementation that extends `AbstractMorphiumRepository`, call `setMorphium(...)` once a
`Morphium` instance is available, and either hand-implement the derived-query methods
(as above) or reuse `MethodNameParser`/`QueryMethodBridge`,
`JdqlParser`/`JdqlMethodBridge`, and `FindMethodBridge` to interpret method names,
`@Query` strings, and `@Find`/`@Delete`/`@By` annotations at runtime instead of
generating bytecode.

## Limitations

Jakarta Data 1.0, as implemented by this module, does **not** cover every Morphium
capability. Fall back to `MorphiumRepository.query()` (or `MorphiumRepository.morphium()`
for the full Morphium API) when you need:

- **Joins / references across collections.** JDQL explicitly excludes subqueries and
  joins (`JdqlParser.java:33`). Cross-collection lookups need Morphium's `@Reference`
  resolution or manual queries.
- **Aggregation pipeline stages beyond `COUNT`/`SUM`/`AVG`/`MIN`/`MAX` with `GROUP BY`.**
  JDQL's aggregate support compiles to a fixed `$match → $group → $match(HAVING) → $sort`
  pipeline shape. Anything requiring `$unwind`, `$lookup`, `$facet`, or custom pipeline
  stages needs `morphium().createAggregator(...)` directly.
  `MorphiumRepository.distinct(fieldName)` is provided as a targeted escape hatch for
  distinct-value queries, since Jakarta Data has no equivalent.
- **Atomic field operations** (`$inc`, `$push`, `$pull`, `$set` on individual fields) —
  use `Morphium`'s update methods via `morphium()` directly.
- **Change streams, messaging, and other Morphium-specific runtime features** — none of
  these have a Jakarta Data equivalent; access them through `morphium()`.
- **Lifecycle callbacks on bulk `deleteBy*` methods.** Derived `deleteBy*` methods use a
  bulk `query.delete()` for performance and therefore do **not** fire `@PreRemove`/
  `@PostRemove` (documented explicitly at `QueryExecutor.java:63-66`). If lifecycle hooks
  must run, load and delete entities individually via `Morphium.delete(entity)` — this
  is exactly what `@Delete`-with-`@By` methods do (`FindMethodBridge.java:251-254`),
  so prefer that annotation style over derived `deleteBy*` when lifecycle callbacks
  matter.
- **Complex boolean nesting beyond one level of parenthesized grouping in method-name
  derivation.** `MethodNameParser` only understands a single flat `And`/`Or` chain per
  method name (with `OrderBy` split off). Nested boolean logic needs JDQL's
  parenthesized groups (`@Query`) or a hand-written Morphium `Query`.

For anything not covered by `findBy*`/`@Find`/`@Query`, `MorphiumRepository.query()`
returns a plain Morphium `Query<T>` you can compose with the full fluent API — no
Jakarta Data restrictions apply beyond that point.
