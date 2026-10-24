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
  <version>6.3.9</version>
</dependency>
```

This module follows Morphium's regular release versioning and is released in lockstep
with Morphium core; there is no separate version line to track.

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

`IgnoreCase` in front of an operator compares without case: `findByCodeIgnoreCaseBetween`, `findByStatusIgnoreCaseNotIn`, `findByNameIgnoreCaseContains`, `...IgnoreCaseGreaterThan`. It applies to its own
condition only. Equality, `Not`, `In`, `NotIn`, `Like`, `Contains`, `StartsWith`, `EndsWith` and `Matches` use a regular expression with the option `i`; the ranges compare the lower-cased values
(`$expr` with `$toLower`, so only ASCII letters are folded). `findByNameIgnoreCase` alone still means "equals, ignoring case".

## JDQL via `@Query`

For queries that don't fit the method-name convention, annotate a method with
`@jakarta.data.repository.Query("...")` using JDQL (Jakarta Data Query Language).
`JdqlParser` parses the string into a `JdqlQuery`; `JdqlMethodBridge` executes it.

Parameters are named (`:min`, bound by `@Param` or the method parameter name) or ordinal (`?1`, `?2`), never both in one query. Ordinal
*n* is the *n*-th method parameter that is not a `Sort`, `Order`, `PageRequest` or `Limit`, counting from one; the same ordinal may appear
more than once. A query that uses an ordinal without a matching parameter, or mixes the two styles, fails when the application is built
(Quarkus) or on the first call (Spring).

A `DELETE FROM Entity [WHERE ...]` statement deletes in one server-side command. The method returns `void`, `int`, `long` or `boolean` (the number of
deleted documents; `int` is checked, a count above `Integer.MAX_VALUE` fails; `boolean` says whether at least one document was deleted); other return types, a `Sort`, `Order`, `PageRequest` or `Limit`
parameter, `ORDER BY`, `GROUP BY` and `HAVING` are refused (at build time in Quarkus, when the proxy is created in Spring). The entity name must be
the simple name of the entity of the repository, or be left out (`DELETE FROM WHERE ...`). Like the derived `deleteBy`, the delete runs no entity
lifecycle callbacks (`@PreRemove`, `@PostRemove`: there is no instance to call them on); the storage listeners get `preRemove(Query)` and
`postRemove(Query)`.

An `UPDATE Entity SET item, ... [WHERE ...]` statement is ONE `update` command, built with the update builder (so field names are translated, values are
marshalled and a `@Version` field is incremented). The method returns `void`, `int`, `long` or `boolean` (at least one document matched), and the number is the number of MATCHED documents, not of
modified ones. A SET item maps to a classic update operator:

| item | operator |
|---|---|
| `f = :x`, `f = 5`, `f = 'text'`, `f = TRUE` | `$set` |
| `f = NULL` | `$unset` (a query for `f IS NULL` finds the document, the field is not stored) |
| `f = f + :n`, `f = f - :n` | `$inc` (with the negated operand for minus) |
| `f = f * :n` | `$mul` |

Everything else, among it a field on the right side that is not the field itself (`a = b + 1`), `||` and functions like `UPPER`, would need an
aggregation pipeline update and is refused, at build time in Quarkus and when the proxy is created in Spring, with a message that names the item. The same rules
for return types, special parameters, `ORDER BY` and the entity name as for `DELETE` apply.

The supported grammar for queries, verified against `JdqlParser.java`:

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
- `field IN :param`, `field IN (1, 'a', :param)` (a list of literals and parameters)
- `field NOT IN :param` / `field NOT IN (...)`
- `field LIKE :param`, `field NOT LIKE :param`; `field NOT BETWEEN :min AND :max`
- `field IS NULL` / `field IS NOT NULL`
- Boolean literals: `field = true` / `field = false`
- Numeric literals: `field > 100`, `-2.5`, `1e3`, and with the suffix of Java: `7L`, `0.0d`, `1.5f`
- String literals: `field = 'value'`, with `''` for a quote inside (`'it''s'`); a keyword, an operator or a parenthesis inside a literal is text
- The constant of an enum: `kind = com.example.Kind.PRIME` (the enum is stored by its name, so the part after the last dot is compared)
- `NOT` prefix on any condition or parenthesized group: `NOT field = :param`,
  `NOT (cond1 OR cond2)`
- Parenthesized groups with `AND`/`OR` nesting: `field1 = :a AND (field2 IS NULL OR field2 = '')`; `AND` binds stronger than `OR`
  (`a OR b AND c` is `a OR (b AND c)`); keywords are case-insensitive

`SELECT field` with ONE field answers the values of that field when the method returns them: `List<String>`, `Optional<Integer>`, `Stream<Long>`, `Page<String>` or a single
`String` (the value is converted to the declared type). On a method that returns the entity, the entity is returned with only that field filled. Several fields need a record
(`GROUP BY`).

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

Every parameter is bound as an equality, so the Jakarta Data 1.1 ways to say something else are refused instead of being run as an equality: a parameter with
`@Is` (`@Is(AtLeast.class) int min` would become `price = min`), a parameter of a `jakarta.data.constraint` type or a `Restriction`, and a method with `@First`
(also on `@Query`). The Quarkus build fails with a message that names the method; the Spring repository fails when its proxy is created. Use a derived query
(`findByPriceGreaterThan`) or `@Query` instead. The check works on the names, so it needs no 1.1 API; support for these features is MOR-020.

## Repositories without a primary entity, and lifecycle methods

A repository does not have to extend `BasicRepository` or `CrudRepository`. A `@Repository` interface that extends nothing and declares `@Insert`, `@Save`, `@Update`, `@Delete` and query methods works too:

```java
@Repository
public interface Library {
    @Insert Book add(Book book);
    @Insert Book[] addAll(Book... books);
    @Update boolean change(List<Book> books);
    @Delete void remove(Book book);
    long countByAuthor(String author);
    @Query("DELETE FROM Review WHERE bookId = ?1") long removeReviews(String bookId);
}
```

- **Lifecycle methods** (`@Insert`, `@Save`, `@Update`, and a `@Delete` whose parameter is an entity) have any name. The parameter is one entity, a collection, or an array or varargs. The result is nothing, the entity (for one entity), a list or an array of entities, or for `@Update` and `@Delete` a `boolean`, `int` or `long` (the number of entities given, as `Morphium.delete()` does not tell whether a document was there). A result the method cannot give fails the build (Quarkus) or the creation of the repository (Spring).
- **The primary entity** of such a repository is the entity of its lifecycle methods, if they all use the same one. With several entities there is none.
- **Every method works on one entity:** a lifecycle method on the entity of its parameter, a JDQL statement on the entity it names (`DELETE FROM Review`, `UPDATE Book SET ...`), a query or find method on the entity it returns; otherwise on the primary entity. A method that names no entity, in a repository without a primary entity, fails the build or the creation of the repository. A `DELETE` or `UPDATE` that names an entity that is none of the repository fails too; the name after `FROM` of a `SELECT` is still ignored if it is unknown.

## `@Update` and optimistic locking

`@Update` replaces an entity that exists. If no entity with that id exists, or its `@Version` is not the one of the entity you pass,
the call fails with `jakarta.data.exceptions.OptimisticLockingFailureException` and nothing is written.

- With a `@Version` field the check is atomic: the id and the version are matched by the store itself, there is no extra lookup.
- Without a version only the id can be matched. Morphium looks the id up first (`findById`), which is not atomic: an entity that is deleted
  between the lookup and the store is inserted again. Give the entity a `@Version` where that matters.
- `updateAll` rejects the whole list when an entity without a version is missing. A stale version is found by the store, so the entities
  stored before it stay updated.

## Atomic updates and find-and-modify: `@AtomicUpdate` (Morphium extension, not portable)

The Jakarta Data specification has no upsert and no find-and-modify. `@AtomicUpdate` is a Morphium-specific annotation for exactly that, with structured
assignments instead of a JSON string. Code that uses it does not run on another Jakarta Data provider.

```java
@Repository
public interface Leases extends BasicRepository<Lease, String> {

    @AtomicUpdate(where = "id = :id AND (expiresAt < :now OR owner = :owner)",
                  set = {@Assign(field = "owner", param = "owner"), @Assign(field = "expiresAt", param = "until")},
                  setOnInsert = @Assign(field = "createdBy", param = "owner"),
                  inc = @Assign(field = "fencingToken", value = "1"),
                  upsert = true)
    Optional<Lease> tryAcquire(@Param("id") String id, @Param("owner") String owner, @Param("now") Date now, @Param("until") Date until);
}
```

That is one atomic conditional upsert: it acquires a free lease, renews your own, takes over an expired one, and returns an empty `Optional` if somebody else holds it
(the insert of the upsert collides with the document that the condition excluded; that is not an exception here).

The return type chooses the form:

| return type | command | result |
|---|---|---|
| `void`, `boolean`, `int`, `long` | `update` | the number of documents matched or inserted by the upsert (`boolean`: at least one); a lost conditional upsert is 0 / `false` |
| the entity, `Optional<Entity>` | `findAndModify` | the document as `returns` says (`AFTER`, the default, or `BEFORE`); null / empty if nothing matched, the upsert was lost, or `BEFORE` was asked for an upsert that inserted |
| anything else, collections and streams included | | fails the build |

`where` is a JDQL condition over named parameters (with or without `WHERE`); `set`, `setOnInsert` and `inc` are lists of `@Assign`, each with a `field` of the entity
and either the `param` that holds the value or a literal `value` (a number, a string in single quotes, `TRUE`, `FALSE`). `many = true` updates every match in the count
form; an `@OrderBy` on the method chooses the document in the find-and-modify form. The values go through the update builder (`Query.modify()`), so field names are
translated, values are marshalled, `@Version` is incremented and `@LastChange` is set.

The build fails (Quarkus), or the repository cannot be created (Spring), for an unknown field, a parameter that the method does not have, a condition that is not
valid JDQL (the operands must be parameters or literals), no assignment, `setOnInsert` without `upsert`, an increment by something that is not a number, `many` on the
find-and-modify form, an `@OrderBy` on the count form, a `Sort`, `Order`, `PageRequest` or `Limit` parameter, and every return type not in the table.

The same operand check now applies to the conditions of `DELETE` and `UPDATE` statements in `@Query`.

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

## Conformance (Jakarta Data TCK)

The standalone entity tests of the Jakarta Data TCK 1.0.0 (`jakarta.data:jakarta.data-tck`) run against the repositories of the Spring module, without Arquillian, as
`JakartaDataTckTest` in `spring-boot-morphium`. 69 of the 73 assertions pass, on the InMemoryDriver and on a real MongoDB 8. The test fails when a test that is not excluded fails and when an
excluded test passes, so its list of exclusions (each with its reason) is always the truth:

| Excluded | Reason |
|---|---|
| `testOrderByHasPrecedenceOverSorts`, `testOrderByHasPrecedenceOverPageRequestSorts`, `testBasicRepository`, `testLimitedRange` | they sort an enum by its ordinal; Morphium stores an enum by its name, so it sorts alphabetically |

```bash
cd spring-boot-morphium && mvn -nsu test -Dtest=JakartaDataTckTest
# against a real MongoDB
cd spring-boot-morphium && mvn -nsu test -Dtest=JakartaDataTckTest -Dmorphium.driver-name=PooledDriver -Dmorphium.hosts=localhost:27017 -Dmorphium.database=tck
```

`-Dtck.trace=<part of a test name>` prints the stack of a failing test. The CDI tests, the signature test and the web tests of the TCK need a Jakarta EE container and are not run.

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
