# Jakarta Data 1.0 -- Gap Analysis & Improvement Roadmap

> **quarkus-morphium** Jakarta Data provider
> Last updated: 2026-03-15

---

## Current State

The Jakarta Data 1.0 integration lives entirely in `quarkus-morphium` (not morphium-core).
Repository implementations are generated at **build time** via Quarkus Gizmo bytecode
generation -- no runtime reflection, GraalVM native-image compatible.

### What works today

| Area | Status | Details |
|------|--------|---------|
| Repository interfaces | Full | `DataRepository`, `BasicRepository`, `CrudRepository`, `MorphiumRepository` |
| CRUD annotations | Full | `@Insert`, `@Update`, `@Save`, `@Delete`, `@Find` |
| Query derivation | Good | 15+ operators: Equals, Not, GT/GTE/LT/LTE, Between, In, NotIn, Like, StartsWith, EndsWith, Null, NotNull, True, False |
| JDQL (`@Query`) | Good | WHERE, ORDER BY, BETWEEN, IN, LIKE, IS NULL, named params, SELECT projection, aggregate functions (COUNT/SUM/AVG/MIN/MAX), GROUP BY (single + multi-field), HAVING. |
| Return types | Good | `List<T>`, `Stream<T>`, `Optional<T>`, `Page<T>`, `CompletionStage<X>`, `long`, `boolean`, `void`, single `T` |
| Sorting | Full | `Sort<T>`, `Order<T>`, `@OrderBy`, JDQL ORDER BY |
| Pagination | Good | `PageRequest`, `Limit`, `MorphiumPage<T>`. No keyset/cursor pagination |
| StaticMetamodel | Full | Auto-generated `Entity_` classes for type-safe field refs |
| Morphium transparency | Full | `@Version`, `@Cache`, `@Reference`, lifecycle callbacks all work through repos |

### Key files

| File | Purpose |
|------|---------|
| `runtime/.../data/AbstractMorphiumRepository.java` | Base class for generated repo impls |
| `runtime/.../data/MorphiumRepository.java` | Morphium-specific extension interface |
| `runtime/.../data/MethodNameParser.java` | Query derivation from method names |
| `runtime/.../data/QueryDescriptor.java` | Parsed query representation |
| `runtime/.../data/QueryExecutor.java` | Query execution orchestration |
| `runtime/.../data/FindMethodBridge.java` | `@Find`/`@By`/`@OrderBy` execution |
| `runtime/.../data/JdqlParser.java` | JDQL query string parsing |
| `runtime/.../data/JdqlMethodBridge.java` | `@Query` JDQL execution |
| `runtime/.../data/MorphiumPage.java` | `Page<T>` implementation |
| `runtime/.../data/SortMapper.java` | Jakarta Data Sort -> MongoDB sort |
| `deployment/.../MorphiumDataProcessor.java` | Build-time bytecode generation (~900 LOC) |

---

## Gaps & Roadmap

### Quick Wins (< 1 day each)

#### #1 Jakarta Data Standard Exceptions
- **Status:** DONE
- **Gap:** No Jakarta Data exceptions thrown. Generic exceptions or null returned instead.
- **Required:** `EmptyResultException`, `NonUniqueResultException`, `EmptyResultException`
- **Impact:** Spec compliance, better error diagnostics for developers
- **Effort:** 2-3 hours
- **Details:** See [Detailed Plan](#1-detailed-plan-jakarta-data-standard-exceptions) below

#### #2 Missing Query Derivation Operators
- **Status:** DONE
- **Gap:** `Contains`, `Empty`, `Size`, `Matches`/`Regex`, `IgnoreCase` not supported
- **Required by spec:** Contains (collection membership), Empty (collection/string), pattern matching
- **Impact:** Users must fall back to `@Query` JDQL for these common patterns
- **Effort:** 3-4 hours
- **Files:** `MethodNameParser.java`, `QueryExecutor.java`

#### #3 `deleteAll()` no-arg + `deleteBy*` Query Derivation
- **Status:** DONE
- **Gap:** `deleteAll()` (no-arg, delete entire collection) not implemented. `deleteBy*` method name prefix not tested/verified in query derivation.
- **Impact:** Standard CrudRepository method missing
- **Effort:** 2 hours
- **Files:** `AbstractMorphiumRepository.java`, `MorphiumDataProcessor.java`, `QueryMethodBridge.java`

#### #4 Test Coverage Extension
- **Status:** DONE
- **Gap:** Untested operators: StartsWith, EndsWith, Like (with wildcards), In, NotIn, Null/IsNull, OR combinator, deleteBy derivation, multiple OrderBy fields, exception scenarios
- **Impact:** Quality assurance, regression safety
- **Effort:** 3-4 hours
- **Files:** `integration-tests/src/test/java/.../MorphiumData*Test.java`

### Medium Effort (1-2 days each)

#### #5 `CursoredPage<T>` (Keyset Pagination)
- **Status:** DONE
- **Gap:** Only offset-based pagination (`Page<T>` + `PageRequest`). No keyset/cursor pagination.
- **Why it matters:** Offset pagination degrades on large collections (`skip(100000)` is slow in MongoDB). Keyset pagination uses indexed field values for O(1) page jumps.
- **Effort:** 1-2 days
- **Files:** New `MorphiumCursoredPage.java`, changes to `FindMethodBridge.java`, `MorphiumDataProcessor.java`

#### #6 Stream Support in Repositories
- **Status:** DONE
- **Gap:** `Stream<T>` return type works but delegates to `asList().stream()` (eager loading). Should use `Query.stream()` (lazy cursor-backed) for large result sets.
- **Impact:** Memory-efficient processing of large collections via repos
- **Effort:** 0.5 days
- **Files:** `AbstractMorphiumRepository.java`, `FindMethodBridge.java`

#### #7 JDQL `SELECT` with Projection
- **Status:** DONE
- **Gap:** JDQL always returns full entities. `SELECT name, price FROM Product WHERE ...` not supported.
- **Impact:** Network/memory savings for queries that only need a few fields
- **Effort:** 1 day
- **Files:** `JdqlQuery.java`, `JdqlParser.java`, `JdqlMethodBridge.java`

### Larger Effort (3+ days)

#### #8 JDQL Aggregate Functions
- **Status:** DONE (v3 — global aggregation + single/multi-field GROUP BY + HAVING)
- **Gap:** `COUNT()`, `SUM()`, `AVG()`, `MIN()`, `MAX()` not supported in JDQL
- **Impact:** Analytics queries require dropping down to Morphium Aggregation API
- **Effort:** 2-3 days
- **Files:** `JdqlQuery.java`, `JdqlParser.java`, `JdqlMethodBridge.java`, `MorphiumDataProcessor.java`
- **v1 supports:** `SELECT COUNT(this)`, `SELECT SUM(field)`, `SELECT AVG(field)`, `SELECT MIN(field)`, `SELECT MAX(field)` with WHERE clauses. Return types: `long` for COUNT, `double` for SUM/AVG/MIN/MAX.
- **v2 adds:** Single-field GROUP BY with Java Record return type mapping. `SELECT status, COUNT(this), SUM(amount) GROUP BY status` returns `List<StatusStats>`. ORDER BY with GROUP BY (field + aggregate references). WHERE + GROUP BY.
- **v3 adds:** Multi-field GROUP BY (`GROUP BY status, customerId`) with compound `_id` → `$project` promotion. HAVING with comparison operators, named params, numeric literals, and AND/OR-combined conditions. HAVING filters are emitted as separate `$match` stages (AND) or a single `$match` with `$or` array (OR) after `$group`.
- **v4 adds:** `COUNT(field)` NULL filtering via `$addFields` + `$cond`/`$ne` before `$group`. `Page<Record>` pagination for GROUP BY queries (Java-level, avoids InMemAggregator `$skip` bug). HAVING OR combinator.
- **Remaining limitations:**
  - No `COUNT(DISTINCT ...)` or expressions inside aggregates

##### #8 v1 Known Gaps (GAP-A1 through GAP-A8)

These are **deliberate scope decisions** for the v1 implementation, not bugs.
Each gap documents: what's missing, why, the required effort, and workarounds.

###### GAP-A1: GROUP BY (single + multi-field) — DONE

**Implemented in v2 (single-field) and v3 (multi-field).** `SELECT status, COUNT(this) GROUP BY status`
and `SELECT status, customerId, COUNT(this) GROUP BY status, customerId` both work with Java Record
return types. Record component order must match SELECT clause order (group fields first, then aggregates).
Multi-field GROUP BY uses compound `_id` maps with a `$project` stage to promote sub-fields to top level.

---

###### GAP-A2: HAVING — DONE

**Implemented in v3 (AND), extended in v4 (OR).** `SELECT status, COUNT(this) GROUP BY status HAVING COUNT(this) > 5` works.
Supports comparison operators (`>`, `>=`, `<`, `<=`, `=`, `!=`), named parameters (`:param`),
numeric literals, and both AND and OR combinators.

- **AND** (default): conditions are emitted as separate `$match` stages after `$group` (one per condition)
  to work around an InMemoryDriver limitation where multi-field `$match` documents short-circuit
  on the first matching field (fix submitted as morphium PR #151).
- **OR**: conditions are emitted as a single `$match` stage with a `$or` array.
  Example: `HAVING COUNT(this) > 5 OR SUM(amount) >= 1000`.

---

###### GAP-A3: COUNT(field) NULL Filtering — DONE

**Implemented in v4.** `SELECT COUNT(customerId) WHERE status = 'OPEN'` now correctly counts only
documents where `customerId IS NOT NULL` (standard SQL COUNT semantics).

**Implementation:** An `$addFields` stage is inserted before `$group` that creates a helper field
(`_cnt_notnull_N`) using `$cond`/`$ne` to produce 1 for non-null values and 0 for null.
The `$group` accumulator then sums this helper field instead of a constant 1.
This avoids modifying `Group.sum()` and works with the InMemory driver (which evaluates
`$cond` via `Expr.evaluate()`).

---

###### GAP-A4: Mixed SELECT (Aggregate + Field Projections) — DONE

**Implemented in v2.** `SELECT status, SUM(amount) GROUP BY status` works when all plain
fields appear in GROUP BY. Without GROUP BY, mixing still throws `IllegalArgumentException`.

---

###### GAP-A5: Record Return Types for GROUP BY — DONE

**Implemented in v2.** `List<RecordType>` return types are detected at build time via
Jandex (`superName == java.lang.Record`). Record canonical constructor is invoked via
reflection at runtime. Record component order must match SELECT clause order.

---

###### GAP-A6: No DISTINCT or Expressions Inside Aggregates

**What's missing:**
- `SELECT COUNT(DISTINCT status) WHERE ...` — DISTINCT within aggregates
- `SELECT SUM(amount * quantity) WHERE ...` — arithmetic expressions within aggregates

**Why not in v1:**
- DISTINCT requires `$addToSet` + `$size` in the pipeline — complex mapping
- Expressions require `$multiply`/`$add` etc. inside `$group` accumulators
- Parser would need to handle arithmetic expressions within function parentheses

**Effort:** 2+ days

**MongoDB pipeline for COUNT DISTINCT:**
```json
[
  { "$group": { "_id": null, "distinctStatuses": { "$addToSet": "$status" } } },
  { "$project": { "count": { "$size": "$distinctStatuses" } } }
]
```

---

###### GAP-A7: ORDER BY with GROUP BY — DONE

**Implemented in v2.** ORDER BY in GROUP BY queries adds a `$sort` stage after `$group`.
Supports sorting by group fields (mapped to `_id`) and aggregate function references
(e.g. `ORDER BY COUNT(this) DESC`). ORDER BY is still ignored for global aggregation
(single result).

---

###### GAP-A8: Pagination for GROUP BY Aggregates — DONE

**Implemented in v4.** `Page<Record>` return type with `PageRequest` parameter now works
for GROUP BY queries. Example: `Page<StatusCount> countGroupByStatusPaged(PageRequest pageRequest)`.

**Implementation:** Pagination is applied in Java after the full aggregation completes
(slice the mapped result list) rather than via `$skip`/`$limit` pipeline stages.
This is a deliberate workaround for an InMemAggregator `$skip` bug
(line 1316: `data.subList(idx, data.size() - idx)` — incorrect, should be `data.subList(idx, data.size())`).

**Build-time:** `Page<Record>` return types are now detected by `MorphiumDataProcessor` via Jandex,
extending the existing `List<Record>` detection to also cover parameterized `Page<T>` types.

**Note:** `CursoredPage<Record>` is not yet supported for GROUP BY queries.

---

#### #9 `CompletionStage<T>` (Async Repositories)
- **Status:** DONE
- **Gap:** No async/reactive return types. All repository methods are synchronous.
- **Impact:** Non-blocking repository methods for reactive Quarkus applications
- **Effort:** 1 day
- **Files:** `MorphiumDataProcessor.java`, `AbstractMorphiumRepository.java`, `QueryMethodBridge.java`, `FindMethodBridge.java`, `JdqlMethodBridge.java`
- **What works:** `CompletionStage<List<T>>`, `CompletionStage<Optional<T>>`, `CompletionStage<Long>` (aggregates) for query derivation (`findBy*Async`), `@Find` annotated methods, and `@Query` JDQL methods.
- **Convention:** Query derivation methods use `*Async` suffix (e.g. `findByStatusAsync`). The "Async" suffix is stripped before method name parsing.
- **Implementation:** Async execution via `CompletableFuture.supplyAsync()` on Morphium's virtual-thread-backed `asyncOperationsThreadPool`.
- **v1 limitations:**
  - No `Uni<T>` / SmallRye Mutiny support (would need Mutiny dependency)
  - No async CRUD methods (standard `findById`, `save`, etc.) — only custom query/find/jdql methods
  - No `CompletionStage<Stream<T>>` (Stream is inherently pull-based, conflicts with async push model)

---

## Not Planned

| Feature | Reason |
|---------|--------|
| Jakarta NoSQL support | Spec too immature (v1.0, 1 impl, not in EE 11). Morphium's annotation model is richer. See analysis in session 2026-03-14. |
| `PageableRepository` interface | Deprecated pattern in Jakarta Data 1.0; pagination via method params (`PageRequest`, `Limit`) is the recommended approach and already supported. |
| JDQL JOINs | MongoDB has no native JOINs. `$lookup` is aggregation-only and doesn't map to JDQL semantics. |
| JDQL subqueries | Same reason -- no native support in MongoDB query language. |

---

## #1 Detailed Plan: Jakarta Data Standard Exceptions

### Goal

Throw the correct Jakarta Data exceptions from repository method executions so that
application code can catch standardized exception types instead of getting `null`,
generic `IllegalStateException`, or raw Morphium exceptions.

### Jakarta Data Exception Types

From `jakarta.data.exceptions` (verified in `jakarta.data-api:1.0.0`):

| Exception | When to throw |
|-----------|--------------|
| `EmptyResultException` | A query that expects exactly one result finds none (e.g., `findByEmail(...)` returning `T` not `Optional<T>`, or `findById(K)` returning `T`) |
| `NonUniqueResultException` | A query that expects at most one result finds multiple |
| `EntityExistsException` | Insert fails because entity with same ID already exists |
| `OptimisticLockingFailureException` | `@Version` conflict on update/delete |
| `MappingException` | Entity mapping/conversion fails |
| `DataConnectionException` | Database not reachable |
| `DataException` | Base class for all Jakarta Data exceptions |

**Note:** There is no `EmptyResultException` in the spec. `EmptyResultException` covers both
query-no-result and findById-not-found scenarios.

### Current Behavior (what's wrong)

1. **`FindMethodBridge.java:136`** -- single-entity queries return `null` when not found
   - Should throw `EmptyResultException` if return type is `T` (non-Optional, non-null)
   - Should return `Optional.empty()` if return type is `Optional<T>` (already correct)

2. **`FindMethodBridge.java`** -- no check for multiple results on single-entity return
   - `query.get()` returns the first match silently even if 100 rows match
   - Should throw `NonUniqueResultException` if >1 result and return type is `T` or `Optional<T>`

3. **`AbstractMorphiumRepository.java:doFindById()`** -- returns `Optional.empty()` (correct for Optional)
   - But generated code for `T findById(K)` (non-Optional) also returns null → should throw `EmptyResultException`

4. **`JdqlMethodBridge.java`** -- same issues as FindMethodBridge for single-result queries

5. **No `MappingException` wrapping** -- deserialization errors from `ObjectMapperImpl` bubble up as raw exceptions

### Implementation Plan

#### Step 1: Add jakarta.data-api dependency check (verify version)

File: `quarkus-morphium/pom.xml`
- Verify `jakarta.data:jakarta.data-api:1.0.0` is on classpath (already present)
- Verify exception classes are available: `jakarta.data.exceptions.EmptyResultException`, `NonUniqueResultException`, `EmptyResultException`, `MappingException`

#### Step 2: Modify `FindMethodBridge.java`

**Single-entity return type (`T`, not `Optional<T>`):**

```java
// Current (line ~136):
T result = query.get();
return result;  // returns null silently

// New:
List<T> results = query.limit(2).asList();
if (results.isEmpty()) {
    throw new EmptyResultException("Query returned no result");
}
if (results.size() > 1) {
    throw new NonUniqueResultException("Query returned more than one result");
}
return results.get(0);
```

**Optional return type (`Optional<T>`):**

```java
// Current:
return Optional.ofNullable(query.get());

// New:
List<T> results = query.limit(2).asList();
if (results.size() > 1) {
    throw new NonUniqueResultException("Query returned more than one result");
}
return results.isEmpty() ? Optional.empty() : Optional.of(results.get(0));
```

**List/Stream/Page return types:** No changes needed -- multiple results are expected.

#### Step 3: Modify `JdqlMethodBridge.java`

Same pattern as FindMethodBridge for single-result JDQL queries.

#### Step 4: Modify `AbstractMorphiumRepository.java`

**`doFindById(K id)` method:**

```java
// Current:
T entity = morphium.findById(entityClass, id);
return Optional.ofNullable(entity);

// Keep as-is for Optional<T> return type.
// But generated code for T findById(K) needs unwrapping with exception:
```

**In `MorphiumDataProcessor.java` (generated findById code):**

When the declared return type is `T` (not `Optional`), the generated bytecode should:
1. Call `doFindById(id)` which returns `Optional<T>`
2. Call `.orElseThrow(() -> new EmptyResultException("No entity found for given id"))`

#### Step 5: Add tests

New test class: `MorphiumDataExceptionTest.java` in integration-tests:

| Test | Scenario | Expected |
|------|----------|----------|
| `findSingle_noResult_throwsEmptyResult` | `findByEmail("nonexistent")` returns `T` | `EmptyResultException` |
| `findSingle_multipleResults_throwsNonUnique` | `findByCategory("common")` returns `T` with 5 matches | `NonUniqueResultException` |
| `findOptional_noResult_returnsEmpty` | `findOptionalByEmail("nonexistent")` | `Optional.empty()` (no exception) |
| `findOptional_multipleResults_throwsNonUnique` | `findOptionalByCategory("common")` returns `Optional<T>` | `NonUniqueResultException` |
| `findById_notFound_throwsEmptyResult` | `findById("missing-id")` returns `T` | `EmptyResultException` |
| `findById_notFound_optional_returnsEmpty` | `findByIdOptional("missing-id")` | `Optional.empty()` |
| `jdql_noResult_throwsEmptyResult` | `@Query("WHERE email = :e")` returns `T` | `EmptyResultException` |

#### Step 6: Verify exception class availability

Check whether `jakarta.data.exceptions` package is in the `jakarta.data-api:1.0.0` JAR.
If the exception classes don't exist in 1.0.0 (they may have been added in 1.0.1 or later),
we define our own that extend `DataException`.

### Files to modify

| File | Change |
|------|--------|
| `FindMethodBridge.java` | Add uniqueness check + EmptyResultException for single-entity returns |
| `JdqlMethodBridge.java` | Same pattern for JDQL single-result queries |
| `AbstractMorphiumRepository.java` | No change (Optional return already correct) |
| `MorphiumDataProcessor.java` | Generate `orElseThrow(EmptyResultException)` for non-Optional findById |
| New: `MorphiumDataExceptionTest.java` | 7 integration tests |
| New: test repository interface with single-return methods | Test fixture |

### Acceptance Criteria

- [ ] `T findByX(...)` throws `EmptyResultException` when no result
- [ ] `T findByX(...)` throws `NonUniqueResultException` when >1 result
- [ ] `Optional<T> findByX(...)` returns `Optional.empty()` when no result (no exception)
- [ ] `Optional<T> findByX(...)` throws `NonUniqueResultException` when >1 result
- [ ] `T findById(K)` (non-Optional return) throws `EmptyResultException` when not found
- [ ] `Optional<T> findById(K)` returns `Optional.empty()` (no exception)
- [ ] `@Query` JDQL single-result methods follow the same rules
- [ ] `List<T>`, `Stream<T>`, `Page<T>` return types are unaffected
- [ ] All 7 integration tests pass
- [ ] Existing integration tests remain green
