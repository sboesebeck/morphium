# Jakarta Data 1.1: what it adds and what Morphium can realise

Research note, no production code changed. Date of research: 2026-10-15.
Every statement carries a source. Paths below `/tmp/jd-research` are the throw-away copies used for inspection.

## 0. Source key and versions

| Key | Source |
|---|---|
| [JAR-1.0] | `https://repo1.maven.org/maven2/jakarta/data/jakarta.data-api/1.0.0/jakarta.data-api-1.0.0(-sources).jar` (inspected with `unzip`, `javap`, reading the `-sources.jar`) |
| [JAR-M4] | `https://repo1.maven.org/maven2/jakarta/data/jakarta.data-api/1.1.0-M4/jakarta.data-api-1.1.0-M4(-sources).jar` |
| [SPEC-main] | `https://github.com/jakartaee/data`, branch `main` at commit `a05cbc71` (2026-10-14), `spec/src/main/asciidoc/*.asciidoc` |
| [SPEC-1.0] | same repo, tag `1.0.0`, `spec/src/main/asciidoc/query-language.asciidoc`, `spec/src/antlr/JDQL.g4`, `tck/` |
| [JCQL] | `https://github.com/jakartaee/query`, `spec/src/antlr/JCQL.g4` (commit of 2026-10-08, tags `1.0-M1`, `1.0-M2` only) |
| [MVN-META] | `https://repo1.maven.org/maven2/jakarta/data/jakarta.data-api/maven-metadata.xml` and the directory listing of the same path |
| [SITE] | `https://jakarta.ee/specifications/data/1.1/` (page title "Jakarta Data 1.1 (Under Development)") |

Versions (from [MVN-META], directory listing dates):

- Repo today: `jakarta.data.version` = **1.0.0** (`pom.xml:112`), used by `morphium-jakarta-data` (`morphium-jakarta-data/pom.xml:21-22`).
- Newest release on Maven Central: **1.0.2** (2026-08-21; 1.0.1 is 2024-09-19, 1.0.0 is 2024-05-08).
- Newest 1.1 artifact: **1.1.0-M4** (2026-08-27; M3 2026-04-07, M2 2026-02-06, M1 2025-10-08). There is **no 1.1.0 final** yet; `<release>` and `<latest>` in the metadata are both `1.1.0-M4`; [SITE] still says "Under Development".
- `quarkus-morphium/README.md:124-128` still says "latest is 1.1.0-M3"; that is outdated (M4 exists).
- Implementers (from the `quarkus-bom` and Hibernate `gradle/libs.versions.toml` files):
  - Quarkus 3.39.5 (latest 3.x on Central) manages `jakarta.data-api` **1.0.1** with Hibernate ORM 7.4.9.Final (`https://repo1.maven.org/maven2/io/quarkus/quarkus-bom/3.39.5/quarkus-bom-3.39.5.pom`).
  - Quarkus 4.0.0.Beta1 (2026-09-30) manages **1.1.0-M3** with Hibernate ORM 8.0.0.Beta3 (`.../quarkus-bom/4.0.0.Beta1/quarkus-bom-4.0.0.Beta1.pom`).
  - Hibernate ORM: branch 7.4 builds against 1.0.1, branch 8.0 against 1.1.0-M3, `main` (8.1.0-SNAPSHOT) against 1.1.0-M4 (`https://github.com/hibernate/hibernate-orm/blob/<branch>/gradle/libs.versions.toml`, key `data`).
  - Whether Hibernate 8.x actually implements `@Is`, `Restriction`, `@First`: **not verified** (only the compile-time API version was read).

## 1. JDQL `UPDATE ... SET ... WHERE` and `DELETE FROM`

**Answer: both are part of 1.0.** They are not new in 1.1.

- Grammar of 1.0: `update_statement : 'UPDATE' entity_name set_clause where_clause?`, `delete_statement : 'DELETE' from_clause where_clause?`, `set_clause : 'SET' update_item (',' update_item)*`, `update_item : state_field_path_expression '=' (scalar_expression | 'NULL')` [SPEC-1.0 `spec/src/antlr/JDQL.g4`]. The prose says the `update` statement has an implied `from` ("`update Entity`") [SPEC-1.0 query-language.asciidoc, "Update statements"/NOTE].
- 1.0.0 Javadoc of `@Query`: for `update` or `delete` statements the method return type must be `void`, `int` or `long` [JAR-1.0 `jakarta/data/repository/Query.java`].
- In 1.1 the language was donated to the Jakarta Query specification and renamed JCQL ("remains fully backward compatible with JDQL") [SPEC-main query-language.asciidoc, NOTE at the top]. JCQL grammar: `delete_clause : 'DELETE' 'FROM' entity_name`, `update_item : simple_path_expression '=' new_value`, adds `ID(THIS)` and multi-item `SELECT` [JCQL `JCQL.g4`]. Update/delete grammar is otherwise the same as JDQL (diff of the two `.g4` files).

Semantics:

- **Return value:** the number of matching entities (`int` or `long`), or `void`. The count "might not be precise" on eventually consistent stores; providers may then throw `UnsupportedOperationException` [JAR-M4 `Query.java` Javadoc, "For update or delete statements"]. The 1.0 spec says an update returns "the number of updated records", a delete "the number of deleted records" [SPEC-1.0 query-language.asciidoc "Update statements"/"Delete statements"]. The 1.1 Javadoc wording is "matching", which in MongoDB terms is `n` (matched), not `nModified`. The TCK 1.0.0 even declares `boolean move(...)` for an UPDATE (`tck/.../standalone/entity/MultipleEntityRepo.java` at tag `1.0.0`; this contradicts the Javadoc and is kept as an observation only).
- **Expressions in SET:** `scalar_expression`: parentheses, unary `+`/`-`, binary `* /`, `+ -`, `||` (string concatenation); primary expressions: functions `ABS`, `LENGTH`, `LOWER`, `UPPER`, `LEFT`, `RIGHT`, special `LOCAL DATE|TIME|DATETIME`, `TRUE`/`FALSE`, state-field paths (so the right side may reference **other fields of the same record**), enum literals, input parameters (`:name` or `?n`), literals; or the keyword `NULL` [SPEC-1.0 `JDQL.g4`; JCQL identical for these rules].
- **Operators:** there is **no** increment operator or `$inc`-style syntax. An increment is the arithmetic expression `SET length = length + ?1`. TCK 1.0.0 examples: `UPDATE Box SET length = length + ?1, width = width - ?1, height = height * ?2`, `UPDATE Coordinate SET x = :newX, y = y / :yDivisor WHERE id = :id` (`MultipleEntityRepo.java` at tag `1.0.0`, still present on `main`).
- Integer division need not be integer division in the database ("portability is maximized" if it is) [SPEC-1.0 query-language.asciidoc, NOTE after the operator list].
- `WHERE` is optional; without it all records are updated or deleted [SPEC-1.0 "Where clause"].
- NoSQL: if the store cannot do conditional updates, cannot count matches reliably, or cannot compute expressions in SET, the method must throw `UnsupportedOperationException` [SPEC-main query-language.asciidoc, "Update statements" under "Limitations for non-relational datastores"]. Document stores are not required to support arithmetic or parentheses [same file, "Arithmetic operations and parentheses"].
- Parameters: named (`:x`) or ordinal (`?1`), never mixed [JAR-M4 `Query.java`].

## 2. `@Is`, `jakarta.data.constraint.*`, `Restriction`/`Restrict`, metamodel, `@Find`, `@First`

All of these are new in 1.1 (`@since 1.1`; classes absent from [JAR-1.0], present in [JAR-M4]: diff of `unzip -l`). Other new 1.1 items seen in the jar: `@Select` (projection), `jakarta.data.event.*` (`PreInsertEvent`, `PostUpsertEvent`, ...), `jakarta.data.expression.*`, `jakarta.data.spi.expression.*`.

| Item | What it is (source: [JAR-M4] Javadoc of the class) |
|---|---|
| `jakarta.data.constraint.*` | `Constraint<V>` and subtypes `EqualTo`, `NotEqualTo`, `GreaterThan`, `AtLeast`, `LessThan`, `AtMost`, `Between`, `NotBetween`, `In`, `NotIn`, `Like`, `NotLike`, `Null`, `NotNull`; `Constraint.negate()`; static factories (`Like.prefix`, `Between.bounds`, ...). Usable as method parameter type `C<T>` on `@Find`/`@Delete` methods. |
| `@Is` (`repository/Is.java`) | Parameter annotation on `@Find`/`@Delete` methods; `value()` is a `Constraint` subtype, default `EqualTo`. Supported list in the Javadoc: `AtLeast`, `AtMost`, `EqualTo`, `GreaterThan`, `In`, `LessThan`, `Like`, `NotEqualTo`, `NotIn`, `NotLike`. Attribute name comes from `@By` or from the parameter name (`-parameters`). |
| `Restriction<T>` (`restrict/Restriction.java`) | Immutable object; special parameter type allowed on `@Find`, `@Delete`, `@Query` methods, at most one per method; combines **conjunctively** with the static restrictions [SPEC-main repository.asciidoc "Restrictions", "Special parameters"]. Has `negate()`. Implementations: `BasicRestriction` (expression + constraint), `CompositeRestriction` (type `ALL`/`ANY`, negated flag, list), `Unrestricted`, `Unmatchable`. |
| `Restrict` (`restrict/Restrict.java`) | Static factories `Restrict.all(...)`, `Restrict.any(...)` (varargs and `List`). |
| Extended static metamodel | `Attribute` subtypes `BasicAttribute`, `ComparableAttribute`, `NumericAttribute`, `TextAttribute`, `TemporalAttribute`, `BooleanAttribute`, `NavigableAttribute` now implement `jakarta.data.expression.*Expression`, which yield `Restriction`s and `Sort`s: `_Product.price.greaterThan(..)`, `.between`, `.in`, `.isNull`, `.like`, `.contains`, `.startsWith`, `.upper()`, `.length()`, `.plus/minus/times/dividedBy`, `.asc()/.desc()` (`javap` of `jakarta.data.expression.*`). Expressions only produce **restrictions and sorts**; there is no method that produces an assignment/update item (searched `expression`, `metamodel`, `restrict` packages). |
| `@Find` with constraints | `@Find` parameters may be (a) same name+type as an attribute, (b) `C<T>` with `C extends Constraint`, (c) `@Is(C.class)`, or (d) `Restriction`, `Sort`, `Order`, `Limit`, `PageRequest` (`repository/Find.java`; [SPEC-main repository.asciidoc "Parameter-based automatic query methods"]). `@Find` can also project via `@Select` into an attribute or a record. |
| `@First` (`repository/First.java`) | Method annotation, static result limit, `value()` default 1, must be > 0; allowed on `@Find` and `@Query`; not combinable with a `PageRequest`/`Limit` parameter nor with the `First` keyword in a method name. |
| `@Query` in 1.1 | Javadoc now says JCQL/JPQL and adds `Restriction` as allowed parameter type (`Query.java`). A provider need not support `Restriction`/`Constraint` parameters for JPQL or native queries [SPEC-main repository.asciidoc, "Limitations of JPQL and native SQL queries"]. |

## 3. Partial/atomic updates, upsert, find-and-modify

**The specification has none of these.** Evidence:

- `@Update` (1.1-M4) takes an entity (or `List`/array) only; it matches by id (and version if versioned), returns the updated entity or `void`, and throws `OptimisticLockingFailureException` when no row/version matches [JAR-M4 `repository/Update.java`]. No parameter-style "update attribute" annotation exists in the package `jakarta.data.repository` (listing: `BasicRepository, By, CrudRepository, DataRepository, Delete, Find, First, Insert, Is, OrderBy, Param, Query, Repository, Save, Select, Update`).
- `@Delete` has an automatic-query form (parameters / `Restriction`, return `void`/`int`/`long`) [JAR-M4 `Delete.java`]; `@Update` has no counterpart.
- `@Save` is the only "upsert": it behaves like `@Update` if an entity with that id exists, else like `@Insert`; providers may raise `PreUpsertEvent`/`PostUpsertEvent` [JAR-M4 `Save.java`]. It is entity-based, returns the entity, and is not a conditional/atomic field-level operation.
- The only partial update is the JDQL/JCQL `UPDATE ... SET` statement in `@Query` (section 1); it returns a count, never a document.
- Search of the Javadoc sources for `upsert|findAndModify|find-and-modify|atomic` hits only `Insert`, `Save`, `PreUpsertEvent`, `PostUpsertEvent`. Searching issues of `jakartaee/data` for "upsert", "partial update", "findAndModify" found only closed issue #1024 (SQL MERGE and lifecycle events) and #229 (semantics of CrudRepository operations) (`gh search issues`). No open proposal found.
- A conditional upsert, a find-and-modify with returned document, `$setOnInsert`, `$addToSet`, `$each` have no Jakarta Data representation at all. Providers can only offer them as vendor extensions (the spec explicitly allows extra "automatic query annotation" types, not portable: [SPEC-main repository.asciidoc, NOTE after "Automatic query methods annotated with @Find or @Delete are portable"]).

## 4. What Morphium can realise

### 4.1 Current state of the module (verified in the checkout)

- Built against 1.0.0 (`pom.xml:112`). `JdqlParser` is a regex parser for `[SELECT ...] [FROM] [WHERE] [GROUP BY/HAVING] [ORDER BY]` only (class Javadoc `JdqlParser.java:21-55`; `parse` at `:107`, only `SELECT ` prefix is recognised at `:119`). There is no UPDATE/DELETE handling anywhere in `JdqlParser`/`JdqlMethodBridge` (grep for `update|delete` finds only a field name in a comment, `JdqlParser.java:343`). An `UPDATE ...` string would be fed into the WHERE-condition parser.
- `MorphiumDataProcessor` rejects ordinal parameters `?1` at build time (`MorphiumDataProcessor.java:1876-1881`), although the 1.0 TCK uses them for UPDATE (`?1`, `?2`) and the spec allows them.
- For `@Query` the generator only treats a `long` return as count (`MorphiumDataProcessor.java:1747`).
- `@Find`/`@Delete`: every non-special parameter becomes an **equality** condition (`MorphiumDataProcessor.java:1245-1262` for `@Find`, `:1383-1400` for `@Delete`; runtime `FindMethodBridge.java:83` and `:314`: `query.f(mongoField).eq(args[paramIdx])`). Consequence for 1.1 sources: `@Is` is never read, so `@By("price") @Is(AtLeast.class) int min` would silently be executed as equality; a `Restriction` parameter would be treated as an attribute named like the parameter. A 1.1 upgrade needs an explicit build-time rejection or implementation.
- `@Update` is `requireExists` (one `findById`) + `Morphium.store()` (`AbstractMorphiumRepository.java:324-328`, check at `:362`). Not atomic, throws `IllegalStateException`, not the exception the 1.1 Javadoc prescribes (`OptimisticLockingFailureException`).
- `@Delete` with conditions already uses server-side `query.delete()` and returns the driver's `n` (`FindMethodBridge.java:301-331`).

### 4.2 Atomic operations in core (verified)

| Operation | Core API | Source |
|---|---|---|
| `$set` of several fields, `multiple`, `upsert` | `Query.set(Map, upsert, multiple)`; translates field names (`getMongoFieldName`), marshals values (`marshallIfNecessary`, which handles only a single entity, a `List` or a `Map`), applies `@LastChange`/`@CreationTime` | `Query.java:2028-2051`; `MorphiumWriterImpl.java:2352-2381` |
| `$inc` | `Query.inc(field, Number, ...)` and `MorphiumWriter.inc(Query, Map<String,Number>, ...)` (several fields in one command; keys are put into `$inc` **without** field-name translation) | `Query.java:2148-2240`; `MorphiumWriterImpl.java:2002-2026` |
| `$unset`, `$push`, `$pull`, `$pop` | `Query.unset`, `push`, `pull`, `pullAll`, `pushAll` | `Query.java:2054-2145` |
| `$addToSet` (one value) | `Morphium.addToSet(query, field, value, upsert, multiple)` via `pushPull(ADD_TO_SET)` | `MorphiumBase.java:1061-1073`; `MorphiumWriterImpl.java:2650` |
| `$mul`, `$min`, `$max`, `$rename`, `$currentDate` | **Only in the bulk context** (`addMulRequest`, `addMinRequest`, `addMaxRequest`, `addRenameRequest`, `addCurrentDateRequest`); not on `Query`/`Morphium` | `MorphiumBulkContext.java:250-302` |
| `Morphium.currentDate` | builds `$set: {"$currentDate": ...}`, i.e. probably wrong operator (initial hypothesis, confirmed by reading; later reproduced, see reproduction-results.md) | `MorphiumBase.java:1364-1370` |
| `$setOnInsert` | only through a raw map in `findOneAndUpdate` | see below |
| Raw combined update document | `MorphiumBulkContext.addCustomUpdateRequest(query, Map cmd, upsert, multiple)` (no name translation, no marshalling) | `MorphiumBulkContext.java:205-214` |
| findAndModify | `Query.findOneAndUpdate(Map)` (returns the old document, not upsert) and `findOneAndUpdate(Map, upsert, returnNew)` (supports `Query.sort`, adds generated `_id` via `$setOnInsert`, no name translation/marshalling/events) | `Query.java:432` and `:493-535`; `Query.findOneAndDelete()` at `:337` |
| Pipeline update | `FindAndModifyMongoCommand` has a `pipeline` field, but `Query` does not expose it; `UpdateMongoCommand` update field `u` is a `Map` only | `FindAndModifyMongoCommand.java:17,66-71`; `UpdateMongoCommand.java:59-70` |
| InMemoryDriver | implements `$set,$setOnInsert,$unset,$inc,$currentDate,$mul,$rename,$min,$max,$addToSet` | `InMemoryDriver.java:9087-9286`, `:9451`, `:9541` |
| Result of multi-update | `Map` summed from the driver result (counters such as `n`, `nModified` are summed) | `MorphiumWriterImpl.java:1974-1999`; `WriteBatchSplitter.java:79-98` |

One typed `Query` call issues exactly one operator. A statement such as `SET status = :s, version = version + 1` therefore cannot be a single atomic `updateMany` through `Query` today; it would be two commands, or a hand-built `{ "$set": .., "$inc": .. }` through `UpdateMongoCommand`/`addCustomUpdateRequest`.

### 4.3 Mapping table

Legend: **A** = realisable with existing core API, **B** = realisable only with a hand-built command (`UpdateMongoCommand`, `addCustomUpdateRequest`, `findOneAndUpdate(Map)`) and module-side name translation/marshalling, **C** = needs new Morphium core API, **N** = out of reach for MongoDB classic update operators.

| Jakarta Data feature | Mongo mapping | Class | Notes |
|---|---|---|---|
| JDQL `DELETE FROM E [WHERE ...]` returning `void`/`int`/`long` | `Query.delete()` returns map with `n` | **A** | Core exists (`FindMethodBridge.java:327-331`). Missing: parser + processor branch (only front-end work). `WHERE` can reuse the existing parser. |
| `UPDATE E SET f = :v ...` (assign param/literal) | `$set` via `Query.set(Map, false, true)` | **A** | Count = `n` of result map (matched). Only when all items are plain assignments. |
| `SET f = f + :x`, `f = f - :x` (constant/param) | `$inc` (negative for minus) | **A** for one operator kind; **C/B** if mixed with `$set` in one statement | Single atomic command needs `{$set,$inc}` together. |
| `SET f = f * :x` | `$mul` | **B** (bulk context only) | No `Query.mul`. |
| `SET f = f / :x` | `$mul` with `1/x` | **B**, with caveat | Only valid for floating-point fields; integer division semantics differ (spec allows either, see section 1). |
| `SET f = NULL` | `$set: null` or `$unset` | **A** | Pick one and document: null versus missing field. |
| `SET f = g`, `f = g + h`, `||`, `UPPER/LOWER/LEFT/RIGHT/LENGTH/ABS`, `LOCAL DATETIME` with field references | needs aggregation-pipeline update (`[{$set: {...}}]`, MongoDB 4.2+) | **C** | Neither `Query` nor `UpdateMongoCommand` can send a pipeline; `FindAndModifyMongoCommand.setPipeline` exists in the driver. `LOCAL DATETIME` alone can be a `Date` computed client-side or `$currentDate` (**B**). |
| `@Is(...)` on `@Find`/`@Delete` params | operator map `$ne,$gt,$gte,$lt,$lte,$in,$nin,$regex` | **A** | `QueryExecutor.java:226-261` already builds all of these for derived queries; only the processor (`MorphiumDataProcessor.java:1245-1262`, `:1383-1400`) and `FindMethodBridge` must read `@Is` and the `Constraint` parameter types. `Between`, `Null`, `NotNull`, `NotLike` map to existing `BETWEEN`, `IS_NULL`, `IS_NOT_NULL`, `LIKE`+`$not`. |
| `@First` | `Query.limit(n)` | **A** | `Query.java:1064`; processor must read the annotation. Rules about combination with `Limit`/`PageRequest` must be enforced. |
| `Restriction<T>` with attribute-only expressions (`_P.price.greaterThan(..)`, `Restrict.all/any`, `negate()`) | interpret the tree at runtime to a query document (`$and`/`$or`/`$nor`) | **A** | Runtime walk over `BasicRestriction`/`CompositeRestriction`; needs the 1.1 API on the classpath (version bump). |
| `Restriction` with expressions (`price.minus(discount).lessThan(..)`, `upper()`, `length()`) | `$expr` | **B/C** | `Query.expr(Expr)` exists (`Query.java:1341`) but covering the full expression tree is new work. |
| `@Select` projection in `@Find` | Mongo projection | **A** (not analysed further) | Not required for the atomic-update question. |
| `@Save` as upsert | `store()` | already present | Not atomic and not field-level (`AbstractMorphiumRepository.java:262-268`). |
| `@Update` entity-based, version check, `OptimisticLockingFailureException` | `$set` of whole doc with version filter | **C** (optional) | Current `findById` + `store()` is a separate defect class. |
| Partial update by parameter annotation, upsert, find-and-modify | none in the spec | **vendor extension** | Would be a Morphium-specific annotation (for example `@Upsert`/`@FindAndModify`) implemented on `Query.findOneAndUpdate(Map, upsert, returnNew)` (**B** today). Clean implementation needs the mapper-aware update builder (option A (mapper-aware update builder)), because `findOneAndUpdate` does no name translation and no marshalling of embedded objects (`Query.java:493-535`). |

### 4.4 Consequences (short)

1. JDQL `DELETE FROM` and the plain/`$inc`-only/`$set`-only `UPDATE` subset are cheap and independent of 1.1; they work on 1.0.0. They need parser, processor and bridge work, not new core API.
2. A fully conforming `UPDATE` (mixed `$set`/`$inc`/`$mul`, field-to-field expressions) needs a new core primitive: a typed, mapper-aware multi-operator update (and optionally a pipeline update). That is the same builder as option A (mapper-aware update builder), so one core change serves both the Jakarta Data `UPDATE` and atomic upserts in applications.
3. `@Is`, `@First`, `Restriction` need no new core API for the attribute-level subset; they need the dependency bump to 1.1.x (not before a final or at least a pinned milestone) and processor/bridge changes. Until then, the processor should fail the build for an `@Is` or `Restriction` parameter instead of silently using equality (`MorphiumDataProcessor.java:1245-1262`).
4. Upsert and find-and-modify cannot be expressed in Jakarta Data at all; they remain Morphium-specific (section 3).

## 5. Not verified

- Whether Hibernate ORM 8.x, Eclipse JNoSQL, or any provider implements `@Is`/`Restriction`/`@First` (only dependency versions were read).
- The Jakarta Data 1.1 specification as rendered on jakarta.ee (the page says "Under Development"); the text used is the `main` branch of the repository, which may still change after M4. The M4 jar and `main` differ only in Javadoc formatting for the files compared (`diff -rq` shows changed files; content-level diff not done for every file).
- Exact keys (`n` versus `nModified`) in the map returned by `Query.set`/`Query.inc` at runtime (read from `sumUp` and `WriteBatchSplitter`, not executed).
- `Morphium.currentDate` producing the wrong operator (read, not run).
- InMemoryDriver behaviour for POJOs inside `$set` (not looked at).
- Whether TCK 1.1 contains update tests beyond the `UPDATE ... SET` statements listed above (grep of `tck/` on `main` for `@Query("UPDATE`/`@Is`/`@First` only).
- Jakarta EE 12 / Jakarta Data 1.1 final release date (no source found).
