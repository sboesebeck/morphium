# Reproduction results: raw update documents, embedded key names, `currentDate`, conditional upsert

Reproduces five hypotheses about raw update documents (observed in an application built on Morphium's Jakarta Data
layer) with small JUnit tests, against a real MongoDB and against the InMemoryDriver. No production code was changed.

## Setup of the runs

| Item | Value |
|---|---|
| Morphium | working tree of branch `chore/ledger-and-update-research`, version 6.3.7-SNAPSHOT |
| Real MongoDB | `mongo:8` container, `mongod` v8.3.11, standalone (no replica set needed), port 27099 on 127.0.0.1 |
| Real-MongoDB driver | `PooledDriver` (driver name `PooledDriver`) |
| Other driver | `InMemoryDriver` (driver name `InMemDriver`) |
| JDK | Temurin 25 (build target 21) |
| Server-side proof | `logComponentVerbosity.command = 2` on the container, "About to run the command" entries |

Notes on the term "real MongoDB": the exception of hypothesis 1 is raised by the client-side `BsonEncoder` before anything
is sent, so it applies to every wire-protocol server (also PoppyDB), not only to MongoDB.

## How to run

The tests extend `MultiDriverTestBase`, use `@MethodSource("getMorphiumInstances")` and are tagged `core`. Without an
external MongoDB configured they run against the InMemoryDriver only (this is what a default build does and what keeps it
green). With a MongoDB URI they run against `PooledDriver` and `InMemDriver`, and every test asserts the CURRENT behaviour
of the driver it receives (`if (driver instanceof InMemoryDriver) ... else ...`), so a divergence is documented, not hidden.

```bash
# InMemoryDriver only (default)
./runtests.sh --driver inmem --test PojoInSetOperatorReproductionTest

# real MongoDB + InMemoryDriver
docker run -d --name morphium-research-mongo -p 127.0.0.1:27099:27017 mongo:8
./runtests.sh --uri mongodb://127.0.0.1:27099/morphium_repro --driver all --test PojoInSetOperatorReproductionTest

# same with Maven directly (all four classes)
mvn -pl morphium-core test -Pexternal -Dmorphium.driver=pooled,inmem \
    -Dmorphium.uri=mongodb://127.0.0.1:27099/morphium_repro \
    -Dtest='PojoInSetOperatorReproductionTest,EmbeddedCamelCaseReproductionTest,CurrentDateReproductionTest,ConditionalUpsertReproductionTest'
```

Every observed outcome is also logged as a greppable line: `grep REPRO-RESULT <logfile>`.

Expected-failure mechanism: the repository has no dedicated "known defect" annotation. The tests therefore pin the
observed behaviour as assertions (they are green today). When a defect is fixed, the matching assertion fails on purpose
and has to be flipped to the positive expectation; each test class documents this in its Javadoc.

Test classes (all in `morphium-core/src/test/java/de/caluga/test/mongo/suite/repro/`):

| Class | Hypothesis | Tests (per driver) |
|---|---|---|
| `PojoInSetOperatorReproductionTest` | 1, 4 | `rawPojoShapesInSet`, `writerPathShapes`, `preSerialisedShapesWork` |
| `EmbeddedCamelCaseReproductionTest` | 2 | `embeddedDefaultUnderNonTranslatingEntity`, `embeddedOptOutUnderTranslatingEntity` |
| `CurrentDateReproductionTest` | 3 | `currentDateByFieldName`, `currentDateByEnumAndWithUpsert`, `rawCurrentDateControl`, `bulkCurrentDateControl` |
| `ConditionalUpsertReproductionTest` | 5 | `conditionalUpsertOnHeldLease`, `conditionalUpsertOnHeldLeaseViaRawCommand`, `conditionalUpsertViaQueryGeneratesConflictingId`, `conditionalUpdateWithoutUpsertReturnsNullOnNoMatch`, `upsertInsertWithReturnNewFalseReturnsNull`, `versionTrapAfterSetOnInsertWithoutVersion`, `versionTrapPrimitiveLongVersion`, `versionInitialisedInSetOnInsertAvoidsTrap` |
| `ReproSupport` | helper | outcome capture, exception chain with Mongo codes |

Result of the last run: all green, 17 tests on the InMemoryDriver only, 34 invocations with MongoDB + InMemoryDriver.

## Verdicts

| # | Hypothesis | Verdict |
|---|---|---|
| 1 | POJO in `$set` through `Query.findOneAndUpdate(Map, upsert, returnNew)` fails with "Unhandled Data type" | Confirmed (List, Set, array, single value, Map, Map in Map) |
| 2 | `@Embedded` default `translateCamelCase` is not inherited from `@Entity(translateCamelCase=false)`; nested keys become snake_case | Confirmed |
| 3 | `MorphiumBase.currentDate` produces `$set: {"$currentDate": ...}` | Confirmed; worse than assumed: the server error is swallowed, the caller sees no exception |
| 4 | InMemoryDriver accepts a POJO in `$set` where `BsonEncoder` throws | Partly: List, array, Map and Map in Map are accepted and stored as live Java objects; Set and single POJO are stored too but then fail with a `ClassCastException` |
| 5a | Conditional upsert, non-matching filter on an existing document: `MorphiumDriverException`, code 11000 | Confirmed on MongoDB (with the id pinned or via the raw command); InMemoryDriver silently overwrites instead; plus a new finding (generated `_id` conflict, code 66) |
| 5b | Upsert with `returnNew=false` on insert returns `null` | Confirmed on both drivers |
| 5c | `@Version` trap after `$setOnInsert` without version | Confirmed on both drivers (duplicate key 11000 inside `writeErrors`) |

## Hypothesis 1: POJO in `$set` via `Query.findOneAndUpdate`

Test: `PojoInSetOperatorReproductionTest#rawPojoShapesInSet`. Call:
`createQueryFor(Holder).f("key").eq(k).findOneAndUpdate({"$set": {"key": k, <field>: <value>}}, true, true)`,
`Holder`/`Item` use `@Entity(translateCamelCase=false)` / `@Embedded(translateCamelCase=false)`.

### Real MongoDB (`PooledDriver`)

Identical for all six shapes (`List<Item>`, `Set<Item>`, `Item[]`, single `Item`, `Map<String,Item>`,
`Map<String,Map<String,Item>>`), nothing is stored:

```
de.caluga.morphium.driver.MorphiumDriverException[mongoCode=null, mongoReason=null]: Error sending Request:
  <-  java.lang.RuntimeException: Unhandled Data type: de.caluga.test.mongo.suite.repro.PojoInSetOperatorReproductionTest$Item
```

The wrapper's message is exactly `Error sending Request:` (trailing colon), `getMongoCode()` is `null`, the cause is the
`RuntimeException` from `BsonEncoder`. The wrapper is a `MorphiumDriverException` (unchecked), thrown directly from
`Query.findOneAndUpdate`. The failure is client side: the same happens against any server.

Control (`preSerialisedShapesWork`): with `morphium.getMapper().serialize(item)` per element the call succeeds, raw document
`{items=[{amount=1, name=a}, {amount=2, name=b}], key=pre, _id=...}`; the mapper does not add `class_name`.

### Writer path for comparison (`writerPathShapes`)

`Query.set(Map, upsert=true, multiple=false)` goes through `MorphiumWriterImpl.marshallIfNecessary`:

| Shape | MongoDB | InMemoryDriver |
|---|---|---|
| `List<Item>` | ok, elements stored with `class_name` (`...$Item`) | ok, same |
| single `Item` | ok, with `class_name` | ok, same |
| `Map<String,Item>` | ok, values with `class_name` | ok, same |
| `Set<Item>` | same exception as above, nothing stored | accepted, raw `Item` objects stored |
| `Item[]` | same exception as above | accepted, raw `Item[]` stored |
| `Map` in `Map` | same exception as above | accepted, raw objects stored |

This confirms the finding's description of `marshallIfNecessary` (List, Map one level, single entity handled; Set, array,
nested Map not) and shows that option B would also fix `Query.set` with a Set. Note also: the writer path adds
`class_name` for non-polymorphic embeddeds, the mapper path (`serialize`) does not.

## Hypothesis 4: InMemoryDriver and POJOs in `$set`

Same test as hypothesis 1, InMemoryDriver branch. The driver does not encode BSON and keeps whatever object it gets:

| Shape | Result |
|---|---|
| `List<Item>` | accepted, raw document `{items=[Item@f81, Item@fa1], key=k-list, _id=...}`, the stored elements are `Item` instances |
| `Item[]` | accepted, stored as `Item[]` |
| `Map<String,Item>` | accepted, `{itemMap={x=Item@f81, y=Item@fa1}}` |
| `Map` in `Map` | accepted, `{itemMap={outer={x=Item@f81, y=Item@fa1}}}` |
| `Set<Item>` | call fails, but the document IS already written (`{itemSet=[Item@f81, Item@fa1], ...}`) |
| single `Item` | call fails, but the document IS already written (`{single=Item@f81, ...}`) |

Failure text for the two rejected shapes (a plain `RuntimeException` wrapping a `ClassCastException`, no
`MorphiumDriverException`, thrown while the driver builds the response):

```
java.lang.RuntimeException: java.lang.ClassCastException: class java.util.LinkedHashSet cannot be cast to class java.util.List (...)
java.lang.RuntimeException: java.lang.ClassCastException: class ...$Item cannot be cast to class java.util.Map (...)
```

So InMemory tests with a POJO in a List/Map pass while MongoDB rejects them (the defect is invisible in InMem tests), and
the two failing shapes leave a half-written document with a different exception type than MongoDB.

## Hypothesis 2: `@Embedded` default under `@Entity(translateCamelCase=false)`

Test: `EmbeddedCamelCaseReproductionTest`, raw document read with `createQueryFor(Owner.class).asMapList()`. Identical on
both drivers (key order differs only). Entity `Owner` is `@Entity(translateCamelCase=false)`; `DefaultChild` is `@Embedded`
(annotation default), `ExplicitChild` is `@Embedded(translateCamelCase=false)`.

```
{ _id, ownerName=o,
  defaultChild    = {item_version_id=v1, file_name=f.txt, inner_grand_child={deep_value=d}},
  defaultChildren = [{item_version_id=v1, file_name=f.txt, inner_grand_child={deep_value=d}, class_name=...$DefaultChild}],
  explicitChild   = {fileName=f.txt, itemVersionId=v1},
  explicitChildren= [{fileName=f.txt, itemVersionId=v1, class_name=...$ExplicitChild}] }
```

- Entity-level keys stay camelCase (`ownerName`, `defaultChild`); the embedded's keys are snake_case, also in lists and
  also for deeper levels (`inner_grand_child.deep_value`).
- An embedded with `translateCamelCase=false` keeps camelCase regardless of the owner.
- The opposite combination (`@Entity` default, embedded opt-out) stores `owner_name`, `explicit_child={fileName, itemVersionId}`,
  `default_child={item_version_id, file_name, ...}` - the embedded's own flag always wins, the owner's flag is never consulted.
- The round trip is lossless (read back via the mapper gives the original values), so the defect is silent and only visible in
  raw documents or in queries/updates which use the camelCase names.
- Side observation for a test that detects snake_case keys in stored documents ("no key contains `_` except `_id` and `class_name`"): `class_name` is
  written for embedded elements inside Lists, not for single embedded fields.

## Hypothesis 3: `MorphiumBase.currentDate`

Test: `CurrentDateReproductionTest`. `currentDate(query, "lastSeen", false, false)` calls
`set(query, {"$currentDate": {"lastSeen": 1}}, upsert, multiple)`; `getMongoFieldName` returns names containing `$`
unchanged, so the writer wraps it in `$set`.

Update document actually sent (mongod command log, `db=morphium_repro_5`):

```
{"update":"Clocked","updates":[{"q":{"name":"n"},"u":{"$set":{"$currentDate":{"lastSeen":1}}},"upsert":false,"multi":false}]}
```

Not a valid `$currentDate` (and the value is `1`, not `true`/`{$type}` either). The correct control sends
`"u":{"$currentDate":{"lastSeen":true}}`.

| Driver | Returned map | Document afterwards |
|---|---|---|
| MongoDB | `{nModified=0, writeErrors=[{index=0, code=52, errmsg=Plan executor error during update :: caused by :: The dollar ($) prefixed field '$currentDate' in '$currentDate' is not allowed in the context of an update's replacement document. Consider using an aggregation pipeline with $replaceWith.}], ok=1.0, n=0}` | unchanged (`lastSeen` still 1970), no exception thrown |
| InMemoryDriver | `{nModified=1, ok=1.0, n=1, ...}` (reports success) | unchanged, no `$currentDate` key stored |

The same holds for `currentDate(query, Enum, upsert=true, ...)` (no insert happens, same `writeErrors` code 52).
So `MorphiumBase.currentDate` is a no-op on both drivers; on MongoDB the error is hidden in the returned map (the update
path does not turn `writeErrors` into an exception), on InMemory success is even reported.

Controls that work on both drivers: raw `{"$currentDate": {"lastSeen": true}}` through `Query.findOneAndUpdate` and
`MorphiumBulkContext.addCurrentDateRequest(query, false, false, "lastSeen")` (bulk result
`{num_matched=1, num_modified=1, ...}`, `lastSeen` refreshed).

## Hypothesis 5: conditional upsert, `returnNew=false`, `@Version` trap

Test: `ConditionalUpsertReproductionTest`. Entity `Lease {@Id String id; owner; Date expiresAt}`, filter
`_id = "job" AND (expiresAt < now OR owner = me)`, update `{$set: {owner, expiresAt}}`, `upsert=true`, `returnNew=true`.
Query object produced by Morphium: `{$and=[{_id=job}, {$or=[{expiresAt={$lt=...}}, {owner=A}]}]}`.

### 5a: held lease (A holds, B tries)

1. With the id pinned in the update (`"$setOnInsert": {"_id": "job"}`, the form that avoids finding 5d), through
   `Query.findOneAndUpdate`:

   MongoDB:
   ```
   de.caluga.morphium.driver.MorphiumDriverException[mongoCode=11000, mongoReason=null]:
     Error: 11000 - Plan executor error during findAndModify :: caused by :: E11000 duplicate key error collection:
     morphium_repro_1.Lease index: _id_ dup key: { _id: "job" }
   ```
   `getMongoCode()` is an `Integer` 11000 (callers should treat the code as a `Number`), `getWriteErrors()` is `null`,
   `getMongoReason()` is `null`, the exception is thrown directly (not wrapped). The existing document is untouched.
   Renewal by the owner and takeover of an expired lease both work and keep one document.

2. Through a hand-built `FindAndModifyMongoCommand` (the path applications use today, query = `q.toQueryObject()`): same exception, same
   code and text, no pinned id needed because the command layer adds nothing.

3. InMemoryDriver diverges: no exception. The upsert silently replaces the held document, afterwards
   `[{owner=B, expiresAt=..., _id=job}]` - B "stole" the lease. An InMem unit test of a lease can therefore never see the
   "held" outcome (neither E11000 nor any other signal).

### 5d (new finding): `Query.findOneAndUpdate` adds a conflicting generated `_id`

`addGeneratedIdOnInsertIfNeeded` only looks for `_id` at the TOP level of the query object. A compound filter is `{$and: [...]}`,
so the helper adds `"$setOnInsert": {"_id": "<generated>"}`. Command seen by mongod:

```
{"findAndModify":"Lease","query":{"$and":[{"_id":"job"},{"$or":[...]}]},
 "update":{"$set":{...},"$setOnInsert":{"_id":"6ad0659c2cd6fd035e679ffa"}},"new":true,"upsert":true}
```

| Driver | Result of the naive call (no pinned id) |
|---|---|
| MongoDB | `MorphiumDriverException[mongoCode=66]: Error: 66 - Plan executor error during findAndModify :: caused by :: Performing an update on the path '_id' would modify the immutable field '_id'` - already on the FIRST acquire (plain insert), and on every later call; nothing is ever stored |
| InMemoryDriver | succeeds and stores the document under the GENERATED id (`_id=6ad0...`), the filter's `_id=job` is ignored; every further call inserts another document (`[{owner=A, _id=6ad0...}, {owner=B, _id=6ad0...}]`) |

This is why a lease through `Query.findOneAndUpdate` cannot work today unless the id is pinned in `$set`/`$setOnInsert`
(the helper's two escape hatches) or a single `_id` filter is used. It likely explains why applications fall back to the raw command.

### 5b: upsert insert with `returnNew=false`

`Query.findOneAndUpdate(update, true, false)` on a missing document returns `null` on both drivers, and the document is
created (`[{owner=A, expiresAt=..., _id=fresh}]`). A second call on the now existing document returns the OLD state
(`owner=A`). `findOneAndUpdate(update, false, true)` with a non-matching filter returns `null`, no exception, on both drivers.

### 5c: `@Version` trap

Entity `VersionedDoc {@Id String id; String payload; @Version Long entityVersion}` (and a variant with primitive `long`).

1. `Query.findOneAndUpdate({"$setOnInsert": {"payload": "p"}}, true, true)` on `_id = d1` returns the entity with
   `entityVersion == null` (primitive variant: `0`); stored raw document `{payload=p, _id=d1}`, no version field.
2. Loading it and calling `morphium.store(entity)` fails on both drivers (version null/0 means "new", so Morphium inserts):

   MongoDB:
   ```
   MorphiumDriverException[mongoCode=null, mongoReason=null, writeErrors=[{index=0, code=11000,
     errmsg=E11000 duplicate key error collection: morphium_repro_N.VersionedDoc index: _id_ dup key: { _id: "d1" },
     keyPattern={_id=1}, keyValue={_id=d1}}]]: Failed to write: 1 - succeeded: 0
   ```
   InMemoryDriver: same shape, `errmsg=E11000 duplicate key error collection: morphium_repro_N.VersionedDoc dup key: { _id: d1 }`.
   Here the duplicate key is reported in `getWriteErrors()` and `getMongoCode()` is `null` - the opposite of the findAndModify
   path (code set, `writeErrors` null). A single "is duplicate key" helper has to look at both.
3. Control: `"$setOnInsert": {"payload": "p", "entityVersion": 1}` makes the later `store()` succeed and the version becomes 2.

## Consequences for the option analysis (options A to E)

- Option B must cover Set, array, single value and nested Map in `Query.findOneAndUpdate` AND in
  `MorphiumWriterImpl.marshallIfNecessary` (the writer path has the same hole for Set/array/nested Map).
  Decide whether `class_name` is added (writer path does, mapper path does not).
- Option C (encoder hook) would also have to fix the InMemoryDriver, which currently stores raw objects; otherwise InMem
  keeps hiding the problem.
- Option E (lint) is cheap and would have caught hypothesis 2; the embedded's flag is never consulted for the owner today.
- Core gaps confirmed as real tasks: `currentDate` is a no-op on both drivers (and silent), `findOneAndUpdate` generated-id
  logic breaks compound filters, the InMemoryDriver replaces instead of raising 11000 on a non-matching upsert, and the
  `writeErrors` of the update path are not turned into exceptions.

## What could not be verified

- Replica sets, authentication, and the `SingleMongoConnectDriver` were not exercised (single standalone `mongod`, `PooledDriver` only).
- PoppyDB was not run; hypothesis 1 is client-side so it should behave like MongoDB, but PoppyDB's own handling of
  `$currentDate`, E11000 on findAndModify and `writeErrors` is untested.
- Only MongoDB 8 (v8.3.11) was used; older server versions word the code-52 and code-66 messages differently and MongoDB
  versions before 5.0 treat `$`-prefixed field names in `$set` differently.
- Whether a real-world lease filter has the `_id` at top level (which would avoid finding 5d) was not checked;
   the 11000 reproduction therefore used a pinned id and the raw command.
- Hypothesis 1 was reproduced with `@Embedded` POJOs only; `@Entity` instances and `@Reference` fields in the same positions were not tried.
