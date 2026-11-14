# Migrating from Morphium 6 to 7

Morphium 7.0 keeps the storage format of 6.x and changes behaviour in the **write path**: a rejected write is no longer silent, update documents are mapped
like `store()` maps, a few things that used to run wrongly are refused. Most applications compile unchanged and need a pass over the places listed here.
Every item names what changed, how to find the code that is affected, and what to do. The complete list with the reasons is the `CHANGELOG.md` ("7.0.0").

Come from 6.2 or older? Do [6.2 to 6.3](migration-v6_2-to-v6_3.md) first.

## At a glance

| # | Change | You are affected if | Do |
|---|---|---|---|
| 1 | A rejected write raises | a unique index, `$pull` on a non-array or a path below a scalar was ignored before | catch `MorphiumDriverException`, or `query.setRaiseOnWriteErrors(false)` |
| 2 | `WriterSettings.raiseOnWriteErrors` does not exist | you used it in a snapshot build of the 7.0 work | drop it |
| 3 | A map without operators in `findOneAndUpdate(Map)` is a `$set` | you passed a whole document to replace | name the fields to keep, or `store()` |
| 4 | `Query.set/push/pushAll/pull` refuse an entity for `@Reference`, plaintext for `@Encrypted` | you pushed entities into reference fields | write the id, or `store()` |
| 5 | Update values are mapped like `store()` does | you built `Date.from(...)` or maps by hand | pass the value, remove the workaround |
| 6 | JDQL: `AND` binds stronger than `OR`, `@Is`/`Restriction`/`@First` are refused | `@Query` with `OR`, Jakarta Data 1.1 types | check the queries, use `@Query` |
| 7 | `@Version(fieldName)` is stored under that name | an entity has `@Version(fieldName = ...)` | migrate the field |
| 8 | `short`, `char` are stored as numbers | an entity has such a field | nothing to do, queries work now |
| 9 | InMemoryDriver is as strict as mongod | unit tests run on it | fix the test data or the update |
| 10 | Quarkus 3.40.1 for the extension | you build or run the Quarkus extension | align your versions |

## 1. A rejected write raises

The server reports a statement that it rejects (duplicate key, `$pull` on something that is no array, a path below a scalar) in the **reply** of the
`update` command and does not fail. Morphium used to ignore that reply, so `query.set(...)` that violated a unique index returned as if it had written.
In 7 these throw a `MorphiumDriverException`: `Query.set/unset/inc/dec/push/pushAll/pull/pullAll`, `Morphium.addToSet/currentDate/inc/dec`, the entity variants,
the versioned `store()` paths, `store()` of an existing entity, `Query.remove()` and the bulk context.

- `getMongoCode()` is the code of the first error, `isDuplicateKey()` tells the common case, `getWriteErrors()` has all of them.
- A write that was applied but missed the write concern throws `WriteConcernNotSatisfiedException`. A bulk context throws one `BulkWriteException`
  (`getPartialResult()`, `getFailures()`). Documents that an ordered multi update changed before the error are **not rolled back**.
- A unique index violation on `store()` of a **versioned** entity used to be a `VersionMismatchException`. It is the `MorphiumDriverException` now:

```java
try {
    morphium.store(account);
} catch (VersionMismatchException e) {          // somebody else changed it: reload and retry
    ...
} catch (MorphiumDriverException e) {
    if (e.isDuplicateKey()) { ... }              // the unique index, 6.x reported this as a version mismatch
}
```

**Find it:** `catch (VersionMismatchException` around entities with a unique index; code that "tries" a write and ignores the result (a racing upsert).
**Best effort on purpose:** `query.setRaiseOnWriteErrors(false)` for one query. There is no global switch.

## 2. `WriterSettings.raiseOnWriteErrors` does not exist

A global switch that restored the silent behaviour existed in snapshot builds of the 7.0 work and was removed before the release. A released 6.x has no such property, so only
users of a snapshot are affected: the property `writerSettings.raiseOnWriteErrors` is not read. Use the per-query override of item 1.

## 3. A map without operators in `findOneAndUpdate(Map)` is a `$set`

```java
query.findOneAndUpdate(Map.of("status", "done"));   // 6.x: replaced the document (every other field gone); 7: $set status
```

A map with an operator at the top level is unchanged, `findOneAndUpdateEnums(Map)` follows the same rule. **Find it:** `findOneAndUpdate(` with a map whose keys do not start with `$`.
If you relied on the replacement, name the fields you want to keep, or `store()` the entity.

## 4. Entities for `@Reference`, plaintext for `@Encrypted`

`Query.set/push/pushAll/pull` throw an `IllegalArgumentException` (before anything is written) for an entity in a `@Reference` field, which would have been embedded as a full
document, and for a plaintext value in an `@Encrypted` field, which cannot be read back. `findOneAndUpdate` and the builder refused both already. Pass the **id** of the referenced entity
(or a `MorphiumReference`); an encrypted field is written by `store()`.

## 5. Update values are mapped like `store()` maps

`Query.set`, `findOneAndUpdate(Map)`, `Query.modify()`, the buffered writer and the bulk context map embedded objects, entities, enums, `java.time`, collections the way `store()` does,
and **translate the keys** to the stored names (`expiresAt` becomes `expires_at`). An `Instant` is a BSON date; embedded values carry `class_name`.

- Remove workarounds such as `Date.from(instant)` and hand-written snake_case keys in updates that go through these entry points.
- **Raw driver commands stay raw** (`FindAndModifyMongoCommand`, `UpdateMongoCommand`): no key translation, no value mapping. Prefer `query.modify()`.
- A stray field next to the real one (a key that was written as given, `expiresAt` next to `expires_at`) was a bug of 6.x; existing documents keep it, a migration can merge it.

## 6. Jakarta Data: queries that ran wrongly are refused

- `AND` binds stronger than `OR` in JDQL, keywords inside string literals are text, `IN` takes lists of literals. A query that was written for the old precedence changes its meaning.
- `@Is`, a `jakarta.data.constraint` parameter, a `Restriction` and `@First` on `@Find`, `@Delete`, `@Query` fail the Quarkus build (naming the method) and the creation of the Spring repository:
  they ran as an equality before. Use a derived query or `@Query`.
- `@Update` raises `OptimisticLockingFailureException` for a missing entity and a stale version.

## 7. `@Version(fieldName = ...)`

The version is stored under the name of `fieldName` now (6.x wrote it under the Java name while filters used `fieldName`). Documents written before keep the Java name: they read back with a version of 0
and are not found by a filter on the stored name. Rename the field in those documents, or remove the `fieldName`.

## 8. `short` and `char` fields

A `short`/`Short` field was written as a Base64 Java-serialization blob and is a number now (old documents are still read); `char`/`Character` is read back. Nothing to do, but a field that you
queried by hand as a blob has to be queried as a number.

## 9. InMemoryDriver

The InMemoryDriver rejects what mongod rejects: unknown operators, mixed operators and fields, `$inc` on a non-number, paths below a scalar, a conflicting pair of paths, a unique index on an array field is
unique **per element**, an upsert whose insert collides with an existing document raises E11000 instead of replacing it, `$inc/$mul/$min/$max` follow the number rules of mongod. A unit test that passed on a wrong update fails earlier. Fix the update, not the test.

## 10. Versions

The Quarkus extension is built and tested against Quarkus 3.40.1 (an application on 3.32.3 still works). With the Quarkus 3.40 BOM the `scram-client` pin of `docs/quarkus-extension.md` is no longer needed.
The Jakarta Data API stays at 1.0; 1.1 features are refused at build time (item 6).

## What does not change

- The storage format: enums by **name** (ordinal only as opt-in: `@Enumerated(ORDINAL)` or `enum-type`), `_id`, `class_name`, the names of the stored fields.
- `store()`, queries, the cache, messaging and the drivers behave as in 6.3 apart from the points above.

## New in 7 (opt in)

`Query.modify()` (atomic update builder), `LeaseManager` and the [conditional writes](conditional-writes.md) pattern, `@AtomicUpdate` and `updateBy...` in Jakarta Data repositories, `@Query` with `UPDATE` and `DELETE`,
`@Embedded` records and `SingleValueMapper`, `KeyRename` ([migrating key names](embedded-key-migration.md)), named Quarkus instances, `MorphiumMessaging` as a CDI bean, the optional showcase module. Read
[update pitfalls](update-pitfalls.md) before you touch update code.

## Checklist

1. Search for `catch (VersionMismatchException` and for writes whose failure was ignored (items 1, 2).
2. Search for `findOneAndUpdate(` with a map without `$` keys (item 3), for entities pushed into `@Reference` fields (item 4).
3. Delete `Date.from(...)` and hand-translated keys in update calls (item 5).
4. Check `@Query` strings with `OR` and any `@Is`/`Restriction` (item 6); `@Version(fieldName)` (item 7).
5. Run the tests on the InMemoryDriver **and** on a real MongoDB; the first run will show the rejected writes that were silent before.
