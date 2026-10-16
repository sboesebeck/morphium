# Plan: MOR-050 count terminals of the update builder (short)

Scope (decided in plan-mor-006 F1): `UpdateBuilder.updateOne()`, `updateMany()`, the result type `UpdateResult`, a package-private `Query.updateMapped`, the `writeErrors` normalisation. Everything else of the builder (mapping, merging, conflicts, version and timestamp injection, rejections) is reused through `UpdateBuilder.build()`.

## Design

- `UpdateResult` is a final class (not a record, so components can be added later): `getMatchedCount()`, `getModifiedCount()`, `getUpsertedId()` (converted to the type of the entity's `@Id`), `isAcknowledged()` (false if the reply has no `n`, as with `w:0`). Incubating like the builder.
- `Query.updateMapped(mapped, upsert, multiple)` is a sibling of `findOneAndUpdateMapped`: same connection handling, class write concern, collation, `arrayFilters`, `overrideDB`; it also adds the generated `$setOnInsert._id` for upserts (`addGeneratedIdOnInsertIfNeeded`), clears the cache and counts `WRITES`. It uses `UpdateMongoCommand` (the only command with `multi`).
- The update command REPORTS errors in the reply (`writeErrors`, `writeConcernError`) instead of throwing, and a real MongoDB and the InMemoryDriver differ (MOR-030 note in the CHANGELOG: the InMemoryDriver throws). `updateMapped` normalises: a `writeErrors` entry becomes a `MorphiumDriverException` whose `mongoCode` is the code of the first error, with `getWriteErrors()` set, so `isDuplicateKey()` is true on both drivers; the message names the counts that the reply carried (documents may have been updated before the error: ordered multi updates stop at the first error and do not roll back). A `writeConcernError` (the write is applied but not acknowledged as requested) is also thrown, with the message saying the write was applied. The `n` of the reply is read as a `Number` (no `(int)` cast).
- `matchedCount = n - upserted.size()` (`n` includes upserted documents on a real MongoDB and on the InMemoryDriver). `upsertedId` comes from `upserted[0]._id`.
- Terminal rules: the builder validations of `findOneAndUpdate` apply; additionally `sort` is rejected (MongoDB supports it in an update statement only from 8.0, the InMemoryDriver ignores it: it would silently update another document), `updateOne` allows a limit of 1, `updateMany` rejects any limit.
- Version and timestamp injection apply to all selected documents (`$inc` on the version of every document that matches).

## Tests (first)

`UpdateResultTest` (pure) and `ModifyCountTest` (both drivers): counts for many/one, a no-op update (modified 0, on an entity without `@Version`), upsert insert (matched 0, upserted id equals the stored id, `@Id` String and MorphiumId), no match, duplicate key from a conditional upsert (`isDuplicateKey()` on both drivers), a unique index violation in a multi update, `arrayFilters`, cache cleared, rejected options (sort, hint, projection, limit on `updateMany`, skip), empty builder, version `$inc` on every matched document.

Not in this task: the typed outcome of a lost upsert (MOR-010), `min/max/mul/rename` (MOR-007), options object (MOR-008).
