# Plan: MOR-010 typed outcome of a findAndModify upsert (short)

## Problem

`findOneAndUpdate` with upsert returns a document or `null`; the caller cannot tell the cases apart:

- `BEFORE` + upsert that inserted returns `null` (an insert has no pre-image), the same as "nothing matched" without upsert;
- a lost conditional upsert (the filter did not match an existing document, the insert collides on the unique key, typically `_id`) is an exception with code 11000 that the caller has to classify.

MongoDB reports the first distinction in `lastErrorObject` (`updatedExisting`, `upserted`), which `FindAndModifyMongoCommand.execute()` drops (it returns only `value`), and the InMemoryDriver does not produce it at all.

## Design

- `FindAndModifyMongoCommand`: new `executeReply()` returns the full reply (after the `writeErrors` handling), `execute()` keeps its contract and returns `value`. No new field (the command is serialised by reflection). The `(int) n` cast is replaced by a `Number` read.
- InMemoryDriver: `findAndOneAndUpdate` is split into an internal variant returning value, `updatedExisting` and the upserted id; the public overloads keep their signature. The command handler adds `lastErrorObject {updatedExisting, n, upserted?}` to the reply (also for `remove`: `{n}`), like mongod.
- `Query`: the body of `findOneAndUpdateMapped` becomes `findAndModifyReply(...)` (raw reply, cache handling unchanged); `findOneAndUpdateMapped` deserialises `value` as before; new package-private `findOneAndUpdateOutcome(...)` builds the typed result.
- Result type `UpdateOutcome<T>` (final class, incubating): `getKind()` of the enum `UpdateOutcome.Kind {INSERTED, UPDATED, NOT_MATCHED, DUPLICATE_KEY}`, `getDocument()` (per the `ReturnDocument` table, may be `null` for INSERTED + BEFORE), `getUpsertedId()` (converted to the `@Id` type), helpers `wasApplied()` (INSERTED or UPDATED) and `isDuplicateKey()`.
- Builder terminal `tryFindOneAndUpdate(ReturnDocument)`: same validation, mapping and injection as `findOneAndUpdate`. It turns exactly one failure into a value: a `MorphiumDriverException` with `isDuplicateKey()`, and only when `upsert()` was requested (without an upsert a duplicate key cannot be the lost insert of this call, it is rethrown). Everything else is thrown as before. `findOneAndUpdate` is unchanged.
- `DUPLICATE_KEY` means "the insert of the upsert collided with an existing document"; for a conditional upsert on a natural key that is the "held by somebody else" case. The typed lease primitive on top of it is MOR-011.

## Tests (first)

- `UpdateOutcomeTest` (pure): constructors, helpers, equals/toString.
- `FindAndModifyLastErrorObjectTest` (both drivers, on the command level): the reply carries `lastErrorObject.updatedExisting` / `upserted` for update, upsert-insert, no match; `execute()` still returns the value.
- `ModifyOutcomeTest` (both drivers): INSERTED with BEFORE (document `null`, upserted id set) and AFTER, UPDATED with both, NOT_MATCHED without upsert, DUPLICATE_KEY for a lost conditional upsert without any code inspection, the document untouched after it, a duplicate key without `upsert()` is rethrown, other errors are thrown, a race of N contenders has exactly one INSERTED and N-1 DUPLICATE_KEY, `@Version`/timestamps unaffected, the validation of the builder applies, `findOneAndUpdate` unchanged.

Not in this task: the lease primitive (MOR-011), `min/max/mul/rename` (MOR-007), the options object (MOR-008).
