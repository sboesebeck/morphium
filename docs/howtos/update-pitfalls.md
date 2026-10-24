# Updates: Pitfalls of Raw Maps, Embedded Objects and Conditional Upserts

Updates that go through a query (`Query.set`, `Query.findOneAndUpdate(Map)`, `Query.modify()`) skip the part of `store()` that
knows the entity. This page lists what that costs, how to recognise it and what to use instead. Each item points to the test that
reproduces it; the tests run against the InMemoryDriver and, with `-Pexternal`, against a real MongoDB.

Which call does what:

| Call | Keys | Values | Lifecycle |
|---|---|---|---|
| `morphium.store(entity)` | stored names | mapper | `@PreStore`, `@PostStore`, `@Version`, `@LastChange`, `@CreationTime` |
| `query.modify()...` (builder) | Java names are translated | mapper | no callbacks; `@Version`, `@LastChange`, `@CreationTime` are maintained |
| `query.findOneAndUpdate(Map, ...)` | operator keys are translated | mapper | no callbacks; no `@Version` bump; `@LastChange` only if the class carries it too (see 7) |
| `query.set(...)`, `push(...)`, ... | Java names are translated | mapper | as `findOneAndUpdate(Map)` |
| `FindAndModifyMongoCommand`, `UpdateMongoCommand`, ... | **as given** | **as given** | none |

## 1. Raw commands stay raw

A driver command sends the map you give it. It translates no key (`ownerName` is not turned into `owner_name`) and it
maps no value: an `Instant` in a filter or an update document is not a BSON date unless you wrap it with `Date.from(...)`,
and a `$lte` against a stored date silently matches nothing.
This is the contract of the command layer (see also #402 for `java.time`); the typed entry points above do the mapping.

Use `query.modify()` or `query.findOneAndUpdate(Map)` and let the mapper marshal. If a raw command is unavoidable, translate the keys
and wrap the dates yourself, and test against a real MongoDB.

## 2. POJOs in a raw `$set`

A POJO (or a `List`, `Set`, array or `Map` of POJOs) in the update document of a raw command cannot be encoded by the wire
driver and fails with `Unhandled Data type`. The InMemoryDriver used to accept it and store the live Java object, so a unit test
passed that a real MongoDB then rejected. Since MOR-029 the InMemoryDriver refuses what the wire encoder refuses, before
anything is written.

The typed entry points marshal embedded objects, entities, enums, `Set` and `java.time` values (MOR-002, MOR-042). A map without
operators passed to `findOneAndUpdate(Map)` or `findOneAndUpdateEnums(Map)` is a `$set` of the fields it names, not a replacement
document (MOR-013, 7.0), so its values are marshalled like those of `$set`. An `Expr` used as a value is replaced by what it
describes and the values inside it are marshalled, too (MOR-038).

Tests: `PojoInSetOperatorReproductionTest`, `UnencodableValuesTest`.

## 3. `@Embedded` does not inherit `translateCamelCase=false`

```java
@Entity(translateCamelCase = false)
class Owner { String ownerName; Child child; }

@Embedded                       // the annotation default is translateCamelCase = true
class Child { String fileName; }
```

`Owner` stores `ownerName`, but the embedded `Child` stores `file_name`. The round trip through the mapper is lossless, so the
mismatch is invisible until a raw query or a second application reads the document.

The mapper logs a warning once per entity class that names owner, field and embedded class. With
`ObjectMappingSettings.strictEmbeddedNaming` it throws an `IllegalStateException` at the first use instead (MOR-014). The fix is
`@Embedded(translateCamelCase = false)` on the embedded class. An opt-in inheritance mode is MOR-015. Documents that were stored with the old names are migrated with `KeyRename` (MOR-016), see
[Migrating the key names inside embedded documents](embedded-key-migration.md).

Tests: `EmbeddedCamelCaseReproductionTest`, `EmbeddedNamingGuardTest`, `KeyRenameTest`.

## 4. `$setOnInsert` without the version: the later `store()` fails

An upsert that inserts a document of an `@Version` entity through a raw `$setOnInsert` writes no version. Loaded again, the
entity has version `null` (or `0`), so `store()` takes it for a new document, tries an insert and fails with a duplicate key.

The builder increments the `@Version` field and starts an insert at 1 unless the update touches it, so the trap does not exist
there. On the raw map path put the version into `$setOnInsert` yourself. A `@Version(fieldName = ...)` is stored under that name by
`store()`, the filters and the builder (MOR-044).

Test: `ConditionalUpsertReproductionTest` (5c).

## 5. A conditional upsert that loses raises a duplicate key

The filter `_id = job AND (expiresAt < now OR owner = me)` with `upsert` is the usual lease. If somebody else holds the lease the
filter does not match, the upsert tries to insert and collides with the existing `_id`. On a real MongoDB this is error 11000.

- A `findAndModify` reports it as a top-level command error: `getMongoCode()` is 11000 and `getWriteErrors()` is `null`.
- A bulk or `update` command reports it in `writeErrors`, and `getMongoCode()` is `null`.

`MorphiumDriverException.isDuplicateKey()` looks at both. The builder offers the outcome as a value, and `LeaseManager` packages the
whole recipe:

```java
UpdateOutcome<Lease> o = taken.modify().set("owner", me).set("expiresAt", until).upsert().tryFindOneAndUpdate(ReturnDocument.AFTER);
// INSERTED, UPDATED, NOT_MATCHED or DUPLICATE_KEY (held by somebody else)

LeaseResult<Lease> r = LeaseManager.of(morphium, Lease.class, "owner", "expiresAt").tryAcquire("relay", me, Duration.ofSeconds(30));
```

The InMemoryDriver answers like MongoDB here (it used to replace the held document silently). A duplicate key aborts a real
MongoDB transaction, so do not run this inside one.

Tests: `ConditionalUpsertReproductionTest` (5a), `FindOneAndUpdateCompoundFilterUpsertTest`, `LeaseManagerTest`.

## 6. An upsert that inserts returns `null` for `BEFORE`

There is no document before the insert. `ReturnDocument.BEFORE` therefore returns `null` for an upsert that inserts, and so does a
filter that matches nothing. `ReturnDocument.AFTER` returns the document; `tryFindOneAndUpdate` tells the two `null`s apart
(`INSERTED` and `NOT_MATCHED`).

Test: `ConditionalUpsertReproductionTest` (5b).

## 7. Query-based updates: no entity callbacks, and not every annotation is maintained

`store()` calls `@PreStore` and `@PostStore` on the entity. A query-based update has no entity instance and calls neither. If a
`@PreStore` method derives a field (a hash, a normalised name), an update that changes its inputs must set the field too.

The storage-listener events are another thing: `preUpdate` and `postUpdate` of a `MorphiumStorageListener` fire for every query-based update, the
builder and `findOneAndUpdate(Map)` included, once per call.

The annotations that describe the document differ by entry point:

- The builder (`query.modify()`) increments `@Version` and sets `@LastChange` and `@CreationTime` (on an insert), also when the
  annotation sits on the field only.
- The writer methods (`Query.set`, `inc`, `unset`, `push`, `pull`, ... and their `Morphium` variants) set `@LastChange` too, also when the
  annotation sits on the field only (7.0, MOR-071); a value that the call sets for that field itself is kept. They do **not** touch
  `@Version`: that is the contract of the writer path. A `store()` of an entity that was read before such an update is not refused as stale,
  because the version did not move.
- `findOneAndUpdate(Map)` maintains neither: the map is the document that the caller wrote.

Use the builder where `@Version` matters, and for `findOneAndUpdate`-style updates where `@LastChange` does.

Test: `UpdateEntryPointsLifecycleTest`.

## 8. An entity for a `@Reference`, a plaintext value for an `@Encrypted` field

An update does not know what the declared type of a field means for the storage. An entity in a `@Reference` field would be embedded
as a full document instead of a reference, and a plaintext value in an `@Encrypted` field bypasses the encryption and cannot be read
back. Every entry point refuses both with an `IllegalArgumentException` before anything is written: `findOneAndUpdate(Map)`, the builder
and, since 7.0, the writer path (`Query.set`, `push`, `pushAll`, `pull` and the buffered writer). Pass the id of the referenced entity (or a
`MorphiumReference`), and `store()` the entity to write an encrypted field (MOR-049).

Test: `WriterPathGuardTest`.

## 9. `$pull` conditions and elements of mixed shapes

**The keys of a `$pull` condition are not translated.** `pull("items", Map.of("fileName", "x"))` removes the elements whose field is
called `fileName` in the stored document. The keys of the condition name fields of the *embedded documents*, which the update does not
know the type of, so they are sent as given (the keys of every other operator are translated). Write the stored name (`file_name` for an
`@Embedded` class with `translateCamelCase=true`).

**Elements are written like `store()` writes the members of a container.** `push`, `addToSet`, `$each` members and the element of a `pull`
are marshalled alike: an embedded value or an entity is a document with `class_name`, a custom-mapped scalar is wrapped
(`{"value": scalar}`). A document in an array that `store()` or a current update wrote is therefore equal to a later `addToSet` of the same
value, and `pull` of that value removes it. MongoDB compares whole documents, field by field, so the equality is lost when the array holds
another shape of the same element: a document written by a raw command, by another application or by an update path before 7.0 (a custom-mapped
scalar was written flat then; `pull` removes both forms). `addToSet` adds a second copy and `pull` by element leaves the old one.

To remove an element whatever shape it has, pull by a condition on its fields (`pull("items", Map.of("name", "x"))`). To keep an array free of
duplicates when the shapes may differ, make the update conditional on the key (`f("items.name").ne("x").push("items", item)`) instead of relying on
`addToSet`.

Tests: `UpdateDocumentMapperKeyTest` (the keys of a `$pull` condition), `UpdateValueMarshallingTest` (the shape of the elements).

## Checklist

- Prefer `store()` for whole entities, the builder for partial updates.
- Never put a POJO into a raw command; run the tests of a raw path against a real MongoDB too.
- Set `strictEmbeddedNaming` in the tests of an application with `translateCamelCase=false` entities.
- Treat a duplicate key of a conditional upsert as an outcome (`isDuplicateKey()`, `UpdateOutcome.DUPLICATE_KEY`), not as a failure.
