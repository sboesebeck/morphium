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

The typed entry points marshal embedded objects, entities, enums, `Set` and `java.time` values (MOR-002, MOR-042). Still open:
replacement documents, `findOneAndUpdateEnums` and `Expr` values (MOR-038).

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
`@Embedded(translateCamelCase = false)` on the embedded class. An opt-in inheritance mode is MOR-015.

Tests: `EmbeddedCamelCaseReproductionTest`, `EmbeddedNamingGuardTest`.

## 4. `$setOnInsert` without the version: the later `store()` fails

An upsert that inserts a document of an `@Version` entity through a raw `$setOnInsert` writes no version. Loaded again, the
entity has version `null` (or `0`), so `store()` takes it for a new document, tries an insert and fails with a duplicate key.

The builder increments the `@Version` field and starts an insert at 1 unless the update touches it, so the trap does not exist
there. On the raw map path put the version into `$setOnInsert` yourself. Open: a `@Version(fieldName = ...)` whose stored name
differs from the Java name (MOR-044).

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
- `Query.set` and `findOneAndUpdate(Map)` do not touch `@Version`, and they set `@LastChange` only when the class carries
  `@LastChange` on the type as well as on the field (MOR-071). A field-only `@LastChange` is enough for `store()` and the builder.

Use the builder where `@Version` or `@LastChange` matter.

Test: `UpdateEntryPointsLifecycleTest`.

## Checklist

- Prefer `store()` for whole entities, the builder for partial updates.
- Never put a POJO into a raw command; run the tests of a raw path against a real MongoDB too.
- Set `strictEmbeddedNaming` in the tests of an application with `translateCamelCase=false` entities.
- Treat a duplicate key of a conditional upsert as an outcome (`isDuplicateKey()`, `UpdateOutcome.DUPLICATE_KEY`), not as a failure.
