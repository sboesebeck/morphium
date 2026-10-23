# Implementation plan: MOR-006 mapper-aware update builder

Status: DRAFT by the architect (read-only, nothing was run), to be reviewed by the challenger. Paths: `MC` = `morphium-core/src/main/java/de/caluga/morphium`,
`MT` = `morphium-core/src/test/java/de/caluga/test/mongo/suite`, `Q` = `MC/query/Query.java`, `UDM` = `MC/query/UpdateDocumentMapper.java`, `W` = `MC/writer/MorphiumWriterImpl.java`.
Base: branch `fix/mor-030-inmem-upsert`.

## 0. Findings that shape the design

1. An empty builder would wipe the document: `UDM.map` returns any document without a `$` key unchanged (`UDM:54-56`) and `findAndModify` treats that as a replacement document. A builder with no operations must be rejected at the terminal.
2. There is no mapper-aware count path. `UpdateMongoCommand.execute()` is `WriteMongoCommand.execute()` (`WriteMongoCommand.java:65-80`) and returns `writeErrors` in the result instead of throwing; `FindAndModifyMongoCommand.execute()` throws (`:159-171`) and returns only `value` (`:173-174`), the `lastErrorObject` is dropped, so "inserted vs updated" cannot be derived (matters for MOR-010).
   MOR-030/CHANGELOG record that a real MongoDB reports `writeErrors` 11000 on the update-command path while the InMemoryDriver throws. The count terminals need a new package-private `Query.updateMapped` that normalises `writeErrors` into a thrown `MorphiumDriverException`.
3. `findAndModify` cannot carry everything a `Query` can: `FindAndModifyMongoCommand` has fields for `fields`, `hint`, `let`, `collation`, `sort` but no `arrayFilters` (`:13-24`); `findOneAndUpdateMapped` sets only sort, writeConcern, collation (`Q:573-583`); the InMemoryDriver's `runCommand(FindAndModifyMongoCommand)` passes only query, update, sort, collation, upsert, new (`InMemoryDriver.java:4117-4131`). `Query.getArrayFilters()` (`Q:153-171`) is honoured only on the `UpdateMongoCommand` path (`UpdateMongoCommand.java:59-71`; InMemoryDriver `:3408-3429`).
4. `n` includes upserted documents (`InMemoryDriver.java:3452-3458`); an insert with `newFlag=false` returns `null` (Javadoc `:11838-11850`).
5. `findOneAndUpdateMapped` already does most of the work: never uses the read cache (`Q:550-556`), adds the generated `$setOnInsert._id` unless the filter or update pins the id (`Q:561-563`, `Q:635-670`), rethrows `MorphiumDriverException` (`Q:589-595`), clears the cache (`Q:604-606`), deserialises and fires postLoad (`Q:614-619`). The one-argument `findOneAndUpdate(Map)` swallows driver errors (`Q:485-486`): the builder must not use it.
6. The writer's `set` is not a basis: `$set`-only, strict `getMongoFieldName`, fires pre/post events (`W:2377`, `W:2456`), applies `handleLastChange` and `handleCreationTimeOnUpsert` (`W:2413-2414`, the latter does a count query and copies filter equality fields into `$set`, `W:2278-2335`), uses `getDbName()` whereas `Query` honours `overrideDB` via `getDB()` (`Q:175-181`).
7. Two Javadoc passages are stale: `Q:518-519` and `MorphiumObjectMapper.java:66-67` say a single embedded value carries no `class_name`; `UDM.mapValue` (`UDM:122-126`), `marshallUpdateElement` (`MorphiumObjectMapper.java:76-87`) and the CHANGELOG say it does. Fix in the docs work package.
8. Transactions need no builder code: the transaction context is attached to every command in `SingleMongoConnection.java:582-588`.

## 1. API design

### 1.1 Two shapes compared

| | A: builder bound to a `Query` (`query.update()...`) | B: standalone `Update` object (Spring/driver style) |
|---|---|---|
| Fits existing API | yes: `Query` owns filter, sort, collation, arrayFilters, db, collection and has `set/inc/push` | needs new overloads on `Query` (`findOneAndUpdate(Update, ...)`, `updateMany(Update)`), growing a 2,900-line class |
| Reuse | one builder per call; fine for leases and counters | one definition can be applied to many queries or kept as a constant |
| Type link | `UpdateBuilder<T>` returns `T` | untyped, bound late; works only because field names are Strings |
| Pure testability | needs the split below | pure by construction |
| Jakarta Data consumers (MOR-018/021/022) | generated code calls `q.update()...` | same plus an extra object |
| Risk | mutable `Query` aliasing | a forgotten binding |

Recommendation: A with a pure core: `UpdateOperations` (package-private, pure: operators, merging, conflict checks, no Morphium dependency) + `UpdateBuilder<T>` (public: `Query` snapshot + `UpdateOperations` + terminals). Shape B later is additive (promote `UpdateOperations`, add `Query.update(UpdateOperations)`).

### 1.2 Surface (package `de.caluga.morphium.query`)

```java
public UpdateBuilder<T> Query.update()                // snapshot of the query (clone(), Q:1976)

public final class UpdateBuilder<T> {
  UpdateBuilder<T> set(String field, Object value);        // + Enum<?> overload (name())
  UpdateBuilder<T> setOnInsert(String field, Object value);// + Enum<?>
  UpdateBuilder<T> inc(String field, Number amount);       // + Enum<?>; null amount rejected
  UpdateBuilder<T> unset(String... fields);                // + Enum<?>...; zero names rejected
  UpdateBuilder<T> upsert();  UpdateBuilder<T> upsert(boolean upsert);

  T            findOneAndUpdate(ReturnDocument which);     // typed entity, no default
  UpdateResult updateOne();                                // UpdateMongoCommand, multi=false
  UpdateResult updateMany();                               // UpdateMongoCommand, multi=true
  Map<String,Object> toUpdateDocument();                   // mapped document, diagnostics/tests (COULD)
}
public enum ReturnDocument { BEFORE, AFTER }               // reused by MOR-008
public record UpdateResult(long matchedCount, long modifiedCount, Object upsertedId) {}
```

- Field arguments are Java names, stored names or dotted paths (`child.fileName`, `childList.$[x].fileName`); Enum overloads call `name()` in one private helper; MOR-012 later adds metamodel overloads delegating to the same String core.
- Mutable, not thread-safe, sequentially reusable: terminals build a fresh document each call and do not consume the builder. Immutable copy-on-write was rejected because `b.set(..);` with the result ignored would silently do nothing. The `Query` is cloned in `update()`. The multi-thread tests use one builder per thread.
- Query part: the filter is what `Query.f(...)/or/and` built. Honoured: filter, sort (for `findOneAndUpdate`; for `updateOne` only on MongoDB 8.0 or later, unverified), collation, arrayFilters (count terminals only), `overrideDB`, collection name, class write concern. Rejected: `skip != 0`, `limit > 1`, `limit == 1` on `updateMany`; arrayFilters on `findOneAndUpdate` throw `UnsupportedOperationException` until MOR-008. Ignored (listed in the Javadoc): projection, hint, read preference, `translateUpdateFieldNames` override.

### 1.3 Usage

```java
// lease: conditional upsert, "held" == duplicate key (typed outcome arrives with MOR-010)
Query<Lease> q = morphium.createQueryFor(Lease.class).f(Lease.Fields.id).eq(leaseId);
q.or(morphium.createQueryFor(Lease.class).f(Lease.Fields.expiresAt).lt(now),
     morphium.createQueryFor(Lease.class).f(Lease.Fields.ownerName).eq(me));
try {
    Lease l = q.update().set(Lease.Fields.ownerName, me).set(Lease.Fields.expiresAt, now.plus(ttl))
               .setOnInsert(Lease.Fields.createdBy, me).upsert()
               .findOneAndUpdate(ReturnDocument.AFTER);
    return acquired(l);
} catch (MorphiumDriverException e) {
    if (e.getMongoCode() instanceof Number n && n.intValue() == 11000) return held();
    throw e;
}
// counter
long hits = morphium.createQueryFor(Counter.class).f("id").eq(key).update().inc("hits", 1).upsert().findOneAndUpdate(ReturnDocument.AFTER).hits;
// create-if-absent
Cfg c = q.update().setOnInsert("payload", p).upsert().findOneAndUpdate(ReturnDocument.AFTER);
// bulk state change, count form
UpdateResult r = jobs.f("state").eq(PENDING).update().set("state", RUNNING).unset("error").updateMany();
```

## 2. Semantics

| Topic | Decision |
|---|---|
| Same-operator merge | `UpdateOperations` keeps `operator -> LinkedHashMap<key, value>`. Two `set` calls give one `$set`; operator order is insertion order |
| Same field twice in one operator | equal values (`Objects.deepEquals`) merge; differing values throw `IllegalArgumentException` at call time (consistent with decision D4 of plan-mor-041). `ownerName` and `owner_name` collide only after translation; `UDM:83-90` throws for differing values. Leaves room for MOR-007 to turn repeated `push`/`addToSet` into `$each` |
| Conflicts across operators (`set`+`inc`, `set`+`unset`, `set`+`setOnInsert` on one field, `a` with `a.b`) | pre-checked in `UpdateOperations.rejectConflicts` on the translated keys, segment-based prefix matching; pairs containing a `$` segment skipped and left to the server. Deterministic on both drivers, fails before a connection is acquired. Deviates from D4 (which left cross-operator conflicts to the server because the InMemoryDriver may accept them), Q6 |
| Null values | `set/setOnInsert(k, null)` stores BSON null, distinct from `unset`. `inc(k, null)`, null/empty field name, `unset()` with no names throw `IllegalArgumentException` |
| Empty builder | `IllegalStateException` at every terminal (finding 1) |
| Unknown fields | lenient by default (matches MOR-006 text, Spring, `findOneAndUpdate`); strict rejection would break polymorphic subclass fields, `@AdditionalData` and map paths. Opt-in `strict()` possible later (Q4) |
| Key translation | always `resolveFieldPath` through `new UpdateDocumentMapper(morphium, type, true)` (D1); the switch and `Query.setTranslateUpdateFieldNames` are ignored by the builder; stray field cleanup via the raw Map overload plus the override |
| Values | `marshallUpdateElement` for every value (`UDM:122-126`): entity keeps `_id` and carries `class_name`, `MorphiumId` -> `ObjectId`, enums -> `name()` |
| Guards | `@Reference` entity and `@Encrypted` write rejection come with `UDM`, unchanged |
| `@Version` (this task) | NOT injected (MOR-009). To avoid silent corruption the terminal throws `IllegalStateException` if the entity has a `@Version` field and the mapped update does not touch it (`$set/$setOnInsert/$inc/$unset`): a builder write without a bump lets a later `store()` with a stale version overwrite it. Compared via `resolveStoredFieldName` (MOR-044 caveat). MOR-009 replaces the guard with injection, so the change only relaxes behaviour. Q2 |
| `@LastChange`, `@CreationTime` | not applied; Javadoc says so, WARN logged once per entity class. Q3 |
| Events | no pre/post update events; `postLoad` and `updateLastAccess` fire on the returned entity as `Q:616-619` does. Contract in the Javadoc for MOR-027/MOR-009 |
| Cache | `clearCacheIfNecessary(type)` after the write (`Q:604-606`); count path does the same plus `morphium.inc(StatisticKeys.WRITES)` |
| Write concern | `getWriteConcernForClass(type)` on both paths; an unacknowledged write (`w:0`) yields zero counts (unverified) |
| Transactions | automatic (finding 8); write concern inside a transaction not verified |
| Index/capped checks | `checkIndexAndCaps` is private in the writer and `findOneAndUpdateMapped` skips it too; the builder does the same (R6) |
| Upsert id seeding | the count path reuses `addGeneratedIdOnInsertIfNeeded` (`Q:635-670`) |
| `$setOnInsert` without upsert | allowed (server no-op); documented |

Null-result trap: `ReturnDocument` is a mandatory argument with no default; the terminal Javadoc carries the table below; a pinning test shows the same result on both drivers; the typed "inserted/updated/held" outcome is deferred to MOR-010 (needs `lastErrorObject` in `FindAndModifyMongoCommand.execute` and in the InMemoryDriver).

| upsert | which | document matches | result |
|---|---|---|---|
| no | any | no | `null` |
| yes | BEFORE | no (insert) | `null` |
| yes | AFTER | no (insert) | inserted document |
| any | BEFORE | yes | old document |
| any | AFTER | yes | new document |
| yes, filter does not match an existing `_id` | any | no, but the key exists | `MorphiumDriverException`, code 11000 |

## 3. Implementation design

- New files in `MC/query/`: `UpdateBuilder` (public), `UpdateOperations` (package-private, pure), `UpdateResult` (public record; Java 21 per `pom.xml:73-77`), `ReturnDocument` (public enum).
- Edits to `Query`: `public UpdateBuilder<T> update()`; package-private `Map<String,Object> updateMapped(Map<String,Object> mapped, boolean upsert, boolean multiple)` as a sibling of `findOneAndUpdateMapped` (same try/finally connection handling, `UpdateMongoCommand().setDb(getDB()).setColl(getCollectionName())`, class write concern, `addUpdate(q, u, null, upsert, multiple, collation, arrayFilters, null, multiple ? null : sort)`); after `execute()`, if `writeErrors` is present throw `MorphiumDriverException` (message format of `FindAndModifyMongoCommand:159-171`, `setWriteErrors`, `mongoCode` from the first error) so `mongoCode 11000` is observable on both paths and both drivers. No refactor of `findOneAndUpdateMapped`.
- Delegation: `findOneAndUpdate(rd)`: validate, map with `UDM`, version guard and conflict check, then `query.findOneAndUpdateMapped(mapped, upsert, rd == AFTER)`. Count terminals: same pipeline, then `updateMapped`, then `UpdateResult`; `matchedCount = n - upsertedCount` because `n` includes upserts; `upsertedId` from `upserted[0]._id` (InMemoryDriver shape `:3436-3470`; Pooled shape not verified).
- Shared with the writer: only the mapper call through `UDM`. Deliberately not shared: strict `getMongoFieldName`, `$set`-only structure, `WriterTask`/async/callbacks (sync only), events, `handleLastChange`, `handleCreationTimeOnUpsert`, `simplifyQueryObject`.
- Thread safety: `UDM` stateless; ARH caches concurrent (`storedNamesCache`, `ARH:1074`); builder single-thread.
- Extension seams (nothing implemented now): MOR-007 new methods on `UpdateOperations` calling one generic `put(operator, key, value)`, `$each` values are plain Maps the walk already marshals; MOR-008 `findOneAndUpdate(FindAndModifyOptions)` overload plus `Query.findOneAndUpdateMapped(Map, FindAndModifyOptions)`, the private builder method is `run(boolean upsert, ReturnDocument)`, the `arrayFilters` field in `FindAndModifyMongoCommand` and the InMemoryDriver wiring come with MOR-008 and the arrayFilters guard is removed there; MOR-009 the guard step becomes `applyAutoValues(mapped, type, upsert)` on the translated document; MOR-010 `MorphiumDriverException` passed through unwrapped with `mongoCode` on both paths, a `tryFindOneAndUpdate` can be added later; MOR-012 overloads delegating to the String core; MOR-018/021/022 public API only (`x = x * n` needs MOR-007; `updateMany().matchedCount()` is the Jakarta Data count).

## 4. Work packages (tests first)

Commits: C1 = WP1+WP2 (green); C2 = WP3-WP5 (red tests plus the implementation that turns them green in one commit, bisect-safe with `--merge`); C3 = WP6. M = MECHANICAL, J = JUDGEMENT.

WP0 (M, reviewer-run): JDK 21 (Byte Buddy fails on 25); `docker run -d --rm --name morphium-mor006 --tmpfs /data/db -p 127.0.0.1::27017 mongo:8`; baseline of `UpdateKeyTranslationTest`, `ConditionalUpsertReproductionTest`, `UpdateValueMarshallingTest`, `FindOneAndUpdateCompoundFilterUpsertTest`, `InMemUpsertDuplicateKeyTest`.

WP1 (J) `UpdateOperationsTest` (pure JUnit, `@Tag("core")`, package `de.caluga.morphium.query`): `twoSetCallsAreOneSetOperator`, `operatorsKeepInsertionOrder`, `sameKeyEqualValuesMerge`, `sameKeyDifferentValuesThrowAndNameKeyAndOperator`, `deepEqualsHandlesArraysAndNestedCollections`, `nullAllowedForSetAndSetOnInsertRejectedForInc`, `nullOrEmptyFieldNameRejected`, `unsetWithoutNamesRejected`, `isEmptyOnNewInstance`, `enumFieldUsesName`, `conflictSetAndIncSameField`, `conflictSetAndUnset`, `conflictSetAndSetOnInsert`, `conflictParentAndChildPath`, `siblingPathsDoNotConflict`, `prefixMatchIsSegmentBased` (`ab` vs `a`), `positionalPathsAreNotJudged`, `toOperatorMapReturnsACopy`.
WP2 (J) `UpdateOperations`, `ReturnDocument`, `UpdateResult`.

WP3 (J) integration tests (`MultiDriverTestBase`, `getMorphiumInstances`, under `MT/base/`). Fixtures: `Lease` (default `@Entity`, `@Id String id`, `ownerName`, `expiresAt`, `retryCount`, `renamed` with `@Property(fieldName="custom_x")`, `@Aliases`, single `Child`, `List<Child>`, `Set<Child>`, `Map<String,Child>`, `Mode` enum, `Instant`), `RawLease` (`translateCamelCase=false` with a default `@Embedded`), `Counter`, `VersionedDoc`; raw documents read as in `UpdateKeyTranslationTest.rawDocs` (`:91-100`).
- `UpdateBuilderLeaseTest`: `firstAcquireInsertsUnderFilterId`, `renewByOwnerKeepsOneDocument`, `heldLeaseThrowsDuplicateKeyAndLeavesDocumentUntouched` (mongoCode Number 11000, message contains E11000, same on both drivers), `takeoverAfterExpiry`, `releaseWithUnsetOfJavaNameRemovesRealField`, `raceOnFreshLeaseHasExactlyOneWinner` (8 threads, latch, one builder per thread, 5 rounds with distinct ids), `raceOnExpiredLeaseHasExactlyOneWinner`.
- `UpdateBuilderCounterTest`: `incCreatesOnInsertAndAddsOnUpdate`, `incNegativeAndDouble`, `concurrentIncrementsAreAtomic` (8 threads x 50 = 400), `beforeAndAfterReturnOldAndNewValue`, `incNullRejected`.
- `UpdateBuilderCreateIfAbsentTest`: `setOnInsertCreatesOnlyWhenAbsent`, `setOnInsertDoesNotOverwrite`, `createIfAbsentWithBeforeReturnsNullOnCreateAndExistingOtherwise` (the trap pin), `noMatchWithoutUpsertReturnsNull`.
- `UpdateBuilderValuesTest`: `embeddedListSetSingleAndMapValuesNeedNoConversion` (elements and single value carry `class_name`), `instantReadBackOnBothDrivers` (InMemoryDriver result unverified), `enumValueAndEnumInListStoredByName`, `entityForReferenceFieldRejected`, `writeToEncryptedFieldRejected`, `idStoredAsUnderscoreIdAndPinsId` (`setOnInsert("id", x)`).
- `UpdateBuilderKeysTest`: `javaNamesStoredUnderMongoNames`, `nonTranslatingEntityKeepsJavaNames`, `propertyFieldNameHonoured`, `dottedPathThroughSingleEmbeddedTranslated`, `pathBehindListTranslatesOnlyLeadingField` (documents the limit), `positionalPathPassesThrough`, `unknownFieldWrittenAsGiven`, `aliasKeyWrittenVerbatim`, `killSwitchAndPerQueryOverrideDoNotAffectBuilder`, `enumConstantEqualsStringName`, `javaNameAndStoredNameWithDifferentValuesRejected`, `javaNameAndStoredNameWithEqualValuesMerge`.
- `UpdateBuilderCountTest`: `updateManyReturnsMatchedAndModified` (3 documents, 2 match; second identical call gives modified 0), `updateOneUpdatesExactlyOneDocument`, `upsertReportsUpsertedIdAndMatchedZero` (pins that `n` includes upserts on the real server), `duplicateKeyThrowsOnBothDrivers` (the `writeErrors` normalisation), `arrayFiltersWorkOnUpdateMany`, `sortPicksDocumentForUpdateOne` (MongoDB 8 only, InMemoryDriver behaviour unverified), `combinedOperatorsAreOneCommand` (Pooled only, `serverStatus.opcounters.update` delta via `GenericCommand`; mechanism unverified, fallback `toUpdateDocument()` plus the profiler).
- `UpdateBuilderValidationTest`: `emptyBuilderRejectedAndDocumentIntact`, `versionedEntityWithoutVersionOperationRejected`, `versionedEntityAcceptsExplicitVersionInc`, `skipAndLimitRejected`, `arrayFiltersOnFindOneAndUpdateRejected`, `conflictingOperatorsRejectedBeforeAnyCommand`, `cacheClearedAfterBothTerminals`, `autoValuesNotAppliedAndWarnedOnce`, `collationHonoured`, `rollsBackInsideTransaction` (InMemoryDriver always; Pooled only with `assumeTrue` on a replica set).
- One pure `UpdateDocumentMapper` test confirming the builder's `translateKeys=true` and that the no-operator passthrough is never reached.

WP4 (J) `Query.update()` and `Query.updateMapped`; acceptance: count tests and `writeErrors` normalisation green on both drivers.
WP5 (J) `UpdateBuilder`: terminals, validation, version guard, limit/skip/arrayFilters guards, WARN once; acceptance: all WP3 classes green on `--driver all` against `mongo:8` and on `--driver inmem`.
WP6 (M with prose, stronger model): Javadoc with the result table and the event contract; fix the stale lines `Q:518-519` and `MorphiumObjectMapper.java:66-67`; CHANGELOG; `docs/api-reference.md` section; ledger: close MOR-006 with evidence, amend MOR-008 (`arrayFilters` missing from `FindAndModifyMongoCommand`, the InMemoryDriver drops it, projection and hint), amend MOR-010 (`lastErrorObject` needed in the command result), quote YAML titles that contain `: `, regenerate with `ledger_md.py`, run `--check`.
WP7 (M, reviewer-run): WP0 baseline plus the new classes on `--driver all`; `core` tag on `--driver inmem`; `ledger_md.py --check`; `git diff --stat`.

Haiku: no package in the production path (the trial supports Haiku only for transcription with a script-checkable result; WP7a prose scored 38-40/43 with invented content). Two optional scripted experiments, neither a dependency: a rerun of the ledger bookkeeping (MOR-008/MOR-010 amendments, closing text) with every title and sentence pre-quoted, scored by script; typing `ReturnDocument` and `UpdateResult` from exact source (so small that writing it directly is cheaper). No evidence for test matrices or conflict rules.

## 5. Risks, CHANGELOG, open questions, unverified

Risks: R1 the version guard may block the Quarkus adapter's entity if it uses `@Version` (Q2). R2 `Instant` on the InMemoryDriver: the encoder writes the `InstantMapper` shape (`BsonEncoder.java:373-374`) but the InMemoryDriver stores live objects (MOR-029), so "an Instant needs no manual conversion" may fail there. R3 the count path throws on `writeErrors` whereas `Query.set(..., upsert)` swallows them (MOR-005): builder and writer path differ, the Javadoc must say so. R4 `updateOne` with a sort needs MongoDB 8.0+ (unverified). R5 public names (`UpdateBuilder`, `UpdateResult`, `ReturnDocument`) become semver commitments; MOR-008 must reuse `ReturnDocument`. R6 no `checkIndexAndCaps` on upsert: with `IndexCheck.CREATE_ON_WRITE_NEW_COL` a builder upsert that creates the collection skips index creation, as `findOneAndUpdateMapped` does today. R7 `Query.clone()` is shallow for `arrayFilters` and `collation` (`Q:1976-2012`); the builder never mutates them. R8 a filter-less `update().updateMany()` updates every document (allowed, like `Query.delete()`). R9 no support for `@WriteBuffer` entities (bypasses the writer, so MOR-046 does not affect it).
CHANGELOG (Unreleased, "Added"): `Query.update()`, `UpdateBuilder`, `ReturnDocument`, `UpdateResult`; semantics bullets (always translated keys, element-form values, one command, no events and no auto values, the version guard, the lenient unknown-field rule, the null-result table, the arrayFilters limitation, the `writeErrors` normalisation). "Changed": only the two Javadoc corrections.
Open questions (recommendation first): Q1 builder on `Query` with a pure core vs public standalone `Update`; Q2 reject `@Version` entities when the update does not touch the version vs Javadoc only; Q3 WARN once per class for ignored `@LastChange`/`@CreationTime` vs silence; Q4 lenient unknown fields now, opt-in `strict()` later; Q5 differing duplicate key in one operator throws vs last wins; Q6 cross-operator conflict pre-check (deviation from D4) vs leave to the server; Q7 `ReturnDocument` mandatory argument vs `upsert()`/`returnNew()` flags; Q8 `setOnInsert` without upsert allowed vs rejected; Q9 public `toUpdateDocument()` vs package-private; Q10 lease helper (MOR-011) later vs a "held" Javadoc snippet only.
Not verified: everything above is read, not run; `n` semantics with upsert on a real server; the Pooled `upserted` shape; `Instant` on the InMemoryDriver; sort in an update statement on MongoDB 8 and on the InMemoryDriver; the `opcounters` approach via `GenericCommand`; write concern inside transactions and `w:0` results; the writer's limit loop line numbers (`W:2395-2420` region); `SingleMongoConnectDriver`, replica sets, PoppyDB, the Quarkus and Spring modules; whether `toQueryObject()` is immune to MOR-037.

---

# Review outcome (challenger verdict: go with changes)

Both reviews ran on a Sonnet-class model (the default model of the architect and challenger agents was not available). The challenger read the code; its line claims were not re-run by the orchestrating session. Nothing of MOR-006 is implemented.

## Corrections to the plan above (accepted)

- Finding 4: the `n += upserted.size()` code is at `InMemoryDriver.java:3461-3466` (not `:3452-3458`, which is the writeErrors indexing). The inner `update()` returns matched-only `n` (`:9752`); `runCommand` adds `upserted.size()`, so there is no double count. A real MongoDB's update reply includes upserted documents in `n`, so `matchedCount = n - upserted.size()` is right (the pinning test remains the confirmation).
- Finding 3 is too strong: the server accepts `arrayFilters` on findAndModify (since 3.6). Only Morphium's `FindAndModifyMongoCommand` lacks the field (`MongoCommand.asMap()` `:240-285` serialises by reflection, so adding a field puts it on the wire) and `InMemoryDriver.findAndOneAndUpdate` (`:11884`) passes `null` for arrayFilters although `updateInternal` (`:8881-8884`) already supports them. The gap is about 15 lines, not a limitation; do it instead of the guard that throws `UnsupportedOperationException`.
- The writer's limit loop is at `W:2435-2443` (not `W:2395-2420`).
- R7 is incomplete: `Query.clone()` copies the lists but shares their elements; sub-queries of `or`/`nor`, `FilterExpression` objects, `rawQuery`, `fieldList`, `hint` and the `Collation` object are shared, so later mutation of a sub-query leaks into the snapshot.
- "UDM rejects `@Reference`/`@Encrypted`" holds only for undotted keys given in Java form: `fieldOf` (`UDM:128-138`) returns null for `.` and `$` keys, so a write to `child.secret` with `secret` `@Encrypted` inside the embedded class passes.
- `findOneAndUpdateMapped` needs no change functionally, but its last step `updateLastAccess` (`Q:618`, body `Q:1707-1750`) sends a second, non-atomic `UpdateMongoCommand` for `@LastAccess` entities that `$set`s the untranslated Java field name with a Long. So "one atomic command" is false for those entities; the count path has no such write; `WRITES` is incremented only on the count path.
- Do not copy the template `(int) writeResult.get("n")` of `FindAndModifyMongoCommand:162` into `updateMapped` (ClassCastException/NPE for a Long or missing `n`).
- The InMemoryDriver ignores `sort` in update statements (`:3427` passes `null`) and drops projection/fields/hint in findAndModify (`:4129-4130`); its `$inc` (`:9133-9173`) has no else-branch for Short, Byte, BigDecimal (silently does nothing).
- Duplicate key: the InMemoryDriver throws `MorphiumDriverException` with Integer code 11000 and `writeErrors == null`; Pooled returns a reply map with `writeErrors` and no mongoCode on the update command (normalisation needed).

## Objections and proposed changes

| Rank | Objection | Proposed change |
|---|---|---|
| BLOCKER | The `@Version` guard is too weak (accepts `setOnInsert(version,1)`, which does not bump on update, `unset(version)`, `set(version,1)`), too strict (the consumer's entities carry `@Version Long entityVersion` initialised to 1 by hand, so every call site would throw until MOR-009; the workaround `setOnInsert(entityVersion,1L)` protects nothing) and throwaway (replaced by MOR-009). `store()` treats a missing/0 version as new and inserts (`W:826-841`, `:217-233`, `:924-929`); a builder upsert without a version leaves the field absent so the next `store()` inserts and hits E11000 (reproduction hypothesis 5c). | Two-case injection in this task (about 15 lines, after UDM): `getFields(type, Version.class)`, `resolveStoredFieldName`, skip if any operator already holds the key (an explicit touch wins), otherwise `$inc 1L` when the update has a modifying operator and `$setOnInsert {v: 1}` when it has none (create-if-absent must stay a no-op for an existing document; never `$inc` and `$setOnInsert` on one path, the server rejects that with code 40); skip injection or throw for `@Version(fieldName=...)` until MOR-044; honour `Query.isAutoValuesEnabled()` (`Q:216`). MOR-009 then only adds `@LastChange`/`@CreationTime`; amend MOR-009's scope in the ledger. |
| MAJOR | Scope: the consumer requirement (lease, counter, create-if-absent) needs only `findOneAndUpdate`; the count terminals carry most unverified assumptions (`n` with upserts, Pooled `upserted` shape, sort on 8.0 and the InMemoryDriver, arrayFilters, `writeErrors` normalisation) and the `UpdateResult` semver. | Ship `findOneAndUpdate` first; land `updateOne`/`updateMany`/`UpdateResult` as a separate follow-up change (serves MOR-018/022, priority 2/3). |
| MAJOR | Silent `updateOne` sort: the InMemoryDriver silently picks the first match, ignoring sort; a pre-8.0 server errors. Hint, projection, sort on `updateMany` are ignored although both commands carry hint. | Reject sort on `updateOne`/`updateMany` (`UnsupportedOperationException`) until the InMemoryDriver honours it and the server capability is checked; honour or reject hint and projection instead of ignoring them (the plan's own rule for skip/limit/arrayFilters, applied consistently). |
| MAJOR | `UpdateResult` as a record: adding a component later breaks the canonical constructor and record patterns; cannot express "unacknowledged" (`w:0` counts are 0, indistinguishable from no match); `Object upsertedId` is driver-dependent (Pooled decodes `ObjectId` to `MorphiumId`, the InMemoryDriver returns what was stored). | Final class with `getMatchedCount()`, `getModifiedCount()`, `getUpsertedId()` (converted to the entity's `@Id` type via the mapper), `isAcknowledged()`; mark `UpdateBuilder`, `ReturnDocument`, `UpdateResult` as incubating (`@apiNote`) until 6.3 because MOR-021 will generate code that references them. |
| MAJOR | `UpdateOperations` "pure" contradicts the requirement that conflicts are detected on translated keys (`set("ownerName")` + `inc("owner_name")` collide only after translation) and values are held by reference until the terminal (aliasing; plan-mor-042 E1 decided against exactly this); `deepEquals` on raw values fails for equal-content entity instances without `equals()` although the marshalled maps are equal, while UDM compares marshalled values. | Map each call eagerly through UDM on a one-entry operator document and store the mapped key and marshalled value in `UpdateOperations`; run merge and all-pairs conflict checks on mapped keys and values (also same-operator parent/child `set("a")` + `set("a.b")`, which the server rejects with code 40 and the InMemoryDriver accepts); do not marshal before UDM, or the `@Reference` guard never sees an entity; keep the terminal UDM pass as a safety net (idempotent). Errors are then raised at the offending call site. |
| MAJOR | `Instant` is the consumer's central type and unverified on the InMemoryDriver: a raw `Instant` in an update document stays an `Instant` (`UPDATE_VALUE_MAPPED_TYPES` excludes java.time); on Pooled the encoder writes `{type:"instant", seconds, nanos}` (`BsonEncoder:372-377`) identical to `InstantMapper`, on the InMemoryDriver the document holds a live `Instant` while `store()` holds the Map form, and `InstantMapper.unmarshall` casts to `Map` so a read-back likely throws ClassCastException (unverified). | WP0 spike before the design freeze; if confirmed, add `Instant` to `UPDATE_VALUE_MAPPED_TYPES` (shape-neutral on Pooled, fixes the InMemoryDriver). |
| MINOR | Entry name `update()` collides with the eager verbs (`delete()`, `set()`, `inc()`, `unset()` execute immediately); a forgotten terminal is a silently skipped write. | Non-verb entry (`updates()`/`modify()`), `@CheckReturnValue`-style Javadoc. |
| MINOR | `arrayFilters` guard on `findOneAndUpdate`. | Wire `arrayFilters` into `FindAndModifyMongoCommand` and `InMemoryDriver.findAndOneAndUpdate` (about 15 lines) instead of the guard plus its later removal. |
| MINOR | Partial-failure counts, `writeConcernError` and the `(int)` cast in the error template: an ordered multi update stops at the first error with earlier documents already modified (the InMemoryDriver mutates earlier documents without rollback); normalising `writeErrors` into an exception drops `n`/`nModified`; `writeConcernError` arrives with `ok:1`. | Put counts into the exception message, handle `writeConcernError`, avoid the cast. |
| MINOR | Natural-key lease uniqueness (R6 is a concurrency-safety issue, not cosmetic: a lease keyed by a natural key gets a random generated `_id` and uniqueness rests on a unique index, which the default `IndexCheck.WARN_ON_STARTUP` does not create); `@WriteBuffer` (R9 is reversed: the builder writes immediately and a still-buffered earlier `store()` can flush later and overwrite the atomic update); `@LastAccess` second write; at-least-once `$inc` (`WriteMongoCommand:239-257` retries after network errors without `txnNumber` dedupe, a counter can double-count); a duplicate key inside a real transaction aborts it server-side, so "held == catch 11000" cannot be control flow inside `@MorphiumTransactional`; cache clearing happens before commit inside a transaction. | Javadoc and tests; reject or WARN in `update()` when `isBufferedWrite(type)`. |
| MINOR | `setOnInsert`-only without `upsert` can never do anything; `inc(Number)` with Short/Byte/BigDecimal is a silent no-op on the InMemoryDriver. | Reject at the terminal; normalise to Integer/Long/Double or reject. |
| MINOR | `@CreationTime`/`@LastChange` WARN-once is not a contract: those fields stay null or stale on documents written by the builder while `Query.set` sets them. | Javadoc and CHANGELOG prominently instead of log noise; the real fix is MOR-009 honouring `isAutoValuesEnabled`. |
| MINOR | The lease sample still hand-parses `getMongoCode() instanceof Number n && n.intValue() == 11000`. | Add `MorphiumDriverException.isDuplicateKey()` now (mongoCode 11000 or any `writeErrors` code 11000). |
| MINOR | `Query.setTranslateUpdateFieldNames` exists but the builder ignores it. | State it in the Javadoc of that setter. |

## Test plan corrections

`combinedOperatorsAreOneCommand` via server-wide `opcounters` breaks under `--parallel` and findAndModify's counter behaviour is unproven (assert `toUpdateDocument()` and the stored result, profiler on an isolated database as fallback); `rollsBackInsideTransaction` tests the driver (the builder has no transaction code) and the plain `mongo:8` container is not a replica set, so the Pooled variant is always skipped;
`sortPicksDocumentForUpdateOne` cannot pass on the InMemoryDriver. Missing: the consumer's lease flow with `Instant` fields, `Long entityVersion = 1` and `store()` afterwards; `Set<Child>`, polymorphic and `@Embedded` inside lists; interplay with `Query.set` and `store()` on the same collection; a natural-key lease with and without a unique index; `@LastAccess` and `@WriteBuffer` entities; a `MorphiumId` `@Id` with a generated upsert; `overrideDB`; PoppyDB (shares the InMemoryDriver path). The lease races are deterministic (compound filter does not qualify for the server's automatic duplicate-key retry; the InMemoryDriver serialises on a collection lock);
`concurrentIncrementsAreAtomic` on a fresh key relies on the server retry for `{_id: key}` equality (4.2+) - say so. Commit structure: C2 bundled ~60 tests, four new production files and the `Query` edits; split into C2a (`Query.updateMapped`, normalisation, findAndModify `arrayFilters` wiring, package-private tests) and C2b (the builder); add a mutation run (PIT) for the conflict logic of `UpdateOperations`.

## Haiku

The recommendation stands (no Haiku in the production path; only optional scripted bookkeeping experiments; typing `ReturnDocument`/`UpdateResult` is too small to measure; no evidence for test matrices).

## Decisions needed before an implementation spec can be frozen

1. `@Version`: two-case injection in this task (recommended) vs. the guard.
2. Scope: `findOneAndUpdate` first, count terminals later (recommended) vs. both now.
3. Eager mapping per call through UDM (recommended) vs. late mapping at the terminal.
4. `UpdateResult` as a final class with `isAcknowledged()` (only relevant if the count terminals stay in scope).
5. Entry method name (`update()` vs. `updates()`/`modify()`), incubating marker.
6. `Instant` spike in WP0 and, if confirmed, add `Instant` to the mapped types.
7. `isDuplicateKey()` helper, `arrayFilters` wiring instead of the guard, reject sort/hint/projection instead of ignoring.

---

# Plan v2: decisions of the maintainer applied (supersedes the matching parts above)

Maintainer decisions (2026-10-15): all recommendations of the review are taken: automatic `@Version` injection, `findOneAndUpdate` first (count terminals later), eager mapping per call, an entry method that is not called `update()`, an `Instant` spike, `isDuplicateKey()`, `arrayFilters` wired into `findAndModify`, sort/hint/projection rejected instead of ignored.
Everything not mentioned here stays as in the plan above after the corrections of the review outcome. Nothing is implemented yet.

## F1. Scope of this task (replaces sections 1.2 and 4 where they contradict)

In scope: the builder with `set`, `setOnInsert`, `inc`, `unset`, `upsert`, the terminal `findOneAndUpdate(ReturnDocument)`, `ReturnDocument`, `Query.<entry>()`, `@Version` injection, the `arrayFilters` wiring in `FindAndModifyMongoCommand` and `InMemoryDriver.findAndOneAndUpdate`, `MorphiumDriverException.isDuplicateKey()`, and the `Instant` fix if the spike confirms the defect.
Out of scope, new ledger task MOR-050: `updateOne()`, `updateMany()`, `Query.updateMapped`, the `writeErrors` normalisation for the update command, `UpdateResult` (final class with `getMatchedCount()`, `getModifiedCount()`, `getUpsertedId()` converted to the entity's `@Id` type, `isAcknowledged()`), the sort question on `updateOne`, partial-failure counts, `writeConcernError`.
The builder surface is designed so that MOR-050 only adds terminals (`UpdateBuilder` keeps its operations in `UpdateOperations`; the terminal helper is one private method).

## F2. Names and API status

- Entry method on `Query<T>`: `modify()` returns `UpdateBuilder<T>` (not `update()`, which reads like the eager verbs `set()`, `delete()`, `inc()`). The Javadoc says that nothing is written until a terminal is called and carries a `@CheckReturnValue`-style note.
- `UpdateBuilder`, `ReturnDocument` (and later `UpdateResult`) are marked incubating with an `@apiNote` until 6.3: MOR-021 will generate code that references them. `ReturnDocument { BEFORE, AFTER }` is reused by MOR-008.
- `MorphiumDriverException.isDuplicateKey()` returns true if `getMongoCode()` is 11000 (any `Number`) or any entry of `getWriteErrors()` has code 11000. The lease snippet uses it. Tested on both drivers.

## F3. Eager mapping per call (replaces the late-mapping part of section 3, resolves Challenge 2 and 7)

Each builder call (`set`, `setOnInsert`, `inc`, `unset`) maps its one-entry operator document through `UpdateDocumentMapper(morphium, type, true)` immediately and stores the mapped key and the marshalled value in `UpdateOperations`. Consequences:
- Errors are raised at the offending call (`@Reference` entity, `@Encrypted` write, non-String map key, differing duplicate, `null` amount for `inc`), not at the terminal.
- The caller's collections are copied at the call (no aliasing), which matches plan-mor-042 E1.
- `UpdateOperations` is no longer "pure" in the sense of having no Morphium dependency at construction: the entity-aware part is the call into UDM; the merge and conflict logic (`UpdateOperations.add(operator, mappedKey, marshalledValue)`) stays a pure class and is unit-tested without a database on mapped keys and values.
- Merge rule: a second entry for the same mapped key and operator merges if `Objects.deepEquals` holds for the marshalled values (so two equal-content entity instances without `equals()` merge), otherwise `IllegalArgumentException` naming key and operator.
- Conflict rule: all pairs, on mapped keys, segment-based prefix matching, within one operator and across operators (`set("a")` + `set("a.b")`, `set` + `inc`, `set` + `unset`, `set` + `setOnInsert`, `inc` + `setOnInsert` on one path); pairs with a `$` segment are skipped; case-sensitive; `a.0` and `a.1` are siblings. The server rejects these with code 40; the InMemoryDriver accepts them. The InMemoryDriver parity fix (reject path conflicts for raw maps, too) is filed as MOR-051.
- The terminal pass through UDM stays as an idempotent safety net before the command is built; it is not the first line of defence.
- `inc(Number)`: `Integer`, `Long` and `Double` pass through, `Short`/`Byte`/`Float` are widened, `BigDecimal`/`BigInteger`/others are rejected with `IllegalArgumentException` (the InMemoryDriver `$inc` silently ignores them, `InMemoryDriver.java:9133-9173`).
- A builder that contains only `setOnInsert` operations and no `upsert()` is rejected at the terminal (it can never do anything). An empty builder is rejected (`IllegalStateException`), as before.

## F4. `@Version` injection (replaces the guard of section 2; resolves the BLOCKER)

After the call-time mapping, at the terminal, on the mapped document:
1. Find the `@Version` fields: `getFields(type, Version.class)`, stored name through `resolveStoredFieldName`. No `@Version`: nothing happens.
2. If `Query.isAutoValuesEnabled()` (`Q:216`) is false: nothing happens.
3. If any operator already holds the stored name of the version field (an explicit touch of the caller wins): nothing happens.
4. If the update has a modifying operator (`$set`, `$inc`, `$unset`): add `$inc {version: 1}`. On an insert `$inc` on a missing field yields 1, which equals `initVersionFieldForInsert` (1L); on an update it is the +1 that `store()` does.
5. If the update has only `$setOnInsert` operations: add `$setOnInsert {version: 1}` instead (create-if-absent must stay a no-op for an existing document); never `$inc` and `$setOnInsert` on one path (server code 40).
6. `@Version(fieldName=...)`: until MOR-044 is fixed `serialize` and `getMongoFieldName` disagree about the stored name; the terminal throws `IllegalStateException` explaining this instead of guessing.
MOR-009 then only adds `@LastChange` and `@CreationTime` (the ledger text of MOR-009 is amended). `@LastChange`/`@CreationTime` are NOT applied in this task: stated prominently in the Javadoc of `modify()` and in the CHANGELOG (no log noise; the WARN-once of the plan above is dropped).
Tests (both drivers): `versionIncrementedOnUpdate`, `versionInitialisedToOneOnInsert`, `createIfAbsentDoesNotBumpAnExistingVersion`, `explicitVersionTouchWins`, `unsetOfVersionIsNotInjectedOver`, `autoValuesDisabledSkipsInjection`, `storeAfterBuilderUpsertDoesNotHitDuplicateKey` (the hypothesis 5c trap), `staleStoreAfterBuilderUpdateFailsOptimistically` (a `store()` with the old version is rejected after a builder write), `versionFieldNameAnnotationRejectedWithMessage`, entity without `@Version` unaffected.

## F5. Rejections instead of silent ignores (resolves Challenge 9 and the "silently ignored options" point)

The terminal rejects with `UnsupportedOperationException` (message names the option and the reason): `skip != 0`, `limit > 1`, `hint`, field list/projection. `sort` is honoured for `findOneAndUpdate` (`FindAndModifyMongoCommand` supports it; the InMemoryDriver passes it, `InMemoryDriver.java:4117-4131`, to be verified by a test). Honoured: filter, sort, collation, `overrideDB`, collection name, class write concern, arrayFilters.
`arrayFilters`: `FindAndModifyMongoCommand` gets an `arrayFilters` field (it is put on the wire by `MongoCommand.asMap()` reflection, `:240-285`), `Query.findOneAndUpdateMapped` sets it from `getArrayFilters()`, and `InMemoryDriver.findAndOneAndUpdate` passes it to `updateInternal` (`:11884` passes `null` today; `updateInternal` supports it, `:8881-8884`). This also fixes the raw-Map `findOneAndUpdate` for array filters. MOR-008 keeps projection/hint wiring (amended: the `arrayFilters` part is done here).
Tests: `arrayFiltersWorkOnFindOneAndUpdate` (positional `$[x]` with filters, both drivers), `skipLimitHintProjectionRejected`, `sortPicksTheDocument` (both drivers).

## F6. Entities with write buffering, `@LastAccess`, natural keys (resolves the MINOR hazards)

- `modify()` throws `IllegalStateException` for an entity with `@WriteBuffer` when the buffered writer is active on a real MongoDB (`isBufferedWrite(type)`, `Morphium.getWriterForClass`): a still-buffered `store()` could flush later and overwrite the atomic update.
- `@LastAccess` entities: `findOneAndUpdateMapped` ends with `updateLastAccess` (`Q:618`, body `Q:1707-1750`), a second, non-atomic update command that `$set`s the untranslated Java field name with a Long. The builder documents that the "one atomic command" statement does not hold for `@LastAccess` entities; fixing the untranslated name is filed as MOR-052 (small defect).
- Javadoc and tests for natural-key leases: a lease keyed by a natural key (not `_id`) gets a generated `_id` and its uniqueness needs a unique index (the default `IndexCheck.WARN_ON_STARTUP` creates none); the builder does not call `checkIndexAndCaps` (same as `findOneAndUpdateMapped`). Test `naturalKeyLeaseWithUniqueIndexHasOneWinner`.
- Documented: `$inc` retries after a network error are at-least-once (`WriteMongoCommand:239-257`); a duplicate key inside a real transaction aborts it, so "held == catch duplicate key" cannot be control flow inside a transaction; the cache is cleared before commit inside a transaction; the builder ignores `Query.setTranslateUpdateFieldNames` on purpose (the Javadoc of that setter says so).

## F7. `Instant` spike (WP0) and the `UPDATE_VALUE_MAPPED_TYPES` decision

Before the design is frozen a spike test (`InstantUpdateSpikeTest`, both drivers) answers: does `findOneAndUpdate(Map.of("$set", Map.of("at", Instant)))` read back as the same `Instant` through `get()` on the InMemoryDriver and on a real MongoDB, and what does the raw document look like?
The challenger's reading: the InMemoryDriver stores a live `Instant` while `store()` stores the `InstantMapper` Map form, and `InstantMapper.unmarshall` casts to `Map` (ClassCastException). If the spike confirms it, `Instant` is added to `ObjectMapperImpl.UPDATE_VALUE_MAPPED_TYPES` (mapper output is identical to the encoder's `{type, seconds, nanos}` shape, so Pooled is unchanged) and a differential test pins that the stored shapes of `store()` and the update agree. If the spike refutes it, the spike test stays as a regression test. The same spike covers `LocalDateTime`, `LocalDate`, `LocalTime` (the other java.time mappers) and `Date`.

## F8. Work packages (revised)

Commits: C1 (green on the old code): `InstantUpdateSpikeTest` (documents or fixes), `isDuplicateKey()` + test, `UpdateOperationsTest` (pure) + `UpdateOperations`/`ReturnDocument`. C2: `arrayFilters` wiring (command, `findOneAndUpdateMapped`, `InMemoryDriver.findAndOneAndUpdate`) with its tests. C3: `UpdateBuilder`, `Query.modify()`, version injection, rejections, integration tests, docs, CHANGELOG, ledger. Each commit is green on its own; red tests are written first inside the commit that makes them green.
- WP0 (reviewer-run): JDK 21; `docker run -d --rm --name morphium-mor006 --tmpfs /data/db -p 127.0.0.1::27017 mongo:8` (a single node: no transactions; the transaction test runs on the InMemoryDriver only); baseline of the update test classes; the `Instant` spike.
- WP1 (J): `UpdateOperationsTest` (pure, on mapped keys and marshalled values): the cases of the plan above plus all-pairs conflicts within one operator, `deepEquals` on equal-content maps, `inc` number normalisation, `setOnInsert`-only detection, `isModifying()`.
- WP2 (J): `UpdateOperations`, `ReturnDocument`; `MorphiumDriverException.isDuplicateKey()`.
- WP3 (J): `arrayFilters` wiring; tests as in F5.
- WP4 (J): `UpdateBuilder`, `Query.modify()`, integration tests: `ModifyLeaseTest` (first acquire inserts under the filter `_id`, renewal, held lease -> `isDuplicateKey()` on both drivers and document untouched, takeover after expiry, release with `unset` of a Java name, races with 8 threads on a fresh and an expired lease (deterministic: compound filter does not qualify for the server's automatic duplicate key retry), natural-key lease with unique index, `store()` afterwards with `@Version Long entityVersion` and `Instant` fields, mix with `Query.set` on the same collection), `ModifyCounterTest` (`inc` creates on insert and adds on update, negative/double, 8 x 50 concurrent increments equal 400 - note: relies on the server's automatic retry for `{_id: key}` equality on 4.2+ - before/after values, `null` amount and unsupported number types rejected), `ModifyCreateIfAbsentTest` (setOnInsert creates only when absent and never overwrites; upsert + BEFORE returns null on create and the old document otherwise - the trap pin; no match without upsert returns null; `setOnInsert` without upsert rejected), `ModifyValuesTest` (embedded list/set/single/map values carry `class_name`, `Set<Child>`, polymorphic and `@Embedded` inside lists, `Instant` and java.time round trip, enum values by name, `@Reference` entity and `@Encrypted` write rejected also for dotted keys through UDM where possible, `id` -> `_id`), `ModifyKeysTest` (the translation cases of the plan above), `ModifyValidationTest` (empty builder leaves the document intact, version injection tests F4, rejections F5, `WriteBuffer` entity, conflicts rejected before any command, cache cleared, collation, `overrideDB`, `@LastAccess` second write documented, `autoValues` not applied for `@LastChange`/`@CreationTime` - pinned as the documented behaviour). `toUpdateDocument()` (package-private or public per Q9) is asserted instead of server `opcounters`; the transaction test runs on the InMemoryDriver only and asserts the builder adds nothing of its own.
- WP5 (M with prose, stronger model): Javadoc incl. result table and event contract, fix the stale lines `Q:518-519` and `MorphiumObjectMapper.java:66-67`, `docs/api-reference.md` section, CHANGELOG ("Added": builder, `isDuplicateKey()`, `arrayFilters` for `findAndModify`; "Fixed": `Instant` in update documents if confirmed; "Changed": the two Javadoc corrections), ledger.
- WP6 (reviewer-run): WP0 baseline + new classes on `--driver all`, `core` tag on `--driver inmem`, `ledger_md.py --check`, `git diff --stat`; a PIT run scoped to `UpdateOperations`.
- Haiku: unchanged recommendation (no production package; optional scripted ledger-bookkeeping rerun with pre-quoted titles for the ledger edits below; no evidence for test matrices).

## F9. Ledger changes (applied with this plan update)

- MOR-006 is rewritten to the narrower scope (entry method `modify()`, `findOneAndUpdate` terminal, eager mapping, `@Version` injection, rejections, `isDuplicateKey()`, `arrayFilters` wiring, `Instant` spike).
- MOR-009 is narrowed: `@Version` is done by MOR-006; MOR-009 adds `@LastChange` and `@CreationTime` (honouring `isAutoValuesEnabled`).
- MOR-008 is amended: `arrayFilters` is done by MOR-006; MOR-008 keeps the options object, projection and hint (and the missing `FindAndModifyMongoCommand`/`InMemoryDriver` wiring of those).
- MOR-010 is amended: `isDuplicateKey()` exists (MOR-006); MOR-010 adds `lastErrorObject` to the command result and the typed outcome.
- New: MOR-050 count terminals (`updateOne`, `updateMany`, `UpdateResult`, `writeErrors` normalisation, sort on `updateOne`); MOR-051 InMemoryDriver rejects path conflicts in update documents like MongoDB (code 40); MOR-052 `updateLastAccess` writes the untranslated Java field name.

## F10. Risks and not verified (additions)

Public names are semver commitments (incubating marker mitigates); `modify()` shares its name with nothing else on `Query` but is a new verb; version injection changes the stored version for builder writes (intended, documented); an entity with `@Version(fieldName)` cannot use the builder until MOR-044; eager mapping calls UDM per call (cost is small). Not verified: the `Instant` behaviour (spike), sort in `findAndModify` on the InMemoryDriver, `arrayFilters` in `findAndModify` on the server (documented since 3.6, to be confirmed by the test), PoppyDB (shares the InMemoryDriver `runCommand`), `SingleMongoConnectDriver`, replica sets, the Quarkus and Spring modules.
