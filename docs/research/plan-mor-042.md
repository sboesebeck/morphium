# Implementation plan: MOR-042 writer path delegates to the shared update-value walk (PR 3)

Status: DRAFT by the architect (read-only, nothing was run), to be reviewed by the challenger. Paths: `MC` = `morphium-core/src/main/java/de/caluga/morphium`, `MT` = `morphium-core/src/test/java/de/caluga/test/mongo/suite`.
Base: branch `feat/mor-041-key-translation` at `092f264dd`.

## 0. Findings that change the task

1. The writer must keep its legacy single-value `class_name` (and the `_id` of a single `@Entity`). `MT/base/TypeIdTests.java:47-61`: `Query.set("test", new EmbeddedObject)` into an `@AdditionalData` map must come back as an
   `EmbeddedObject` and carry `class_name`; `TypeIdTests.java:123-134`: after `setInEntity` of a single embedded, `checkTypeId` must find `class_name` ("as during update we do not know if the field has java representation that matches").
   The writer never looks at the declared field type. `store()` writes `class_name` for `@AdditionalData` values through `serializeMap` (`MC/ObjectMapperImpl.java:605-612`, `:967-970`); the walk (PR 1) gives a single value none (`ObjectMapperImpl.java:324-328`).
   Side finding: PR 1's `findOneAndUpdate` has this gap for unknown, `@AdditionalData` and `Object`-typed keys (item F in section 5).
2. No mode parameter on the mapper interface: "single value" vs "array element" is expressible by wrapping the value in a one-element list, which `UpdateDocumentMapper.mapValue` already does (`MC/query/UpdateDocumentMapper.java:123-132`).
   Element form equals the legacy single-value output: `serialize` + `class_name`, `_id` kept.
3. `pushPull` must use the element form: the legacy helper gave `$push` of a single entity `class_name` (`MorphiumWriterImpl.java:2739-2743`); the walk in field form would drop it.
4. Writer errors are wrapped: marshalling runs inside `WriterTask.run()` (`MorphiumWriterImpl.java:2372-2375`, `:2674`, `:2860`); `submitAndBlockIfNecessary` (`:1298-1336`) wraps every non-driver exception in `RuntimeException(e)` after
   `retriesOnNetworkError` attempts (default 1, `config/ConnectionSettings.java:20`). An `IllegalArgumentException` for a bad map key reaches the caller as `RuntimeException(IAE)`, unlike `findOneAndUpdate`. Keep and pin with a test.
5. `MorphiumBase.pullAll(Query, String, List<Object>, ...)` sends `$pull`, not `$pullAll` (`MorphiumBase.java:1301-1303` calls `pull(...)` -> `pushPull(PULL)`); `Query.pullAll(String, List)` (`Query.java:2160-2162`) takes this path. The 6-argument variants
   (`MorphiumBase.java:496-498`, `Query.java:2169-2175`) correctly use `pushPullAll`. Defect, not in PR 3; the golden test pins it.
6. `Query.pull(Enum, Expr)` (`Query.java:2193-2203`) passes `Expr.toQueryObject()` (an `Object`) into `pushPull`; the walk now deep-copies it (legacy: shallow). Needs its own regression test.

## 1. Behaviour-difference table: old helper (`MorphiumWriterImpl.java:2733-2783`) vs walk (`ObjectMapperImpl.java:253-340`)

KEEP = legacy output preserved by construction, SWITCH = new behaviour, FLAG = switch + CHANGELOG + maintainer question.

| # | Case | Old helper | Walk | Decision |
|---|---|---|---|---|
| 1 | Single `@Embedded`, field | `serialize` + `class_name = getTypeIdForClass(cls)` (`:2739-2743`) | no `class_name` (polymorphic class still gets one from `serialize`, `ObjectMapperImpl.java:552-558`) (`:324-328`) | KEEP legacy: writer calls the element form for a top-level entity/embedded (TypeIdTests) |
| 2 | Single `@Entity`, field | `serialize` keeps `_id`, plus `class_name` | `_id` removed, no `class_name` (`:326-328`) | KEEP legacy (element form keeps `_id`, adds `class_name`); `store()` removes `_id` (`ObjectMapperImpl.java:710-714`), arguably a defect, leave it, Q1 |
| 3 | `class_name` value | `getTypeIdForClass` (`AnnotationAndReflectionHelper.java:178-190`) | `getTypeId` (`ObjectMapperImpl.java:909-929`) | equivalent by reading (Entity.typeId, Embedded.typeId, class name, real class); test `typeIdsOfCustomTypeIdClassesAreUnchanged` |
| 4 | Single value, element (`$push`/`$addToSet`/`$pull`) | same as 1/2 | field form drops `class_name` | element form (`marshallUpdateElement`) so output is identical to legacy |
| 5 | Top-level Enum | `name()` (`:2735-2737`) | `name()` (`:264-266`) | KEEP |
| 6 | Enum inside a List or Map value | untouched; encoder writes `toString()` (`driver/bson/BsonEncoder.java:354-357`); a constant with a body fails with "Unhandled Data type" | `name()` | SWITCH; changes stored value only if `toString()` is overridden; `name()` is what `store()` writes; FLAG, Q3 |
| 7 | `null` element in a List | NPE at `o.getClass()` (`:2748`) | kept as `null` | SWITCH (fix) |
| 8 | `MorphiumId` inside a container or as a single `$push`/`$addToSet`/`$pull` element | raw `MorphiumId`; encoder writes ObjectId (`BsonEncoder.java:242-245`) | `ObjectId` | SWITCH; wire-identical on MongoDB; on InMemoryDriver the stored object type changes to the `store()` shape (PR 1 pinned `$addToSet`/`$pull` then match). Top-level `MorphiumId` in `Query.set` stays raw |
| 9 | Non-String map key | Enum and others: `IllegalArgumentException("Can't push maps with Key not of type String!")` (`:2764-2766`); `null` key: NPE | Enum: `name()`; other or `null`: IAE with a different text (`:279-283`) | SWITCH for Enum keys; same exception type for the rest; no test pins the old text; via the writer wrapped in `RuntimeException` |
| 10 | `Set`, other non-List `Collection` | untouched | copied to `ArrayList` | SWITCH; real MongoDB: `Set<String>` wire-identical (encoder `BsonEncoder.java:195`), `Set<POJO>` was the hole; InMemoryDriver: raw Set becomes a List |
| 11 | Object arrays | untouched | `ArrayList` | SWITCH (same wire shape; POJO hole); primitive arrays and `byte[]` unchanged |
| 12 | Map-in-Map, List-in-Map, Map-in-List, List-in-List | not recursed | recursed | SWITCH (hole fix); a POJO in a `$each` map of `$push` is closed too |
| 13 | Non-Collection `Iterable` (`Path`, custom) | untouched | untouched (only `Collection`) | KEEP; do not follow the ledger wording "any Iterable" (`Path` is an Iterable of itself; encoder encodes only `Collection`); correct the ledger text |
| 14 | `List` subtypes | `List.class.isAssignableFrom` | `Collection` | identical for Lists |
| 15 | `@Entity`/`@Embedded` class that is itself a Map or Collection | entity check first | Map/Collection first | edge case, no example in the repo; document, no test |
| 16 | Custom-mapped types | not touched | only `AtomicBoolean/Integer/Long`, `BigInteger` | SWITCH only for those four (they failed with "Unhandled Data type"); java.time, `BigDecimal`, `Date`, `Character` unchanged |
| 17 | `Expr` object | passthrough | passthrough (not Map/Collection) | KEEP; `Query.pull(Enum, Expr)` passes the Map/List of `toQueryObject()`: now deep copy, enum to name, non-String key anywhere throws; test `pullWithExprStillWorks` |
| 18 | Copy semantics | List/Map copied, Set/array aliased | always copied | SWITCH (better): the writer no longer hands a live caller Set/array to the async task |
| 19 | Result types | `ArrayList`, `LinkedHashMap` | same | KEEP |
| 20 | `$currentDate` via `MorphiumBase.currentDate` (`:1364-1370`) | `{field:1}` Map copied | same | KEEP; `CurrentDateReproductionTest` stays green and unchanged |
| 21 | Key handling | strict `getMongoFieldName` (`:2373`) | not touched | KEEP (plan-mor-041 section 0 item 2): the walk shares values only |
| 22 | `@Reference` / `@Encrypted` guard | none | only in `UpdateDocumentMapper` | KEEP (no guard on the writer path; would be breaking), Q7 |

## 2. Design

Only in `MC/writer/MorphiumWriterImpl.java` plus one default method on the mapper interface. No change to `Query`, `MorphiumBase`, `BsonEncoder`, `InMemoryDriver`, `ObjectMapperImpl.marshallUpdateValue`.

Mapper interface (`MC/objectmapping/MorphiumObjectMapper.java`, after `:74`):
```java
/** Marshals a value that is one element of an array ($push, $addToSet, $pull, $pullAll) ... */
default Object marshallUpdateElement(Object value) {
    return ((java.util.List<?>) marshallUpdateValue(java.util.Collections.singletonList(value))).get(0);
}
```
`marshallUpdateValue` stays abstract; the new method is a default derived from it so external implementors still compile (the only implementor in the workspace is `ObjectMapperImpl`). No boolean/enum mode parameter. `UpdateDocumentMapper.mapValue` (`:123-132`) is simplified to call it
(the only edit outside the writer).

Writer (signatures unchanged; `marshallIfNecessary` stays public and non-interface, so `AsyncWriterImpl` still inherits it):
```java
public Object marshallIfNecessary(Object value) {      // field context: Query.set
    if (value != null && morphium.getARHelper().isEntity(value)) {
        return morphium.getMapper().marshallUpdateElement(value);   // legacy: class_name (+ _id of an @Entity)
    }
    return morphium.getMapper().marshallUpdateValue(value);
}
private Object marshallElement(Object value) { return morphium.getMapper().marshallUpdateElement(value); }   // element context
```
`set` (`:2374`) keeps `marshallIfNecessary`; `pushPull` (`:2674-2676`) uses `marshallElement` and drops the redundant `v instanceof Enum` ternary; `pushPullAll` (`:2860`) uses
`value = (List<?>) morphium.getMapper().marshallUpdateValue(value)` once for the whole list (elements are then in container context = element form), keeping an explicit null check for `v` (legacy NPE at `.stream()`; do not silently send `$each: null`).
Keep `getMongoFieldName` for keys (`:2373`, `:2675`, `:2873`) and `handleLastChange`/`handleCreationTimeOnUpsert`. Marshalling stays inside `run()` (eager move rejected, Q6). Remove unused imports (`Collectors`, `LinkedHashMap`; check the file). Javadoc on `marshallIfNecessary` states the legacy single-value rule.

## 3. Caller impact

Writer methods whose values reach `marshallIfNecessary`: `set(Query, Map, ...)`, `set(T, col, Map, ...)` (`:1270` calls it), `pushPull`, `pushPullAll`. Not callers: `inc`, `unset` (field names only), `BufferedMorphiumWriterImpl` (`set` `:631-658`, `pushPull` `:944-985`, `pushPullAll` `:988-1030` pass values raw),
`MorphiumBulkContext.createUpdateRequest` (`bulk/MorphiumBulkContext.java:56-64`, raw values).

| Caller (file:line) | Value types | Effect |
|---|---|---|
| `Query.set/setEnum` (`Query.java:2023-2109`) | any; `Map.of` (immutable; NPE for a `null` value in the `String,Object` overloads, `:2024`, `:2028`) | holes closed, rows 1-20 |
| `MorphiumBase.set` deprecated overloads (`:1179-1338`), `setInEntity` family (`:757-860`) -> `Morphium.setInEntity` (`Morphium.java:1229-1243`) | any | same |
| `MorphiumBase.currentDate` (`:1364-1370`) | `{"$currentDate":{field:1}}` | unchanged (row 20) |
| `MorphiumBase.setEnum` (`:1210-1219`) | `values.get(est.getValue())` looks up by value: defect, read only | unchanged; ledger candidate |
| `push`, `addToSet`, `pull` in Query/MorphiumBase/`Morphium.push` (`Morphium.java:1066-1072`, `MorphiumBase:1061-1155`, `1248-1255`) | scalar, POJO, `MorphiumReference` (an `@Embedded`, `MorphiumReference.java:10`), `Map` (`$each`, conditions), `Expr` result | element form, rows 4, 8, 12, 17 |
| `pushAll`, `addAllToSet`, `pullAll` 6-arg (`MorphiumBase:493-497`, `1220-1245`, `1257`; `Query:2143-2175`) | `List<?>` | whole-list walk |
| `Query.pullAll(String, List)` 2-arg | `$pull` of a list (defect) | structure unchanged; pinned in golden |
| Quarkus `MorphiumMigrationRunner.java:524-530`, `:584` | `Map.of(String, Date)`, with `upsert`; entity `MorphiumMigrationLock` with `@Property(fieldName=...)` | all leaves pass through; same output; keys `getMongoFieldName`. Regression: core mirror test plus the Quarkus tests (`MorphiumMigrationRunnerOrderingTest`, `integration-tests/.../MorphiumMigrationTest`, not run) |
| Messaging `DualChannelMessaging.java:1451, 2007, 2029`, `SingleCollectionMessaging.java:1382, 1404`, `MultiCollectionMessaging.java:1106` | `Date` | leaf, identical |
| `morphium-jakarta-data`, `spring-boot-morphium`, `poppydb` | no calls to the writer methods (grep) | none |

Regression strategy: (1) golden test recorded on `092f264dd`: identical output for all KEEP rows; (2) existing suites on both drivers: `TypeIdTests`, `UpdateTest`, `QueryUpdateOperatorsTest`, `ThrowOnErrorTest`, `AutoVariableTest`, `LastAccessTest`, `MultiUpdateTests`, `AsyncOperationTest`, `LifecycleTest`,
`NetworkRetryTest`, `DefaultValuesTest`, `QuerySubDocsTest`, `BasicFunctionalityTest`, `CoreFunctionalityTests`, `BufferedWriterTest`, the repro classes (`CurrentDateReproductionTest` unchanged), `UpdateValueMarshallingTest`, `UpdateKeyTranslationTest`, messaging `ExclusiveMessageTests`,
`MessagingRequeueEventTest`, `DualChannelMessagingFallbackTest`; (3) Quarkus module tests above.

## 4. Work packages (test first)

Commit order: C1 golden test and baselines, green on the old code. C2 red tests. C3 implementation. C4 flips, docs and ledger.

WP0 (MECHANICAL, reviewer-run): JDK 21 (Byte Buddy fails on 25); `docker run -d --name morphium-mor042 -p 127.0.0.1::27017 mongo:8`, `docker port`; baseline run of the regression subset (`./runtests.sh --uri mongodb://127.0.0.1:<port>/morphium_mor042 --driver all --test <Class>`, plus `--driver inmem` alone).

WP1 golden characterization (JUDGEMENT): new `MT/base/WriterUpdateGoldenTest.java` (`MultiDriverTestBase`, `@Tag("core")`); resources `morphium-core/src/test/resources/golden/writer-update-<driverName>.txt` (one per `PooledDriver`, `InMemDriver`).
Fixture `GoldenDoc` (`@Entity(translateCamelCase=false)`): `key`, `text`, `counter`, `when` (Date), `color` (Enum without `toString` override), `item` (`@Embedded`), `poly` (polymorphic `@Embedded` + subclass), `anything` (`Object`), `items` (`List<Item>`), `tags` (`List<String>`), `nums` (`List<Long>`),
`itemMap`, `strMap`, `ids` (`List<MorphiumId>`), `mid`, `@AdditionalData extra`; a second small `@Entity` with default camelCase pins the strict key path. Raw documents read without the projection (as `UpdateKeyTranslationTest.rawDocs`). Canonical text: sorted keys, type-tagged leaves (`i32:5`, `i64:5`, `str:"a"`, `Date`, `Oid`), `_id` dropped,
dynamic `Date` from `@LastChange` shown as `Date*`; for an exception only the class chain. Record mode: env var `MORPHIUM_GOLDEN_RECORD=1`; assert mode compares.
Scenarios (one document per scenario): Field (`Query.set`): S01 String, S02 int, S03 Date, S04 Enum, S05 `null`, S06 single Item, S07 polymorphic Dog, S08 single `@Entity` into `anything` (keeps `_id`+`class_name`), S09 `List<Item>`, S10 `List<String>`, S11 `Map<String,Item>`, S12 `Map<String,String>`, S13 top-level `MorphiumId`, S16 mixed List,
S17 Item into `@AdditionalData` key, S18 camelCase key under the default entity. Element: P01 push String, P02 push Item, P03 push Dog, P04 push Long, P05 pushAll `List<String>`, P06 pushAll `List<Item>`, P07 addToSet String, P08 addToSet Item, P09 addAllToSet, P10 pull String, P11 pull Item, P12 pull `{ $gt: 3 }` on `nums`,
P13 pullAll 6-arg, P14 `pull(Enum, Expr)`, P17 push `{ $each: [scalars] }`, P18 push a `Map<String,Item>` as one element, P16 push with `upsert=true` on a missing document (simple filter). Pins: C01 `currentDate` result (`writeErrors` code 52 on a real MongoDB, `n=1` on the InMemoryDriver) and no change to the document; D01 `Query.pullAll(String, List)` 2-arg (`$pull` of a list; defect).
Recorded but in the SWITCH set (not asserted equal): X01 `List<Color>` with a `toString` override, X02 `null` list element, X03 Enum map key, X04 `AtomicLong`/`BigInteger`, X05 `Set<String>`/`String[]`, X06 `List<List<Item>>`, X07 `List<MorphiumId>`, `push(MorphiumId)`, X08 non-String key.
Acceptance: green on `092f264dd` on both drivers; after C3 the same file is green with no golden file modified, except that the X-scenarios move to WP2's assertions. Write errors are swallowed on update paths (MOR-005): the golden compares stored documents, never return maps (except pin C01).

WP2 red tests for the SWITCH and hole rows (JUDGEMENT): new `MT/base/WriterPathMarshallingTest.java`, both drivers, raw-doc read-back: `querySetOfSetOfEmbeddedStoresListWithClassName`, `querySetOfEmbeddedArrayStoresListWithClassName`, `querySetOfMapInMapOfEmbeddedStoresNestedDocsWithClassName`;
`querySetOfSingleEmbeddedKeepsClassNameButFindOneAndUpdateDoesNot` (documents the deliberate divergence, rows 1, 2; the `@Entity` keeps `_id`); `querySetOfEnumListAndMapValuesStoresNames` (incl. body constant and `toString` override); `querySetWithNullListElementStoresNull`; `querySetWithEnumMapKeyStoresName`;
`querySetWithNonStringMapKeyIsRejectedAndNothingIsWritten` (`RuntimeException` with IAE in the cause chain, document unchanged); `morphiumIdsInsideContainersAreObjectIdsAndTopLevelStaysRaw`; `pushAndAddToSetOfEmbeddedAndSetOfListOfEmbedded`; `pushWithEachMapMarshalsEveryElement`; `pullAllAndPullByEmbeddedCondition`;
`addToSetOfEqualEmbeddedDoesNotDuplicate`; `pushAllOfNestedListsOfEmbedded`; `addAllToSetOfEmbedded`; `pullWithExprStillWorks`; `callersCollectionsAreNotMutatedAndMayBeImmutable` (`List.of`, `Map.of`, `Set.of`); `mutationAfterCallDoesNotLeakIntoAsyncWrite` (callback overload);
`migrationLockShapedUpsertAcquiresAndRenews` (mirror of the runner: `Map.of(String, Date)`, `@Property(fieldName)` entity, upsert, filter `_id` and `expires_at <= now`; held-lease case `assumeFalse(InMem)`, MOR-030); `typeIdsOfCustomTypeIdClassesAreUnchanged`; `AtomicLong` and `BigInteger` via `Query.set`.
Pure unit test: `MorphiumObjectMapperDefaultMethodTest.marshallUpdateElementEqualsFirstElementOfWalkedSingletonList`; a minimal stub implementor still compiles.

WP3 implementation (JUDGEMENT, about 60 lines): `MorphiumObjectMapper.java`, `MorphiumWriterImpl.java` (3 call sites plus helper), `UpdateDocumentMapper.java:123-132` (use the default method). Acceptance: WP1 and WP2 green on `--driver all`.

WP4 flip pins (MECHANICAL, exact expectations): `MT/repro/PojoInSetOperatorReproductionTest.writerPathShapes` (`:182-235`): all 6 shapes succeed on both drivers; remove the `handled`/"Unhandled Data type" branch and the InMemoryDriver `continue`; raw documents: list, set, array = `[{name:"a",amount:1,class_name:typeId}, {..b..}]`, single = WITH `class_name`, map and map-in-map values with `class_name`.
Update the class Javadoc and note the single-value divergence against `rawPojoShapesInSet`. `CurrentDateReproductionTest`: no edit (proof that row 20 is unchanged).

WP5 Javadoc, CHANGELOG, ledger (MECHANICAL with prose, stronger model). WP6 verification (reviewer-run): WP0's subset plus new classes on `--driver all`; `core` tag on `inmem`; `ledger_md.py` + `--check`; `git diff --stat`.

Haiku: nothing in the production path (WP2, WP3, WP5). One scripted experiment, not a dependency: WP4 (the flip of `writerPathShapes`) with the exact expected maps in the spec, scored by script (diff limited to the one file; compiles; red on `092f264dd`, green after C3; no `Co-Authored-By`), 3 Haiku runs + 1 stronger baseline. Optionally WP1's canonicaliser as a second experiment.

## 5. Scope for related defects

| Defect | In PR 3? | Reason |
|---|---|---|
| `MorphiumBase.currentDate` sends `{$set:{$currentDate:...}}` (MOR-004) | Out | not a marshalling problem; needs a real operator entry point (new writer method touching `MorphiumWriter`/Buffered/Async, or an operator pass-through) and an InMemoryDriver change; flips `CurrentDateReproductionTest`, which must stay green and unedited in PR 3 |
| `inc(Query, Map)` puts keys into `$inc` untranslated (`MorphiumWriterImpl.java:2025` vs the single-field `inc` at `:2159`) | Out, confirmed by reading | fold into MOR-005 with a real-MongoDB test; needs a decision (strict `getMongoFieldName` throws for unknown names) |
| swallowed `writeErrors` on update paths (MOR-005, GitHub #408) | Out | test-design rule: assert stored documents |
| `simplifyQueryObject` `putAll` data loss (MOR-037) | Out | used on upsert paths (`pushPull :2671`, `pushPullAll :2866`, `inc :2030`, `handleCreationTimeOnUpsert :2280`); PR 3 tests use single-predicate filters for upserts |

New ledger items to file (next free ids MOR-046 and up; quote YAML titles that contain `: `): A. `Query.pullAll(String, List)` and `MorphiumBase.pullAll(String,...List<Object>)` send `$pull` (`MorphiumBase.java:1301-1303`); B. `BufferedMorphiumWriterImpl`: `set`, `pushPull`, `pushPullAll` do not marshal values
(`:631-658`, `:944-985`, `:988-1030`), and `pushPullAll` for PULL emits `$push` (about `:1010-1012`); C. `MorphiumBulkContext.createUpdateRequest` does not marshal values (`bulk/MorphiumBulkContext.java:56-64`); D. `MorphiumBase.setEnum` uses `values.get(est.getValue())` (`:1215`); `Query.set(String, Object, ...)` with `Map.of` NPEs on `null` (`Query.java:2024`, `:2028`);
E. writer path has no `@Reference`/`@Encrypted` guard; F. `findOneAndUpdate` single embedded value for an unknown, `@AdditionalData` or `Object`-typed key has no `class_name` (relevant to MOR-006); G. correct MOR-042 wording: "Collection" instead of "any Iterable".

CHANGELOG (Unreleased): Fixed: `Query.set/push/pushAll/pull/addToSet/addAllToSet` with `Set`, array, Map-in-Map and nested containers of `@Embedded`/`@Entity` values (MOR-042). Changed: Enum constants inside Lists and Maps written as `name()`, not `toString()`; `null` list elements no longer throw; Enum map keys accepted; `MorphiumId` inside containers and as an element becomes `ObjectId`
(visible on the InMemoryDriver only); `Set`, non-primitive arrays and other collections reach the InMemoryDriver as Lists; `AtomicBoolean/Integer/Long` and `BigInteger` written via their mapper. Added: `MorphiumObjectMapper.marshallUpdateElement` (default method). Unchanged and stated: key handling of the writer, single embedded keeps `class_name`, no guards, `currentDate` unchanged.
Rewrite the closing sentence of the MOR-002 entry ("Not covered: Query.set/push/pushAll"). Ledger edits: close MOR-042 with evidence; fix the "Iterable" wording; add items A-G; MOR-005 gets the `inc` finding.

## 6. Risks

1. Enum `toString` to `name()` (row 6): stored values change for classes that override `toString`; old data not touched; maintainer ack (Q3). 2. InMemoryDriver behaviour changes for Set/array/MorphiumId (rows 8, 10, 11): tests reading raw InMemoryDriver arrays or Sets from `$set` would fail; grep showed none.
3. Deep copy of `Expr` results (row 17): a non-String key at depth now throws; `Expr.toQueryObject` not read in detail. 4. Entity that is also a Map/Collection (row 15). 5. Async writer: marshalling happens in the worker; unchanged by PR 3; exceptions arrive via the callback (`:1341-1370`).
6. `Query.pull(Enum, Expr)` and `pullAll(2-arg)` oddities, pinned by the golden test. 7. Writer and `findOneAndUpdate` stay inconsistent on single-value `class_name`, key resolver and guards; documented, not fixed.

## 7. Open questions

Q1 keep `class_name` and `_id` for single values on the writer path (recommended yes) or align to `store()` later behind a setting? Q2 default method `marshallUpdateElement` on the interface (recommended) or a private helper? Q3 accept Enum `toString` to `name()` as a bug fix (recommended yes)? Q4 route for MOR-004 (new writer method vs pass-through)?
Q5 file items A-G? Q6 marshal eagerly outside the task so IAE is thrown directly and callers cannot mutate after the call? Recommended no for PR 3. Q7 add `@Reference`/`@Encrypted` guards to the writer later (breaking for callers who embed entities today)? Q8 golden files as committed text resources acceptable?

## 8. Not verified

Nothing was run. Whether `Set<String>` through `Query.set` works on the InMemoryDriver today (the reproduction covered `Set<Item>` only); whether the env-var record switch reaches the surefire fork through `runtests.sh`; `Expr.toQueryObject` key types; `AsyncWriterImpl` error timing; Buffered writer details (read at the lines cited only); the claims about
`Query.pullAll(2-arg)`, `MorphiumBase.setEnum`, `inc(Query, Map)` and Buffered `$push` for PULL come from reading, not from a test; Quarkus tests, `SingleMongoConnectDriver`, replica sets, PoppyDB; whether any test reads InMemoryDriver raw arrays or Sets from `$set`.

---

# Review outcome (challenger verdict: go with changes)

Both reviews ran on a Sonnet-class model (the default model of the architect and challenger agents was not available). The challenger read the code; its line claims were not re-run by the orchestrating session. Nothing of PR 3 is implemented.

## Corrections to the plan above (accepted)

- Row 18 and the WP2 test `mutationAfterCallDoesNotLeakIntoAsyncWrite` are false under the chosen design: marshalling stays inside `WriterTask.run()` (Q6), which runs on the executor after the caller returned and is re-run on retry (`submitAndBlockIfNecessary`, `:1341-1370`); the caller can still mutate its List/Set before the walk iterates it.
- Row 12 is incomplete: for `pushAll` the legacy helper is applied per element, so a nested List or Map element is recursed one level and inner entities get `class_name`; only deeper levels and a List-of-Lists in `Query.set`/`push` were not handled.
- Row 7 overstates: the NPE occurs only for a null element inside a List value; `pushAll(field, [null])` calls `marshallIfNecessary(null)` per element and returns null today (no change); a null element nested one level inside a `pushAll` element List does NPE today.
- Row 6 is half true: `serializeEnum` (`ObjectMapperImpl.java:1001-1020`) writes `name()` only for typed enum element types and `{class_name, name}` for `Object`, `List<Object>`, `Map<String,Object>` and `@AdditionalData`; the walk gives a plain String there (as the legacy top-level enum already does). CHANGELOG: "typed enums". On the InMemoryDriver the stored value goes from a raw Enum object to a String.
- "Default method so external implementors still compile" is vacuous (`marshallUpdateValue` is abstract since PR 1); the default method is still fine because the walk needs the mapper instance and the callers live in `writer`, `query` and `objectmapping`.
- Row 8 expectations are driver-dependent: `BsonDecoder.java:122` decodes ObjectId to `MorphiumId` on a real MongoDB, so a raw read-back cannot show `ObjectId` on Pooled (meaningful on InMem or via a typed read); the same applies to golden `Oid` leaves.
- Section 3 `:1270` is not a direct call: `set(T,...)` reaches `marshallIfNecessary` only through `set(Query,...)` at `:2374`.
- WP6 Quarkus tests resolve `morphium-core` from `~/.m2` unless run in the reactor (`pom.xml:503-505`): state the command (with `-am`).
- Missing rows: (a) `Class` as a value: `AnnotationAndReflectionHelper.isEntity(Object)` special-cases `o instanceof Class`, so `Query.set("type", SomeEntity.class)` takes the entity branch in the writer conditional and the walk (`ObjectMapperImpl.java:318`) and calls `serialize(Class instance)`; legacy passed it through raw. A PR 1 latent bug that PR 3 widens;
  fix with `isEntity(o.getClass())`; (b) nested non-String map keys: legacy checked keys only at the top level, nested ones passed (InMem) or failed with ClassCastException in `BsonEncoder.encodeDocument` (Mongo); the walk throws IAE on both: name it in the CHANGELOG; (c) `Query.set(Enum field, ...)` uses `field.toString()` (`Query.java:2028`), not `name()` like the other overloads;
  (d) `Query.set(String/Enum, null, ...)` NPEs (`Map.of(field, value)` at `Query.java:2024/2028`); the showcase `AddArchivedFlagMigration.java:62` calls exactly `query.set(archived, null, false, true, null)`.
- Conventions: `store()` (minimal tags, `_id` stripped), `findOneAndUpdate` (single value untagged, `_id` stripped) and the writer (tagged, `_id` kept) are three conventions; the planned test `querySetOfSingleEmbeddedKeepsClassNameButFindOneAndUpdateDoesNot` would cement the divergence. Only `class_name` is pinned by `TypeIdTests` (41-61, 118-134); no test pins `_id` of a single embedded `@Entity` on the writer path.
- `marshallIfNecessary` is a public, overridable hook and `AsyncWriterImpl` shows the class is designed for subclassing; `pushPull`/`pushPullAll` would stop calling it.

## Objections and proposed changes

| Rank | Objection | Proposed change |
|---|---|---|
| BLOCKER | Row 18, the WP2 async test and the CHANGELOG claim contradict Q6 (lazy marshalling on the executor). | Either drop the claim and test, or marshal the VALUES eagerly on the caller thread (keys stay lazy): gives a direct IAE, no retries on deterministic errors, no unbalanced pre/post events, copy semantics. Recommended: eager for values; decide before the first commit. |
| BLOCKER | The golden strategy pins known defects (C01 `currentDate`, D01 `pullAll`) so the next fix is a re-record, and the record switch blesses every other diff in the file; the Pooled file is unverifiable in the default InMem-only CI and drifts with server/BSON typing (Pooled decodes ObjectId to `MorphiumId`); ~40 scenarios x 2 drivers as canonical text are not reviewable; `surefire:test` skips resource processing so a freshly recorded file is stale on the classpath; parallel slots race on one file; committed golden files would be new to the repo. | Replace by (1) an in-process differential test: a verbatim copy of the legacy `marshallIfNecessary` (about 50 lines) in the test, an input matrix (String, int, Date, Enum, null, single Item, polymorphic Dog, `List<Item>`, `List<String>`, `Map<String,Item>`, `MorphiumReference`, `@AdditionalData` Item, nested `pushAll` lists), assert equality with the new writer function for every KEEP row and assert the SWITCH rows explicitly; no Docker, no files, no record mode; (2) about 12 explicit-expectation wiring tests, one per entry point (`set`, `push`, `addToSet`, `pushAll`, `addAllToSet`, `pull`, `pullAll`, `set` with upsert on a non-`_id` filter), raw-doc read-back per driver; (3) defect pins stay in `repro/` (the repo's idiom). |
| MAJOR | `pullAll` -> `$pull` defect (`MorphiumBase.java:1301-1303`) would be pinned in a golden file. | Fix it in PR 3 as a separate small commit (call the 6-argument `pullAll(..., null)`, i.e. `pushPullAll`); 2-3 tests: pull two elements, pull embedded elements, the `multiple` default (the 2-arg form uses `multiple=false`). |
| MAJOR | Test expectations: ObjectId vs MorphiumId on Pooled; `pushAll(..., null)` behaviour; async and callback error cases (callback gets the raw exception through `onOperationError`; `AsyncWriterImpl` without callback only logs; `firePreUpdateEvent` already fired so a listener sees an unbalanced pre/post pair); with `retriesOnNetworkError > 1` a deterministic IAE is retried with sleeps. | Adjust tests; pin or note these. |
| MAJOR | `Class` value bug. | `isEntity(o.getClass())` in the walk and the writer; add a test. |
| MAJOR | Three conventions. | Make `findOneAndUpdate` follow the writer for single values (always tagged with `class_name`): the cheaper direction (an extra `class_name` is always readable, dropping tags is not safe for `@AdditionalData`/`Object` fields) and it fixes the PR 1 gap (item F); cost: `UpdateDocumentMapper` uses the element form for every value, `rawPojoShapesInSet`/`UpdateValueMarshallingTest` flip; PR 1 and PR 2 are unreleased. At least do not assert the divergence in a test named after it. Document the `_id` embedded-entity read-back effect. Optionally drop the `isEntity` conditional and always use the element form (a top-level `MorphiumId` in `Query.set` then becomes `ObjectId`, wire-identical on Mongo, InMem comparison tolerant: `QueryHelper` 1503, 2422), leaving one writer code path. |
| MAJOR | Commit order: a standalone red commit stays in `develop` with `--merge` and breaks bisect and per-commit CI. | C1: differential test and wiring tests that are green on the old code; C2: implementation, red tests green, repro flip, docs. If red-first matters: commit red tests as `@Disabled` and enable them in the implementation commit. |
| MINOR | Table corrections above; hook semantics for subclassers (keep a protected `marshallElement` or note in the CHANGELOG); fix the `Query.set(String, null)` NPE in PR 3 (2-line change, use a `HashMap`) and the `Query.set(Enum...)` `toString()` use; Quarkus verification with `-am`; `@WriteBuffer` entities (buffered writer is only selected on a real MongoDB, `Morphium.java:1289`; the CHANGELOG must say the holes remain there); Haiku experiment: the ledger items A-G bookkeeping rerun of WP7a with pre-quoted titles (existing scorer, tests the hypothesis from the trial) is better than the WP4 flip (needs Maven/Docker for scoring); drop the vacuous "external implementors" wording. |

## Decisions needed before an implementation spec can be frozen

1. Eager marshalling of values on the caller thread (recommended) vs. dropping the async-snapshot claim.
2. Differential test + about 12 wiring tests instead of golden files (recommended).
3. Fix `pullAll`, the `Query.set(..., null)` NPE, the `Query.set(Enum...)` `toString()` and the `Class` value in PR 3 (recommended).
4. Converge on the writer convention: single values always tagged also in `findOneAndUpdate`, and optionally always the element form in the writer (recommended for both).
5. Two commits (tests green on old code, then implementation) (recommended).
