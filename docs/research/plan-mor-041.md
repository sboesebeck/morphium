# Implementation plan: MOR-041 key translation for operator maps (PR 2)

Status: DRAFT by the architect (read-only, nothing was run), to be reviewed by the challenger. Paths: `MC` = `morphium-core/src/main/java/de/caluga/morphium`,
`MT` = `morphium-core/src/test/java`, `ARH` = `MC/AnnotationAndReflectionHelper.java`, `Q` = `MC/query/Query.java`.

## 0. Conflicts with the ledger and earlier decisions

1. The MOR-041 ledger text (`docs/tasks/ledger.yaml:728-733`: "including @Aliases, and cover the operands of $rename and $unset") contradicts decision B3 of
   `docs/research/plan-mor-001-002-003.md` (v2: never resolve by `@Aliases`, leave `$rename`/`$unset` untouched). The plan follows B3; WP6 narrows the ledger text and acceptance to:
   "With the switch on, `$set`, `$inc`, `$push` etc. keys given as Java name or Mongo name of a persistent field are stored under the Mongo name; `@Aliases` names, `$unset`/`$rename` keys,
   dotted, positional and `$` keys and unknown keys are written verbatim."
2. MOR-042 (writer path) should share only the value walk, not the key resolver: the writer's `Query.set` uses the strict `getMongoFieldName` (`MorphiumWriterImpl.java:2373`), which resolves
   `@Aliases` through `getField` (`ARH:551-562`), throws on unknown keys (`ARH:370`) and returns dotted keys unchanged (`ARH:339-343`). Moving it to the new resolver would silently change
   alias and unknown-key behaviour. MOR-042 becomes "values only".
3. MOR-006 says the builder resolves "via getMongoFieldName" (`ledger.yaml:142-146`); it should call the new resolver for single-segment names and always translate (its input is Java names by
   contract), independent of the PR 2 switch. It calls `findOneAndUpdateMapped` (`Q:520`) with an already mapped document, which never reads the switch.
4. `MC/aggregation/internal/FieldNameTranslation.java:49-53` already resolves aliases via `getField` and calls itself "the single implementation". After PR 2 there are two resolvers with deliberately
   different alias semantics; leave it alone, state it in the Javadoc of the new method.

## 1. Semantics

### 1.1 Operator table (switch on)

Values are always marshalled by the PR 1 walk, independent of the switch ("switch off = byte-identical to PR 1").

| Operator | Key | Value |
|---|---|---|
| `$set`, `$setOnInsert`, `$inc`, `$mul`, `$min`, `$max` | translated | PR 1 walk |
| `$push`, `$addToSet` | translated (`$each`, `$position`, `$slice`, `$sort` sit inside the value and are untouched) | PR 1 element-context walk |
| `$pull` | translated; the keys of the condition (`{fileName: ...}`) are not (known pitfall MOR-039) | PR 1 walk |
| `$pullAll` | translated | PR 1 walk |
| `$pop`, `$currentDate`, `$bit` | translated | passed through |
| `$unset` | untouched | untouched |
| `$rename` | untouched (key and target string) | untouched |
| replacement document (no `$` key) | untouched (`UpdateDocumentMapper.java:45-47`); auto-`$set` is MOR-013 and must run the resolver on the wrapped map | PR 1 |

Why `$unset`/`$rename` stay raw: their job is to act on stored names including stray and legacy ones. After switching on, `{$unset:{expiresAt:""}}` is the only way to delete the stray
`expiresAt` that earlier raw writes created; `$rename` is the migration tool for the same fields. Same argument for never resolving `@Aliases` (an alias is a legacy stored name; see
`ComplexObject`/`AliasesEntity` in `MT/de/caluga/test/mongo/suite/data`). Consequence to document: `$set` is translated, `$unset` is not.
Always passed through: unknown keys, dotted keys (`a.b`), positional keys (`$`, `$[]`, `$[x]`), keys starting with `$`, null or empty keys. Keys inside a Map value are data and never translated.

### 1.2 Resolver return values

Let M be the set of names `serialize()` writes for a class (with `_id` for the `@Id` field), J the Java field names.

| Input | Output |
|---|---|
| Java name of a persistent field | its stored name (`ownerName` -> `owner_name`, `id` -> `_id`, `@Property(fieldName="p")` -> `p`, `@Reference(fieldName="r")` -> `r`) |
| a stored name (member of M) | itself |
| an `@Aliases` name | itself (never resolved) |
| snake_case form of a Java name under a non-translating entity | itself (`getMongoFieldName` would rewrite it back via `getField` rule 6, `ARH:576`; the update resolver does not) |
| `@Transient`, `@AdditionalData`, static, `@IgnoreFields`/`@LimitToFields`-excluded fields | not in M, unchanged |
| unknown, dotted, `$...`, null, empty | itself |

The resolver never throws. `@Version(fieldName=...)`: the resolver follows the stored shape; `getFields` (`ARH:996-1004`) honours only `@Reference` and `@Property`, and `serialize` writes under that
list (`ObjectMapperImpl.java:536,560-564,627-628,725`), whereas `getMongoFieldName` honours `@Version.fieldName` (`ARH:389-395`), so filter and store may already disagree for a custom-named
`@Version` (read, not run; no test uses it). Separate defect candidate; the parity test pins whatever `serialize()` does.

### 1.3 Nested and dotted paths: recommendation N1, not in PR 2

- N1 (recommended): dotted keys verbatim (matches the decision, `getMongoFieldName` `ARH:339-343` and today's `Query.set`). `{$set:{"child.fileName":..}}` still writes a stray field; filter and update
  disagree on single-embedded paths because `Query.f` translates them (`Q:1007-1045`). Document and pin with a test.
- N2 first segment only: half-translated paths look right but are wrong for default-naming embeddeds; violates "dotted passes through". Reject.
- N3 full segment-by-segment by declared types: failure modes: Map-typed segments (next segment is data); List/Set/array segments need the generic element type (raw types, `List<T>`,
  `List<List<X>>`); positional/numeric segments keep the current type; polymorphic embeddeds (declared base type, `class_name` decides at runtime); `@Reference` segments; `$[x]` needs arrayFilters
  (`findOneAndUpdateMapped` sets none, `Q:544-554`) and their keys would need translating in the same change; MOR-015 INHERIT mode needs owner context; `Query.f` itself discards all segments after a
  List/Map segment (`Q:1032,1041-1042`).
Follow-up task "Nested path translation shared by `Query.f` and the update builder" after MOR-006 (JUDGEMENT/L).

## 2. Configuration switch

- Name `translateUpdateFieldNames`, in `MC/config/ObjectMappingSettings.java` next to `translateAggregationFieldNames` (`:13`, `:95-112`): field, `isTranslateUpdateFieldNames()`, `setTranslateUpdateFieldNames(boolean)`,
  `enable...()`, `disable...()`. Picked up by reflection (`Settings.asProperties`, `MC/config/Settings.java:76-86`; `MorphiumConfig.java:191-202`).
- Default `false` for this release: precedent is the opt-in aggregation switch ("Default off = exactly the previous behavior", `CHANGELOG.md:1952`); the change rewrites stored keys for raw callers who may
  depend on stray fields (`@AdditionalData`, raw reads, other services, shell scripts). Callers already passing Mongo names are unaffected (idempotent). Path: this release default false (+ optional
  WARN-once-per-class-and-key diagnostic), next minor flip to true under "Changed" with the opt-out kept; per-query override covers migration code.
- Per-query override `Query#setTranslateUpdateFieldNames(Boolean)` (null = config), mirrors `Aggregator.setTranslateAggregationFieldNames(Boolean)` (`MC/aggregation/Aggregator.java:501-517`,
  `AggregatorImpl.java:61-69`); `Q.clone()` is shallow (`Q:1947`) so the Boolean is copied.
- Read per call in `Q` (`override != null ? override : config`), passed as `new UpdateDocumentMapper(morphium, type, translateKeys)` at `Q:443` and `Q:512` (not snapshotted).
- Docs row in `docs/configuration-reference.md` "Object Mapping Settings" (`:236-242`). Test warning: do not assert the setting through `Settings.equals` (returns true at the first identical field,
  `Settings.java:38`); use the getter and a properties round trip.

## 3. The resolver

Public method on `ARH` (owns `ccc`, `getFields`, `getAllFields`, the caches; `Q` is in another package):

```java
public String resolveStoredFieldName(Class<?> type, String key)   // never throws
```

Index: `ConcurrentHashMap<Class<?>, StoredNames>` with immutable `StoredNames(Set<String> mongoNames, Map<String,String> javaToMongo)`, `computeIfAbsent` on `getRealClass(type)`; separate from
`fieldNameCache` (`ARH:44`, which writes with `cls` and reads with `clz`, `ARH:345` vs `:411`). Build: (1) `stored = new HashSet<>(getFields(C))` (applies `@Transient`, `@IgnoreFields`,
`@LimitToFields`, `tcc && ccc`; `ARH:937-942,1014-1016`); (2) for each `Field f` in `getAllFields(C)` (`ARH:476`): skip static, skip `@AdditionalData`, `n = storedName(f, tcc)`, skip if null or
not in `stored`, `m = f.isAnnotationPresent(Id.class) ? "_id" : n`, `mongoNames.add(m)`, `javaToMongo.putIfAbsent(f.getName(), m)`; (3) `javaToMongo.keySet().removeAll(mongoNames)`. Extract
`storedName(Field, boolean tcc)` and `translatesCamelCase(Class)` from `getFields` (`ARH:937-942,996-1016`); the parity test is the safety net.

Resolution: `key == null || empty || contains '.' || starts with '$'` -> key; `mongoNames.contains(key)` -> key; else `javaToMongo.getOrDefault(key, key)`. Flags: global `ccc=false` yields identity; `@Entity(translateCamelCase=false)` yields Java names;
for `@Embedded` types the embedded's own flag is used (entity wins when both exist, `ARH:942`).

Idempotency by construction: result is always in `mongoNames` or the unchanged input; invariants `values(javaToMongo) subset of mongoNames` and `keys(javaToMongo) intersect mongoNames = empty`; proof: if `x in M`
then `f(x)=x`; if `x in keys(javaToMongo)` then `f(x) in M`; otherwise `f(x)=x`. Assert the invariants at index build. The existing `getMongoFieldName`/`getField` chain is not idempotent in general
(`ARH:538-585` walks fields in declaration order).

Pathological cases: `owner_name` and `ownerName` both declared, translating entity: both store as `owner_name` (serialize logs "shadowed", `ObjectMapperImpl.java:619-623`); `mongoNames={owner_name}`,
both keys map to it. Same pair, non-translating: two stored names, identity. A: Java `x` with `@Property("y")`, B: Java `y` plain non-translating: both store as `y`; `x`->`y`, `y`->`y`. A: `x`->`@Property("y")`,
B: `y`->`@Property("z")`: `mongoNames={y,z}`, key `y` means A's stored name `y` (Mongo name wins); `Query.f("y")` is order-dependent here (`getField` goes field by field), the update is deterministic.

Thread safety: ConcurrentHashMap + immutable value; no handling of toggling `ccc` after first use (the other caches, `ARH:917`, `:88-99`, have the same contract). Pre-existing: the one-arg constructor passes a plain
`HashMap` as `realClassCache` (`ARH:63`), written by `getRealClass` (`ARH:300`); file a small fix, not in this PR.

Filter and update must agree for every Java or stored name of a top-level persistent field (test 19). Deliberate differences: `@Aliases` (filter resolves, update does not); snake form under a non-translating
entity (filter rewrites to the Java name, update leaves it); unknown keys (filter throws, update passes); dotted paths; the pathological collision case.

## 4. Interaction with other tasks

- PR 1 values untouched; `rejectEntityForReference(operator, key, value)` keeps using the raw key (`UpdateDocumentMapper.java:63-65`, uses `getField`, sees aliases); only the output key changes.
- M1 (typed serialisers per resolved field) deferred to MOR-006: using `serializeIterable(v, fld.getType(), fld.getGenericType())` now would couple the value shape to the switch.
- `addGeneratedIdOnInsertIfNeeded` runs on the mapped update (`Q:512` -> `Q:533`; checks at `Q:611-619`): with the switch on, `$setOnInsert:{id:X}` becomes `_id` and pins the id (today `id` is stored
  as a stray field and a second generated `_id` is added); `$set:{id:..}` on an existing document with a different `_id` becomes a server error.
- Key collisions: with the switch on, two input keys that resolve to the same output key in one operator throw an `IllegalArgumentException` naming both (alternative: last wins + warning like
  `FieldNameTranslation.translateKeys`, `:68-77`, silently loses data). Mapping happens before a connection is acquired.
- MOR-009: inject auto values into the already mapped document; the "unless the update already touches the version field" test works only on translated names, so the resolver runs first (check `$set`, `$setOnInsert`,
  `$inc`, `$unset`); idempotency makes injected values (built with `getMongoFieldName` as `handleLastChange`, `MorphiumWriterImpl.java:2127-2133`) safe.
- MOR-006 reuses `UpdateDocumentMapper` and `findOneAndUpdateMapped` with `translateKeys=true` always. MOR-042: values only. MOR-015/014: top-level keys depend only on the entity's flag; hypothesis 2 stays
  (`EmbeddedCamelCaseReproductionTest` keeps passing). Other `getMongoFieldName(..., ignoreUnknownField=true)` callers (`MorphiumBulkContext.java:59`) rewrite unknown snake_case keys to camelCase
  (`ARH:362-364`): out of scope. The Quarkus `MorphiumMigrationRunner` uses `Query.set` (writer path), not touched.

## 5. Work packages

WP0: docker `mongo:8` on a free port, baseline run of `UpdateValueMarshallingTest`, `FindOneAndUpdateCompoundFilterUpsertTest`, `UpsertFilterInspectorTest`, `PojoInSetOperatorReproductionTest`,
`EmbeddedCamelCaseReproductionTest`, `AliasesTest`, `AnnotationAndReflectionHelperTest`, `ObjectMapperTest`, `AggregatorFieldNameTranslationTest`, `MorphiumConfigTest`; use JDK 21 (`ReferenceCascadeTest` fails on 25).

| WP | Content | Rating |
|---|---|---|
| WP1 | config setting + docs row + `MorphiumConfigTest` (`translateUpdateFieldNamesDefaultsToFalse`, properties round trip via getter) | MECHANICAL |
| WP2 | tests first (below), red until WP3/WP4 | JUDGEMENT |
| WP3 | resolver in `ARH` | JUDGEMENT |
| WP4 | wiring: `UpdateDocumentMapper` 3-arg constructor, `RAW_KEY_OPERATORS = {$unset,$rename}`, key resolution + collision detection in `map()` (`:60-68`), `Q` field/getter/setter + helper `newUpdateMapper()` at `:443`/`:512`, Javadoc | JUDGEMENT (medium) |
| WP5 | optional dry-run WARN once per (class,key) when the switch is off and the resolver would change a key (try/catch around the resolver) | JUDGEMENT (small) |
| WP6 | Javadoc, CHANGELOG ("Added"), ledger edits (narrow MOR-041/042/006; file: nested path translation, `@Version(fieldName)` mismatch, `realClassCache` HashMap, `Settings.equals`; quote YAML titles containing `: `) | MECHANICAL with prose |
| WP7 | verification: new classes + baseline on `--driver all`, `core` tag on inmem, `git diff --stat` | MECHANICAL, reviewer-run |

WP2 tests: `StoredFieldNameResolverTest` (pure JUnit, `@Tag("core")`, `new AnnotationAndReflectionHelper(true/false)`): java name -> snake_case; stored name unchanged; `id` -> `_id`; `@Property` both directions;
`@Reference(fieldName)`; alias not resolved; non-translating entity keeps Java name (and documents `getMongoFieldName("owner_name")` differs); pass-through keys (unknown, `a.b`, `$`, `$[x]`, `$[]`, `$set`, "", null);
transient/additional-data/static pass through; `@Version` matches the serialized key; idempotency over `mongoNames` + javaNames + pseudo-random strings; duplicate `owner_name`/`ownerName`; Mongo name of one field equals
Java name of another; global camelCase off; concurrent first use (16 threads, many fresh classes). `UpdateDocumentMapperKeyTest` (package `de.caluga.morphium.query`, InMem-only mapping): operator table over all
15 operators with exact output (`$push` keeps `$each/$sort/$position`, `$pull` keeps condition keys), `translateKeys=false` equals PR 1, replacement document untouched, collision throws, idempotent, caller's map not mutated.
`UpdateKeyTranslationTest` (`MultiDriverTestBase`, both drivers, switch via `m.getConfig().objectMappingSettings().setTranslateUpdateFieldNames(true)` as `AggregatorFieldNameTranslationTest:218`; fixtures `Lease` default
`@Entity` with `ownerName`, `expiresAt`, `retryCount`, `tagList`, `@Property(fieldName="custom_x")`, `@Aliases("legacy_owner")`, `@Version Long entityVersion`; `RawLease` non-translating with default `@Embedded Child{fileName}`),
21 tests: set camelCase writes mapped field; stored name unchanged; switch off writes the raw key verbatim (current behaviour); per-query override wins over config (true/false/null); non-translating entity keeps keys;
embedded default under non-translating entity: nested keys unchanged by this PR and `"defaultChild.fileName"` writes a stray `fileName` (documented N1 gap); `@Property` honoured; `$setOnInsert:{id}` -> `_id`;
unknown key passes through; dotted/positional pass through; alias key not resolved and `$unset` of it works; `$unset`/`$rename` keys not translated (stray `ownerName` seeded with switch off, removed with switch on);
`$inc/$mul/$min/$max` keys translated; `$push/$addToSet/$pull/$pullAll/$pop` keys translated; `$currentDate/$bit` keys translated (InMem support unverified, `assumeFalse` if needed); collision rejected before a
connection; one-argument overload translates; lease scenario with Java-named keys (first acquire, renewal, held lease E11000 on a real MongoDB); filter and update agree on every persistent field; resolver names equal the
keys of `mapper.serialize(fullyPopulated)` for `Lease` and `RawLease`; value shape unchanged by the switch.

Haiku suitability: WP1 (copy of `ObjectMappingSettings.java:95-112` + compile + test check; review the diff; no CHANGELOG/Javadoc by Haiku) and optionally the `operatorTable_...` matrix once the table in 1.1 is frozen,
with the pre-registered mutation check. Not Haiku: WP2 integration expectations, WP3, WP4, all WP6 prose/ledger edits.

## 6. Risks, CHANGELOG list, open questions, unverified

Risks: switch on writes to different fields (stray fields stop appearing; existing stray data stays and needs `$unset`/`$rename`); the resolver is only as good as `getFields` (cache staleness after toggling `ccc`;
the quirk that `@Transient` + `@Property` is still listed, `ARH:996-1008`); polymorphic top-level queries (subclass fields not in the base class index, Java names pass through); first use per class builds an index by
reflection; filter and update still disagree on dotted embedded paths.
CHANGELOG list (switch on): Java-name keys reach the real field; `id` -> `_id` (pins the id, suppresses the generated `$setOnInsert._id`); `@Property`/`@Reference` Java names translated; key collisions throw;
`$unset`/`$rename` keys, `@Aliases`, dotted, positional, `$` and unknown keys and replacement documents unchanged; default off changes nothing (plus optional WARN); new public API `ARH.resolveStoredFieldName`,
`Query#setTranslateUpdateFieldNames`, `ObjectMappingSettings.translateUpdateFieldNames`.
Open questions: name and default (false now, flip next minor?); collision policy (throw recommended vs. last-wins + warn); include the WP5 dry-run WARN?; narrow MOR-041/042/006 as in section 0?;
`@Version(fieldName)` follow the store (recommended) or the filter?; nested path translation as a separate task after MOR-006 (recommended)?; keep the per-query override?
Not verified: InMemoryDriver support for `$bit`, `$mul`, `$min`, `$max`, `$pullAll`, `$pop`, `$currentDate` via `findAndModify`; `@Version(fieldName=...)` behaviour on store and the `@Transient`+`@Property` quirk
(read, not exercised); property parsing for the new boolean (read through `Settings`/grep only); ArchUnit/dependency rules between `query` and `ARH`; `SingleMongoConnectDriver`, replica sets, PoppyDB; whether
`morphium-jakarta-data`/Quarkus/Spring call `getMongoFieldName`-style translation in ways that interact; the claim that no production code outside `Query` calls `findOneAndUpdate` is taken from plan v2, not re-grepped.

---

# Review outcome (challenger verdict: go with changes)

Both reviews ran on a Sonnet-class model (the default model of the architect and challenger agents was not available). The challenger read the code; its line claims were not re-run by the
orchestrating session. No implementation exists for PR 2 yet.

## Corrections to the plan above (accepted)

- `ObjectMapperImpl.java:725` is a comment; the write is `dbo.put(fName, v)` at `:768`. `MorphiumConfig` is `de/caluga/morphium/MorphiumConfig.java`; the property loop is `:185-205`. `MorphiumBulkContext` is `bulk/MorphiumBulkContext.java`.
  `AggregatorFieldNameTranslationTest` is an InMem-only test (`suite/inmem/`), so it is not a precedent for a `MultiDriverTestBase` test.
- `Query.f` falls back to the WHOLE original string after a List/Map segment (`Query.java:1041-1042`), it does not discard only the later segments; and it returns the raw string when the class has `@AdditionalData` (`:998-1005`).
- The plan's section 1.1 table says `$pop`/`$currentDate`/`$bit` values are "passed through", but `UpdateDocumentMapper.mapValue` marshals every operator's value (harmless for those values).
- MOR-042 is already "values only" in the ledger; nothing to narrow there.
- "Never throws" is false: `getAllFields` on an interface type NPEs (`ARH:489-492`); the resolver needs a catch-all that falls back to identity.
- The parity claim "resolver names equal the keys `serialize()` writes" is false for `@ReadOnly` fields (skipped by `serialize`, `ObjectMapperImpl:576`) and the planned fixtures (`Lease`, `RawLease`) make the test nearly vacuous.
- The snake form of a Java name (`my_id` for `@Id myId`, `owner_name` for `@Property("p") ownerName`) is accepted by `getField` rule 6 (`ARH:576`) but not resolved by the planned index: add it to the list of deliberate filter/update differences.
- `getFields` lets `@Entity` win over `@Embedded` (`ARH:942`) while `getMongoFieldName` ORs them (`ARH:405-406`); `getFields` strips `$` only when not `tcc && ccc` (`ARH:1012` vs `:1015`).
- InMemoryDriver has `case` labels for `$currentDate`, `$mul`, `$rename`, `$min`, `$max`, `$pullAll`, `$pop`, `$bit` (`InMemoryDriver.java:9181-9588`); `CurrentDateReproductionTest:137` already runs `$currentDate` through `findOneAndUpdate` on both drivers.
  The `assumeFalse` guards are probably unnecessary; `$bit` needs an Integer/Long field.
- Enum keys: value maps accept enum keys via `name()` but `UpdateDocumentMapper.map` casts operator-map keys to `String` (ClassCastException on an enum key).
- `Morphium.java:195` uses the one-argument `AnnotationAndReflectionHelper` constructor, i.e. the plain `HashMap` realClassCache (`ARH:63`, `:288-301`) is on the production path.

## Objections and proposed changes

| Rank | Objection | Proposed change |
|---|---|---|
| BLOCKER | `$unset`/`$rename` raw while `$set` translates: `{$unset:{expiresAt:""}}` silently unsets a non-existent stray and leaves the real field set (stuck lease, `nModified=0`, no error); `{$set:{ownerName:a},$unset:{ownerName:""}}` is rejected by the server today and would become a silent partial success; the `$rename` target stays a Java name and writes a new stray. The plan's own per-query override is already the stray-cleanup escape. The ledger acceptance of MOR-041 explicitly lists `$rename`/`$unset`; rewriting it to close the task is a governance smell. | Translate `$unset` keys and both `$rename` source and target with the same idempotent resolver; stray cleanup via `setTranslateUpdateFieldNames(false)` or the raw driver; keep aliases unresolved; split the ledger task instead of narrowing it. Alternative if raw is kept: throw `IllegalArgumentException` when the resolver would change an `$unset`/`$rename` key. |
| MAJOR | A default-false switch has a permanent two-state matrix, the headline fix is invisible by default, and the flip is unrealistic: the aggregation switch was added in 6.2.9 (2026-07-14), the repo is at 6.3.7-SNAPSHOT and `ObjectMappingSettings.java:13` is still `false`. It contradicts other-clients.md pattern 17 / MOR-006 acceptance / PR 1's own Javadoc ("raw by contract"). Every sibling write path (`MorphiumWriterImpl` set/inc/push/pull/unset, `MorphiumBulkContext`, `Query.f`) already translates unconditionally and strictly. | Decide between (a) always translate with a kill switch (default true) plus per-query override, shipped together with PR 1 under `[Unreleased]`, and (c) no PR 2, translate only in the MOR-006 builder. If default false stays: file a ledger task with a concrete flip trigger and keep WP5 (dry-run WARN) to get data. Keep exactly one control, not global + per-query + WP5. |
| MAJOR | Dotted paths untouched (N1) although `$set:{"address.zipCode":x}` is the canonical partial update and MOR-006's description requires nested paths. N2 (first segment only) is no worse than N1 (always wrong for the child, right for the first segment). | N4: extract the segment walk from `Query.f` (`Query.java:1007-1047`) into a lenient `ARH.resolveFieldPath(Class,String)` (never throws, falls back to the whole path as `f` does at `:1041-1042`) so filter and update agree by construction; at minimum WARN once when a dotted key's first segment would change. |
| MAJOR | Index derivation: refactoring `getFields` (the core of serialize/deserialize) in a PR that promises "switch off = byte-identical", with a parity test on two fixtures. | Mirror `serialize` without refactoring: for each name `n` in `getFields(C)`, `fld = getField(C,n)`, skip static and `@AdditionalData`, `stored = @Id ? "_id" : n`, `mongoNames.add(stored)`, `javaToMongo.putIfAbsent(fld.getName(), stored)`; optionally add `convertCamelCase(fld.getName())` as an extra key; corpus-based parity test over all `@Entity`/`@Embedded` classes in `suite/data` plus fixtures for `@ReadOnly`, `@Transient`, inherited/shadowed, `@IgnoreFields`/`@LimitToFields`, `@AdditionalData`, `@Id`+`@Property`; try/catch around index construction with identity fallback. |
| MAJOR | `@Encrypted` and `@ReadOnly`: a Java-name `$set` used to write a harmless stray; now it hits the real field. A plaintext `$set` on an `@Encrypted` field creates a document that throws on every read (base64/decrypt, `ObjectMapperImpl:1280-1300`) and bypasses encryption; `@ReadOnly` fields are skipped by `serialize` (`:576`). | Reject `@Encrypted` targets with `IllegalArgumentException` like PR 1 does for `@Reference` (+ test); document `@ReadOnly`. |
| MAJOR | Collision policy: callers who worked around the bug by passing both `ownerName` and `owner_name` become runtime failures (and an eventual default flip breaks them). | Merge silently when the values are `Objects.deepEquals`, throw only when they differ; add a cross-operator case (`$set:{ownerName}` + `$inc:{owner_name}`); the test cannot assert "before a connection is acquired" without a connection-counting wrapper, assert the exception only. |
| MAJOR | The one-argument constructor's plain `HashMap` realClassCache is on the production path, yet the plan defers its fix and adds a 16-thread first-use test that exercises the race. | One-line fix `new ConcurrentHashMap<>()` in the one-arg constructor in this PR (as MOR-040 did for `fieldNameCache`); concurrency test with fresh ARH instances plus a barrier on the same class. |
| MINOR | Id type: `$setOnInsert:{id:X}` becomes `_id` and pins the id; a `String` X for a `MorphiumId` `@Id` writes an `_id` the mapper cannot read back; enum keys in operator maps; `findOneAndUpdateEnums` is a replacement document (untouched by PR 1 and PR 2, say so); polymorphic base-type queries do not know subclass fields; the resolver result type `String` hides "unknown" (consider `Resolution(key, kind)` for WP5 and a strict builder mode); the "filter and update agree" test must carve out `@Id`/`@Property` snake forms; "switch off writes raw key" pins a bug and must set the switch explicitly; missing tests: runtime toggle, `ccc=false` end-to-end, `@Embedded` elements in Lists, field-name-like keys in Map values, upsert + `$setOnInsert:{id}` with a non-matching type, reference rejection under alias/snake keys, `MorphiumConfig` JSON and `copy()` round trip, cross-operator path conflict on both drivers, `@Encrypted`/`@ReadOnly`, `$unset`/`$set` same-name case, dual-annotated class. | See the table; adopt in the implementation spec. |

## Haiku recommendation (challenger)

WP1 (settings class + test + docs row) is extrapolated beyond the trial evidence: the trial's WP1 was a note transcription (23/23 x3 on a spec containing every fact) while WP7a with prose scored 38-40/43 with invented content; the
`operatorTable_` matrix has no evidence at all; WP1 depends on the still-open design decisions. Acceptable only as a scripted experiment (diff limited to listed files, compile + `MorphiumConfigTest` as scorer, no docs row by Haiku);
the traps (`Settings.equals` returns true at the first identical field, `asProperties` omits default values) argue against it. Do not freeze its spec before the decisions below.

## Decisions needed before an implementation spec can be frozen

1. Scope/default: (a) always translate + kill switch (shipped with PR 1), (c) builder-only, or (b') the plan's default-false switch with a flip task.
2. `$unset`/`$rename`: translate (challenger BLOCKER fix) or keep raw with a loud error.
3. Dotted paths: lenient `resolveFieldPath` shared with `Query.f` (N4) or N1 with a WARN.
4. Collision policy: merge on equal values, throw on differing values.
5. Whether PR 2 also carries the `realClassCache` fix and the `@Encrypted` rejection (both recommended).

---

# Plan v2: decisions of the maintainer applied (supersedes the matching parts above)

Maintainer decisions (2026-10-15): always translate with a kill switch; translate `$unset` and `$rename`; `resolveFieldPath` for dotted keys; collision throws only for differing values; the `realClassCache` fix and the
rejection of encrypted fields are part of the PR. Everything not mentioned here stays as in the plan above after the corrections of the review outcome. PR 1 is still unreleased, so PR 1 and PR 2 ship together under `[Unreleased]`.

## D1. Scope and switch

- PR 2 always translates keys of operator maps (all operators, see D2). Kill switch: `ObjectMappingSettings.translateUpdateFieldNames`, **default `true`**; `false` restores the PR 1 behaviour (keys verbatim, values marshalled).
  Properties/JSON: because `asProperties` omits default values, the round-trip test sets `false`.
- Per-query override `Query#setTranslateUpdateFieldNames(Boolean)` stays (null = config): it is the escape for cleaning stray fields, since `$unset` is now translated too. The WP5 dry-run diagnostic is dropped (nothing to flip).
- The builder (MOR-006) calls the mapper with `translateKeys=true` regardless of the switch.
- PR 1's Javadoc ("Field names are not translated"), its CHANGELOG entry and its class comment in `UpdateDocumentMapper` are rewritten in WP6; a CHANGELOG "Changed" note says Java-named keys now reach the real field.

## D2. Operator semantics (switch on)

| Operator | Key | Value |
|---|---|---|
| `$set`, `$setOnInsert`, `$inc`, `$mul`, `$min`, `$max`, `$push`, `$addToSet`, `$pull`, `$pullAll`, `$pop`, `$currentDate`, `$bit`, `$unset` | `resolveFieldPath` | PR 1 walk (`$unset` value passed through) |
| `$rename` | `resolveFieldPath` for the key AND for the target string value (non-String targets pass through) | - |
| replacement document | untouched (auto-`$set` is MOR-013) | PR 1 |

Aliases are never resolved. Keys of Map values and `$pull` condition maps are data and are not translated (MOR-039). Enum keys in operator maps are accepted (`name()`), consistent with value maps.
A document that sets and unsets the same Java name in one update (`$set:{ownerName}` + `$unset:{ownerName}`) now maps both to `owner_name` and the server rejects it with a path conflict, as it did before for the identical raw keys.

## D3. Resolver (revised: no `getFields` refactor)

`ARH.resolveStoredFieldName(Class<?> type, String key)` (single segment) and `ARH.resolveFieldPath(Class<?> type, String path)` (dotted), both public, never throw (a catch-all returns the input; interface types and
`getAllFields` NPE, `ARH:489-492`).

Index per class (`ConcurrentHashMap<Class<?>, StoredNames>`, immutable value, `computeIfAbsent` on `getRealClass`), derived the way `serialize` iterates: for each name `n` in `getFields(C)` take `fld = getField(C, n)`
(`ObjectMapperImpl:536,564`), skip null, static and `@AdditionalData` fields, `stored = fld has @Id ? "_id" : n`, `mongoNames.add(stored)`, `storedType.put(stored, fld.getType())`,
`javaToMongo.putIfAbsent(fld.getName(), stored)`, and when the class translates camelCase also `javaToMongo.putIfAbsent(convertCamelCase(fld.getName()), stored)` (parity with `getField` rule 6, `ARH:576`, so the filter and
the update agree on the snake form). Afterwards `javaToMongo.keySet().removeAll(mongoNames)` (a Mongo name always beats a foreign Java name). `@ReadOnly` fields are included (an explicit update may write them; `serialize` does not);
`getFields` is NOT refactored. Index construction is wrapped in a try/catch that falls back to identity.

`resolveStoredFieldName`: null, empty, containing `.`, or starting with `$` -> unchanged; `mongoNames.contains(key)` -> key; else `javaToMongo.getOrDefault(key, key)`. Idempotency by construction as before (invariants asserted
at index build). Aliases never resolve.

`resolveFieldPath(type, path)` (lenient version of the segment walk in `Query.f`, `Query.java:1007-1047`):
- no `.` -> `resolveStoredFieldName`;
- otherwise split on `.`; `clz = type`; for each segment: if `clz` is null/`Object`/unknown, or the segment is positional or numeric (`$`, `$[]`, `$[x]`, digits) or starts with `$`, or empty, append the segment and all remaining
  segments verbatim and stop; else resolve the segment with the index of `clz`, append the result, and set `clz` to the declared type of that stored name only if it is a single `@Embedded`/`@Entity` type (not a
  Collection/array/Map/`Object`, not a `@Reference` field, not an interface/abstract polymorphic base without index); otherwise stop and append the remaining segments verbatim;
- the embedded class's own `translateCamelCase` flag applies per segment automatically because each segment is resolved with the index of the class it belongs to (hypothesis 2 behaviour for nested keys is kept);
- it differs from `Query.f` on purpose where `f` falls back to the raw whole string (after a List/Map segment `f` returns the raw path, also the first segment; with `@AdditionalData` `f` returns the raw string): the update translates
  the leading segments it can know. A new ledger task (MOR-043) makes `Query.f` use `resolveFieldPath`; until then filter and update disagree for List/Map-leading paths and the test documents it.
- polymorphic embedded subclasses: fields of a subclass are not in the base class index and pass through verbatim (documented).

## D4. Collisions and conflicts

After resolving all keys of one operator map: if two input keys map to the same output key and `Objects.deepEquals` holds for their (marshalled) values, one entry is kept; otherwise `IllegalArgumentException` naming both keys.
Conflicts across operators (e.g. `$set:{ownerName}` + `$inc:{owner_name}`) are left to the server (path conflict, code 40); the test runs on a real MongoDB only because the InMemoryDriver may accept it.

## D5. Guards

- `@Encrypted`: for the value-writing operators (`$set`, `$setOnInsert`, `$push`, `$addToSet`, `$pull`, `$pullAll`, `$inc`, `$mul`, `$min`, `$max`, `$bit`) a key that resolves (via `getField` on the raw key or via the resolver) to an `@Encrypted`
  field is rejected with `IllegalArgumentException` (a plaintext value would bypass encryption and break every read, `ObjectMapperImpl:1280-1300`); `$unset`, `$rename`, `$currentDate` are allowed. The check does not depend on the switch.
  `@ReadOnly` is documented, not rejected.
- `@Reference` rejection of PR 1 keeps using the raw key.
- `$setOnInsert:{id:X}` becomes `_id` and pins the id; a String X for a `MorphiumId` `@Id` is not coerced (new ledger task MOR-045); the CHANGELOG states it.

## D6. Cache fix

`new AnnotationAndReflectionHelper(boolean)` passes `new ConcurrentHashMap<>()` as `realClassCache` (`ARH:63`, written by `getRealClass` `ARH:288-301`; production path `Morphium.java:195`).

## D7. Work packages (revised)

| WP | Content | Rating |
|---|---|---|
| WP0 | docker mongo:8 on a free port, JDK 21 (Byte Buddy), baseline of the PR 1 classes + `AliasesTest`, `AnnotationAndReflectionHelperTest`, `ObjectMapperTest`, `AggregatorFieldNameTranslationTest`, `MorphiumConfigTest`, `EncryptedObjectMappingTests` | MECHANICAL |
| WP1 | `realClassCache` fix + test (concurrent first use with a barrier on one class, fresh helper instances) | MECHANICAL, do directly |
| WP2 | config setting (default `true`) + docs row + `MorphiumConfigTest` (default true; properties and JSON round trip with `false`; `copy()`) | MECHANICAL |
| WP3 | tests first (D8), red until WP4/WP5 | JUDGEMENT |
| WP4 | resolver and index in `ARH` (D3), Javadoc contrasting it with `getMongoFieldName`, `getField` and `FieldNameTranslation` | JUDGEMENT |
| WP5 | wiring: `UpdateDocumentMapper(morphium, type, translateKeys)`, key resolution for all operators incl. `$unset`/`$rename` key and target, enum keys, collision rule (D4), `@Encrypted` guard (D5); `Query` field/getter/setter + helper at the two call sites | JUDGEMENT |
| WP6 | Javadoc, CHANGELOG ("Changed" + rewrite of PR 1's entry), ledger (below) | MECHANICAL with prose, stronger model |
| WP7 | verification: new classes + baseline on `--driver all` against docker mongo:8, `core` subset on inmem, `git diff --stat` | MECHANICAL, reviewer-run |

No Haiku package in PR 2: the only mechanical candidates are WP1 (one line) and WP2 (copy of `ObjectMappingSettings.java:95-112`), too small to measure; the trial gives no evidence for test matrices or prose.

## D8. Tests (concrete, replaces WP2 of the plan above)

- `StoredFieldNameResolverTest` (pure JUnit, `@Tag("core")`): java name -> snake_case; stored name unchanged; `id` -> `_id`; snake form of `@Id`/`@Property` fields resolves like `getField` rule 6; `@Property`/`@Reference` both directions; alias not resolved; non-translating
  entity keeps Java names; pass-through keys (unknown, `$`, `$[x]`, `$[]`, `$set`, "", null); transient/additional-data/static pass through; `@ReadOnly` resolves; idempotency over `mongoNames` + javaNames + pseudo-random strings; duplicate
  `owner_name`/`ownerName`; Mongo name of one field equals the Java name of another; alias equal to another field's name follows `serialize`; global camelCase off; interface type returns identity; parity corpus: every `@Entity`/`@Embedded`
  class under `suite/data` plus fixtures for `@ReadOnly`, `@Transient`, inherited/shadowed fields, `@IgnoreFields`/`@LimitToFields`, `@AdditionalData`, `@Id`+`@Property`: keys of `serialize(populated)` minus `class_name` are a subset of the
  index names and equal it except for `@ReadOnly`.
- `ResolveFieldPathTest`: single embedded `child.fileName` (default-naming embedded under a non-translating entity -> `child.file_name`; embedded with its own flag false -> `child.fileName`); two-level embedded; List/Set/array/Map leading segment
  (`tagList.0.x` -> `tag_list.0.x`, rest verbatim); positional (`items.$.name`, `items.$[x].name`, `items.$[].name`); `@Reference` leading segment; `Object`-typed segment; unknown first segment; polymorphic subclass field verbatim; idempotent.
- `UpdateDocumentMapperKeyTest` (package `de.caluga.morphium.query`): operator table over all 14 operators with exact output (`$push` keeps `$each/$sort/$position`, `$pull` keeps condition keys, `$rename` key and target, `$unset` key), enum keys, `translateKeys=false`
  equals PR 1 output, replacement document untouched, collision with equal values merges, with differing values throws, idempotent, caller's map not mutated, `@Encrypted` rejected for `$set` and allowed for `$unset`.
- `UpdateKeyTranslationTest` (`MultiDriverTestBase`, both drivers; fixtures `Lease` default `@Entity` and `RawLease` non-translating with default `@Embedded Child`): set camelCase key writes the mapped field (no stray);
  stored name unchanged; kill switch false writes the raw key verbatim (switch set explicitly); per-query override wins over config (true/false/null); non-translating entity keeps keys; dotted embedded key `defaultChild.fileName`
  writes the same field as `defaultChild` value does; `@Property` honoured; `$setOnInsert:{id}` -> `_id` and `countAll()==1`; unknown key passes through; positional keys pass through; alias key not resolved; `$unset` of a Java name removes
  the real field (the lease-release scenario) and with the override false removes the stray; `$rename` key and target translated; `$inc/$mul/$min/$max/$bit/$pop/$pullAll/$currentDate` keys translated (InMem labels exist,
  `InMemoryDriver.java:9181-9588`); `$push/$addToSet/$pull` keys translated; `@Embedded` elements inside a List; Map value with field-name-like keys is data; both `ownerName` and `owner_name` with equal values merge, differing values throw; cross-operator
  conflict rejected by a real MongoDB; `@Encrypted` rejected; one-argument overload translates; runtime toggle between calls; Morphium instance with `ccc=false`; lease scenario with Java-named keys (first acquire, renewal, held lease E11000 on a real
  MongoDB, release via `$unset` leaves the lease free); filter and update agree on every persistent top-level field (carving out aliases, unknown keys and polymorphic subclasses explicitly); value shape identical with the switch on and off.
- `MorphiumConfigTest`: default true, properties/JSON round trip with `false`, `copy()`; do not assert through `Settings.equals` (returns true at the first identical field).

## D9. Ledger changes (applied together with this plan update)

- MOR-041 is rewritten: always translate with kill switch and per-query override, all operators including `$unset` and `$rename` (key and target), `resolveFieldPath`, collision rule, `@Encrypted` guard, `realClassCache` fix; aliases never resolved.
- MOR-006 acceptance/description: the builder uses `resolveFieldPath` and always translates.
- New: MOR-043 `Query.f` should use `resolveFieldPath` (raw whole-path fallback after List/Map segments and with `@AdditionalData`); MOR-044 `@Version(fieldName=...)` store/filter mismatch (`getFields` honours only `@Reference`/`@Property`,
  `getMongoFieldName` also `@Version.fieldName`); MOR-045 coerce or reject a String `_id` value for a `MorphiumId` `@Id` in update documents.

## D10. Risks and not verified (additions)

Default-true changes where Java-named raw keys land: stray fields that earlier raw writes created stay and need `$unset` (override false) or a migration; `@AdditionalData` readers of strays are affected. Other write paths
(`Query.set`, bulk) keep their strict semantics (`getMongoFieldName`), so the three paths differ on aliases and unknown keys until MOR-042/MOR-043 are done. Not verified: that the InMemoryDriver's `findAndModify` path reaches
the `$bit`/`$pop`/`$mul`/`$min`/`$max`/`$pullAll` labels (the repro test only covers `$currentDate`), `@Encrypted` write behaviour end to end, JSON round trip of the new setting, `SingleMongoConnectDriver`, replica sets, PoppyDB.
