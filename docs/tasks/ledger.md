# Morphium task ledger

As of 2026-10-15. Generated from `docs/tasks/ledger.yaml` with `python3 scripts/ledger/ledger_md.py`, do not edit by hand.

Sources: [reproduction](../research/reproduction-results.md), [jakarta-data](../research/jakarta-data-1.1.md), [other-clients](../research/other-clients.md)

Status: open 44, done 8

## Overview

| ID | Status | Prio | Type | Title | Effort | Ticket | Depends on |
|---|---|---|---|---|---|---|---|
| MOR-006 | open | 1 | implementation | Option A core: mapper-aware update builder (set, setOnInsert, inc, unset) with findOneAndUpdate terminal | L |  | MOR-001, MOR-002 |
| MOR-027 | open | 1 | documentation | Documentation page: raw commands, embedded objects and lifecycle contract | S |  |  |
| MOR-028 | open | 1 | infrastructure | Contract tests InMem vs real MongoDB for all update paths | M |  |  |
| MOR-005 | open | 2 | verification | Audit Query.set/inc/push write paths for swallowed writeErrors and missing name translation | S |  |  |
| MOR-007 | open | 2 | implementation | Option A extension: min, max, mul, rename, currentDate, addToSet/push with each | M |  | MOR-006 |
| MOR-008 | open | 2 | implementation | Options object for findAndModify and a defined result for upsert without returnNew | S |  | MOR-006 |
| MOR-009 | open | 2 | implementation | Lifecycle handling in the builder: @LastChange, @CreationTime, @Version | M |  | MOR-006 |
| MOR-010 | open | 2 | implementation | Typed duplicate-key exception and typed result for a lost conditional upsert | M |  | MOR-008 |
| MOR-014 | open | 2 | implementation | Option E: startup guard for @Embedded under @Entity(translateCamelCase=false) | S |  | MOR-001 |
| MOR-017 | open | 2 | implementation | Jakarta Data: JDQL DELETE FROM | M |  |  |
| MOR-018 | open | 2 | implementation | Jakarta Data: JDQL UPDATE ... SET ... WHERE as one atomic update | L |  | MOR-006, MOR-007 |
| MOR-019 | open | 2 | defect | Jakarta Data: reject unsupported @Find/@Delete parameter styles at build time | S |  |  |
| MOR-021 | open | 2 | implementation | Morphium extension: atomic update and find-and-modify repository annotations | L |  | MOR-008, MOR-010 |
| MOR-024 | open | 2 | defect | Jakarta Data: support ordinal parameters (?1) in @Query | S |  |  |
| MOR-025 | open | 2 | defect | Jakarta Data: @Update is not atomic and throws IllegalStateException | M |  |  |
| MOR-026 | open | 2 | infrastructure | Run the Jakarta Data TCK against morphium-jakarta-data | M |  |  |
| MOR-029 | open | 2 | defect | InMemoryDriver: reject values BsonEncoder cannot encode | M |  |  |
| MOR-031 | open | 2 | implementation | Quarkus: Micrometer / OpenTelemetry metrics for pool and update paths | M |  |  |
| MOR-034 | open | 2 | implementation | Quarkus testing module: real MongoDB test resource, data reset and fixtures | M |  |  |
| MOR-036 | open | 2 | verification | Re-run the reproduction on other topologies and server versions | S |  |  |
| MOR-037 | open | 2 | defect | Morphium.simplifyQueryObject loses predicates for repeated operator keys | S |  |  |
| MOR-043 | open | 2 | defect | Query.f should resolve field paths with resolveFieldPath | M |  | MOR-041 |
| MOR-046 | open | 2 | defect | BufferedMorphiumWriterImpl does not marshal update values and emits push for a pull of a list | M |  | MOR-042 |
| MOR-050 | open | 2 | implementation | Update builder: updateOne, updateMany and UpdateResult | M |  | MOR-006 |
| MOR-011 | open | 3 | implementation | Typed lease / try-acquire primitive | M |  | MOR-010 |
| MOR-012 | open | 3 | implementation | Builder accepts type-safe field references | M |  | MOR-006 |
| MOR-013 | open | 3 | implementation | findOneAndUpdate(Map) accepts a plain field map (auto $set) | S |  | MOR-002 |
| MOR-015 | open | 3 | implementation | Option D: opt-in INHERIT mode for @Embedded translateCamelCase | L |  | MOR-001, MOR-014 |
| MOR-016 | open | 3 | implementation | Migration tooling for changed key names | M |  | MOR-015 |
| MOR-020 | open | 3 | implementation | Jakarta Data 1.1: @Is, constraints, @First and attribute-only Restriction | L |  | MOR-019, MOR-023 |
| MOR-022 | open | 3 | implementation | Jakarta Data: derived update methods (updateByX) | M |  | MOR-006 |
| MOR-023 | open | 3 | verification | Verify which Jakarta Data providers implement the 1.1 features | S |  |  |
| MOR-032 | open | 3 | implementation | Quarkus: named Morphium instances | M |  |  |
| MOR-033 | open | 3 | implementation | Quarkus: CDI integration for Morphium messaging | M |  |  |
| MOR-035 | open | 3 | implementation | Records and value objects in the mapper | L |  |  |
| MOR-038 | open | 3 | defect | Replacement documents, findOneAndUpdateEnums and Expr values still fail with POJOs in update maps | M |  | MOR-002 |
| MOR-039 | open | 3 | documentation | Document pitfalls of update maps: $pull condition keys and mixed element shapes | S |  | MOR-027 |
| MOR-044 | open | 3 | defect | Version annotation with fieldName: store and filter disagree on the stored name | S |  |  |
| MOR-045 | open | 3 | defect | Update documents: a String value for _id of a MorphiumId entity is not coerced | S |  | MOR-041 |
| MOR-047 | open | 3 | defect | MorphiumBulkContext.createUpdateRequest does not marshal update values | S |  | MOR-042 |
| MOR-048 | open | 3 | defect | MorphiumBase.setEnum looks the value up by the map value instead of the key | S |  |  |
| MOR-049 | open | 3 | implementation | Writer path has no guard for @Reference and @Encrypted fields | M |  | MOR-042 |
| MOR-051 | open | 3 | defect | InMemoryDriver: reject path conflicts in update documents like MongoDB | S |  |  |
| MOR-052 | open | 3 | defect | updateLastAccess writes the untranslated Java field name | S |  |  |
| MOR-001 | done | 1 | analysis | Decide the serialisation approach for update operator maps (options A to E) | S |  |  |
| MOR-002 | done | 1 | defect | Option B: marshal every operator map in Query.findOneAndUpdate | M |  | MOR-001 |
| MOR-003 | done | 1 | defect | findOneAndUpdate with a compound filter and upsert collides on _id (code 66) | S |  |  |
| MOR-004 | done | 1 | defect | MorphiumBase.currentDate sends $set: {$currentDate: ...} and fails silently | S |  |  |
| MOR-030 | done | 1 | defect | InMemoryDriver: honour the filter in conditional upsert and raise E11000 | M |  |  |
| MOR-040 | done | 2 | defect | fieldNameCache: inner HashMap is not thread safe | S |  |  |
| MOR-041 | done | 2 | implementation | Key translation for operator maps in findOneAndUpdate | M |  | MOR-002 |
| MOR-042 | done | 3 | implementation | Writer path: marshal values with the shared update-value walk and use one convention for single values | L |  | MOR-002 |

## Details

### MOR-001 Decide the serialisation approach for update operator maps (options A to E)

Status **done**, priority 1, type analysis, effort S

Compare options A (mapper-aware update builder), B (marshal operator maps in findOneAndUpdate), C (POJO fallback hook in BsonEncoder), D (embedded inherits translateCamelCase) and E (startup guard) using the reproduced results. Expected outcome: B as immediate fix, A as target API, E as guard, D only as opt-in tri-state, C rejected or deferred (driver layer would depend on the mapper, InMemoryDriver would still differ). Record the decision as a short ADR-style note in docs/research/ and adjust the dependent tasks. Also consider that no other client makes embedded naming depend on the owner (other-clients.md pattern 15) and that Spring maps update documents idempotently (pattern 4).

Evidence: docs/research/serialisation-decision.md: verdicts for options A-E, each naming its task.

Acceptance:

- A decision note in docs/research/ lists each option with the evidence from reproduction-results.md (hypotheses 1, 2, 4) and a verdict (adopt / opt-in / reject)
- Every verdict names the ledger task that implements it or states that none is planned

Source: docs/research/reproduction-results.md, docs/research/other-clients.md, docs/research/reproduction-results.md (serialisation analysis)

Commits: 44157c934

### MOR-002 Option B: marshal every operator map in Query.findOneAndUpdate

Status **done**, priority 1, type defect, effort M

Query.findOneAndUpdate(Map, upsert, returnNew) hands the raw map to FindAndModifyMongoCommand; an @Embedded POJO in a List, Set, array, single value, Map or Map-in-Map fails client-side with 'Unhandled Data type' (hypothesis 1 confirmed). Marshal the values of every operator map ($set, $setOnInsert, $inc, $addToSet, $push, ...) through the mapper and translate field names. Make MorphiumWriterImpl.marshallIfNecessary handle any Iterable, array and nested Map; this also fixes Query.set with a Set, an array or a Map-in-Map (same hole, reproduced). Expose ObjectMapperImpl.marshallIfNecessary on MorphiumObjectMapper if that is the clean way. Marshalling must be idempotent for already serialised documents (Spring UpdateMapper pattern); watch class_name on non-polymorphic embeddeds. Scope of this task is values only (PR 1 of 3): key translation is MOR-041 and the writer path is MOR-042. The new mapper method is abstract on the interface, @Reference targets are not serialised inline, and the fieldNameCache inner map is made thread safe (MOR-040).

Evidence: PojoInSetOperatorReproductionTest.rawPojoShapesInSet flipped (6 shapes, both drivers); UpdateValueMarshallingTest 28 invocations on PooledDriver (mongo:8, v8.3.11) and InMemoryDriver, 0 failures, 1 skipped (replacement document on InMem, GitHub #399); regression subset of 32 classes: 523 tests on Mongo + InMem and 353 on InMem, only ReferenceCascadeTest fails (Byte Buddy does not support Java 25, the JDK used for the run; unrelated). Acceptance 'Query.set with a Set' moved to MOR-042.

Acceptance:

- PojoInSetOperatorReproductionTest assertions are flipped to the positive expectation for List, Set, array, single value, Map and Map-in-Map on a real MongoDB and on the InMemoryDriver
- A test passes the output of mapper.serialize() through the new path and the stored document is unchanged (double marshalling is stable)
- No class_name is added to non-polymorphic embedded elements beyond the existing serializeIterable behaviour, documented by a test
- Verified against a real MongoDB (not only the InMemoryDriver).

Depends on: MOR-001

Source: docs/research/reproduction-results.md hypothesis 1, docs/research/reproduction-results.md (serialisation analysis)

Commits: 491945d60

### MOR-003 findOneAndUpdate with a compound filter and upsert collides on _id (code 66)

Status **done**, priority 1, type defect, effort S

addGeneratedIdOnInsertIfNeeded only looks for _id at the top level of the filter. A compound filter (_id = X AND (a OR b)) is wrapped in $and, so a generated _id is added via $setOnInsert and MongoDB rejects the first insert with code 66 ('would modify the immutable field _id'). The InMemoryDriver silently stores the document under the generated id and inserts another document on every call. Found by the reproduction. A probe on a real MongoDB showed that the server seeds _id from an equality on _id reachable through $and and single-branch $or, from a one-element $in, and from a dotted _id.x key.

Evidence: UpsertFilterInspectorTest 10 tests; ConditionalUpsertReproductionTest.conditionalUpsertViaQueryKeepsFilterId and FindOneAndUpdateCompoundFilterUpsertTest (first acquire inserts under the filter id, held lease gives E11000 on a real MongoDB, 8 contenders: exactly one winner) green on mongo:8 and InMem. Rule derived from the server probe docs/research/upsert-id-seeding-probe.md; multi-branch $or stays out of scope because the server does not seed there.

Acceptance:

- An upsert whose filter contains an equality on _id reachable through $and or a single-branch $or, a one-element $in, or a dotted _id.x key inserts the document with the _id from the filter, on a real MongoDB
- Filters that the server does not seed from (multi-valued $in, multi-branch $or, range operators, regex, $nor) still receive the generated _id and insert without error
- ConditionalUpsertReproductionTest.conditionalUpsertViaQueryGeneratesConflictingId is flipped accordingly

Source: docs/research/reproduction-results.md hypothesis 5, docs/research/upsert-id-seeding-probe.md

Commits: 491945d60, d6537bcb7

### MOR-004 MorphiumBase.currentDate sends $set: {$currentDate: ...} and fails silently

Status **done**, priority 1, type defect, effort S

currentDate(query, field, upsert, multiple) delegates to set(...), so the wire command is {"$set":{"$currentDate":{"lastSeen":1}}}. MongoDB answers with code 52 in writeErrors (n=0) and NO exception is thrown; the field is unchanged. The InMemoryDriver reports n=1, nModified=1 and changes nothing. Send a real $currentDate operator (raw $currentDate through findOneAndUpdate and the bulk addCurrentDateRequest already work). Both enum and String overloads. Related: the silent writeErrors are covered by MOR-005.

Evidence: CurrentDateReproductionTest flipped and extended (12 invocations: field name, enum, upsert that inserts, operator mixed with a plain field, operator only, $-prefixed field under $set), green on PooledDriver (mongo:8) and InMemoryDriver. Cause was twofold: the operator was nested under $set, and the value was the int 1, which MongoDB rejects for $currentDate; both fixed. The InMemoryDriver now rejects a $-prefixed field name under $set with code 52 (it still counts the matched document in n, MongoDB reports n=0). Regression: inmem package suite and the writer/mapper subset, 769 tests on the InMemoryDriver, 0 failures and 2 ReferenceCascadeTest errors (Byte Buddy does not support Java 25, the JDK of the run; unrelated); WriterPathWiringTest, WriterPathMarshallingTest and WriterMarshallingDifferentialTest green on a real MongoDB.

Acceptance:

- currentDate by field name and by enum updates the field on a real MongoDB and on the InMemoryDriver, with and without upsert
- The InMemoryDriver rejects a $currentDate nested under $set like MongoDB does (no false n=1)
- CurrentDateReproductionTest assertions are flipped to the positive expectation
- Verified against a real MongoDB (not only the InMemoryDriver).

Source: docs/research/reproduction-results.md hypothesis 3

Commits: 38b5f81d8

### MOR-005 Audit Query.set/inc/push write paths for swallowed writeErrors and missing name translation

Status **open**, priority 2, type verification, effort S

The currentDate reproduction shows that a writeErrors entry (code 52) does not raise an exception on the Query.set path. Check which update paths (set, inc, dec, push, pull, addToSet, unset, bulk) ignore writeErrors, and fix or file the confirmed ones. Also verify the unconfirmed claim that MorphiumWriter.inc(Query, Map) puts keys into $inc without field-name translation (MorphiumWriterImpl ~line 2025). Do not duplicate GitHub #408 (writeConcernError ignored); link it. Finding from MOR-042: MorphiumWriterImpl.inc(Query, Map) puts the keys of the map into $inc without name translation (around line 2025), unlike the single-field inc; verify with a test on a real MongoDB.

Acceptance:

- A table in docs/research/ lists each update path with: raises on writeErrors yes/no, translates field names yes/no, proven by a test on a real MongoDB
- Every 'no' has a fix task in the ledger or a fix in the same change

Source: docs/research/reproduction-results.md hypothesis 3, docs/research/jakarta-data-1.1.md section 4, GitHub #408

### MOR-006 Option A core: mapper-aware update builder (set, setOnInsert, inc, unset) with findOneAndUpdate terminal

Status **open**, priority 1, type implementation, effort L

Query.modify() returns an UpdateBuilder with set, setOnInsert, inc, unset and upsert and the terminal findOneAndUpdate(ReturnDocument). Each call is mapped immediately through the UpdateDocumentMapper (keys always translated with resolveFieldPath, values in the element form, @Reference and @Encrypted guards), merged per operator and checked for path conflicts. The terminal injects the @Version field (inc when the update modifies, setOnInsert when it only creates; an explicit touch wins; honours isAutoValuesEnabled), rejects an empty builder, setOnInsert without upsert, skip, limit, hint and projection, and runs the existing findOneAndUpdateMapped. Also in this task: arrayFilters support in FindAndModifyMongoCommand and InMemoryDriver.findAndOneAndUpdate, MorphiumDriverException.isDuplicateKey(), and an Instant spike for update documents on the InMemoryDriver. @LastChange and @CreationTime are not applied (MOR-009); updateOne, updateMany and UpdateResult are MOR-050.

Acceptance:

- A lease acquisition (conditional upsert) with Java field names, an Instant field and an @Version Long field works on a real MongoDB and on the InMemoryDriver: first acquire inserts under the filter _id, a held lease raises an exception for which isDuplicateKey() is true and leaves the document untouched, 8 racing threads have exactly one winner
- A counter with inc and upsert creates on insert and adds on update, 8 x 50 concurrent increments give 400
- Create-if-absent with setOnInsert never overwrites an existing document and does not bump its version
- A store() after a builder upsert does not hit a duplicate key, and a store() with a stale version after a builder update is rejected
- Embedded lists, sets, single values, maps, enums and Instant values need no manual conversion
- An empty builder, setOnInsert without upsert, conflicting paths, skip, limit, hint and projection are rejected before any command is sent and the document stays intact
- arrayFilters work with findOneAndUpdate on both drivers
- Verified against a real MongoDB (not only the InMemoryDriver).

Depends on: MOR-001, MOR-002

Source: docs/research/plan-mor-001-002-003.md, docs/research/plan-mor-006.md

### MOR-007 Option A extension: min, max, mul, rename, currentDate, addToSet/push with each

Status **open**, priority 2, type implementation, effort M

Close the update-operator gaps (no setOnInsert, no each, no min/max/mul/rename on Query) in the builder: typed min/max/mul/rename, a correct currentDate, addToSet(...).each(...) and push(...).each(...) (with PushOptions-like slice/sort/position). Today $each needs a hand-built Map and min/max/mul/rename only exist in the bulk context (MorphiumBulkContext).

Acceptance:

- Each operator has a test on a real MongoDB that checks the stored document
- addToSet with each of embedded values stores each element once and marshalled
- Verified against a real MongoDB (not only the InMemoryDriver).

Depends on: MOR-006

Source: docs/research/reproduction-results.md (update API gaps), docs/research/other-clients.md pattern 5

### MOR-008 Options object for findAndModify and a defined result for upsert without returnNew

Status **open**, priority 2, type implementation, effort S

Replace findOneAndUpdate(Map, boolean upsert, boolean returnNew) by findOneAndUpdate(update, FindAndModifyOptions) with upsert, returnDocument(BEFORE/AFTER), sort, projection, arrayFilters, hint, collation, maxTime. Keep the old signature as a delegating overload. Document and define the result for an upsert that inserts with returnDocument BEFORE (null today, reproduced on both drivers). The arrayFilters wiring of FindAndModifyMongoCommand and InMemoryDriver.findAndOneAndUpdate is done by MOR-006; this task keeps the options object, projection and hint, which the InMemoryDriver currently drops in findAndModify (InMemoryDriver.java around line 4129).

Acceptance:

- The options object is available for the builder and the raw Map path
- The null result of an inserting upsert with BEFORE is documented in the Javadoc and covered by a test on a real MongoDB
- ConditionalUpsertReproductionTest.upsertInsertWithReturnNewFalseReturnsNull still passes or is flipped deliberately
- Verified against a real MongoDB (not only the InMemoryDriver).

Depends on: MOR-006

Source: docs/research/reproduction-results.md hypothesis 5, docs/research/other-clients.md pattern 6

### MOR-009 Lifecycle handling in the builder: @LastChange, @CreationTime, @Version

Status **open**, priority 2, type implementation, effort M

@LastChange and @CreationTime in the builder: findOneAndUpdate through the builder (MOR-006) skips pre/post update events, @LastChange and @CreationTime; Query.set applies them. Add them to the builder on the mapped document, honouring isAutoValuesEnabled. The @Version injection is done by MOR-006. State the event contract in the Javadoc.

Acceptance:

- A builder write on an entity with @LastChange sets the field, and an upsert on an entity with @CreationTime sets it only on insert, on a real MongoDB
- isAutoValuesEnabled false skips both

Depends on: MOR-006

Source: docs/research/reproduction-results.md hypothesis 5, docs/research/other-clients.md patterns 7, 8

### MOR-010 Typed duplicate-key exception and typed result for a lost conditional upsert

Status **open**, priority 2, type implementation, effort M

A conditional upsert on an existing but non-matching document throws MorphiumDriverException with mongoCode 11000 (Integer, writeErrors null); a store() duplicate reports the code inside writeErrors with mongoCode null. Add a typed exception or flag for duplicate key independent of the path, and a typed outcome (acquired / held) for the conditional upsert so callers need not parse getMongoCode(). Align with the exception model work in GitHub #210 and #211. MorphiumDriverException.isDuplicateKey() is added by MOR-006; this task adds the lastErrorObject to the findAndModify result (FindAndModifyMongoCommand.execute drops it today) and the typed outcome.

Acceptance:

- A single predicate/exception recognises duplicate key for both the findAndModify path and the writeErrors path, tested on a real MongoDB
- The conditional upsert returns or throws a typed 'held' outcome that tests assert without inspecting codes
- Verified against a real MongoDB (not only the InMemoryDriver).

Depends on: MOR-008

Source: docs/research/reproduction-results.md hypothesis 5, docs/research/other-clients.md pattern 9, GitHub #210, #211

### MOR-011 Typed lease / try-acquire primitive

Status **open**, priority 3, type implementation, effort M

Applications implement the same lease pattern by hand (job lease, outbox relay lease, command outcome): conditional upsert with filter (expiresAt < now OR owner = me), E11000 meaning 'held'. Offer a small typed primitive or documented recipe built on MOR-010.

Acceptance:

- A documented primitive or recipe with a concurrency test (two contenders, one winner) on a real MongoDB
- Verified against a real MongoDB (not only the InMemoryDriver).

Depends on: MOR-010

Source: update-path research, 2026-10-15

### MOR-012 Builder accepts type-safe field references

Status **open**, priority 3, type implementation, effort M

Applications commonly use Lombok @FieldNameConstants. The builder should accept Java field names, enum constants and the generated static metamodel and resolve Mongo names (incl. rename annotations) itself.

Acceptance:

- Field references from an enum and from the metamodel resolve to the same Mongo name as the String variant, tested

Depends on: MOR-006

Source: update-path research, 2026-10-15

### MOR-013 findOneAndUpdate(Map) accepts a plain field map (auto $set)

Status **open**, priority 3, type implementation, effort S

Spring and Panache wrap a plain field map in $set when no top-level key starts with '$'. Adopt this rule for the raw Map overloads without changing behaviour for maps that already contain operators.

Acceptance:

- A map without operators is applied as $set on a real MongoDB; a map with operators is unchanged
- Verified against a real MongoDB (not only the InMemoryDriver).

Depends on: MOR-002

Source: docs/research/other-clients.md pattern 3

### MOR-014 Option E: startup guard for @Embedded under @Entity(translateCamelCase=false)

Status **open**, priority 2, type implementation, effort S

Flag an @Embedded class with default translateCamelCase that is nested in an @Entity with translateCamelCase=false. Reproduced: nested keys silently become snake_case (file_name, item_version_id, ...). Non-breaking: warn (and offer a strict mode) at entity registration; also expose a test helper that asserts no snake_case key in a stored document.

Acceptance:

- Registering such an entity logs a warning naming both classes, tested
- An embedded with its own translateCamelCase=false produces no warning
- EmbeddedCamelCaseReproductionTest keeps documenting the unchanged storage behaviour

Depends on: MOR-001

Source: docs/research/reproduction-results.md hypothesis 2, docs/research/reproduction-results.md (option E)

### MOR-015 Option D: opt-in INHERIT mode for @Embedded translateCamelCase

Status **open**, priority 3, type implementation, effort L

Tri-state INHERIT/TRUE/FALSE on @Embedded with the current behaviour as default, owner setting threaded through serialize and deserialize (fieldNameCache and getFields are per class today, deserialisation has no owner context). Never change the default: stored data would change.

Acceptance:

- With INHERIT an embedded under a non-translating entity stores camelCase keys and reads them back, on a real MongoDB
- Default behaviour is byte-identical to before, shown by the existing and the reproduction tests
- Verified against a real MongoDB (not only the InMemoryDriver).

Depends on: MOR-001, MOR-014

Source: docs/research/reproduction-results.md hypothesis 2, docs/research/other-clients.md pattern 15

### MOR-016 Migration tooling for changed key names

Status **open**, priority 3, type implementation, effort M

If option D or an application-side fix changes stored key names, provide a @MorphiumChangeUnit based helper that renames nested keys in existing documents.

Acceptance:

- A change-unit example renames nested snake_case keys to camelCase in an embedded list on a real MongoDB
- Verified against a real MongoDB (not only the InMemoryDriver).

Depends on: MOR-015

Source: update-path research, 2026-10-15

### MOR-017 Jakarta Data: JDQL DELETE FROM

Status **open**, priority 2, type implementation, effort M

DELETE FROM is part of Jakarta Data 1.0 (not 1.1). JdqlParser only accepts SELECT/WHERE/GROUP BY/HAVING/ORDER BY. Add DELETE FROM [WHERE] returning void/int/long (matched count) in JdqlParser, the Spring proxy path and MorphiumDataProcessor. Query.delete() already returns n. State whether lifecycle callbacks run (derived deleteBy skips them).

Acceptance:

- A @Query("DELETE FROM Entity WHERE ...") method works in the Quarkus and the Spring module against a real MongoDB
- Return types void, int and long are supported; other types are rejected at build time
- Verified against a real MongoDB (not only the InMemoryDriver).

Source: docs/research/jakarta-data-1.1.md section 1

### MOR-018 Jakarta Data: JDQL UPDATE ... SET ... WHERE as one atomic update

Status **open**, priority 2, type implementation, effort L

UPDATE is part of 1.0. SET items are field = expression or NULL; no increment operator, an increment is x = x + ?1. Map to one atomic command: assignment to $set, NULL to $unset, x = x + :n to $inc, x = x * :n to $mul, built through the builder so names and values are mapped. Return the matched count (n, not nModified). Arithmetic on other fields, '||' and string functions need pipeline updates (FindAndModifyMongoCommand has a pipeline field that Query does not expose): reject them at build time with a clear message, or handle in a follow-up. JNoSQL rejects arithmetic entirely, so this goes beyond it.

Acceptance:

- A method with SET a = :x, n = n + 1 WHERE id = :id issues exactly one update command on a real MongoDB
- Return value equals the matched count
- Unsupported expressions fail at build time
- Verified against a real MongoDB (not only the InMemoryDriver).

Depends on: MOR-006, MOR-007

Source: docs/research/jakarta-data-1.1.md sections 1, 4, docs/research/other-clients.md pattern 12

### MOR-019 Jakarta Data: reject unsupported @Find/@Delete parameter styles at build time

Status **open**, priority 2, type defect, effort S

FindMethodBridge and MorphiumDataProcessor turn every @Find/@Delete parameter into an equality. With Jakarta Data 1.1 an @Is(AtLeast.class) parameter would silently run as eq and a Restriction parameter would be treated as an attribute. Reject @Is, Constraint, Restriction and @First usage with a clear build error until MOR-020 supports them.

Acceptance:

- A repository with an @Is or Restriction parameter fails the Quarkus build with a message naming the method (processor test)
- The Spring proxy path throws at startup with the same message

Source: docs/research/jakarta-data-1.1.md section 4

### MOR-020 Jakarta Data 1.1: @Is, constraints, @First and attribute-only Restriction

Status **open**, priority 3, type implementation, effort L

Adopt the 1.1 query features once 1.1 is final (latest is 1.1.0-M4, no final date known). @Is, jakarta.data.constraint.*, @First and attribute-only Restriction need no new core API (operators exist in QueryExecutor); arithmetic/function restrictions need $expr. Bump jakarta.data.version, keep 1.0 compatibility decisions explicit. Do not duplicate GitHub #302 (lifecycle CDI events); link it.

Acceptance:

- Methods using @Is with each constraint type and a Restriction parameter return correct results on a real MongoDB
- @First(n) limits results; combining it with Limit/PageRequest is rejected
- The dependency is bumped and the module builds against it
- Verified against a real MongoDB (not only the InMemoryDriver).

Depends on: MOR-019, MOR-023

Source: docs/research/jakarta-data-1.1.md, GitHub #302

### MOR-021 Morphium extension: atomic update and find-and-modify repository annotations

Status **open**, priority 2, type implementation, effort L

The Jakarta Data spec has no upsert or find-and-modify annotation, so this is a vendor extension. Provide an annotation pair in morphium-jakarta-data and MorphiumDataProcessor: a count-returning update and a document-returning find-and-modify with upsert, returnDocument and sort, using structured set / setOnInsert / inc arrays of field and parameter pairs (compile-time field-name check) instead of JSON strings. Return-type convention: numeric, boolean or void gives the count form, entity or Optional gives the find-and-modify form, collections are rejected. Parameters are bound through the builder (MOR-006).

Acceptance:

- A repository method acquires a lease with one atomic upsert on a real MongoDB without touching the driver API
- Invalid return types fail at build time
- Documented as non-portable in docs/jakarta-data.md
- Verified against a real MongoDB (not only the InMemoryDriver).

Depends on: MOR-008, MOR-010

Source: docs/research/jakarta-data-1.1.md section 3, docs/research/other-clients.md patterns 10, 13

### MOR-022 Jakarta Data: derived update methods (updateByX)

Status **open**, priority 3, type implementation, effort M

Micronaut turns update(@Id id, int pages) and updateByTitle(...) into $set at compile time. Fit this into MethodNameParser, returning a long, built on the builder.

Acceptance:

- updateByX(value, ...) issues one $set for non-criteria parameters on a real MongoDB
- Verified against a real MongoDB (not only the InMemoryDriver).

Depends on: MOR-006

Source: docs/research/other-clients.md pattern 11

### MOR-023 Verify which Jakarta Data providers implement the 1.1 features

Status **open**, priority 3, type verification, effort S

Not verified in the research: whether Hibernate 7.4/8.x, Eclipse JNoSQL or Micronaut implement @Is, Restriction and @First, whether the 1.1.0-M4 jar differs from main in more than the file set, and whether TCK 1.1 contains update tests beyond UPDATE ... SET. Check before MOR-020 fixes its scope.

Acceptance:

- docs/research/jakarta-data-1.1.md gets a verified provider table with sources

Source: docs/research/jakarta-data-1.1.md 'could not be verified'

### MOR-024 Jakarta Data: support ordinal parameters (?1) in @Query

Status **open**, priority 2, type defect, effort S

MorphiumDataProcessor rejects ordinal parameters, but the spec and TCK use them (SET length = length + ?1).

Acceptance:

- @Query methods with ?1 style parameters work in the Quarkus and Spring module against a real MongoDB
- Verified against a real MongoDB (not only the InMemoryDriver).

Source: docs/research/jakarta-data-1.1.md section 4

### MOR-025 Jakarta Data: @Update is not atomic and throws IllegalStateException

Status **open**, priority 2, type defect, effort M

@Update does findById then store(): an extra round trip, not atomic, and a missing entity or version mismatch raises IllegalStateException instead of jakarta.data.exceptions.OptimisticLockingFailureException (AbstractMorphiumRepository ~line 324). Align the exception type with the spec and use a versioned filter update if the entity has @Version.

Acceptance:

- A stale-version @Update throws OptimisticLockingFailureException, tested on a real MongoDB
- A missing entity behaves as the spec describes, tested
- Verified against a real MongoDB (not only the InMemoryDriver).

Source: docs/research/jakarta-data-1.1.md sections 3, 4

### MOR-026 Run the Jakarta Data TCK against morphium-jakarta-data

Status **open**, priority 2, type infrastructure, effort M

No jakarta.data-tck dependency exists in any pom. Micronaut Data has a jakarta-data-tck/mongodb module as a model. Add the TCK matching the implemented API version and run it against a real MongoDB and PoppyDB/InMemoryDriver.

Acceptance:

- A documented command runs the TCK; the list of excluded tests has a reason per entry
- The TCK runs in CI or a task for CI is added to this ledger
- Verified against a real MongoDB (not only the InMemoryDriver).

Source: docs/research/other-clients.md pattern 14

### MOR-027 Documentation page: raw commands, embedded objects and lifecycle contract

Status **open**, priority 1, type documentation, effort S

Write a page (docs/howtos/) on the pitfalls reproduced so far: POJOs in raw update maps, embedded translateCamelCase default, the @Version trap, E11000 on a conditional upsert, null result of an inserting upsert, findOneAndUpdate skipping lifecycle events, raw Maps staying raw by contract. Add it to mkdocs nav. Also correct quarkus-morphium/README.md which names M3 as the latest Jakarta Data milestone (M4 exists) and docs/jakarta-data.md where needed.

Acceptance:

- The page exists, is in the mkdocs nav and each pitfall links to a reproduction test or ledger task
- mkdocs build succeeds

Source: docs/research/reproduction-results.md, update-path research, 2026-10-15

### MOR-028 Contract tests InMem vs real MongoDB for all update paths

Status **open**, priority 1, type infrastructure, effort M

Turn the reproduction tests into a maintained contract suite that runs the same update paths ($set with embedded lists/sets, $setOnInsert, $inc, $addToSet, $currentDate, conditional upsert, raw key names) against both drivers and declares divergences explicitly. The repo has no known-defect annotation; introduce a small one or an assumption convention so divergences are tracked, not hidden. Move the tests out of the repro package when the defects are fixed.

Acceptance:

- One test class family runs every update path on PooledDriver and InMemDriver via MultiDriverTestBase
- Each divergence references the ledger task that removes it
- The default build stays green
- Verified against a real MongoDB (not only the InMemoryDriver).

Source: docs/research/reproduction-results.md, update-path research, 2026-10-15

### MOR-029 InMemoryDriver: reject values BsonEncoder cannot encode

Status **open**, priority 2, type defect, effort M

The InMemoryDriver stores POJOs in $set as live Java objects (List, array, Map, Map-in-Map) and, for a Set or a single POJO, stores them and then fails with a ClassCastException wrapped in RuntimeException, leaving a half-written document. A real MongoDB fails with 'Unhandled Data type' up front. Make the InMemoryDriver fail the same way before changing anything. Related to GitHub #336 (unmapped values stored verbatim); link it. After MOR-002 only hand-built commands reach the encoder or the InMemoryDriver with POJOs.

Acceptance:

- Update with a POJO inside $set fails identically in type and message prefix on both drivers and leaves the document unchanged
- Reproduction assertions for hypothesis 4 are flipped

Source: docs/research/reproduction-results.md hypothesis 4, GitHub #336

### MOR-030 InMemoryDriver: honour the filter in conditional upsert and raise E11000

Status **done**, priority 1, type defect, effort M

On an existing document whose conditional filter does not match, MongoDB throws E11000 (held lease). The InMemoryDriver silently overwrites the document, so contender B steals the lease of A; with a compound filter it also inserts a new document per call (see MOR-003). Fix the upsert semantics and the error. Related to GitHub #399 (InMemoryDriver update documents); link it.

Evidence: InMemUpsertDuplicateKeyTest 9 tests (findAndModify path, update command path, id pinned by $setOnInsert, matching filter, filter without id, store() still replaces, 8 racing threads with exactly one winner); the pins that documented the overwrite were flipped to the duplicate key expectation (ConditionalUpsertReproductionTest, FindOneAndUpdateCompoundFilterUpsertTest, UpdateKeyTranslationTest) and now hold for PooledDriver (mongo:8) and InMemoryDriver alike. Before the fix 11 of these tests failed on the InMemoryDriver only. Regression: inmem package suite and the writer/mapper/messaging subset, 778 tests, 0 failures, 2 ReferenceCascadeTest errors (Byte Buddy does not support Java 25, the JDK of the run; unrelated). Remaining divergence: the update command path throws on the InMemoryDriver while a real MongoDB returns writeErrors code 11000 (MOR-005).

Acceptance:

- Held-lease scenario: second contender gets the E11000 MorphiumDriverException on both drivers
- ConditionalUpsertReproductionTest conditionalUpsertOnHeldLease asserts the same outcome for both drivers

Source: docs/research/reproduction-results.md hypothesis 5, GitHub #399

Commits: 88ae60b4b

### MOR-031 Quarkus: Micrometer / OpenTelemetry metrics for pool and update paths

Status **open**, priority 2, type implementation, effort M

Applications currently write their own connection pool metrics. Provide connection pool metrics and counters/timers for update paths in quarkus-morphium, building on docs/monitoring-metrics-guide.md.

Acceptance:

- Metrics appear on the Quarkus metrics endpoint in an integration test against a real MongoDB
- Verified against a real MongoDB (not only the InMemoryDriver).

Source: review of quarkus-morphium

### MOR-032 Quarkus: named Morphium instances

Status **open**, priority 3, type implementation, effort M

quarkus-morphium supports a single Morphium instance. Add named instances with qualifier injection, config under quarkus.morphium."name".*, health checks per instance.

Acceptance:

- Two instances with different databases can be injected and used in an integration test

Source: review of quarkus-morphium

### MOR-033 Quarkus: CDI integration for Morphium messaging

Status **open**, priority 3, type implementation, effort M

No CDI integration exists for MorphiumMessaging in quarkus-morphium (producer, lifecycle, listener annotation).

Acceptance:

- A @QuarkusTest sends and receives a message through an injected messaging bean

Source: review of quarkus-morphium

### MOR-034 Quarkus testing module: real MongoDB test resource, data reset and fixtures

Status **open**, priority 2, type implementation, effort M

The testing module offers only InMemMorphiumTestProfile. Add a MongoDB test resource (container or URI), a reset helper between tests and simple fixtures, so applications can run the contract-style tests of MOR-028.

Acceptance:

- An application-style test uses the resource and resets data between tests
- Verified against a real MongoDB (not only the InMemoryDriver).

Source: review of quarkus-morphium

### MOR-035 Records and value objects in the mapper

Status **open**, priority 3, type implementation, effort L

Applications keep scalars in entities to avoid {"value": ...} sub-documents. Add record and single-value converter support. Do not duplicate GitHub #116 (records as entities) and #334 (container fields of custom mappers); link them and reuse their scope.

Acceptance:

- A record and a single-value converter round-trip as embedded and scalar values on a real MongoDB
- Verified against a real MongoDB (not only the InMemoryDriver).

Source: update-path research, 2026-10-15, docs/research/other-clients.md pattern 16, GitHub #116, #334

### MOR-036 Re-run the reproduction on other topologies and server versions

Status **open**, priority 2, type verification, effort S

The reproduction ran on a standalone mongo:8 with PooledDriver only. Re-run on a replica set, with auth, with SingleMongoConnectDriver, on PoppyDB and on an older MongoDB (error wording and $-field handling differ), and re-check hypothesis 1 with @Entity and @Reference values.

Acceptance:

- reproduction-results.md has a matrix of topology x result

Source: docs/research/reproduction-results.md 'could not verify'

### MOR-037 Morphium.simplifyQueryObject loses predicates for repeated operator keys

Status **open**, priority 2, type defect, effort S

Morphium.java simplifyQueryObject merges the operator maps of a query with putAll, so a repeated key such as $or or $and overwrites an earlier predicate and the query silently matches too much. The merge has to combine repeated keys instead of replacing them.

Acceptance:

- A query that adds two $or conditions returns only documents matching both, shown by a test on a real MongoDB

Source: docs/research/plan-mor-001-002-003.md

### MOR-038 Replacement documents, findOneAndUpdateEnums and Expr values still fail with POJOs in update maps

Status **open**, priority 3, type defect, effort M

After MOR-002 the operator maps are marshalled, but a replacement document, the values passed to findOneAndUpdateEnums and the values inside an Expr still reach the encoder with POJOs. Route these three paths through the same value walk.

Acceptance:

- A replacement document, a findOneAndUpdateEnums call and an Expr containing an @Embedded POJO store the serialised value on a real MongoDB

Depends on: MOR-002

Source: docs/research/plan-mor-001-002-003.md

### MOR-039 Document pitfalls of update maps: $pull condition keys and mixed element shapes

Status **open**, priority 3, type documentation, effort S

Document that the keys inside a $pull condition are raw field names and are not translated, and what happens with mixed element shapes in $addToSet and $push, including when class_name is added to elements.

Depends on: MOR-027

Source: docs/research/plan-mor-001-002-003.md

### MOR-040 fieldNameCache: inner HashMap is not thread safe

Status **done**, priority 2, type defect, effort S

AnnotationAndReflectionHelper keeps the fieldNameCache as a map of plain HashMap instances that is read and filled from several threads. Replace the inner map with a ConcurrentHashMap.

Evidence: AnnotationAndReflectionHelper field-name cache uses ConcurrentHashMap per class; the lease race test (8 threads, real MongoDB) exercises the lookup concurrently and is green. No dedicated unit test for the cache itself.

Acceptance:

- A concurrent test resolving field names for the same class from many threads passes without exceptions

Source: docs/research/plan-mor-001-002-003.md

Commits: 491945d60

### MOR-041 Key translation for operator maps in findOneAndUpdate

Status **done**, priority 2, type implementation, effort M

Translate the keys of operator maps in findOneAndUpdate to the Mongo field names for every operator, including the key of $unset and the key and target of $rename, dotted keys through a lenient resolveFieldPath and the single-segment resolver, never through @Aliases. The translation is always on, with a kill switch (translateUpdateFieldNames, default true) and a per-query override. Two keys that resolve to the same name throw only if their values differ. An update that would write a plaintext value into an @Encrypted field is rejected. The one-argument AnnotationAndReflectionHelper constructor gets a thread safe realClassCache.

Evidence: New tests: StoredFieldNameResolverTest 19, ResolveFieldPathTest 8, UpdateDocumentMapperKeyTest 12, UpdateKeyTranslationTest 48 invocations (1 skipped: cross-operator path conflict on InMem), MorphiumConfigTest 16; all green on PooledDriver (mongo:8, v8.3.11) and InMemoryDriver. The raw-document reads of UpdateKeyTranslationTest bypass the projection to known fields that Query adds, so stray fields are really detected (the first run with asMapList was vacuous for strays). Regression subset of 38 classes: 672 tests on Mongo + InMem and 470 on InMem, 0 failures, only ReferenceCascadeTest errors (Byte Buddy does not support Java 25, the JDK of the run; unrelated). Scope kept to the decisions in docs/research/plan-mor-041.md; Query.f is unchanged (MOR-043).

Acceptance:

- $set, $inc, $push and the other value operators with a Java-named key (translateCamelCase entity) are stored under the Mongo name, on a real MongoDB and on the InMemoryDriver, without a stray field
- $unset of a Java name removes the real field and $rename translates key and target
- A dotted key through a single embedded field uses the embedded class's own naming; positional, $-prefixed, unknown and @Aliases keys pass through verbatim
- With translateUpdateFieldNames=false, or the per-query override false, keys are written verbatim as in the previous release
- A collision of two keys with equal values is merged and with differing values rejected; an update of an @Encrypted field is rejected
- Resolver names match serialize() for a corpus of entities and the resolver is idempotent
- Verified against a real MongoDB (not only the InMemoryDriver).

Depends on: MOR-002

Source: docs/research/plan-mor-001-002-003.md, docs/research/plan-mor-041.md

Commits: 0ec3a9562

### MOR-042 Writer path: marshal values with the shared update-value walk and use one convention for single values

Status **done**, priority 3, type implementation, effort L

Query.set, push, pushAll, pull, pullAll, addToSet and addAllToSet marshal their values with the shared update-value walk, eagerly on the caller thread, in the element form, so Set, array, Map-in-Map and nested containers of @Embedded/@Entity values no longer fail on a real MongoDB or get stored as live objects by the InMemoryDriver. Output that works today stays identical (single embedded values keep class_name, a single entity keeps its _id); enum constants inside containers become name(), null list elements are kept and Enum map keys are accepted. The same PR changes findOneAndUpdate so that single values also carry class_name (one convention for update paths), fixes pullAll sending $pull, Query.set with a null value (NPE) and Query.set(Enum, ...) using toString(), and stops treating a Class value as an entity. Keys keep the strict getMongoFieldName semantics of the writer; currentDate (MOR-004), inc keys and swallowed write errors (MOR-005) are not part of this task. The marshalling covers Collection, not every Iterable.

Evidence: New: WriterMarshallingDifferentialTest 4 (KEEP rows equal a verbatim copy of the previous function, SWITCH rows asserted), WriterPathWiringTest 34 (1 skipped), WriterPathMarshallingTest 30, the repro pins of PojoInSetOperatorReproductionTest flipped (6); all green on PooledDriver (mongo:8) and InMemoryDriver. With the writer, Query and MorphiumBase changes reverted, 25 of the 34 new tests of the last two classes fail, so they test real behaviour. Regression subset of 52 classes: 564 tests on the InMemoryDriver with 2 errors (ReferenceCascadeTest: Byte Buddy does not support Java 25, the JDK of the run); on a real MongoDB plus InMem 853 tests, the failures were environmental: the Docker VM disk was full (Mongo refused writes, 14031), and after moving the data directory to tmpfs ExclusiveMessageTests.exclusiveTest and exclusivityTest still fail identically on the commit before the change (4e378f261), CoreFunctionalityTests.cachedObjectOperationsTest passed in 3 of 3 repetitions and on the baseline. Follow-ups filed: MOR-046 to MOR-049; the inc(Query, Map) finding is noted in MOR-005.

Acceptance:

- Query.set with a Set, an array, a Map-in-Map, nested lists and an enum list of @Embedded/Enum values stores the serialised value on a real MongoDB and on the InMemoryDriver
- A differential test shows that the new writer function equals a verbatim copy of the previous one for every input whose output must not change
- push, addToSet, pushAll, addAllToSet, pull and pullAll store documents with class_name for embedded elements, and pullAll with a list sends $pullAll, on a real MongoDB
- Query.set(field, null, ...) and Query.set(Enum, ...) work and use the constant name
- An invalid value throws an IllegalArgumentException directly from the call, fires no store event, and a collection mutated after the call does not change the write
- findOneAndUpdate of a single embedded value carries class_name
- The MorphiumMigrationRunner shaped upsert (Map.of(String, Date) with a @Property entity) behaves as before on a real MongoDB
- Verified against a real MongoDB (not only the InMemoryDriver).

Depends on: MOR-002

Source: docs/research/plan-mor-001-002-003.md, docs/research/plan-mor-042.md

Commits: 4e378f261, 39ac39ac3

### MOR-043 Query.f should resolve field paths with resolveFieldPath

Status **open**, priority 2, type defect, effort M

Query.f returns the raw whole path after a List or Map segment (even the first segment) and the raw string for entities with @AdditionalData, so a filter on a dotted path can miss the stored name while the update, which translates the leading segments, hits it. Use resolveFieldPath for the dotted branch and decide the AdditionalData behaviour.

Acceptance:

- A filter and an update on the same dotted path address the same stored field, on a real MongoDB
- Existing Query field tests stay green

Depends on: MOR-041

Source: docs/research/plan-mor-041.md

### MOR-044 Version annotation with fieldName: store and filter disagree on the stored name

Status **open**, priority 3, type defect, effort S

getFields honours only @Reference and @Property field names, so serialize writes a @Version(fieldName=...) field under its Java name, while getMongoFieldName honours the annotation. This was read in the code but not reproduced.

Acceptance:

- A test shows the stored name and the filter name of an @Version(fieldName=...) field, and one of the two is changed so that they agree

Source: docs/research/plan-mor-041.md

### MOR-045 Update documents: a String value for _id of a MorphiumId entity is not coerced

Status **open**, priority 3, type defect, effort S

With key translation an id key of the update becomes _id and pins the id; a String value for an entity whose @Id is a MorphiumId is written as a String _id that the mapper cannot read back. Coerce the value or reject it.

Acceptance:

- An upsert with $setOnInsert id as a String for a MorphiumId entity either stores an ObjectId _id that reads back or fails with a clear message, on a real MongoDB

Depends on: MOR-041

Source: docs/research/plan-mor-041.md

### MOR-046 BufferedMorphiumWriterImpl does not marshal update values and emits push for a pull of a list

Status **open**, priority 2, type defect, effort M

For entities with @WriteBuffer on a real MongoDB the buffered writer passes the values of set, pushPull and pushPullAll to the driver raw, so embedded values, Sets and enums fail or are stored differently from the synchronous writer, and pushPullAll for PULL emits $push (BufferedMorphiumWriterImpl around lines 631-658, 944-985 and 988-1030). It is selected on a real MongoDB only, so the InMemoryDriver tests never see it.

Acceptance:

- Query.set, push, pushAll and pull on a @WriteBuffer entity store the same documents as on a synchronous entity, on a real MongoDB
- pullAll on a @WriteBuffer entity removes the elements

Depends on: MOR-042

Source: docs/research/plan-mor-042.md

### MOR-047 MorphiumBulkContext.createUpdateRequest does not marshal update values

Status **open**, priority 3, type defect, effort S

The bulk context hands the values of its update requests to the driver as given (bulk/MorphiumBulkContext.java around lines 56-64), so embedded values, Sets and enums in a bulk update fail on a real MongoDB or are stored as live objects by the InMemoryDriver.

Acceptance:

- A bulk update with an embedded value, a Set and an enum stores the same documents as Query.set, on a real MongoDB

Depends on: MOR-042

Source: docs/research/plan-mor-042.md

### MOR-048 MorphiumBase.setEnum looks the value up by the map value instead of the key

Status **open**, priority 3, type defect, effort S

MorphiumBase.setEnum builds the field map with values.get(est.getValue()) around line 1215, which looks up the value as if it were a key, so the value written is null unless a value equals a key. Read in the code, not reproduced.

Acceptance:

- setEnum with a map of constants and values stores each value under the name of its constant, on a real MongoDB

Source: docs/research/plan-mor-042.md

### MOR-049 Writer path has no guard for @Reference and @Encrypted fields

Status **open**, priority 3, type implementation, effort M

UpdateDocumentMapper rejects an entity for a @Reference field and any write to an @Encrypted field in findOneAndUpdate; Query.set, push and pull accept both and embed the entity or store a plaintext value. Adding the guard would be a breaking change for callers who rely on it, so it needs a decision and a CHANGELOG entry.

Acceptance:

- Query.set of an entity into a @Reference field and of a plaintext value into an @Encrypted field fail with a clear message, or the decision to keep the behaviour is documented in the Javadoc and the CHANGELOG

Depends on: MOR-042

Source: docs/research/plan-mor-042.md

### MOR-050 Update builder: updateOne, updateMany and UpdateResult

Status **open**, priority 2, type implementation, effort M

Add the count terminals to the update builder: updateOne and updateMany through the update command, with a package-private Query.updateMapped that turns writeErrors into a MorphiumDriverException (counts in the message, writeConcernError handled, no int cast of n). UpdateResult is a final class with getMatchedCount (n minus upserted), getModifiedCount, getUpsertedId converted to the @Id type, and isAcknowledged. Sort on updateOne is rejected until the InMemoryDriver honours it and the server capability is checked. Serves the Jakarta Data UPDATE count (MOR-018).

Acceptance:

- updateMany reports matched and modified counts, a second identical call reports modified 0, on a real MongoDB and on the InMemoryDriver
- An upsert reports the upserted id and matched 0
- A duplicate key raises an exception with isDuplicateKey() true on both drivers
- arrayFilters work with updateMany

Depends on: MOR-006

Source: docs/research/plan-mor-006.md

### MOR-051 InMemoryDriver: reject path conflicts in update documents like MongoDB

Status **open**, priority 3, type defect, effort S

A real MongoDB rejects an update document that touches the same path twice or a path and its child (set a and set a.b, set and inc on one field) with code 40. The InMemoryDriver accepts them, so raw-map users get a different result per driver.

Acceptance:

- The InMemoryDriver rejects set a plus set a.b and set plus inc on one path with code 40, like mongo:8

Source: docs/research/plan-mor-006.md

### MOR-052 updateLastAccess writes the untranslated Java field name

Status **open**, priority 3, type defect, effort S

Query.findOneAndUpdateMapped ends with updateLastAccess, a second non-atomic update command for @LastAccess entities that sets the Java field name with a Long. With camel case translation the real field is not updated. Read in the code, not reproduced.

Acceptance:

- After a findOneAndUpdate on an @LastAccess entity the stored last-access field is updated under its stored name, on a real MongoDB

Source: docs/research/plan-mor-006.md

