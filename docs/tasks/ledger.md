# Morphium task ledger

As of 2026-10-15. Generated from `docs/tasks/ledger.yaml` with `python3 scripts/ledger/ledger_md.py`, do not edit by hand.

Sources: [reproduction](../research/reproduction-results.md), [jakarta-data](../research/jakarta-data-1.1.md), [other-clients](../research/other-clients.md)

Status: open 22, done 73

## Overview

| ID | Status | Prio | Type | Title | Effort | Ticket | Depends on |
|---|---|---|---|---|---|---|---|
| MOR-063 | open | 2 | defect | quarkus-morphium migration lock: recognise a held lock by isDuplicateKey() | S |  |  |
| MOR-069 | open | 2 | defect | PoppyDB: the update fast path reports every error as 11000 and ignores the ordered flag | S |  |  |
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
| MOR-048 | open | 3 | defect | MorphiumBase.setEnum looks the value up by the map value instead of the key | S |  |  |
| MOR-049 | open | 3 | implementation | Writer path has no guard for @Reference and @Encrypted fields | M |  | MOR-042 |
| MOR-052 | open | 3 | defect | updateLastAccess writes the untranslated Java field name | S |  |  |
| MOR-070 | open | 3 | defect | PoppyDB: the delete fast path swallows errors | S |  |  |
| MOR-071 | open | 3 | defect | Query.set and findOneAndUpdate(Map) maintain neither @Version nor a field-only @LastChange | S |  |  |
| MOR-075 | open | 3 | defect | JdqlParser accepts an operand that is neither a parameter nor a literal | S |  |  |
| MOR-001 | done | 1 | analysis | Decide the serialisation approach for update operator maps (options A to E) | S |  |  |
| MOR-002 | done | 1 | defect | Option B: marshal every operator map in Query.findOneAndUpdate | M |  | MOR-001 |
| MOR-003 | done | 1 | defect | findOneAndUpdate with a compound filter and upsert collides on _id (code 66) | S |  |  |
| MOR-004 | done | 1 | defect | MorphiumBase.currentDate sends $set: {$currentDate: ...} and fails silently | S |  |  |
| MOR-006 | done | 1 | implementation | Option A core: mapper-aware update builder (set, setOnInsert, inc, unset) with findOneAndUpdate terminal | L |  | MOR-001, MOR-002 |
| MOR-027 | done | 1 | documentation | Documentation page: raw commands, embedded objects and lifecycle contract | S |  |  |
| MOR-028 | done | 1 | infrastructure | Contract tests InMem vs real MongoDB for all update paths | M |  |  |
| MOR-030 | done | 1 | defect | InMemoryDriver: honour the filter in conditional upsert and raise E11000 | M |  |  |
| MOR-059 | done | 1 | defect | Query, entity and bulk write paths swallow the writeErrors of the update reply | M |  |  |
| MOR-005 | done | 2 | verification | Audit Query.set/inc/push write paths for swallowed writeErrors and missing name translation | S |  |  |
| MOR-007 | done | 2 | implementation | Option A extension: min, max, mul, rename, currentDate, addToSet/push with each | M |  | MOR-006 |
| MOR-008 | done | 2 | implementation | Options object for findAndModify and a defined result for upsert without returnNew | S |  | MOR-006 |
| MOR-009 | done | 2 | implementation | Lifecycle handling in the builder: @LastChange, @CreationTime, @Version | M |  | MOR-006 |
| MOR-010 | done | 2 | implementation | Typed duplicate-key exception and typed result for a lost conditional upsert | M |  | MOR-006 |
| MOR-014 | done | 2 | implementation | Option E: startup guard for @Embedded under @Entity(translateCamelCase=false) | S |  | MOR-001 |
| MOR-017 | done | 2 | implementation | Jakarta Data: JDQL DELETE FROM | M |  |  |
| MOR-018 | done | 2 | implementation | Jakarta Data: JDQL UPDATE ... SET ... WHERE as one atomic update | L |  | MOR-006, MOR-007 |
| MOR-019 | done | 2 | defect | Jakarta Data: reject unsupported @Find/@Delete parameter styles at build time | S |  |  |
| MOR-021 | done | 2 | implementation | Morphium extension: atomic update and find-and-modify repository annotations | L |  | MOR-008, MOR-010 |
| MOR-024 | done | 2 | defect | Jakarta Data: support ordinal parameters (?1) in @Query | S |  |  |
| MOR-025 | done | 2 | defect | Jakarta Data: @Update is not atomic and throws IllegalStateException | M |  |  |
| MOR-026 | done | 2 | infrastructure | Run the Jakarta Data TCK against morphium-jakarta-data | M |  |  |
| MOR-029 | done | 2 | defect | InMemoryDriver: reject values BsonEncoder cannot encode | M |  |  |
| MOR-031 | done | 2 | implementation | Quarkus: Micrometer / OpenTelemetry metrics for pool and update paths | M |  |  |
| MOR-034 | done | 2 | implementation | Quarkus testing module: real MongoDB test resource, data reset and fixtures | M |  |  |
| MOR-036 | done | 2 | verification | Re-run the reproduction on other topologies and server versions | S |  |  |
| MOR-037 | done | 2 | defect | Morphium.simplifyQueryObject loses predicates for repeated operator keys | S |  |  |
| MOR-040 | done | 2 | defect | fieldNameCache: inner HashMap is not thread safe | S |  |  |
| MOR-041 | done | 2 | implementation | Key translation for operator maps in findOneAndUpdate | M |  | MOR-002 |
| MOR-043 | done | 2 | defect | Query.f should resolve field paths with resolveFieldPath | M |  | MOR-041 |
| MOR-046 | done | 2 | defect | BufferedMorphiumWriterImpl does not marshal update values and emits push for a pull of a list | M |  | MOR-042 |
| MOR-050 | done | 2 | implementation | Update builder: updateOne, updateMany and UpdateResult | M |  | MOR-006 |
| MOR-053 | done | 2 | defect | InMemoryDriver: type checks of the update operators ($inc, $mul, $push, $addToSet, $pull, $pop) | M |  |  |
| MOR-054 | done | 2 | defect | InMemoryDriver: validate the shape of an update document (unknown operator, mixed fields, _id, names) | S |  |  |
| MOR-055 | done | 2 | defect | InMemoryDriver: number and BSON comparison semantics of $inc, $mul, $min, $max and $addToSet | M |  |  |
| MOR-058 | done | 2 | defect | InMemoryDriver: findAndModify does not reject what the update command rejects | S |  |  |
| MOR-060 | done | 2 | defect | Write paths that write the Java field name: inc/dec with a Map and dotted paths through embedded documents | S |  |  |
| MOR-062 | done | 2 | defect | Morphium.dec(Query, Map) and dec(Map, Query) increment instead of decrement | S |  |  |
| MOR-064 | done | 2 | defect | store() of an existing entity swallows a rejected update (StoreMongoCommand) | M |  |  |
| MOR-067 | done | 2 | defect | Query.toQueryObject puts the or queries into the $nor list when both are used | S |  |  |
| MOR-068 | done | 2 | defect | SingleMongoConnectDriver: cannot connect to a standalone server, and a rejected bulk request keeps its only connection | S |  |  |
| MOR-076 | done | 2 | defect | Jakarta Data: derived queries without By (countAll and similar) are unsupported | S |  |  |
| MOR-077 | done | 2 | verification | Jakarta Data: repositories without a single primary entity are unsupported | M |  |  |
| MOR-078 | done | 2 | defect | A char or Character field cannot be read back | S |  |  |
| MOR-079 | done | 2 | defect | Jakarta Data: findFirstBy and findFirstNBy are unsupported | S |  |  |
| MOR-080 | done | 2 | defect | Jakarta Data: a repository method that returns an array of the entity is unsupported | S |  |  |
| MOR-081 | done | 2 | defect | Jakarta Data: the static order comes first, and every dynamic Sort, Sort... and Order counts | S |  |  |
| MOR-082 | done | 2 | defect | Jakarta Data: JDQL — AND binds stronger than OR, literals are text, NOT before BETWEEN/IN/LIKE, IN lists of literals | S |  |  |
| MOR-083 | done | 2 | defect | Jakarta Data: SELECT field answers the values of the field when the method returns them | S |  |  |
| MOR-084 | done | 2 | defect | A short or Short field is stored as a number, not as a Java-serialization blob | S |  |  |
| MOR-085 | done | 2 | defect | InMemoryDriver: a missing value or null sorts last when descending | S |  |  |
| MOR-086 | done | 2 | defect | Jakarta Data: a derived query may return a CursoredPage, one cursor paging for all four callers | S |  |  |
| MOR-087 | done | 2 | defect | Jakarta Data: the keyset of a cursored @Query includes the dynamic sorts | S |  |  |
| MOR-088 | done | 2 | defect | Jakarta Data: COUNT(THIS) is the keyword in any case | S |  |  |
| MOR-089 | done | 2 | defect | Jakarta Data: a one-character string literal compares with a char field as the character | S |  |  |
| MOR-090 | done | 2 | defect | Jakarta Data: an enum in Not, In, NotIn and range operators of a derived query is its name | S |  |  |
| MOR-091 | done | 2 | defect | Jakarta Data: several ORs in one query narrow each other | S |  |  |
| MOR-093 | done | 2 | implementation | Enums can be stored by their ordinal: @Enumerated and an opt-in setting | M |  |  |
| MOR-094 | done | 2 | documentation | README: the Jakarta Data TCK result and our modules added to the entry page, release history of 6.2 and older moved out | S |  |  |
| MOR-095 | done | 2 | verification | Verification: the showcase against this version, and two findings (scram-client pinned by the Quarkus BOM, no LICENSE file) | S |  |  |
| MOR-096 | done | 2 | infrastructure | Quarkus extension: build and test against Quarkus 3.40.1 (was 3.32.3) | S |  |  |
| MOR-011 | done | 3 | implementation | Typed lease / try-acquire primitive | M |  | MOR-010 |
| MOR-042 | done | 3 | implementation | Writer path: marshal values with the shared update-value walk and use one convention for single values | L |  | MOR-002 |
| MOR-047 | done | 3 | defect | MorphiumBulkContext.createUpdateRequest does not marshal update values | S |  | MOR-042 |
| MOR-051 | done | 3 | defect | InMemoryDriver: reject path conflicts in update documents like MongoDB | S |  |  |
| MOR-056 | done | 3 | defect | InMemoryDriver: upsert seeds the equalities of a single-branch $or | S |  |  |
| MOR-057 | done | 3 | defect | InMemoryDriver: an inserted document is stored by reference | S |  |  |
| MOR-061 | done | 3 | defect | Query.dec(field, Integer) turns an int counter into a double | S |  |  |
| MOR-065 | done | 3 | defect | Delete paths ignore the writeErrors of the delete reply | S |  |  |
| MOR-072 | done | 3 | defect | The update builder, findOneAndUpdate(Map), updateOne and updateMany fire no storage-listener update events | M |  |  |
| MOR-073 | done | 3 | defect | Query.push, pull and addToSet fire the update events twice, and the post event is always PUSH | S |  |  |
| MOR-074 | done | 3 | verification | MorphiumMigrationTest (2) and MultithreaddedMessagingTests (1) fail on this machine, with and without the latest changes | S |  |  |
| MOR-092 | done | 3 | defect | Jakarta Data: IgnoreCase before Between, NotIn and the comparison operators | M |  |  |

## Details

### MOR-001 Decide the serialisation approach for update operator maps (options A to E)

Status **done**, priority 1, type analysis, effort S

Compare options A (mapper-aware update builder), B (marshal operator maps in findOneAndUpdate), C (POJO fallback hook in BsonEncoder), D (embedded inherits translateCamelCase) and E (startup guard) using the reproduced results. Expected outcome: B as immediate fix, A as target API, E as guard, D only as opt-in tri-state, C rejected or deferred (driver layer would depend on the mapper, InMemoryDriver would still differ). Record the decision as a short ADR-style note in docs/research/ and adjust the dependent tasks. Also consider that no other client makes embedded naming depend on the owner (other-clients.md pattern 15) and that Spring maps update documents idempotently (pattern 4).

Evidence: docs/research/serialisation-decision.md: verdicts for options A-E, each naming its task.

Acceptance:

- A decision note in docs/research/ lists each option with the evidence from reproduction-results.md (hypotheses 1, 2, 4) and a verdict (adopt / opt-in / reject)
- Every verdict names the ledger task that implements it or states that none is planned

Source: docs/research/reproduction-results.md, docs/research/other-clients.md, docs/research/reproduction-results.md (serialisation analysis)

Commits: ee834df0a

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

Commits: c884c64a4

### MOR-003 findOneAndUpdate with a compound filter and upsert collides on _id (code 66)

Status **done**, priority 1, type defect, effort S

addGeneratedIdOnInsertIfNeeded only looks for _id at the top level of the filter. A compound filter (_id = X AND (a OR b)) is wrapped in $and, so a generated _id is added via $setOnInsert and MongoDB rejects the first insert with code 66 ('would modify the immutable field _id'). The InMemoryDriver silently stores the document under the generated id and inserts another document on every call. Found by the reproduction. A probe on a real MongoDB showed that the server seeds _id from an equality on _id reachable through $and and single-branch $or, from a one-element $in, and from a dotted _id.x key.

Evidence: UpsertFilterInspectorTest 10 tests; ConditionalUpsertReproductionTest.conditionalUpsertViaQueryKeepsFilterId and FindOneAndUpdateCompoundFilterUpsertTest (first acquire inserts under the filter id, held lease gives E11000 on a real MongoDB, 8 contenders: exactly one winner) green on mongo:8 and InMem. Rule derived from the server probe docs/research/upsert-id-seeding-probe.md; multi-branch $or stays out of scope because the server does not seed there.

Acceptance:

- An upsert whose filter contains an equality on _id reachable through $and or a single-branch $or, a one-element $in, or a dotted _id.x key inserts the document with the _id from the filter, on a real MongoDB
- Filters that the server does not seed from (multi-valued $in, multi-branch $or, range operators, regex, $nor) still receive the generated _id and insert without error
- ConditionalUpsertReproductionTest.conditionalUpsertViaQueryGeneratesConflictingId is flipped accordingly

Source: docs/research/reproduction-results.md hypothesis 5, docs/research/upsert-id-seeding-probe.md

Commits: c884c64a4, 2ab9da751

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

Commits: a4885dcfc

### MOR-005 Audit Query.set/inc/push write paths for swallowed writeErrors and missing name translation

Status **done**, priority 2, type verification, effort S

The currentDate reproduction shows that a writeErrors entry (code 52) does not raise an exception on the Query.set path. Check which update paths (set, inc, dec, push, pull, addToSet, unset, bulk) ignore writeErrors, and fix or file the confirmed ones. Also verify the unconfirmed claim that MorphiumWriter.inc(Query, Map) puts keys into $inc without field-name translation (MorphiumWriterImpl ~line 2025). Do not duplicate GitHub #408 (writeConcernError ignored); link it. Finding from MOR-042: MorphiumWriterImpl.inc(Query, Map) puts the keys of the map into $inc without name translation (around line 2025), unlike the single-field inc; verify with a test on a real MongoDB.

Evidence: WritePathAuditContractTest (4 tests, 7 invocations) and docs/research/audit-write-paths.md: 37 write paths of Query, Morphium and the bulk context measured on mongo:8 (v8.3.x, PooledDriver) for raising on a rejected write and for name translation, translation also on the InMemoryDriver. Result: only Morphium.inc(entity) and dec(entity) raise, every other path swallows the writeErrors (MOR-059, priority 1; GitHub #408 is the same family); Morphium.inc(Query, Map), inc(Map<Enum,Number>, Query) and dec(Query, Map) write the Java name, confirming the MOR-042 finding, and dotted paths through an embedded document are translated only in the first segment by Query.set, Query.inc and currentDate (MOR-060); dec with a Map increments (MOR-062); Query.dec(field, Integer) stores a double (MOR-061). Every no has a ledger task; the current behaviour is pinned in the test with the task id and UpdateContractLedgerTest fails when such a task is done. Not covered: writeConcernError (GitHub #408), the async callback variants (same implementation), the Morphium.set/store paths of whole entities, replica sets.

Acceptance:

- A table in docs/research/ lists each update path with: raises on writeErrors yes/no, translates field names yes/no, proven by a test on a real MongoDB
- Every 'no' has a fix task in the ledger or a fix in the same change

Source: docs/research/reproduction-results.md hypothesis 3, docs/research/jakarta-data-1.1.md section 4, GitHub #408

Commits: 0e83869cb

### MOR-006 Option A core: mapper-aware update builder (set, setOnInsert, inc, unset) with findOneAndUpdate terminal

Status **done**, priority 1, type implementation, effort L

Query.modify() returns an UpdateBuilder with set, setOnInsert, inc, unset and upsert and the terminal findOneAndUpdate(ReturnDocument). Each call is mapped immediately through the UpdateDocumentMapper (keys always translated with resolveFieldPath, values in the element form, @Reference and @Encrypted guards), merged per operator and checked for path conflicts. The terminal injects the @Version field (inc when the update modifies, setOnInsert when it only creates; an explicit touch wins; honours isAutoValuesEnabled), rejects an empty builder, setOnInsert without upsert, skip, limit, hint and projection, and runs the existing findOneAndUpdateMapped. Also in this task: arrayFilters support in FindAndModifyMongoCommand and InMemoryDriver.findAndOneAndUpdate, MorphiumDriverException.isDuplicateKey(), and an Instant spike for update documents on the InMemoryDriver. @LastChange and @CreationTime are not applied (MOR-009); updateOne, updateMany and UpdateResult are MOR-050.

Evidence: New: JavaTimeInUpdateDocumentTest 6 (the spike: an Instant in an update document was not readable on the InMemoryDriver, fixed by routing the java.time types through their mappers, shapes equal store() on both drivers), FindOneAndUpdateArrayFiltersAndSortTest 8 (6 failed before the arrayFilters wiring), UpdateOperationsTest 16, MorphiumDriverExceptionDuplicateKeyTest 4, ModifyLeaseAndCounterTest 32, ModifyValuesAndKeysTest 26, ModifyValidationTest 34; all green on PooledDriver (mongo:8, v8.3.11) and InMemoryDriver. Lease: first acquire under the filter _id, held lease isDuplicateKey on both drivers with the document untouched, 8 racing threads on a fresh and on an expired lease have exactly one winner, store() after a builder upsert works and a stale store() is rejected. Mutation check: with the version injection disabled 10 of the new tests fail. Regression subset (63 classes): 851 tests on the InMemoryDriver with 2 errors (ReferenceCascadeTest, Byte Buddy does not support Java 25, the JDK of the run); on a real MongoDB plus InMem 992 tests with ReferenceCascadeTest (4), ExclusiveMessageTests.exclusiveTest/exclusivityTest (fail identically on the commit before PR 3, see MOR-042), CoreFunctionalityTests.cachedObjectOperationsTest (timing, passes when repeated, see MOR-042) and WriterPathWiringTest.currentDateStillDoesNotSetTheField, which pinned the MOR-004 defect and was turned into the positive expectation. Not covered: updateOne/updateMany (MOR-050), @LastChange/@CreationTime (MOR-009), PoppyDB, SingleMongoConnectDriver, replica sets, the Quarkus and Spring modules.

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

Commits: c84172c12, 3549acf79, 4e889b715, 792cc3924

### MOR-007 Option A extension: min, max, mul, rename, currentDate, addToSet/push with each

Status **done**, priority 2, type implementation, effort M

Close the update-operator gaps (no setOnInsert, no each, no min/max/mul/rename on Query) in the builder: typed min/max/mul/rename, a correct currentDate, addToSet(...).each(...) and push(...).each(...) (with PushOptions-like slice/sort/position). Today $each needs a hand-built Map and min/max/mul/rename only exist in the bulk context (MorphiumBulkContext).

Evidence: ModifyOperatorsTest (19 tests, 38 invocations) on mongo:8 and the InMemoryDriver, all reading the raw document: min, max and mul write the stored names and leave no stray field, min and max keep a value that is not beyond, max seeds a missing field and mul makes it 0, rename moves the stored field with both names translated, currentDate sets a BSON date (also in an upsert, also with the enum overload), push and pushEach store the same raw element as Query.push, pushEach with slice, position and sort (whole array and by a field of the elements) gives the arrays mongod gives, addToSetEach stores each element once and marshalled and recognises an element that store() wrote, the update document shows the operators, conflicts (push and addToSet on one field, min and set, a rename onto a set field or onto itself) are rejected at the call, and a max-only update bumps the version and the last change. Mutation check: with the old definition of a modifying operator the version test fails. The builder tests of earlier tasks (262 invocations) stay green. Regression: 1527 test methods (core and messaging) on the InMemoryDriver, all passed. Not covered: pull, pullAll and pop, $bit, array filters inside the builder, and the translation of the field names inside the sort of PushOptions (used as given, documented).

Acceptance:

- Each operator has a test on a real MongoDB that checks the stored document
- addToSet with each of embedded values stores each element once and marshalled
- Verified against a real MongoDB (not only the InMemoryDriver).

Depends on: MOR-006

Source: docs/research/reproduction-results.md (update API gaps), docs/research/other-clients.md pattern 5

Commits: 4617f9763

### MOR-008 Options object for findAndModify and a defined result for upsert without returnNew

Status **done**, priority 2, type implementation, effort S

Replace findOneAndUpdate(Map, boolean upsert, boolean returnNew) by findOneAndUpdate(update, FindAndModifyOptions) with upsert, returnDocument(BEFORE/AFTER), sort, projection, arrayFilters, hint, collation, maxTime. Keep the old signature as a delegating overload. Document and define the result for an upsert that inserts with returnDocument BEFORE (null today, reproduced on both drivers). The arrayFilters wiring of FindAndModifyMongoCommand and InMemoryDriver.findAndOneAndUpdate is done by MOR-006; this task keeps the options object, projection and hint, which the InMemoryDriver currently drops in findAndModify (InMemoryDriver.java around line 4129).

Evidence: ModifyOptionsTest 34 invocations (17 tests, 2 skipped on the InMemoryDriver: unknown hint rejected, collation) and FindAndModifyOptionsTest 5, green on PooledDriver (mongo:8, v8.3.x) and InMemoryDriver: upsert with BEFORE returns null for an insert and the document for AFTER, sort of the options overrides the query sort, projection (include, exclude, Java names translated, _id kept, stored document complete), hint by name and by key document, unknown hint rejected by mongod, array filters, maxTime, collation on mongod, returnDocument required, legacy overload equals the options form, builder terminals (projection and hint through the options, builder upsert wins over upsert(false), the option can request the upsert, tryFindOneAndUpdate(options) tells insert from update), count terminals still reject projection and hint. Mutation check: without the projection in the command the projection tests fail. ConditionalUpsertReproductionTest passes unchanged. Regression (update, upsert, builder, lease, contract and reproduction classes, 521 invocations) green on both drivers. The InMemoryDriver applies the projection (inclusion, exclusion, dotted paths; no $slice/$elemMatch) and ignores hint and maxTimeMS. Not covered: findOneAndReplace/findOneAndDelete options, PoppyDB, SingleMongoConnectDriver.

Acceptance:

- The options object is available for the builder and the raw Map path
- The null result of an inserting upsert with BEFORE is documented in the Javadoc and covered by a test on a real MongoDB
- ConditionalUpsertReproductionTest.upsertInsertWithReturnNewFalseReturnsNull still passes or is flipped deliberately
- Verified against a real MongoDB (not only the InMemoryDriver).

Depends on: MOR-006

Source: docs/research/reproduction-results.md hypothesis 5, docs/research/other-clients.md pattern 6

Commits: 35f991906

### MOR-009 Lifecycle handling in the builder: @LastChange, @CreationTime, @Version

Status **done**, priority 2, type implementation, effort M

@LastChange and @CreationTime in the builder: findOneAndUpdate through the builder (MOR-006) skips pre/post update events, @LastChange and @CreationTime; Query.set applies them. Add them to the builder on the mapped document, honouring isAutoValuesEnabled. The @Version injection is done by MOR-006. State the event contract in the Javadoc.

Evidence: ModifyAutoValuesTest 22 invocations (11 tests, PooledDriver mongo:8 and InMemoryDriver): last change refreshed by update, inc and unset; creation time only on insert and never changed by an update; an upsert that inserts sets both from one clock reading; a setOnInsert-only update sets both on insert and touches nothing (also not the version) on an existing document; explicit touches win (incl. an explicit unset); Query.disableAutoValues and the thread switch skip both; Long/long, Date, String (dateFormat), LocalDateTime and Instant fields; an unsupported field type is rejected and nothing is written; toUpdateDocument shows the injection; store() afterwards works. Mutation check: with the injection disabled 10 of the 11 tests fail on the InMemoryDriver. Regression subset (64 classes): 862 tests on the InMemoryDriver with 2 errors (ReferenceCascadeTest, Byte Buddy does not support Java 25); on a real MongoDB plus InMem 1014 tests with ReferenceCascadeTest, ExclusiveMessageTests.exclusiveTest/exclusivityTest and CoreFunctionalityTests.cachedObjectOperationsTest (all known, see MOR-042) and LastAccessTest.testLastAccessInc, which is intermittent: it failed 2 of 3 repetitions on this branch and 1 of 4 on a clean develop worktree (it exercises setInEntity, not the builder).

Acceptance:

- A builder write on an entity with @LastChange sets the field, and an upsert on an entity with @CreationTime sets it only on insert, on a real MongoDB
- isAutoValuesEnabled false skips both

Depends on: MOR-006

Source: docs/research/reproduction-results.md hypothesis 5, docs/research/other-clients.md patterns 7, 8

Commits: 814dcac7c

### MOR-010 Typed duplicate-key exception and typed result for a lost conditional upsert

Status **done**, priority 2, type implementation, effort M

A conditional upsert on an existing but non-matching document throws MorphiumDriverException with mongoCode 11000 (Integer, writeErrors null); a store() duplicate reports the code inside writeErrors with mongoCode null. Add a typed exception or flag for duplicate key independent of the path, and a typed outcome (acquired / held) for the conditional upsert so callers need not parse getMongoCode(). Align with the exception model work in GitHub #210 and #211. MorphiumDriverException.isDuplicateKey() is added by MOR-006; this task adds the lastErrorObject to the findAndModify result (FindAndModifyMongoCommand.execute drops it today) and the typed outcome.

Evidence: ModifyOutcomeTest 28 invocations (14 tests, one skipped on the InMemoryDriver), FindAndModifyLastErrorObjectTest 10 and UpdateOutcomeTest 5, green on PooledDriver (mongo:8) and InMemoryDriver: lastErrorObject (updatedExisting, upserted, n) in the reply for update, inserting upsert with BEFORE and AFTER, and no match; INSERTED with BEFORE has no document but the upserted id; a lost conditional upsert is DUPLICATE_KEY without any code inspection and leaves the holder untouched; a duplicate key without upsert (unique index) and other failures are still thrown; a race of eight contenders yields exactly one INSERTED and seven DUPLICATE_KEY; findOneAndUpdate unchanged. The depends_on on MOR-008 was dropped: the typed outcome needs neither the options object nor the projection; arrayFilters came with MOR-006. Regression subset (72 classes plus the inmem package): 919 tests on the InMemoryDriver with the 2 known ReferenceCascadeTest errors (Byte Buddy does not support Java 25); on mongo:8 716 tests with the same ReferenceCascadeTest errors and the known flaky ExclusiveMessageTests x2; UpdateDocumentMapperKeyTest and WriterMarshallingDifferentialTest need both drivers in one run and pass with pooled,inmem. Known difference not addressed: the InMemoryDriver does not enforce the immutability of _id nor reject $inc on a string field. Not covered: PoppyDB, SingleMongoConnectDriver, replica sets.

Acceptance:

- A single predicate/exception recognises duplicate key for both the findAndModify path and the writeErrors path, tested on a real MongoDB
- The conditional upsert returns or throws a typed 'held' outcome that tests assert without inspecting codes
- Verified against a real MongoDB (not only the InMemoryDriver).

Depends on: MOR-006

Source: docs/research/reproduction-results.md hypothesis 5, docs/research/other-clients.md pattern 9, GitHub #210, #211

Commits: ae5bba8ae

### MOR-011 Typed lease / try-acquire primitive

Status **done**, priority 3, type implementation, effort M

Applications implement the same lease pattern by hand (job lease, outbox relay lease, command outcome): conditional upsert with filter (expiresAt < now OR owner = me), E11000 meaning 'held'. Offer a small typed primitive or documented recipe built on MOR-010.

Evidence: LeaseManagerTest 22 invocations (11 tests) and LeaseResultTest 4, green on PooledDriver (mongo:8) and InMemoryDriver: acquire inserts, a held lease is HELD and untouched, renewal moves the deadline and the version, takeover after expiry, release frees it for another owner, a stranger or an unknown id cannot release, Instant and Date deadline fields, invalid definitions and arguments are rejected, eight contenders on a fresh lease (five rounds) and on an expired lease have exactly one winner on both drivers. Mutation check: without the owner branch of the filter the renewal test fails. Builder, outcome, lease and upsert suites together (256 invocations) were rerun on both drivers without a failure. Not covered: natural-key leases (not the @Id), PoppyDB, SingleMongoConnectDriver, replica sets, clock skew between hosts.

Acceptance:

- A documented primitive or recipe with a concurrency test (two contenders, one winner) on a real MongoDB
- Verified against a real MongoDB (not only the InMemoryDriver).

Depends on: MOR-010

Source: update-path research, 2026-10-15

Commits: 8dfd9f71b

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

Status **done**, priority 2, type implementation, effort S

Flag an @Embedded class with default translateCamelCase that is nested in an @Entity with translateCamelCase=false. Reproduced: nested keys silently become snake_case (file_name, item_version_id, ...). Non-breaking: warn (and offer a strict mode) at entity registration; also expose a test helper that asserts no snake_case key in a stored document.

Evidence: EmbeddedNamingGuardTest (7 tests, 14 invocations) on mongo:8 and the InMemoryDriver: the analysis names owner, field and embedded class; an embedded with translateCamelCase=false, a translating owner and transient, @Transient and @Reference fields give no finding; fields behind a List, a Map and an array and a mismatch nested in an embedded owner are found; a fresh mapper warns exactly once per entity class with a message that names both classes and the field; the stored keys of such an entity are unchanged (owner keeps ownerName, the embedded is stored as file_name) and the document reads back; strictEmbeddedNaming throws an IllegalStateException with the same names and is off by default. StoredDocumentKeysTest (3) covers the helper, MorphiumConfigTest the setting through properties and copy. EmbeddedCamelCaseReproductionTest keeps documenting the unchanged storage. Mutation check: without the call of the check 4 of the 14 invocations fail. Two details found on the way: Morphium looks at every entity class of the classpath at startup, so the warning is logged then, and the analysis skips interfaces (the mapper checks internal classes in its constructor). Regression: 1538 test methods (core and messaging) on the InMemoryDriver, all passed. Not covered: the opposite combination (a translating owner with an embedded that opts out), which the research documents but the task does not flag, and a check at registration of classes that are not used.

Acceptance:

- Registering such an entity logs a warning naming both classes, tested
- An embedded with its own translateCamelCase=false produces no warning
- EmbeddedCamelCaseReproductionTest keeps documenting the unchanged storage behaviour

Depends on: MOR-001

Source: docs/research/reproduction-results.md hypothesis 2, docs/research/reproduction-results.md (option E)

Commits: dfe3f118c

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

Status **done**, priority 2, type implementation, effort M

DELETE FROM is part of Jakarta Data 1.0 (not 1.1). JdqlParser only accepts SELECT/WHERE/GROUP BY/HAVING/ORDER BY. Add DELETE FROM [WHERE] returning void/int/long (matched count) in JdqlParser, the Spring proxy path and MorphiumDataProcessor. Query.delete() already returns n. State whether lifecycle callbacks run (derived deleteBy skips them).

Evidence: JdqlDeleteTest (11 invocations: statement recognition, entity name and WHERE clause split, malformed statements, then on the InMemoryDriver and on mongo:8 with the pooled driver: delete by named parameters returns the count and removes only the matches, no WHERE removes all, ordinal parameters work, zero matches, another entity name and ORDER BY are refused and delete nothing). Quarkus: the build generates the method for void, int and long and refuses boolean, Long, another entity name, a Sort parameter, ORDER BY and an ordinal without a parameter (MorphiumDataProcessorJdqlDeleteTest, 3 tests); AbstractMorphiumDataJdqlDeleteTest (long, int with ordinals, void) runs on the InMemoryDriver and on a real mongo:8 from Dev Services. Spring: three proxy tests (long, int with ordinals, void) in MorphiumRepositoryProxyTest, a wrong return type fails when the proxy is created. Whole suites: jakarta-data 118, Quarkus runtime 83, deployment 34, integration 278, Spring 28. Lifecycle: the delete runs no @PreRemove/@PostRemove callbacks of the entities (there is no instance), the storage listeners get preRemove(Query) and postRemove(Query); documented in docs/jakarta-data.md and the Javadoc. Not covered: the Spring tests run on the InMemoryDriver only (the bridge runs on mongo:8 in the jakarta-data module), async (CompletionStage) DELETE methods are refused by the return type rule, UPDATE statements (MOR-018), the listener events were not asserted by a test.

Acceptance:

- A @Query("DELETE FROM Entity WHERE ...") method works in the Quarkus and the Spring module against a real MongoDB
- Return types void, int and long are supported; other types are rejected at build time
- Verified against a real MongoDB (not only the InMemoryDriver).

Source: docs/research/jakarta-data-1.1.md section 1

Commits: 8b3b80eda

### MOR-018 Jakarta Data: JDQL UPDATE ... SET ... WHERE as one atomic update

Status **done**, priority 2, type implementation, effort L

UPDATE is part of 1.0. SET items are field = expression or NULL; no increment operator, an increment is x = x + ?1. Map to one atomic command: assignment to $set, NULL to $unset, x = x + :n to $inc, x = x * :n to $mul, built through the builder so names and values are mapped. Return the matched count (n, not nModified). Arithmetic on other fields, '||' and string functions need pipeline updates (FindAndModifyMongoCommand has a pipeline field that Query does not expose): reject them at build time with a clear message, or handle in a follow-up. JNoSQL rejects arithmetic entirely, so this goes beyond it.

Evidence: JdqlUpdateTest (16 tests with the driver variants, 21 invocations): the parser recognises UPDATE, splits entity, items and WHERE (also ordinals, no WHERE, no entity name, a comma and a keyword inside a string literal) and refuses another field on the right side, division, ||, functions, a non-number operand, a field set twice and an empty SET, naming the item; then on the InMemoryDriver and on mongo:8 with the pooled driver: SET a = :x, n = n + 1 WHERE ... changes exactly the matching documents, returns the matched count (also for a document that already has the value), minus, times, a literal and NULL work, no WHERE updates all, zero matches is zero, a wrong entity name, a non-number for + and a missing argument are refused and change nothing, and @Version grows by one. On the real MongoDB a statement with four operators (SET, $inc, $mul, $unset) raises opcounters.update by exactly 1: one update command. Quarkus: the build generates void, int and long and refuses an item that is not one update command, boolean, another entity (MorphiumDataProcessorJdqlDeleteTest, 3 more tests); AbstractMorphiumDataJdqlUpdateTest (set and increment with the matched count, multiply with ordinals and int, literal and NULL with void) runs on the InMemoryDriver and on a real mongo:8 from Dev Services. Spring: three proxy tests (long, int with ordinals, void). Found on the way: the build and the proxy check the statement as written, so the parser has to accept ?n operands. Whole suites: jakarta-data 134, Quarkus runtime 83, deployment 37, integration 284, Spring 31. Not covered: the Spring tests run on the InMemoryDriver only, a replica set, UPDATE with a pipeline (refused by design), a SET on an embedded path was not tried, the unset of a primitive field reads back as its default.

Acceptance:

- A method with SET a = :x, n = n + 1 WHERE id = :id issues exactly one update command on a real MongoDB
- Return value equals the matched count
- Unsupported expressions fail at build time
- Verified against a real MongoDB (not only the InMemoryDriver).

Depends on: MOR-006, MOR-007

Source: docs/research/jakarta-data-1.1.md sections 1, 4, docs/research/other-clients.md pattern 12

Commits: a7920d6a2

### MOR-019 Jakarta Data: reject unsupported @Find/@Delete parameter styles at build time

Status **done**, priority 2, type defect, effort S

FindMethodBridge and MorphiumDataProcessor turn every @Find/@Delete parameter into an equality. With Jakarta Data 1.1 an @Is(AtLeast.class) parameter would silently run as eq and a Restriction parameter would be treated as an attribute. Reject @Is, Constraint, Restriction and @First usage with a clear build error until MOR-020 supports them.

Evidence: UnsupportedParameterStyles (morphium-jakarta-data) checks class names, so it needs no Jakarta Data 1.1 API: @Is on a parameter, a parameter of a jakarta.data.constraint or jakarta.data.restrict type, @First on a method (UnsupportedParameterStylesTest, 2 tests). Quarkus: MorphiumDataProcessor.rejectUnsupportedParameterStyles runs for every @Find, @Delete and @Query method and fails the build with a message that names the method and the way out; MorphiumDataProcessorJdqlDeleteTest has a test with @Is on a @Find and on a @Delete, a Restriction on a @Find and @First on a @Query (each names the method) and one that a plain @Find is generated. The 1.1 types do not exist in the 1.0 API of the build, so the tests use stand-in annotations and types with the same names in the test sources. Spring: the repository is checked when its invocation handler is created, not at the first call (UnsupportedParameterStylesTest in the Spring module, 4 tests: @Is on @Find and @Delete, Restriction, @First on @Query, and a repository without them is fine). Whole suites: jakarta-data 118 (without the MongoDB variants), Quarkus runtime 83, deployment 38, integration 284, Spring 35. Docs: docs/jakarta-data.md and CHANGELOG. Not covered: derived query methods (findBy...) with such parameters are not checked, the stand-in types would clash with the real ones once the build uses Jakarta Data 1.1 (they can then be deleted), and the Spring check covers the methods of the interface that carry @Find, @Delete or @Query.

Acceptance:

- A repository with an @Is or Restriction parameter fails the Quarkus build with a message naming the method (processor test)
- The Spring proxy path throws at startup with the same message

Source: docs/research/jakarta-data-1.1.md section 4

Commits: 39b01aed2

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

Status **done**, priority 2, type implementation, effort L

The Jakarta Data spec has no upsert or find-and-modify annotation, so this is a vendor extension. Provide an annotation pair in morphium-jakarta-data and MorphiumDataProcessor: a count-returning update and a document-returning find-and-modify with upsert, returnDocument and sort, using structured set / setOnInsert / inc arrays of field and parameter pairs (compile-time field-name check) instead of JSON strings. Return-type convention: numeric, boolean or void gives the count form, entity or Optional gives the find-and-modify form, collections are rejected. Parameters are bound through the builder (MOR-006).

Evidence: AtomicUpdateTest (11 invocations on the InMemoryDriver and on mongo:8 with the pooled driver): the lease of the task is ONE conditional upsert that acquires (fencing token 1, createdBy set on insert only), loses against the holder (empty, nothing changed), renews (token 2) and takes over after the expiry (token 3, createdBy stays); BEFORE returns the old state and null for an upsert that inserts; without upsert nothing is created; an order picks the document; the count form returns matched or inserted, 0 for a lost upsert, and many updates all matches; a missing argument and a non-number for inc are refused and change nothing. Quarkus: the build generates both forms with void, boolean, int, long, the entity and Optional, and refuses Collection, Long, Stream, an unknown field, a parameter that does not exist (assignment and condition), a condition that is not valid JDQL, ordinals in the condition, no assignment, param and value together, a text for inc, setOnInsert without upsert, many on the document form, an order on the count form and a Sort parameter, each naming the method (MorphiumDataProcessorAtomicUpdateTest, 5 tests). AbstractMorphiumAtomicUpdateTest (lease lifecycle through Optional, the boolean and int count forms, the entity with BEFORE) runs on the InMemoryDriver and on a real mongo:8 from Dev Services. Spring: two proxy tests and a test that a wrong @AtomicUpdate refuses the repository when it is created. Documented as non-portable in docs/jakarta-data.md, the Quarkus README and the CHANGELOG. Found on the way: JdqlParser takes whatever stands after the operator as the operand, so WHERE id == :id is the condition id = "= :id" and matches nothing silently; JdqlOperands now refuses that for @AtomicUpdate and for DELETE and UPDATE statements (JdqlOperandsTest), the parser itself is MOR-075. Whole suites: jakarta-data 149 (with the MongoDB variants), Quarkus runtime 83, deployment 43, integration 290, Spring 38. Later checks: the declared lease answers like the LeaseManager step by step (AtomicUpdateTest), atomic updates on a user object work (counter, guarded state change, create if absent), parameters are bound by name without @Param (Quarkus integration test, the build keeps the names), and on a replica set (mongo:8, AtomicUpdateTransactionTest, needs MORPHIUM_TEST_MONGO_RS) a conditional update without upsert runs INSIDE a Morphium transaction: an abort takes it back, a commit keeps it, a stale token is false and leaves the transaction usable, and a takeover from another instance waits for the commit. Not covered: sort for the count form (refused by design), @AtomicUpdate on a derived-query name, ordinals in the condition (refused), the Spring tests run on the InMemoryDriver only.

Acceptance:

- A repository method acquires a lease with one atomic upsert on a real MongoDB without touching the driver API
- Invalid return types fail at build time
- Documented as non-portable in docs/jakarta-data.md
- Verified against a real MongoDB (not only the InMemoryDriver).

Depends on: MOR-008, MOR-010

Source: docs/research/jakarta-data-1.1.md section 3, docs/research/other-clients.md patterns 10, 13

Commits: cdb6f5153

### MOR-075 JdqlParser accepts an operand that is neither a parameter nor a literal

Status **open**, priority 3, type defect, effort S

Found by MOR-021. JdqlParser takes whatever stands after the operator as the operand: WHERE id == :id is the condition id = "= :id", WHERE price >> 5 is price > "> 5", WHERE id = :id garbage is an operand "id garbage" that fails at the call. Such a condition matches nothing and nobody tells; in a SELECT it returns an empty list. JdqlOperands (MOR-021) refuses it for @AtomicUpdate, DELETE and UPDATE at build time; a plain @Query SELECT and the parser itself still accept it. Make the parser throw a JDQL parse error with the position, like it does for a condition it cannot split.

Acceptance:

- JdqlParser.parse throws a parse error with the position for the three examples above
- The existing JdqlParserTest cases still pass

Source: morphium-jakarta-data/src/main/java/de/caluga/morphium/data/JdqlOperands.java, morphium-jakarta-data/src/main/java/de/caluga/morphium/data/JdqlParser.java

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

Status **done**, priority 2, type defect, effort S

MorphiumDataProcessor rejects ordinal parameters, but the spec and TCK use them (SET length = length + ?1).

Evidence: JdqlOrdinalParametersTest (7: detection, rewrite, string literals are skipped, binding skips Sort/Order/PageRequest/Limit, out-of-range and ?0 rejected, mixed styles, highest ordinal) and JdqlOrdinalParametersQueryTest (4 tests, 8 invocations: ?1/?2 bound by position, one ordinal used twice and out of order, BETWEEN, IN and LIKE, a Limit before the first parameter is not counted, a missing argument and mixed styles rejected) on the InMemoryDriver and on mongo:8 with the pooled driver. Quarkus: the build no longer refuses ?1 (MorphiumDataProcessor.validateJdqlSyntax now rejects mixed styles and an ordinal without a parameter, Sort/Order/PageRequest/Limit not counted); AbstractMorphiumDataOrdinalQueryTest in the integration tests (4 tests: ?1/?2, BETWEEN, a Sort parameter is not counted, ordinals out of order) runs on the InMemoryDriver and on a real mongo:8 from Dev Services. Spring: two proxy tests (ordinals by position, Sort skipped) in MorphiumRepositoryProxyTest; the Spring module (25) passes. Whole suites: jakarta-data 107, Quarkus runtime 82, deployment 31, integration 272. Docs: docs/jakarta-data.md, the Quarkus README, CHANGELOG. Decision to confirm: ordinal n counts the method parameters without Sort, Order, PageRequest and Limit; the Jakarta Data text was not available offline, so this is not checked against the TCK. Not covered: the Spring tests run on the InMemoryDriver only (they use the same bridge that runs on mongo:8 in the jakarta-data module), and ordinals in UPDATE statements (MOR-018: the parser has no UPDATE yet).

Acceptance:

- @Query methods with ?1 style parameters work in the Quarkus and Spring module against a real MongoDB
- Verified against a real MongoDB (not only the InMemoryDriver).

Source: docs/research/jakarta-data-1.1.md section 4

Commits: 69581ba25

### MOR-025 Jakarta Data: @Update is not atomic and throws IllegalStateException

Status **done**, priority 2, type defect, effort M

@Update does findById then store(): an extra round trip, not atomic, and a missing entity or version mismatch raises IllegalStateException instead of jakarta.data.exceptions.OptimisticLockingFailureException (AbstractMorphiumRepository ~line 324). Align the exception type with the spec and use a versioned filter update if the entity has @Version.

Evidence: AbstractMorphiumRepositoryOptimisticLockingTest (5 tests, 10 invocations) on the InMemoryDriver and on mongo:8 with the pooled driver (MORPHIUM_TEST_MONGO): a versioned entity is updated and its version grows, a stale version is an OptimisticLockingFailureException that changes nothing, a versioned entity that was deleted is one too and is not created again, an unversioned missing entity is one and is not created, updateAll rejects a batch with a missing unversioned entity before it stores any and raises the same exception for a stale version. Against the old code 8 of the 10 fail. AbstractMorphiumRepositoryUpdateTest now expects the new exception; the whole jakarta-data module (92) passes with the mongo:8 variant, and the Quarkus runtime (82), deployment (31) and integration suites (264) pass (one run had two timing failures in MorphiumMigrationTest, which passed alone and in the repeated full run). Fix in AbstractMorphiumRepository: a versioned entity goes to the versioned store() alone and its VersionMismatchException becomes the Jakarta exception, so the extra findById is gone for it; an unversioned entity is still looked up first, which stays non-atomic (documented in docs/jakarta-data.md and the Javadoc). Not covered: updateAll with a stale version leaves the entities stored before it updated, a replica set, the async repository variants.

Acceptance:

- A stale-version @Update throws OptimisticLockingFailureException, tested on a real MongoDB
- A missing entity behaves as the spec describes, tested
- Verified against a real MongoDB (not only the InMemoryDriver).

Source: docs/research/jakarta-data-1.1.md sections 3, 4

Commits: 998dda853

### MOR-026 Run the Jakarta Data TCK against morphium-jakarta-data

Status **done**, priority 2, type infrastructure, effort M

No jakarta.data-tck dependency exists in any pom. Micronaut Data has a jakarta-data-tck/mongodb module as a model. Add the TCK matching the implemented API version and run it against a real MongoDB and PoppyDB/InMemoryDriver.

Evidence: JakartaDataTckTest (Spring module, replaces the spike) runs the 73 standalone assertions of jakarta.data-tck 1.0.0 without Arquillian and fails when a test that is not excluded fails or an excluded test passes. all 73 pass on the InMemoryDriver and on a real mongo:8 (PooledDriver, port 27099), with morphium.enum-type=ORDINAL (MOR-093) for the four tests that sort an enum by its ordinal; there are no exclusions. The test is part of the normal test run of the Spring module, so CI runs it wherever that module is built. Documented in docs/jakarta-data.md (Conformance). Fixed on the way, one task per cause: MOR-076, 078 to 091.

Acceptance:

- A documented command runs the TCK; the list of excluded tests has a reason per entry
- The TCK runs in CI or a task for CI is added to this ledger
- Verified against a real MongoDB (not only the InMemoryDriver).

Source: docs/research/other-clients.md pattern 14

Commits: 1a5ef1ca5

### MOR-076 Jakarta Data: derived queries without By (countAll and similar) are unsupported

Status **done**, priority 2, type defect, effort S

Found by the TCK spike of MOR-026. The Jakarta Data TCK repository NaturalNumbers declares long countAll() without an annotation, and its tests call it in @BeforeEach, so every standalone TCK test fails on it. MethodNameParser only knows find/count/exists/delete followed by By; the Quarkus processor and the Spring handler throw "Unsupported repository method" for countAll. Check which no-By forms the specification names (countAll, findAll, existsAll, deleteAll) and support them in both modules.

Evidence: The TCK repositories were searched for derived names without By: countAll() is the only one that is not annotated (findAll and deleteAll are CRUD names, the others carry @Query or @Find). MethodNameParser.parse("countAll") is a count without conditions (MethodNameParserTest, red without the change); the Quarkus processor and the Spring handler route countAll to the derived-query generation. Quarkus: OrderRepository.countAll() counts all documents on the InMemoryDriver and on a real mongo:8 (AbstractMorphiumDataOrdinalQueryTest, 5 tests each); Spring: countAllHasNoByAndCountsEveryDocument. Suites: jakarta-data 129, Quarkus runtime 83, deployment 43, integration 294 (one failure, MorphiumMigrationTest.renewLockBetweenChangeUnitsAdvancesExpiry, which fails the same without this change on a busy machine, see MOR-074), Spring 41. Only countAll is supported, not existsAll or other invented forms.

Acceptance:

- countAll() and the other no-By forms the specification names work in the Quarkus and the Spring module

Source: morphium-jakarta-data/src/main/java/de/caluga/morphium/data/MethodNameParser.java

Commits: c39dbab99

### MOR-077 Jakarta Data: repositories without a single primary entity are unsupported

Status **done**, priority 2, type verification, effort M

Found by the TCK spike of MOR-026. RepositoryMetadata holds one entity class per repository. The TCK repositories MultipleEntityRepo (lifecycle methods for Box and Coordinate, no BasicRepository) and CustomRepository (no generic parameters) have no primary entity, and the Spring factory refuses them. The specification allows such repositories (the primary entity is determined from the lifecycle methods). Decide: support them, or list them as excluded in MOR-026 with this reason.

Evidence: Decision: supported. Three gaps were behind it: the Spring module knew @Insert/@Save/@Update/@Delete(entities) only by the names insert, save and so on, and its registrar skipped BasicRepository and repositories that extend nothing; a repository without type arguments had no entity; a repository with several entities had one entity for all methods. Now LifecycleBridge runs the lifecycle methods in every shape (one entity, collection, array or varargs; nothing, entity, list, array, boolean, int, long), the primary entity of a repository that extends nothing is the one of its lifecycle methods, and RepositoryEntities / JandexEntities give every method its entity (statement, parameter, result) with AbstractMorphiumRepository.forEntity. The TCK tests needed also: UPDATE ... SET f = f / n (as $mul by the inverse), boolean as the result of DELETE and UPDATE statements, numeric literals with the suffix 0.0d, 7L. Tests: RepositoryEntitiesTest, LifecycleBridgeTest, JdqlUpdateDivisionTest, JandexEntitiesTest, NoPrimaryEntityRepositoryTest (Spring), MorphiumNoPrimaryEntityInMemTest and MorphiumNoPrimaryEntityRealMongoTest (Quarkus, mongo:8). JakartaDataTckTest: 68 of 73, the three exclusions of this task are gone.

Acceptance:

- A decision, and either support or an entry in the TCK exclusion list

Source: morphium-jakarta-data/src/main/java/de/caluga/morphium/data/RepositoryMetadata.java

Commits: 501128543, 2d5f46f47, 0b620126a, c878f62d7, 9aec21b91, 8d3be2673, 869760159

### MOR-027 Documentation page: raw commands, embedded objects and lifecycle contract

Status **done**, priority 1, type documentation, effort S

Write a page (docs/howtos/) on the pitfalls reproduced so far: POJOs in raw update maps, embedded translateCamelCase default, the @Version trap, E11000 on a conditional upsert, null result of an inserting upsert, findOneAndUpdate skipping lifecycle events, raw Maps staying raw by contract. Add it to mkdocs nav. Also correct quarkus-morphium/README.md which names M3 as the latest Jakarta Data milestone (M4 exists) and docs/jakarta-data.md where needed.

Evidence: docs/howtos/update-pitfalls.md (seven pitfalls: raw commands stay raw, POJOs in a raw $set, @Embedded naming under translateCamelCase=false, the @Version trap after $setOnInsert, the duplicate key of a lost conditional upsert, null for BEFORE on an inserting upsert, lifecycle of the query-based updates), each with its reproduction test, in the mkdocs nav; the Quarkus README names M4. New UpdateEntryPointsLifecycleTest (5 tests) on mongo:8 and the InMemoryDriver pins which entry point calls the callbacks and maintains @Version and @LastChange; it found that Query.set and findOneAndUpdate(Map) maintain neither the version nor a field-only @LastChange (MOR-071). docs/jakarta-data.md needed no change (it names no milestone). NOT verified: mkdocs is not installed here, so the build was not run; the nav entry points to an existing file.

Acceptance:

- The page exists, is in the mkdocs nav and each pitfall links to a reproduction test or ledger task
- mkdocs build succeeds

Source: docs/research/reproduction-results.md, update-path research, 2026-10-15

Commits: ef135f9ae

### MOR-028 Contract tests InMem vs real MongoDB for all update paths

Status **done**, priority 1, type infrastructure, effort M

Turn the reproduction tests into a maintained contract suite that runs the same update paths ($set with embedded lists/sets, $setOnInsert, $inc, $addToSet, $currentDate, conditional upsert, raw key names) against both drivers and declares divergences explicitly. The repo has no known-defect annotation; introduce a small one or an assumption convention so divergences are tracked, not hidden. Move the tests out of the repro package when the defects are fixed.

Evidence: Contract suite (FieldOperator, ArrayOperator, UpsertAndError, StatementIsolation and UpdateContractLedgerTest, 26 invocations) with 125 statements run as update command and as findAndModify: green on PooledDriver (mongo:8, v8.3.x) with no pinned deviation, and on the InMemoryDriver with the deviations pinned. The expectations were written from the MongoDB semantics and all matched mongo:8 on the first run. The suite found 31 InMemoryDriver deviations, tracked as MOR-051, MOR-053 to MOR-058; each pin names its task and UpdateContractLedgerTest fails when the task is done or unknown (mutation checked: marking MOR-058 done fails it). Harness mutation check: a wrong expectation fails with the case name. Default build (InMemoryDriver only) stays green. Not covered: $currentDate, $bit, pipeline updates, collation, hint, transactions, the Morphium write paths above the raw statement layer.

Acceptance:

- One test class family runs every update path on PooledDriver and InMemDriver via MultiDriverTestBase
- Each divergence references the ledger task that removes it
- The default build stays green
- Verified against a real MongoDB (not only the InMemoryDriver).

Source: docs/research/reproduction-results.md, update-path research, 2026-10-15

Commits: 98a063ac9

### MOR-029 InMemoryDriver: reject values BsonEncoder cannot encode

Status **done**, priority 2, type defect, effort M

The InMemoryDriver stores POJOs in $set as live Java objects (List, array, Map, Map-in-Map) and, for a Set or a single POJO, stores them and then fails with a ClassCastException wrapped in RuntimeException, leaving a half-written document. A real MongoDB fails with 'Unhandled Data type' up front. Make the InMemoryDriver fail the same way before changing anything. Related to GitHub #336 (unmapped values stored verbatim); link it. After MOR-002 only hand-built commands reach the encoder or the InMemoryDriver with POJOs.

Evidence: UnencodableValuesTest (6 tests, 12 invocations) on mongo:8 and the InMemoryDriver: an update command with a POJO in the six shapes (single value, List, Set, array, Map, Map in Map), a push, addToSet and setOnInsert operand, a findAndModify, an insert and a store fail with a MorphiumDriverException Error sending Request: caused by the RuntimeException Unhandled Data type: and the document is unchanged; the values the encoder takes (Set of strings, LocalDate, Date, UUID, Pattern, byte array, nested lists) are accepted on both drivers. Before the change the five refusal tests failed on the InMemoryDriver only. BsonEncoderRequireEncodableTest (6 tests) checks that requireEncodable agrees with encodeDocument, including the message. The reproduction note for hypothesis 4 is updated. Regression: 1508 test methods (core and messaging) on the InMemoryDriver, all passed. Not covered: Map keys that are not strings, and the update-with-pipeline form of u, which the check walks as a list of documents but no test covers.

Acceptance:

- Update with a POJO inside $set fails identically in type and message prefix on both drivers and leaves the document unchanged
- Reproduction assertions for hypothesis 4 are flipped

Source: docs/research/reproduction-results.md hypothesis 4, GitHub #336

Commits: b3d7d9340

### MOR-030 InMemoryDriver: honour the filter in conditional upsert and raise E11000

Status **done**, priority 1, type defect, effort M

On an existing document whose conditional filter does not match, MongoDB throws E11000 (held lease). The InMemoryDriver silently overwrites the document, so contender B steals the lease of A; with a compound filter it also inserts a new document per call (see MOR-003). Fix the upsert semantics and the error. Related to GitHub #399 (InMemoryDriver update documents); link it.

Evidence: InMemUpsertDuplicateKeyTest 9 tests (findAndModify path, update command path, id pinned by $setOnInsert, matching filter, filter without id, store() still replaces, 8 racing threads with exactly one winner); the pins that documented the overwrite were flipped to the duplicate key expectation (ConditionalUpsertReproductionTest, FindOneAndUpdateCompoundFilterUpsertTest, UpdateKeyTranslationTest) and now hold for PooledDriver (mongo:8) and InMemoryDriver alike. Before the fix 11 of these tests failed on the InMemoryDriver only. Regression: inmem package suite and the writer/mapper/messaging subset, 778 tests, 0 failures, 2 ReferenceCascadeTest errors (Byte Buddy does not support Java 25, the JDK of the run; unrelated). Remaining divergence: the update command path throws on the InMemoryDriver while a real MongoDB returns writeErrors code 11000 (MOR-005).

Acceptance:

- Held-lease scenario: second contender gets the E11000 MorphiumDriverException on both drivers
- ConditionalUpsertReproductionTest conditionalUpsertOnHeldLease asserts the same outcome for both drivers

Source: docs/research/reproduction-results.md hypothesis 5, GitHub #399

Commits: e2a1167a8

### MOR-031 Quarkus: Micrometer / OpenTelemetry metrics for pool and update paths

Status **done**, priority 2, type implementation, effort M

Applications currently write their own connection pool metrics. Provide connection pool metrics and counters/timers for update paths in quarkus-morphium, building on docs/monitoring-metrics-guide.md.

Evidence: The pool, cache and write-buffer meters existed (MorphiumMetricsBinder). New: the timer morphium.operations.duration (MorphiumOperationMetrics, tags database, operation, update_type, entity) for the update operations of the writer path, the switch quarkus.morphium.observability.include-storage-listener-metrics, the section Metrics in docs/quarkus-extension.md and the extension CHANGELOG. MorphiumOperationMetricsTest (7 unit tests: pairing, a failed write does not distort the next one, bounded pending starts, close removes the timers, the binder adds and removes the listener) and MorphiumOperationMetricsWithMorphiumTest (a real Morphium; Query.inc, set and push appear as timers) run on the InMemoryDriver and on mongo:8 (PooledDriver; the server counted the updates). MorphiumMetricsEndpointTest in the integration tests starts a mongo:8 through Dev Services, writes through Query.inc and Query.set and reads /q/metrics: the pool gauges are there and the timer counts exactly 3 inc and 1 set (the dependency quarkus-micrometer-registry-prometheus was added to the integration tests). The whole runtime (82), deployment (31) and integration-tests (256) suites pass. Not covered, filed: the builder, findOneAndUpdate(Map), updateOne and updateMany fire no storage-listener events (MOR-072), push, pull and addToSet fire them twice (MOR-073, the push count in the real-Morphium test is therefore asserted as at least 1). Not done: OpenTelemetry metrics (Micrometer only), the per-host connection tags and the transaction counters of the plan.

Acceptance:

- Metrics appear on the Quarkus metrics endpoint in an integration test against a real MongoDB
- Verified against a real MongoDB (not only the InMemoryDriver).

Source: review of quarkus-morphium

Commits: cf98386fd, ba295b3e7

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

Status **done**, priority 2, type implementation, effort M

The testing module offers only InMemMorphiumTestProfile. Add a MongoDB test resource (container or URI), a reset helper between tests and simple fixtures, so applications can run the contract-style tests of MOR-028.

Evidence: quarkus-morphium-testing now has MongoDB test profile MongoMorphiumTestProfile (pooled driver; a mongo container from Dev Services, or the server named by MORPHIUM_TEST_HOSTS; own database, hooks database() and replicaSet()), MorphiumTestSupport.reset (deletes the documents of all collections, keeps the collections and their indexes, clears the cache) and load (fixtures through Morphium.store), and the JUnit extension ResetMorphiumExtension. AbstractMorphiumTestSupportTest in the integration tests is an application-style test: two tests that leave data behind both start with an empty database, fixtures get ids and are queryable, reset keeps the collections. It runs on the in-memory driver (MorphiumTestSupportInMemTest) and on a real mongo:8 started by Dev Services (MorphiumTestSupportRealMongoTest), 8 tests. Mutation check: with the delete of reset commented out four of the eight fail. The whole runtime (82), deployment (31) and integration-tests (264) suites pass. Docs: section Testing against a real MongoDB in docs/quarkus-extension.md and the extension CHANGELOG. Not verified: MORPHIUM_TEST_HOSTS against an existing server (only the container path ran), a replica set profile, and the reset against a database with unique indexes.

Acceptance:

- An application-style test uses the resource and resets data between tests
- Verified against a real MongoDB (not only the InMemoryDriver).

Source: review of quarkus-morphium

Commits: ce8c34616

### MOR-035 Records and value objects in the mapper

Status **open**, priority 3, type implementation, effort L

Applications keep scalars in entities to avoid {"value": ...} sub-documents. Add record and single-value converter support. Do not duplicate GitHub #116 (records as entities) and #334 (container fields of custom mappers); link them and reuse their scope.

Acceptance:

- A record and a single-value converter round-trip as embedded and scalar values on a real MongoDB
- Verified against a real MongoDB (not only the InMemoryDriver).

Source: update-path research, 2026-10-15, docs/research/other-clients.md pattern 16, GitHub #116, #334

### MOR-036 Re-run the reproduction on other topologies and server versions

Status **done**, priority 2, type verification, effort S

The reproduction ran on a standalone mongo:8 with PooledDriver only. Re-run on a replica set, with auth, with SingleMongoConnectDriver, on PoppyDB and on an older MongoDB (error wording and $-field handling differ), and re-check hypothesis 1 with @Entity and @Reference values.

Evidence: The update path suite (41 classes, 338 invocations, listed in docs/research/reproduction-results.md) ran against the InMemoryDriver, mongo:7.0.45, mongo:8.3.11 and mongo:9.0.4 standalone, mongo:8 with SCRAM authentication, single node replica sets of mongo:8 and mongo:9, and a single node PoppyDB, with the PooledDriver and the SingleMongoConnectDriver. Result: 0 failures on every MongoDB and on the InMemoryDriver, 14 failing methods on PoppyDB with either driver. The matrix is in reproduction-results.md. Findings: the SingleMongoConnectDriver could not connect to a standalone server and kept its connection after a rejected bulk request (MOR-068, fixed); PoppyDB answers all rejections of the update path with error 11000 and ignores ordered (MOR-069), and swallows the errors of a delete (MOR-070); hypothesis 1 for @Entity and @Reference values: findOneAndUpdate and the builder reject an entity for a @Reference field, Query.set and push embed it and the document cannot be read back (MOR-049, pinned in EntityAndReferenceValueReproductionTest). Not run: a replica set with authentication, a three node PoppyDB replica set, TLS, sharded clusters, Atlas, MongoDB before 5.0.

Acceptance:

- reproduction-results.md has a matrix of topology x result

Source: docs/research/reproduction-results.md 'could not verify'

Commits: 2fe674707

### MOR-037 Morphium.simplifyQueryObject loses predicates for repeated operator keys

Status **done**, priority 2, type defect, effort S

Morphium.java simplifyQueryObject merges the operator maps of a query with putAll, so a repeated key such as $or or $and overwrites an earlier predicate and the query silently matches too much. The merge has to combine repeated keys instead of replacing them.

Evidence: SimplifyQueryObjectTest (5 tests, 10 invocations) on mongo:8 and the InMemoryDriver: two $or conditions and two conditions on one field keep the $and, branches with different keys and equal predicates are still flattened, and an upsert-flag inc with two $or conditions updates only the documents that match both and inserts nothing. Without the change 6 of the 10 invocations fail, with it all pass. Regression: 1492 test methods (core and messaging) on the InMemoryDriver; 5 test classes failed (ChangeStreamMonitorHistoryLostBackoffTest, ReadCommandStepDownStubServerTest, SingleMongoConnectionTimeoutTest, WatchResumeTokenTest, DnsSrvResolverTest) and every one of them with Operation not permitted when it opens a server socket or resolves DNS, which the sandbox of that run did not allow; none touches the query object. They are expected to pass without the sandbox restriction and were not rerun. The acceptance names a real MongoDB; the integration case runs on it. Found on the way and filed as MOR-067: Query.toQueryObject puts the or queries into the $nor list when both are used. Not covered: the other writer methods that do not simplify the query (set).

Acceptance:

- A query that adds two $or conditions returns only documents matching both, shown by a test on a real MongoDB

Source: docs/research/plan-mor-001-002-003.md

Commits: 96ac49921

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

Commits: c884c64a4

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

Commits: c0a2aede1

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

Commits: 508f64686, ffc8ba05c

### MOR-043 Query.f should resolve field paths with resolveFieldPath

Status **done**, priority 2, type defect, effort M

Query.f returns the raw whole path after a List or Map segment (even the first segment) and the raw string for entities with @AdditionalData, so a filter on a dotted path can miss the stored name while the update, which translates the leading segments, hits it. Use resolveFieldPath for the dotted branch and decide the AdditionalData behaviour.

Evidence: QueryFieldPathTranslationTest (6 tests, 12 invocations) on mongo:8 and the InMemoryDriver: a filter on a path behind a List (lineItems.label), behind a Map (labelMap.color) and through a single embedded field, a filter and a findOneAndUpdate on the same dotted path address the same stored field, an unknown first segment is still rejected, and an entity with @AdditionalData translates a known name and a known dotted path and takes a free-form key as written. Before the change 8 of the 12 invocations failed. Regression: 1486 test methods (core and messaging) on the InMemoryDriver with only the known flaky MultithreaddedMessagingTests, and the query, key translation and shape parity tests on mongo:8 and the InMemoryDriver. Decision on AdditionalData: free-form keys cannot be rejected, so the known part of a dotted path is resolved leniently with resolveFieldPath. Not covered: a Map key with a dot, and the field operators of MongoFieldImpl, which resolve a name again.

Acceptance:

- A filter and an update on the same dotted path address the same stored field, on a real MongoDB
- Existing Query field tests stay green

Depends on: MOR-041

Source: docs/research/plan-mor-041.md

Commits: f966c6c76

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

Status **done**, priority 2, type defect, effort M

For entities with @WriteBuffer on a real MongoDB the buffered writer passes the values of set, pushPull and pushPullAll to the driver raw, so embedded values, Sets and enums fail or are stored differently from the synchronous writer, and pushPullAll for PULL emits $push (BufferedMorphiumWriterImpl around lines 631-658, 944-985 and 988-1030). It is selected on a real MongoDB only, so the InMemoryDriver tests never see it.

Evidence: BufferedWriterUpdateValuesTest (5 tests, 10 invocations) on mongo:8 and the InMemoryDriver: set with an embedded object, an enum and a Set, push and addToSet of an embedded object, pushAll of embedded objects, pull of an element and pullAll of elements on a @WriteBuffer entity store the documents the synchronous writer stores. Before the change 4 of the 5 failed on mongo:8 (the three embedded cases with Unhandled Data type, pullAll grew the list from [a, b, c] to [a, b, c, a, c]); the InMemoryDriver uses the synchronous writer and passed before. BufferedWriteIsolationTest, WriteBufferCountTest, BufferedWriterConcurrentFlushTest, RaiseOnWriteErrorsTest and the writer path tests are green. Not covered: setInEntity and the other entity variants of the buffered writer, the bulk context (MOR-047).

Acceptance:

- Query.set, push, pushAll and pull on a @WriteBuffer entity store the same documents as on a synchronous entity, on a real MongoDB
- pullAll on a @WriteBuffer entity removes the elements

Depends on: MOR-042

Source: docs/research/plan-mor-042.md

Commits: 29651b94a

### MOR-047 MorphiumBulkContext.createUpdateRequest does not marshal update values

Status **done**, priority 3, type defect, effort S

The bulk context hands the values of its update requests to the driver as given (bulk/MorphiumBulkContext.java around lines 56-64), so embedded values, Sets and enums in a bulk update fail on a real MongoDB or are stored as live objects by the InMemoryDriver.

Evidence: BulkUpdateValuesTest (5 tests, 10 invocations) on mongo:8 and the InMemoryDriver: a bulk set of an embedded object, an enum and a Set, the Map variant of addSetRequest, a push of an embedded object (the same raw document as Query.push), a java.time value (the same raw document as Query.set) and set plus unset stay as expected. Before the change 8 of the 10 invocations failed on both drivers (Unhandled Data type on mongo:8, ClassCastException and live objects on the InMemoryDriver). BulkRaiseOnWriteErrorsTest, WritePathAuditContractTest, BulkOperationTest, CurrentDateReproductionTest and BufferedWriterUpdateValuesTest are green. SendMessagesBulkTest fails on a standalone mongo:8 as before (messaging needs a replica set) and passes on the InMemoryDriver. Not covered: addCustomUpdateRequest, which sends the command map as given.

Acceptance:

- A bulk update with an embedded value, a Set and an enum stores the same documents as Query.set, on a real MongoDB

Depends on: MOR-042

Source: docs/research/plan-mor-042.md

Commits: 32215e4a2

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

Status **done**, priority 2, type implementation, effort M

Add the count terminals to the update builder: updateOne and updateMany through the update command, with a package-private Query.updateMapped that turns writeErrors into a MorphiumDriverException (counts in the message, writeConcernError handled, no int cast of n). UpdateResult is a final class with getMatchedCount (n minus upserted), getModifiedCount, getUpsertedId converted to the @Id type, and isAcknowledged. Sort on updateOne is rejected until the InMemoryDriver honours it and the server capability is checked. Serves the Jakarta Data UPDATE count (MOR-018).

Evidence: ModifyCountTest 32 invocations (16 tests) and UpdateResultTest 5, green on PooledDriver (mongo:8, v8.3.11) and InMemoryDriver: counts for updateMany/updateOne, a no-op update modifies 0, upsert that inserts (matched 0, upserted id converted to String and MorphiumId, filter id kept), a lost conditional upsert and a unique index violation in a multi update are duplicate keys on both drivers (isDuplicateKey), arrayFilters, version incremented on every matched document, cache cleared, rejected options. Mutation check: with the writeErrors normalisation disabled the two duplicate key tests fail on mongo:8. Regression subset (66 classes): 883 tests on the InMemoryDriver with 2 errors (ReferenceCascadeTest, Byte Buddy does not support Java 25); on a real MongoDB plus InMem 1051 tests, the same eight known failures as before (ReferenceCascadeTest, ExclusiveMessageTests x2, CoreFunctionalityTests.cachedObjectOperationsTest, intermittent LastAccessTest.testLastAccessInc, see MOR-042/MOR-009), no new one. Not covered: unacknowledged write concerns (w:0) are only unit tested through UpdateResult, PoppyDB, SingleMongoConnectDriver, replica sets.

Acceptance:

- updateMany reports matched and modified counts, a second identical call reports modified 0, on a real MongoDB and on the InMemoryDriver
- An upsert reports the upserted id and matched 0
- A duplicate key raises an exception with isDuplicateKey() true on both drivers
- arrayFilters work with updateMany

Depends on: MOR-006

Source: docs/research/plan-mor-006.md

Commits: 07e929f6f

### MOR-051 InMemoryDriver: reject path conflicts in update documents like MongoDB

Status **done**, priority 3, type defect, effort S

A real MongoDB rejects an update document that touches the same path twice or a path and its child (set a and set a.b, set and inc on one field) with code 40. The InMemoryDriver accepts them, so raw-map users get a different result per driver.

Evidence: Contract suite (FieldOperator, ArrayOperator, UpsertAndError, StatementIsolation, UpdateContractLedgerTest; 26 invocations, 149 statements, each as update command and as findAndModify) green on PooledDriver (mongo:8) and on the InMemoryDriver; the pins of this task are gone, only MOR-055 and MOR-056 are still pinned. Before the driver change (pins removed, 24 cases added, all of them green on mongo:8) 46 distinct cases failed on the InMemoryDriver. The harness now also compares the collection after an error (.unchanged(), multi update: the documents before the error). Conflict cases (40): set a plus set a.b, set plus inc, set plus unset, parent set plus child unset, push plus pull, set plus setOnInsert, rename target plus set, and an upsert with a conflict inserts nothing. Paths are compared segment by segment on the literal statement. An index of an array next to a $[] or $[id] of the same array (a.$[] against a.0 or a.1.x) conflicts too (40, probed on mongo:8); two positional paths of one array and $ against an index do not. Not detected: two $[id] paths that address the same element (40 at runtime). Added in d017357da. InMemoryDriver, runtests.sh --driver inmem --parallel 8: 2337 test methods; failures only in ReferenceCascadeTest, LazyLoadingTest and ReferenceTest (Byte Buddy does not support Java 25, the JDK of the run), TtlCappedTest.updatingTheTtlFieldPushesOutTheOldExpiryNotJustTheNewOne (fails the same way on the base commit 70b0bce71) and MultithreaddedMessagingTests (timing: another method fails per run on the base commit too, the method that failed here passes alone 3 of 3). No new failure.

Acceptance:

- The InMemoryDriver rejects set a plus set a.b, set plus inc, set plus unset, a parent set plus a child unset and push plus pull on one path with code 40, like mongo:8
- The pinned deviations in the update contract suite that name this task are removed

Source: docs/research/plan-mor-006.md

Commits: 247a3f1d1, 302718c42

### MOR-052 updateLastAccess writes the untranslated Java field name

Status **open**, priority 3, type defect, effort S

Query.findOneAndUpdateMapped ends with updateLastAccess, a second non-atomic update command for @LastAccess entities that sets the Java field name with a Long. With camel case translation the real field is not updated. Read in the code, not reproduced.

Acceptance:

- After a findOneAndUpdate on an @LastAccess entity the stored last-access field is updated under its stored name, on a real MongoDB

Source: docs/research/plan-mor-006.md

### MOR-053 InMemoryDriver: type checks of the update operators ($inc, $mul, $push, $addToSet, $pull, $pop)

Status **done**, priority 2, type defect, effort M

Found by the update contract suite (MOR-028). A real MongoDB rejects $inc and $mul on a string or null and with a non-numeric operand (code 14), $push and $addToSet on a field that is not an array (code 2), $pop on a non-array (code 14) and $pull on a non-array (code 2), and a failing statement changes nothing. The InMemoryDriver ignores $inc, $mul and $pull on the wrong type without an error, applies the other operators of the statement before the failing one, and fails $push, $addToSet and $pop with an exception that is not a MorphiumDriverException. Tests that run on the InMemoryDriver therefore pass with updates that a real MongoDB refuses.

Evidence: Contract suite (FieldOperator, ArrayOperator, UpsertAndError, StatementIsolation, UpdateContractLedgerTest; 26 invocations, 149 statements, each as update command and as findAndModify) green on PooledDriver (mongo:8) and on the InMemoryDriver; the pins of this task are gone, only MOR-055 and MOR-056 are still pinned. Before the driver change (pins removed, 24 cases added, all of them green on mongo:8) 46 distinct cases failed on the InMemoryDriver. The harness now also compares the collection after an error (.unchanged(), multi update: the documents before the error). Type cases: $inc and $mul on a string, on null and with a non-numeric operand (14), $mul of null, $push/$addToSet on a non-array and on null (2), $push with an $each that is no array (2), $pull on a non-array and on null (2), $pop on a non-array and on null (14), a failing statement after a successful operator and a failing multi update (documents before the error kept). UpdateAtomicityTest: the document is restored, a failing multi update still delivers the change stream events of the documents it changed (mutation checked: dispatching the events after the unlock outside the finally block fails the test). Behaviour change: $inc/$mul on an explicit null used to count as 0 and now fails like on mongo:8. InMemoryDriver, runtests.sh --driver inmem --parallel 8: 2337 test methods; failures only in ReferenceCascadeTest, LazyLoadingTest and ReferenceTest (Byte Buddy does not support Java 25, the JDK of the run), TtlCappedTest.updatingTheTtlFieldPushesOutTheOldExpiryNotJustTheNewOne (fails the same way on the base commit 70b0bce71) and MultithreaddedMessagingTests (timing: another method fails per run on the base commit too, the method that failed here passes alone 3 of 3). No new failure.

Acceptance:

- The InMemoryDriver raises a MorphiumDriverException with the same code as mongo:8 for each of these cases, before any change of the document (a multi update stops at the first error without a rollback of earlier documents)
- The pinned deviations in the update contract suite that name this task are removed

Source: docs/research/plan-mor-028.md

Commits: 247a3f1d1

### MOR-054 InMemoryDriver: validate the shape of an update document (unknown operator, mixed fields, _id, names)

Status **done**, priority 2, type defect, effort S

Found by the update contract suite (MOR-028). A real MongoDB rejects an unknown operator and operators mixed with plain fields (code 9), a change of _id (code 66), an empty field name (code 56), $rename onto itself or with a non-string target (code 2) and an arrayFilters identifier that the update does not use (code 9). The InMemoryDriver applies a change of _id, stores a field with an empty or numeric name, ignores a rename onto itself, and fails the other cases with an exception that is not a MorphiumDriverException.

Evidence: Contract suite (FieldOperator, ArrayOperator, UpsertAndError, StatementIsolation, UpdateContractLedgerTest; 26 invocations, 149 statements, each as update command and as findAndModify) green on PooledDriver (mongo:8) and on the InMemoryDriver; the pins of this task are gone, only MOR-055 and MOR-056 are still pinned. Before the driver change (pins removed, 24 cases added, all of them green on mongo:8) 46 distinct cases failed on the InMemoryDriver. The harness now also compares the collection after an error (.unchanged(), multi update: the documents before the error). Shape cases: unknown operator and operators mixed with fields (9, also if nothing matches), a field in front of an operator (52), a modifier that is no document (9), a change of the _id by $set of another value, $unset, $rename and a replacement (66; $set of the same value works), empty field name, empty path segment and trailing dot (56), $rename onto itself, into a child of the source and with a non-string target (2), unused arrayFilters identifier (9). Probed against mongo:8 with mongosh first (codes and the cases without a match). Also: $pop with an operand other than 1 or -1, a fraction or a string (9; 1.0 works), an upsert whose update changes the _id named by the filter (66, also with $setOnInsert, $inc and a replacement; a double for an int is another value). Added in d017357da. InMemoryDriver, runtests.sh --driver inmem --parallel 8: 2337 test methods; failures only in ReferenceCascadeTest, LazyLoadingTest and ReferenceTest (Byte Buddy does not support Java 25, the JDK of the run), TtlCappedTest.updatingTheTtlFieldPushesOutTheOldExpiryNotJustTheNewOne (fails the same way on the base commit 70b0bce71) and MultithreaddedMessagingTests (timing: another method fails per run on the base commit too, the method that failed here passes alone 3 of 3). No new failure.

Acceptance:

- The InMemoryDriver raises a MorphiumDriverException with the same code as mongo:8 for each of these cases and changes nothing
- The pinned deviations in the update contract suite that name this task are removed

Source: docs/research/plan-mor-028.md

Commits: 247a3f1d1, 302718c42

### MOR-055 InMemoryDriver: number and BSON comparison semantics of $inc, $mul, $min, $max and $addToSet

Status **done**, priority 2, type defect, effort M

Found by the update contract suite (MOR-028). $inc on an int that overflows wraps around to a negative int in the InMemoryDriver, a real MongoDB widens to a long. $mul of an int by a double and $min / $max across types (a number against a string) or with an equal value of another number type fail with an exception, a real MongoDB follows the BSON comparison order. $addToSet treats 1 and 1.0 as different values and a document with another field order as equal, a real MongoDB does the opposite. A wrapped counter is silent data corruption in tests.

Evidence: The seven pins of this task are removed from FieldOperatorContractTest and ArrayOperatorContractTest, and the contract suite (83 invocations including the ledger test) passes on mongo:8 and on the InMemoryDriver. Without the driver change 3 of the 6 contract methods fail on the InMemoryDriver. The cases: inc of an int that overflows becomes a long, mul of an int by a double is a double, max and min across types follow the BSON order, max of an equal value of another number type keeps the old value, addToSet treats 1 and 1.0 as equal and a document with another field order as different. Regression: 1486 test methods (core and messaging) on the InMemoryDriver with only the known flaky MultithreaddedMessagingTests. Not covered: Decimal128 and BigInteger operands, the comparison of binary data, ids and regular expressions beyond their type order, and the operators of aggregation expressions.

Acceptance:

- The InMemoryDriver gives the results of mongo:8 for the cases of the update contract suite that name this task
- The pinned deviations in the update contract suite that name this task are removed

Source: docs/research/plan-mor-028.md

Commits: c60b7348a

### MOR-056 InMemoryDriver: upsert seeds the equalities of a single-branch $or

Status **done**, priority 3, type defect, effort S

Found by the update contract suite (MOR-028). A real MongoDB seeds the document of an upsert from the equality of a $or with a single branch, the InMemoryDriver ignores it, so the inserted document lacks the field.

Evidence: The contract case upsert seeds a single-branch $or passes on mongo:8 and the InMemoryDriver (pin removed), and InMemInsertCopyAndOrSeedTest covers a single-branch $or inside an $and and that a multi-branch $or still seeds nothing. Without the driver change 3 of the 5 tests of that class fail. Regression: 1487 test methods (core and messaging) on the InMemoryDriver with only the known flaky MultithreaddedMessagingTests. Not covered: null equalities, which the InMemoryDriver still drops (documented divergence).

Acceptance:

- An upsert with a single-branch $or inserts the seeded field on the InMemoryDriver, like mongo:8
- The pinned deviation in the update contract suite is removed

Source: docs/research/plan-mor-028.md

Commits: de3ca1859

### MOR-057 InMemoryDriver: an inserted document is stored by reference

Status **done**, priority 3, type defect, effort S

Found by the update contract suite (MOR-028). InsertMongoCommand on the InMemoryDriver stores the map the caller passed, so a later update through the driver changes the caller's own map (and a change of the map changes the stored document). A wire driver serialises the document and cannot do either. Morphium.store() is not affected because the mapper creates a new map per call; raw command users and test fixtures are.

Evidence: The contract case anInsertedDocumentIsNotKeptByReference now expects the unchanged document of the caller on every driver (pin removed), and InMemInsertCopyAndOrSeedTest covers a change of the caller map and of a nested document and list after the insert, that the generated _id is not written into the caller map, and that the field order of the stored document is kept. UpdateContractLedgerTest accepts a suite without pinned deviations and tests its pin pattern. Regression as for MOR-056. Not covered: the stored copy of documents inserted through the bulk context, which goes through the same insert method.

Acceptance:

- After an insert and an update on the InMemoryDriver the document the caller passed to the insert is unchanged
- The pinned deviation in StatementIsolationContractTest is replaced by the common expectation

Source: docs/research/plan-mor-028.md

Commits: de3ca1859

### MOR-058 InMemoryDriver: findAndModify does not reject what the update command rejects

Status **done**, priority 2, type defect, effort S

Found by the update contract suite (MOR-028). The InMemoryDriver update command rejects a field name with a leading dollar sign (code 52) and setting a field below a scalar or below an array with a non-numeric segment (code 28), like a real MongoDB. The findAndModify path of the InMemoryDriver, which the lease and counter use cases run through, silently accepts all three: it stores nothing, or leaves the document unchanged, and reports success.

Evidence: Contract suite (FieldOperator, ArrayOperator, UpsertAndError, StatementIsolation, UpdateContractLedgerTest; 26 invocations, 149 statements, each as update command and as findAndModify) green on PooledDriver (mongo:8) and on the InMemoryDriver; the pins of this task are gone, only MOR-055 and MOR-056 are still pinned. Before the driver change (pins removed, 24 cases added, all of them green on mongo:8) 46 distinct cases failed on the InMemoryDriver. The harness now also compares the collection after an error (.unchanged(), multi update: the documents before the error). findAndModify cases: dollar prefixed field name (52), field below a scalar and below an array with a non-numeric segment (28), now also unchanged afterwards, plus UpdateAtomicityTest.findAndModifyFailsWithTheCodeAndInsertsNothingOnUpsert (public findAndOneAndUpdate raises the code, an upsert with an error inserts nothing). A statement without a match is validated too. InMemoryDriver, runtests.sh --driver inmem --parallel 8: 2337 test methods; failures only in ReferenceCascadeTest, LazyLoadingTest and ReferenceTest (Byte Buddy does not support Java 25, the JDK of the run), TtlCappedTest.updatingTheTtlFieldPushesOutTheOldExpiryNotJustTheNewOne (fails the same way on the base commit 70b0bce71) and MultithreaddedMessagingTests (timing: another method fails per run on the base commit too, the method that failed here passes alone 3 of 3). No new failure.

Acceptance:

- findAndModify on the InMemoryDriver raises the same error as mongo:8 for these cases and changes nothing
- The pinned deviations in the update contract suite that name this task are removed

Source: docs/research/plan-mor-028.md

Commits: 247a3f1d1

### MOR-059 Query, entity and bulk write paths swallow the writeErrors of the update reply

Status **done**, priority 1, type defect, effort M

Found by the write path audit (MOR-005), measured on mongo:8. The update command reports a rejected statement (duplicate key, a path below a scalar, a pull on a non-array) in the reply and does not throw. Every path through MorphiumWriterImpl and the bulk context ignores that reply and returns normally: Query.set, set(Map), setEnum, unset, inc and dec (all number types, enum variants), push, pushAll, pull, pullAll, Morphium.addToSet, currentDate, inc/dec with a Map, setInEntity, unsetInEntity, push(entity) and the bulk requests. The caller sees success while nothing was written. Only Morphium.inc(entity) and dec(entity) raise. Related to GitHub #408 (writeConcernError ignored). Query.updateMapped (MOR-050) already normalises the reply and is the model: a writeErrors entry becomes a MorphiumDriverException with the code of the first error, isDuplicateKey() works.

Evidence: Every write path of the audit table raises: WritePathAuditContractTest (4 tests, probes of 37 paths) asserts on mongo:8 (v8.3.x) a MorphiumDriverException with the server code (11000 / 28 / 2), isDuplicateKey() for the unique violations, the write errors and an unchanged document for Query.set/set(Map)/setEnum/unset/inc/dec/push/pushAll/pull/pullAll, Morphium.addToSet/currentDate/inc(Map)/dec(Map), setInEntity/unsetInEntity/push/inc/dec(entity) and the bulk requests (BulkWriteException); RaiseOnWriteErrorsTest (13 tests) and BulkRaiseOnWriteErrorsTest (6): the per-query override and the configuration switch bring the old behaviour back (real MongoDB), an async callback gets onOperationError once and never onOperationSucceeded, a multi update keeps the documents before the error and says so, the post-update event still fires, updateUsingFields and the versioned store paths raise the duplicate key instead of a VersionMismatchException, an ordered bulk stops at the first rejected request and an unordered one runs all of them (partial counts on the exception), a rejected bulk insert raises; WriteReplyErrorsTest (7, pure) for the normaliser including WriteConcernNotSatisfiedException. Regression: 1377 test methods (tags core and messaging) on the InMemoryDriver with only the known failures (LazyLoadingTest, ReferenceCascadeTest, ReferenceTest: Byte Buddy does not support Java 25; MultithreaddedMessagingTests: timing) plus CurrentDateReproductionTest, which pinned the old silent behaviour and was flipped; 1229 invocations of the update, upsert, builder, lease, contract, bulk and reproduction classes on mongo:8 and the InMemoryDriver with the known failures only (ExclusiveMessageTests x2, ReferenceCascadeTest). Follow-up on a single-node mongo:8 replica set (commit 161e9f9a3): WriteConcernErrorTest (4) - Query.set and inc(entity) raise WriteConcernNotSatisfiedException with the data applied, the query override returns the reply with the writeConcernError, a bulk wraps it in a BulkWriteException with the counts; BufferedWriteIsolationTest (2, both drivers, mutation checked) - a rejected collection of the buffered flush does not keep the other one from being written and reports to its callbacks; TransactionTest, ExclusiveMessageTests, ExclusiveMessageBasicTests, AnsweringBasicTests, AdvancedMessagingTests, RaiseOnWriteErrorsTest and BulkRaiseOnWriteErrorsTest are green there. Not covered: a failing statement inside a real transaction beyond TransactionTest, the store() and delete paths (MOR-064, MOR-065).

Acceptance:

- Each path of the audit table in docs/research/audit-write-paths.md raises a MorphiumDriverException when the server rejects the statement, on a real MongoDB
- The pinned deviation MOR-059 in WritePathAuditContractTest is removed
- The behaviour change is in the CHANGELOG (a rejected write used to be silent)

Source: docs/research/audit-write-paths.md, GitHub #408

Commits: c0efc3677, 7d444f31c, 94baaeaa8, 5ebfd40b9, 89011678e, a9451e74b, 6ba62ac72

### MOR-060 Write paths that write the Java field name: inc/dec with a Map and dotted paths through embedded documents

Status **done**, priority 2, type defect, effort S

Found by the write path audit (MOR-005), measured on mongo:8 and on the InMemoryDriver. Morphium.inc(Query, Map), inc(Map<Enum,Number>, Query) and dec(Query, Map) put the keys of the map into $inc without translation (MorphiumWriterImpl.inc(Query, Map) copies the map), so counterValue is written next to the stored counter_value. Query.set, Query.inc and Morphium.currentDate with a dotted path through an @Embedded document (box.innerValue) translate the first segment but write the second as given, next to the stored box.inner_value; the update builder (resolveFieldPath) translates the same path correctly.

Evidence: WritePathAuditContractTest (translation probes, 37 paths) green on PooledDriver (mongo:8, v8.3.x) and InMemoryDriver with no pinned deviation left: inc(Query, Map), inc(Map<Enum,Number>, Query), dec(Query, Map) and the dotted paths through an embedded document of Query.set, Query.inc and Morphium.currentDate write the stored name. Regression (1166 invocations on both drivers) shows only the known failures (ReferenceCascadeTest, ExclusiveMessageTests x2, TransactionTest needs a replica set).

Acceptance:

- The three Map variants and the dotted paths of Query.set, Query.inc and currentDate write the stored name, on both drivers
- The pinned deviation MOR-060 in WritePathAuditContractTest is removed

Source: docs/research/audit-write-paths.md

Commits: fa03a4142

### MOR-061 Query.dec(field, Integer) turns an int counter into a double

Status **done**, priority 3, type defect, effort S

Found by the write path audit (MOR-005). Query.dec(String, Integer) resolves to the Number overload of Morphium.dec, which subtracts amount.doubleValue(), so a decrement of an int or long field stores a double (5 becomes 5.0). dec(field, int) and dec(field, long) keep the type; inc has no such asymmetry.

Evidence: decKeepsTheNumberTypeOfTheAmount on mongo:8 and the InMemoryDriver: dec with an Integer amount keeps an int field an int, a Long amount makes it a long as inc does, a Double amount a double; the Number overloads of Query, Morphium and the entity variants negate in the type of the amount.

Acceptance:

- dec with an Integer or Long amount keeps the number type of the field, on a real MongoDB

Source: docs/research/audit-write-paths.md

Commits: fa03a4142

### MOR-062 Morphium.dec(Query, Map) and dec(Map, Query) increment instead of decrement

Status **done**, priority 2, type defect, effort S

Found by the write path audit (MOR-005), measured on mongo:8 and on the InMemoryDriver. MorphiumBase.dec(Query, Map<String,Number>) and dec(Map<Enum,Number>, Query) pass the amounts to the writer's inc(Query, Map) without negating them, so dec(query, {counter: 1}) adds 1. The single-field dec variants negate.

Evidence: decWithAMapDecrements on mongo:8 and the InMemoryDriver: dec(Query, Map) and dec(Map<Enum,Number>, Query) subtract the amounts and write the stored name; the audit probe that provoked a collision with dec by -1 is raised/unchanged as before.

Acceptance:

- dec with a Map subtracts the amounts, on a real MongoDB and on the InMemoryDriver
- The pinned deviation MOR-062 in WritePathAuditContractTest is removed

Source: docs/research/audit-write-paths.md

Commits: fa03a4142

### MOR-063 quarkus-morphium migration lock: recognise a held lock by isDuplicateKey()

Status **open**, priority 2, type defect, effort S

MorphiumMigrationRunner.acquireLock (quarkus-morphium/runtime) takes the lock with an upsert (Query.set(update, true, false)) and recognises a held lock only by e.getMessage().contains("duplicate key"). Since MOR-059 the write raises a MorphiumDriverException for the lost upsert; the message of the normaliser contains the server text, so the check still matches, but it is fragile, and a WriteConcernNotSatisfiedException (the lock IS written) is rethrown as a failure of the run while the lock stays held for its TTL. Use MorphiumDriverException.isDuplicateKey() and treat WriteConcernNotSatisfiedException as acquired (or release the lock). Released in lockstep with core 6.4.

Acceptance:

- A held lock is reported as "lock held" through isDuplicateKey(), tested against a real MongoDB with two runners
- A write concern error while taking the lock does not leave a lock held by an aborted run

Source: docs/research/plan-mor-059.md

### MOR-064 store() of an existing entity swallows a rejected update (StoreMongoCommand)

Status **done**, priority 2, type defect, effort M

Follow-up of MOR-059, not covered by it: StoreMongoCommand.execute() delegates to UpdateMongoCommand, and the store paths of MorphiumWriterImpl (store, storeList, the non-versioned update half) do not look at the writeErrors of the reply. A unique index violation on the store() of an existing entity is presumably silent (the versioned paths raise since MOR-059). Verify on a real MongoDB, then raise like the other paths (switch, async callbacks, FailedStore for storeList with continueOnError).

Evidence: StoreRaisesOnRejectedUpdateTest (5 tests, 10 invocations, 1 skipped) on mongo:8 and the InMemoryDriver: store() of an existing entity raises the duplicate key and changes nothing, storeList raises and fires no postStore for the rejected entity, storeList(list, collection, true) reports exactly the rejected entity as a FailedStore (index in the original list, MorphiumDriverException with isDuplicateKey) and stores the entities before AND after it (unordered), a successful store still works, with WriterSettings.raiseOnWriteErrors off the rejection is ignored as before (real MongoDB). Regression: 1382 test methods (core and messaging tags) on the InMemoryDriver with only the known failures (LazyLoadingTest, ReferenceCascadeTest, ReferenceTest: Byte Buddy on Java 25; MultithreaddedMessagingTests: timing) plus WriteConcernErrorTest, whose provider was empty in an inmem-only run and now skips; 1263 invocations of the update, store, bulk, contract and reproduction classes on mongo:8 and the InMemoryDriver with the known failures only (ReferenceCascadeTest x2, CoreFunctionalityTests.cachedObjectOperationsTest, timing). The InMemoryDriver reports a rejected statement of an unordered update command in the reply (with its index) and goes on. Not covered: the async-callback store variants (the callback variant rethrows a RuntimeException as before), a write concern error during store.

Acceptance:

- store() and storeList() of an existing entity raise a MorphiumDriverException (isDuplicateKey) for a unique index violation, on a real MongoDB; with continueOnError the FailedStore carries it

Source: docs/research/plan-mor-059.md

Commits: 2209bb92e

### MOR-065 Delete paths ignore the writeErrors of the delete reply

Status **done**, priority 3, type defect, effort S

Follow-up of MOR-059: DeleteMongoCommand is the same hole as the update command; Query.remove and Morphium.delete do not look at the reply (the bulk context does since MOR-059). Verify which server errors a delete can report (a write concern error, a failing collation) and raise them like the update paths.

Evidence: DeleteRaisesOnRejectedDeleteTest (8 tests, 16 invocations, 2 skipped: the write concern tests on the InMemoryDriver) on a single-node replica set (mongo:8) and the InMemoryDriver: a delete with an unknown query operator raises the MorphiumDriverException with code 2 and deletes nothing (Query.remove and Morphium.remove), the async callback gets onOperationError once, the per-query override and the configuration switch bring the reply back, a successful remove of a query, an entity and a list still works, and a delete that misses its write concern (w:2 on one node) raises WriteConcernNotSatisfiedException with the data deleted, for a query and for an entity. The InMemoryDriver now reports an unparsable delete query as code 2 in the reply, like mongod. Regression: 1480 test methods (core and messaging) on the InMemoryDriver, only the known flaky MultithreaddedMessagingTests failed. Not covered: the delete of the bulk context (raises since MOR-059), cascade deletes and the TTL index.

Acceptance:

- A rejected delete raises a MorphiumDriverException, on a real MongoDB

Source: docs/research/plan-mor-059.md

Commits: 0026ad543

### MOR-067 Query.toQueryObject puts the or queries into the $nor list when both are used

Status **done**, priority 2, type defect, effort S

Found while testing MOR-037. Query.toQueryObject builds the $or list and then the $nor list from the same list variable without resetting it when the query has no $and expression: a query with one or() and one nor() becomes {$or: [A, B], $nor: [A, B]}, which matches nothing, instead of {$or: [A], $nor: [B]}. With an $and expression the or branch is fine and the nor branch has the same problem. Reproduced on the InMemoryDriver; the query object is built on the client, so every driver is affected.

Evidence: QueryOrNorTest (4 tests, 8 invocations) on mongo:8 and the InMemoryDriver: a query with or() and nor() builds {$or: [own queries], $nor: [own queries]}, with an $and condition both stay inside the $and, a query with only or() or only nor() is unchanged, and the documents that the query returns from mongo:8 and from the InMemoryDriver are the ones that match the or and not the nor. Without the change 5 of the 8 invocations fail (the query object contained the or and the nor queries in both lists). Regression: 1496 test methods (core and messaging) on the InMemoryDriver, all passed.

Acceptance:

- A query with or() and nor() produces an $or with its own queries and a $nor with its own queries, shown by a test on a real MongoDB

Source: morphium-core/src/main/java/de/caluga/morphium/query/Query.java

Commits: 92455e3fd

### MOR-068 SingleMongoConnectDriver: cannot connect to a standalone server, and a rejected bulk request keeps its only connection

Status **done**, priority 2, type defect, effort S

Found by the topology matrix of MOR-036. (1) connect() compared the host seed with HelloResult.getHosts(), which a standalone server does not send: a NullPointerException, reported as max retries exceeded, so the driver could not be used with a standalone mongod at all. (2) The bulk context of the driver claimed its one connection per request and released it only on success; a rejected insert throws from execute(), the connection stayed claimed and the next call of the driver waited until its timeout (MOR-059 made rejections throw more often). Not seen before because the test base excludes the SingleMongoConnectDriver unless it is asked for.

Evidence: SingleConnectDriverTopologyTest (connect, store, inc, set, read) passes with the SingleMongoConnectDriver on a standalone mongo:8, a single node replica set and a mongo:8 with authentication; before the change it failed on the standalone server with the NullPointerException in connect(). BulkRaiseOnWriteErrorsTest, RaiseOnWriteErrorsTest and BufferedWriterUpdateValuesTest pass with the driver (before, aRejectedInsertIsRaisedToo ran into the 5 minute timeout because the next query waited for the connection that the rejected insert kept). The update path suite (41 classes, 338 invocations) with the SingleMongoConnectDriver: green on mongo:8 standalone. Found on the way and fixed in the tests: RaiseOnWriteErrorsTest kept the only connection of the driver while it queried, and two buffered writer tests called dropCollection for a @WriteBuffer entity, which is queued and runs asynchronously, so a late drop could delete the document of the test. Not covered: a replica set with authentication (the topology matrix has authentication on a standalone server only).

Acceptance:

- The SingleMongoConnectDriver connects to a standalone server, to a replica set and to a server with authentication, and writes and reads
- A rejected bulk request does not block the next call of the driver

Source: docs/research/reproduction-results.md (topology matrix)

Commits: 23754ea9b

### MOR-069 PoppyDB: the update fast path reports every error as 11000 and ignores the ordered flag

Status **open**, priority 2, type defect, effort S

Found by the topology matrix of MOR-036 (update contract suite against a single node PoppyDB, PooledDriver). MongoCommandHandler.processUpdateDirect catches every exception of driver.update and writes writeErrors with the fixed code 11000, although the driver sets the code of mongod (2, 9, 14, 40, 52, 56, 66): all 56 rejection cases of the contract suite come back as duplicate key errors. The statements ARE rejected, only the code is wrong. The loop also goes on after the first error, so an ordered update (the default) applies the statements behind it, which mongod does not. processInsertDirect uses the code of the exception when it has one; the update path should do the same, and stop at the first error unless ordered is false.

Acceptance:

- The update contract suite passes against PoppyDB with the PooledDriver except for the cases that depend on the query planner
- An ordered update stops at the first rejected statement

Source: poppydb/src/main/java/de/caluga/poppydb/netty/MongoCommandHandler.java

### MOR-070 PoppyDB: the delete fast path swallows errors

Status **open**, priority 3, type defect, effort S

Found by the topology matrix of MOR-036. MongoCommandHandler.processDeleteDirect catches every exception of driver.delete, logs a warning and answers ok with the count of what was deleted, without writeErrors. A delete with a query mongod refuses (an unknown operator) looks successful to the client; DeleteRaisesOnRejectedDeleteTest fails against PoppyDB for this reason (the InMemoryDriver reports the rejection in its own command path since MOR-065).

Acceptance:

- A delete with an unparsable query is answered with a writeErrors entry (code 2) by PoppyDB, like by mongod

Source: poppydb/src/main/java/de/caluga/poppydb/netty/MongoCommandHandler.java

### MOR-071 Query.set and findOneAndUpdate(Map) maintain neither @Version nor a field-only @LastChange

Status **open**, priority 3, type defect, effort S

Found while writing docs/howtos/update-pitfalls.md (MOR-027). The builder (MOR-009) increments @Version and sets @LastChange also when the annotation sits on the field only. The writer path (Query.set, push, ... and findOneAndUpdate(Map)) does not touch the version, and handleLastChange in MorphiumWriterImpl looks for the annotation on the class, so a field-only @LastChange (which store() and the builder accept) is not set. UpdateEntryPointsLifecycleTest pins the current behaviour on mongo:8 and the InMemoryDriver. Decide: bring the writer path to the builder rule, or document the difference as the contract.

Acceptance:

- Query.set and findOneAndUpdate(Map) set a field-only @LastChange, or the contract page says why not
- The pins in UpdateEntryPointsLifecycleTest flip to the decision

Source: morphium-core/src/main/java/de/caluga/morphium/writer/MorphiumWriterImpl.java handleLastChange, morphium-core/src/test/java/de/caluga/test/mongo/suite/base/UpdateEntryPointsLifecycleTest.java

### MOR-072 The update builder, findOneAndUpdate(Map), updateOne and updateMany fire no storage-listener update events

Status **done**, priority 3, type defect, effort M

Found by MOR-031. MorphiumStorageListener.preUpdate/postUpdate (Morphium.firePreUpdateEvent) are fired by the writer path (Query.set, inc, push, ...), the bulk context and the buffered writer, but not by Query.modify() (UpdateBuilder), Query.findOneAndUpdate(Map, ...), updateOne and updateMany. Listeners and metrics (morphium.operations.duration in quarkus-morphium) do not see the newest update path. Decide whether they fire the events (a veto from preUpdate then blocks them) or the contract says why not.

Evidence: UpdateEventsTest now has 12 tests (6 more): the builder fires one preUpdate/postUpdate pair with the type of its operator (inc, push, set, also through tryFindOneAndUpdate) and CUSTOM for several operators, updateOne and updateMany fire one pair, findOneAndUpdate(Map) in all three overloads fires one pair (SET, CUSTOM), an invalid builder fires nothing, and a MorphiumAccessVetoException from preUpdate stops the builder, updateOne and the map update with nothing written. Red before (5 failures), green after on the InMemoryDriver and on mongo:8. Fix in Query: findAndModifyReply, the legacy findOneAndUpdate(Map) and updateMapped fire preUpdate before anything is sent and postUpdate after the cache was cleared (before the reply of an update command is turned into an exception, like the writer path); the type is the single operator of the update document or CUSTOM. Consequences, in the CHANGELOG: a vetoing listener blocks these updates now, and MessagingCacheSynchronizer sends its clear message for them. The Quarkus timer includes them (test with builder, updateMany, findOneAndUpdate(Map) and a CUSTOM update, on the InMemoryDriver and mongo:8; docs and changelog adjusted). Regression core and messaging on the InMemoryDriver: 1561 of 1562 passed, the one failure (MultithreaddedMessagingTests) also fails without this change on the same machine, see MOR-074. The Quarkus integration run has two failures in MorphiumMigrationTest that also fail without the change, MOR-074. Not covered: findOneAndDelete (not an update), a buffered writer.

Acceptance:

- A listener sees preUpdate and postUpdate for the builder terminals, findOneAndUpdate(Map) and the count terminals, or the contract page says they are not fired

Source: morphium-core/src/main/java/de/caluga/morphium/query/UpdateBuilder.java, quarkus-morphium/runtime/src/main/java/de/caluga/morphium/quarkus/observability/MorphiumOperationMetrics.java

Commits: d1654434b

### MOR-074 MorphiumMigrationTest (2) and MultithreaddedMessagingTests (1) fail on this machine, with and without the latest changes

Status **done**, priority 3, type verification, effort S

Seen on 2026-10-20 while verifying MOR-072. The Quarkus integration tests MorphiumMigrationTest.inFlightHeartbeatAdvancesExpiryDuringSingleUnit and renewLockBetweenChangeUnitsAdvancesExpiry (timing based, 300 to 900 ms windows; the log says the collection still exists 10 s after dropCollection, so the test runs slowly) and the core test MultithreaddedMessagingTests (NullPointerException: Morphium.getDriver() is null in worker threads after close) failed, also on a checkout without the MOR-072 change (stashed). All three passed in earlier runs of the same stack (MOR-073, MOR-034). One earlier run of MorphiumMigrationTest failed in the full suite and passed alone. Find out whether the machine (about 25 leftover testcontainers, OrbStack just restarted) or a real race is the cause, and make the timing tests robust.

Evidence: Not reproducible after the machine was rebooted (it had been up for days, with about 25 leftover testcontainers and OrbStack just restarted; load averages up to 18 on 12 cores while the three tests failed, free memory was about 90 MB). On the rebooted machine MorphiumMigrationTest passes 9 of 9 in 7.5 s in four runs (the failing runs took 40 s and logged "Collection ... still exists 10s after drop" from dropCollection) and MultithreaddedMessagingTests passes 3 of 3 in 27.7 s in four runs (the failing runs took 46 s). Artificial CPU load is not the cause: MorphiumMigrationTest also passes with 16 busy loops running (load average above 40). The cause is therefore a state of the host, probably memory pressure or the container runtime, not a race in the code; this was not proven. If it comes back, the sign is the dropCollection warning and a test time several times the usual one. Not done: no change to the tests.

Acceptance:

- The three tests pass reliably on a quiet machine, or the cause is documented and the tests no longer depend on it

Source: quarkus-morphium/integration-tests/src/test/java/de/caluga/morphium/quarkus/it/MorphiumMigrationTest.java, morphium-core/src/test/java/de/caluga/test/morphium/messaging/MultithreaddedMessagingTests.java

### MOR-073 Query.push, pull and addToSet fire the update events twice, and the post event is always PUSH

Status **done**, priority 3, type defect, effort S

Found by MOR-031 (a timer count of 2 for one push). MorphiumWriterImpl.pushPull fires firePreUpdateEvent(type) and pushIt fires it again, pushIt fires the post event and pushPull fires it a second time, with UpdateTypes.PUSH hard-coded, so a pull or an addToSet is followed by a post event of type PUSH. A listener counts every push, pull and addToSet twice and cannot pair a pull. Fire each event once, with the real type.

Evidence: UpdateEventsTest (6 tests) records the events of a MorphiumStorageListener: push, pull, addToSet, pushAll, pullAll, set and inc each fire exactly one preUpdate and one postUpdate with the type of the call. Before the fix four of them failed (push and addToSet fired pre/post twice, pull and pullAll ended with post PUSH). Fix in MorphiumWriterImpl: pushIt no longer fires the pre event and fires the post event with the real type, pushPull no longer fires a second post event, pushPullAll fires post with its type. Runs on the InMemoryDriver and on mongo:8; the quarkus timer test now asserts exactly 1 for a push. Regression: 1556 test methods (core and messaging) on the InMemoryDriver, all passed.

Acceptance:

- A listener sees one preUpdate and one postUpdate per Query.push, pull and addToSet, with the type of the call

Source: morphium-core/src/main/java/de/caluga/morphium/writer/MorphiumWriterImpl.java pushPull and pushIt

Commits: 1358b433d

### MOR-078 A char or Character field cannot be read back

Status **done**, priority 2, type defect, effort S

Found by the TCK of MOR-026: 18 of its 73 tests failed with 'could not set field thisCharacter: Field has type char got type class java.lang.Integer'. A character is written as its number (store(), the update-value walk), AnnotationAndReflectionHelper.convertType had no branch for char and Character, so the document could not be read. 18 TCK tests are this cause.

Evidence: CharacterFieldTest (3 tests: read back after store, after Query.set and the update builder, and a query on a char) on the InMemoryDriver and on mongo:8; red before the change (could not set field letter), green after. Regression and the TCK spike run are listed with the series.

Acceptance:

- A char and a Character field are read back after store() and after an update, on the InMemoryDriver and on a real MongoDB

Source: docs/research/jakarta-data-1.1.md, morphium-core/src/main/java/de/caluga/morphium/AnnotationAndReflectionHelper.java convertType

Commits: 32d0ceca7

### MOR-079 Jakarta Data: findFirstBy and findFirstNBy are unsupported

Status **done**, priority 2, type defect, effort S

Found by the TCK of MOR-026: AsciiCharacters declares findFirst3ByNumericValueGreaterThanEqualAndHexadecimalEndsWith and findFirstByHexadecimalStartsWithAndIsControlOrderByIdAsc; the name was not routed to the derived-query code (4 TCK tests). The first n results are returned in the order of the name; more matches are no error for a single entity or an Optional; First belongs to find only; First with a Limit or PageRequest parameter is refused, at build time in Quarkus.

Evidence: MethodNameParserTest (findFirst, derived names; red before), Quarkus integration test findFirst on the InMemoryDriver and on a real mongo:8 (AbstractMorphiumDataOrdinalQueryTest), Spring proxy tests (findFirstLimitsTheResult, findFirstCannotBeCombinedWithALimitParameter), Quarkus deployment test for First with Limit. Reviewed in three passes until no finding was left (a lost limit with a PageRequest, a duplicated name regex, the refusal only at the first call in Quarkus).

Acceptance:

- findFirstBy and findFirst3By work in the Quarkus and the Spring module
- One derived-name check serves both modules

Source: morphium-jakarta-data/src/main/java/de/caluga/morphium/data/MethodNameParser.java

Commits: 3c2cd7e13

### MOR-080 Jakarta Data: a repository method that returns an array of the entity is unsupported

Status **done**, priority 2, type defect, effort S

Found by the TCK of MOR-026: NaturalNumbers.findByIdLessThan(...) returns NaturalNumber[]. The Quarkus build and the Spring proxy took an array for a single entity: ClassCastException of the entity to the array (3 TCK tests). An array is now answered like a List and converted by ResultShapes.toArray.

Evidence: ResultShapesTest (List, Stream, single value, empty, array stays, null, primitive component); Quarkus integration test arrays (derived, @Find, @Query; empty result) on the InMemoryDriver and on a real mongo:8; Spring proxy test aMethodMayReturnAnArray. Reviewed in two passes until no finding was left that could be fixed; the branch for a primitive component (long[]) is not covered by a test because no repository can return such an array yet (skipped, noted in the review).

Acceptance:

- A derived query, @Find and @Query method may return an array of the entity in the Quarkus and the Spring module

Source: morphium-jakarta-data/src/main/java/de/caluga/morphium/data/ResultShapes.java

Commits: e1870e2c3

### MOR-081 Jakarta Data: the static order comes first, and every dynamic Sort, Sort... and Order counts

Status **done**, priority 2, type defect, effort S

Found by the TCK of MOR-026: a method with a static order (@OrderBy, ORDER BY, derived OrderBy) and a dynamic Sort lost the static part or the second Sort, and Sort[] and several Sort parameters were rejected or ignored. SortPlan now merges the static order first and then every dynamic Sort, Sort... and Order in parameter order (first occurrence of a property wins) for derived queries, @Find, @Query and keyset pagination in the Quarkus and the Spring module.

Evidence: SortPlanTest; JdqlMethodBridgeTest.orderByThenDynamicSorts; Spring proxy tests (several Sorts, static order first); Quarkus integration test sorts on the InMemoryDriver and a real mongo:8. Reviewed in two passes: the first found a duplicated sort map in applySorting and a missing JDQL ORDER BY plus Sort test (both fixed), the second had no finding.

Acceptance:

- The static order is applied first, then every dynamic Sort, Sort[] and Order parameter in parameter order
- A property named twice is sorted once, by its first occurrence
- JDQL ORDER BY plus a dynamic Sort works

Source: morphium-jakarta-data/src/main/java/de/caluga/morphium/data/SortPlan.java

Commits: 4ba59d0da, ca6cdab43

### MOR-082 Jakarta Data: JDQL — AND binds stronger than OR, literals are text, NOT before BETWEEN/IN/LIKE, IN lists of literals

Status **done**, priority 2, type defect, effort S

Found by the TCK of MOR-026: a mixed AND/OR condition was split at the OR and the AND part failed, a keyword or operator inside a string literal (ORDER BY, IN, a quote written as two quotes) broke the query, NOT BETWEEN and NOT LIKE put the NOT into the field name, and IN took no list of literals. The parser now searches keywords, operators and parentheses in a text whose string literals are masked, AND binds stronger than OR, NOT may stand before BETWEEN/LIKE, IN takes a list of literals and parameters, and a qualified enum constant compares by its name for an enum field. Also fixes the operand typos for the common case of MOR-075 only where the literal handling touches it; MOR-075 stays open.

Evidence: JdqlParserLiteralsTest (7), JdqlLiteralsExecutionTest (6, on the InMemoryDriver; red before the change), JdqlOperandsTest.listsAndConstants, all existing Jdql tests green. Reviewed in two passes: the first found a dotted-name-as-enum by shape, an untested collection parameter in a list and a changelog overstatement (all fixed), the second had no finding.

Acceptance:

- a OR b AND c is a OR (b AND c), also inside parentheses
- a keyword, operator, comma or parenthesis inside a string literal is text, and two quotes are one quote
- NOT BETWEEN, NOT LIKE and NOT IN (...) work, IN takes a list of literals and parameters
- a qualified constant compares by the name of the constant for an enum field only

Source: morphium-jakarta-data/src/main/java/de/caluga/morphium/data/JdqlParser.java, morphium-jakarta-data/src/main/java/de/caluga/morphium/data/JdqlLiterals.java

Commits: 6b339657b, f95bdae97

### MOR-083 Jakarta Data: SELECT field answers the values of the field when the method returns them

Status **done**, priority 2, type defect, effort S

Found by the TCK of MOR-026: @Query("SELECT name WHERE ...") on a method that returns List<String>, Optional<Integer>, Stream, Page or a single value answered partly filled entities (ClassCastException for the caller). The Quarkus build and the Spring proxy now pass the class of one result to the bridge, which returns the values of the selected field (converted to the declared type) when that class is neither the entity nor a record. A CursoredPage of values is refused with a message. The Spring proxy passes the record of a GROUP BY result now, which it did not before.

Evidence: JdqlProjectionTest (5, red before the change); Spring proxy test selectFieldAnswersTheValuesWhenTheMethodReturnsThem; Quarkus integration test selectedValues on the InMemoryDriver. Reviewed in three passes: the second found a duplicated class loading and the silent CursoredPage of values (both fixed), the third had no finding.

Acceptance:

- A single-field SELECT returns List, Optional, Stream, Page and single values of the field
- A method that returns the entity still gets the entity
- A CursoredPage of values fails with a clear message

Source: morphium-jakarta-data/src/main/java/de/caluga/morphium/data/JdqlMethodBridge.java

Commits: f3f4330ae, 10db59964

### MOR-084 A short or Short field is stored as a number, not as a Java-serialization blob

Status **done**, priority 2, type defect, effort S

Found by the TCK of MOR-026 (sorting by a Short field failed inside the InMemoryDriver). ShortMapper existed but was never registered, so a short or Short field was written as a Base64 Java-serialization blob ({_b64data: ...}) that the database cannot compare, sort or increment. It is registered now and writes a number; documents written as a blob are still read (filtered to Short), a number that does not fit is refused instead of wrapping around.

Evidence: ShortFieldTest (6) on the InMemoryDriver and on a real mongo:8, ShortCollectionsTest (list, map, set, arrays). Reviewed in two passes: the first found a silent wrap-around and untested collections (both fixed), the second had no finding.

Acceptance:

- A Short field is stored as a number and read back as a Short
- It can be compared, sorted and updated in the database
- A blob that was written before is still read, a blob of another class and a number that is no short are refused

Source: morphium-core/src/main/java/de/caluga/morphium/objectmapping/ShortMapper.java

Commits: a826fd7f3, d8b4a609b

### MOR-085 InMemoryDriver: a missing value or null sorts last when descending

Status **done**, priority 2, type defect, effort S

Found while fixing MOR-084. MongoDB sorts a missing value or null before every number, so it is first in an ascending and last in a descending sort; the InMemoryDriver put it first in both directions. buildSortComparator now applies the direction to the null comparison.

Evidence: SortNullOrderTest (2) on the InMemoryDriver and on a real mongo:8 (the real server confirms the expected order); red before the change. The aggregation $sort already applied the direction.

Acceptance:

- Ascending: nulls first, descending: nulls last, on the InMemoryDriver as on mongod
- A second sort field still breaks the tie between nulls

Source: morphium-core/src/main/java/de/caluga/morphium/driver/inmem/InMemoryDriver.java

Commits: 606a59ab7

### MOR-086 Jakarta Data: a derived query may return a CursoredPage, one cursor paging for all four callers

Status **done**, priority 2, type defect, effort S

Found by the TCK of MOR-026: findBy...OrderBy...(args, PageRequest) with a CursoredPage return type answered an offset Page (ClassCastException in the generated code). The derived-query bridge builds the keyset from the order of the method name, then @OrderBy, then every dynamic Sort/Order and returns a CursoredPage; a CursoredPage without a PageRequest parameter fails the build (Quarkus) or the first call (Spring). The cursor paging existed three times (@Find, @Query, the repository base class) and is one implementation now: CursorHelper.page.

Evidence: QueryMethodBridgeCursoredTest (5), Spring proxy test aDerivedQueryMayReturnACursoredPage and UnsupportedParameterStylesTest.aCursoredPageWithoutAPageRequestRefusesTheRepository, Quarkus integration test derivedCursoredPage and deployment test cursoredPageNeedsAPageRequest; the existing cursor tests (CursorHelperTest, MorphiumDataCursoredPageTest) stay green after the refactor. Reviewed in three passes (reuse, validation and async findings fixed, no open finding).

Acceptance:

- A derived query with PageRequest can return a CursoredPage in the Quarkus and the Spring module, also asynchronously
- The keyset is name order, @OrderBy, dynamic sorts
- A CursoredPage without a PageRequest parameter is refused with a message that names the method

Source: morphium-jakarta-data/src/main/java/de/caluga/morphium/data/CursorHelper.java, morphium-jakarta-data/src/main/java/de/caluga/morphium/data/QueryMethodBridge.java

Commits: e5e3e642d, af008022b

### MOR-087 Jakarta Data: the keyset of a cursored @Query includes the dynamic sorts

Status **done**, priority 2, type defect, effort S

Found by the TCK of MOR-026: a @Query without ORDER BY and with an Order or Sort parameter that returns a CursoredPage had no keyset (the cursor was made from the ORDER BY of the query or @OrderBy only) and failed with "No values were provided". The keyset is the ORDER BY of the query (or @OrderBy), then every dynamic Sort, Sort... and Order.

Evidence: JdqlCursoredSortTest (2, red before the change); the other Jdql and cursor tests stay green. Reviewed in one pass without finding.

Acceptance:

- A dynamic Order is the keyset when the query has no ORDER BY
- A dynamic Sort comes after the ORDER BY of the query

Source: morphium-jakarta-data/src/main/java/de/caluga/morphium/data/JdqlMethodBridge.java

Commits: 5c5059479

### MOR-088 Jakarta Data: COUNT(THIS) is the keyword in any case

Status **done**, priority 2, type defect, effort S

Found by the TCK of MOR-026: SELECT COUNT(THIS) WHERE ... counted a field named THIS and answered 0, because only the lower case this was the keyword. The argument is normalized in the SELECT, in HAVING and in the lookups of ORDER BY and HAVING.

Evidence: JdqlLiteralsExecutionTest.countWithLiterals and orderAndHavingWithUpperCaseThis, JdqlParserLiteralsTest.thisInAnyCase. Reviewed in one pass: the ORDER BY lookup was still case-sensitive (fixed).

Acceptance:

- COUNT(THIS) and COUNT(this) answer the same, in SELECT, HAVING and ORDER BY

Source: morphium-jakarta-data/src/main/java/de/caluga/morphium/data/JdqlParser.java

Commits: c87b162ae

### MOR-089 Jakarta Data: a one-character string literal compares with a char field as the character

Status **done**, priority 2, type defect, effort S

Found by the TCK of MOR-026: letter NOT IN (E, G) in JDQL (written with quotes) excluded nothing, because the text was compared with the number that a character is stored as. resolveValue reads a literal by the type of the field now: char and Character get the character, every other field keeps the text, an enum field gets the constant (MOR-082).

Evidence: JdqlCharLiteralTest (2, red before the change). Reviewed in one pass without finding.

Acceptance:

- Equality, IN, NOT IN and BETWEEN with one-character literals work on char and Character fields
- A string field keeps comparing strings

Source: morphium-jakarta-data/src/main/java/de/caluga/morphium/data/JdqlMethodBridge.java

Commits: b1249bf11

### MOR-090 Jakarta Data: an enum in Not, In, NotIn and range operators of a derived query is its name

Status **done**, priority 2, type defect, effort S

Found by the TCK of MOR-026: findByKindNot(PRIME) returned every document and findByKindIn(Set.of(...)) none. Only a plain equality encoded the enum by its name; a value inside $ne, $in or $gt was not converted. QueryExecutor.mapValue converts an enum to its name in every operator.

Evidence: QueryExecutorEnumTest (red before the change). Reviewed in one pass without finding.

Acceptance:

- Equality, Not, In, NotIn and a comparison with an enum work in a derived query

Source: morphium-jakarta-data/src/main/java/de/caluga/morphium/data/QueryExecutor.java

Commits: 12b162db2

### MOR-091 Jakarta Data: several ORs in one query narrow each other

Status **done**, priority 2, type defect, effort S

Found by the TCK of MOR-026 (the second page of a cursored query with an OR showed the first page again) and by the review of that fix. Query.or(...) adds its branches to the one list of or-queries of the query, so (a OR b) AND (c OR d), an OR group next to NOT BETWEEN and the cursor condition on a query with an OR all became one OR of every branch. OrFilter adds every OR as a filter of its own, ANDed with the rest; JDQL, derived queries and the cursor condition use it.

Evidence: JdqlCursoredSortTest.orInTheQueryAndTheCursor and JdqlLiteralsExecutionTest.severalOrsAreAnded (both red before the change); Spring module and the Quarkus query integration tests stay green. Reviewed in one pass: the JDQL group merge was found and fixed.

Acceptance:

- The next page of a cursored query with an OR continues after the cursor
- Two OR groups in an AND chain, and an OR group next to NOT BETWEEN, narrow each other

Source: morphium-jakarta-data/src/main/java/de/caluga/morphium/data/OrFilter.java

Commits: 9a94434d3

### MOR-092 Jakarta Data: IgnoreCase before Between, NotIn and the comparison operators

Status **done**, priority 3, type defect, effort M

Found by the TCK of MOR-026 (testIgnoreCase): findByHexadecimalIgnoreCaseBetweenAndHexadecimalNotIn(min, max, set, sorts) answers nothing. IgnoreCase is parsed as an operator of its own (equals ignoring case); in the specification it is a modifier that applies to the operator after it (Between, NotIn, Like, Contains and so on). A case-insensitive range needs a collation of strength 2 on the query or a comparison of the lower-cased values; check what the InMemoryDriver supports before choosing.

Evidence: IgnoreCaseModifierTest (parser, ranges, sets, text operators, a NotIn next to it stays exact) on the InMemoryDriver; the Quarkus integration test ignoreCaseModifier on the in-memory driver and on a real mongo:8; JakartaDataTckTest 69 of 73, testIgnoreCase leaves the exclusion list. Condition has the flag ignoreCase; QueryExecutor builds a regex with the option i for equality, text operators, In and NotIn, and $expr with $toLower for the ranges.

Acceptance:

- IgnoreCase before Between, In, NotIn, Like, Contains, StartsWith, EndsWith and the comparison operators compares without case
- testIgnoreCase of the TCK passes and leaves the exclusion list of JakartaDataTckTest

Source: morphium-jakarta-data/src/main/java/de/caluga/morphium/data/MethodNameParser.java

Commits: e02ec8a43, 4d532da30

### MOR-093 Enums can be stored by their ordinal: @Enumerated and an opt-in setting

Status **done**, priority 2, type implementation, effort M

Requested by the maintainer after the TCK (four tests sort an enum by the order of its constants). An enum was always stored by its name. Now @Enumerated(EnumType.STRING|ORDINAL) on a field decides, like @Enumerated of JPA, and objectMappingSettings().setEnumType(...) (morphium.enum-type in Spring, quarkus.morphium.enum-type in Quarkus) is the default for fields without it; the default of the setting is STRING, so nothing changes unless it is asked for. Reading accepts both forms. Queries, updates (UpdateBuilder, set/push/pull, bulk and buffered writers, partial store), JDQL and the derived queries write the form of the field.

Evidence: EnumStorageTest (7: fields, setting, reading both forms, queries, sort, updates, bad ordinal) on the InMemoryDriver and on a real mongo:8; QueryExecutorEnumTest (derived queries and JDQL in both modes, UPDATE ... SET enum); Quarkus MorphiumEnumTypeTest for quarkus.morphium.enum-type; JakartaDataTckTest with morphium.enum-type=ORDINAL: 73 of 73 on the InMemoryDriver and on mongo:8.

Acceptance:

- An enum field is stored by name by default, by ordinal with @Enumerated(ORDINAL) or with the setting; the annotation wins over the setting
- Queries, updates, JDQL and derived queries match the stored form; sorts and range queries follow the order of the constants for an ordinal field
- Reading takes both forms; an ordinal that is no constant is refused with the field name
- The setting is available in the Spring and the Quarkus module

Source: morphium-core/src/main/java/de/caluga/morphium/annotations/Enumerated.java, morphium-core/src/main/java/de/caluga/morphium/ObjectMapperImpl.java

Commits: 8dc2017ee, 72aec1d28

### MOR-094 README: the Jakarta Data TCK result and our modules added to the entry page, release history of 6.2 and older moved out

Status **done**, priority 2, type documentation, effort S

The README is the project page of Morphium, whose focus is the message queue and PoppyDB, so those keep the front: tagline, feature list, the comparison table, the full PoppyDB section and the quick start are unchanged. Added: the Jakarta Data TCK result in the feature list, as a badge and as one compact section (a repository example, what is covered, the TCK result with scope and command, Quarkus and Spring Boot, enums by ordinal), and links in the documentation list. The release highlights of 6.2, 6.1 and 6.0 moved to docs/releases/whats-new-v6.md (6.3 stays). The TCK result is stated as available with the release after 6.3.14. It is also on the Jakarta Data, Quarkus, Spring Boot and hub pages and in the two module READMEs.

Evidence: Links checked with a script (all resolve except LICENSE, which does not exist in the repository: see MOR-095). The examples were checked against the code (sort("-field"), derived IgnoreCase, CursoredPage, JDQL UPDATE with arithmetic, record results of GROUP BY). README.de.md is NOT updated and still has the old structure.

Acceptance:

- The README states the TCK result with its scope and the command
- Examples for the ODM, a Jakarta Data repository, Quarkus, Spring Boot, enums, messaging and testing
- Release history is kept in docs/releases/whats-new-v6.md
- Relative links of the README resolve

Source: README.md, docs/releases/whats-new-v6.md

Commits: eb722ce92, d492947fe

### MOR-095 Verification: the showcase against this version, and two findings (scram-client pinned by the Quarkus BOM, no LICENSE file)

Status **done**, priority 2, type verification, effort S

The Quarkus Morphium Showcase (92 tests) was run against 6.3.15-SNAPSHOT of all modules: all pass. Two findings. (1) The Quarkus platform BOM (3.32.3) manages com.ongres.scram:scram-client to 3.2 and wins over the 3.4 that Morphium manages (security: ongres scram 2.1 -> 3.4, CVE-2025-59432 and CVE-2026-53712), so an application on the Quarkus BOM gets 3.2 unless it pins 3.4 itself; the showcase had to pin it to resolve. Check that 3.2 is safe for SaslAuthCommand and whether it carries the CVEs, then document the pin in the Quarkus guide (and in the showcase pom). (2) The repository has no LICENSE file at the root, though README, README.de and the pom point to it.

Evidence: docs/quarkus-extension.md has a section that explains the pin and gives the dependencyManagement entry; the file LICENSE has the unmodified text of the Apache License 2.0 (11358 bytes, the size of the original), taken from a jar of Apache Commons Lang that was already in the local repository; the README links it. The showcase pom still has to get the pin (its own repository).

Acceptance:

- Decision whether scram-client 3.2 works and is safe; the pin is documented where the extension is described
- A LICENSE file with the Apache License 2.0 text at the root of the repository

Source: quarkus-morphium/README.md

Commits: db4f125a9, 417dfbedb

### MOR-096 Quarkus extension: build and test against Quarkus 3.40.1 (was 3.32.3)

Status **done**, priority 2, type infrastructure, effort S

The extension was built against Quarkus 3.32.3. quarkus.version in the parent pom moved to 3.40.1. quarkus update (3.32.3 to 3.40.1, recipes 3.37, 3.38, 3.39) was run as asked; it migrates applications and changed nothing in this extension, so the version was set by hand.

Evidence: Runtime and deployment tests pass; the integration tests (313) failed twice in the first full run (MorphiumMigrationTest, lock heartbeat tests, known from MOR-074), pass alone on 3.32.3 and 3.40.1, and all 313 passed in a second full run. The 3.40.1 BOM manages scram-client 3.3 (3.2 in 3.32.3), so the pin of MOR-095 is only needed for 3.4 or on older Quarkus. Checked at both ends: the extension jars built on 3.40.1 were run in the integration tests with the Quarkus version set to 3.32.3 in the pom (311 of 313, the same two migration tests fail as on 3.40.1) and in the showcase (92 of 92 on 3.32.3 and on 3.40.1). A -D override of the Quarkus version on the command line does not reach the test JVM (the bootstrap reads the pom) and gives a mixed class path: change the property in the pom. Not checked: Quarkus 3.33 to 3.39.

Acceptance:

- All four Quarkus modules build and their tests pass on 3.40.1
- The documentation states the Quarkus version and what the 3.40.1 BOM manages for scram-client

Source: pom.xml, quarkus-morphium/README.md, docs/quarkus-extension.md

Commits: 02ae7b5bd, 0e4599de9

