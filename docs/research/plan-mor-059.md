# Plan: MOR-059 raise on rejected writes (v1 architect draft + review outcome + plan v2 below)

Status: planning only, no implementation. Evidence of the problem: `docs/research/audit-write-paths.md`, pinned in `WritePathAuditContractTest` (`RAISE_TODAY`).

## 0. Findings that change the design

1. **The bulk contexts swallow every `MorphiumDriverException`.** `PooledDriver.createBulkContext`, `SingleMongoConnectDriver.createBulkContext` and `InMemoryDriver.createBulkContext` wrap the request loop in `catch (MorphiumDriverException e) { log.error(...) }`. Insert and delete errors of a bulk are lost as well; raising in `UpdateMongoCommand` alone would not fix the bulk paths. The InMemoryDriver bulk calls `update()` directly and drops `writeErrors` of the result.
2. **`UpdateMongoCommand.execute()` is also the engine of `store()` of existing entities** (`StoreMongoCommand.execute()` delegates to it; `MorphiumWriterImpl.store`, `storeList`). A unique-index violation on `store()` of an existing entity is presumably swallowed too (unverified, not in the audit table). The versioned-store paths read `n` of the reply; a `writeErrors` reply has `n=0`, so a duplicate key most likely surfaces as a misleading `VersionMismatchException`.
3. **Raw `UpdateMongoCommand.execute()` replies are consumed in many places**: tests (`UpdateContract`, `UpdateAtomicityTest`, `WriteBatchSplitterTest`, `WireWriteSplittingTest`) and the four messaging sites that test `ret.get("writeErrors") != null`.
4. **`sumUp` is unsuitable for error handling**: it overwrites the `writeErrors` list with the last chunk's list and adds `ok` per chunk (`ok` becomes 2.0).
5. **Adjacent bug**: `set(Query, Map)` does not call `settings.getUpdates().clear()` after a mid-loop batch execute, so accumulated statements are re-sent (the `inc` and `pushPull` loops do clear).

## 1. Where to normalise

**Recommendation: (b) a shared pure normaliser used by one private writer method; `UpdateMongoCommand.execute()` stays raw.**

- New public helper `WriteReplyErrors` in `driver/commands` with `MorphiumDriverException rejectionOf(String db, String coll, Map reply)` (null for a clean reply): `writeErrors` becomes a `MorphiumDriverException` (`mongoCode` of the first error, `setWriteErrors`, collection and db, message with the counts and "earlier documents are not rolled back"); a `writeConcernError` becomes an exception whose message says the write WAS APPLIED. It is the logic of `Query.toUpdateResult` moved out, so there is one implementation. It returns the exception instead of throwing so that call sites can clear the cache first.
- One private writer method `runUpdate(UpdateMongoCommand, ...)`: `execute()`, check the switch, normalise, return the reply for merging. It replaces the scattered pairs at `updateUsingFields`, `inc(entity)`, `inc(Query, Map)`, `inc(Query, field)`, `set`, `unset`, `pushIt`, `pushPullAll`, `doUpdate` (used by `setInEntity`/`unsetInEntity`); the versioned store paths call the normaliser before they inspect `n`.
- Bulk contexts: inject the same normaliser in the three drivers; remove the swallowing catch or rethrow after logging.
- Why not (a) (raise inside `UpdateMongoCommand.execute()` like Insert/FindAndModify): breaks the low-level contract tests and every raw reply reader, silently changes `store()`, and makes a `writeConcernError` throw inside messaging (`updateProcessedBy` would return false although the mark was applied). More consistent, but a breaking change of the low-level API: defer to 7.0.
- Why not (c): the command has no config access; the switch belongs in the writer (`morphium.getConfig()`), the bulk contexts reach the config through `BulkRequestContext.getMorphium()`.

Other `UpdateMongoCommand` users stay on raw `execute()`: `SingleCollectionMessaging` (`updateProcessedBy`, answer mark, `removeProcessedBy`), `DualChannelMessaging`, `MultiCollectionMessaging`, `MessageRejectedException`, `LegacyProcessedByRepair` (they handle both shapes: `nModified==0` and the #291 repair via `writeErrors` or exception); `Query.updateLastAccess` (a write inside a read: keep silent, log WARN); `SequenceGenerator` (imports the command but uses `morphium.inc(entity)`, which already raises). The messaging sites do not rely on a silent lost race (a race is `nModified=0` without an error).

## 2. Default, compatibility, exception type, async, buffered

- Raise by default (task type is `defect`). Temporary kill switch `WriterSettings.raiseOnWriteErrors` (default true) following `ObjectMappingSettings.translateUpdateFieldNames`; deprecated, removed in 7.0. No per-query override.
- `ThrowOnError` stays; the reply map is still returned on success; its `writeErrors` branch becomes a defensive no-op unless the switch is off; `ThrowOnErrorTest` stays green (update its comment).
- Exception type `MorphiumDriverException` (as `updateMapped`). COULD: a subclass or flag to tell "applied but not acknowledged" from "rejected".
- Async: the `WriterTask`s already catch `Exception` and call `onOperationError`; raising inside the existing `try` suffices. SHOULD: exempt `MorphiumDriverException` from the retry loop of the async path (the sync path already rethrows without retry). `onOperationSucceeded` and the post-update event must not fire on rejection.
- Buffered writer: it fires `onOperationSucceeded` when an entry is queued; the flush runs all contexts in one `try`, so once bulk contexts raise, one failing collection aborts the rest after `localQueue` was taken (data loss). MUST: `try/catch` per context, log collection and cause; SHOULD: `onOperationError` for the entries of the failed context.

## 3. Semantics

- Ordered multi-document updates: documents before the error stay updated, no rollback (message says so). Normalise after `WriteBatchSplitter.mergeInto` so counts are aggregated and indexes offset. The writer's limit loops execute in batches: the first failing batch throws and earlier counts are lost; carry a running total or say "earlier batches applied". `InMemoryDriver.runCommand(UpdateMongoCommand)` does not stop after the first statement with `writeErrors` (a real ordered update does): add the ordered stop.
- Upserts: a duplicate key on the insert half arrives as 11000, `isDuplicateKey()` true. Callers that treated "someone else created it" as silent success must catch it: the main compatibility risk.
- `writeConcernError`: throw with the "WAS APPLIED" message (closes #408); clear the cache and fire the post-update event first. Alternative: log WARN.
- Retry loop: normalise after `execute()`, never inside `executeSingleMessage`. Hazard to document: after a network-error retry whose first attempt applied, a non-idempotent update can surface a spurious duplicate key.
- `w:0`: a reply without `n`/`writeErrors` is clean (probe test needed).
- Transactions: a failed statement aborts a real transaction; with raising the interceptor sees the exception at the statement.
- Events/cache: clear the cache whenever the reply may have changed data (`finally`); fire `firePostUpdateEvent` only after a fully accepted write.

## 4. Test plan

- Flip the pin in `WritePathAuditContractTest`: `expected` true for every `FAILING` entry, delete `RAISE_TODAY` and the pinned comment; assert the exception type, `getMongoCode()` (11000 unique, 28 path below a scalar, 2 pull/pullAll on a non-array, verify on mongo:8), `isDuplicateKey()`, `getWriteErrors()` non-empty, document unchanged. Grow the "must raise" set per step.
- `WriteReplyErrorsTest` (pure): writeErrors, writeConcernError, both, empty list, null, ok-only, several errors, an aggregate from `WriteBatchSplitter.mergeInto`.
- Async (`set`, `inc`, `pushPull`, `unset(entity)`): `onOperationError` once, never `onOperationSucceeded`, no retry, no post-update event.
- Bulk on all three drivers: request 1 ok, 2 fails, 3 does not run; insert/delete errors no longer swallowed.
- Buffered flush with a failing and a good context.
- Messaging regressions (#291 legacy `processed_by: null` on both drivers; exclusive-message lock handling unaffected).
- Switch off = today's behaviour. InMemoryDriver parity (remove `assumeFalse(isInMem)` of the audit test, ordered stop). Transaction (replica set). `writeConcernError` through a stubbed reply.

## 5. Order, CHANGELOG, owner decisions

Order: (1) `WriteReplyErrors` + unit test + `Query.toUpdateResult` refactor + switch (no behaviour change); (2) `runUpdate` in `MorphiumWriterImpl`, replace `sumUp`, fix the missing `clear()`, flip the `Query` rows; (3) entity paths (`setInEntity`, `unsetInEntity`, `push(entity)`, `updateUsingFields`; check `inc(entity)`/`dec(entity)`, which raise today only because `nModified != 1` throws "Update failed", also for a legitimate inc by 0; versioned store paths); (4) bulk contexts on three drivers and the buffered flush, InMemoryDriver ordered stop; (5) flip remaining pins, close MOR-059, CHANGELOG; (6) new ledger tasks: `store()`/`StoreMongoCommand` unique violations, `DeleteMongoCommand` writeErrors, whether `UpdateMongoCommand.execute()` should raise (7.0).

CHANGELOG (Changed): "A rejected write is no longer silent (MOR-059)": the paths now throw a `MorphiumDriverException` (`getMongoCode()` of the first error, `isDuplicateKey()`, `getWriteErrors()`), async variants call `onOperationError`, earlier documents of an ordered multi update are not rolled back, a `writeConcernError` is thrown with a message saying the write was applied (#408), the bulk context no longer swallows errors; `writerSettings().disableRaiseOnWriteErrors()` restores the old behaviour for one release.

Owner decisions: (1) raise by default plus a deprecated kill switch and its removal date; (2) `writeConcernError`: throw (recommended) or warn, own subtype or not; (3) scope: `store()`/`StoreMongoCommand` and `DeleteMongoCommand` here or follow-ups; (4) events and cache on partial application; (5) buffered writer: a second `onOperationError` after an earlier "succeeded" callback, or defer the success callbacks until after the flush; (6) release vehicle: minor release with a loud CHANGELOG, or hold the default flip for 7.0.

---

# Review outcome (challenger, verified against the code)

Verdicts: Q1 (no internal caller depends on silence) mostly agree, **three exceptions**; Q2 (raise by default in a release) disagree as stated; Q3 (semantics) partly disagree, four defects; Q4 (order and scope) disagree on the order, agree on the gaps; Q5 (tests) mostly agree, two claims unrealistic.

Findings that change the plan:

1. **`MorphiumMigrationRunner.acquireLock`** (quarkus-morphium/runtime, ~530-555) is the one real consumer: `q.set(update, true, false)` on a lock with an upsert; a held lock is an E11000 `writeErrors` entry today (silent, then `n==0`, then `throwLockHeld()`). After the change it throws, and its catch recognises it only by `e.getMessage().contains("duplicate key")`. A thrown `writeConcernError` ("applied") would be rethrown and the lock stays held for its TTL. `renewLock` catches `Exception` and is safe. The v1 plan did not look at quarkus-morphium.
2. **Messaging post-processing is unguarded**: `checkDeleteAfterProcessing` (`SingleCollectionMessaging` ~1371-1405, `DualChannelMessaging` ~1451, ~2007) calls `setInEntity(deleteAt...)` and `query.set(MsgLock.Fields.deleteAt...)`; `Msg` and `MsgLock` use `@WriteSafety(MAJORITY)`, so a `writeConcernError` during a failover is realistic; a throw skips the scheduled cleanup and `unlockIfExclusive(msg)` (exclusive locks stay held until their TTL).
3. **`SequenceGenerator`** uses `morphium.inc(entity)`, which throws "Update failed" when `nModified != 1`; the normaliser changes message and code for existing callers.
4. **Cluster cache invalidation**: `MessagingCacheSynchronizer.postUpdate` sends the cluster-wide clear. "Fire the post-update event only after a fully accepted write" would leave other nodes with stale caches after a partially applied multi update. Fire `postUpdate` (and clear the local cache, in `finally`) whenever the reply may have changed data.
5. **`writeConcernError` as an unmarked `MorphiumDriverException`** is the riskiest piece (discriminator would be message text; a thrown exception after an applied `$inc`/`$push` invites a caller retry that double-applies; the Java driver has a distinct `MongoWriteConcernException`). The subtype is a MUST; throw vs warn is an owner decision. The audit did not measure it.
6. **Unordered bulks**: `ordered` is ignored in all three `createBulkContext` implementations (every request is a separate command). Raising at the first failure turns unordered contexts into ordered ones; unordered contexts should run all requests, then throw an aggregate carrying the counts (the `num_*` result is lost on a throw). Removing the swallowing catch also changes behaviour for network and other driver exceptions, so the switch must cover that too.
7. `MorphiumBulkContext.execute()` is `firePre(); ctx.execute(); firePost();` and never clears the cache: needs `try/finally`.
8. Buffered writer: the immediate path (`size==0` branch of `addToWriteQueue`) runs `ctx.execute()` synchronously, so buffered `set`/`inc` start throwing to the caller; the flush MUST isolate contexts (verified: one failing context aborts the rest and drops their entries).
9. Async retry: in `set`, `inc` and `pushPull` the inner `try` already routes every exception to `onOperationError` without rethrow, so the retry loop is not reached there; the exemption is only a hardening for other task variants (verify per method). The sync path rethrows without retry.
10. Global-only kill switch is the wrong granularity (a single racing upsert would force disabling error raising for every write); `translateUpdateFieldNames` has a per-query override.
11. `ThrowOnError.throwOnWriteError` users catch `IllegalStateException`; after the change they get `MorphiumDriverException`: needs a migration note.
12. Unverified: whether the InMemoryDriver leaves earlier documents applied on a mid-update error (for `findAndModify`, `updateInternal` restores the document), so the message "earlier documents are not rolled back" may be wrong on that driver. The InMemoryDriver does not enforce unique multikey indexes on `$push`, so several probes cannot raise there: keep the real-MongoDB gate or exempt per driver. Inc/dec(entity) rows of the audit table were wrong (fixed in the audit doc).

# Plan v2 (what changes against v1)

Recommendation first, then the alternative.

1. **Default and granularity.** Raise by default, in a **minor release (6.4.0), not in the 6.3.x patch line**, with a deprecated global switch `WriterSettings.raiseOnWriteErrors` **and a per-query override** (like `Query.setTranslateUpdateFieldNames`), and a CHANGELOG that names the quarkus migration runner, the messaging paths and the `ThrowOnError` exception type. Alternative: default false for one release (loses the point of the task).
2. **`writeConcernError`**: its own subtype `WriteConcernNotSatisfiedException extends MorphiumDriverException` (MUST); **throw** (closes #408) but never from the internal messaging post-processing and cache paths, which log WARN. Alternative: warn-only by default.
3. **Internal callers.** Before the default flips: `MorphiumMigrationRunner.acquireLock` uses `isDuplicateKey()` instead of message sniffing (and a test for "lock held" on E11000, and that a `writeConcernError` is not mistaken for a held lock); guard `checkDeleteAfterProcessing` (both messaging classes) with try/catch + WARN; `SequenceGenerator`/`inc(entity)` keep their behaviour but go through the normaliser with a test that the exception type stays `MorphiumDriverException`; `Query.updateLastAccess` stays silent with a WARN.
4. **Events and caches.** Clear the local cache and fire `firePostUpdateEvent` (cluster invalidation) in `finally` whenever the reply may have changed data (`n > 0`, `nModified > 0`, upserted, or `writeConcernError`); fire the success callback and `onOperationSucceeded` only after full acceptance.
5. **Bulk.** The context aggregates: ordered stops at the first failing request, unordered runs all requests; both then throw one `MorphiumDriverException` carrying the counts and all write errors; `MorphiumBulkContext.execute()` uses `try/finally` for events and cache; the swallowing catch is replaced (kill switch restores it). Insert and delete errors of a bulk are included (same engine).
6. **Buffered writer.** Per-context try/catch in the flush (log collection and cause, `onOperationError` for the entries of the failed context); document that the immediate path now throws to the caller; owner decision on the second callback (below).
7. **Scope.** In: all `Query`/`Morphium` update paths, entity paths, versioned store paths (normaliser before the `n == 0` check, so a duplicate key is no longer reported as `VersionMismatchException`), bulk and buffered flush. Out (own ledger tasks): `store()` of existing entities via `StoreMongoCommand`, `DeleteMongoCommand`, and raising inside `UpdateMongoCommand.execute()` (7.0). Raising for `Query.set` while `store()` stays silent is documented as a known inconsistency until the follow-ups land.
8. **Order of work** (each step shippable, the "must raise" set of the audit test grows per step): (1) `WriteReplyErrors` + subtype + unit test + `Query.toUpdateResult` refactor + switch and per-query override, no behaviour change; (2) `runUpdate` in `MorphiumWriterImpl` (replaces `sumUp`, fixes the missing `clear()` in `set(Query, Map)`), flip the `Query` rows; (3) internal-caller guards of point 3 **before** the default is on; (4) entity paths and versioned store paths; (5) bulk contexts on three drivers + `MorphiumBulkContext` + buffered flush + InMemoryDriver ordered stop; (6) flip remaining pins, close MOR-059, CHANGELOG, migration note for `ThrowOnError`.
9. **Tests added to v1**: quarkus-morphium `acquireLock` on E11000 and on a stubbed `writeConcernError`; cluster-cache invalidation on a partially applied multi update; unordered and ordered bulk; messaging `checkDeleteAfterProcessing` with a throwing write; the immediate buffered path; per-query override; InMemoryDriver parity only where the driver can reproduce the server behaviour (probes depending on unique multikey indexes stay gated to a real MongoDB, with a per-driver exemption naming the ledger task).

# Open questions for the project owner (recommendation first)

1. Release vehicle: **6.4.0 with a loud CHANGELOG** (recommended) / 6.3.7 / hold the default flip for 7.0. Is a per-query override acceptable (recommended: yes)?
2. `writeConcernError`: **throw with the subtype** (recommended) or warn only? (Msg and MsgLock use MAJORITY.)
3. Scope: **`Query`/`Morphium` update paths, bulk, versioned store only** (recommended) or also `store()` and `DeleteMongoCommand` in this task?
4. Bulk: **unordered contexts keep running and aggregate** (recommended) or stop at the first failure?
5. `ThrowOnError` users: accept the exception type change `IllegalStateException` to `MorphiumDriverException` with a migration note (recommended), or keep `IllegalStateException` as a superclass-compatible wrapper?
6. Buffered writer: a later `onOperationError` after an earlier "succeeded" callback (recommended, documented) or defer the success callbacks until after the flush?
7. Who changes and releases the quarkus-morphium migration runner, in lockstep?

---

# Owner decisions and implementation (all recommendations of plan v2 taken)

Decided: release vehicle **6.4.0** with a loud CHANGELOG; deprecated global switch **and per-query override**; **`WriteConcernNotSatisfiedException`** thrown (never from the internal best-effort writes); scope **without `store()` and `DeleteMongoCommand`** (ledger tasks MOR-064, MOR-065); **bulk aggregates** (unordered contexts run every request, then one `BulkWriteException`); **`ThrowOnError` migration note** (CHANGELOG, Javadoc, configuration reference); the **Quarkus migration runner is a follow-up** (MOR-063, released in lockstep); the buffered writer reports the error to the callbacks of the failed collection after an earlier "succeeded" (documented); events and caches follow what was applied.

Implemented in six commits on `feat/mor-059-raise-on-write-errors`, tests first in every step:

1. `WriteReplyErrors` (the normaliser, extracted from `Query.toUpdateResult`, wording neutral for update and delete), `WriteConcernNotSatisfiedException`, `WriterSettings.raiseOnWriteErrors` (deprecated, default true), `Query.setRaiseOnWriteErrors(Boolean)` / `isRaiseOnWriteErrors()`.
2. `MorphiumWriterImpl`: `rejectionOf(...)` at the six query-based sites (inc(Query, Map), inc(Query, field), set, unset, pushIt, pushPullAll); the rejection is returned, the cache is cleared and the post-update event fired, then it is thrown; limited multi updates stop at the first rejected batch; `set(Query, Map)` clears the statements of a sent batch (it re-sent them before).
3. The best-effort `deleteAt` writes of the three messaging classes do not raise (`setInEntity` logs, the lock write uses `setRaiseOnWriteErrors(false)`).
4. Entity paths (`inc/dec(entity)`, `doUpdate` for `unsetInEntity`/`pop`, `updateUsingFields`) and the versioned store paths check the reply; a unique index violation is no longer a `VersionMismatchException`.
5. Bulk: `BulkWriteException`, `BulkRequestContext.raiseOrLog` used by the Pooled, SingleMongoConnect and InMemory drivers; the update and delete replies are checked; `MorphiumBulkContext.runBulk` fires the post events in `finally`; the buffered flush isolates the contexts; the InMemoryDriver's ordered `update` command stops at the first statement with a write error.
6. Flip of the last pins of `WritePathAuditContractTest` (now asserts `MorphiumDriverException`, the server code and the write errors for every path, the bulk requests with `BulkWriteException`), `CurrentDateReproductionTest` flipped, CHANGELOG, configuration reference, API reference, `ThrowOnError` Javadoc.

Deviations from plan v2: `renewLock`/`acquireLock` of the Quarkus module are not changed in this task (MOR-063, done); `SequenceGenerator` needed no change (it uses `inc(entity)`, which now raises the normalised exception).

Coverage that was open after the six steps and is closed afterwards (branch `test/mor-059-coverage-gaps`): `WriteConcernErrorTest` (a real single-node replica set; an entity with `@WriteSafety(WAIT_FOR_SLAVE)` cannot get its second acknowledgement): `Query.set` and `inc(entity)` raise `WriteConcernNotSatisfiedException` and the data IS written, `setRaiseOnWriteErrors(false)` returns the reply with the `writeConcernError`, a bulk wraps it in a `BulkWriteException` with the counts of what was applied; `BufferedWriteIsolationTest` (both drivers): a rejected collection of the buffered flush does not keep the other collection from being written and its callback hears about the error (mutation check: with the old single try/catch the test fails); the transaction and messaging tests on the replica set (`TransactionTest`, `ExclusiveMessageTests`, `ExclusiveMessageBasicTests`, `AnsweringBasicTests`, ...) are green, the two `ExclusiveMessageTests` failures seen on a standalone mongod do not occur there.
