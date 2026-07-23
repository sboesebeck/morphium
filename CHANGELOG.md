# Changelog

All notable changes to Morphium will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).


## [Unreleased]

### Added

#### PoppyDB: DevOps command surface — live currentOp/killOp, rs.conf(), listCommands, hostInfo, real connection gauges
Closes the gaps that made mongosh's admin helpers fail against PoppyDB. A server-wide **op registry** tracks every command for the duration of its dispatch: `db.currentOp()` (mongosh's `{aggregate: 1, pipeline: [{$currentOp: {}}]}` shape, `$match` filters included) and the `currentOp` command answer from it with mongod-shaped op documents (opid, ns, command, secs_running, client, killPending — SASL/createUser payloads redacted); `killOp` marks an op kill-pending and best-effort interrupts its thread, cooperatively like mongod (never a Netty event loop; write-concern waits on the executor are interruptible). New commands: `listCommands` (generated from the real command surface — the wire handlers plus the driver's registered command classes), `hostInfo`, `connectionStatus` (reports the connection's SCRAM user under `--auth`), `whatsmyuri`, and `replSetGetConfig` — `rs.conf()` now works, reconstructed from `--rs-seed`/`--rs-priorities`. `serverStatus.connections` reports the server's **real** client-socket gauges (Netty channel group) instead of the in-memory driver's internal connection borrows. The embedded InMemoryDriver answers the `$currentOp` stage with an honest empty set (commands execute synchronously — there is never a concurrent op to report).

#### InMemoryDriver/PoppyDB: `dbHash` and `validate` — consistency checks with teeth; `top` fails explicitly
`dbHash` computes an MD5 per collection over the BSON-encoded documents (plus mongod's combined hash, optional `collections` filter) in a **canonical document order**, so two replica-set members holding the same data produce the same hash even though initial sync and live replication materialize their collections in different order — the one-command consistency check for failover/replication tests, deliberately answered on secondaries too. `validate` is a real check, not a stub: it walks every index of the collection's index store and reports entries referencing documents that are no longer in the collection and documents missing from an index (`extraIndexEntries`/`missingIndexEntries`, capped at 20 with full counts in the error strings, plus `keysPerIndex`/`nrecords`), `valid: false` when anything is off; a missing collection answers `NamespaceNotFound` (26) like mongod. `top` now fails with an explicit `CommandNotSupported` (115, "per-collection operation counters are not tracked") instead of a generic CommandNotFound — real mongod has the command, so the error says why PoppyDB does not.

#### PoppyDB: `--log-level` option — the server no longer logs everything at DEBUG
The CLI fat jar shipped no Logback configuration (the module jars deliberately exclude `logback*.xml`, as libraries should), so Logback fell back to its basic setup: **every logger at DEBUG on the console**. Long-running servers produced enormous logs — one orphaned instance filled a test runner's disk with a 28GB log file. The fat jar now bundles a server configuration (root `INFO`, Netty `WARN`), and verbosity is adjustable at startup: `--log-level ERROR|WARN|INFO|DEBUG|TRACE`, or `-Dpoppydb.log.level=<level>`, or a full replacement via `-Dlogback.configurationFile=...`.

#### Driver: configurable `appName` in the connection handshake
New setting `DriverSettings.appName` (default `"Morphium"`), sent to MongoDB as `client.application.name` in the `hello` handshake. Set it per service to tell instances apart in `db.currentOp()`, server logs and profiler output (MongoDB truncates values over 128 bytes). Third-party `MorphiumDriver` implementations keep compiling — the new interface methods are defaults.

#### InMemoryDriver: aggregation stages `$documents`, `$densify`, `$fill`, `$setWindowFields`, `$collStats`, `$listSessions` — and a real `$out` (#254)
`$out` no longer pretends: it actually replaces the target collection (through the driver's primitives, so index/capped/TTL bookkeeping and watchers stay intact), is terminal and supports `{db, coll}`. `$documents` provides literal document sources, `$densify` fills numeric and date gaps (partition/full bounds, calendar-aware month/quarter/year steps, 500k generation cap), `$fill` supports `value`, `locf` and `linear` with partitioning, and `$setWindowFields` implements partitionBy/sortBy with documents-windows for `$sum/$avg/$min/$max/$count/$push/$first/$last/$rank/$denseRank/$documentNumber/$shift` (the remaining window functions and range windows followed in #255, see the next entry). `$collStats` returns real counts (byte gauges 0 as per the dbStats precedent), `$listSessions` an honest empty set.

#### InMemoryDriver: remaining $setWindowFields window functions (#255)
`$setWindowFields` now covers the full window-function surface: the statistical accumulators `$stdDevPop/$stdDevSamp` and `$covariancePop/$covarianceSamp`, the N-forms `$firstN/$lastN/$minN/$maxN` and `$top/$bottom/$topN/$bottomN` (with their own per-operator `sortBy`), the time-series functions `$derivative` and `$integral` (trapezoid rule; optional `unit` down from `week` against a date sortBy), `$expMovingAvg` (`N` or `alpha`), and the gap-fillers `$linearFill` (proportional to the sortBy distance, strictly increasing sort values enforced) and `$locf`. Range windows (`window: {range: [lo, hi], unit?}`) work for the whole accumulator family, resolved against an ascending single-field sortBy — numeric distances, or date distances with a `unit`. Invalid specs keep failing loudly with mongod-style codes (`5787908` for a bad `n`, `5339902` for a non-ascending range sortBy, `605001` for non-monotonic `$linearFill` input) instead of returning silently wrong results.

#### Expr: ~40 aggregation expression operators implemented, three silent mis-calculations fixed (#255)
All stubbed operators are real now — among them `$map`, `$arrayToObject`, `$first`/`$last` (array form), the byte/codepoint string family (`$strLenBytes/CP`, `$substrBytes/CP`, `$indexOfBytes/CP`), `$strcasecmp`, `$toDate`, `$type`, the set family, `$binarySize`/`$bsonSize` — plus new ones: `$sortArray`, `$firstN/$lastN/$maxN/$minN`, `$dateAdd/$dateSubtract/$dateDiff/$dateTrunc` (UTC defaults, boundary-crossing semantics), `$round` (2-arg, half-to-even like MongoDB), `$sinh/$cosh/$tanh`, `$rand`, `$sampleRate`, `$median`/`$percentile` (nearest-rank). Fixed on the way: `$asinh` computed **sinh**, `$setUnion` collected the arrays instead of their elements, and 2-arg `$atanh` silently returned 0 (now an error). `$function`/`$accumulator` throw (no server-side JS); the window-context accumulators live in `$setWindowFields`, where they are implemented now (see above).

#### InMemoryDriver: positional update operators `$`, `$[]`, `$[<identifier>]` with `arrayFilters`, and `$bit` (#256)
Array element updates work now: `{$set: {"items.$.qty": 5}}` resolves the query's match position, `$[]` applies to all elements, `$[elem]` + `arrayFilters` filters them, all combinable with `$set/$inc/$mul/$min/$max/$push/$pull/...` and nested paths behind the positional segment. `arrayFilters` are read from the wire command (they were silently dropped before), validated upfront (unknown/unused/duplicate identifiers, replacement updates) and honored by `findAndModify` too. `$bit` supports and/or/xor on int/long. Error behavior matches MongoDB — no silent no-ops.

#### Query API: `arrayFilters` for update operations
`Query.setArrayFilters(...)` (list or varargs of filter documents) makes filtered positional updates reachable from the high-level API — previously `arrayFilters` existed only on the driver-level `UpdateMongoCommand`, so `$[<identifier>]` paths were unusable via `query.set/inc/unset/push/...`. The filters are applied to all update operations executed on that query, alongside the existing collation handling: `q.setArrayFilters(Doc.of("elem", Doc.of("$gte", 90))).set("values.$[elem]", 100, false, true)`. Paths containing `$` skip property-name translation as before, so positional segments pass through unchanged.

#### Aggregator: typed builder methods for `$documents`, `$densify`, `$fill` and `$setWindowFields`
The stages implemented in #254 were only reachable via `genericStage()`; the `Aggregator` interface now offers `documents(...)`, `densify(...)` (bounds/unit/partition overloads), `fill(...)` and `setWindowFields(partitionBy, sortBy, output)`. Field names in the specs are translated like in every other typed stage method (keys always, `$`-references with the opt-in `translateAggregationFieldNames`). Implemented in both `AggregatorImpl` and `InMemAggregator`.

#### InMemoryDriver/PoppyDB: `currentOp` shape, `serverStatus`, `bulkWrite` (#257)
`currentOp` returns mongod's `{inprog: [], ok: 1.0}` shape (and no longer NPEs on a plain `{currentOp: 1}` — a parse bug in `CurrentOpCommand.fromMap`), `serverStatus` provides the fields tooling commonly reads (host/version/process/uptime/connections/mem, JVM-backed), and the MongoDB-8.0-style top-level `bulkWrite` command maps onto the existing insert/update/delete primitives with `ordered`/`errorsOnly`, per-op results and proper write-error reporting. `saslContinue` from the same issue already shipped with the SCRAM work.

#### PoppyDB: replica-set replication now covers index definitions (#258)
Replication used to copy documents only — a secondary (and any node promoted after a failover) had **none** of the primary's user-defined indexes: unique constraints went unenforced, TTL indexes never expired anything, and index-backed queries fell back to full scans. The initial sync now replicates the primary's `listIndexes` output after the data snapshot (a failure here fails the sync — the node never reports "synced" while missing the primary's constraints), and a periodic 30s diff converges afterwards: missing indexes are created with their full options (unique/TTL/partial/sparse/…), indexes dropped on the primary are dropped locally, the `_id` index is never touched. The periodic diff also picks up whatever the secondary missed while disconnected (change streams carry no index DDL). On the way, InMemoryDriver's `listIndexes` learned to report `partialFilterExpression` — it silently swallowed it before, which would have replicated partial indexes as full ones.

#### PoppyDB: opt-in auth enforcement (`--auth`) with initial admin user
With `--auth`, a connection may only run the handshake, SASL, `logout`, `ping` and `buildInfo` commands until it completes a SCRAM exchange; everything else is rejected with code 13 Unauthorized. Authentication state is per connection (one wire handler per channel); `logout` locks the connection again. `--rootUser`/`--rootPassword` create an initial admin user at startup if absent — there is no localhost exception, so a fresh `--auth` server without them would be unreachable (a warning says so). The default remains completely open: without `--auth` nothing changes for existing setups. Combine with the existing `--ssl`/`--sslKeystore` options for encrypted, authenticated deployments.

#### InMemoryDriver/PoppyDB: real SCRAM authentication (verification) and a working `createUser` (#245)
The in-memory server now implements server-side SCRAM-SHA-1 and SCRAM-SHA-256 (RFC 5802/7677, validated against the RFC test vectors) including MongoDB's specifics (MD5-digested password for SHA-1, SASLprep for SHA-256, `skipEmptyExchange`, the three-step exchange used by clients like mongosh). `createUser` actually creates users now, stored mongod-shaped in `admin.system.users` (per-mechanism base64 credentials: salt, iterationCount, storedKey, serverKey — mongod default iteration counts), so morphium's own SCRAM client authenticates against InMemoryDriver/PoppyDB exactly like against real MongoDB; wrong passwords and unknown users are rejected indistinguishably (no user enumeration). Verification is always active when a client attempts to authenticate; **enforcement** is opt-in via PoppyDB's `--auth` switch (see the entry above) and TLS is available via the existing `--ssl` options. X.509 `authenticate` and `createRole` keep failing honestly. Authorization is authentication-only for now — roles are stored but not evaluated.

#### Messaging: configurable default TTL and fallback-poll cadence
Two new `MessagingSettings`: `messagingDefaultTtl` (default 30s — the historical hardcoded value) is applied on send to timing-out messages that carry no TTL, and `messagingFallbackPollInterval` (default 10s = default TTL / 3) controls the safety-net poll behind change-stream delivery. Applications using short message TTLs should tune the poll interval below their shortest TTL so a lost change-stream event is rescued before the message expires.

#### Messaging: requeued messages are delivered event-driven
Requeueing a message by clearing its `processedBy` via a plain DB update produces no insert event — such messages were only ever found by the interval fallback poll (up to `messagingFallbackPollInterval` latency, risky for short TTLs). The change-stream pipelines of both messaging implementations now additionally match update events whose `updateDescription` shows `processed_by` set to an *empty* array — the requeue signature; normal processing marks use positional keys (`processed_by.0`, …) and stay filtered out — and react with an immediate poll. Requeue latency drops from seconds to milliseconds; the fallback poll remains as safety net. Works on real MongoDB and the InMemoryDriver/PoppyDB event path alike.

#### Messaging: processing decision trace for answer-timeout diagnostics
`SingleCollectionMessaging` keeps a bounded trace (512 entries) of every per-message processing decision — change-stream skips, queue/dequeue, the silent bail-outs (sender==me, not a recipient, already processed, no listener), answer matches. It is dumped **only** by the answer-timeout diagnostics, so normal operation stays log-quiet. Second diagnostics round for the recurring BasicJMSTests flaky: the first round proved misleading ("answer never sent" can be a TTL artifact when the answer TTL equals the await timeout), and a captured occurrence showed an answer being queued for processing and then silently never processed — the trace now names the exact point where a message stops moving. Also exposed as `getProcessingDecisions(msgId)` for tests.

#### Messaging: skipped messages were wrongly marked "recently completed" (blocked requeues for 10s)
When the change-stream listener of `MultiCollectionMessaging` skipped a message *without* processing it (already processed by another instance, lock lost, reread failed), the cleanup path still recorded it in `recentlyCompletedMessages` — making both the listener and all polls ignore that message for the 10s retention. A message requeued during that window was invisible. Only messages that actually reached a listener are recorded now.

#### Messaging: change-stream liveness drives the fallback poll
The change-stream watch loop receives a server reply at least every `maxTimeMS` (an empty batch when there are no events); that heartbeat is now stamped on the `WatchCommand` and exposed as `ChangeStreamMonitor.isStreamLive()`. Both messaging implementations use it to poll *immediately* when a stream falls silent — faster than any timer — instead of waiting for the next interval. The regular `messagingFallbackPollInterval` poll still always runs, deliberately: messages can (re-)appear without any matching stream event, e.g. requeueing by clearing `processedBy` via a plain DB update, and must be found before their TTL expires. `SingleCollectionMessaging` (whose own counter-based gate effectively polled every ~25s) now honors the configurable interval too, and gets the catch-up poll on every watch (re-)establishment for its message and lock monitors — including the one recreated by its stall watchdog. New diagnostics: `MultiCollectionMessaging.topicStreamsLive(topic)` and `SingleCollectionMessaging.changeStreamsLive()`.

### Changed

#### InMemoryDriver/PoppyDB: dbStats and collStats report real sizes instead of zeros
`db.stats()` answered all byte-size fields with 0, and `collStats` reported jol's *shallow* `sizeOf` — the ArrayList object header, not the data (and NPE'd on a missing collection). Both now compute real values: `dataSize`/`size` is the actual BSON size of every document (mongod's definition; computed on demand, O(data) — fine for a diagnostic command), `storageSize` equals it (no padding or compression in memory), `avgObjSize` follows, and index sizes are estimates proportional to the entry count (64 bytes per document per index). New fields: `totalSize`, and on dbStats `fsUsedSize`/`fsTotalSize` reporting the JVM heap — the "filesystem" an in-memory database actually lives on. Index counts now include the implicit `_id` index like mongod. The `$collStats` aggregation stage's `storageStats` uses the same computation; `collStats` on a missing collection answers zeros instead of failing.

#### PoppyDB: reports its real version instead of "5.0.0-ALPHA" / "PoppyDB V0.1ALPHA"
`buildInfo.version` and `serverStatus.version` were hardcoded to `5.0.0-ALPHA` (mongosh greeted every connect with `Using MongoDB: 5.0.0-ALPHA`), and the hello `msg` field still said `PoppyDB V0.1ALPHA (Netty)`. All three now carry the actual product version from the Maven build (via `MorphiumVersion`, shared constant `InMemoryDriver.REPORTED_SERVER_VERSION`) — PoppyDB releases in lockstep with morphium, so mongosh now shows `Using MongoDB: 6.3.0`. Deliberately the PoppyDB version, not a MongoDB compatibility version: protocol capabilities are negotiated via `maxWireVersion`, not this string.

#### InMemoryDriver: O(1) change-stream replay-buffer bound
The ring-buffer bound check in `notifyWatchers` used `ConcurrentLinkedDeque.size()` — O(n), ~200k node traversals per write at PoppyDB's 100k-event replay bound. The deque size is now tracked in an `AtomicInteger`; eviction semantics are unchanged.

### Added

#### InMemoryDriver: the `$merge` aggregation stage is implemented (#241)
`$merge` previously reported success and wrote nothing at all — every persistence call was commented-out dead code — so pipelines materialising results (rollups, denormalised views, ETL-style flows) silently produced no data. It now works: `whenMatched` `merge` (default, incoming fields win) / `replace` / `keepExisting` / `fail`, `whenNotMatched` `insert` (default) / `discard` / `fail`, `on` defaulting to `_id` and accepting a single field or a list, and `into` as a collection name or `{db, coll}`. `merge` and `replace` preserve the target document's `_id`; ambiguous `on` matches and documents missing an `on` field are refused rather than silently guessed; `$merge` is terminal and yields no documents. Writes go through the driver's `find()`/`store()`, so index maintenance, capped/TTL bookkeeping, locking and watcher events all happen. `whenMatched` may also be a custom update pipeline: it runs per match with the existing target document as input and the incoming document bound to `$$new`, supports the stages mongod allows there (`$addFields`/`$set`, `$project`/`$unset`, `$replaceRoot`/`$replaceWith` — anything else is refused), and honours `let` (which, as in mongod, *replaces* the default `{new: "$$ROOT"}`, is evaluated against the incoming document, and is rejected when `whenMatched` is not a pipeline). References to undefined `$$variables` fail up front instead of evaluating to null; the pipeline result keeps the target document's `_id`.

### Fixed

#### InMemoryDriver: `$sample` larger than the collection threw instead of returning all documents
`$sample` cut its shuffled copy with `subList(0, size)`, so a sample size exceeding the collection count failed with `IndexOutOfBoundsException: toIndex = N` instead of returning all documents in random order like mongod. Visible in every mongosh session against PoppyDB: tab completion samples schema documents with `$sample {size: 10}`, so completing on any collection with fewer than 10 documents printed a `Tab completion error: ... aggregate failed: toIndex = 10` stack trace.

#### InMemoryDriver/PoppyDB: unknown commands are answered like mongod instead of throwing
An unregistered command made `InMemoryDriver.runCommand` throw `IllegalArgumentException` — over the wire that meant an ERROR stack trace in the server log and a reply without an error code. mongosh probes `atlasVersion` on **every** connect (Atlas detection) and expects the mongod-shaped rejection, so every mongosh session logged a spurious exception. Unknown commands now return `{ok: 0, code: 59, codeName: "CommandNotFound", errmsg: "no such command: '...'"}`, which clients handle silently — for any unknown command, exactly like mongod.

#### PoppyDB: rs.status spoke Raft and mis-identified wildcard-bound nodes
Two defects in `replSetGetStatus`: the self member's `stateStr` reported the internal Raft enum name (`LEADER`/`FOLLOWER`/`CANDIDATE`) instead of MongoDB's nomenclature (`PRIMARY`/`SECONDARY`/`RECOVERING`), which clients and monitoring tools cannot parse. And with `--bind 0.0.0.0` the node used its bind address as member identity, so it failed to recognize itself in the seed list: rs.status showed the node **twice** (as `0.0.0.0:<port>` and again under its seed name, wrongly marked SECONDARY), the node requested election votes from itself as a "peer", and the `--rs-priorities` lookup missed. The member identity is now canonicalized to the unique seed entry matching the node's port (with a WARN when no unambiguous match exists), and hello's `me`, rs.status' `self` flag and the election identity all agree.

#### PooledDriver: expired connections were pooled on release instead of closed
`releaseConnection` returned connections to the pool even when they had exceeded their `maxConnectionLifetime`/`maxConnectionIdleTime` while borrowed — only the heartbeat's expiry sweep removed them, one sweep later. A borrow burst (e.g. 20 connections) therefore parked a mountain of already-expired connections in the pool, and under load the sweep lagged behind, keeping the pool far above its per-host minimum for many seconds (the `testLotsConnectionPool` flaky; diagnosed with the new `PoolConvergenceReproTest` counter telemetry — the pool's bookkeeping itself is drift-free). Expired connections are now closed on release, like the official MongoDB drivers do; the pool converges within one lifetime window even after bursts.

#### BufferedMorphiumWriterImpl: NPE race between write-buffer users and the flusher
The flush paths remove a type's buffer via `opLog.remove()` without holding the `opLog` monitor, while `addToWriteQueue` and the housekeeping thread re-read `opLog.get(type)` repeatedly between check and use — a concurrent flush in that window turned into an NPE (seen as a BufferedWriterTest failure under parallel-phase load; one code path even caught the NPE with a "can happen" comment instead of fixing the pattern). All check-then-re-get sequences now take a single snapshot reference (`computeIfAbsent` where the entry must exist), and the buffer-full strategies (`WRITE_OLD`/`DEL_OLD`) sort/mutate that snapshot inside the lock instead of re-reading the map outside it.

#### Messaging: answers without an explicit TTL were stored already expired (the BasicJMSTests flaky)
`Msg.sendAnswer` computed `deleteAt = now + getTtl()` **before** any TTL defaulting ran. An answer created via plain `new Msg()`/`new JMSMessage()` (ttl 0 — the JMS ack pattern) was therefore stored with `deleteAt = now`: the TTL sweeper raced the consumer for the freshly inserted document and won in roughly 1–5% of runs, deleting the answer between its change-stream event and the consumer's reread. The result was the long-hunted answer-timeout flaky (BasicJMSTests et al.) — persistent within a run, because the queued-for-processing marker also blocked the fallback poll from rescuing the vanished message. `sendAnswer` now leaves `deleteAt` unset when no TTL was chosen, so the send path applies `messagingDefaultTtl` first and `preStore` derives `deleteAt` from the *defaulted* TTL. Explicit answer TTLs behave as before. Root-caused via the new processing decision trace: `queued → dequeued → runnable started → reread returned null - message gone` told the whole story.

#### InMemoryDriver/PoppyDB: creating a time-series collection now fails loudly (#262 interim)
`create` with a `timeseries` spec used to log a WARN and create a **plain** collection — a silent divergence: no `timeField` enforcement, no retention, `listCollections` reporting the wrong type. It now returns a proper command error (code 115 `CommandNotSupported`) over the wire and raises a `MorphiumDriverException` for embedded users. On the way, `CreateCommand.execute()` was switched from cursor-style reading to `readSingleAnswer` — mongod's create reply is a plain document, and the cursor path silently swallowed cursor-less replies (including error documents) on the in-memory connection. Real time-series support is tracked in #261 (API) and #262 (in-memory emulation), both scheduled for 6.4.0.

#### InMemoryDriver/PoppyDB: resumed change streams could deliver an event twice
A watch resuming with `resumeAfter` registers its subscription *before* replaying the event history (the reverse order would lose events written between history snapshot and live stream). An event written exactly in that window was delivered twice — once by the asynchronous live dispatch to the already-registered subscription, once by the replay — and, because the live dispatch can overtake the replay, in arbitrary order. Resumed subscriptions now suppress exact duplicates by resume token (a bounded recent-token window; a monotonic guard would have turned the reordering into losses). Fresh watches have no replay and are unaffected — no overhead on the messaging path. Real MongoDB never had this problem (oplog-cursor resume is snapshot-consistent); morphium's own consumers (messaging, PoppyDB replication) were already idempotent, so this mainly protects custom `ChangeStreamListener`s running against InMemoryDriver/PoppyDB.

#### PoppyDB: the wire fast path dropped `arrayFilters` (#256 follow-up)
`processUpdateDirect` — PoppyDB's direct dispatch for plain `update` commands — passed the request's per-update `collation` but not its `arrayFilters` to the driver, so a `$[<identifier>]` update sent over the wire (mongosh, any standard client) failed with "No array filter found" while the identical update worked against the InMemoryDriver directly. Third instance of the fast-path-drops-request-options bug class (#252: `ordered`/`collation`, createIndexes: index specs); covered by a `FastPathOptionsTest` seam test like the others.

#### InMemoryDriver/PoppyDB: auth commands no longer pretend to succeed (#245)
The entire server-side authentication surface — `saslStart`, X.509 `authenticate`, `createUser`, `createRole` — consisted of empty stubs that queued no result, which the command-dispatch machinery resolved to `{ok:1.0}`: every client "authenticated" successfully with any or no credentials, and `createUser`/`createRole` reported success while creating nothing. These commands now fail loudly (`AuthenticationFailed`/`NotImplemented` with an unmistakable message) until real SCRAM verification and a user/role store exist. InMemoryDriver/PoppyDB still perform **no** authentication — do not expose them to untrusted networks.

#### Driver: mid-message read timeouts desynchronized the wire stream
A socket timeout that struck after part of a reply had already been read (header consumed, body still in flight — likely under load) left the TCP stream misaligned, and the driver kept using it: `readNextMessage` retried the parse on the same stream, reading payload bytes as a message header (the `Illegal opcode ...` errors, whose "opcode" values decode to ASCII fragments of BSON field names), and returned `null` at its deadline while leaving the half-read connection open for the next pool borrower. Any command on any connection could be hit. `parseFromStream` now distinguishes a timeout at a message boundary (0 bytes consumed — still aligned, retryable as before) from a mid-message timeout, which is surfaced as a fatal network error; the connection is closed instead of retried or pooled. A deadline expiring without any reply also closes the connection now — a late reply would otherwise be delivered to the next borrower (`watch()` reads without `responseTo` verification). `ChangeStreamMonitor` additionally closes, rather than releases, its connection after errors that leave the stream state unknown (a reply without a cursor, unclassified failures); the pool discards closed connections and replaces them.

#### Changestream: events written during a watch restart were lost; messaging could drop messages
When a change stream died and was re-established, a consumer that had not yet received any event had no resume token, so the new stream started at "now" — every document inserted during the retry gap was silently skipped. For messaging this meant lost messages (observed as a subscriber never seeing a broadcast that was sent ~200ms after its stream went down). `watch()` now captures the cursor's `postBatchResumeToken`, which real MongoDB includes in every reply — also for empty batches — and publishes its freshest token on the `WatchCommand` on every exit; `ChangeStreamMonitor` adopts it for the next attempt, so restarts resume where the dead stream stopped. Messaging additionally polls the affected topic (and the DM collection, and all topics for the shared lock monitor) once every time a watch is (re-)established, deterministically catching up on anything written while the stream was down. The messaging fallback poll, documented as running every second but effectively gated to every ~125 seconds by a tick counter, is time-based now and runs every 10 seconds as a pure safety net behind the event-driven catch-up.

#### InMemoryDriver: `store()` failed with a duplicate-key error when replacing an existing document
`storeInternal` located the document to replace via `findByFieldValue`, which returns *copies*, while `CollectionIndexStore` removes index entries by *identity*. The copy never matched, so the old `_id` entry stayed in the index and the following insert reported `E11000 duplicate key` — the ordinary "find it, change it, store it back" round-trip threw for every existing document, and the failed store left the index holding an entry for an already-removed document. The previous document is now resolved through the `_id` index, which yields the live reference. Unnoticed until now because morphium's usual update path goes through `update()`, not `store()`.

#### PoppyDB: wire fast path dropped client options (#244, #252)
The hot-dispatch handlers bypass the generic command path and hardcoded several options to their defaults instead of reading them from the request, so whether an option was honoured depended on which internal path a request happened to take. `createIndexes` forwarded only `unique`/`name` and silently dropped `expireAfterSeconds` (a TTL index was created but never expired anything), `sparse`, `background`, `hidden` and `partialFilterExpression` — the whole index spec is now forwarded. `insert` hardcoded `ordered=true`, so `ordered:false` stopped at the first failing document instead of continuing; `update`/`delete`/`count`/`distinct` hardcoded `collation` to null, silently falling back to binary comparison. All are now read from the request.

#### InMemoryDriver: update-operator correctness cluster (#249)
Six update operators silently did nothing, crashed, or applied only part of the requested change while reporting success: `$pull` with `$elemMatch` never removed anything (each array element was wrapped as a pseudo-document, so the `$elemMatch` list check always failed); `$rename` with a dotted source never resolved it and destructively removed the *target* field instead; `$min`/`$max` threw a `NullPointerException` whenever the target field was absent; `$mul` was a no-op on a missing field (MongoDB creates it as `0`); `$currentDate` only ever wrote the first listed field; and `$push`'s `$sort` modifier was never implemented, so arrays kept insertion order.

#### InMemoryDriver: `$geoWithin` with `$center`/`$centerSphere`/`$polygon` matched every document (#242)
Only `$box` had an implementation; the other shapes matched no branch and fell through to an unconditional `return true`, so those queries silently returned the entire unfiltered collection. All three are now implemented (planar circle, great-circle central angle, ray-casting point-in-polygon), and an unknown shape now fails closed instead of matching everything.

#### InMemoryDriver: query-operator correctness cluster (#251)
`$size` matched documents whose field is entirely absent; `$all` with an empty array matched everything (MongoDB matches nothing) and `$all`+`$elemMatch` never matched at all; `$mod` threw a `ClassCastException` on array-valued fields instead of matching per element; `$type` ignored the array-of-types form; and the bits operators' `byte[]` mask decoder ran its loop backwards, throwing `ArrayIndexOutOfBoundsException` on multi-byte masks and silently decoding single-byte masks to zero. Fixed in both the interpreter and `CompiledQuery`, which carries its own copies of these operators.

#### Expr: date operators use UTC, 1-based `$month`, real ISO week fields (#250)
All date-component operators used the JVM's default timezone, so results depended on the deployment environment; they now evaluate in UTC as MongoDB documents. `$month` was 0-based, `$isoWeek` returned the week-of-*month*, `$isoWeekYear` returned a week number instead of a year, and `$isoDayOfWeek` used Java's Sunday=1 numbering instead of ISO Monday=1. `$week` additionally followed the JVM locale's week rules and now implements MongoDB's Sunday-based 0-53 definition.

#### Expr: `$dateFromParts` returned its JSON shape instead of a date (#260)
`$dateFromParts` was a `MapOpExpr`, which never overrides `evaluate()`, so evaluating it returned the operator's own `{"$dateFromParts": {...}}` map instead of a `Date` — silently, via both the JSON pipeline and the fluent builder. It now constructs the date (UTC by default, honouring an explicit `timezone`, with MongoDB's out-of-range rollover). The `isoDateFromParts(...)` builders, which mapped the ISO week to `month` and the ISO weekday to `day`, are fixed too.

#### InMemoryDriver: `$project` inclusion mode now restricts output to selected fields (#240)
`$project` inclusion (`{field: 1}`) was a no-op — only exclusion (`{field: 0}`) removed anything, so field selection (the most common use of `$project`) silently returned the whole document. An explicit inclusion flag now switches `$project` into strict inclusion mode (output starts empty, only `_id` plus listed/computed fields are kept); computed-only projections keep their historical lenient behaviour. Also live inside `$facet`.

#### InMemAggregator: `$indexStats` no longer silently runs `$geoNear` (#243)
`$indexStats` shared a `case` body with `$geoNear` (distance calc + sort) via mis-grouped labels — the same anti-pattern as #237. It is not implemented, so it now surfaces as a proper command error instead of silently running geoNear logic.

#### Expr: `$avg`/`$max`/`$min` single-arg forms reduce arrays; `$ln`/`$range`/`$reverseArray` fixes (#246, #253)
The single-argument forms of `$avg`/`$max`/`$min` returned an array argument unchanged instead of reducing it (unlike `$sum`); they now reduce to mean/largest/smallest. `$ln` computed `ln(1+x)` (now `ln(x)`), `$range` returned an empty list for descending ranges (now honours step direction), and `$reverseArray` mutated its source list in place (now copies first).

#### InMemoryDriver: `dbStats` per-database, `renameCollection` keeps index definitions (#247, #248)
`dbStats` ignored the requested database and returned a global database count; it now returns per-db `collections`/`objects`/`indexes` scoped to the requested db. `renameCollection` dropped all index definitions on the renamed collection (unique/compound/TTL/sparse) — they now migrate to the new name alongside the capped/TTL bookkeeping from #239.

#### InMemoryDriver: aggregation stages that silently ran `$bucket` now error (#237)
Several pipeline stages (`$planCacheStats`, `$redact`, `$unionWith`, `$currentOp`, `$listLocalSessions`, `$findAndModyfy`, `$update`) shared one `switch` body with `$bucket` via mis-grouped `case` labels, so issuing any of them silently ran `$bucket` logic (or returned an empty result) instead of a real implementation. They are not implemented by the in-memory driver and now surface as an "Unrecognized pipeline stage name" command error (code 40324). The sibling `$bucket`/`$bucketAuto` output-accumulator helper likewise returned `null` for an unknown accumulator operator; it now reports "unknown group operator" (15952).

#### InMemoryDriver: `$avg` leaked an internal `$_calc_` bookkeeping key (#238)
The `$group` `$avg` accumulator kept a running `$_calc_<field>` (sum/count) entry that was never removed, so it leaked into every group output document. A catch-all prefix sweep after the two-pass finalize now drops any residual `$_calc_` keys, covering `$avg` and any future single-pass accumulator using the same pattern.

#### InMemoryDriver: capped/TTL bookkeeping lost on `renameCollection`, stale after `dropIndexes` (#239)
`renameCollection` moved only the document list, leaving the capped config/byte-counter/size-cache and the TTL sweep registration under the origin name — a renamed capped collection silently stopped enforcing its limit and a renamed TTL collection stopped expiring. The bookkeeping now migrates to the target under both collections' write locks. Separately, `dropIndexes` removed the index definition but never cleared the TTL sweep registration, so the driver kept deleting documents by a dropped TTL index; the registration and expiry queue are now cleared when a TTL index is dropped.

#### PoppyDB: replication is now lossless and order-preserving
The secondary's replication pipeline had several correctness defects that could silently lose or reorder data: the initial sync copied the snapshot *before* opening the change-stream watch (writes during the copy were lost), replication batches applied all inserts before updates/deletes (a delete-then-reinsert of the same document within one batch ended up applying insert-then-delete — the document wrongly disappeared), failed bulk applies still acknowledged their sequences to the primary, and bulk-insert `writeErrors` from the InMemoryDriver were silently treated as success. All of this is fixed: the watch now starts before the snapshot and buffered events are replayed afterwards; a snapshot is redone if the watch dies mid-copy (with in-thread backoff so a failing snapshot cannot leave the node permanently ungated); batches preserve global event order and only bundle contiguous same-collection insert runs; sequences are acknowledged only after a successful apply, and failed bulks are replayed as idempotent per-document upserts. A secondary also rejects data-plane traffic (RECOVERING) while its initial sync is running, and change-stream resume across a namespace/db drop is refused instead of silently skipping the drop.

#### PoppyDB: election-mode followers never started replicating
`ElectionManager.handleAppendEntries` stored the incoming leader before the "only on actual change" check compared against it, so `onLeaderDiscovered` never fired and a follower brought up via `--rs-seed` never started its ReplicationManager. The primary consequently saw no secondaries and every `w>1` write failed with `writeConcernError: no secondaries available`. Present since the anti-flapping change (2026-03-30); it became visible only now that write concern is actually enforced (below). Followers now start replication on the first heartbeat from a new leader.

#### PoppyDB: primary/readPreference/transaction/write-concern semantics enforced on the command fast path
Direct-dispatched commands (insert/find/update/delete/count/distinct/createIndexes) bypassed the not-primary rejection, `$readPreference` check, transaction-context setup and the write-concern replication wait — a secondary silently accepted fast-path writes, and `w`/`wtimeout` were ignored for them. A shared `preDispatch()`/`postWrite()` pair now runs before/after every dispatch variant, the replication coordinator is resolved live instead of being frozen per connection (stale after elections), and the per-connection transaction context is cleared after each command.

#### PoppyDB: TLS support was non-functional
An explicitly configured `SSLContext` was ignored (warn-logged), after which the server tried to load the non-existent classpath resources `/server.crt`/`/server.key` and failed with an NPE — SSL-enabled PoppyDB could never start. The configured context is now honored (adapted via the non-deprecated `JdkSslContext` constructor), with a WARN-logged self-signed certificate as dev/test fallback.

#### PoppyDB: find cursors leaked on client disconnect
`channelInactive` never cleaned up open find cursors, and watch/tailable event queues were unbounded. Cursors are now cleaned up on disconnect, idle cursors expire via TTL, and event queues are bounded.

#### Driver: client-side wire compression (snappy/zlib) broke every connection
`SingleMongoConnection.sendQuery()` gave the `OP_COMPRESSED` envelope a *fresh* request id while the reply matcher waited for the inner message's id. Any server replying to the envelope id — per spec the requestID of the original message, which PoppyDB and real MongoDB both do — triggered `connection out of sync` on every reply, killing the connection and eventually removing the host from the pool (`No such host`). Client-side compression now works against PoppyDB and MongoDB; server-side-only compression was unaffected.

#### InMemoryDriver: transaction commit no longer clobbers concurrent writes
`commitTransaction` replaced the *entire* database with the transaction's start snapshot, silently discarding every write other threads committed to unrelated collections while the transaction was open. Commit now merges back only the collections the transaction actually touched.

#### PooledDriver: empty hosts map is re-seeded from the host seed — driver no longer permanently dead after a full replica-set outage (#233)
When every replica-set member was unreachable long enough (rolling restart with overlapping windows, short network partition), `onConnectionError` evicted all hosts and the driver had no way back: the heartbeat only iterates the hosts map, and `handleHelloResult` — the only place re-adding hosts — only runs from heartbeat threads. Every operation failed with `No primary node found - not connected yet?` until the application was restarted, even though the cluster was healthy again (observed in production on morphium 6.1.8, 2026-07-16; the defect existed unchanged on develop). The heartbeat now re-seeds the hosts map from the configured host seed when it finds it empty, restarting the normal discovery cycle.

#### InMemoryDriver: event dispatcher no longer uses virtual threads — JVM-wide logging deadlock under JDK 21 (#234)
The change-stream event dispatcher used a virtual-thread factory. Under load, dispatcher threads pinned to their carriers while parked on the logback appender lock could occupy every carrier of the common ForkJoinPool; the unmounted virtual thread holding the lock then never got scheduled again, freezing every thread that logs (observed as a 20+ minute hang of the InMem CI phase in `SingleCollectionMessaging.terminate()` → `log.info()`). This is the same JDK-21 pinning/starvation class that led to the earlier project-wide virtual-thread rollback; the dispatcher had been missed. It now uses daemon platform threads.

#### Expr: `$in` rejects a non-array second operand — matching MongoDB (error 40081)
The `$in` aggregation expression (also used in query `$expr`) silently returned `false` when its array operand resolved to null (e.g. a missing field path), a scalar or any other non-array — pipelines that fail on real MongoDB (`$in requires an array as a second argument`) passed against the in-memory evaluation. It now throws an `IllegalArgumentException` instead. **This reverts the lenient behavior introduced in 6.2.9**, which had replaced the previous `NullPointerException` with `false`; the clean error message stays. Java arrays are accepted as operand alongside `List`, and elements are compared null-safely.

#### InMemoryDriver: `$in` / `$nin` reject scalar and null operands — matching MongoDB (`$in needs an array`)
The 6.2.9 operand normalization went too far: besides accepting Java arrays and `Iterable`s (which stays), it silently wrapped scalars into single-element lists and turned `null` into an empty list — `{$in: "a"}` behaved like `{$in: ["a"]}`, hiding query bugs that real MongoDB rejects with `BadValue: $in needs an array`. Non-array operands now fail query validation (also on empty collections) with an `IllegalArgumentException`.

#### InMemoryDriver: `$unset` supports array-index path segments (e.g. `ratings.0.rating`)
The dotted-path `$unset` support added in 6.2.9 stopped at `List` intermediates, so valid paths through array indexes were a silent no-op. Numeric segments now index into arrays, matching MongoDB semantics: `ratings.0.rating` removes the field inside the first element, and `$unset` on an array element itself (`tags.1`) sets it to `null` instead of removing it. Non-numeric segments on arrays and out-of-range indexes remain a no-op.

#### Driver: handshake metadata sent the hardcoded version "6.2"
The `hello` client metadata reported `driver.version: "6.2"` regardless of the actual Morphium version, making the field useless for telling patch levels apart on the server side. The real version is now read at runtime from `morphium-version.properties`, a Maven-filtered classpath resource (`MorphiumVersion.getVersion()`, fallback `"unknown"`) — this also works in GraalVM native images, unlike the jar manifest. Additionally, the connect handshake built its `HelloCommand` without a connection, so `driver.name` was always reported as `Morphium V6/unknown`; the driver name is now resolved (`Morphium/PooledDriver` etc.). Verified end-to-end against a real replicaset via `db.currentOp()`.

## [6.2.10] - 2026-07-21

### Fixed

#### Driver: mid-message read timeouts desynchronized the wire stream
A socket timeout that struck after part of a reply had already been read (header consumed, body still in flight — likely under load) left the TCP stream misaligned, and the driver kept using it: `readNextMessage` retried the parse on the same stream, reading payload bytes as a message header (the `Illegal opcode ...` errors, whose "opcode" values decode to ASCII fragments of BSON field names), and returned `null` at its deadline while leaving the half-read connection open for the next pool borrower. Any command on any connection could be hit. `parseFromStream` now distinguishes a timeout at a message boundary (0 bytes consumed — still aligned, retryable as before) from a mid-message timeout, which is surfaced as a fatal network error; the connection is closed instead of retried or pooled. A deadline expiring without any reply also closes the connection now — a late reply would otherwise be delivered to the next borrower (`watch()` reads without `responseTo` verification). `ChangeStreamMonitor` additionally closes, rather than releases, its connection after errors that leave the stream state unknown (a reply without a cursor, unclassified failures); the pool discards closed connections and replaces them.

#### Changestream: events written during a watch restart were lost; messaging could drop messages
When a change stream died and was re-established, a consumer that had not yet received any event had no resume token, so the new stream started at "now" — every document inserted during the retry gap was silently skipped. For messaging this meant lost messages (observed as a subscriber never seeing a broadcast that was sent ~200ms after its stream went down). `watch()` now captures the cursor's `postBatchResumeToken`, which real MongoDB includes in every reply — also for empty batches — and publishes its freshest token on the `WatchCommand` on every exit; `ChangeStreamMonitor` adopts it for the next attempt, so restarts resume where the dead stream stopped. Messaging additionally polls the affected topic (and the DM collection, and all topics for the shared lock monitor) once every time a watch is (re-)established, deterministically catching up on anything written while the stream was down. The messaging fallback poll, documented as running every second but effectively gated to every ~125 seconds by a tick counter, is time-based now and runs every 10 seconds as a pure safety net behind the event-driven catch-up. The interval is derived as one third of the default message TTL (30s/3 = 10s), so a lost event is always rescued well before the message expires; 6.3.0 makes both values configurable (`messagingDefaultTtl`, `messagingFallbackPollInterval`).

## [6.2.9] - 2026-07-14

### Added

#### Aggregator: WARN when a renamed project(Map) key is referenced by its original spelling (#208)
`project(Map)` translates its keys through the entity's field-name mapping. When a later stage references such a key by the name the user wrote, the reference points at a non-existent field and MongoDB silently returns `$sum: 0` / `$push: []`. Both aggregator implementations now log a WARN (once per reference) naming both spellings. `$$`-variables and `$literal` subtrees are ignored; dot-paths are matched by their first segment.

#### Aggregator: opt-in consistent field-name translation (#208, #217)
New opt-in setting `translateAggregationFieldNames` (`ObjectMappingSettings`, overridable per aggregator via `Aggregator.setTranslateAggregationFieldNames`): when enabled, Java property names are translated to Mongo field names. Covered stages: group operator `$`-references and id values, `project(Map)` and `addFields`/`set` keys *and values*, `sort(Map)` keys, `graphLookup` connect fields and `startWith` — including `$`-references inside `Expr` values there. **Not covered** (tracked in #221): stages taking a raw `Expr` — `match(Expr)`, `sortByCount`, `replaceRoot`/`replaceWith`, `redact`, `bucket`, `facetExpr`, `unwind(Expr)` — use Mongo field names or `Expr.field(Enum)` there. Dot-paths translate their first segment; `$$`-variables and `$literal` subtrees are never touched. **Default off = exactly the previous behavior.** The effective config value is snapshotted when the aggregator is created; the per-aggregator override wins at any time.

New helpers `Aggregator.ref(Enum)` / `Aggregator.name(Enum)` translate enum field references explicitly (`F.itemCount` → `"$item_count"` / `"item_count"`), independent of the flag: `group.sum(agg.name(F.itemCount), agg.ref(F.itemCount))`. All new `Aggregator` interface methods are default methods — third-party implementations keep compiling.

Known limitation: translation operates on the serialized pipeline, where `Expr.string("$...")` is indistinguishable from a field reference. Wrap string values that look like field references in `$literal` when the flag is on.

#### PoppyDB: priority-based leader step-back after failover (#177)
A PoppyDB leader now voluntarily hands leadership to a peer with higher election priority, mirroring MongoDB's priority takeover. Previously a failover to a lower-priority node was permanent — the preferred primary never returned, even after it recovered.

The leader yields only once the higher-priority peer answers its heartbeats and has acknowledged everything replicated during the leader's term, and only after it has been leader for `priorityTakeoverMinStabilityMs` (default 30s), so a settling cluster does not flap. Followers report their priority in the `appendEntries` response; nodes that omit it (older versions) never trigger a takeover.

Enabled by default. Configurable via `ElectionConfig.priorityTakeoverEnabled` / `-Dmorphiumserver.priorityTakeoverEnabled=false` plus `priorityTakeoverCheckIntervalMs`, `priorityTakeoverMinStabilityMs`, `priorityTakeoverMaxLag` and `priorityTakeoverStepDownSecs`. In a cluster where all nodes share the default priority (50), behavior is unchanged.

### Deprecated

#### 7.0-removal candidates now carry `@Deprecated(since = "6.3", forRemoval = true)` (#218)
Members confirmed for removal in 7.0 (#172 et al.) are now annotated `@Deprecated(since = "6.3", forRemoval = true)`, and their Javadoc names the replacement — IDEs flag usages a full minor release before anything is removed. Covered groups: the flat `MorphiumConfig` setters/getters (use the `Settings` sub-objects via `connectionSettings()`, `objectMappingSettings()`, ... instead), the `MorphiumBase.set…`/`unsetQ…` variants, the legacy `SingleCollectionMessaging` constructors, `Query.complexQuery`/`getById`/`textSearch`, `Msg.name`, `MorphiumMessaging.setProcessMultiple`, `MongoBob` and `@UseIfnull` (use `@IgnoreNullFromDB`). Members that stay deprecated-but-kept, and the BSON-spec deprecations in `MongoType`, are unchanged. Pure annotation/Javadoc change, zero runtime impact.

### Changed

#### Messaging: unified `processed_by` field-name handling (#219)
The Mongo field name of `Msg.processedBy` is now resolved once per messaging instance via the object mapper instead of being hardcoded at ~15 call sites. The dual-name defensive read (`processed_by`/`processedBy`) in the exclusive-message path was removed — documents written with the non-canonical camelCase spelling (never produced by Morphium itself) are no longer recognized there.

#### Aggregator: `graphLookup` enum overload now translates connect fields (#217)
`graphLookup(Class, Expr, Enum, Enum, ...)` passed `connectFromField.name()` / `connectToField.name()` through untranslated — same defect family as the `lookup` enum overload fixed in 6.2.5 (#198). The enum overload now always translates both connect fields against the given from type, independent of the `translateAggregationFieldNames` flag. Code that relied on the raw enum name reaching the pipeline must use the String overload instead.

### Fixed

#### InMemAggregator: `$count` on empty input emitted `{field: 0}` — MongoDB emits no document (#228)
The in-memory `$count` stage always produced a result document; real MongoDB returns an empty result set when the stage input is empty. The stage now matches MongoDB, and `InMemAggregator.getCount()` gained the same empty-result guard `AggregatorImpl` already had.

#### MorphiumConfig: `getMaximumRetriesBufferedWriter()` returned the AsyncWriter value (#227)
The deprecated flat getter delegated to `WriterSettings.getMaximumRetriesAsyncWriter()` instead of `getMaximumRetriesBufferedWriter()` — callers silently got the async-writer retry count whenever the two settings differed (both default to 10, which is why it never surfaced). Found while writing the #218 replacement Javadoc.

#### Aggregator: `Group.stdDevSamp(String, Object)` emitted `stdDevSamp` without the `$` prefix (#222)
The operator map was built as `{stdDevSamp: ...}` instead of `{$stdDevSamp: ...}`, so the String-based `stdDevSamp` accumulator never worked. (The `$stdDevPop` sibling was correct.)

#### InMemoryDriver: `$unset` now supports dotted (nested) field paths
`$unset` only removed top-level keys via a flat `Map.remove(key)`, so unsetting a nested field such as `es_upload.acceptance.idx` was a silent no-op — the field stayed and the update reported `nModified: 0`. It now navigates the sub-documents and removes the leaf key, matching MongoDB (missing/non-document intermediate segments remain a no-op). Regression test in `InMemUnsetDottedPathTest`.

#### InMemoryDriver: `$in` / `$nin` accept Java-array operands, not just `List`
`$in`/`$nin` hard-cast their operand to `List`, throwing `ClassCastException` when a raw query supplied a Java array (e.g. a `String[]` passed into `rawQuery` as `{_id: {$in: ids}}`) — MongoDB/BSON serialization would deliver a list, but the in-memory driver sees the original array. The operand is now normalized (`List`, object/primitive arrays and other `Iterable`s all accepted). Regression test in `InMemInArrayOperandTest`.

#### Expr: `$in` expression no longer throws on a null/missing array operand
The `$in` aggregation expression (also used in query `$expr`) iterated its array operand unguarded, throwing a `NullPointerException` when it resolved to a missing field path (e.g. `$source_shortcuts`) or a non-list value. It now treats a null/non-list array as "not contained" and returns `false`, and compares elements null-safely.

## [6.2.8] - 2026-07-13

### Fixed

#### Driver: reply/request matching and watch cursor leak on `SingleMongoConnection`
A production incident (JEF runners, 2026-07-11/12) showed waves of `Error 43 - cursor id not found` on unrelated fresh queries, ending in a permanently stalled consumer. Two causes: watch `getMore`s used `maxTimeMS=maxWaitTime` while the client also waited only `maxWaitTime` for the reply and regularly lost that race, restarting the change stream in place and leaking the server-side cursor (hundreds of idle `$changeStream` cursors); and `readSingleAnswer()`/`getAnswerFor()` ignored the reply's `responseTo`, so once a connection was out of sync every caller got its predecessor's answer until one blocked forever. `readReplyFor()` now verifies `responseTo` against the request id and poisons/closes the connection on mismatch (retriable `MorphiumDriverNetworkException`, same pattern as the code-251 handling).

## [6.2.7] - 2026-07-10

### Fixed

#### Messaging: exclusive messages processed twice when the `MsgLock` was lost mid-processing
Exclusive messages relied solely on the `MsgLock` for exactly-once delivery — `processed_by` was written only *after* `onMessage` (unless the listener opted into `markAsProcessedBeforeExec`). If the lock vanished mid-processing (TTL, cleanup, failover) and the message was re-fetched via the poll path (active during change-stream stalls), a second instance re-locked it, saw an empty `processed_by` and processed it again (observed in production as 1 message → 2 JEF tasks → 2 invoices with the same number). Exclusive messages now mark `processed_by` *before* invoking the listener and roll the mark back (new helper `removeProcessedBy`) on rejection or listener failure, preserving retry semantics. New fault-injection test `ExclusiveOnceReproTest`.

## [6.2.6] - 2026-07-08

### Added

#### InMemoryDriver: `$setOnInsert` and upsert/`new` support in `findAndModify` (#203)
The `InMemoryDriver` now honors `$setOnInsert` and the `upsert`/`new` flags in `findAndModify`, matching MongoDB behavior. Includes a regression test for upsert via `$and`-nested `_id` filters (#202, #204).

### Changed

#### SequenceGenerator: duplicated lock lifecycle extracted (#171)
`getNextValue()` and `getNextBatch()` shared ~40 identical lines of insert-based lock acquisition (retry with jitter, proactive stale-lock clearing) and release. Both now run their critical section through a single `withSequenceLock(Supplier)` helper. No behavioral change.

#### Internal: legacy `Vector`/`Hashtable` replaced with concurrent collections (#173, #212)
`AbstractCacheSynchronizer`, `MorphiumCacheImpl`/`MorphiumCacheJCacheImpl` and `jms/Producer` now use `ConcurrentHashMap`/`CopyOnWriteArrayList` instead of `Hashtable`/`Vector`; `BufferedMorphiumWriterImpl` uses `Collections.synchronizedList` consistently. Thread-safety guarantees are unchanged or strengthened (listener iteration is now safe against `ConcurrentModificationException`); no API change. Remaining `printStackTrace()` calls in production code were routed through SLF4J.

### Fixed

#### Driver: replicaset failover repaired — bounded timeouts, write retries, changestream recovery
During a primary failure (crash, frozen VM, network partition) the driver effectively never recovered: writes failed or hung indefinitely, messaging never reconnected. Root causes and fixes:

- `readNextMessage` tolerated 100 consecutive socket timeouts, multiplying the intended timeout by 100 (`maxWaitTime` 60s → >1h hang per operation). The timeout is now a hard total deadline.
- `WriteMongoCommand`'s step-down handling was dead code (string comparison against `"not primary"` never matched the formatted error). Step-downs are now detected via mongo error codes (10107/189/91/11600/11602/13435) and retried on the newly resolved primary; network errors and missing replies are retried the same way (at-least-once, like `retryWrites`).
- `ChangeStreamMonitor` terminated permanently on "No such host" (thrown in the window between host eviction and re-add during failover), killing messaging for good. It now retries; error handling is extracted into a testable `handleWatchError()`.
- `handleHelloResult` compared the advertised primary against the hosts map without `normalizeHostKey`, breaking primary discovery via secondaries on casing/port differences.
- Dead-host detection: heartbeat hellos and the connect handshake use a bounded timeout instead of `maxWaitTime`; eviction closes borrowed connections so in-flight operations fail fast and get retried; `borrowConnection` polls in slices and aborts when the host is evicted.
- `SingleMongoConnectDriver` slept `sleepBetweenErrorRetries * 10000` (~16min) on a null hello during reconnect.

Verified with unit tests plus a manual failover suite (`FailoverReproTest`: SIGTERM, kill -9, SIGSTOP freeze, restart-while-primary-down against a local 3-node replicaset). Before: 1 successful write in 45s after a hard kill, messaging dead. After: full write throughput ~25s after failure, no lost messages.

#### SingleMongoConnectDriver: `dropCollection` self-deadlock and `connectionInUse` race (#215)
Two related defects around the single connection's in-use flag. `MorphiumWriterImpl.dropCollection` held the drop connection while polling `morphium.exists()` — which borrows its own connection. With the `SingleMongoConnectDriver` (exactly one connection) the poll starved against the caller's own claim until `maxWaitTime * 5` (minutes), whenever the dropped collection actually existed. The connection is now released before polling. Additionally, `connectionInUse` was a plain non-volatile boolean with check-then-act races between `getConnection()` and the heartbeat; it is now an `AtomicBoolean` claimed via `compareAndSet`, and the `connection` field is `volatile`. Affects all users of the `SingleMongoConnectDriver`, including PoppyDB's Raft `ElectionNetworkClient`. CI only exercises the PooledDriver, so this never surfaced on the test runner.

#### Query: `findOneAndUpdate(Map)` deleted the matched document on a read-cache hit (#214)
The read-cache branch in `findOneAndUpdate(Map)` was copy/pasted from `findOneAndDelete()` and **deleted** the cached document instead of applying the update — silent, timing-dependent data loss for entities with `@Cache(readCache = true)`. A find-and-update always has a write side-effect, so it is never served from the read cache anymore: the `FindAndModifyMongoCommand` executes unconditionally, the pre-update document state is no longer written to the cache, and a successful modification invalidates the type's read cache (`clearCacheIfNecessary`).

#### InMemoryDriver: dotted field paths in queries
`find` no longer rewrites dotted query keys, so nested paths containing upper-case segments match correctly; `distinct` resolves dotted paths into the nested document instead of doing a flat lookup.

### CI / Tests

#### Test runner: retry classification fixed — failed retries were reported as "passed on retry"
`get_test_stats` parsed a hardcoded `test.log` directory; phase retries log to `test.log.<phase>.retries_log`, so retry statistics always came back empty and **every** retried test was classified as flaky, even when the retry failed identically. `stats.sh` now honors `MORPHIUM_TESTLOG`. Flaky classifications from earlier runs are unreliable.

#### Test tags: new `manual` tag — real failover tests never run in CI
`-Pexternal` cleared the surefire tag excludes entirely, so manual-only tests (hardcoded localhost replicaset, mongod process kills) leaked into the external CI phases. New semantics: `external` = needs a real MongoDB (CI-safe, enabled by `-Pexternal`); `manual` = process-killing/hardcoded-local tests, excluded by default, by `-Pexternal` and by `runtests.sh`. All real failover tests (`FailoverReproTest`, `SingleConnectDriverFailoverTests` incl. `testHeartbeat`, pool `FailoverTests`, `FailoverTest`) are tagged `manual`; the remaining `failover` tag only marks tests to skip on PoppyDB phases.

## [6.2.5] - 2026-06-26

### Added

#### ClassGraph: `preRegisterClassesWithAnnotation()` for build-time discovered classes (#200)
Adds a pre-registration hook to `ClassGraphCache` so frameworks that know all annotated classes at build time (e.g. the quarkus-morphium extension via Jandex) can inject them and skip the runtime ClassGraph scan — essential for Quarkus native images, where a live scan finds nothing. Pre-registrations live in a separate map that always wins over the scan cache and survives `invalidate()`; `clearPreRegistrations()` drops them explicitly. Empty lists are valid pins (skip the scan, return empty). Covers the name-based `getClassesWithAnnotation()` path.

#### DNS: resolve TXT seedlist options for `mongodb+srv://` (#169)
`mongodb+srv://` URLs previously resolved the SRV host list but ignored the companion TXT record, forcing Atlas users to set `authSource`/`replicaSet` by hand. `DnsSrvResolver` now also resolves and parses the TXT record (RFC 1035 length-prefixed character-strings, `k=v&k=v` options). `Morphium.resolveAtlasUrlIfNeeded()` applies `authSource → mongoAuthDb` and `replicaSet → requiredReplicaSetName`, but only when not already configured, so explicit user configuration always wins (per the DNS Seedlist spec). TXT resolution failures yield empty options and never block a connection.

### Changed

#### Messaging/ChangeStream: configurable change stream batch size
The change stream `getMore` batch size is no longer hardcoded to `1`. It is now configurable via `DriverSettings.changeStreamBatchSize` (default `100`) and can be overridden per monitor through `ChangeStreamMonitor.setBatchSize()`.

A batch size of `1` delivers exactly one event per `getMore` round-trip, which caps stream throughput at roughly one event per network round-trip. On localhost this is unnoticeable, but over a high-latency link (e.g. an SSH/SOCKS tunnel with tens of milliseconds RTT) a busy stream cannot keep up: it drains a backlog at only ~1/RTT events per second and falls behind, delivering events — including messaging answers awaited by `sendAndAwaitAnswers()` — up to tens of seconds late, until traffic drops and the cursor catches up.

Because `awaitData` returns as soon as the first event is available, a larger batch size adds no latency at low traffic but lets a single round-trip drain many backlogged events. The original reason for `batchSize=1` (a multi-document-batch hang in the previous `watch()` implementation) no longer reproduces after the change stream rewrite. The effective batch is still bounded by MongoDB's ~16MB per-reply limit regardless of the configured count.

### Fixed

#### InMemoryDriver: seed upserted document from equality predicates nested in `$and` (#201)
On upsert the `InMemoryDriver` seeded the new document only from top-level non-`$` filter keys. With a filter like `{$and:[{_id:"lock"},{expires_at:{$lte:now}}]}` the `_id` equality was never seeded, so the upserted document got a generated `ObjectId` and a later `delete({_id:"lock"})` never matched (lock leak in the quarkus-morphium migration runner). `collectUpsertEqualityFields()` now seeds the document the way MongoDB does: scalar and `$eq` predicates are seeded, `$and` is recursed, dotted names become nested documents, and operator predicates / `$or` / `$nor` are not seeded. Verified against MongoDB 8.0.13.

#### DNS: only use public DNS as a last-resort fallback (#170)
`DnsSrvResolver.systemDnsServers()` appended `8.8.8.8`/`1.1.1.1` unconditionally, even when system name-servers were present. In split-DNS / private-Atlas setups this could resolve SRV records against public DNS (wrong results) and caused a per-server timeout when outbound UDP/53 is firewalled. Public DNS is now only added when no system name-server is configured (e.g. a minimal container without `/etc/resolv.conf`); an existing system resolver is treated as authoritative and fails fast.

#### Aggregation: field name translation in `unset(Enum...)` and `lookup` foreignField (#198)
Follow-up to #198: two remaining field-name translation gaps. `unset(Enum...)` in `AggregatorImpl` and `InMemAggregator` passed `Enum.name()` raw to the pipeline instead of translating via `tf()`, and `AggregatorImpl.lookup(Class, Enum, Enum, ...)` did not translate the `foreignField` with the lookup type. Both `Aggregator` implementations are now covered by explicit tests.

#### InMemoryDriver: `$expr` queries with aggregation operators no longer rejected
`QueryHelper.validateQuery` now only recurses into operators whose payload is a query document (`$and`, `$or`, `$nor`, `$not`, `$elemMatch`), so aggregation expression operators inside `$expr` (e.g. `$dateFromString`) are no longer misclassified as unknown query operators. Unknown top-level and field-level operators are still rejected.

#### ObjectMapping: `BigDecimalMapper.unmarshall` tolerates `Integer`/`Long`
`unmarshall` did `new BigDecimal((double) d)` and threw `ClassCastException` when MongoDB returned an integer-literal field as int32/int64. It now goes through `Number#doubleValue()` for any `Number` type, with a passthrough for already-decoded `BigDecimal`.

## [6.2.4] - 2026-05-08

### Added

#### `MorphiumDocumentTooLargeException` for BSON size limits
Introduced a dedicated `MorphiumDocumentTooLargeException` that is thrown when a document exceeds the 16MB BSON limit. This replaces generic `MorphiumDriverException` for these cases, allowing callers to programmatically handle oversized documents.

#### Messaging: Server-side recipient/sender filtering
`SingleCollectionMessaging` now uses a server-side `$match` stage in its change stream pipeline. This significantly reduces wire traffic and client-side decoding overhead by filtering out messages not intended for the current node directly on the MongoDB server.

#### Messaging: Passive liveness watchdog and cursor recovery
Added a watchdog that monitors the health of the messaging change stream. It can detect when a cursor has fallen behind or stalled and automatically restarts it to ensure timely message delivery.

#### Aggregator: Field name translation support (#198)
The `Aggregator` pipeline now supports field name translation, ensuring that Java camelCase field names are correctly mapped to their MongoDB snake_case counterparts during aggregation.

### Fixed

#### Messaging: Robustness against Errors in main loop
The messaging main loop now catches `Throwable` instead of just `Exception`. This prevents the messaging thread from dying silently due to `Error`s (like `OutOfMemoryError`), keeping the system more resilient.

#### Field translation in `Query.distinct()` (#197)
Fixed a bug where `Query.distinct()` and `explainDistinct()` did not translate Java field names, leading to incorrect results when using camelCase names.

#### Messaging: Thread liveness check
Added a FATAL log message when the messaging main thread terminates unexpectedly, improving visibility into component failures.

## [6.2.3] - 2026-04-20

### Added

#### `defaultQueryTimeoutMS` configuration (#182)
A new `defaultQueryTimeoutMS` setting decouples the query/operation timeout from the connection pool wait time. Previously both reused `maxWaitTime`, making it impossible to wait long for a connection while still timing out individual queries quickly. Applied as fallback to both `Query` execution and aggregation commands.

#### `storeList(..., continueOnError)` for partial-failure batch stores (#190)
New overload `storeList(List<T>, String collection, boolean continueOnError)` continues processing remaining entities when individual stores fail, mirroring MongoDB's `ordered: false` insert semantics. Successful entities are persisted; failures are reported via the returned result. As part of this work, entity classification logic was refactored into a shared helper using Java records instead of `Object[]`.

#### Batched versioned-entity updates in `store(List)` (#185)
Versioned-entity updates within a `store(List)` are now batched per connection instead of executing one round-trip per entity, reducing pool overhead noticeably for large lists.

### Fixed

#### Connection swap in `StoreMongoCommand` not propagated to caller (#191)
When `StoreMongoCommand` swapped to a fresh connection (e.g. after a network error), the new connection reference was not returned to the caller. The caller continued using the stale reference, leading to inconsistent connection state. The swap is now propagated back correctly.

#### Transient `WriteConflict` (error 112) not retried (#184)
Single-document writes hitting a transient `WriteConflict` outside a transaction were surfaced to the caller instead of being retried. `WriteMongoCommand` now retries on error 112 — except inside an explicit transaction, where the caller must own the retry decision.

#### `null` collation sent in write commands (CosmosDB compatibility) (#186)
Write commands serialized an explicit `collation: null` field when no collation was set. CosmosDB rejects this with a parse error. Null collations are now omitted from the command document.

#### Insert/upsert `writeErrors` not surfaced as structured errors (#187, #188)
- `InsertMongoCommand` and `WriteMongoCommand` failures now attach a structured `writeErrors` list to the thrown `MorphiumDriverException`, matching MongoDB's response format.
- `InMemoryDriver.insert()` now produces proper `writeError` documents (with `index`, `code`, `errmsg`) for duplicate-key failures.
- `FindAndModifyMongoCommand` now throws `MorphiumDriverException` with structured `writeErrors` on failure instead of returning a partial result.
- Dead `writeErrors` checks following `InsertMongoCommand.execute()` were removed (the command now throws instead of returning errors).

#### `InMemoryDriver` insert did not honor `ordered: false` (#189)
When `ordered=false` was requested, `InMemoryDriver.insert()` still aborted at the first failure like the ordered case. It now continues inserting remaining documents and returns all `writeErrors` together, matching MongoDB semantics.

#### Missing `return` in `save(T, String, AsyncOperationCallback)` (#183)
A missing `return` after the `saveList()` call caused execution to fall through and double-process the entity.

#### PoppyDB startup checks and `status` command
Stabilized PoppyDB startup checks and added the missing `status` command implementation.

## [6.2.2] - 2026-03-31

### Fixed

#### PoppyDB: Update operations now return correct matched/modified counts
The InMemoryDriver returned `"matched"` instead of the MongoDB-standard `"n"` key in update results. This caused all update-based operations (inc, set, sequence, bulk updates) to fail with "Update failed" or "Error - not updated" when running against PoppyDB over the wire protocol.

#### PoppyDB: Find queries now respect batchSize (server-side cursor support)
`processFindDirect` previously returned all matching documents in a single `firstBatch` regardless of the requested `batchSize`, with cursor ID always 0. This broke iterators and cursors that rely on batched fetching. PoppyDB now returns only the requested batch and registers a server-side cursor for `getMore` requests.

#### PoppyDB: Insert error response includes nModified field
Duplicate-key error responses from insert operations were missing the `nModified` field, causing a `NullPointerException` in `ThrowOnError` predicates that call `Number.intValue()` on the missing map entry.

#### Expr.arrayExpr() parse roundtrip
`ArrayExpr.toQueryObject()` used `Arrays.asList(stream.toArray())` which wrapped the result array as a single element instead of unpacking it. Also fixed `Expr.parse(List)` which returned `List<Expr>` objects instead of mapped query objects, and added proper `evaluate()` overrides for both `ArrayExpr` and parsed list expressions.

#### IndexDescription.equals() false mismatches
The comparison treated `null` and `false`/`0` as different values for boolean and integer fields (e.g., `background`, `sparse`, `unique`). Since MongoDB may return explicit `false` for fields that Java leaves `null`, this caused indices to appear "missing" on every startup, triggering repeated create-index attempts that fail with "Index already exists". Also removed a stale `log.info()` call inside `equals()` that logged every single index comparison at INFO level.

#### PoppyDB: Upsert operations now correctly report document count
Upserted documents were not included in the `"n"` count of update responses. MongoDB returns `n: 1` for a successful upsert (even though `matchedCount` is 0), but PoppyDB returned `n: 0`. This broke `storeMap()` assertions and any code that checks the update result count after an upsert.

#### PoppyDB: Wire protocol corruption on concurrent change stream responses
The `CompletableFuture.whenComplete()` callback for watch/tailable cursor `getMore` responses wrote directly to the Netty channel from a background thread. When a change stream event arrived while the I/O thread was writing another response on the same connection, the bytes were interleaved, producing corrupted wire protocol messages (`Illegal opcode 0`, `wrong section ID`). Responses are now dispatched back to the Netty event loop thread, serializing all writes per connection.

#### PoppyDB: writeErrors from InMemoryDriver not forwarded
`processUpdateDirect` in the Netty command handler silently dropped `writeErrors` returned by the InMemoryDriver (e.g., duplicate key errors on upsert). These errors are now included in the wire protocol response, matching MongoDB behavior.

#### Thread leak in PooledDriver.close() and ReplicationManager reconnect
`PooledDriver.close()` did not signal `waitCounterCondition`, leaving `ConnectionWaiter` threads blocked forever. Over time this accumulated thousands of leaked threads. Fixed by calling `signalAll()` before shutdown. Additionally, `ReplicationManager.replicationLoop()` now calls `disconnectFromPrimary()` before `connectToPrimary()` to prevent accumulating stale Morphium instances on repeated reconnects.

#### Change stream events lost after collection drop and resume
Several race conditions in the InMemoryDriver's change stream implementation could cause events to be lost or duplicated after a collection drop:
- **Stale async events**: Events dispatched by virtual threads after a collection drop could sneak into the change stream history with tokens from the pre-drop era. Fixed by advancing the sequence counter by 100 on drop and filtering events whose tokens fall below the drop boundary.
- **Resume-after replay**: `replayHistory()` now uses the maximum of the resume token and the drop boundary sequence, preventing stale events from being replayed.
- **History purge**: `drop()` now purges the change stream history for the dropped collection both before and after the drop notification, ensuring no stale events survive.

#### ChangeStreamMonitor race condition on startup
`running` was set to `true` after `Thread.start()`, creating a window where the `run()` method could see `running=false` and exit immediately. Fixed by setting `running=true` before calling `Thread.start()`.

#### PoppyDB: Tailable cursor events not delivered from direct insert path
The performance-optimized direct insert path (`processInsertDirect`) did not call `notifyTailableCursorsOnInsert()`. Only the generic command path had this notification. Tailable cursors on capped collections never received new documents, causing `TailableQueryTests` to fail on all PoppyDB phases.

#### PoppyDB: Hostname 0.0.0.0 in hello response breaks client connections
When PoppyDB binds to `0.0.0.0`, the `hello` response reported `hosts: ["0.0.0.0:17017"]`. Clients tried connecting to `0.0.0.0` which is unreachable from remote hosts. PoppyDB now resolves `0.0.0.0` to the actual hostname via `InetAddress.getLocalHost()`.

#### PoppyDB: Raft election flapping under load
Three nodes on the same host with equal priority (50) caused endless split-vote elections. Combined with `onLeaderDiscovered` firing on every heartbeat (not just on changes) and non-atomic `isLeader()`/`getCurrentLeader()` reads in `getHelloResult()`, the PooledDriver saw rapid primary flapping ("Primary failover?" multiple times per second). Fixed by:
- Election timer generation guard prevents stale timer callbacks from triggering spurious elections
- `cancel(true)` instead of `cancel(false)` for all timer tasks
- `getLeaderSnapshot()` provides atomic leader state reads
- `onLeaderDiscovered` only fires on actual leader changes
- RS nodes should use different priorities (e.g. `--rs-priorities 100,75,50`)

#### Wire protocol corruption: concurrent writes on shared connection
`SingleMongoConnection.sendQuery()` was not synchronized. When the PooledDriver gave the same connection to multiple threads, their bytes interleaved on the wire, producing corrupted messages (`Illegal opcode 0` with `responseTo=0x6B6C0000` — bytes from `$clusterTime` mid-stream). Fixed by synchronizing `sendQuery`, `sendCommand`, and `sendAndWaitForReply`.

#### Network retry on closed connection reuses dead connection
When a `MorphiumDriverNetworkException` closed the connection (e.g. corrupt stream), the `NetworkCallHelper` retried on the same dead connection — guaranteed to fail again. `MongoCommand.executeAsync()` and `WriteMongoCommand.execute()` now check `isConnected()` before each retry and get a fresh connection from the pool if needed.

#### MongoCommand.getLog() StackOverflow
`MongoCommand` had a `log` field initialized via `getLog()` which recursively called itself. Fixed to use `LoggerFactory.getLogger()` directly.

#### Count command Long/Integer cast
`processCountDirect` in InMemoryDriver returned `long` but `CountMongoCommand.getCount()` cast to `Integer`, causing a `ClassCastException`. Now returns as `int`.

### Changed

- `WriteSafety` downgrade message (standalone MongoDB) reduced from WARN to DEBUG
- Index creation message (`CREATE_ON_STARTUP`) reduced from WARN to INFO; `WARN_ON_STARTUP` remains WARN as intended
- `MultiCollectionMessaging` fallback poll interval reduced from 5000ms to 1000ms for faster message delivery when change streams are unavailable
- `SingleMongoConnectDriver` reconnect sleep reduced from 1000ms to 200ms for faster failover detection

### Performance

#### ClassGraphCache: 4.7x faster Morphium startup
Introduced a JVM-wide singleton cache for ClassGraph classpath scan results. Previously, each `new Morphium()` triggered 2–4 full classpath scans (~100–500ms each), which dominated test setup time and slowed down applications that create multiple Morphium instances. The scan now happens once per JVM; all subsequent instances reuse cached results. In tests, `BasicFunctionalityTest` dropped from 67s to 14s.

- Zero-copy BSON decoder, reduced BsonEncoder allocations per document
- Shallow copy instead of deep copy for change stream events
- Direct dispatch for hot-path commands (insert, update, delete, find, count, distinct)
- PoppyDB: fixed thread pool instead of virtual threads (prevented OOM under load)
- PoppyDB: orphaned cursor cleanup on client disconnect
- PoppyDB: 3x faster than MongoDB for individual operations (insert 0.74ms vs 4.48ms, find 0.45ms vs 1.95ms, update 0.66ms vs 5.19ms in local benchmarks)

## [6.2.0]

### Breaking Changes

#### PoppyDB: Server extracted into separate module (renamed from MorphiumServer)
The server component has been extracted into its own Maven module and renamed to **PoppyDB**.

**Why?** The server pulled in dependencies (Netty, etc.) that 90% of Morphium users don't need — most projects just use the core library to talk to MongoDB. By extracting PoppyDB into a separate module, `de.caluga:morphium` stays lean. Beyond testing, PoppyDB is a fully functional MongoDB-compatible server — particularly useful as a **messaging backend**, providing a lightweight messaging solution without requiring a full MongoDB deployment. Add it as a test dependency or use it standalone:

```xml
<dependency>
    <groupId>de.caluga</groupId>
    <artifactId>poppydb</artifactId>
    <version>6.2.0</version>
    <scope>test</scope>
</dependency>
```

This also makes standalone deployment and testing of PoppyDB much simpler.

**What changed:**
- **Module**: `de.caluga:poppydb` (was part of `de.caluga:morphium`)
- **Package**: `de.caluga.poppydb` (was `de.caluga.morphium.server`)
- **CLI JAR**: `poppydb-<version>-cli.jar` (was `morphium-<version>-server-cli.jar`)
- **Main classes**: `PoppyDB` / `PoppyDBCLI` (were `MorphiumServer` / `MorphiumServerCLI`)
- Netty handlers → `de.caluga.poppydb.netty`, election → `de.caluga.poppydb.election`
- Morphium core library (`de.caluga:morphium`) is **unaffected**
- Wire protocol backward compatible: server sends both `poppyDB: true` and `morphiumServer: true` in hello response

#### Multi-module Maven structure
The project is now a multi-module build:
- `morphium-parent` — parent POM (`de.caluga:morphium-parent`)
- `morphium-core` — the core library, artifactId stays `de.caluga:morphium`
- `poppydb` — the server (`de.caluga:poppydb`)

Dependency coordinates for the core library are unchanged: `de.caluga:morphium:6.2.0`

#### `MongoField.not()` return type changed from `Query<T>` to `MongoField<T>`
The `not()` method now returns `MongoField<T>` instead of `Query<T>`, enabling fluent chaining:

```java
// now compiles and works correctly
query.f("field").not().eq("val");
```

**Migration:** Any code that captured the return value of `not()` as a `Query<T>` must be updated to `MongoField<T>`. In practice `not()` was always intended to be chained with an operator (`.eq()`, `.gt()`, etc.), so no valid use of the old return type exists.

#### MorphiumDriverException is now unchecked (extends RuntimeException)
Aligns with MongoDB Java driver (`MongoException`), JPA, jOOQ, and Spring Data conventions.

**Migration:**
- `catch (MorphiumDriverException e)` blocks continue to work — no changes needed
- `catch (RuntimeException | MorphiumDriverException e)` must be simplified to `catch (RuntimeException e)`
- Code inspecting `getCause()` for wrapped exceptions must catch `MorphiumDriverException` directly

#### Entity instantiation: `ReflectionFactory` → `Unsafe.allocateInstance()`
Replaced `sun.reflect.ReflectionFactory` (progressively hidden since JDK 17) with `sun.misc.Unsafe.allocateInstance()` for creating entity instances without no-arg constructors. This matches what Spring, Jackson, Gson, and Hibernate use. Best practice: add a no-arg constructor to `@Entity` classes.

### Added

#### `@Reference` cascade features and cycle detection
- **`cascadeDelete = true`** — Referenced entities are automatically deleted when the parent is deleted. Supports single references, collections, and maps.
- **`orphanRemoval = true`** — References removed from a collection after update are automatically deleted.
- **Cycle detection** — Circular `@Reference` chains (A→B→A) are detected during serialization and deserialization. Objects with IDs return `{_id: ...}`; objects without IDs throw `IllegalStateException`.
- New `CascadeHelper` utility with `ThreadLocal`-based cycle detection.
- Documentation: `docs/howtos/references-and-relationships.md`

#### `@AutoSequence` annotation — zero-boilerplate sequence assignment
```java
@Entity
public class ImportRecord {
    @Id private MorphiumId id;
    @AutoSequence(name = "import_number", startValue = 1000, inc = 1)
    private Long importNumber;
}
```
- Supported field types: `long`, `Long`, `int`, `Integer`, `String`
- Explicit values are never overwritten — only `null` (or `0` for primitives) triggers assignment
- **Batch optimization:** `storeList()` allocates all sequence numbers in a single round-trip via `SequenceGenerator.getNextBatch()`

#### Automatic CosmosDB backend detection
- `BackendType` enum (`MONGODB`, `COSMOSDB`, `POPPY_DB`, `UNKNOWN`) in the driver layer
- Auto-detected from `hello` handshake response (CosmosDB: `msg` field, PoppyDB: `poppyDB` field)
- `morphium.isCosmosDB()` / `driver.isPoppyDB()` for application-level checks
- Supports Azure sovereign cloud domains

#### `@CreationTime` / `@LastChange` enhancements
- **`LocalDateTime` support** as a fourth field type (alongside `long`, `Date`, `String`)
- **Field-only usage** — class-level `@CreationTime` annotation is no longer required; the field annotation alone is sufficient
- **Preset values preserved** — explicitly set `@CreationTime` values are no longer overwritten on insert

#### `resetThreadLocalOverrides()`
New method to clean up all per-thread boolean overrides (`disableAutoValuesForThread()`, `disableReadCacheForThread()`, etc.) in a single call. Prevents state leaking between requests in thread-pool and virtual-thread environments.

#### `@Version` annotation — Optimistic Locking
Full optimistic locking via `@Version` on `Long` fields. On insert, version is initialized to `1`; on update, a version-match filter is added and the version incremented atomically. `VersionMismatchException` on concurrent modification.

#### Other additions
- **MONGODB-X509** client-certificate authentication
- **`mongodb+srv://`** connection string support for MongoDB Atlas (pure-Java DNS, no JNDI)
- **Configurable `LocalDateTimeMapper`** storage format (Date vs. ISO-8601 string)
- **`SequenceGenerator.getNextBatch(int)`** for bulk sequence allocation in a single round-trip

### Changed

#### Lazy-loading proxies: spring-cglib → ByteBuddy
Replaced `org.springframework:spring-core` (cglib) with `net.bytebuddy:byte-buddy` for lazy-loading proxy generation. ByteBuddy is actively maintained, has native Java 21 support, and requires no `--add-opens` JVM flags. Proxy classes are cached per entity type via `ConcurrentHashMap` to avoid Metaspace leaks. The new `MorphiumProxyMarker` interface replaces the fragile `$$EnhancerByCGLIB$$` string check for proxy detection.

#### DNS SRV resolver logging
`DnsSrvResolver` now logs SRV resolution at INFO (start/result), DEBUG (per-server queries), WARN (failures), and TRACE (raw hex dump) for diagnosing Atlas connectivity in containers.

### Fixed

#### PoppyDB: wrong BSON limits caused write failures
`maxBsonObjectSize` was reported as 10KB (should be 16MB) and `maxMessageSizeBytes` as 100KB (should be 48MB) in PoppyDB's hello response. The MongoDB driver uses these values to validate documents — the tiny limits caused BSON assertion errors and silent write failures under normal load.

#### PoppyDB: idle timeout killed change stream connections
Default idle connection timeout was 60 seconds. Change stream connections are idle by design between `getMore` polls — the short timeout killed them mid-wait, causing "Broken pipe" cascades. Increased to 300 seconds.

#### PoppyDB: stale primary status after elections
The `primary` boolean was a snapshot from connection init and became stale after replica set elections. Write-concern handling now uses the dynamic `isCurrentPrimary()` check via `ElectionManager`.

#### PoppyDB: aggressive connection close on parse errors
The wire protocol decoder closed the entire connection on unknown opcodes or payload parse errors. Now skips the malformed message (bytes are consumed so the stream stays in sync) and only closes on irrecoverable stream corruption or I/O errors. Prevents cascade failures where one bad message kills the connection.

#### Wire protocol: EOF handling and stream corruption
`WireProtocolMessage.parseFromStream()` could enter an infinite loop when `InputStream.read()` returned -1 (EOF) during header or body reads. Now returns null gracefully. Added message size validation and diagnostic logging (size, messageId, responseTo) on illegal opcodes.

#### Thread visibility: volatile running flags
`SingleMongoConnection`, `SingleCollectionMessaging`, `BufferedMorphiumWriterImpl`, `WatchingCacheSynchronizer`, and `CacheHousekeeper` used non-volatile `running` flags read by worker threads in while-loops. Without volatile, the JIT could cache the value and the worker thread would never see the stop signal. (`MultiCollectionMessaging` already used `AtomicBoolean`, `ChangeStreamMonitor` and `PooledDriver` already used volatile.)

#### `@CreationTime` not set on primitive `long` fields
`f.get(o)` on a primitive `long` field returns `Long(0)` (not null), so the "don't overwrite manually set" check always skipped setting the creation time. Now treats zero as "not set" for numeric types.

#### `MongoField.not()` produced wrong BSON structure
`not()` wrapped `$not` around the value instead of the operator, producing `{$regex: {$not: val}}` instead of the correct `{$not: {$regex: val}}`. Fixed operator grouping and `addSimple()` for `not().eq()`.

#### `QueryHelper.matchesQuery` short-circuit on multi-field queries
The for-loop over query keys returned on the first field match without checking remaining fields, breaking AND semantics. Also fixed the same short-circuit in the Map/array-index pre-loop.

#### Auto-detect single-node replica sets
When no RS name is configured but the server's hello response contains a `setName`, the driver now auto-upgrades to RS mode. Covers Docker/Testcontainers setups where the server runs as a single-node replica set.

#### Index and capped collection checks
- `setAutoIndexAndCappedCreationOnWrite()` now also sets `CappedCheck` (previously only `IndexCheck`)
- Missing indices no longer reported for collections that don't exist yet
- WriteConcern on standalone MongoDB: queries `driver.isReplicaSet()` instead of config flag; gracefully downgrades w>1 to w:1

#### Enum serialization/deserialization round-trip in untyped containers
Enums stored in untyped containers (`Object`, `List<Object>`, `Map<String, Object>`) were serialized as `{class_name, name}` maps, but the deserialization path never routed back through enum handling — causing `ClassCastException` on read. New `deserializeEnumValue()` method handles both String and Map formats (backwards-compatible with existing data). Also fixed: enums in typed `Map<String, SomeEnum>` or `List<SomeEnum>` were not converted back to their enum type.

#### Custom TypeMappers ignored in queries
Custom `MorphiumTypeMapper` implementations were not consulted when resolving field values in `MongoFieldImpl`. Queries now call `ObjectMapperImpl.marshallIfNecessary()` during value resolution.

#### WriteBuffer WAIT strategy lock starvation
The entire `switch(strategy)` block was wrapped in `synchronized(opLog)` — the WAIT strategy slept while holding the lock, preventing the flush thread from ever draining the buffer. Also fixed: missing `break` after WAIT (fall-through to JUST_WARN caused double-add), off-by-one in buffer limit check (`> size` vs `>= size`), TOCTOU race in WAIT branch, and `WRITE_OLD`/`DEL_OLD` creating plain `ArrayList` instead of `Collections.synchronizedList()`.

#### Transaction isolation with write buffer
`commitTransaction()` called `flush()`, which drained the shared write buffer from ALL threads into the committing thread's transaction — breaking cross-thread isolation. Fix: `startTransaction()` now saves and disables the per-thread write buffer, `commitTransaction()`/`abortTransaction()` restore the previous state in `finally`. Also fixed: `PooledDriver.markTransactionCommitted()` was in the `finally` block, updating the read-routing timestamp even after a failed commit.

#### Transient transaction error 251 (`NoSuchTransaction`) handling
After MongoDB aborts a transaction, the TCP connection's server-side session retains the poisoned state. Subsequent operations on the same pooled connection receive error 251, which was thrown as non-retriable `MorphiumDriverException`. Fix: detect error 251, close the poisoned connection, throw `MorphiumDriverNetworkException` (retriable), and retry with a fresh connection. Also fixed: `WireProtocolMessage.parseFromStream()` and `SingleMongoConnection.sendQuery()`/`readNextMessage()` were wrapping `MorphiumDriverNetworkException` in `RuntimeException`/`MorphiumDriverException`, destroying the type information that `NetworkCallHelper` needs for retry decisions.

#### RS auto-detect race condition
Concurrent heartbeat threads could race on `setReplicaSet()`/`setReplicaSetName()` when auto-detecting a replica set from hello responses. Wrapped in double-checked locking with `synchronized(primaryNodeLock)`.

#### Concurrent double-write in `BufferedMorphiumWriterImpl.flush()`
`flush()` used `opLog.get()` which returned a live reference. Concurrent calls would write the same entries, causing `E11000 duplicate key` errors. Fixed via `opLog.remove()` for atomic ownership transfer.

#### Quarkus / OSGi ClassLoader compatibility
All `Class.forName()` call sites now use a centralized helper preferring the thread context ClassLoader. Fixes `ClassNotFoundException` in Quarkus dev mode, OSGi, and JBoss.

#### Other fixes
- `@Version` hardening: initialized to `1L` on insert, `$and` filter in InMemoryDriver
- BufferedWriter: immediate execution for non-buffered entities (buffer size = 0)
- BufferedWriter: `setIdIfNull` support for `UUID` and `ObjectId` ID types
- Sequence `@WriteSafety`: changed to `BASIC` for standalone MongoDB compatibility
- `BsonEncoder` `java.time` type support
- InMemoryDriver: no-op handler for X509 auth command
- Multi-collection messaging bootstrapping speedup

### Code Quality
- Resolved all source and test compilation warnings
- Replaced deprecated `MorphiumConfig` API calls with new sub-object API
- `CascadeHelper` uses `@CascadeAware` marker annotation instead of `ConcurrentHashMap` caches

### Tests
- Increased timeouts for flaky messaging, changestream, and `LastAccessTest` tests
- Comprehensive failover tests for PoppyDB replica sets
- InMemory backend detection tests

### Dependencies
| Dependency | Previous | Updated |
|---|---|---|
| io.netty:netty-all | 4.1.100.Final | 4.2.9.Final |
| org.mongodb:bson | 4.7.1 | 4.11.5 |
| org.slf4j:slf4j-api | 2.0.0 | 2.0.17 |
| ch.qos.logback:logback-core | 1.5.24 | 1.5.25 |
| org.assertj:assertj-core | 3.23.1 | 3.27.7 |
| org.springframework:spring-core | 5.3.39 | **removed** |
| net.bytebuddy:byte-buddy | — | 1.15.11 |

## [6.1.8]

### Tests
- splitting long running tests for better maintainability 
- tuning some timeouts in tests in order to be more resiliant to load related slowdowns

### Fixed
#### Connection Pool counter drift
• PooledDriver: fixes counter drift / incorrect borrowed counter decrement under topology changes (prevents apparent pool exhaustion).
• ChangeStreamMonitor: fixes connection release fallback when watch exists but has no connection (prevents lingering borrowed counter of +1).

#### Heartbeat connection leak on error
• When `getHelloResult()` or `connect()` threw an exception during heartbeat, the connection container was polled from the pool but never returned or closed — invisible leak since it was not tracked in `borrowedConnections` either. Now properly closed in `finally`.

#### ReadPreference fall-through clarification
• Explicit fall-through comments for `NEAREST` → `PRIMARY_PREFERRED` → `SECONDARY` cascade in `getReadConnection()`. No behavioral change — documents the intentional degradation path.


#### Connection Pool Exhaustion due to Hostname Case Mismatch
- **Pool exhaustion when MongoDB reports hostnames with different casing**: When MongoDB's `hello` response reported hostnames with different casing than the seed list (e.g., `SERV-MSG1.example.com` vs `serv-msg1.example.com`), connections were being closed instead of returned to the pool. The borrowed connections counter was not decremented, causing the pool to fill up to `maxConnections` with all connections appearing "borrowed" but none available.
- **Root cause**: The `hosts` map was keyed by the hostname as reported by MongoDB, but `releaseConnection()` looked up by the hostname stored in the connection object (from the seed list). Case mismatch caused lookup failures.
- **Fix**: All hostname operations now normalize to lowercase:
  - `normalizeHostKey()` converts to lowercase and ensures port suffix
  - `SingleMongoConnection.getConnectedTo()/getConnectedToHost()` return lowercase
  - `addToHostSeed()/setHostSeed()` normalize on write
  - `getWaitCounterForHost()`, `getTotalConnectionsToHost()`, `onConnectionError()` normalize inputs
  - `ConnectionWaiter` thread normalizes before all host lookups

#### ChangeStreamHistoryLost
- forget resume token as it is invalid
- restart changestream
- might cause loss of a message or two, but is stable


#### Messaging Lock TTL Bug
- **Lock expires immediately when message has no timeout**: When a message had `timingOut=false`, the TTL was 0, causing the lock to be created with `deleteAt = now`. MongoDB's TTL monitor would delete the lock almost immediately, allowing duplicate message processing. Now uses 7 days as fallback TTL for messages without timeout.

#### ChangeStreamMonitor Stability
- **ChangeStreamMonitor dies on "connection closed"**: Previously, a "connection closed" exception would cause the ChangeStreamMonitor to stop permanently with no auto-recovery. This is often a transient error (network issues, MongoDB failover). Now the monitor will retry the connection instead of giving up.
- **Improved exit logging**: ChangeStreamMonitor now logs at WARN level when it stops, explaining the reason (config null, connection closed, no such host, etc.). Previously most exit conditions were logged at DEBUG level, making it hard to diagnose why messaging stopped working.
- **Resume token support for ChangeStreamMonitor**: ChangeStreamMonitor now tracks the resume token from each event and uses it when restarting the watch after connection issues. This prevents duplicate events and ensures no events are missed during reconnection. Also handles `ChangeStreamHistoryLost` errors gracefully by discarding the stale token and starting fresh.

## [6.1.0] 

### Added

#### PoppyDB Enhancements
- **Replica set support**: PoppyDB now supports replica set configuration with automatic primary election and failover
- **Server CLI**: New standalone `poppydb-cli.jar` for running PoppyDB from command line with `--help` option
- **Replication**: Data replication between PoppyDB instances in a replica set
- **Custom election protocol**: Implemented Raft-inspired election system for PoppyDB replica sets with:
  - Configurable election priorities per node
  - Heartbeat-based leader detection
  - Automatic leader election on primary failure
  - Vote request/response protocol for consensus
- **Netty-based wire protocol handler**: New `MongoCommandHandler` using Netty for improved performance and connection handling
- **Messaging optimization**: PoppyDB-specific optimizations for messaging workloads

#### Messaging
- **Topic Registry / Network Registry**: New `NetworkRegistry` implementation for discovering messaging topics across the network
- **MessagingSettings**: New configuration class for messaging-related settings

#### InMemoryDriver
- **Tailable cursor support**: InMemoryDriver now supports tailable queries
- **Shared InMemory databases**: Multiple Morphium instances can share the same InMemory database (configurable via `DriverSettings.setShareInMemoryDatabase()`)
- **MongoDB-compatible `$text` query support**: Full text search with MongoDB-standard query syntax
  - Root-level queries: `{ $text: { $search: "search terms" } }`
  - Phrase search: `{ $text: { $search: "\"exact phrase\"" } }`
  - Term negation: `{ $text: { $search: "include -exclude" } }`
  - Case sensitivity: `{ $text: { $search: "...", $caseSensitive: true } }`
  - Automatically searches fields defined in text indexes

#### Driver
- **Host class**: New `Host` class for improved readability in connection pool management
- **Shared connection pools**: Connection pool sharing between Morphium instances

#### PoppyDB
- **SSL/TLS support**: PoppyDB can now accept SSL/TLS encrypted connections
  - `server.setSslEnabled(true)` to enable SSL
  - `server.setSslContext(sslContext)` for custom SSL configuration
  - Automatic TLS 1.2/1.3 protocol selection
- **Periodic snapshots/persistence**: PoppyDB can now dump databases to disk and restore on startup
  - `--dump-dir <path>` CLI option to enable persistence
  - `--dump-interval <seconds>` for periodic dumps during runtime
  - Automatic restore from dump files on startup
  - Final dump on graceful shutdown
  - Programmatic API: `setDumpDirectory()`, `setDumpIntervalMs()`, `dumpNow()`, `restoreFromDump()`

### Fixed
- **MultiCollectionMessaging DM polling when change streams disabled**: When `setUseChangeStream(false)` is called on `MultiCollectionMessaging`, direct messages (DMs) are now also polled instead of using change streams. Previously, DMs were always using change streams regardless of the setting, causing inconsistent behavior. Added new `pollAndProcessAllDms()` method and updated the poll trigger handler to support "dm_all" triggers
- **Graceful thread pool shutdown in Morphium**: Changed `asyncOperationsThreadPool.shutdownNow()` to graceful shutdown to prevent abrupt task termination
- **PooledDriver NPE and race conditions**: Fixed null pointer exception for `primaryNode`, race condition with `primaryNodeLock`, and connection cleanup improvements
- **MorphiumWriterImpl graceful shutdown**: Added graceful shutdown in `close()` and `onShutdown()` methods
- **InMemoryDriver change stream race condition**: Fixed race condition in change stream handling (line 633-646)
- **Flaky IteratorTest.concurrentAccessTest**: Fixed race condition where multiple threads sharing a single iterator would call `hasNext()` and `next()` non-atomically, causing incorrect element counts (e.g., 29130 instead of 25000). The test now properly synchronizes the hasNext+next critical section
- **Parallel test database isolation**: Fixed race condition in MultiDriverTestBase where database cleanup would drop ALL databases matching the prefix pattern, including databases from other parallel tests that were still running. Now each test only drops its own database, preventing "expected X but was 0" failures in parallel execution
- **PoppyDB listDatabases**: Added explicit handler for `listDatabases` command in PoppyDB. Previously this command returned null when forwarded through GenericCommand, causing NullPointerException in tests that call `morphium.listDatabases()`
- **PoppyDB stepDown for standalone servers**: Standalone PoppyDB instances (no replica set configured) now immediately become primary again after receiving a `replSetStepDown` command. Previously, stepDown would leave the server in secondary state with no way to recover, causing "no primary" errors for subsequent operations
- **InMemoryDriver database-level change streams via PoppyDB**: Fixed change stream event delivery for database-level watches registered through PoppyDB. When a client creates a database-level watch via the wire protocol, MongoDB convention sets collection to "1". The InMemoryDriver now correctly delivers events to subscribers registered under the `db.1` namespace key
- **Message sending to self**: Fixed broken message sending when sender equals recipient
- **Deadlocks**: Fixed multiple deadlock scenarios in messaging and server components
- **Robust shutdown**: Improved shutdown handling across components
- **NPE in QueryHelper.matchesQuery**: Fixed null pointer exception when comparing MorphiumId/ObjectId fields against null query values
- **Flaky test fixes**: Replaced timing-dependent `Thread.sleep()` + assertion patterns with `TestUtils.waitForConditionToBecomeTrue()` polling in messaging and changestream tests
- **Pooled driver updates**: Updates now apply proper `writeConcern` consistently and single-document updates honor sort
- **Buffered writer bulk inserts**: Fixed a race where mutating a list after `storeList/insert(list)` could flush as "0 operations" and/or cause duplicate inserts
- **Change stream lifecycle**: `ChangeStreamMonitor` no longer misses early events as easily and terminates reliably (stops blocking watches on shutdown)
- **PoppyDB dropDatabase handling**: Added "dropdatabase" to WRITE_COMMANDS set so database drops are properly forwarded to primary instead of being rejected by secondaries
- **Test database cleanup**: Fixed `MultiDriverTestBase` to clean databases for ALL morphium instances (both PooledDriver and InMemoryDriver), not just the first one. Previously only one storage backend was cleaned, causing test isolation failures
- **GenericCommand key ordering**: Changed `cmdData` from `HashMap` to `LinkedHashMap` in `GenericCommand.fromMap()` to preserve key ordering, which is critical for MongoDB wire protocol where the command name must be the first key
- **Test configuration default hosts**: Changed `TestConfig` to default to single host (localhost:27017) instead of 3-host replica set for simpler test setup. Multi-node replica sets can still be configured via `morphium.hostSeed` property
- **PoppyDB getMore for regular query cursors**: Fixed `getMore` command to forward regular query cursors to InMemoryDriver instead of only handling change stream cursors. Previously, iterators would hang infinitely when fetching additional batches because non-change-stream cursors were returning empty batches with non-zero cursor IDs
- **PoppyDB replica set replication**: Extended change stream replication to handle `drop`, `dropDatabase`, `replace`, and `rename` operations. Previously only `insert`, `update`, and `delete` were replicated, causing collection drops and document replacements to not sync to secondaries
- **PoppyDB collection metadata forwarding**: Added forwarding of `listCollections` command to primary when running as secondary. This ensures `isCapped()` checks return correct results for capped collections created on primary
- **InMemoryDriver listCollections capped status**: Fixed `listCollections` response to include `capped`, `size`, and `max` options for capped collections. Previously the options field was always empty, causing `isCapped()` to return false even for capped collections
- **PoppyDB capped collection replication**: Added initial and periodic sync of capped collection metadata from primary to secondaries. Capped collections created on primary are now properly registered on secondaries, ensuring capped behavior is enforced during replication
- **InMemory backend detection for tests**: Added `isInMemoryBackend()` method to MorphiumDriver interface and `inMemoryBackend` field to hello response from PoppyDB. Tests that need to skip unsupported features (like Collation) can now correctly detect when connected to PoppyDB with InMemory backend, not just when using InMemoryDriver directly
- **PoppyDB changestream event delivery via wire protocol**: Fixed changestream events not being delivered to clients connecting via the wire protocol. Watch cursors are now properly created with callbacks, events are queued via `LinkedBlockingQueue`, and `getMore` requests correctly return queued events to clients. This enables reliable messaging when using PoppyDB as a messaging hub
- **PoppyDB killCursors command handler**: Added missing `killCursors` command handler to PoppyDB. Without this, watch cursors were never cleaned up when clients disconnected, causing virtual threads to accumulate and eventually block new watch thread creation. The fix properly removes cursors from `watchCursors` and `tailableCursors` maps
- **InMemoryDriver watch thread cleanup**: Modified `watchInternal()` to periodically check `callback.isContinued()` after each wait timeout (max 5 seconds). This ensures watch threads properly terminate when cursors are killed, preventing resource exhaustion when many clients connect/disconnect
- **PooledDriver connection leak**: Fixed connection leak in `releaseConnection()` where connections were removed from `inUse` set but not returned to the pool when the connection's host was no longer in the valid hosts set. Connections are now properly closed instead of being leaked
- **InMemoryDriver serverMode premature shutdown**: Fixed InMemoryDriver to not clear data or shut down when `serverMode=true` and `close()` is called. PoppyDB instances now properly maintain their data when client Morphium instances disconnect
- **SingleMongoConnection watch loop termination**: Fixed watch loop to check `isContinued()` after each individual event instead of only after processing the entire batch. This ensures watches terminate immediately when the callback returns false, matching InMemoryDriver behavior
- **ChangeStreamMonitor reconnection loop on shutdown**: Fixed ChangeStreamMonitor to stop gracefully when receiving "No such host" errors instead of endlessly retrying. Also added driver connectivity check before attempting to get connections. This prevents resource exhaustion when PoppyDB instances are shut down
- **PooledDriver parallel connection creation**: Changed connection creation from sequential to parallel (up to 10 virtual threads) to handle burst scenarios where many connections are needed simultaneously. This prevents connection timeouts when many async operations are queued at once
- **PoppyDB write concern handling with partial replica sets**: Fixed write concern handling when configuring a replica set programmatically before all secondaries are started. Previously, writes with `w > 1` would block for the full `wtimeout` (10 seconds) waiting for non-existent secondaries, causing client-side timeouts. The `ReplicationCoordinator` now fails fast (100ms grace period) when no secondaries have registered, returning a proper `writeConcernError` response instead of timing out. This enables tests to store documents on a primary before starting secondary nodes
- **Replication staleness detection**: Added staleness detection mechanism to ReplicationManager that detects when a secondary's change stream watch connection has gone stale (no response for 30+ seconds). When detected, the connection is forcibly closed and a new one is established. This prevents secondaries from falling behind when connections silently break
- **SingleMongoConnection socket timeout limit**: Modified `readNextMessage()` to limit consecutive socket timeout retries to 100 (approximately 10 seconds with 100ms timeout). After reaching this limit, it returns null to allow the calling code to check `isContinued()` and handle connection issues. Previously, the method would retry indefinitely, causing watch loops to never detect broken connections
- **Connection pool issues**: Fixed multiple connection pool problems including proper connection release, leak prevention, and handling of stale connections
- **Messaging stability**: Fixed various messaging issues including connection handling, message processing, and proper cleanup on shutdown
- **Server status on startup**: Fixed PoppyDB status reporting during initial startup phase
- **NPE fixes**: Fixed null pointer exceptions in various components during edge cases
- **Election priorities**: Fixed election priority handling to ensure highest-priority node becomes primary
- **Read preference on secondary**: Fixed read preference checks when operating on secondary nodes
- **Flaky CollationTest timing**: Added wait conditions for collation queries to handle replica set replication delay. Previously, tests would fail intermittently because collation queries were executed before data was fully replicated
- **Flaky ExclusiveMessageBasicTests timing**: Increased timing tolerance from 30s to 35s to account for timing variance in message processing
- **Flaky LastAccessTest assertions**: Added better error messages for debugging timing-related assertion failures
- **CacheTests write buffer timeout**: Increased write buffer flush timeout from 3s to 10s to handle PoppyDB latency

### Added (Tests)
- **Failover tests for PoppyDB replica sets**: Added comprehensive failover tests (`FailoverTest.java`) that verify:
  - Primary election based on configured priorities
  - Automatic failover when primary is terminated
  - Write operations succeed after failover
  - Rejoining nodes integrate correctly into the cluster
  - Tests cover both `PooledDriver` and `SingleMongoConnectDriver`

### Changed (Test Infrastructure)
- **Unified multi-driver test base**: Migrated 72 test classes from `MorphiumTestBase` to `MultiDriverTestBase`
  - Converted 356+ test methods from `@Test` to `@ParameterizedTest` with `@MethodSource`
  - Each test now declares driver compatibility via `@MethodSource`:
    - `getMorphiumInstancesNoSingle()` - pooled + inmem (default for most tests)
    - `getMorphiumInstances()` - all drivers including single connection
    - `getMorphiumInstancesPooledOnly()` - pooled driver only
    - `getMorphiumInstancesInMemOnly()` - inmem driver only
  - Tests receive `Morphium morphium` as parameter instead of using inherited field

- **Driver selection via runtests.sh**: Tests can now run against different backends:
  ```bash
  # InMemory only (fast, default without --external)
  ./runtests.sh --driver inmem

  # All drivers against external MongoDB
  ./runtests.sh --uri mongodb://host1,host2/db --driver all

  # Against PoppyDB (run separately from MongoDB tests)
  ./runtests.sh --poppydb --driver pooled
  ```

- **Multi-backend testing workflow**: To test against all backends:
  1. `./runtests.sh --driver inmem` - InMemory driver (fast, no dependencies)
  2. `./runtests.sh --uri mongodb://... --driver all` - Real MongoDB with all drivers
  3. `./runtests.sh --poppydb --driver pooled` - PoppyDB

- **External test tagging**: Added `@Tag("external")` to driver tests that require a real MongoDB connection (PooledDriverTest, PooledDriverConnectionsTests, SharedConnectionPoolTest). Fixed pom.xml to use correct `<excludedGroups>` parameter instead of invalid `<excludeTags>` for Maven Surefire plugin JUnit 5 tag filtering

- **Test script improvements**: Major refactoring of `runtests.sh` for:
  - Modular script architecture with separate utility scripts in `scripts/` directory
  - Better temporary file management and cleanup
  - Improved parallel test execution and slot management
  - Enhanced failure reporting and log management
  - Support for different test backends via `--driver`, `--uri`, and `--poppydb` options
  - Memory settings optimization for test execution

### Changed
- **Modernized concurrent collections**: Replaced legacy `Vector` with `CopyOnWriteArrayList` and `Hashtable` with `ConcurrentHashMap` for better performance
- **Optimized string operations**: Consolidated multiple `replaceAll()` calls into single regex patterns, replaced `replaceAll()` with `replace()` for literal string replacements
- **ChangeStream implementation**: Improved change stream handling and event delivery reliability

### Dependencies
- **logback-core**: Bumped from 1.5.13 to 1.5.19

### Performance

#### InMemoryDriver Optimizations
- **Removed global synchronization on `sendCommand()`**: Operations on different collections can now execute in parallel. Previously all commands were serialized through a single synchronized method, causing unnecessary contention.

- **Optimized `find()` deep copy behavior**: Documents are now only copied after query matching succeeds, and projection-aware copying avoids redundant work:
  - Non-matching documents: No copy (previously copied before match check)
  - Include projections: Only projected fields are copied (previously full document copied twice)
  - Exclude projections: Single copy (previously double copy)

- **Improved index lookups for equality queries**: Simple equality queries (e.g., `{field: value}`) now use fast `Objects.equals()` instead of full `matchesQuery()` evaluation. Operator queries (`$gt`, `$lt`, etc.) skip the index path entirely to avoid ineffective bucket scanning.

- **Rewrote TTL expiration checking**:
  - Collections without TTL indexes have zero overhead (previously all collections scanned every 10 seconds)
  - TTL index info is cached when indexes are created
  - No snapshot copy during expiration check - iterates directly on CopyOnWriteArrayList
  - Auto-cleanup of tracking when collections are dropped

- **`$in` operator optimization**: Changed from O(n*m) to O(n+m) using HashSet lookups

- **Aggregator reuse**: Aggregators are now reused to reduce object allocation

- **Subdocument projection support**: Improved projection handling for nested documents

- **Stats performance**: Improved performance for driver statistics collection

#### PoppyDB Optimizations
- **Buffered I/O**: Added 64KB buffered streams for socket read/write operations
- **ZLIB decompression buffer**: Increased from 100 bytes to 8KB with pre-sized output buffer
- **Reduced redundant serialization**: Avoid calling `bytes()` multiple times in logging paths

---

## [6.0.3] - 2025-11-28

### Fixed
- **NPE in MultiCollectionMessaging**: Fixed null pointer exception in `getLockCollectionName()` when building lock collection names

---

## [6.0.2] - 2025-10-16

### Fixed
- **NPE in Query.set() methods**: Changed from `Map.of()` to `Doc.of()` to allow null values in set operations
- **NPE in Msg.preStore()**: Initialize `processedBy` list if null before validation

### Changed
- **Default queue name handling**: Setting queue name to "msg" now resets to default (null) for backward compatibility
- **Build configuration**: Added `runOrder=filesystem` to surefire plugin for consistent test execution

---

## [6.0.1] - TBD

> 📖 **Detailed release notes**: [docs/releases/CHANGELOG-6.0.1.md](docs/releases/CHANGELOG-6.0.1.md)
> 📝 **Quick summary**: [docs/releases/RELEASE-NOTES-6.0.1.md](docs/releases/RELEASE-NOTES-6.0.1.md)

### Breaking Changes
- **Null Handling Behavior Change**: Default behavior now matches standard ORM conventions
  - **Previous behavior**: Null values were NOT stored in the database by default (fields omitted)
  - **New behavior**: Null values ARE stored as explicit nulls in the database by default
  - Fields WITHOUT annotation: Accept and store null values (standard ORM behavior)
  - Fields WITH `@IgnoreNullFromDB`: Reject nulls, field omitted when null
  - **Migration impact**: Existing code that relies on null values being omitted by default may need to add `@IgnoreNullFromDB` to those fields

- **@UseIfNull Deprecated**: Replaced with `@IgnoreNullFromDB` for clearer semantics
  - Old annotation had inverted logic that was confusing
  - `@UseIfNull` is now deprecated but still functional
  - Migration: Replace `@UseIfNull` with `@IgnoreNullFromDB` and remove the annotation (behavior is inverted)

### Added
- **New `@IgnoreNullFromDB` annotation**: Protects fields from null contamination
  - Prevents null values from being stored during serialization (field omitted)
  - Rejects null values during deserialization (preserves default value)
  - Distinguishes between "field missing from DB" vs "field present with null value"
  - Special handling for `@Id` fields: NEVER stored when null (MongoDB auto-generates)
  - Comprehensive documentation with behavior matrix and use cases
- Comprehensive test suites for null handling behavior
- Enhanced documentation for null handling with detailed examples

### Changed
- **Default null handling now matches standard ORMs**:
  - Serialization: Null values stored as explicit null in database
  - Deserialization: Null values from database accepted and set to null
  - This aligns with Hibernate, JPA, and other standard ORMs
- **@Id field handling**: Fields annotated with `@Id` are NEVER stored when null
  - Ensures MongoDB can auto-generate unique `_id` values
  - Prevents E11000 duplicate key errors from null `_id` values
- `runtests.sh`: Added local PoppyDB cluster convenience mode (`--poppydb-local`) with optional auto-start (`--start-poppydb-local`)
  - Auto-start logs now go to `.poppydb-local/logs/`
  - Auto-start is idempotent and keeps a locally started cluster running by default

### Fixed
- Socket timeout handling in `SingleMongoConnection` - automatic retry on timeout exceptions
- Better timeout detection in watch operations
- Multi-collection messaging error handling and lock release
- Connection management in message rejection handler
- PoppyDB: fix replica set startup to avoid ending up with no primary
- PoppyDB: support `aggregate` command over the wire (enables aggregation stage tests against PoppyDB)
- **Bulk operations now return proper operation counts**: `runBulk()` now returns statistics including `num_inserted`, `num_matched`, `num_modified`, `num_deleted`, `num_upserts`, and `upsertedIds`

### Performance
- Added collection name caching to reduce reflection overhead

### Known Issues

#### Messaging with PoppyDB Replicaset
- **ExclusiveMessageTests#exclusivityTest**: This test is flaky when running with multiple Morphium instances connecting to a PoppyDB replicaset. The test sometimes passes and sometimes times out due to slower message processing compared to real MongoDB. Change stream events ARE being delivered correctly, but processing throughput with PoppyDB is lower than with real MongoDB, causing occasional timeouts with the default test timeout.
  - Workaround: Increase test timeout or use InMemoryDriver directly for messaging tests, or use a real MongoDB replicaset
  - Status: Performance issue, not a correctness issue

#### Test Suite Notes
- **ShardingTests**: These tests require a sharded MongoDB cluster and will fail on standalone or replica set deployments
- **SharedConnectionPoolTest**: Infrastructure test that requires specific connection pool setup
- **TopicRegistryTest**: Network registry discovery tests may fail due to timing issues in some environments

#### Test Results Summary (v6.1.0)
| Backend | Tests Run | Passed | Errors | Skipped |
|---------|-----------|--------|--------|---------|
| InMemory Driver | 1046 | 929 | 0 | 105 |
| MongoDB (Replicaset) | 1046 | 933 | 0 | 105 |
| PoppyDB (Replicaset) | 1024 | 1024 | 0 | 92 |

## [6.0.0] - 2024-XX-XX

### Major Release
- Java 21+ requirement
- Significant architectural improvements
- Enhanced driver support
- **SSL/TLS support**: Added SSL/TLS support for secure connections to MongoDB
  - `driver.setUseSSL(true)` to enable SSL connections
  - `driver.setSslContext(sslContext)` for custom SSL configuration
  - `driver.setSslInvalidHostNameAllowed(true)` to disable hostname verification
  - New `SslHelper` utility class for creating SSLContext from keystores
- Improved documentation

---

For detailed release notes, see individual release documentation in [docs/releases/](docs/releases/).
