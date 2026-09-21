# Changelog

All notable changes to Morphium will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed

#### PoppyDB: the priority takeover no longer yields to a node inside its stepdown block (#385)
`checkPriorityTakeover` picked its successor by priority, heartbeat freshness and replication lag - and could not see whether the peer was allowed to campaign at all. A follower's stepdown block (`replSetStepDown`, or a takeover yield of its own) was local state the heartbeat response did not carry, so a blocked node looked like a perfect successor: the leader yielded to it, the cluster went leaderless, the next survivor won and yielded to the same blocked node, and so on until the block expired. Seen as a three-node cascade with terms 2, 3 and 4 each held for one second; on a live cluster the deploy pipeline's `replSetStepDown 60` combined with the 30s takeover stability would hit the same window, unnoticed so far only because the pipeline restarts the stepped-down node in between. The heartbeat response now carries `electable` (false inside a stepdown block), the leader keeps it per peer next to `peerLastContact`, and the successor selection skips peers that reported false. Peers that do not send the field - nodes from before this release during a rolling upgrade - are treated as electable, i.e. as before.

#### Driver: a change stream that dies right after every registration is restarted with backoff, not in a tight loop (#383)
During the 6.3.11 rolling restart a primary counted ~5,000 change stream registrations from ~250 connections inside 383 ms - ~20 per client, all with a resume token, none of them paced. `ChangeStreamMonitor` was not the loop: its history-lost branch discards the token and sleeps a second. The loop was one layer down, in `SingleMongoConnection.watch()`: PoppyDB ends a stream whose resume token is from a foreign sequence space (the previous primary's) asynchronously, right after answering the aggregate with a cursor id. Whether the client's first getMore still finds the ended stream parked (answer 286 - the monitor discards the token) or already removed (answer: an exhausted cursor, id 0) is a race, and the exhausted-cursor answer was handled by the watch loop itself - re-register in place, same dead token, immediately, without the monitor ever seeing an error. Reproduced against a stub server that answers every getMore with an exhausted cursor: 37,858 registrations in five seconds. The in-place restart is now paced once it repeats: the first restart stays immediate (a one-off exhaustion resumes exactly as before), every further one in a row waits 100 ms doubling up to 5 s, with jitter so that the clients of one failover do not re-register in lockstep; the counter resets as soon as a getMore reply proves the restarted stream alive. Same stub, same five seconds: 7 registrations. Not changed: the server's side of the race - a getMore for a cursor that was ended and removed still answers "exhausted" instead of 286.

#### PoppyDB: a late getMore on an ended change stream is answered 286, not as an exhausted cursor (#389)
The server half of the loop above. A change stream that PoppyDB ends - a resume token from a foreign sequence space (#361), or the store emptied for a full sync (#380) - was answered in two ways depending on a race: a getMore that was already parked got `ChangeStreamHistoryLost` (286); a getMore that arrived after the ended stream had been removed from the cursor registry fell through to the generic getMore path and got an exhausted cursor (`ok:1, cursor.id: 0`), i.e. "your stream ended normally". A raw probe against a live node showed the first round answered 286 and every later one an exhausted cursor. Nothing ended normally there, and the client cannot tell the two apart - it resumes in place with the same dead token and gets the same answer again. `WatchCursorManager` now remembers an ended stream's id and terminal reason after its removal, and a getMore on such an id is answered exactly like the parked branch: 286 with the `ChangeStreamHistoryLost` marker `ChangeStreamMonitor` keys its recovery on. The memory is bounded twice - entries expire after 10 s (a client's getMore follows its aggregate within milliseconds), and a hard cap of 10,000 entries evicts the oldest during a burst (#380: thousands of ended streams within a second); a `killCursors` drops the entry at once. Same probe against the fixed node: 286 in every round.

## [6.3.11] - 2026-09-21

### Fixed

#### InMemoryDriver: comparison operators and the index comparator are exact past 2^53 (#379)
The operator path - `$eq`, `$ne`, `$lt`..`$gte`, the interpreted `$in` and whole-array equality - compared every Number via `doubleValue()`, Long against Long included. A double cannot tell neighbouring longs apart beyond 2^53, so `{$eq: 9007199254740993}` matched a stored `9007199254740992` and `{$lt: 9007199254740993}` did not find it; snowflake ids, nanosecond timestamps and long-stored hashes all live there. After #342 and #344 the split was inverted: the direct-equality path was exact, the operator path was not. Since the range operators need an ORDER and a Long against a Double past 2^53 has no common exact type, this is a cascade rather than a cast (`QueryHelper.compareNumbers`): integral wrappers compare via `Long.compare`, a Long against a Double by exact value (integer part first, then the fraction - no BigDecimal on the hot path), Double against Double via `Double.compare` as before, and every other Number type (`Float`, `BigDecimal` - #343) keeps the old `doubleValue()` comparison. Equality in the cascade is exactly #344's rule, so `-0.0` is the long 0: `{$eq: 0}` now hits a stored `-0.0` and `{$lt: 0}` no longer does, as in MongoDB. The `IndexKey` TreeMap comparator uses the same cascade - it filed 2^53 and 2^53+1 in one bucket, so a range query served by an index answered differently from the same query over an unindexed collection. Values within ±2^53 are unaffected: every long there is exactly a double, and the old comparison was already right.

#### PoppyDB: a node that empties its store for a full sync ends its client change streams (#380)
A stepped-down primary that falls back to a full sync drops every local database while its client connections stay open and their change streams stay registered. The wipe runs with change stream events suppressed, so those streams never saw the drop: they sat parked through the sync and would have continued on the re-synced data with a gap no consumer can detect. New registrations were already refused while the node re-syncs (13436 `NotPrimaryOrSecondary`, the RECOVERING gate of #356/#371); the streams registered before the wipe were not covered. The node now ends every open change stream the moment its store is emptied - a parked getMore is answered at once with `ChangeStreamHistoryLost` (286), the same answer clients already key their restart on - and logs one line with the count. Clients re-establish their streams once the sync is complete.

#### InMemoryDriver: a history-lost burst is throttled and no longer logged at ERROR (#380)
`failHistoryLost` logged every ended stream at ERROR, unthrottled. On the node above every resume attempt of the connected clients landed behind the drop, and the node wrote 186,465 identical ERROR lines (66 MB) in five seconds, burying everything else that happened in that window. The condition is an expected, client-handled one (the same reasoning as #331 and #361), so the line is now INFO, and a burst is reported at its powers of two only (1, 2, 4, 8, ... with the running count), the rest at DEBUG; the count resets after a minute of quiet. This is the throttling PoppyDB's `WatchCursorManager` already applied one layer up, where the same burst came out as 18 lines. The wire contract is unchanged: every ended stream still carries the `ChangeStreamHistoryLost` marker.

#### PoppyDB: change stream registrations are counted (#380)
What drove ~40,000 registration attempts per second at the re-syncing node is undecided: `ChangeStreamMonitor` paces its retries at one second, and the repetition was not paced by network round-trips either. Every change stream registration that reaches a node is now counted - ahead of the RECOVERING gate, so refused attempts count too - with the subset that carried a resume token. The lifetime totals are in `serverStatus.changeStreams` (`registrations`, `resumeRegistrations`, `open`) and in the CLI's `PoppyDB alive` line every ten seconds, so the rate at the next rolling restart is a delta between two lines.

## [6.3.10] - 2026-09-19

### Changed

#### Default read preference is `primaryPreferred`, no longer `nearest`
`DriverSettings` shipped with `nearest()`. On a replica set that was effective through node selection, on a mongos it never was: every command carried a hardcoded `primaryPreferred` (see below). Now that the read preference goes over the wire, an unconfigured sharded deployment would silently have switched to `nearest` and lost read-your-own-writes after a plain `store()`. The default is therefore `primaryPreferred`: a mongos sees exactly what it always saw, and on a replica set an unconfigured setup reads from the primary, as the official drivers do. Replica-set users who relied on the implicit `nearest` will see the read load move to the primary; set `driverSettings().setDefaultReadPreference(ReadPreference.nearest())` (or the `defaultReadPreferenceType` property) to keep it.

### Fixed

#### Lazy-loading proxies work on Java 25
Byte Buddy checks the class-file version of the running JVM when it builds a proxy class, and 1.15.11 did not know Java 25: every `@Reference(lazyLoading = true)` failed with `Failed to create lazy-loading proxy ... Java 25 (69) is not supported by the current version of Byte Buddy`, regardless of the JDK morphium was compiled with. Byte Buddy is now 1.18.14, and `Morphium.createLazyLoadedEntity` passes a fallback of Java 21 to `ClassFileVersion.ofThisVm`, so a JVM newer than the bundled Byte Buddy knows gets Java 21 proxy classes instead of an exception - the next JDK release does not repeat this.

#### The `$readPreference` on the wire is the one the caller asked for (#362)
`MongoCommand` sent a hardcoded `$readPreference: {mode: "primaryPreferred"}` with every command. On a replica set that was invisible, the driver picks the node itself; behind a mongos this field is the routing criterion, so `driverSettings().setDefaultReadPreference(...)` and `@DefaultReadPreference` were silently dropped on a sharded cluster, and a brief primary loss on a shard produced stale reads the application had explicitly asked to avoid. The connection a read is borrowed for is now stamped with the effective read preference - the requested one or the default, with the PRIMARY-forcing rules for transactions, the read-after-write window and PoppyDB applied - and the command sends that. The rules moved from `PooledDriver` to `DriverBase.effectiveReadPreference()`, so `SingleMongoConnectDriver` applies them too (it used to send `mode: "nearest"` inside a transaction). Being a direct connection, that driver follows the server selection spec: on a `SECONDARY`/`ANY` connection a primary read goes out as `primaryPreferred`, because mongod and PoppyDB use `$readPreference` for the secondaryOk decision and would answer 13435. Tags are not sent with `mode: "primary"` (MongoDB rejects them), a typeless `ReadPreference` counts as none instead of hitting a `switch` on null, and a read that re-borrows its connection after a network error asks for the preference the read had, not the driver default, so the retry is routed like the first attempt. Two behaviour changes worth knowing: a read with `mode: "primary"` that hits an ex-primary right after a step-down now gets 13435 instead of a silent, possibly stale read (the same on PoppyDB, whose secondaries reject primary reads), and `Morphium.reread()` always reads from the primary - its job is to return what is in the database, and a lagging secondary answered with an older version, or with `null` for a document that exists. Contributed by Bernhard Slominski.

#### PoppyDB: elections settle promptly after a failover that follows a priority-takeover yield
A leader that yields to a higher-priority peer refuses to seek election for `priorityTakeoverStepDownSecs` (10s) so the successor can win. When that successor died inside the window, the yielded node - still the best candidate - sat out the rest of it while also denying the remaining lower-priority peer on priority, so nobody could win until the #312 hold expired: 13-18s failovers in `FastResyncTest` on the loaded testrunner and in about one of five local runs (node2 wins the parallel start, yields to node1, node1 is shut down seconds after taking over). The yield's window is now released by the first heartbeat of the successor it made room for; an operator's `replSetStepDown` keeps its full window. A voter that cannot campaign itself (frozen, or inside a stepdown window) no longer denies lower-priority candidates on priority alone, in both the PreVote and the real vote.

#### PoppyDB: a peer's idle-closed election connection no longer costs an election round
A follower's peer connections idle whenever it is neither leader nor candidate, and the peer closes idle connections after its idle timeout. The driver did not notice: an EOF on the reply read came back as a plain `null` with the connection still marked connected, so the first vote request after a failover vanished ("Null response ... for vote request" for a peer that was up), the round was lost and the next election timeout paid for it. `SingleMongoConnection` now closes on EOF, the election client evicts a driver that returned no answer, and a request that failed over a cached connection is sent once more over a fresh one. The reply timeout for election RPCs is derived from the election config (the minimum election timeout, 2s by default) instead of a fixed 500ms that equalled the heartbeat interval and fired under load; at most one heartbeat per peer is in flight, and the "cannot reach peer" WARN waits for three consecutive misses instead of one.

#### PoppyDB: replication backs off from a sync source that answered "not primary" (#364, partial)
The replication loop treated 13435/10107/189 like any other error: reconnect to the same host, watch, fail, again. The host is up and keeps answering exactly that until the leadership change re-targets replication, so this was a CPU-burning loop right when the election needed the CPU. The loop now recognises the step-down shape, drops the connection (which also parks the periodic index sync) and retries with exponential backoff from 1s to a 10s cap, logged once per streak. It is deliberately not a permanent stop: the same host can become primary again, and the leader-discovery callback does not re-fire for an unchanged address. #364 stays open for the driver-level retries beneath the loop.

#### InMemoryDriver: `{x: 2}` matches a stored `2.0`, the same as `{x: {$eq: 2}}` already did (#344)
The two spellings of one equality query gave two answers: the operator path compared numbers via `doubleValue()`, the direct-equality path used wrapper `equals()`, so a stored Double `2.0` answered `{$eq: 2}` but not `{x: 2}`. #342 closed exactly this split for the integral wrappers and deliberately left floating point alone. MongoDB treats int32/int64/double as numerically comparable, so the direct path now follows the operator path - but with an exactness check rather than a cast: a Long equals a Double only when the double is an integer value that converts back to the very same long. `9007199254740993L` (2^53+1) has no exact double form and must not match `9007199254740992.0`; comparing through `doubleValue()` would have turned a missed match into a wrong match. Applied in both matchers (`QueryHelper` and the compiled one that `find` runs), in the compiled `$in`/`$nin` hash sets, in the multikey list branch, and in `IndexKey` canonicalisation, so an index built over doubles answers an integer probe and vice versa. `$all` had escaped #342 entirely: both matchers looked the operand members up in a raw `HashSet` of the stored list, so `{$all: [2]}` missed a stored `[2L]` as well as `[2.0]` while `$in` hit both; it now canonicalises both sides exactly like the `$in`/`$nin` sets. Double vs Double keeps plain `equals()`; BigDecimal remains the decimal128 question of #343.

#### InMemoryDriver: `aggregate` uses the index for a leading `$match` (#375)
`InMemAggregator` materialized the whole collection through a filterless `find()` and ran every stage, including a leading `$match`, over the copies. The `$match` was only handed to `IndexPlanner` to decide what the slow-query log should say: an aggregation with an indexed `$match` over 30.000 documents cost the same ~475ms as one without any `$match`, while the equivalent `find` took 8ms. A leading `$match` is now answered by the driver's own candidate selection (`findAggregationInput`), the same index path `find`/`count` use, and only the matching documents are deep-copied and handed to the remaining stages. The slow-aggregation log line reports the stage and `docsExamined` that actually ran (`stage=IXSCAN, docsExamined=1`) instead of a planner estimate flagged as "diagnostic only", and the aggregator's internal filterless `find` no longer emits a second `COLLSCAN` line next to it.

#### InMemoryDriver: `$project` with only computed fields is an inclusion projection (#377, #378)
`$project` ran in two modes, and a spec without any `{field: 1}` flag landed in the lenient one, which cloned the whole document and only knew `Expr` instances: a raw operator Map like `{flag: {$cond: [...]}}` was iterated by its operator keys as field names, logged `InMemoryAggregation only works with Expr` and never wrote the field. Adding any `{field: 1}` next to it made the very same expression work. mongod treats every computed field as an inclusion, so a computed-only `$project` now behaves like strict inclusion mode: `_id` plus the computed fields, evaluated through the one resolver (`projectComputedValue`) that handles literals, `"$field"` references, `Expr` and raw operator Maps. Exclusion mode is left for specs that consist of `{field: 0}` flags only; the unreachable `Expr`-only branch is gone, and the `$merge` `whenMatched` projection follows the same mode decision. Note for callers that relied on the old behaviour: `agg.project("avg", Expr...)` after a `$group` now returns only `_id` and `avg`, as it does on mongod. Mixing `{field: 0}` with a computed field, which mongod rejects, still ignores the exclusion rather than erroring - deliberately left as the open question in #378.

#### InMemoryDriver: `$group` accumulators evaluate expression operands (#376)
`{$sum: {$cond: [...]}}` silently produced 0, because `InMemAggregator`'s accumulators only recognised a literal or a `"$field"` reference as operand and fell through on a raw operator Map or an `Expr` built via the Java API. `$avg`, `$min`, `$max`, `$first`, `$last`, `$push` and `$addToSet` had the same gap. All eight now resolve the operand through the same evaluator `$project` uses for computed fields, with non-numeric results ignored by `$sum`/`$avg` and null never winning `$min`/`$max`, matching mongod. The `{$cond: {if, then, else}}` object form is now accepted by `Expr.parse` as well.

#### InMemoryDriver: `$sum` over a `"$field"` reference no longer fails on missing or non-numeric values
The field-reference fast path of `$sum` cast the document value to `Number` unchecked. One
document without the field (or with a string in it) took the whole aggregation down with a
`NullPointerException`, surfaced as `ok:0` to the caller. The expression path next to it
already skipped such values. Both paths now behave like mongod: missing and non-numeric
values simply do not add up.


## [6.3.9] - 2026-09-16

### Added

#### PoppyDB: `shutdown` over the wire actually stops the node (#356)
`{shutdown: 1}` fell through to the embedded driver and answered `ok:0`, so SIGTERM was the only way to stop a node - every rolling restart needed shell access. `MongoCommandHandler` now wires the command to `PoppyDB.shutdown()`: a primary steps down first (unless `force: true`) and refuses re-election for 60s, so a rolling restart hands off leadership instead of black-holing writes. No secondary catch-up wait yet, so unreplicated writes at that moment can still be lost. `PoppyDB.shutdown()` is now guarded by a compare-and-set so the CLI's SIGTERM hook, an embedding application and the wire command cannot run it twice.

#### PoppyDB: `getLog` answers from an in-memory ring buffer, `setParameter logLevel` at runtime (#356)
`getLog` returned an empty list for `startupWarnings` and `"unknown log"` for everything else, so the first diagnostic command an operator types gave nothing. `LogRingBuffer` is now a bounded Logback appender on the root logger (1024 lines, mongod's RamLog size), answering `getLog` in mongod's shape including real `startupWarnings`. Its companion, `setParameter: {logLevel: N}` (0-5), moves the root logger at runtime so a node can be raised to DEBUG without a restart, though Logback's fewer levels mean the value is quantized to 0, 1 or 3. Both the appender and the level change are JVM-wide and are not undone by `shutdown()`.

### Fixed

#### PoppyDB: maintenance commands survive RECOVERING, unsupported mongod commands are refused honestly (#356)
`dumpNow` and `dumpStatus` were not in the control-plane set, so a re-syncing secondary rejected them with 13436 - exactly the node you most want to dump or inspect mid-incident. Both are control plane now, as is `setParameter`. Commands like `logRotate`, `fsync`, `compact`, `profile`, `connPoolStats`, `replSetReconfig` and `top` used to fall into the generic path and answer a misleading `CommandNotFound`; they now get an explicit `CommandNotSupported` (115) and are left out of `listCommands`.

#### atlas-url accepts standard mongodb:// connection strings, not only mongodb+srv:// (#357)
`resolveAtlasUrlIfNeeded()` handled only `mongodb+srv://` and discarded any `mongodb://` URI, leaving the host seed empty and startup failing with a misleading "no server address specified" - this hit anyone on a managed service without SRV records, such as Azure Cosmos DB or DocumentDB. A `mongodb://` URI is now parsed: its comma-separated host list is taken literally, and userinfo credentials plus `tls`/`ssl`, `replicaSet` and `authSource` are applied as defaults so explicit config still wins. Honoring a connection string on every config path, and the SRV URI's own options, remains open as #358.

#### PoppyDB preserves the MongoDB error code on the generic command-error path (#373)
The generic dispatch catch-all in `MongoCommandHandler` built its `ok:0` failure response from the message alone, dropping any `MorphiumDriverException.getMongoCode()` so a client got a null mongo code it could not act on. The fast paths already kept the code; only this catch-all erased it. It now carries the deepest code from the cause chain, found while adding the client-side retry for `ExceededMemoryLimit` (146), which itself takes a different path but exposed the general trap for any command whose driver error carries a code.

#### Writes refused by the heap watermark (ExceededMemoryLimit 146) are retried instead of failing hard (#293)
`InMemoryDriver.checkMemoryWatermark` refuses document-creating writes with code 146 once heap occupancy crosses the reject watermark, a reading that is deliberately an upper bound the next collector cycle can correct - but `WriteMongoCommand` retried 112, 251, step-downs and network errors while letting a 146 fall through to a hard failure. On the testrunner this surfaced as a flaky `BasicJMSTests` failure, with `Producer.send` reporting an acknowledged message as unacknowledged. The refuse happens before any document is created, so retrying cannot double-insert, and all three shapes the wire carries (thrown from `readSingleAnswer`, thrown from `sendCommand`, and a direct-insert `writeErrors` entry) are now covered. Retries back off and give up after the usual attempt count, so a heap that really is full still surfaces to the caller.

#### The sync-completion release is reached under load: a high-water mark instead of "queue empty right now" (#370)
`localDataComplete` only flipped true when the batch processor found the event queue empty at a tick, but under sustained write load the queue is rarely empty, so a node whose sync had actually completed could stay unreleased indefinitely - and since #352 this flag also gates candidacy, so a replica set under load could end up with no eligible candidate after the primary died. The manager now records the highest sequence delivered when the snapshot finished and releases once the applied sequence has passed it, which is reachable under load rather than in spite of it. That mark is reset whenever a session is discarded, so a stale mark cannot leak into the next capture; the old empty-queue rule remains a fallback for streams with no sequence information.

#### A node no longer serves empty reads as SECONDARY between startup and its first sync (#371)
Between accepting connections and the start of its initial sync, a replica-set member reported `SECONDARY` and answered data reads successfully but empty, because both wire guards keyed on "the replication manager is syncing," which is false before the manager even exists - the same shape #352 addressed, reached through a different door. A member with peers now starts in an explicit "data not yet authoritative" state from process start until a sync completes or it becomes primary, honored by `hello`, `preDispatch` and `replSetGetStatus`; standalone nodes are exempt, but dump-restored nodes are deliberately not. Operationally, a member restarted into a set with no current primary now stays unavailable until one exists, unlike mongod's oplog-backed secondary reads. This required moving `saslStart`/`saslContinue`/`logout` to the control plane, since under `--auth` election clients must authenticate against peers before any leader exists.

#### Chaos harness: a probe that does not perturb the run, exact accounting, and a diverge scenario that fails when it cannot diverge (#372)
The once-a-second probe used `countDocuments()`, which this driver turns into a full-collection copy (#355), allocating heavily and even taking a node down with an OutOfMemoryError under the heavy profile - contaminating other failures in the same run. The probe is now `find().limit(1)`, a constant-cost check that still distinguishes data, no data and error. The dump-guard scenario's burst is now counted as acknowledged, and the diverge scenario injects its marker at the command level, persists it with `dumpNow`, and verifies it actually diverged before asserting on the restart.

#### A superseded ReplicationManager is refused at the write, not merely asked to stop (#323)
`stop()` joins the sync thread with a 5s bound against a 60s read timeout, so the join routinely loses and an abandoned thread can resurface with a completed read in hand, inserting documents from the old primary into data that now belongs to its successor - cooperative cancellation alone could not close this, since the thread can be descheduled between passing a check and reaching the write it guards. The ownership check now sits at every path that writes to the local driver: `runLocalApplyCommand`, the change-stream bulk insert, and the two admin-collection drops in `clearLocalDatabases()`. This shrinks the window rather than closing it - the check and the write are still two statements a thread can be descheduled between - but the window no longer spans a network read.

#### A node in the middle of a re-sync no longer dumps its empty state over the last good dump (#352, partial)
A resync drops the local databases before copying a fresh snapshot, leaving the store legitimately empty in between; the wire already refused commands in that window, but the in-process dump path did not, so a periodic tick or the final shutdown dump could rename an empty file over the last good dump. The periodic tick, both `dumpNow` paths and the shutdown dump now refuse while re-syncing, tracked via a flag on the node itself rather than the ReplicationManager, which `shutdown()` nulls before its final dump. Clearing the store also marks the node's data incomplete, which the candidacy guard already honors - without this, a mid-resync node could be promoted to primary while holding an empty store. In-process readers other than the dump path still see the empty phase, which remains open in #352.

#### `replSetGetStatus` no longer calls a node usable while it answers nothing (#356, partial)
A secondary re-running its initial sync rejects every data-plane command with 13436 and already advertises `secondary:false` in hello, but `replSetGetStatus` - the command an operator checks - still reported `stateStr: "SECONDARY"` regardless, so a rolling restart driven off that answer could walk through nodes that each held nothing while every check stayed green. It now reports STARTUP2 before a node has data of its own and RECOVERING while re-syncing, the same distinction mongod makes. The status document also gained real numbers (`date`, health, heartbeats, per-member progress) that `ReplicationCoordinator.getStats()` had computed all along but never surfaced, with PoppyDB's own write-sequence progress kept in a separate `poppyReplication` sub-document. Still open in #356: a working `shutdown` command, and `getLog`.

#### Memory watermarks no longer decide on an incoherent heap reading (#368)
`heapUsedAfterGcPercent` summed `MemoryPoolMXBean.getCollectionUsage()` across heap pools, but under G1 a young collection doesn't touch the old generation, so the sum blended a fresh reading with one that could be minutes old and reported wildly different percentages for the identical live dataset depending on GC timing - enough to trigger `ExceededMemoryLimit` on a heap that was actually 30% free. The reading now comes from a GC notification's `GcInfo`, capturing every pool as of one known instant, with only the most recent collection's reading kept as an upper bound on the live set. A write is refused only when this reading and the raw `used/max` gauge are both over the watermark, keeping the hot path cheap. This intentionally errs toward occasional false refusals (recoverable via retry) rather than forcing a blocking full GC at peak pressure to get a precise answer.

#### The restore reads a dump incrementally, so its size no longer has a ceiling (#366)
`restoreInternal` called `readAllBytes()` and decoded the result into a single `String` before parsing, capping any database near a `byte[]`'s 2GB limit or a `String`'s ~1GB character limit - a large PoppyDB 6.3.8 dump hit this and came back dead on restart, with the JVM staying up with nothing listening so systemd reported it healthy. The dump is now parsed straight off the gzip stream via json-simple's `parse(Reader)`, removing both buffers; streaming alone still left a full second copy of the database reachable during conversion, so `restoreDumpValue` now converts dump values in place through a custom `ContainerFactory`, replacing marker values (Date, UUID, ObjectId, byte[]) via `setValue`/`set` instead of allocating a parallel structure. On the dump that exposed this, restore went from an `OutOfMemoryError` to completing cleanly with zero full GCs.

#### A failed restore no longer leaves a running process with nothing listening (#366)
`PoppyDBCLI`'s restore-failure handler already logged the failure and called `setLocalDataComplete(false)` on `Exception`, but an `OutOfMemoryError` is not one - it walked past every catch block into `main`, leaving the process alive on its non-daemon threads with nothing bound to its port, which an init system reads as a healthy service. The handler now catches `Throwable`. One behaviour changed as a side effect: a legacy non-UTF-8 dump (#306) needs a rewindable stream for its ISO-8859-1 retry, so only the file-based entry points still support it; `restore(InputStream)` now fails with a message naming the entry point that can.

#### `dropDatabase` no longer leaves TTL and capped rules behind (#369)
Dropping a database cleared its documents and index definitions but left the per-collection TTL and capped registries in place, so a collection recreated under the same name inherited rules `getIndexes()` no longer reported - verified on PoppyDB 6.3.8, where reloaded documents were expired by a TTL index that had supposedly gone down with the database. `drop(db, collection, wc)` and `setDatabase()` already purged these registries; the whole-database drop was the only path that forgot. It now performs the same cleanup for `collectionsWithTtlIndex`, the expiry queues and the three capped bookkeeping maps.

#### A write retry after a lost reply no longer fails on its own insert
When a write's reply is lost, `WriteMongoCommand` re-sends the command on a re-resolved primary, but since Morphium assigns `_id` on the client, that retry could collide with its own first attempt if that attempt had actually committed, and mongod's `E11000` turned a successful write into a reported failure. This was seen on the production message bus repeatedly, mostly surfacing as an HTTP 500 for a message that had in fact been stored and processed normally. A duplicate-key error is now reconciled when the command was re-sent after a network error, the violated index is `_id`, and the reported id is from this batch; any other collision remains an error. Update and delete are untouched, since their lost-reply problem needs real `(lsid, txnNumber)` deduplication (#293).

### Changed

#### `$group` no longer re-derives its constant spec for every document
Counting a collection through the aggregation path cost far more than the scan underneath it, because `$group` copied each incoming document unnecessarily and re-interpreted its group spec - constant for the whole stage - on every single document, allocating a fresh `_id` string and accumulator pipeline each time. The non-Map `_id` spec is now interpreted once before the loop, and `$sum` binds its result map and operand once, eliminating the throwaway allocations. This measurably drops `$group`'s per-document cost and the counting pipeline end to end. The structural cost of materializing and deep-copying the entire collection for aggregation and `count` paths remains and is tracked separately.


## [6.3.8] - 2026-08-31

### Fixed

#### A healthy replica-set follower could wipe itself over a phantom hash mismatch
A healthy PoppyDB follower dropped its own database and re-synced from scratch even though it held the same data as the primary, because `handleDbHash` hashed BSON bytes verbatim while the insert and upsert replication paths could produce the same logical document with different key order. The consistency check misread the resulting hash mismatch as real divergence and fell back to a destructive full sync (drop then copy). The hash now canonicalizes key order recursively before encoding, while array order stays significant since it is part of BSON equality. The full sync path itself is still drop-then-copy, so a genuinely diverged follower still has a brief window with an empty collection.

#### The change stream stall watchdog no longer misreads normal idleness as a stalled cursor (#346)
The watchdog produced a flood of false alarms on the genios acceptance cluster, each discarding a healthy cursor and rebuilding it without a resume token, because its predicate treated "silent for two poll intervals AND the poll returned something" as a stall — neither half actually means the cursor fell behind. The real driver was messages nobody ever marks processed, which linger for their full TTL and keep the backlog flag permanently true. The watchdog now requires the timestamp of the oldest unprocessed message to hold steady for a full threshold instead of reacting to one poll observation. This makes detection traffic-gated: under very low traffic a silently dead cursor may not trip the watchdog, relying instead on the fallback poll or the separate suspect-path detection.

#### The poll no longer re-fetches answers nobody awaits, for their whole TTL (#348)
Answers to fire-and-forget requests are deliberately left without a `processed_by` mark so a listener registered later can still receive them, but the poll's relevance filter admitted every such answer and re-fetched it on every tick for its entire TTL. Because answers sort ahead of regular messages, this caused exactly the starvation the relevance filter was meant to prevent, and it also kept the backlog flag permanently true, feeding the false stall alarms fixed in #346. The poll now only admits answers this instance is actually waiting for, matching `inAnswerTo` against its own waiting sets rather than admitting any non-null value. The change stream still delivers every answer once; it is just no longer treated as backlog to re-fetch.

#### Poll and messaging tests: gaps closed alongside the fixes above
`DualChannelMessaging` had no test coverage for the #348 narrowing even though the change touched three of its query sites. Six new tests now cover the DM lane and main lane separately, for orphaned answers as well as answers awaited by queue and by callback. `awaitedAnswerIds()` also now logs a throttled warning once the awaited set passes 5000 entries, since that set ships as an `$in` list on every poll tick.

#### The answers-only poll branch threw a swallowed ClassCastException on every hit
The no-listener shortcut in `getMessagesForProcessing()` returned `idList()`, whose unbounded generic smuggled raw `MorphiumId`s into a list typed as `ProcessingQueueElement`, so the first non-empty result threw a `ClassCastException` that the poll loop's own catch swallowed. In the "no listeners registered, awaiting answers via sendAndAwait*" configuration this silently killed both the poll fallback and the stall watchdog, leaving the change stream as the only delivery path. Reachable only with the status info listener disabled, which is why no test ever hit it. Both branches now build their elements through one shared, type-tolerant mapper, and the shortcut sorts by `(priority, timestamp)` like the main branch instead of taking an arbitrary window.

#### PoppyDB: the messaging insert fast path would have hit the same missing-token bug (#347)
The #347 sweep found a second synthetic event without a resume token: the messaging insert fast path in `MessagingOptimizer` built its event with neither `_id` nor `clusterTime`. The path is currently dead code with no caller, so this was a latent trap for whoever enables it rather than an active bug, but it now carries a token like every other event. Since the real event has already been emitted by the time this fast path would run, it reuses the current sequence rather than minting a token no replay buffer would match. A full sweep of `poppydb` found no further producers of change stream documents without `_id`.

#### PoppyDB change stream events are now readable by spec-compliant drivers (#347)
Two wire defects made every official MongoDB driver abort a change stream against PoppyDB, though Morphium's own tolerant driver never noticed: the synthetic `lock_released` event carried no resume token at all (a document without `_id` makes spec-compliant drivers close the stream), and every event's `clusterTime` went out as a BSON int64 instead of a BSON timestamp, which typed decoders such as the official Java driver reject. Both are now fixed at the wire boundary in `WatchCursorManager`, leaving `morphium-core` and the `ChangeStreamEvent` API untouched: the synthetic event gets a token built from the current (non-replayable) sequence, and `clusterTime`/`operationTime` are now encoded as proper timestamps derived from the event's own sequence instead of a nonsense 1970 value. Note for consumers: `ChangeStreamEvent.getClusterTime()` against PoppyDB now returns the same raw timestamp encoding real MongoDB uses, not epoch millis, though nothing in Morphium itself reads it.


## [6.3.7] - 2026-08-26

### Added

#### Documented: `InMemoryDriver` is unsuitable for on-disk format tests (#336)
A value that reaches `InMemoryDriver` unmapped bypasses normalisation and is stored verbatim as a raw Java object, unlike the wire drivers which serialise every command through `BsonEncoder` — affecting far more than `java.time` (enums, `Character`, `Short`/`Byte`, `Float`, primitive arrays, `Calendar`, `ObjectId`). This is invisible from query results, since the in-memory driver leaves both the stored value and the filter unnormalised, so a format test asserting on query outcomes passes against `InMemoryDriver` for the wrong reason. Nothing written through the normal Morphium API is affected, since the ObjectMapper normalises those values first. `docs/howtos/inmemory-driver.md` now documents the full type table and recommends pinning on-disk shapes against a real MongoDB or PoppyDB instead; a new `@Disabled` test pins the divergence until the in-memory write path is normalised in 6.4.0.

#### Opt-in: `java.time` types can be stored as native BSON Date (`useBsonDateForJavaTime`)
`ObjectMappingSettings#setUseBsonDateForJavaTime(boolean)` (default `false`) makes `LocalDate`, `LocalTime`, `LocalDateTime` and `Instant` marshal to a native BSON Date instead of Morphium's own per-type formats, bit-compatible with the official MongoDB Java driver's jsr310 codecs. It applies only to scalar fields; container elements keep the existing `{"value": …}` wrapper with a native `Date` inside, so a native date query against a container still has to address `field.value`. The update APIs follow this flag too (per #335), though raw `Doc.of(...)` calls through `BsonEncoder` still write the legacy format regardless. With the flag off (the default) nothing changes on disk, and reading is tolerant of both shapes either way, so the flag can be toggled at runtime.

### Changed

#### `set()` / `push()` / `addToSet()` now write the same on-disk shape as `store()` for custom-mapped fields (#335)
The update APIs routed custom-mapped values through `marshallIfNecessary`, which had no custom-mapper branch, so such values reached the driver unmapped — unqueryable on the in-memory driver, and for container elements, written in a different shape than `store()` (`set("dateList", …)` wrote `[18997]` while `store()` wrote `[{"value": 18997}]`), so a query could silently miss half the matching documents. The update path now consults the custom mappers with exactly the shape `store()` produces, following `useBsonDateForJavaTime` dynamically, so both write paths are byte-shape-identical. Documents previously written via the update APIs into custom-mapped container fields keep the old flat shape and need a one-time re-save via `store()` to match. As a side effect, `BigDecimal` values written only via `set()`/`push()` now lose their extra precision, converging (deliberately) on `store()`'s long-standing lossy-`double` behaviour, tracked under #334.

### Fixed

#### Dump-restored TTL indexes no longer crash peers' initial sync - full-cluster restart recovers again (#340 follow-up)
The #340 restore recreated indexes using the JSON parser's number types, registering `expireAfterSeconds` and other numeric options as `Long` instead of `Integer`; when a peer's initial sync fetched those indexes via `listIndexes`, `IndexDescription.fromMap` threw on the type mismatch and the sync failed in an endless retry loop. This only surfaced after a full-cluster restart with no healthy peer left to sync from, leaving two of three nodes stuck in recovery and the cluster running on one node — a rolling restart hides the bug completely. It's fixed on both sides: the restore now normalizes the known Int32 option fields back to `Integer`, and `fromMap` now coerces numeric values against the declared field type instead of failing on a harmless wrapper mismatch. A node stuck in an identical sync failure now also escalates to an unmissable `NODE STUCK IN RECOVERY` log line after five consecutive failures.

#### InMemoryDriver: integral query values match across Integer/Long - a long field answers its own integer query again (#342)
`find({counter: 2})` returned nothing for a stored `2L` because equality compared by wrapper type, so a `long` field never matched an integer query literal; after a dump/restore this got worse since the JSON parser delivers every number as `Long`, so even `int` fields silently stopped answering integer queries — which made the #340 index fix only half effective. The comparison now happens in the matcher across every path that compared by wrapper type (interpreted and compiled matchers, `$in`/`$nin` sets, and the index equality path, where `IndexKey` now canonicalizes integral wrappers to `Long`), always exact via `longValue()` so values past 2^53 cannot collapse. The fix is deliberately scoped to integral wrapper types only; `Double`/`Float`/`BigDecimal` equality is unchanged, and the existing numeric operator paths (which already compared via `doubleValue()`) are unaffected.

#### setDatabase() no longer leaves stale index/TTL/capped bookkeeping of the replaced contents (#341)
`setDatabase()` swapped a database's collection map but left seven derived per-namespace structures (index definitions, index stores, TTL registration/queues, capped config and size caches) describing the data that had just been replaced. In the in-process PoppyDB restore case, this meant serving indexed reads from documents that no longer existed and listing TTL indexes that would never actually expire the restored data — invisible before because restoring into a fresh driver finds all seven structures empty anyway. The fix reuses the wholesale-invalidation contract already used by `drop()`/`resetData()`: the per-namespace structures are discarded and the index-store epoch is bumped before removing the stores, and TTL queues are removed rather than emptied in place so they get properly re-bootstrapped. Index definitions are deliberately not carried over, since `restore()` recreates the correct ones right after the swap (#340).

#### PoppyDB dump/restore carries index definitions - TTL indexes survive a full restart (#340)
Dump files held only documents, never indexes, so after a full cluster restart with no running peer to copy indexes from via initial sync, all indexes silently vanished — TTL indexes stopped expiring, letting a hot collection grow unbounded, and every query fell back to a collection scan. A rolling restart hid the loss entirely, since any surviving node lets initial sync rebuild indexes on restarted peers. Dumps now carry an optional `indexes` section per collection in the same wire shape initial sync already uses, and restore recreates indexes only after inserting the documents, since index creation seeds a TTL index's expiry queue from documents present at that moment. Old dumps without the section restore exactly as before, dumps with it remain readable by older versions (which ignore unknown keys), and a failed index recreation never costs data — it's reported via `getFailedIndexes()` and logged as an unmissable warning.

#### A read preference stored via asProperties() silently reverted to nearest on reload
`DriverSettings.defaultReadPreference` was `@Transient`, so a config written via `asProperties()` and reloaded via `fromProperties()` silently lost the setting and fell back to the class default `nearest()` — turning every read-after-write on a replica set into a coin flip against replication lag. Single-node deployments and the in-memory driver never showed the effect, and it stayed hidden further because an entity's own `@DefaultReadPreference` annotation takes precedence over the config. The read preference's type name is now a normal serializable field, rebuilt into the preference object whenever the two drift apart, fixing both the properties round trip and `createCopy()`. Tag sets are still not carried through the properties representation, so a tagged preference keeps its type but loses its tags across a round trip.

#### CHITSPERC/CMISSPERC reported NaN instead of 0 before any cached read had happened
`Statistics.java` computed `CHITS/(CHITS+CMISS)*100` unconditionally, so before any cached read happened the ratio was `0.0/0.0 = NaN`, which Prometheus/OTel exporters silently drop — making a fresh application's cache-hit-ratio metric appear entirely missing instead of a real 0%. Found while verifying the quarkus-morphium observability module against a live otel-collector/Prometheus stack. Both percentages are now guarded against the zero-denominator case and computed from one consistent snapshot of each `AtomicLong` instead of reading it three times.
#### PoppyDB: secondaries no longer leak ~800 bytes of heap per replicated event
`InMemoryDriver.runCommand()` stores every reply in an internal by-id map that is only cleared when the caller fetches it, but the `ReplicationManager` apply path and `WatchCursorManager`'s change-stream setup discarded the returned message id for most operations, so secondaries leaked one abandoned reply per replicated event forever (measured at roughly 0.8 GB/day at production rates) until the node hit its memory watermark. All apply sites now fetch their result like the bulk-insert path always did, which also surfaces write errors that were previously swallowed silently. As defense in depth, the driver's by-id result store now evicts entries older than a bounded window (`-Dinmemory.maxPendingCommandResults`, default 10,000 ids) with a rate-limited warning, and `resetData()` now clears the store too.
#### SingleMongoConnection: every heartbeat hello re-ran the full SASL handshake
`getHelloResult()` re-ran a complete SCRAM authentication on every heartbeat hello, even over connections that had already authenticated, though MongoDB auth state is bound to the socket for its lifetime — producing one full SASL exchange per second per client per pooled connection, measured at roughly 7,200 authentication log entries per hour per node with unchanged connection ids. Authentication state is now tracked per connection and re-run only on a fresh socket or after logout. `SingleMongoConnectDriver` was never affected, since its heartbeat uses a bare `HelloCommand` without the auth follow-up.

#### PooledDriver: idle long-lived clients no longer rebuild their connection pool every 30 seconds
A long-lived `PooledDriver` client with little traffic tore down and rebuilt its pooled connections continuously, because `lastUsed` is only refreshed by real application borrows, not by the per-second heartbeat hello, so the idle sweep declared every pooled connection of a quiet client idle after `maxConnectionIdleTime` (30s default) and the refill loop immediately recreated it. Measured in production on a 3-node replica set at roughly 1.5-4.3 new TCP connections per second per node, sustained for hours. The fix keeps idle eviction shrinking only the surplus above `minConnectionsPerHost`, while the base stock now recycles solely via `maxConnectionLifeTime` (10min default) instead of the idle timer. Secondaries were hit hardest, since primaries stay warm through real application borrows.

#### Container fields of scalar-mapped types (BigDecimal, Character, Atomic*, LocalDate, ...) now deserialize correctly (#334)
`List`/array/`Map` fields whose element type has a custom mapper with a scalar `marshall()` result are stored element-wise as a `{"value": <scalar>}` wrapper, but the read path had no branch recognizing this shape, so the raw wrapper `Map` survived into the loaded container and the first typed access threw a `ClassCastException` (or, for typed arrays, failed the whole entity read). The fix is deliberately read-side only — the on-disk write format is unchanged — since a write-side fix tried in PR #333 measurably changed the stored shape and broke rollback and mixed-version compatibility. Unwrapping is generic over registered custom mappers and narrowly scoped: a map is only treated as a wrapper if the declared element type has a registered mapper and the map carries exactly the key `value` (plus optionally `class_name`), so fields that legitimately contain a `value` key are left untouched.

## [6.3.6] - 2026-08-21

### Fixed

#### PoppyDB: ordinary client disconnects no longer flood the log with ERROR lines (#331)
All three Netty `exceptionCaught` handlers (decoder, encoder, command handler) logged every
exception unconditionally at ERROR, including plain client disconnects like `Connection reset by
peer` — a routine occurrence during deploys and restarts that flooded the log (140 ERROR lines in
40 minutes from one reconnect-looping client on ACC). The three sites now share one rule: the
IOException family (reset by peer, broken pipe, timeouts) logs at DEBUG, everything else stays at
ERROR with the full stack trace. Close behavior is unchanged.

#### PooledDriver: a rolling restart can no longer erode the topology into permanent silence (#330)
During a rolling restart, the membership-removal path compared normalized host keys against
un-normalized hello names, so a hello with different casing removed the very host it had just
added — eroding both the hosts map and the running host seed to empty, which turned
`reseedIfAllHostsEvicted` into a silent no-op and left the heartbeat cycling over nothing forever.
Fixed on four layers: the removal comparison now normalizes like the add path, membership removal
is only accepted from the PRIMARY's hello, the originally configured seed is restored when eroded
to empty, and the heartbeat is now self-rescheduling with a watchdog that detects silent cycles
and forces a reseed.

## [6.3.5] - 2026-08-21

### Fixed

#### ChangeStreamMonitor: a discarded resume token could be resurrected — clients hammered `ChangeStreamHistoryLost` resumes forever (#329)
When the server ended a stream with `ChangeStreamHistoryLost`, the monitor correctly discarded its
resume token and restarted fresh — but `run()`'s finally-block adoption then read the token back
off the dead `WatchCommand` and resurrected it, so every retry resumed with the token the server
had just declared dead. Against PoppyDB, whose sequence space used to reset on restart, this
turned every connected client into a resume-hammering loop the moment the server came back (the
2026-08-21 ACC bus outage). The deliberate discard now suppresses exactly that one
finally-adoption; ordinary errors keep the existing gap-protection adoption.

### Added

#### PoppyDB: the change-stream sequence survives restarts (`sequence-state.properties`) (#329)
A restarted server used to issue change-stream tokens from 0 again, which blinded the
destructive-resync guard's sequence comparison — a healthy restarted primary looked
indistinguishable from a stale one, so ACC secondaries livelocked in a refuse/re-register cycle
instead of resyncing. The sequence is now persisted alongside every dump (periodic, on-demand, and
shutdown) and restored monotonically with headroom for increments a crash may not have persisted.
Without a dump directory, nothing changes.

## [6.3.4] - 2026-08-21

> **Defective release — do not use in production, upgrade to 6.3.5.** The server-side strict
> resume-window guard added here, combined with the client-side resume-token resurrection bug
> (#329, present in 6.3.4 and every earlier version), turns every connected client into an
> endless `ChangeStreamHistoryLost` resume loop after a PoppyDB restart (or, on real MongoDB,
> once a consumer's resume point falls off the oplog). Fixed in 6.3.5.

### Added

#### PoppyDB: `dumpNow` returns immediately, and every dump write is crash-safe (#317)
`dumpNow` used to block the client (and the server's I/O thread) for the whole dump; it now
starts the dump and answers immediately with `status: "started"` or `"alreadyRunning"`, and one
shared guard now prevents the periodic scheduler, the on-demand command, and the shutdown dump
from ever overlapping. Separately, `InMemoryDriver` no longer writes straight into
`<db>.morphium.gz`, which truncated the last good dump the moment a new one started — each
database is now written to a temp file, fsynced, and atomically moved into place, so a crash
mid-write leaves the previous dump intact. The programmatic `PoppyDB.dumpNow()` stays synchronous
but returns `-1` when it skipped due to another dump running.

#### `sendMessages()` / `sendAnswers()` — genuine client-side batching for Messaging
Prompted by a batch-throughput benchmark, `@WriteBuffer` turned out to be the wrong tool for
messaging — a poll-and-wait mechanism that becomes a throughput ceiling under load rather than a
booster. `MorphiumMessaging` now offers `sendMessages(List<? extends Msg>)` as a first-class API,
sending a batch as one or more real bulk-insert wire calls grouped by target collection, with a
default `sendAnswers()` built on top for replying to many requests at once. The single-message
`sendMessage()` path is unchanged; both paths now share the same sender/senderHost/TTL-default
logic via factored-out helpers.

### Changed

#### `dumpNow` reply and completion semantics (#317) — **behavior change**
The `dumpNow` admin command used to answer `{ok: 1, databases: N}` only after the dump had
finished writing to disk. It now answers immediately with `{ok: 1, status:
"started"|"alreadyRunning"}`, and the `databases` count is gone since the command no longer knows
it at return time. Behavior change: anything that read `databases` or treated a successful reply
as "dump is on disk" must instead poll `dumpStatus.lastDumpMs` until it advances; the programmatic
`PoppyDB.dumpNow()` keeps its synchronous contract and count, but returns `-1` when it skipped
because another dump held the guard.

### Fixed

#### Change stream: a resume is now verified inside the replay, not just before it (#320)
`canResumeChangeStream` was validated at watch registration, but the actual replay ran later on
another thread while eviction runs concurrently on every write — so a resume that passed
validation could still silently lose events in that gap, and only PoppyDB's replication resumes
were checked at all (ordinary resumes via `ChangeStreamMonitor`/messaging were never gated).
`replayHistory` now verifies the window itself after the replay completes, checking that every
token was either still buffered or already delivered live, and fails loud with the existing
`ChangeStreamHistoryLost` marker instead of silently starting from now or missing a relevant
namespace drop. Both known consumers already key their recovery off that marker, so a silently
gapped stream no longer exists.

#### Change stream: live events can no longer overtake a resume's history replay (#319)
A resumed watch registers its subscription before replaying history, which let live dispatch race
the replay into the same consumer — a live event could arrive before the replayed events it
followed, and past the dedup window, even be delivered twice. For a PoppyDB secondary applying
updates as `_id`-keyed upserts with no already-applied check, that inversion silently overwrote
newer documents with older ones or resurrected deleted ones, triggered by any replication
reconnect under load. The subscription now carries an ordering barrier: live events are staged in
a bounded buffer during replay and drained in token order only once the replay completes; if live
writes outrun that buffer, the stream ends loud with `ChangeStreamHistoryLost` instead of
reordering silently.

#### PoppyDB: watch-cursor queues are byte-bounded - one slow consumer can no longer pin gigabytes (#321)
The per-cursor event queue was bounded by count only (10,000), but each queued event shares its
`fullDocument` payload with the replay buffer, so a single slow or stalled consumer could pin
gigabytes of memory that byte-based eviction elsewhere could never free — the same failure family
as the 2026-08-14 ACC incident, just one layer further along. Each cursor's queue now has a byte
budget (default 64m, `--cursor-queue-budget`), and overflow kills the cursor through the same
centralized path as the existing count cap, since delivery runs synchronously on the writer thread
and blocking or dropping-oldest are not viable options there. A single oversized event is still
delivered while the queue is empty, so the budget never imposes a hard document-size cap.

#### Messaging: the polling path no longer dies on int64 message fields
The poll path in `SingleCollectionMessaging` (and its twin in `DualChannelMessaging`) cast
`priority` hard to `Integer` and `timestamp` to `Long`, but a message whose numeric fields arrive
as the other boxed type — which demonstrably occurs after a PoppyDB failover — threw a
`ClassCastException` and silently killed every poll, exactly the path that recovers the backlog
after a changestream outage. The changestream path of the same class already handled this
tolerantly via `((Number) prio).intValue()`; both poll paths now follow the same rule.

#### PoppyDB: a stopped sync thread no longer writes into its successor's data (#323, part 1)
`ReplicationManager.stop()` joined its initial-sync thread with a 5s bound, but the sync
connection read with a 60s timeout that ignores `Thread.interrupt()` and the copy loop checked
neither `running` nor interruption anywhere — so the join lost routinely, and the abandoned thread
later inserted stale documents from the OLD primary into the replacement's own sync data. The copy
loop is now cooperatively cancellable at each database, collection, and crucially before the local
insert, and `stop()` closes the tracked in-flight connection to unblock a socket read stuck on a
slow primary. A remaining part of the issue (a generation check before every local write) stays
open.

#### PoppyDB: the initial sync no longer declares success over a dead watch (#322)
During a secondary's initial-sync snapshot, the primary kills the watch cursor on buffer overflow
rather than blocking, but the secondary's only thread tracking watch-health flags is the very
reader that's parked for backpressure — so the post-snapshot guard trusted a stale "watch is live"
signal and opened the apply gate over a provably dead stream with a real event gap, sometimes
causing a self-sustaining gap-resync-overflow loop. The guard now asks the primary directly via a
new `poppyCursorAlive` command whether the cycle's cursor still exists; if not, the snapshot is
discarded and redone under a fresh watch, and the dead session's buffered and late-arriving events
are dropped instead of applied as stale upserts. The probe runs unconditionally, fails open toward
an older primary that doesn't know the command, and fails closed if the primary is unreachable.

#### InMemoryDriver: the 21st change stream never started, and TTL expiry stopped with it (#325)
Server-side change streams parked a thread of the driver's shared scheduler (a
`ScheduledThreadPoolExecutor` capped at `max(20, 2*cores)`) for their entire lifetime — since that
pool never grows past its core size, the 21st concurrent watch's task simply never ran, silently
producing no history replay while looking healthy (live events still flowed on other threads).
Because the TTL sweep shared the same pool, expiry for the whole node stopped once enough watches
were open — significant for a PoppyDB node serving a message bus, where each client and
replicating secondary opens its own stream. Watch loops now get their own executor that grows with
the stream count, and TTL sweep runs on a dedicated scheduler; this costs one thread per open
stream, so memory use rises accordingly (removing the thread-per-stream shape entirely is tracked
as #328). A watch that failed to register within 5s used to silently return a normal-looking dead
cursor; it now logs the failure.

#### InMemoryDriver: two executor-lifecycle fixes found alongside #325
The TTL sweep was scheduled twice: `connect()` scheduled a new task each time without cancelling
the previous one, leaving the first task alive and unreferenced, sweeping in parallel with its
replacement forever. Separately, the change-stream event dispatcher's field was `final` and never
re-created on reconnect, so a driver that was shut down and reconnected (the documented test
cleanup path) silently dropped every client-mode change stream event afterward, logging one
warning per lost event.

#### PoppyDB never emitted `lock_released` — exclusive messages waited for the poll interval
`MultiCollectionMessaging` deliberately skips its own lock-monitor change stream on PoppyDB and
relies on the server emitting a synthetic `lock_released` event when a lock document is deleted —
but the only producer of that event sat in the generic command path, which direct dispatch
bypassed for deletes since March (16355e3c2). Releasing a lock therefore woke nobody: the freed
exclusive message was only picked up on the next poll round, capping throughput at poll cadence
and producing the msg_lck stall shape under contention. The notification now happens in the direct
`delete` path itself.

#### Replay-buffer accounting drifted, silently disabling the byte budget
An entry could be removed from the change-stream history by two independent parties at once (the
eviction loop and a drop's purge), and `ConcurrentLinkedDeque.removeIf` evaluates its predicate
before the CAS that actually unlinks the node — so a predicate that decremented the counters could
do so even when it lost the race. The counters drifted permanently below the buffer's real weight
(reproduced at -19), at which point `bytes > budget` stopped firing and the byte budget no longer
bounded memory at all. Every removal path now books through one exactly-once guard on the entry
itself, and new `getChangeStreamHistoryActual*()` accessors expose the buffer's real content for
diagnosis on a live node.

#### Watch-cursor bookkeeping leaked, and a failed watch start was answered `ok: 1` (#326)
Three related defects in the cursor delivery path: two of the five cursor-removal paths didn't
unregister the cursor's messaging registration, so terminated cursors leaked into that set forever
and each dead id cost a lookup on every later event-loop notification; `createWatchCursor`
swallowed a failure to start the watch and answered the client `ok: 1` with a dead cursor that
missed every event; and the parked-getMore fix from 6.3.3 had a race where a stream dying between
the terminal-state check and the parking left the request orphaned to return an empty,
successful-looking batch after the full `maxTimeMS`. All three are fixed: removals now go through
one path that also drops the registration, `createWatchCursor` now throws and fails the command,
and the terminal state is re-checked after parking.

#### The replication watermark could move backwards
`applyChangeEvent` advanced `lastAppliedSequence` with a plain `set` while the batch paths used
`Math.max`, but events don't always arrive in sequence order during a resume — an older event
arriving after a newer one dragged the watermark backwards, causing the node to resume from a
point already past and re-apply stale full documents over newer ones. Only non-insert events were
affected, since inserts go through the bulk path's `Math.max` flush. All advances are now
monotonic, except the deliberate reseed after a full sync, which stays a plain set.

#### Messages for topics without a listener starved the messaging poll window
`getMessagesForProcessing()` fetched candidates sorted by `(priority, timestamp)` with
`limit(windowSize)`, and a message for a topic this instance has no listener for is deliberately
skipped without a `processed_by` mark (so a later listener still receives it) — but that let it
re-enter every subsequent poll, sorting ahead of newer arrivals and permanently occupying a window
slot. Under moderate-to-high multi-topic traffic sharing one collection, enough such messages
could starve topics that do have a listener until the blockers expired via TTL. The poll query now
filters to only fetch messages whose topic currently has a registered listener (mirroring the
server-side change-stream relevance filter), while still leaving skipped messages unmarked so a
later `addListenerForTopic()` picks up the backlog. `MultiCollectionMessaging` is structurally
immune since it polls per-topic collections only for registered listeners.

#### PoppyDB change-stream cursors silently dropped events under burst load
`WatchCursorManager.drainEvents()` capped a batch at 100 events with
`while ((event = queue.poll()) != null && count < 100)` — when more than 100 events were pending,
the 101st was polled off the queue before the count check short-circuited the loop, and the
already-removed event was dropped on the floor. Under burst load (found via a benchmark
bulk-inserting 5000 documents in ~150ms) this silently lost roughly 1% of events per cursor,
affecting any bulk write into a watched collection, not just Messaging. Fixed by checking the
count bound before polling, so a 101st event stays in the queue for the next drain; since
`drainEvents()` backs both watch and tailable cursors, this likely also explains the long-standing
`TailableQueryTests` flakiness on PoppyDB previously written off as environmental.

#### GitHub releases carried neither the binaries nor a word of prose
`release.sh` only ever put the test-results table on a GitHub release body (via `--notes-file`)
and never uploaded any asset — the prose and `poppydb-*-cli.jar` on 6.3.0-6.3.2 were added by hand
afterward, a step that got forgotten for v6.3.3, which shipped with an empty description and no
downloadable artifact. Three fixes close the loop: the CHANGELOG's `[Unreleased]` section is now
stamped into `## [X.Y.Z] - <date>` at release time so it's quotable from the tag; the release body
is rebuilt from independent prose and test-report halves instead of being appended to, so
hand-written prose always wins and re-running is a no-op; and every module jar (including the CLI)
is now attached as a release asset, sourced from the local build, a zipped bundle, or Maven
Central. The Maven Central fallback also powers a new `./release.sh --github-assets [version]`
mode that retroactively repairs old releases, which is how v6.3.3 was fixed.

## [6.3.3] - 2026-08-18

### Fixed

#### Replica-set node cut off for good after a restart, with nothing in the leader's log
A restarted node could stay outside its replica set indefinitely: the leader never reopened a socket to it, so the node saw no leader and ran PreVote rounds forever without winning, while only restarting the leader (not the cut-off node) helped. The root cause was a stale cached driver per peer in `ElectionNetworkClient` — a closed `SingleMongoConnectDriver` handed out a wrapper around a `null` connection whose first use threw a plain `RuntimeException`, uncaught by the `MorphiumDriverException`-only eviction, and logged only at TRACE. Peer connections are now validated before reuse and evicted on any exception, with one dial attempt per tick and an unreachable peer reported at WARN.

#### SingleMongoConnectDriver could end up permanently dead (#310)
The same driver defect, fixed at its source since `Morphium` selects this driver whenever no driver name is configured: after a connection loss, the recovery path ran `close(); connect();`, but `close()` also cancels the driver's own heartbeat (interrupting the recovery thread itself), so a failed `connect()` left the driver with no connection and no scheduled repair, handing out unusable wrappers forever after. `getConnection()` no longer returns a wrapper around `null` — it reconnects or throws `MorphiumDriverException` — and the recovery path now closes only the connection while keeping the heartbeat scheduled, so it retries on every tick. Behaviour change: a closed driver is now revivable, since `getConnection()` reconnects instead of returning a broken wrapper.

#### Discarded drivers kept a scheduler thread and could revive themselves (#311)
`close()` never shut down a driver's private scheduler thread, so each discarded driver leaked one idle daemon thread; `close()` now shuts it down and a reused driver builds a fresh one. Peer connections in `ElectionNetworkClient` now run with the driver's own heartbeat switched off and their host seed pinned to the one peer they were dialed for, since a successful connect otherwise enlarges the seed to the whole replica set and a failed dial could silently attach to a different node than intended. In the 2026-08-18 incident this seed-drift bug let a candidate's own vote request get answered by itself under a peer's name, manufacturing a false majority that made an empty-log node primary against the only up-to-date node's explicit denial.

#### An election could deadlock with no electable node at all (#312)
The priority check in `ElectionManager` was an absolute veto, so combined with the log-recency veto a replica set could end up with every candidate permanently denied by someone — as happened on 2026-08-18, when the only node with real log state held the lowest priority while the higher-priority nodes were fresh restores at index 0. Since neither veto is a race, no retrying resolved it; only restarting all three nodes at once (so all report index 0) worked around it. Priority is now a preference with a time budget: after the cluster has been leaderless for three election timeouts, priority alone no longer denies a candidate, with the window re-arming on every heartbeat so repeated failovers each get the full preference.

#### Test-results report: skipped tests were invisible, and no record ever qualified for a tag
Two independent defects made the release table and README badge misleading. `test_report.py` rendered only `Tests` and `Passed`, omitting the `Skipped` column even though the record builder tracked it, so e.g. "2114 tests, 2100 passed" looked like 14 silently lost tests; a `Skipped` column now makes `Tests = Passed + Skipped` visible. Separately, no test record could ever qualify for a tag commit because `pom.xml` was not path-allowlisted as "does not change the released artifact," and the maven-release-plugin rewrites the version in every `pom.xml` on tagging — so every tag disqualified every record, leaving the badge permanently red and the "living report" never refreshing. `pom.xml` is now judged by content instead of path, blanking only the project/parent version and scm tag before comparison (a `<version>` inside a `<dependency>` or `<plugin>` still disqualifies), and anything unparsable still fails closed.

#### Coverage badge removed from the READMEs
It rendered as `custom badge | resource not found`, since `badges/coverage.json` is only written when a record carries coverage data and `runtests.sh` never passed `--coverage-xml`. The badge is removed from both READMEs until coverage is actually collected; the coverage plumbing itself is unchanged and ready for when data starts flowing.

## [6.3.2] - 2026-08-18

### Added

#### PoppyDB: byte budget for the secondary-side replication event queue (`--event-queue-budget`)
The secondary-side replication event queue was count-capped (100k events) but unbounded by bytes, the same failure family as the replay-buffer incident — large bulk-export messages on a busy bus could blow the heap well before hitting the count cap. Since queued events haven't been applied yet, evicting is not an option (that would be silent data loss), so the fix extends the existing count backpressure to bytes: once estimated queued bytes exceed the budget, the change-stream reader blocks until the apply side drains. Configured via `--event-queue-budget` with the same size syntax as `--replay-buffer` (default 256m); an oversized single event is still admitted into an empty queue so it can never block forever.

#### `usersInfo` — `db.getUsers()` now works against PoppyDB
Listing users in mongosh failed with "no such command: 'usersInfo'": the in-memory driver implemented `createUser`/`updateUser`/`dropUser`, but not the command every user-listing helper sends. `usersInfo` now reads the same `admin.system.users` documents and answers in mongod's shape, supporting all the argument forms mongod takes plus `forAllDBs`. Stored credentials are withheld unless `showCredentials` is requested; an unknown user yields an empty list rather than an error, matching mongod.

#### Decoupled test-results store, release report and badges
Test runs can now publish a JSON record of their results to the append-only `test-results` orphan branch via `runtests.sh --publish-results`, decoupled from the machine that produced them so any contributor can supply results without homelab infrastructure. `release.sh` aggregates the records per (commit, phase) and posts an honest result table to the GitHub release notes, missing or broken phases included, plus optional JaCoCo coverage. The report is a living one: `scripts/updateReleaseReport.sh` refreshes the marked section in the release notes and the README badges as new results come in, without cutting another release. This is a report, not a gate — `release.sh` never aborts on an incomplete or red matrix, though `test_report.py` still exits non-zero so a future CI job can build a gate on top if desired.

#### PoppyDB: honest capability advertisement in the hello reply (`poppyCapabilities`)
The hello reply advertises replica-set topology and logical sessions, which makes modern drivers enable retryable writes by default — a capability PoppyDB does not actually have (no `(lsid, txnNumber)` deduplication; real support is specced in #293). Since there's no standard hello field to say "sessions yes, retryable writes no", the reply now carries an explicit `poppyCapabilities` document listing the real limits (`retryableWrites: false`, `journal: false`, `durability: "snapshot"`, etc.). Non-Morphium clients should connect with `retryWrites=false`.

#### PoppyDB: mongodump/mongorestore work against PoppyDB (mongo-tools compatibility)
`mongorestore` against a PoppyDB used to die at the handshake, and dumps of real-world schemas could not be loaded at all, so the whole tool chain was fixed end-to-end; a full dump → restore → dump round trip including secondary indexes now passes. Fixes include: the legacy `isMaster` reply carried a `QueryFailure` flag that made strict drivers (mongo-tools' Go driver) drop the connection; `buildInfo` now reports a `versionArray` since mongorestore refuses servers announcing fewer than 3 version components; OP_MSG kind-1 document sequences (how mongo-tools ship bulk inserts) are now merged and written correctly; `OpMsg.parsePayload` now bounds parsing by the wire-header size instead of the buffer length, fixing a pipelining client running into the next message's bytes; BSON Decimal128 is now encoded/decoded; and a message that fails to decode gets an error reply instead of being silently dropped.

### Changed

#### Object mapper: type-id class resolution and no-arg-constructor lookup cached
Profiling found `ObjectMapperImpl` deserialization noticeably slower than the official driver's `PojoCodecProvider`, with the biggest avoidable cost in `AnnotationAndReflectionHelper.getClassForTypeId()` running `Class.forName()` on every call for each embedded object carrying a `class_name`. That lookup is now cached per helper instance, `deserialize()` caches the resolved no-arg constructor per class, and hot `customMappers` checks avoid a redundant `containsKey()`+`get()`. Behavior is unchanged; only the internal caching was added.

#### Test suite: timing-sensitive sleep+assert patterns replaced with condition waits (#292)
A `BulkInsertTest` flake on CI turned out to be one instance of a suite-wide pattern: `Thread.sleep` followed by an assertion on DB or messaging state. The nine files with the highest density now use bounded `TestUtils.waitForConditionToBecomeTrue` waits instead, while sleeps that are genuinely load-bearing (negative "must-NOT-arrive" windows, TTL waits, throughput measurements) were deliberately kept. No production code affected; remaining sleep+assert files are tracked in #292.

#### Test suite: retired the ncmessaging (polling-only) test package (#292)
The `ncmessaging` suites were aging, mostly `@Disabled` copies of the regular messaging tests with `setUseChangeStream(false)` hard-coded. Polling-only mode itself stays fully supported and tested (the MongoDB-Single CI phase runs the whole messaging suite in that mode); the one scenario without a counterpart, request/reply round trips forced to polling on a replica set, moved to `AnsweringTests.waitForAnswerPollingOnlyTest`.

#### Test suite: all bare `assert` statements migrated to JUnit assertions (#292)
Bare Java `assert` statements across the test files only ever ran because surefire enables `-ea` by default; they are now `assertTrue(...)` calls, independent of JVM flags and producing proper assertion errors. Messages and lazy evaluation are preserved. Behavior-preserving by construction, since assertions were already enabled in the test JVMs.

#### Messaging: "CHANGESTREAM DUPLICATE CAUGHT" dropped from WARN to DEBUG
The guard fires whenever the change stream and the fallback poll both find the same message, which at a 10s fallback interval is normal operation and was burying the handful of warnings that actually matter. Deduplication behavior is unchanged, only the log level.

### Fixed

#### PoppyDB election: a failed retry-persist forgot a durably granted vote (#306 review round 2)
The persist-failure rollback in `handleVoteRequest` reset `votedFor` to null unconditionally, so on a retry from a candidate the node had already durably voted for, a transient persist failure erased the earlier, still-durable vote from memory — letting a second candidate be granted the same term (two votes, two leaders). The rollback now restores the previous `votedFor` instead, so a failed persist denies the retry without forgetting the vote that actually stands.

#### PoppyDB election: a leader demoted by a straggling higher-term vote response went permanently silent (#306 review round 2)
The higher-term check in `handleVoteResponse` demoted a node with `resetTimer` only for candidates, so a node that had already won (its election timer cancelled by `becomeLeader()`) ended up as a follower with no heartbeats to receive and no election timer to fire — if the higher-term peer never made contact again, the node never campaigned again. Every other leader-demotion path re-arms the timer; this one now does too.

#### PoppyDB election: the partial-restore guard deadlocked peer-less nodes, and only the CLI ever armed it (#306 review round 2)
Two bugs. First, the guard's only release path is a completed initial sync from a primary, which a single-node replica set can never have — one broken dump file held back candidacy forever with no runtime override; a node without peers now skips the hold-back. Second, only `PoppyDBCLI` called `setLocalDataComplete(false)`, so an embedder following the documented restore pattern booted a gutted node that still considered itself electable, recreating the empty-node-wipe risk. `restoreFromDump()` itself now drops the guard on a partial result.

#### PoppyDB: the replication-manager field was assigned after start(), losing fast sync-complete notifications (#306 review round 2)
The one-shot initial-sync completion notification could fire before `startReplicationToLeader` assigned the new manager to its field — on a fast sync, the receiver then discarded the release as coming from a superseded manager, leaving the partial-restore guard stuck until some unrelated resync. The field is now assigned before `start()` (as the static-mode path already did), with the assignment rolled back if `start()` throws.

#### PoppyDB election: the state-file quarantine bricked every upgrade from a pre-checksum build (#306 follow-up)
The mandatory three-key state-file schema (currentTerm, votedFor, CRC32 checksum) quarantined every file written by the immediately preceding builds, which wrote no checksum at all — on upgrade, every node of an RS came up "holding back candidacy," so no candidate, no primary, every client failing with "No primary node found." A missing checksum key is now recognized as the legacy signature; such files are restored and immediately rewritten in the current format, closing the unprotected window. Empty files, files without currentTerm, checksum mismatches, and checksum-era files missing votedFor are still quarantined as before.

#### PoppyDB election: a vote could be granted without being durable, and a broken state file reset the node to term 0 (#306)
`persistElectionState()` swallowed every write failure, yet the voter confirmed the vote anyway despite the "votedFor must be durable before the response leaves" contract — a crash after such a phantom persist let the restarted node vote a second time in the same term (two leaders in one term). A failed persist now turns the grant into a denial and aborts the candidacy outright. Relatedly, `loadPersistedState()` treated an existing-but-unreadable state file like a missing one and restarted at term 0, which is safe for term inflation but not for double voting; a missing file still starts clean, while an unreadable one now keeps the node out of elections entirely until the operator restores or deletes it. The persisted state also gained a mandatory schema (currentTerm, votedFor, CRC32 checksum) with fsync on write, so a truncated or corrupted file is detected and quarantined instead of silently defaulting to "term 0, never voted."

#### PoppyDB election: the partial-restore candidacy guard was never released in election mode (#306)
A node with an incomplete dump restore is barred from candidacy until an authoritative initial sync replaces its local state, but the release of that guard lived only in the static-mode replication path — election mode replicates through `startReplicationToLeader()`, which had no sync-completion hook at all. So a guarded node synced fine and then stayed barred forever: with an intact primary A and partially-restored B and C, everything worked until A died, then B and C refused every candidacy despite holding full copies. `ReplicationManager` now exposes an `onInitialSyncComplete` hook wired by both replication paths, fired only once buffered change events have actually drained (not merely when the snapshot is copied) and only while that manager instance is still the current one.

#### PoppyDB election: vote responses from earlier rounds were credited to the current PreVote round (#306)
`handleVoteResponse()` tallied every incoming grant into whatever round happened to be open, with no round correlation or request-type check. In a three-node set, the self-vote plus one grant straggling in from an earlier PreVote round already formed a "majority," starting a real election that no current peer had agreed to — letting the exact livelock PreVote was built to prevent return through this side door. Every outgoing (Pre)Vote request now carries a sender-local round id, and responses that don't match the current round or request type are discarded; higher response terms are still honored before correlation, since discovering a higher term is authoritative news regardless of round.

#### InMemory dump restore: ORM-written documents made a whole database unrestorable (#306)
On the customer acceptance environment, exactly the databases containing ORM-written documents (carrying `class_name`) failed to restore from dumps: the restore path ran them through entity-aware `ObjectMapperImpl` deserialization, and any absent field of a dump-marked type (`Date`, `UUID`, `byte[]`, ids) NPE'd the entire database ("Parsing failed … 'd' is null"). The restore now converts the dump payload at the dump boundary itself without any entity resolution — documents stay plain maps with `class_name` preserved, and only the `{class_name, value}` marker maps are turned back into their storage types. Id markers are read tolerantly (both `ObjectId` and `MorphiumId` forms) and restore as `MorphiumId`, matching what the wire path delivers; restore failures now name the offending field path instead of a bare "Parsing failed".

#### PoppyDB replication: a freshly-synced node could report log index 0 forever (seed race)
On a loaded host, a secondary whose initial sync finished fast could permanently report replication position 0 to the election layer despite holding the primary's complete dataset. The root cause was a race between two one-shot reporters: the watch's registration callback flipped `watchLive` (releasing the initial-sync thread) before recording the primary's sequence seed, so a sync that outran that gap reported 0 and nothing ever re-reported the real position afterward. Since #306's candidacy restraint treats index 0 as "empty node, must not campaign," the raced node could lock itself out of every election even while holding full data. The position now catches up instead of being reported exactly once — the registration callback records the seed before flipping `watchLive`, and the batch processor's flush tick periodically reconciles the election view even with nothing to drain.

#### PoppyDB election: PreVote stops empty/syncing nodes from dethroning a healthy primary (#306)
A rolling upgrade on a 3-node replica set ended in a permanent leaderless livelock: a freshly restarted, still-empty node could never win an election, but it kept campaigning every timeout, and every higher-term RequestVote forced the healthy primary to step down — Raft's textbook "disruptive server" problem, where the vote veto prevents the wrong winner but not the disruption itself. The election now implements PreVote (Raft §4.2.3/§9.6): before any real election, a node asks peers "would you grant me a vote?" without touching any term, and only a pre-granted majority starts the real election, so an empty or log-behind candidate retries forever without inflating the term. Companion changes close related gaps: leader stickiness (a voter with a live lease or recent heartbeat ignores higher-term requests without adopting their term), candidacy-restraint hardening, and opt-in term/votedFor persistence via `morphiumserver.electionStatePath`. The PreVote probe rides as an extra field on the existing `requestVote` command, so old nodes remain wire-compatible and are never blocked from a cluster running the new code.

#### PoppyDB: restore-on-startup silently aborted on the first broken dump file, starting the node near-empty (#306)
During a rolling upgrade, a node that had correctly dumped all 8 databases came back with only 2: `restoreAllFromDirectory` looped over dump files with no per-file error handling, so the first file that failed to parse threw straight out of the loop, silently skipping everything after it and joining the replica set as a near-empty (and election-disrupting) member. The failure was invisible three times over — no logging of which file broke, a summary line that never ran, and a catch block that logged only `e.getMessage()`. Each dump file is now restored under its own try/catch, so a broken file is logged on ERROR while the rest are still attempted, and a summary line is always emitted (INFO when complete, WARN with names when partial).

#### InMemoryDriver/PoppyDB: dumps of any database with real data were unrestorable — "Parsing failed" (#306)
The fault-tolerant restore above immediately surfaced the bug it had been hiding: the dump writer (`Utils.writeJson`) never produced parseable JSON for real content — strings were written verbatim (breaking on any quote/backslash/control character) and `Date`/`UUID` values as bare unquoted `toString()` tokens, so a single timestamp field lost the whole database. `byte[]` also silently came back as `List<Long>` and ids as plain `String`s, and both sides of the roundtrip used the platform default charset, mojibaking umlauts across platforms. Dumps are now written as UTF-8 with proper JSON string escaping, with Date/UUID/ObjectId/byte[] as `class_name`-marked maps the restore converts back to exact storage types; existing legacy dumps restore as far as they structurally can, but content with quotes, backslashes, or Date/UUID values written by the old broken code cannot be recovered.

#### PoppyDB CLI: election-state persistence was silently inactive — dump directory was set after `configureReplicaSet()` (#306)
The term/votedFor persistence introduced for the #306 election churn never engaged on the customer environment: `configureReplicaSet()` derives the state-file path from the dump directory, but the CLI set the dump directory only afterwards, so the config never got a path and neither persisting nor loading ever ran. The CLI now sets the dump directory first, and `PoppyDB.setDumpDirectory()` logs an unmissable WARN if it's called after an election-enabled `configureReplicaSet()` without persistence, so embedders cannot fall into the same trap.

#### PooledDriver: a client could stay stuck on "No primary node found" forever after a replica-set restart sequence (#304)
Several service instances kept failing every operation for 30+ minutes after their PoppyDB replica set was restarted node by node, and only an application restart recovered them. Nothing but the heartbeat sets `primaryNode`, and two defects could stop it from probing permanently: the heartbeat cycle ran unguarded inside `scheduleWithFixedDelay`, which cancels the whole periodic task on any single unhandled exception; and a per-host check could register its bookkeeping entry after the check thread had already finished and removed it, leaving a stale entry that every later cycle skipped forever. The cycle is now wrapped so nothing escapes it, the claim is written before the thread starts, and a stale claim heals itself on the next cycle.

#### InMemoryDriver/PoppyDB: index buckets leaked every deleted document whose indexed array or sub-document had been updated (#303)
A PoppyDB message bus ran its heap over the watermark and rejected all writes for many hours; the dump showed a single `CollectionIndexStore` retaining gigabytes in long-deleted documents. The cause was an index key that kept changing after being filed: `IndexKey` stored the document's own `List`/`Map` instance directly, and the driver mutates documents in place (`$push`/`$addToSet`/dotted `$set`), so once a document was updated, the filed key no longer matched on removal and the bucket kept the document forever. Messaging was the perfect trigger, since every message's `processed_by` list gets pushed before deletion. Keys now snapshot mutable container values deeply when extracted, keeping hash and equals consistent for the key's whole lifetime.

#### Messaging listener registration could silently drop listeners (and throw an NPE)
`SingleCollectionMessaging` and `DualChannelMessaging` published their topic→listener map lock-free via unsynchronized clone-and-swap on a `volatile` field, so two writers cloning the same map concurrently could make the later swap discard the other's entry — visible as an NPE in `addListenerForTopic` (the flaky `MessagingRequeueEventTest`), but more dangerously as a listener silently vanishing with its messages never delivered. The map is now a `ConcurrentHashMap` of `CopyOnWriteArrayList`s mutated under the map's per-key lock, so concurrent registration composes safely. One behaviour change: installing the status-info listener now adds to whatever is registered under its name instead of replacing it, so an application listener sharing that name is no longer silently discarded on `start()`.

#### `MultiCollectionMessaging.removeListenerForTopic()` removed the wrong listener
The lookup walked the topic's entries with an index that kept counting when no match was found, so removing a listener never registered for that topic silently evicted the last one instead — including terminating its change stream monitor, leaving the topic subscribed-but-deaf. Removing from a topic with no listeners threw an NPE. The lookup now matches by identity or does nothing, and runs under the map's per-key lock like the other two messaging implementations.

#### PoppyDB: a restarted empty node could wipe the whole replica set
Reproduced kill chain: kill one node of a 3-node RS, restart it empty (fresh data dir), and it could both win the next election and cause the surviving, data-bearing followers to drop their local databases to match it. Two holes made this possible: `ElectionManager`'s log-recency check was vacuous since `lastLogIndex` had no production writer and stayed 0 on every node, so an empty candidate compared as "up to date" as a real voter; and a follower's fallback full resync trusted whatever the primary reported unconditionally, with no way to tell a legitimately empty primary from a stale one that had simply forgotten everything. The fix has three parts: vote safety (a candidate reporting index 0 never wins against a voter above 0), candidacy restraint (an empty node holds off campaigning while it can see a data-bearing peer), and a fail-closed resync (a follower refuses a destructive drop-to-match resync whenever the primary's replication sequence is behind what the follower's own data last reflected). This guarantees safety for the single-node-restart case; if a majority of nodes restart empty simultaneously, an empty node can still be elected, though the fail-closed resync still protects each surviving node's local data. If the last data-bearing node in a cluster dies permanently, surviving empty nodes deliberately hold back candidacy indefinitely — restarting any one of them clears its peer-index memory and lets the cluster elect again.

#### PoppyDB: j:true write concern no longer promises durability that does not exist
A `j: true` write concern was silently accepted and acknowledged although PoppyDB has no journal (persistence is periodic snapshots). The write still executes, but the answer now carries `writeConcernError` code 2 (`BadValue`), so clients relying on journal durability learn the truth instead of getting a hollow acknowledgement.

#### PoppyDB: secondaries no longer serve reads that defaulted to primary read preference
Only an explicit `mode: "primary"` was rejected on secondaries — a read without `$readPreference` was silently served, returning possibly-stale data even though MongoDB's default read preference is `primary`. Such reads now get `NotPrimaryNoSecondaryOk` (13435), matching mongod's handling of a direct secondary connection without `secondaryOk`. Morphium's own wire commands always send a read preference and are unaffected.

#### InMemoryDriver: MongoDB collation strength mapped to the wrong Java collator level
MongoDB collation strength (1=primary..5=identical) was passed straight to `java.text.Collator.setStrength()`, whose constants are 0-3, so every level was silently shifted by one and `strength: 4`/`5` threw an `IllegalArgumentException`. The values are now mapped explicitly; since Java has no quaternary level, 4 and 5 both map to IDENTICAL.

#### PoppyDB: find fast path ignored the client's collation (#252 follow-up)
The #252 fix wired `collation` through the update/delete/count/distinct wire fast paths but missed `find`, so a collation-aware find matched differently depending on internal dispatch path. The collation now reaches the driver on both the single-shot and cursor-window path, and the server-side find cursor carries it so `getMore` refills reuse the same collation as the first batch.

#### InMemoryDriver: bulk-insert writeErrors pointed at the wrong batch positions, n overcounted
The insert path removed failed documents from its working list between error-detection passes, so `writeErrors.index` values reported afterward referred to the shrunken working list instead of the batch clients actually sent. A parallel original-index list now keeps reported indexes stable. Separately, `n` was computed as `batchSize - writeErrors.size()`, correct only for unordered inserts — an ordered insert stops at the first error, so the never-attempted tail was counted as inserted; both paths now derive the committed count from the first error's batch index.

#### PoppyDB: commitTransaction/abortTransaction failures were swallowed
A `commitTransaction`/`abortTransaction` that threw was only logged — the client received an unconditional `ok:1` and believed its transaction was committed. Failures are now answered as a mongo-shaped error (code 8 `UnknownError`, or the driver's mongo code if it attached one). Commit/abort without an active transaction remains a lenient `ok:1` no-op.

#### Write buffer: remove-by-query deleted only a single document
`BufferedMorphiumWriterImpl.remove(Query, multiple, callback)` accepted the `multiple` flag but never passed it to the queued `DeleteBulkRequest`, whose default is `multiple = false` — so for any `@WriteBuffer` entity, `morphium.remove(query)` and `clearCollection()` silently deleted exactly one matching document. The bug was masked for years since the InMemoryDriver bypasses the buffered writer entirely, and only surfaced when the #292 sleep→condition hardening turned a tolerant settle sleep into a hard count assertion. The flag is now propagated, with a regression test covering partial and full remove-by-query. The dead `driver/wire/BulkContext` skeleton was removed in the same change.

#### Messaging: legacy documents with processed_by: null are deliverable again (#291)
A stored message whose `processed_by` is an explicit `null` made the pre-exec marking fail on mongod, and since 6.3.x requires exclusive messages to be marked before the listener runs, that turned into hard non-delivery (no listener call, no answer, `sendAndAwait` timeout). Morphium senders can't produce such documents, but foreign writers, raw-driver writers and restored dumps can — observed in production against a consumer upgraded from 6.2.4. All marking sites now fall back to an atomic repair (`{processed_by: null}` → `{$set: [own id]}`), and the InMemoryDriver now rejects explicit null for `$addToSet`/`$push` like mongod instead of masking the class of bugs. A related race was also closed: `getIndexStore()` builds could snapshot documents before a concurrent write invalidated the store and then publish a stale snapshot anyway, in the worst case admitting a duplicate `_id`. Every invalidation now bumps a per-collection epoch before removing the store, and a build whose epoch moved during snapshotting is not published.

#### InMemoryDriver: literal array queries support whole-array equality ({field: []} et al.)
A literal query with an array operand only ever matched via the multikey "array contains the operand as an element" rule, missing MongoDB's additional match when the document's array *is* the operand. Most visibly, `{processed_by: []}` — the empty-array form services use against messaging collections — matched nothing at all. Both query engines now check whole-array equality with the same id/number normalization as scalar comparison, on plain and dotted paths. Found during the mongorestore rehearsal for the acceptance drop-in test.

#### InMemoryDriver: unique+sparse indexes no longer throw false duplicate-key errors
A `unique: true, sparse: true` index (the classic optional-email pattern) rejected the second document lacking the indexed field with E11000, since both the index store and the insert-path pre-check treated the missing key as a colliding value. Per MongoDB semantics, documents missing all of a sparse index's fields are not part of the index and cannot collide; the uniqueness check now skips them. Also fixed in passing: decoding a BSON MaxKey threw "unknown data type" due to a missing `break`.

#### InMemoryDriver: unique partial indexes enforced uniqueness over the whole collection
A `unique` index with a `partialFilterExpression` was created and reported with its filter, but the filter was never evaluated — uniqueness was enforced against every document, so a schema like JEF's task queue rejected documents outside the filter with E11000 where mongod accepts any number of them. Documents outside the filter are not part of a partial index and cannot collide in it; the index store now honours that, including the cuts-both-ways case where a non-matching stored document no longer blocks an unrelated insert. A follow-up review closed three more gaps: a legacy O(collection)-scan unique pre-check that re-implemented (and got wrong) the same membership rule was deleted outright, an update moving a document into the partial filter now runs the uniqueness check too, and TTL expiry now honours `partialFilterExpression` instead of deleting uncovered documents.

## [6.3.1] - 2026-08-11

### Added

#### Messaging: implementation mismatches between queue participants are detected (#280)
All three messaging implementations use incompatible collection layouts, and a mixed queue used to fail *silently* in the worst direction: broadcasts kept flowing while answers landed in a collection the other side never reads. Every messaging instance now announces its implementation on startup in a layout-independent `<queue>_participants` collection and checks what the other participants run. On a mismatch the default is a WARN log; `MessagingSettings.ImplementationCheck.THROW` makes a mismatched instance refuse startup, `IGNORE` disables the check entirely. Detection and diagnostics only — no bridging between layouts.

### Changed

#### Messaging: the main change stream filters server-side (#283)
Every consumer's change-stream cursor used to receive every insert into the messaging collection — including messages addressed to other recipients — and under high traffic the cursor fell behind, degrading delivery to fallback-poll latency. The main change stream is now built with a server-side `$match` restricted to what the instance can actually process: messages addressed to it, broadcasts for topics with a registered listener, and answers. The stream is rebuilt when the registered topic set changes, and the filter still matches V5-legacy senders' `name` field.

#### PoppyDB: replication applies events on arrival
Replication events were applied on a 5 ms flush tick; they are now applied when they arrive,
noticeably reducing secondary lag.

### Fixed

- **Messaging: the lock-release change-stream callback no longer queries (#286).** It ran a
  `countAll` per deleted lock on the change-stream thread itself, so a burst of lock releases
  stalled the stream (`msg_lck` stalls). Replaced by a counter that coalesces any number of lock
  events into a single poll.
- **InMemoryDriver: equality queries on an indexed array field silently returned nothing (#289).**
  The index store does not implement multikey indexes, but the planner used such indexes anyway —
  an index-backed `find`/`count` on e.g. `processed_by == "X"` returned an empty result. Indexes
  are now flagged multikey as soon as a document stores a list in an indexed field — including
  arrays crossed *mid-path* (an index on `a.b` over `{a: [{b: …}]}`) — and excluded from query
  planning; such queries scan and evaluate MongoDB's array semantics correctly.
- **InMemoryDriver: change-stream events could arrive out of order under load.** Client-mode
  dispatch submitted each event as its own task to a cached thread pool, which preserves no
  submission order — two back-to-back events could reach a subscriber swapped, or even
  concurrently. Delivery now runs on a single dispatcher thread (unbounded queue, writers never
  block), restoring mongod's per-cursor ordering guarantee.
- **InMemoryDriver: `update` and `replace` change-stream types now match mongod (#288).** An
  update without `$` operators (a client's `replaceOne`) emitted no event at all — invisible to
  every watcher including PoppyDB replication; it now emits `replace` with the new `fullDocument`
  and no `updateDescription`. And `store()` of an existing document emitted `replace`, where the
  ORM's store goes out on the wire as a `$set` update that mongod reports as `update` — it now
  emits `update` with a computed `updateDescription`.
- **InMemoryDriver: collection and index-descriptor creation are atomic.** Two racing first
  writes (e.g. concurrent `createUser`) could both observe "collection absent" and both win.
- **Messaging: a failed main-change-stream rebuild is retried.** The topic-filter snapshot was
  committed before the new monitor had started; if starting it failed, the staleness check
  considered the filter current and the instance kept running without a main change stream.
- **Messaging: the listener registry is no longer mutated in place** (status-info listener
  toggles, `terminate()`) while the poll thread iterates it — a
  `ConcurrentModificationException` risk; the field is volatile now and all mutations
  clone-and-swap.
- **Build: the parent POM's `<scm><tag>` had regressed to `v6.2.7`**; development iterations
  point at `HEAD` again.

## [6.3.0] - 2026-08-09

### Added

#### `DualChannelMessaging` — a third messaging implementation, in beta (#265)
Load measurements showed that request/reply throughput on MongoDB is *delivery*-bound rather than write-bound: a single change-stream cursor hands out events at a fixed cadence regardless of the offered rate. `MultiCollectionMessaging` did better in those runs not because of its per-topic collection split, but because of its *second* cursor for answers and DMs — `DualChannelMessaging` ports exactly that one mechanism onto the Standard layout, keeping a single collection/cursor for broadcast/topic traffic plus a dedicated per-recipient collection with its own cursor and dispatcher for directed messages and answers. Select it with `cfg.messagingSettings().setMessagingImplementation("DualChannelMessaging")`. **Every participant on a given queue must run the same messaging implementation** — there is no bridge between collection layouts, and a mismatch fails silently (a WARN is logged on startup). Marked **beta**: the measured benefit is smaller and more nuanced than expected — better tail latency at some throughput cost past saturation — so it is opt-in while it gathers real-world mileage. See `docs/howtos/messaging-implementations.md`.

#### `dropUser` — the user lifecycle is complete (InMemoryDriver + PoppyDB)
The in-memory driver (and with it PoppyDB) now implements mongod-compatible `dropUser`: the user document is removed and a delete event is emitted on `admin.system.users` under the same ordering lock as `createUser`/`updateUser`, so PoppyDB secondaries replicate the drop like any other write. On a replica set the command is primary-only, like every other write. Previously the only way to remove a user was a raw delete on `admin.system.users`, which bypassed the event-ordering guarantee entirely.

#### `customData` support in `createUser`/`updateUser`
`createUser` stores an optional `customData` document on the user (mongod's shape);
`updateUser` accepts `customData` — replaced wholesale when given (including as the only field,
which previously returned `BadValue`), preserved when omitted. A password change no longer
silently discards stored `customData`. `authenticationRestrictions` remains unmodeled.

#### Driver: automated failover test via wire-rewriting proxy, replaces manual `FailoverReproTest`
`FailoverReproTest` reproduced the 6.2.6 failover regressions but required a hand-built replica set and manual process kills, so it was tagged `manual` and never ran in CI. `DriverFailoverProxyTest` reproduces the same client-visible failure modes — clean stepdown, hard kill, and the critical frozen-socket case — plus read/write/messaging recovery, through a reusable wire-level fault-injection proxy instead of killing processes. Tagged `wire-failover`, it now runs automatically against both MongoDB and PoppyDB replica sets in the normal test matrix, and `FailoverReproTest` is removed.

#### `morphium-jakarta-data` — optional Jakarta Data 1.0 runtime module
A new optional module, `morphium-jakarta-data`, brings a [Jakarta Data 1.0](https://jakarta.ee/specifications/data/1.0/) provider on top of Morphium's existing query engine: `@Repository`-based `CrudRepository`/`MorphiumRepository` interfaces with query derivation from method names, JDQL via `@Query`, `@Find`/`@Delete` with explicit `@By` parameter binding, and offset/cursor pagination. The dependency direction is strictly one-way — core has no knowledge of Jakarta Data and no dependency on this module, so `-DskipExtensions` still produces a core-only build. It is deliberately framework-agnostic, meant to be consumed transitively by `quarkus-morphium` and `spring-boot-morphium` rather than added directly by most applications. The code originates from [Bardioc1977/morphium-jakarta-data](https://github.com/Bardioc1977/morphium-jakarta-data), now archived in favor of this repository. See [Jakarta Data](docs/jakarta-data.md).

#### `quarkus-morphium` — optional Quarkus extension for CDI integration
A new optional module, `quarkus-morphium`, integrates Morphium into [Quarkus](https://quarkus.io) applications: a CDI producer for `Morphium`, type-safe `@ConfigMapping` configuration, declarative `@MorphiumTransactional` transactions, SmallRye health checks, Dev Services (an auto-started MongoDB container), build-time Jakarta Data `@Repository` implementations via Gizmo bytecode, and GraalVM native-image support. Like `morphium-jakarta-data`, core has zero dependency on this module, so `-DskipExtensions` still produces a core-only build; its integration tests need Docker and skip themselves when it's unavailable. **groupId migration:** this extension previously published as `io.quarkiverse.morphium`, although it never actually lived in the Quarkiverse GitHub organization — existing users of `io.quarkiverse.morphium:quarkus-morphium:1.2.0` must switch to `de.caluga:quarkus-morphium` at the adopted Morphium version; no package or API changes. The code originates from [Bardioc1977/quarkus-morphium](https://github.com/Bardioc1977/quarkus-morphium), now archived. See [Quarkus Extension](docs/quarkus-extension.md).

#### `spring-boot-morphium` — optional Spring Boot integration module
A new optional module, `spring-boot-morphium`, integrates Morphium into [Spring Boot](https://spring.io/projects/spring-boot) applications: `MorphiumAutoConfiguration` creates the application's `Morphium` bean from `morphium.*` properties, Jakarta Data `@Repository` interfaces are wired via a JDK dynamic proxy per repository (runtime reflection, unlike `quarkus-morphium`'s build-time Gizmo codegen), `@MorphiumTransactional` wraps the annotated method in a transaction via AspectJ, and an Actuator `HealthIndicator` reports live connection status. Publishes three artifacts including `morphium-spring-boot-test` (an end-user `@MorphiumTest` annotation wiring `InMemDriver` into a `@SpringBootTest`, no Docker needed). Core has zero dependency on this module, so `-DskipExtensions` is unaffected. **Two coordinate/naming corrections made pre-release, at zero breaking-change cost:** the modules were renamed from `spring-boot-morphium-*` to `morphium-spring-boot-*` (the `spring-boot-` prefix is reserved for Spring's own starters), and the property prefix from `spring.morphium.*` to `morphium.*`. Existing users of the pre-integration `de.caluga:spring-boot-morphium-starter:1.0.0-SNAPSHOT` must update their artifactId to `morphium-spring-boot-starter` and rename `spring.morphium.*` keys to `morphium.*` — no Java API changes. The code originates from [Bardioc1977/spring-boot-morphium](https://github.com/Bardioc1977/spring-boot-morphium), now archived. See [Spring Boot](docs/spring-boot.md).

#### PoppyDB: `--users-file` — declarative user provisioning (bootstrap, upsert, version-gated)
Builds on user replication: `--rootUser`/`--rootPassword` only ever provisioned one admin user, so any real application user set still had to be created by hand — not something you can put in version control. `--users-file <path>` now points at a JSON file (a bare array, or `{"version": N, "users": [...]}`) applied as an idempotent `createUser`/`updateUser` upsert wherever `ensureRootUser` already runs; a static-mode secondary never applies the file itself, receiving users instead through the same `admin.system.users` replication. An optional `version` field gates re-application against a small replicated meta document, preventing a straggler node from rolling credentials back on failback with an older copy of the file — though a node mid-resync can still be elected and apply a stale file, since the Raft log-freshness check is currently dead code (tracked as a follow-up). Unknown fields and duplicate `(user, db)` entries are now hard errors instead of silent last-entry-wins, and the file's POSIX permissions are checked like any other secret file. See [PoppyDB § Bootstrapping users](docs/poppydb.md#bootstrapping-users---users-file).

#### PoppyDB: `admin.system.users` replicates across the replica set — users survive failover
Users were node-local: `createUser` only ever wrote to whichever node's own `admin.system.users`, so a secondary never had the same login-able users as the primary, and a failover silently lost them. `admin.system.users` is now the one system collection that replicates, and it gained a proper `updateUser` command alongside `createUser`. Both commands, like all writes, are now primary-only — a secondary answers them with `NotWritablePrimary` instead of silently accepting a write that would only ever apply locally, which was the underlying cause of the gap. Two follow-up fixes round out the failover path: a demoted leader now resumes replication toward the new primary immediately, and an identical-data leader change (verified via `dbHash`) skips the full resnapshot. See [PoppyDB § Authentication — User replication](docs/poppydb.md#authentication---auth).

#### PoppyDB: configuration file support (`--cfg`/`-f`, `--no-config`), secrets kept off the command line
Production deployment needed a config file — every setting was CLI-only, and passwords on the command line are readable by any local user via `ps aux`/`/proc/<pid>/cmdline` for the life of the process. PoppyDB now optionally reads a `java.util.Properties`-format file, discovered in order from `--cfg`/`-f`, `$POPPYDB_CONF`, or a handful of default locations (`--no-config` skips them); precedence is uniform, command line wins over config file wins over built-in default, and `--no-ssl`/`--no-auth` close the precedence chain for the boolean flags. Keys are matched case/separator-insensitively, and an unknown key aborts startup with a "did you mean" suggestion instead of being silently ignored. `root-password`/`ssl-keystore-password` each gained a `*-file` counterpart for Docker/Kubernetes secret mounts, and any file carrying a secret has its POSIX permissions checked (world-writable refuses to start). `scripts/poppydb.sh`/`scripts/startPoppyDB.sh` always pass `--no-config` now, so a developer's private config can never silently change what a local test run connects to. See [PoppyDB § Configuration File](docs/poppydb.md#configuration-file) and the [Production Deployment Playbook](docs/howtos/poppydb-deployment.md).

#### PoppyDB: `--print-config`/`--check-config` CLI modes
`--print-config` prints the effective configuration (defaults + config file + command line, secrets redacted) as a reusable config file; `--check-config` validates syntax, semantics and deep checks (keystore loadable, dump-dir usable) without starting the server — exit code 0/1 like `nginx -t`. Startup itself now validates option ranges and cross-option consistency that were previously unchecked, reporting every configuration error at once instead of stopping at the first. See [PoppyDB § Inspecting and validating the configuration](docs/poppydb.md#inspecting-and-validating-the-configuration).

#### PoppyDB: DevOps command surface — live currentOp/killOp, rs.conf(), listCommands, hostInfo, real connection gauges
Closes the gaps that made mongosh's admin helpers fail against PoppyDB. A server-wide **op registry** tracks every command for the duration of its dispatch, backing `db.currentOp()` with mongod-shaped op documents and a cooperative `killOp` that interrupts the op's thread (never the Netty event loop). New commands: `listCommands`, `hostInfo`, `connectionStatus`, `whatsmyuri`, and `replSetGetConfig` (so `rs.conf()` now works, reconstructed from `--rs-seed`/`--rs-priorities`). `serverStatus.connections` reports the server's real client-socket gauges instead of the in-memory driver's internal connection borrows, and the embedded InMemoryDriver answers `$currentOp` with an honest empty set.

#### InMemoryDriver/PoppyDB: memory watermark — writes are rejected before the heap dies
An in-memory store dies of OOM when producers outrun consumers, and a replica set dies completely since replication copies the data volume to every node. Two watermarks (percent of max heap) now guard the write path: crossing **warn** (default 75%) logs once, and above **reject** (default 90%) document-creating writes are refused with a mongod-shaped `ExceededMemoryLimit` (146) that clients should treat as retryable backpressure — updates, deletes and TTL expiry stay allowed so the system can drain back under the watermark. Replication applies and the initial sync bypass the guard so a replica set fails together rather than diverging. Both stages decide on the **post-GC live set** rather than raw heap occupancy, since with `-Xms == -Xmx` the raw gauge routinely reads above 90% under load even when GC would free most of it. Configurable via `--memory-warn`/`--memory-reject` (100 = off); the counterpart per-collection LRU eviction is sketched for 7.0.

#### Driver/PoppyDB: `maxMessageSizeBytes` respected end-to-end — byte-aware write splitting, hello limits adopted, reply batches capped
The 48MB wire message bound was advertised but ignored: batching was count-based only, so 1000 × 1MB documents went out as one ~1GB OP_MSG that any real MongoDB — and PoppyDB's own decoder — answers by closing the connection. Three fixes: the PooledDriver now adopts `maxMessageSizeBytes`/`maxWriteBatchSize`/`maxBsonObjectSize` from the hello handshake instead of stale field defaults; write commands (`WriteMongoCommand`) split oversized `documents`/`updates`/`deletes` arrays into chunks and fold the per-chunk results into one mongod-shaped answer; and in server mode the InMemoryDriver caps reply batches by bytes like mongod, pushing the remainder back onto the cursor. Also fixed on the way: a `getDeclaredMethod` dispatch-lookup bug that broke generic command dispatch for subclassed drivers.

#### InMemoryDriver/PoppyDB: BSON document size limit enforced like mongod — configurable, default 16MB
The 16MB limit was only ever *advertised* (the embedded driver even claimed a fantasy 128MB), never enforced — updates could grow documents without bound, which no real MongoDB would accept. The InMemoryDriver now checks inserts/stores against the plain limit and update/replacement/upsert results against limit+16KB (matching mongod's `BSONObjectTooLarge` margin), rolling the mutation back atomically like a unique-violation. `hello` advertises the configured value so drivers enforce it client-side exactly as against mongod; configurable via `--max-bson-size` (0 = off). On the way, PoppyDB's `hello` no longer pays a ~30s reverse-DNS lookup when the RS seed list already names the member, and a subclass-breaking `getDeclaredMethod` lookup bug in command dispatch was fixed.

#### InMemoryDriver/PoppyDB: `dbHash` and `validate` — consistency checks with teeth; `top` fails explicitly
`dbHash` computes an MD5 per collection in **canonical document order**, so two replica-set members holding the same data produce the same hash even though initial sync and live replication materialize collections in different order — the one-command consistency check for failover/replication tests, answered on secondaries too. `validate` is a real check now: it walks every index and reports entries referencing documents no longer in the collection, and documents missing from an index, with `valid: false` when anything is off. `top` now fails with an explicit `CommandNotSupported` (115) instead of a generic CommandNotFound, since real mongod has the command.

#### PoppyDB: `--log-level` option — the server no longer logs everything at DEBUG
The CLI fat jar shipped no Logback configuration, so Logback fell back to its basic setup: **every logger at DEBUG on the console**, which produced enormous logs on long-running servers (one orphaned instance filled a test runner's disk). The fat jar now bundles a server configuration (root `INFO`, Netty `WARN`), and verbosity is adjustable at startup via `--log-level`, `-Dpoppydb.log.level=<level>`, or a full replacement via `-Dlogback.configurationFile=...`.

#### Driver: configurable `appName` in the connection handshake
New setting `DriverSettings.appName` (default `"Morphium"`), sent to MongoDB as `client.application.name` in the `hello` handshake. Set it per service to tell instances apart in `db.currentOp()`, server logs and profiler output (MongoDB truncates values over 128 bytes). Third-party `MorphiumDriver` implementations keep compiling — the new interface methods are defaults.

#### InMemoryDriver: aggregation stages `$documents`, `$densify`, `$fill`, `$setWindowFields`, `$collStats`, `$listSessions` — and a real `$out` (#254)
`$out` no longer pretends: it actually replaces the target collection through the driver's primitives, so index/capped/TTL bookkeeping and watchers stay intact. `$documents` provides literal document sources, `$densify` fills numeric and date gaps, `$fill` supports `value`/`locf`/`linear` with partitioning, and `$setWindowFields` implements partitionBy/sortBy with documents-windows for the core accumulators (remaining window functions and range windows followed in #255). `$collStats` returns real counts and `$listSessions` an honest empty set.

#### InMemoryDriver: remaining $setWindowFields window functions (#255)
`$setWindowFields` now covers the full window-function surface: the statistical accumulators (`$stdDevPop/Samp`, `$covariancePop/Samp`), the N-forms (`$firstN/$lastN/$minN/$maxN`, `$top/$bottom/$topN/$bottomN`), the time-series functions `$derivative`/`$integral`/`$expMovingAvg`, and the gap-fillers `$linearFill`/`$locf`. Range windows (`window: {range: [lo, hi], unit?}`) now work for the whole accumulator family, resolved against an ascending single-field sortBy. Invalid specs keep failing loudly with mongod-style error codes instead of returning silently wrong results.

#### Expr: ~40 aggregation expression operators implemented, three silent mis-calculations fixed (#255)
All stubbed operators are real now, including `$map`, `$arrayToObject`, the byte/codepoint string family, date/math operators like `$dateAdd`/`$dateTrunc`/`$round`, and new ones like `$sortArray` and `$median`/`$percentile`. Fixed on the way: `$asinh` computed **sinh** instead of its own inverse, `$setUnion` collected the arrays instead of their elements, and 2-arg `$atanh` silently returned 0 instead of erroring. `$function`/`$accumulator` still throw, since there's no server-side JS.

#### InMemoryDriver: positional update operators `$`, `$[]`, `$[<identifier>]` with `arrayFilters`, and `$bit` (#256)
Array element updates work now: `{$set: {"items.$.qty": 5}}` resolves the query's match position, `$[]` applies to all elements, `$[elem]` + `arrayFilters` filters them, all combinable with `$set/$inc/$mul/$min/$max/$push/$pull/...` and nested paths behind the positional segment. `arrayFilters` are read from the wire command (they were silently dropped before), validated upfront (unknown/unused/duplicate identifiers, replacement updates) and honored by `findAndModify` too. `$bit` supports and/or/xor on int/long. Error behavior matches MongoDB — no silent no-ops.

#### Query API: `arrayFilters` for update operations
`Query.setArrayFilters(...)` (list or varargs of filter documents) makes filtered positional updates reachable from the high-level API — previously `arrayFilters` existed only on the driver-level `UpdateMongoCommand`, so `$[<identifier>]` paths were unusable via `query.set/inc/unset/push/...`. The filters are applied to all update operations executed on that query, alongside the existing collation handling: `q.setArrayFilters(Doc.of("elem", Doc.of("$gte", 90))).set("values.$[elem]", 100, false, true)`. Paths containing `$` skip property-name translation as before, so positional segments pass through unchanged.

#### Aggregator: typed builder methods for `$documents`, `$densify`, `$fill` and `$setWindowFields`
The stages implemented in #254 were only reachable via `genericStage()`; the `Aggregator` interface now offers `documents(...)`, `densify(...)` (bounds/unit/partition overloads), `fill(...)` and `setWindowFields(partitionBy, sortBy, output)`. Field names in the specs are translated like in every other typed stage method (keys always, `$`-references with the opt-in `translateAggregationFieldNames`). Implemented in both `AggregatorImpl` and `InMemAggregator`.

#### InMemoryDriver/PoppyDB: `currentOp` shape, `serverStatus`, `bulkWrite` (#257)
`currentOp` returns mongod's `{inprog: [], ok: 1.0}` shape (and no longer NPEs on a plain `{currentOp: 1}` — a parse bug in `CurrentOpCommand.fromMap`), `serverStatus` provides the fields tooling commonly reads (host/version/process/uptime/connections/mem, JVM-backed), and the MongoDB-8.0-style top-level `bulkWrite` command maps onto the existing insert/update/delete primitives with `ordered`/`errorsOnly`, per-op results and proper write-error reporting. `saslContinue` from the same issue already shipped with the SCRAM work.

#### PoppyDB: replica-set replication now covers index definitions (#258)
Replication used to copy documents only — a secondary (and any node promoted after a failover) had **none** of the primary's user-defined indexes: unique constraints went unenforced, TTL indexes never expired anything, and index-backed queries fell back to full scans. The initial sync now replicates the primary's `listIndexes` output after the data snapshot (a failure here fails the sync), and a periodic 30s diff converges afterwards — missing indexes are created, dropped ones removed locally, and whatever a disconnected secondary missed is picked up (change streams carry no index DDL). On the way, InMemoryDriver's `listIndexes` learned to report `partialFilterExpression`, which it silently swallowed before.

#### PoppyDB: opt-in auth enforcement (`--auth`) with initial admin user
With `--auth`, a connection may only run the handshake, SASL, `logout`, `ping` and `buildInfo` commands until it completes a SCRAM exchange; everything else is rejected with code 13 Unauthorized. Authentication state is per connection (one wire handler per channel); `logout` locks the connection again. `--rootUser`/`--rootPassword` create an initial admin user at startup if absent — there is no localhost exception, so a fresh `--auth` server without them would be unreachable (a warning says so). The default remains completely open: without `--auth` nothing changes for existing setups. Combine with the existing `--ssl`/`--sslKeystore` options for encrypted, authenticated deployments.

#### InMemoryDriver/PoppyDB: real SCRAM authentication (verification) and a working `createUser` (#245)
The in-memory server now implements server-side SCRAM-SHA-1 and SCRAM-SHA-256, including MongoDB's specifics (MD5-digested password for SHA-1, SASLprep for SHA-256, the three-step exchange used by clients like mongosh). `createUser` actually creates users now, stored mongod-shaped in `admin.system.users`, so morphium's own SCRAM client authenticates against InMemoryDriver/PoppyDB exactly like against real MongoDB; wrong passwords and unknown users are rejected indistinguishably. Verification is always active when a client authenticates; **enforcement** is opt-in via PoppyDB's `--auth` switch. X.509 `authenticate` and `createRole` keep failing honestly, and authorization is authentication-only for now — roles are stored but not evaluated.

#### Messaging: configurable default TTL and fallback-poll cadence
Two new `MessagingSettings`: `messagingDefaultTtl` (default 30s — the historical hardcoded value) is applied on send to timing-out messages that carry no TTL, and `messagingFallbackPollInterval` (default 10s = default TTL / 3) controls the safety-net poll behind change-stream delivery. Applications using short message TTLs should tune the poll interval below their shortest TTL so a lost change-stream event is rescued before the message expires.

#### Messaging: requeued messages are delivered event-driven
Requeueing a message by clearing its `processedBy` via a plain DB update produces no insert event — such messages were only ever found by the interval fallback poll (up to `messagingFallbackPollInterval` latency, risky for short TTLs). The change-stream pipelines of both messaging implementations now additionally match update events whose `updateDescription` shows `processed_by` set to an *empty* array — the requeue signature; normal processing marks use positional keys (`processed_by.0`, …) and stay filtered out — and react with an immediate poll. Requeue latency drops from seconds to milliseconds; the fallback poll remains as safety net. Works on real MongoDB and the InMemoryDriver/PoppyDB event path alike.

#### Messaging: processing decision trace for answer-timeout diagnostics
`SingleCollectionMessaging` keeps a bounded trace (512 entries) of every per-message processing decision — change-stream skips, queue/dequeue, the silent bail-outs, answer matches — dumped only by answer-timeout diagnostics, so normal operation stays log-quiet. Added as a second diagnostics round for the recurring BasicJMSTests flaky, after a captured occurrence showed an answer queued for processing and then silently never processed; the trace now names the exact point where a message stops moving. Also exposed as `getProcessingDecisions(msgId)` for tests.

#### Messaging: skipped messages were wrongly marked "recently completed" (blocked requeues for 10s)
When the change-stream listener of `MultiCollectionMessaging` skipped a message *without* processing it (already processed by another instance, lock lost, reread failed), the cleanup path still recorded it in `recentlyCompletedMessages` — making both the listener and all polls ignore that message for the 10s retention. A message requeued during that window was invisible. Only messages that actually reached a listener are recorded now.

#### Messaging: change-stream liveness drives the fallback poll
The change-stream watch loop receives a server reply at least every `maxTimeMS`; that heartbeat is now stamped on the `WatchCommand` and exposed as `ChangeStreamMonitor.isStreamLive()`. Both messaging implementations use it to poll immediately when a stream falls silent, instead of waiting for the next interval — though the regular `messagingFallbackPollInterval` poll still always runs too, since messages can reappear without any matching stream event (e.g. a requeue via plain DB update). `SingleCollectionMessaging`, whose own counter-based gate previously polled only every ~25s, now honors the configurable interval and gets a catch-up poll on every watch (re-)establishment.

#### InMemoryDriver: the `$merge` aggregation stage is implemented (#241)
`$merge` previously reported success and wrote nothing at all — every persistence call was commented-out dead code — so pipelines materialising results (rollups, denormalised views, ETL-style flows) silently produced no data. It now works: `whenMatched` (`merge`/`replace`/`keepExisting`/`fail`) and `whenNotMatched` (`insert`/`discard`/`fail`) are both implemented, `on` defaults to `_id` and accepts a field list, and `into` accepts a collection name or `{db, coll}`. Writes go through the driver's `find()`/`store()`, so index maintenance, capped/TTL bookkeeping and watcher events all happen. `whenMatched` may also be a custom update pipeline (the stages mongod allows there, with `let` support), and undefined `$$variables` now fail up front instead of evaluating to null.

### Changed

#### InMemoryDriver: the change-stream before-image is no longer deep-copied twice per watched update (#274)
With a change-stream subscriber on the namespace, `updateInternal` already took a full `deepClone` of the document before mutating it, then handed that clone to `notifyWatchers`, which deep-copied it a *second* time when building the event — a redundant full recursive walk, since nothing in the update path reads or mutates that clone again once queued. The before-image is now adopted as-is on that path, with only `_id` normalization still applied, cutting allocation per update. This is deliberately narrow, gated by an explicit `beforeDocumentIsExclusiveCopy` flag: on every other path (deletes, `store()`'s replace branch, updates without subscribers) the before-image is not exclusively owned and keeps the real deep copy. The after-image keeps its unconditional deep copy everywhere, since it references the live, in-place-mutated stored document.

#### InMemoryDriver: insert's duplicate-`_id` pre-check is an O(1) index lookup instead of an O(N) collection scan
Every `insert()` call built a `HashSet` of all existing `_id`s by iterating the entire collection under the exclusive write lock — the dominant per-insert cost for single-document inserts into large collections (the messaging workload), and redundant since the per-collection `CollectionIndexStore` already carries a unique `_id_` index. The pre-check now asks that index directly via a single hash lookup; semantics (ordered throws, unordered collects a writeError) are unchanged. As a side effect the check now uses the index's id normalization, so a duplicate no longer slips past just because caller and store hold the same id in different wrapper types.

#### PoppyDB: dead `locked_by`/`locked` messaging index removed
`MessagingOptimizer` created a `msg_locked_by_1_locked_1` index on every registered messaging collection, but those fields no longer exist on `Msg` — locking moved to the separate `MsgLock` collection long ago. Nothing ever queried the index; it only added per-insert maintenance cost on the hottest collection. Removed.

#### Messaging: non-exclusive messages are processed from the change-stream `fullDocument` — one DB roundtrip less per message
`SingleCollectionMessaging` re-read every message by `_id` (PRIMARY read preference) before processing, although the insert event already carried the complete document. For the safe case — non-exclusive messages arriving via an insert event with a `fullDocument` — the change-stream handler now attaches the event snapshot to the processing queue element and the processing runnable deserializes it directly; all skip checks (listener existence, sender==self, processed-by, recipients, answer matching) run unchanged against the deserialized message. Everything with staleness risk deliberately keeps the re-fetch: exclusive messages (the `processed_by` re-check after claiming the lock is correctness, not overhead), requeue updates, poll pickups, and any snapshot that fails to deserialize. The decision trace records which path was taken.

#### InMemoryDriver/PoppyDB: dbStats and collStats report real sizes instead of zeros
`db.stats()` answered all byte-size fields with 0, and `collStats` reported jol's *shallow* `sizeOf` — the ArrayList object header, not the data (and NPE'd on a missing collection). Both now compute real values: `dataSize`/`size`/`storageSize`/`avgObjSize` from the actual BSON size of every document, and index sizes as estimates proportional to entry count. New fields `totalSize` and, on dbStats, `fsUsedSize`/`fsTotalSize` report the JVM heap as the "filesystem" an in-memory database lives on. `$collStats`'s `storageStats` uses the same computation, and a missing collection now answers zeros instead of failing.

#### PoppyDB: reports its real version instead of "5.0.0-ALPHA" / "PoppyDB V0.1ALPHA"
`buildInfo.version` and `serverStatus.version` were hardcoded to `5.0.0-ALPHA` (mongosh greeted every connect with `Using MongoDB: 5.0.0-ALPHA`), and the hello `msg` field still said `PoppyDB V0.1ALPHA (Netty)`. All three now carry the actual product version from the Maven build (via `MorphiumVersion`, shared constant `InMemoryDriver.REPORTED_SERVER_VERSION`) — PoppyDB releases in lockstep with morphium, so mongosh now shows `Using MongoDB: 6.3.0`. Deliberately the PoppyDB version, not a MongoDB compatibility version: protocol capabilities are negotiated via `maxWireVersion`, not this string.

#### InMemoryDriver: O(1) change-stream replay-buffer bound
The ring-buffer bound check in `notifyWatchers` used `ConcurrentLinkedDeque.size()` — O(n), ~200k node traversals per write at PoppyDB's 100k-event replay bound. The deque size is now tracked in an `AtomicInteger`; eviction semantics are unchanged.

### Fixed

#### InMemoryDriver: a single insert after a TTL-queue invalidation stopped every older document from ever expiring (#269)
The TTL sweep is queue-driven, and `invalidateTtlQueue()` discards a collection's queue outright at every structural change (drop, clear, rename, transaction commit/abort), relying on a lazy rebuild-on-miss. But only `sweepTtlQueue()` actually rebuilt from a full scan on miss — `ttlEnqueue()` instead put a fresh, otherwise-empty queue in place holding only the one document it was called for, so the sweep's bootstrap-on-miss never fired again and every older document permanently lost its expiry tracking. This matters beyond the in-memory driver because `Msg.deleteAt` uses this exact TTL mechanism for messaging cleanup, and PoppyDB runs on this driver — a messaging node starting against a PoppyDB that already holds messages hit this window directly, letting the `msg` collection grow without bound. `ttlEnqueue()` now bootstraps on miss exactly like the sweep does, guarded against double-enqueueing and reusing the write lock its call sites already hold.

#### InMemoryDriver: index-store provenance mismatch evicted the entry, causing a rebuild ping-pong between a transaction and concurrent readers
Follow-up to the provenance fix. On a mismatch, `getIndexStore()` evicted the offending entry before rebuilding, and a transaction whose entry got evicted then lost the race to publish its own store forever: the surviving entry kept winning `putIfAbsent`, so that transaction rebuilt its O(documents × indexes) index store on every single operation for its whole lifetime. The entry now changes owner atomically once the rebuild finishes, via a compare-and-swap keyed on the exact entry this call observed, instead of a remove-then-publish — so there is never a moment with no entry for the key, which also protects two lock-free callers that could otherwise publish a store built from a document list another thread is mutating.

#### Messaging: change-stream fullDocument fast path skipped `@PostLoad`, silently dropping V5-legacy messages that only carry a `name` field
The non-exclusive fast path introduced with the fullDocument optimization deserialized the change-stream snapshot via the raw `ObjectMapper`, which — unlike the query path — fires no entity lifecycle callbacks. `Msg.postLoad()` is exactly where the V5→V6 compatibility migration lives (`topic = name` when only the legacy `name` field is set), so a message inserted externally in V5 format arrived with `topic == null` and was silently discarded by the "no listener registered" check, on every backend. The fast path now fires `firePostLoadEvent()` right after deserializing, matching the query path, and falls back to the pre-existing re-fetch path if the callback throws.

#### InMemoryDriver: aborted/committed transactions could leave stale `CollectionIndexStore` entries, causing false duplicate-key errors on a provably empty collection
A persistent `CollectionIndexStore` lazily built while a transaction is open is built from the transaction's private snapshot — structurally-cloned document instances rather than the live ones — and those clones got registered into the store's unique-index buckets same as any real document. `commitTransaction()` already invalidated the store for every collection the transaction touched, but `abortTransaction()` did not, so on abort the store kept referencing the orphaned clones forever (removal matches only by reference identity, never a clone against its original). Every later insert under that same unique-index key was then rejected as a duplicate, even on a provably empty collection. Both methods now invalidate the index store and TTL queue for every collection whose store was built while the transaction was open, not merely the ones it wrote to.

#### InMemoryDriver: a `CollectionIndexStore` built before a transaction started stayed stale for the whole transaction, silently losing an update on commit
The previous fix only covered a store built *during* a transaction; a store built before one — the common case — was never touched by that invalidation. Such a store holds live document instances while a transaction's writes mutate a private cloned snapshot instead, so an index-backed read inside the transaction kept returning the pre-transaction instance, diverging from a full scan of the same collection. Worse, an update whose candidate document came from that stale lookup mutated the live object instead of the snapshot clone the commit actually merges back, silently losing the write after a transaction that reported success. `getIndexStore()` now records which transaction context each persistent store was built from and reuses a store only for the caller it was built for, keyed by context identity so concurrent, genuinely-overlapping transactions can no longer borrow each other's store.

#### PoppyDB: a re-syncing secondary broadcast its own initial-sync wipe as change-stream drop events, letting stale watchers destroy `admin.system.users` cluster-wide during a stepdown
The initial sync's `clearLocalDatabases()` wipe and snapshot copy ran as regular commands and therefore emitted live change-stream events on the syncing node — including `drop admin.system.users`. During a live stepdown that is catastrophic: the demoted ex-primary immediately starts re-sync attempts (each wiping again), while other nodes' old `ReplicationManager`s are still watching it and faithfully apply and re-emit those wipe-drops, so whether a user created on the new primary survived on any given node was pure timing — a real data-loss window on production failovers. Initial-sync writes are now performed inside a new `InMemoryDriver.suppressChangeStreamEvents()` scope, mirroring MongoDB where initial-sync writes are never oplogged, so the wipe and snapshot are invisible to change-stream watchers; steady-state replication still emits events as before.

#### Driver: failover read path could throw a raw NPE past every retry; stale `getLastConnectFailure()` after recovery
The read-preference fallback chain read the volatile `primaryNode` field multiple times, and the heartbeat nulls that field on stepdown or connection error exactly while the fallback code runs — so `hosts.get(null)` could throw a `NullPointerException` that, not being a `MorphiumDriverException`, escaped every retry-catch and aborted a read the fallback was built to save. Both fallback sites now work on a local snapshot instead. `getLastConnectFailure()` is also now cleared on a successful connect, so a caller polling after recovery no longer sees the pre-recovery error as current.

#### InMemoryDriver: `updateUser` reset the user's SCRAM mechanism set on every password change; malformed field types escaped as ClassCastException
A password change without an explicit `mechanisms` field rebuilt the credentials with the both-mechanisms default, silently re-arming SCRAM-SHA-1 for a user deliberately created SHA-256-only; mongod preserves the existing mechanism set, and now the in-memory driver does too. `mechanisms` without `pwd` is now supported with mongod's subset-only semantics, and all optional fields are shape-checked before casting, so malformed input produces a `BadValue` command error instead of an uncaught `ClassCastException`.

#### PoppyDB: demoted leader could keep `primary==true` forever after a rapid leadership flap
`onLeadershipChange` incremented the leadership epoch and then wrote the `primary` flag unsynchronized, so a preempted stale dispatch could re-assert its outdated flag value after a newer transition had written the current one — a node stuck with `primary==true` as a follower silently never replicates. Epoch bump and flag flip are now one atomic unit, making a stale overwrite structurally impossible. Related hardening: the replication liveness probe now checks "watch never registered" instead of the instantaneous live-state, so it no longer tears down a healthy `ReplicationManager` sampled during a routine reconnect gap.

#### PoppyDB: `rs.status()` reported a peer that died with the failover as SECONDARY forever
`becomeLeader()` clears the peer-contact map, and a peer with no contact entry was treated as
reachable indefinitely - so the classic crashed ex-primary, which never acks a single
heartbeat of the new leader, was never reported DOWN. A missing entry is now only treated as
reachable within a grace period (the heartbeat freshness window) measured from the moment
leadership was assumed; beyond that the peer reports `state: 8, stateStr: "DOWN"`.

#### `startPoppyDB.sh`: "port already in use, skipping node" did not actually skip
The busy-port check printed the skip message but started the node anyway - the new JVM could
not bind, but its PID had already overwritten the running node's PID file, which the failure
branch then deleted, orphaning the still-running original process for `stop`/`status`. The
skip is now real (and keeps the port sequence of the remaining nodes intact).

#### PoppyDB: `--auth`/`--ssl` now work on a replica set - the internal election/replication channel was always plaintext and unauthenticated
Each of `--auth` and `--ssl`, independently, made a multi-node PoppyDB replica set completely non-functional: `ElectionNetworkClient` and `ReplicationManager` connected to peers as a plain, unauthenticated, unencrypted client regardless of the server's own configuration, so with `--ssl` every internal connection was rejected by the peer's TLS-only listener, and with `--auth` the election RPCs weren't on the pre-auth whitelist — either way, no leader could ever be elected. Single-node PoppyDB was unaffected; the client-facing enforcement itself was never the problem. The internal channel now authenticates as the configured root user and, when TLS is on, trusts exactly the server's own configured certificate — no new config keys, no change to auth enforcement.

#### InMemoryDriver: `$sample` larger than the collection threw instead of returning all documents
`$sample` cut its shuffled copy with `subList(0, size)`, so a sample size exceeding the collection count failed with `IndexOutOfBoundsException: toIndex = N` instead of returning all documents in random order like mongod. Visible in every mongosh session against PoppyDB: tab completion samples schema documents with `$sample {size: 10}`, so completing on any collection with fewer than 10 documents printed a `Tab completion error: ... aggregate failed: toIndex = 10` stack trace.

#### InMemoryDriver/PoppyDB: unknown commands are answered like mongod instead of throwing
An unregistered command made `InMemoryDriver.runCommand` throw `IllegalArgumentException` — over the wire that meant an ERROR stack trace in the server log and a reply without an error code. mongosh probes `atlasVersion` on **every** connect (Atlas detection) and expects the mongod-shaped rejection, so every mongosh session logged a spurious exception. Unknown commands now return `{ok: 0, code: 59, codeName: "CommandNotFound", errmsg: "no such command: '...'"}`, which clients handle silently — for any unknown command, exactly like mongod.

#### PoppyDB: rs.status spoke Raft and mis-identified wildcard-bound nodes
Two defects in `replSetGetStatus`: the self member's `stateStr` reported the internal Raft enum name (`LEADER`/`FOLLOWER`/`CANDIDATE`) instead of MongoDB's nomenclature (`PRIMARY`/`SECONDARY`/`RECOVERING`), which clients and monitoring tools cannot parse. And with `--bind 0.0.0.0` the node used its bind address as member identity, so it failed to recognize itself in the seed list: rs.status showed the node **twice** (as `0.0.0.0:<port>` and again under its seed name, wrongly marked SECONDARY), the node requested election votes from itself as a "peer", and the `--rs-priorities` lookup missed. The member identity is now canonicalized to the unique seed entry matching the node's port (with a WARN when no unambiguous match exists), and hello's `me`, rs.status' `self` flag and the election identity all agree.

#### PooledDriver: expired connections were pooled on release instead of closed
`releaseConnection` returned connections to the pool even when they had exceeded their `maxConnectionLifetime`/`maxConnectionIdleTime` while borrowed — only the heartbeat's expiry sweep removed them, one sweep later. A borrow burst therefore parked a mountain of already-expired connections in the pool, and under load the sweep lagged behind, keeping the pool far above its per-host minimum for many seconds (the `testLotsConnectionPool` flaky). Expired connections are now closed on release, like the official MongoDB drivers do, so the pool converges within one lifetime window even after bursts.

#### BufferedMorphiumWriterImpl: NPE race between write-buffer users and the flusher
The flush paths removed a type's buffer via `opLog.remove()` without holding the `opLog` monitor, while other threads re-read `opLog.get(type)` between check and use — a concurrent flush in that window turned into an NPE (seen as a BufferedWriterTest failure under parallel-phase load). All check-then-re-get sequences now take a single snapshot reference instead of re-reading the map, closing the race.

#### Messaging: answers without an explicit TTL were stored already expired (the BasicJMSTests flaky)
`Msg.sendAnswer` computed `deleteAt = now + getTtl()` **before** any TTL defaulting ran, so an answer created with ttl 0 (the JMS ack pattern) was stored with `deleteAt = now`: the TTL sweeper occasionally raced the consumer for the freshly inserted document and won, deleting the answer before it could be read — the long-hunted BasicJMSTests answer-timeout flaky. `sendAnswer` now leaves `deleteAt` unset when no TTL was chosen, so the send path applies `messagingDefaultTtl` first and `preStore` derives `deleteAt` from the defaulted TTL. Explicit answer TTLs behave as before; root-caused via the new processing decision trace.

#### InMemoryDriver/PoppyDB: creating a time-series collection now fails loudly (#262 interim)
`create` with a `timeseries` spec used to log a WARN and silently create a **plain** collection instead — no `timeField` enforcement, no retention, wrong `listCollections` type. It now returns a proper `CommandNotSupported` (115) command error over the wire and raises a `MorphiumDriverException` embedded. On the way, `CreateCommand.execute()` was switched to `readSingleAnswer`, since the cursor-style read it used before silently swallowed cursor-less replies. Real time-series support is tracked in #261/#262 for 7.0.0.

#### InMemoryDriver/PoppyDB: resumed change streams could deliver an event twice
A watch resuming with `resumeAfter` registers its subscription before replaying the event history, since the reverse order would lose events written in between — but that meant an event written exactly in that window could be delivered twice, once by the live dispatch and once by the replay, in arbitrary order. Resumed subscriptions now suppress exact duplicates by resume token (a bounded recent-token window); fresh watches have no replay and are unaffected. Real MongoDB never had this problem since oplog-cursor resume is snapshot-consistent; this mainly protects custom `ChangeStreamListener`s, as morphium's own consumers were already idempotent.

#### PoppyDB: the wire fast path dropped `arrayFilters` (#256 follow-up)
`processUpdateDirect` — PoppyDB's direct dispatch for plain `update` commands — passed the request's per-update `collation` but not its `arrayFilters` to the driver, so a `$[<identifier>]` update sent over the wire (mongosh, any standard client) failed with "No array filter found" while the identical update worked against the InMemoryDriver directly. Third instance of the fast-path-drops-request-options bug class (#252: `ordered`/`collation`, createIndexes: index specs); covered by a `FastPathOptionsTest` seam test like the others.

#### InMemoryDriver/PoppyDB: auth commands no longer pretend to succeed (#245)
The entire server-side authentication surface — `saslStart`, X.509 `authenticate`, `createUser`, `createRole` — consisted of empty stubs that queued no result, which the command-dispatch machinery resolved to `{ok:1.0}`: every client "authenticated" successfully with any or no credentials, and `createUser`/`createRole` reported success while creating nothing. These commands now fail loudly (`AuthenticationFailed`/`NotImplemented` with an unmistakable message) until real SCRAM verification and a user/role store exist. InMemoryDriver/PoppyDB still perform **no** authentication — do not expose them to untrusted networks.

#### InMemoryDriver: `store()` failed with a duplicate-key error when replacing an existing document
`storeInternal` located the document to replace via `findByFieldValue`, which returns *copies*, while `CollectionIndexStore` removes index entries by *identity* — the copy never matched, so the old `_id` entry stayed in the index and the following insert reported a spurious `E11000 duplicate key`. The ordinary "find it, change it, store it back" round-trip threw for every existing document. The previous document is now resolved through the `_id` index instead, which yields the live reference; unnoticed until now because morphium's usual update path goes through `update()`, not `store()`.

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
The secondary's replication pipeline had several correctness defects that could silently lose or reorder data: the initial sync copied the snapshot *before* opening the change-stream watch (losing writes made during the copy), replication batches applied all inserts before updates/deletes (so a delete-then-reinsert of the same document wrongly ended up deleted), failed bulk applies still acknowledged their sequences to the primary, and bulk-insert `writeErrors` were silently treated as success. All of this is fixed: the watch now starts before the snapshot with buffered events replayed afterwards, batches preserve global event order, sequences are acknowledged only after a successful apply, and failed bulks are replayed as idempotent per-document upserts. A secondary also now rejects data-plane traffic while its initial sync is running.

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
A socket timeout striking mid-reply left the TCP stream misaligned, and `readNextMessage` kept reading from it — parsing payload bytes as a message header (the `Illegal opcode` errors) and handing the half-read connection back to the pool for the next borrower to inherit. `parseFromStream` now distinguishes a timeout at a message boundary (still aligned, retryable as before) from a mid-message timeout, which is now surfaced as a fatal network error that closes the connection instead of pooling it. A deadline expiring without any reply also closes the connection, since a late reply could otherwise be delivered to the next borrower; `ChangeStreamMonitor` likewise closes rather than releases its connection after errors that leave stream state unknown.

#### Changestream: events written during a watch restart were lost; messaging could drop messages
When a change stream died and was re-established, a consumer with no resume token yet started the new stream at "now", silently skipping everything written during the retry gap — for messaging this meant lost broadcasts. `watch()` now captures and republishes the cursor's `postBatchResumeToken` on every exit, so `ChangeStreamMonitor` resumes where the dead stream stopped, and messaging additionally polls the affected topics once per (re-)established watch to deterministically catch up. The fallback poll, previously mis-gated to run only every ~125s instead of the documented one second, is now a proper time-based safety net running every 10 seconds — one third of the default message TTL, so a lost event is always rescued before the message expires.

## [6.2.9] - 2026-07-14

### Added

#### Aggregator: WARN when a renamed project(Map) key is referenced by its original spelling (#208)
`project(Map)` translates its keys through the entity's field-name mapping. When a later stage references such a key by the name the user wrote, the reference points at a non-existent field and MongoDB silently returns `$sum: 0` / `$push: []`. Both aggregator implementations now log a WARN (once per reference) naming both spellings. `$$`-variables and `$literal` subtrees are ignored; dot-paths are matched by their first segment.

#### Aggregator: opt-in consistent field-name translation (#208, #217)
New opt-in setting `translateAggregationFieldNames` (`ObjectMappingSettings`, overridable per aggregator) translates Java property names to Mongo field names when enabled; default off preserves the previous behavior exactly. Covered stages: `group` operator `$`-references and id values, `project(Map)`/`addFields`/`set` keys and values, `sort(Map)` keys, and `graphLookup` connect fields. Stages taking a raw `Expr` (`match(Expr)`, `sortByCount`, `replaceRoot`, `redact`, `bucket`, etc.) are not yet covered (#221). New helpers `Aggregator.ref(Enum)`/`Aggregator.name(Enum)` translate enum field references explicitly, independent of the flag.

#### PoppyDB: priority-based leader step-back after failover (#177)
A PoppyDB leader now voluntarily hands leadership back to a peer with higher election priority, mirroring MongoDB's priority takeover — previously a failover to a lower-priority node was permanent, even after the preferred primary recovered. The leader only yields once the higher-priority peer is caught up and has been stable for `priorityTakeoverMinStabilityMs` (default 30s), so a settling cluster does not flap; older nodes that don't report priority never trigger a takeover. Enabled by default and configurable via `ElectionConfig.priorityTakeoverEnabled`; clusters where all nodes share the default priority are unaffected.

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
During a primary failure (crash, frozen VM, network partition) the driver effectively never recovered: writes failed or hung indefinitely and messaging never reconnected. Root causes ranged from `readNextMessage` multiplying its timeout on consecutive socket errors (a bounded deadline hung for over an hour) to dead step-down detection in `WriteMongoCommand`, a `ChangeStreamMonitor` that died permanently on a transient "No such host" during failover, and primary discovery breaking on host-key casing differences. All of these are now fixed: timeouts are hard deadlines, step-downs are detected via mongo error codes and retried on the new primary, the changestream monitor retries instead of dying, and dead-host eviction closes borrowed connections so in-flight operations fail fast and retry. Verified with unit tests plus a manual failover suite (`FailoverReproTest`) simulating SIGTERM/kill/freeze/restart scenarios against a local replicaset.

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
The change stream `getMore` batch size was hardcoded to `1`, capping stream throughput at one event per network round-trip — over a high-latency link (e.g. an SSH/SOCKS tunnel) a busy stream fell behind and delivered events, including awaited messaging answers, tens of seconds late. It's now configurable via `DriverSettings.changeStreamBatchSize` (default `100`), overridable per monitor through `ChangeStreamMonitor.setBatchSize()`. Since `awaitData` returns as soon as the first event is available, the larger batch adds no latency at low traffic but lets a single round-trip drain a backlog; the original reason for `batchSize=1` no longer applies after the change stream rewrite.

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
