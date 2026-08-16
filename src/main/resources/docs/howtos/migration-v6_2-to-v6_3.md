# Migration v6.2.x → v6.3.0

This guide covers breaking changes, deprecations, and the headline new features when upgrading
from Morphium 6.2.x to 6.3.0. 6.3.0 is a large release, dominated by InMemoryDriver/PoppyDB
correctness and production-readiness work; if you only use Morphium against real MongoDB and
never touch the embedded driver or PoppyDB, most of this guide does not apply to you — skip to
[Breaking Changes That Affect Real MongoDB Users](#breaking-changes-that-affect-real-mongodb-users),
[New: DualChannelMessaging](#new-dualchannelmessaging-beta) and
[New: Messaging improvements](#new-messaging-improvements-all-implementations).

If you use the standalone `io.quarkiverse.morphium:quarkus-morphium` artifact, read
[New: Optional Extension Modules](#new-optional-extension-modules-morphium-jakarta-data-quarkus-morphium) —
its Maven coordinates changed.

No dependency version bumps in this release (Netty/BSON/SLF4J/Logback are unchanged from 6.2.10).

## Breaking Changes That Affect Real MongoDB Users

### Mid-message read timeouts now close the connection instead of silently reusing it

*(Shipped in 6.2.10 — skip if you are coming from that patch release.)*

A socket timeout that struck mid-reply (header consumed, body still in flight) used to leave the
connection desynchronized but still pooled — the next borrower would see cryptic `Illegal opcode`
errors or a `null` reply. The driver now detects this case and closes the connection instead of
retrying/pooling it. **What to change:** nothing in your code, but if your logs previously showed
sporadic `Illegal opcode ...` errors under load, they should disappear; if you see more connection
churn than before under timeout-heavy conditions, that is this fix working as intended, not a
regression.

### Expired pooled connections are closed on release, not returned to the pool

Previously only the heartbeat's expiry sweep removed connections past
`maxConnectionLifetime`/`maxConnectionIdleTime`; a connection could sit in the pool one sweep
interval past its expiry. No API change, but expect pool size to converge to its minimum faster
after a connection burst.

### `PooledDriver` re-seeds from the host list after a full outage (#233)

If every replica-set member became unreachable for long enough, the driver previously stayed
permanently dead (`No primary node found`) even after the cluster recovered, requiring an
application restart. It now re-seeds from the configured host list and resumes discovery on its
own. No action needed — this only removes a failure mode.

### The driver adopts the server's real wire limits, and oversized write batches are split

`PooledDriver` ignored the `hello` handshake's `maxMessageSizeBytes`, `maxWriteBatchSize` and
`maxBsonObjectSize` and kept `DriverBase`'s field defaults instead (a 16MB message bound, batch
size 1000, and a `12*1025*1024` typo for the BSON limit) — only `SingleMongoConnectDriver` adopted
the advertised values. All drivers adopt them now (defaults are MongoDB's real 48MB/100000/16MB),
and a write command whose payload would exceed the message bound is cut into sub-batches
(`WriteBatchSplitter`) instead of going out as one huge `OP_MSG` that any real server answers by
closing the connection. **What to change:** nothing. A very large `insert`/`update`/`delete` batch
may now be executed as several wire messages; the results are folded back into one mongod-shaped
answer (counters summed, `writeErrors`/`upserted` indices remapped to your original statement
positions), and an *ordered* batch still stops at the first sub-batch that reports write errors.

### Change-stream restarts resume where the dead stream stopped — and the messaging fallback poll really runs

*(Shipped in 6.2.10 — skip if you are coming from that patch release.)*

A change stream that died before its consumer had received any event had no resume token, so the
re-established stream started at "now" and everything written during the retry gap was silently
skipped — for messaging that meant lost messages. `watch()` now captures the cursor's
`postBatchResumeToken` (which MongoDB sends in every reply, including empty batches) and publishes
the freshest token on the `WatchCommand`, so `ChangeStreamMonitor` resumes from it. Messaging
additionally does one catch-up poll every time a watch is (re-)established. Related: the messaging
fallback poll was documented as running every second but was effectively gated to roughly every
125 seconds by a tick counter; it is time-based now and defaults to 10s.

**What to change:** nothing, but expect a slightly higher steady-state query rate per messaging
instance than in 6.2.x, since the safety-net poll now actually fires at its configured interval.
Tune with `cfg.messagingSettings().setMessagingFallbackPollInterval(...)`.

### Answers sent without an explicit TTL are no longer stored already expired

`Msg.sendAnswer` computed `deleteAt = now + getTtl()` *before* any TTL defaulting ran, so an answer
built with a plain `new Msg()`/`new JMSMessage()` (ttl 0 — the JMS ack pattern) was written with
`deleteAt = now` and could be deleted by the TTL sweeper between its change-stream event and the
consumer's read (roughly 1–5% of runs — the long-hunted answer-timeout flakiness). `sendAnswer`
now leaves `deleteAt` unset when no TTL was chosen, so the send path applies `messagingDefaultTtl`
(30s) first. Explicit answer TTLs behave exactly as before. **What to change:** nothing; if you set
an explicit TTL on every answer to work around sporadic answer timeouts, you can drop that.

### Client-side wire compression (snappy/zlib) works

`SingleMongoConnection.sendQuery()` gave the `OP_COMPRESSED` envelope a *fresh* request id while
the reply matcher waited for the inner message's id, so every reply triggered `connection out of
sync`, killed the connection and eventually removed the host from the pool (`No such host`).
Client-side compression is usable now against both MongoDB and PoppyDB; server-side-only
compression was never affected. **What to change:** if you disabled client-side compression as a
workaround, you can turn it back on.

### Smaller behavior changes and additions

- **`getLastConnectFailure()` is cleared when a connect succeeds** — a caller polling it after a
  recovery no longer sees the pre-recovery error as if it were current.
- **The read-preference fallback no longer throws a raw `NullPointerException`** past every retry
  when the heartbeat nulls `primaryNode` exactly while the fallback runs. It works on a local
  snapshot now.
- **The `hello` handshake reports the real Morphium version and driver name.** `driver.version` was
  hardcoded to `"6.2"` and `driver.name` came out as `Morphium V6/unknown` on the connect
  handshake; both are resolved at runtime now (`MorphiumVersion.getVersion()`, also working in
  GraalVM native images), so `db.currentOp()`, server logs and the profiler show the actual patch
  level.
- **New `DriverSettings.appName`** (default `"Morphium"`), sent as `client.application.name` in the
  handshake — set it per service (`cfg.driverSettings().setAppName("order-service")`) to tell
  instances apart in `db.currentOp()` and the server log. MongoDB truncates values over 128 bytes.
  Third-party `MorphiumDriver` implementations keep compiling: the new interface methods are
  `default`s.
- **Subclassed drivers work with generic command dispatch again.** Both `runCommand` and
  `sendCommand` resolved their handler method via `getClass().getDeclaredMethod(...)`, which fails
  for a subclass; the lookup is now anchored on the declaring driver class. Only relevant if you
  extend `PooledDriver`/`SingleMongoConnectDriver`/`InMemoryDriver`.
- **`BufferedMorphiumWriterImpl` no longer NPEs** when the flusher removes a type's buffer while
  another thread is between check and use (including the `WRITE_OLD`/`DEL_OLD` buffer-full
  strategies).
- **`MultiCollectionMessaging` no longer marks skipped messages as "recently completed".** A
  message the change-stream listener skipped *without* processing it (already processed elsewhere,
  lock lost, reread failed) was recorded in `recentlyCompletedMessages` anyway, so a requeue within
  the 10s retention window was invisible to both the listener and every poll. Only messages that
  actually reached a listener are recorded now.

## Breaking Changes in InMemoryDriver / PoppyDB

These only affect you if you run tests against the InMemoryDriver (`-Dmorphium.driver=inmem`) or
run PoppyDB as a MongoDB-compatible server. All of them make the emulation *more* correct — i.e.
closer to how real MongoDB always behaved — so if you only run tests against real MongoDB, none of
this applies; if you run the same tests against InMemoryDriver/PoppyDB too, some may start failing
because they were relying on previously-wrong lenient behavior.

- **`$in`/`$nin` reject scalar and `null` operands** instead of silently wrapping them into a
  single-element list (`{$in: "a"}` no longer behaves like `{$in: ["a"]}`) — this also reverts the
  6.2.9 leniency for the `$in` aggregation expression specifically. Real MongoDB always rejected
  this (`BadValue: $in needs an array`).
- **`$project` inclusion mode (`{field: 1}`) actually restricts output to selected fields now.**
  It was a no-op before — only exclusion (`{field: 0}`) had any effect — so any test asserting a
  full document came back from an inclusion-style `$project` will now correctly see only the
  projected fields.
- **BSON document size limit (16MB) is enforced** on insert and on the *result* of
  update/replace/upsert operations, matching mongod's `BSONObjectTooLarge` (10334). Previously
  unenforced (and advertised as a fantasy 128MB by the embedded driver). Configurable via
  `--max-bson-size` / `PoppyDB.setMaxBsonObjectSize(...)` / `InMemoryDriver.setMaxBsonObjectSize(...)`
  (`0` = off).
- **`maxMessageSizeBytes` (48MB) is respected end-to-end.** Oversized write commands are now split
  into sub-batches instead of being sent as one huge OP_MSG that would previously get the
  connection closed by any real server (or PoppyDB's own decoder).
- **Creating a time-series collection fails loudly** (`CommandNotSupported`, code 115) instead of
  silently creating a plain collection with none of the promised behavior. Real time-series
  support is tracked for 7.0 (#261/#262).
- **Auth commands fail instead of pretending to succeed.** `saslStart`, X.509 `authenticate`,
  `createUser`, `createRole` used to be empty stubs that the dispatch machinery resolved to
  `{ok: 1.0}` — any client "authenticated" with any or no credentials. They now fail explicitly
  until real SCRAM/role support is in place (SCRAM verification and `createUser` landed later in
  6.3.0, see below — `createRole`/authorization enforcement are still not implemented).
- **Memory watermark rejects writes above 90% post-GC heap** by default (`ExceededMemoryLimit`,
  code 146 — a retryable/backpressure signal). If your tests or embedded usage push large volumes
  into an undersized heap, you may now see this error where you previously got an `OutOfMemoryError`
  or degraded performance. Tune with `--memory-warn`/`--memory-reject` (PoppyDB) or
  `setMemoryWatermarks(...)` (embedded); `100` disables the corresponding threshold. Updates,
  deletes, and TTL expiry are always allowed (the drain paths must keep working).
- **Date expression operators evaluate in UTC, and `$month` is 1-based** (#250). All date-component
  operators used the JVM's default timezone, so results depended on the deployment environment.
  Additionally `$month` was 0-based, `$isoWeek` returned the week-of-*month*, `$isoWeekYear` a week
  number instead of a year, `$isoDayOfWeek` used Java's Sunday=1 numbering, and `$week` followed the
  JVM locale's week rules. All of these now match MongoDB — **if you compensated for any of them
  (the classic `+1` on `$month`), remove the workaround.**
- **Several `Expr` operators returned silently wrong values and now compute correctly**: `$asinh`
  computed *sinh*, `$setUnion` collected the arrays instead of their elements, `$ln` computed
  `ln(1+x)`, `$range` returned an empty list for descending ranges, `$reverseArray` mutated its
  source list in place, the single-argument forms of `$avg`/`$max`/`$min` returned an array
  argument unchanged instead of reducing it, and `$dateFromParts` returned its own
  `{"$dateFromParts": {...}}` map instead of a `Date` (#246/#253/#255/#260). Two-argument `$atanh`
  now raises an error instead of silently returning `0`.
- **`$group`'s `$avg` no longer leaks a `$_calc_<field>` key** into every group output document
  (#238) — group results lose a field that was never meant to be there.
- **Unimplemented stages and commands fail instead of quietly doing something else.**
  `$planCacheStats`, `$redact`, `$unionWith`, `$currentOp`, `$listLocalSessions`, `$findAndModyfy`
  and `$update` shared a `switch` body with `$bucket` and silently ran *its* logic (#237);
  `$indexStats` silently ran `$geoNear` (#243). All of them now return "Unrecognized pipeline stage
  name" (40324). Unknown *commands* are answered mongod-shaped with
  `{ok: 0, code: 59, codeName: "CommandNotFound"}` instead of `InMemoryDriver.runCommand` throwing
  `IllegalArgumentException` — **an embedded caller that caught that exception must inspect the
  reply document instead.** `top` answers `CommandNotSupported` (115).
- **`dbStats`/`collStats` report real byte sizes instead of zeros**, and `dbStats` is scoped to the
  requested database instead of returning global counts (#247). Assertions expecting `0` for
  `dataSize`/`storageSize`/`avgObjSize`, or a global collection count from `dbStats`, will fail.
- **PoppyDB reports its real version.** `buildInfo.version`/`serverStatus.version` were hardcoded to
  `5.0.0-ALPHA` and hello's `msg` said `PoppyDB V0.1ALPHA (Netty)`; all three now carry the actual
  product version (`6.3.0`), so mongosh greets you with `Using MongoDB: 6.3.0`. Tooling that gates
  on that string sees a different value — protocol capabilities are still negotiated via
  `maxWireVersion`, which is unchanged.
- **PoppyDB's `rs.status()` speaks MongoDB, not Raft.** The self member's `stateStr` is
  `PRIMARY`/`SECONDARY`/`RECOVERING` instead of the internal `LEADER`/`FOLLOWER`/`CANDIDATE`, and a
  node started with `--bind 0.0.0.0` identifies itself by its seed entry instead of showing up
  twice (once as `0.0.0.0:<port>`, once wrongly marked SECONDARY). Monitoring that parsed the Raft
  names must be updated. A peer that died with a failover is now reported `DOWN` after the
  heartbeat grace period instead of staying `SECONDARY` forever.
- **PoppyDB enforces primary-only writes, `$readPreference`, transaction context and write concern
  on the fast path.** The hot-dispatch handlers (insert/find/update/delete/count/distinct/
  createIndexes) bypassed all of it: a secondary silently accepted writes, and `w`/`wtimeout` were
  ignored for those commands. Both are enforced now — a `w > 1` write actually waits for
  replication (and can now report a `writeConcernError`), and a write sent to a secondary is
  rejected with `NotWritablePrimary`. User management (`createUser`/`updateUser`/`dropUser`) is
  primary-only for the same reason.
- **PoppyDB picks up a configuration file automatically.** In addition to `--cfg`/`-f` and
  `$POPPYDB_CONF`, PoppyDB now reads the first existing of
  `${XDG_CONFIG_HOME:-~/.config}/poppydb/config`, `~/.config/poppydb.conf`, `/etc/poppydb/config`,
  `/etc/poppydb.conf` — so a file left over on a host changes what a server does without any CLI
  change. Pass `--no-config` to skip the four default locations. An unknown key aborts startup with
  a "did you mean" suggestion instead of being ignored.
- **PoppyDB validates its options at startup.** Ranges and cross-option consistency (e.g. `port` in
  range, `memory-warn <= memory-reject`) were unchecked before; an invalid combination now aborts
  startup, reporting all configuration errors at once. Use `--check-config` (exit code 0/1, like
  `nginx -t`) to validate without starting a server.

## Behavior Fixes You Should Know About in InMemoryDriver and PoppyDB

These are bug fixes, not API changes — but each of them changes what the driver *does* with data
you already have, so they are worth reading before you upgrade a running system.

### TTL indexes expire again after a structural change — expect old documents to disappear (#269)

The TTL sweep is queue-driven, and `invalidateTtlQueue()` discards a collection's queue at every
structural change (drop, clear, rename, transaction commit/abort), relying on a lazy
rebuild-on-miss. Only one of the two paths that can find the queue missing actually rebuilt it:
`sweepTtlQueue()` bootstrapped from a full scan, while `ttlEnqueue()` installed a fresh queue
holding nothing but the one document it was called for. That queue was no longer "absent", so the
sweep's bootstrap never fired again and **every document that existed before the invalidation
permanently lost its expiry tracking**.

This is exactly the mechanism Morphium's messaging relies on (`Msg.deleteAt` carries
`@Index(options = "expireAfterSeconds:0")`), and PoppyDB runs on this driver — so a `msg`
collection could grow without bound once the window had opened. Note which operations actually
open it: a transaction commit or abort, `dropIndexes`, and clearing, dropping or renaming a
collection. Creating an index does *not* — `createIndex` bootstraps the queue directly instead of
invalidating it — so simply starting a messaging node against an existing PoppyDB was never
enough on its own.

**What to change:** nothing in your code — but if you have a long-running PoppyDB or embedded
InMemoryDriver instance whose collections grew and never shrank, the first sweep after the upgrade
will expire everything that is past its `expireAfterSeconds` bound. That can be a large, sudden
delete. Check the affected collections before restarting if you are unsure whether those documents
should still be there. Related: `dropIndexes` no longer leaves the TTL sweep registered for a
dropped TTL index (the driver kept deleting documents by an index that no longer existed), and a
renamed collection carries its capped/TTL bookkeeping to the new name (#239).

### Transactions no longer diverge from — or silently lose writes against — the index store

Three related defects in how `CollectionIndexStore` interacts with transactions:

- A store **built during** an open transaction was populated from the transaction's private
  snapshot, i.e. from cloned document instances. `abortTransaction()` did not invalidate it (only
  `commitTransaction()` did), so it kept referencing orphaned clones forever and every later insert
  under the same unique-index key was rejected as a duplicate — **even on a collection that had
  been cleared to zero documents**. Both commit and abort now invalidate the store (and the TTL
  queue) for every collection whose store was built while the transaction was open, not only for
  the ones it wrote to.
- A store **built before** a transaction started holds live document instances, while the
  transaction mutates its private clones. An index-backed equality lookup inside the transaction
  therefore returned the pre-transaction instance (diverging from a full scan of the same
  collection), and an update whose candidate came from that lookup mutated the live object instead
  of the snapshot clone that commit merges back — **the write was silently lost on commit although
  it succeeded without error inside the transaction.** `getIndexStore()` now records which
  transaction context a store was built from and only reuses it for that caller.
- The provenance check originally evicted a mismatching entry, which made a transaction rebuild its
  index store on *every* operation for its whole lifetime (measured: 20 rebuild passes for 20
  operations, 1 with the fix). Ownership now changes via an atomic compare-and-swap instead.

**What to change:** nothing. If you saw spurious `duplicate key` errors or lost updates when using
transactions against the InMemoryDriver/PoppyDB, they are gone.

### PoppyDB replica sets no longer lose data during a stepdown

A re-syncing secondary ran its initial-sync wipe (`clearLocalDatabases()`) and snapshot copy as
regular commands, so they emitted live change-stream events — including `drop admin.system.users`.
During a stepdown the demoted ex-primary starts re-sync attempts immediately while the other nodes'
old `ReplicationManager`s are still watching it, and they faithfully applied those wipe-drops to
their own data; even a freshly promoted primary could apply the demoted node's wipe at promotion
time. Whether a user created on the new primary survived on any given node was pure timing.
Initial-sync writes now run inside `InMemoryDriver.suppressChangeStreamEvents()`, mirroring
MongoDB, where initial-sync writes are never oplogged. Steady-state replication still emits events.

Two more failover fixes in the same area: a demoted leader could keep `primary == true` forever
after a rapid leadership flap (and a node stuck like that silently never replicates), and a demoted
but still-running leader now resumes replication toward the new primary immediately instead of
waiting for an unrelated later leader change.

### Smaller correctness fixes that change results

Re-run your suite against InMemoryDriver/PoppyDB after upgrading — these all used to succeed while
doing the wrong thing:

- **Query operators** (#251): `$size` matched documents whose field is entirely absent, `$all` with
  an empty array matched everything (MongoDB matches nothing), `$all` + `$elemMatch` never matched,
  `$mod` threw a `ClassCastException` on array-valued fields, `$type` ignored the array-of-types
  form, and the bits operators decoded `byte[]` masks backwards. `$geoWithin` with
  `$center`/`$centerSphere`/`$polygon` **matched every document in the collection** (#242).
- **Update operators** (#249): `$pull` with `$elemMatch` never removed anything, `$rename` with a
  dotted source destructively removed the *target* field, `$min`/`$max` threw an NPE on an absent
  field, `$mul` was a no-op on a missing field, `$currentDate` only wrote the first listed field,
  and `$push`'s `$sort` modifier did nothing. `$unset` through array-index path segments
  (`ratings.0.rating`) was a silent no-op and works now.
- **`store()` on an existing document** failed with `E11000 duplicate key` — the ordinary "find it,
  change it, store it back" round-trip threw for every existing document and left the index in an
  inconsistent state.
- **`$sample` with a size larger than the collection** threw `IndexOutOfBoundsException` instead of
  returning all documents (visible in every mongosh tab completion against PoppyDB).
- **`renameCollection` dropped all index definitions** on the renamed collection (#248), and
  `listIndexes` swallowed `partialFilterExpression` — which would have replicated partial indexes
  as full ones.
- **Resumed change streams could deliver an event twice** (and out of order) when it was written
  exactly between subscription registration and history replay. Resumed subscriptions now suppress
  duplicates by resume token. Fresh watches were never affected. Mostly relevant for custom
  `ChangeStreamListener`s — messaging and PoppyDB replication were already idempotent.
- **PoppyDB's wire fast path dropped client options** (#244/#252/#256): `createIndexes` forwarded
  only `unique`/`name` and silently dropped `expireAfterSeconds` (**a TTL index created over the
  wire never expired anything**), `sparse`, `background`, `hidden` and `partialFilterExpression`;
  `insert` hardcoded `ordered=true`; `update`/`delete`/`count`/`distinct` hardcoded `collation` to
  null; and `update` dropped `arrayFilters`, so `$[<identifier>]` updates failed over the wire while
  working embedded.
- **The change-stream event dispatcher no longer uses virtual threads** (#234) — under JDK 21 it
  could pin every carrier thread of the common ForkJoinPool while parked on the logback appender
  lock, freezing every thread that logs (observed as a 20+ minute hang).
- **A duplicate `_id` can no longer slip past the insert pre-check** because caller and store hold
  the same id in different wrapper types (`MorphiumId` vs `ObjectId`) — the check now runs through
  the `_id` index and its normalization. Ordered inserts still throw, unordered ones still collect a
  code-11000 `writeError`.
- **PoppyDB's wire insert fast path no longer labels every driver exception as a duplicate-key
  error (11000)** — typed codes (e.g. `ExceededMemoryLimit` 146) pass through to the client now, so
  error handling that branched on 11000 sees the real code.
- **PoppyDB's `hello` no longer pays a ~30s reverse-DNS lookup** on hosts without working rDNS when
  the replica-set seed list already names the member — a startup/handshake stall, not a data issue,
  but a very visible one.

## Deprecations — the 7.0-removal wave (#218)

Members confirmed for removal in 7.0 now carry `@Deprecated(since = "6.3", forRemoval = true)`,
so IDEs flag every usage a full minor release ahead of time. This is a pure annotation/Javadoc
change — nothing behaves differently in 6.3.0, and everything listed still works. (The annotations
themselves already shipped in 6.2.9; if you upgrade from 6.2.9/6.2.10 your IDE has been flagging
them for a while.) Covered:

- Flat `MorphiumConfig` setters/getters — use the `Settings` sub-objects instead
  (`connectionSettings()`, `objectMappingSettings()`, `messagingSettings()`, ...).
- `MorphiumBase.set…`/`unsetQ…` variants.
- The legacy `SingleCollectionMessaging` constructors — use `Morphium.createMessaging()`.
- `Query.complexQuery`, `Query.getById`, `Query.textSearch`.
- `Msg.name`.
- `MorphiumMessaging.setProcessMultiple` — use `setWindowSize(int)` instead.
- `MongoBob`.
- `@UseIfnull` — use `@IgnoreNullFromDB`.

**What to change now (optional but recommended):** search your codebase for the members above and
migrate opportunistically. Nothing is urgent for 6.3.0 — this only matters before upgrading to 7.0.

## New: DualChannelMessaging (Beta)

A new, opt-in messaging implementation (`de.caluga.morphium.messaging.DualChannelMessaging`,
[#265](https://github.com/sboesebeck/morphium/issues/265)) targeting request/reply throughput on
real MongoDB: it keeps Standard's simple single-collection layout for broadcast/topic traffic and
adds a second, dedicated per-recipient collection with its own change-stream cursor purely for
directed messages and answers — the second cursor is what actually gives `MultiCollectionMessaging`
its capacity edge over Standard, not the per-topic split, and this isolates just that one mechanism
on top of an otherwise unmodified Standard core.

Enable it with a one-line config change:

```java
cfg.messagingSettings().setMessagingImplementation("DualChannelMessaging");
```

Measured on MongoDB under sustained overload: **+9% throughput and ~33% lower p50 latency than
Standard**, without `MultiCollectionMessaging`'s runaway-RTT failure mode under overload. On
PoppyDB the second cursor makes no measurable throughput difference (PoppyDB's change-stream
delivery is push-based and was never cursor-cadence-bound the way mongod's oplog-tailing is).

**Before enabling in production:** every participant on a given queue must run
`DualChannelMessaging` — see the
[Mixed-Cluster Requirement](./messaging-implementations.md#dual-channel-messaging-beta) and full
measured numbers in [Messaging Implementations](./messaging-implementations.md). Marked `@Beta`:
behavior, collection layout, or API surface may change without a deprecation cycle.

## New: Optional Extension Modules (`morphium-jakarta-data`, `quarkus-morphium`)

Morphium is being split into a core plus opt-in extension modules. Two of them ship with 6.3.0.
The dependency direction is strictly one-way — core has no knowledge of either module, so an
application declaring only `de.caluga:morphium` gets exactly what it got in 6.2.x; nothing new
lands on your classpath unless you add the module yourself.

- **`morphium-jakarta-data`** — a [Jakarta Data 1.0](https://jakarta.ee/specifications/data/1.0/)
  provider on top of Morphium's query engine: `@Repository` interfaces with query derivation from
  method names (`findByCategory`, `countByStatus`, `deleteByX`, `And`/`Or`/`Between`/`In`/`Like`/
  `OrderBy`), JDQL via `@Query` (including `GROUP BY`/`HAVING` compiled into an aggregation
  pipeline), `@Find`/`@Delete` with `@By` binding, offset (`Page<T>`) and cursor/keyset
  (`CursoredPage<T>`) pagination, static and dynamic sorting. Framework-agnostic by design — it is
  meant to be consumed by framework integrations. See [Jakarta Data](../jakarta-data.md).
- **`quarkus-morphium`** — a Quarkus CDI extension: a producer for `Morphium`, typed config
  (`quarkus.morphium.*`), `@MorphiumTransactional` with CDI transaction events, SmallRye
  liveness/readiness/startup health checks, Dev Services, a Dev UI card, build-time Gizmo-generated
  Jakarta Data repository implementations (no runtime reflection or proxies), GraalVM native-image
  reflection registration for `@Entity`/`@Embedded`, `MorphiumId` JSON serialization as its
  canonical 24-char hex string, and a MongoDB-backed migration runner with a distributed lock. See
  [Quarkus Extension](../quarkus-extension.md).

### Breaking: `quarkus-morphium` moved to the `de.caluga` groupId

The Quarkus extension previously published as `io.quarkiverse.morphium:quarkus-morphium:1.2.0`. It
does not actually live in the Quarkiverse GitHub organization, so its Maven coordinates now follow
Morphium's own groupId and version in lockstep with the reactor. **What to change:** update the
dependency's `groupId` to `de.caluga` and its version to the Morphium version you adopt:

```xml
<dependency>
  <groupId>de.caluga</groupId>          <!-- was: io.quarkiverse.morphium -->
  <artifactId>quarkus-morphium</artifactId>
  <version>6.3.0</version>              <!-- was: 1.2.0 -->
</dependency>
```

No package renames, no API changes — only the coordinates move. The module publishes
`quarkus-morphium` (runtime), `quarkus-morphium-deployment` and `quarkus-morphium-testing`.

Building the reactor with `-DskipExtensions` produces a core-only build (core + PoppyDB), exactly
as before this change.

## New: PoppyDB — production-readiness features

- **DevOps command surface**: `db.currentOp()`/`killOp` (with a real op registry), `rs.conf()`,
  `listCommands`, `hostInfo`, `connectionStatus`, `whatsmyuri`, real `serverStatus.connections`.
  Fixes mongosh's admin helpers, which previously failed against PoppyDB.
- **Opt-in auth enforcement** (`--auth`) with real server-side SCRAM-SHA-1/SHA-256 verification
  (RFC 5802/7677) and a working `createUser`. Without `--auth`, behavior is unchanged (fully open).
  Authorization is authentication-only for now — roles are stored but not evaluated.
- **A complete user lifecycle**: `createUser`, the newly added `updateUser` (in-place password/role
  rotation) and `dropUser`, all with optional `customData`, all replicated. `updateUser` no longer
  resets a user's SCRAM mechanism set on a password change (a SHA-256-only user was silently
  re-armed for SHA-1), and a password change no longer discards stored `customData`; malformed
  field types produce a `BadValue` command error instead of an uncaught `ClassCastException`.
- **`admin.system.users` replicates across the replica set** — users were node-local before, so a
  secondary never had the same logins as the primary and a failover (or a dump taken on a
  priority-0 node) silently lost them. It is now the one system collection that replicates, through
  live events, the initial-sync snapshot and resync-clear alike.
- **Declarative user provisioning** via `--users-file <path>` — a JSON file (bare array, or
  `{"version": N, "users": [...]}`) applied as an idempotent `createUser`/`updateUser` upsert
  wherever `ensureRootUser` runs. The optional `version` gates re-application against a replicated
  meta document, so a straggler node cannot roll credentials back on failback. Duplicate
  `(user, db)` entries and unknown fields are hard errors (previously silent last-entry-wins);
  file permissions are checked and the content is never logged.
- **Configuration file support** (`--cfg`/`-f`, `$POPPYDB_CONF`, plus four default locations,
  `--no-config` to skip them) with uniform precedence CLI > file > default, `--no-ssl`/`--no-auth`
  to switch a file's booleans back off, and `root-password-file`/`ssl-keystore-password-file` so
  secrets stay off the command line (where `ps aux` exposes them for the life of the process).
  Files carrying secrets are permission-checked: group/other-readable warns, group/other-writable
  refuses to start. See the note in Breaking Changes above about automatic discovery.
- **`--print-config`/`--check-config`** — print the effective configuration (secrets redacted, with
  per-key source annotations) as a reusable config file, or validate syntax, semantics and deep
  checks (keystore loadable, dump dir usable) without starting the server.
- **TLS actually works now** — it was silently broken (NPE on startup) whenever an `SSLContext` was
  configured. **On a replica set, `--auth` and `--ssl` each made the cluster completely
  non-functional**: the internal election and replication channels connected to peers as plain,
  unauthenticated, unencrypted clients, so with `--ssl` every internal connection was rejected by
  the peer's TLS listener and with `--auth` every election RPC was rejected as unauthorized — no
  leader could ever be elected. The internal channel now authenticates as the configured root user
  and pins the server's own certificate as its truststore. No new config keys.
- **`--log-level` option** — the CLI jar no longer floods disks by logging everything at DEBUG by
  default (root now defaults to `INFO`).
- **Replication correctness**: index definitions are now replicated (previously documents only —
  unique constraints, TTL, and index-backed queries silently didn't work on secondaries or after
  failover); replication is now lossless and order-preserving; a long-standing election bug that
  kept followers from ever starting replication is fixed. A leader change with byte-for-byte
  identical data (verified per namespace via `dbHash`) now skips the clear-and-full-resnapshot.
- **Consistency checks with teeth**: `dbHash` (MD5 per collection over the BSON-encoded documents in
  a canonical order, answered on secondaries too — the one-command check that two members hold the
  same data) and a real `validate` that walks the index store and reports index entries pointing at
  removed documents and documents missing from an index.
- **Resource leaks closed**: find cursors are cleaned up when a client disconnects, idle cursors
  expire via TTL, and watch/tailable event queues are bounded (they were unbounded before).
- **Messaging throughput**: the dead `msg_locked_by_1_locked_1` index that `MessagingOptimizer`
  created on every registered messaging collection is gone — the fields it indexed no longer exist
  on `Msg` (locking moved to the separate `MsgLock` collection long ago) and nothing ever queried
  it, so all it did was add per-insert maintenance cost on the hottest collection. The insert
  duplicate-`_id` pre-check is an O(1) index lookup instead of a full collection scan under the
  write lock, which was the dominant per-insert cost on large collections.
- **Memory watermarks** and **BSON/message size enforcement** — see Breaking Changes above.

## New: InMemoryDriver aggregation & query surface

A large batch of previously-missing or silently-wrong aggregation/query behavior is now
implemented, primarily useful for anyone testing MongoDB-heavy pipelines against the InMemoryDriver
or PoppyDB instead of a real server:

- New pipeline stages: `$documents`, `$densify`, `$fill`, the full `$setWindowFields` window-function
  surface (including range windows), a working `$merge`, and a real `$out` (previously a no-op).
- ~40 additional `Expr` operators implemented (`$map`, date arithmetic, byte/codepoint string
  operators, `$sortArray`, `$round`, `$median`/`$percentile`, and more).
- Positional update operators `$`, `$[]`, `$[<identifier>]` with `arrayFilters` (also reachable from
  the high-level API via the new `Query.setArrayFilters(...)`), and `$bit`.
- `dbHash`, `validate`, `currentOp`, real `serverStatus`, `$collStats`/`$listSessions`, and the
  MongoDB-8.0-style top-level `bulkWrite` command.
- Typed `Aggregator` builder methods for the new stages — `documents(...)`, `densify(...)`,
  `fill(...)`, `setWindowFields(partitionBy, sortBy, output)` — instead of `genericStage()`.
  Implemented in both `AggregatorImpl` and `InMemAggregator`, with the same field-name translation
  as every other typed stage method.

If any of your tests were relying on a previously-stubbed or silently-wrong behavior in this area
(several dozen correctness fixes shipped alongside the new features — see the 6.3.0 section of
`CHANGELOG.md` for the full list, and
[Behavior Fixes You Should Know About](#behavior-fixes-you-should-know-about-in-inmemorydriver-and-poppydb)
above for the ones most likely to change your results), re-run your suite against
InMemoryDriver/PoppyDB after upgrading.

## New: Messaging improvements (all implementations)

- Configurable `messagingDefaultTtl` (default 30s, was hardcoded) and `messagingFallbackPollInterval`
  (default 10s) — tune the poll interval below your shortest message TTL.
- Requeued messages (cleared `processedBy` via a plain DB update) are now picked up event-driven
  instead of waiting for the next fallback poll.
- Change-stream liveness now drives the fallback poll directly — a silent stream triggers an
  immediate poll instead of waiting for the next timer tick.
- A bounded processing-decision trace aids answer-timeout diagnostics (dumped only on timeout, not
  during normal operation). Also exposed as `getProcessingDecisions(msgId)`.

### Non-exclusive messages are deserialized from the change-stream snapshot

`SingleCollectionMessaging` re-read every incoming message by `_id` (PRIMARY read preference)
before processing it, although the insert event already carried the complete document. For the
safe case — a **non-exclusive** message arriving via an insert event with a `fullDocument` — the
message is now deserialized directly from the event snapshot, saving one DB roundtrip per message.
Everything with staleness risk deliberately keeps the re-fetch: exclusive messages (the
`processed_by` re-check after claiming the lock is correctness, not overhead), requeue updates,
poll pickups, and any snapshot that fails to deserialize. All skip checks (listener existence,
sender == self, processed-by, recipients, answer matching) run unchanged.

**What this means for you:**

- **Entity lifecycle callbacks fire on this path too.** The first version deserialized via the raw
  `ObjectMapper`, which — unlike the query path — fires no lifecycle callbacks, so `@PostLoad` was
  skipped for non-exclusive messages. That also silently broke V5-legacy messages: `Msg.postLoad()`
  is where the V5→V6 compatibility migration lives (`topic = name` when only the legacy `name`
  field is set), so a message written in V5 format without a `topic` (e.g. via `storeMap()`)
  arrived with `topic == null` and was dropped by the "no listener for this topic" check — no
  exception, no fallback, on every backend. The fast path now fires `firePostLoadEvent()` right
  after a successful deserialize, matching the query path, and falls back to the re-fetch path if
  the callback throws. Both the optimization and this fix ship in 6.3.0, so upgrading from 6.2.x
  you never see the broken intermediate state — but **if your message entities carry `@PostLoad`
  methods (or you still hold V5-format messages), verify delivery after the upgrade**: this is the
  one path where a message is no longer built by the query path.
- The decision trace records which of the two paths a message took.

## Migration Checklist

1. [ ] **Search for `forRemoval = true` candidates** (see Deprecations above) and migrate
   opportunistically — not urgent for 6.3.0, but IDEs will now flag them.
2. [ ] **If you test against InMemoryDriver/PoppyDB**, re-run your suite — several dozen
   correctness fixes may surface previously-masked test bugs (see the two Breaking Changes sections
   and [Behavior Fixes](#behavior-fixes-you-should-know-about-in-inmemorydriver-and-poppydb) above).
3. [ ] **Check aggregation pipelines for date-operator workarounds.** `$month` is 1-based now, date
   operators evaluate in UTC, and `$ln`/`$setUnion`/`$asinh`/`$reverseArray`/`$dateFromParts` and
   the single-arg `$avg`/`$max`/`$min` return different (correct) values against
   InMemoryDriver/PoppyDB. Anything that compensated for the old behavior is now wrong.
4. [ ] **If you run PoppyDB in production**, review the new `--auth`, `--users-file`, `--cfg`,
   `--memory-warn`/`--memory-reject`, and `--log-level` options — defaults preserve prior (open,
   unbounded, DEBUG) behavior, so nothing changes unless you opt in. **But** check the four default
   config-file locations for leftover files (or pass `--no-config`), and validate your startup
   options with `--check-config` before rolling out — options that were silently accepted before can
   now abort startup.
5. [ ] **If you run PoppyDB or the embedded InMemoryDriver long-running**, be aware that TTL expiry
   works again (#269): collections that stopped expiring documents will shed everything past their
   `expireAfterSeconds` bound on the first sweep after the upgrade. Check before restarting if you
   are unsure whether those documents should still be there.
6. [ ] **If you store documents that could exceed 16MB** or write batches that could exceed 48MB
   against InMemoryDriver/PoppyDB, verify you're within the now-enforced limits (or raise them).
7. [ ] **If you parse PoppyDB's `rs.status()`/`buildInfo` output** in monitoring, update it:
   `stateStr` uses MongoDB's nomenclature now and the reported version is the real one (`6.3.0`),
   not `5.0.0-ALPHA`.
8. [ ] **If your message entities have `@PostLoad` methods or you still hold V5-format messages**,
   verify message delivery after the upgrade — non-exclusive messages now come from the
   change-stream snapshot (lifecycle callbacks included; see Messaging improvements above).
9. [ ] **If you use `io.quarkiverse.morphium:quarkus-morphium`**, change the `groupId` to
   `de.caluga` and the version to `6.3.x`.
10. [ ] **Optional:** if request/reply throughput is your bottleneck on real MongoDB and you can
    run a homogeneous cluster, evaluate the beta `DualChannelMessaging` implementation.
11. [ ] **Optional:** set `cfg.driverSettings().setAppName(...)` per service so `db.currentOp()`
    and the server log can tell your instances apart.
12. [ ] No dependency version changes — nothing to reconcile in your own `pom.xml` (adding
    `morphium-jakarta-data` or `quarkus-morphium` is opt-in; core pulls in nothing new).
