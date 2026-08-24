# Changelog

All notable changes to Morphium will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).


## [Unreleased]

### Fixed

#### CHITSPERC/CMISSPERC reported NaN instead of 0 before any cached read had happened
`Statistics.java` computed `CHITS/(CHITS+CMISS)*100` unconditionally; before any cached read has
happened both are 0, so the ratio was `0.0/0.0 = NaN`. Prometheus/OTel exporters silently drop NaN
samples, so a fresh application's cache-hit-ratio metric appeared entirely missing instead of a
real "no data yet" 0%. Found while verifying the quarkus-morphium observability module against a
live otel-collector/Prometheus stack. Both percentages are now also computed by reading each
`AtomicLong` once instead of three times, so they come from one consistent snapshot.
#### PoppyDB: secondaries no longer leak ~800 bytes of heap per replicated event
Every `InMemoryDriver.runCommand()` stores its reply in an internal by-id map, and the entry
only ever leaves that map when the caller fetches it (`readSingleAnswer` et al.). The
ReplicationManager apply path called `runCommand()` and threw the returned message id away for
every non-bulk-insert operation — update/replace (the dominant type on a live bus), delete,
drop, dropDatabase, the idempotent replay-insert, plus the initial-sync insert batches and the
pre-sync database drops. The same pattern hid in `WatchCursorManager.createWatchCursor`,
which discarded the stub reply of every started change stream (one leaked entry per created
cursor — reconnect-looping messaging clients create them all day). On the primary the Netty
handler fetches every request's answer, so only secondaries leaked per-event — one abandoned
reply per replicated event, forever. Proven by measurement
on a local 3-node replica set: 20,000 update events on the primary grew the secondaries'
live-object count by exactly +1 `java.lang.Double` (the `"ok": 1.0`) per event after full GC,
while the primary stayed flat. At production rates (~800 bytes/event, 12 events/s) that is
roughly 0.8 GB/day until the node runs into the memory-watermark reject. All apply sites now
fetch their result the way the bulk-insert path always did — which also surfaces write errors
that used to be swallowed silently (logged, never thrown: an error reported inside a delivered
result must not make the apply path fail harder than before).

As defense in depth the driver itself no longer allows unbounded growth of the by-id result
store: command ids are strictly monotonic and a legitimate caller fetches its answer
synchronously in the same call stack, so an entry whose id lies more than a full window
(10,000 ids, `-Dinmemory.maxPendingCommandResults`) in the past is abandoned with certainty —
never "about to be read" — and gets evicted with a rate-limited WARN once the store exceeds
the window. `resetData()` now clears the store too (it was the one cleanup path that missed
it), and `REPLY_IN_MEM` in the driver stats finally counts these pending replies, which is
what the new regression tests assert on.
#### SingleMongoConnection: every heartbeat hello re-ran the full SASL handshake
`getHelloResult()` appended a complete SCRAM authentication to every hello, including
hellos sent over a connection that had authenticated long ago. MongoDB auth state is
bound to the socket and survives for its lifetime, so on an auth-enabled cluster this
produced one full SASL exchange per second per client on each pooled connection - all
of it pure overhead, and invisible as connection churn because the socket never
changed. Measured on a production replica set as ~7,200 `Successfully authenticated`
entries per hour per node on unchanged connection ids. Authentication state is now
tracked per connection and re-run only on a fresh socket (or after logout), which is
exactly when it is actually needed. `SingleMongoConnectDriver` was never affected - its
heartbeat uses a bare `HelloCommand` without the auth follow-up.

#### PooledDriver: idle long-lived clients no longer rebuild their connection pool every 30 seconds
A long-lived `PooledDriver` client with little or no application traffic tore down and rebuilt
its pooled connections permanently: measured in production on a 3-node replica set with ~22
long-lived Spring Boot clients, the nodes saw 1.48 (primary), 3.76 and 4.27 (secondaries) NEW
TCP connections per second - steady, for hours - amounting to 347,000 / 762,000 / 937,000
connection establishments over 61h while only 150-220 connections were ever open at a time.
The cause: `lastUsed` on a pooled connection is only refreshed by real application borrows,
not by the heartbeat hello that runs over it every second (deliberately so - otherwise the
heartbeat would keep every connection "warm" forever and `maxConnectionIdleTime` could never
shrink the pool after a burst). The idle sweep therefore declared every pooled connection of a
quiet client idle after `maxConnectionIdleTime` (30s default) and closed it - and the refill
loop immediately re-created it to satisfy `minConnectionsPerHost`. A full TCP handshake every
30s per pooled connection, forever, for a connection that was carrying healthy heartbeat
traffic the whole time. The hypothesis was verified experimentally against a local 3-node
PoppyDB RS: with 9 pooled connections and idle time 10s the reconnect rate was exactly
0.90/s (= pool size / idle time), a 10x longer idle time cut it to a tenth, and a 5x slower
heartbeat left it unchanged. The fix keeps both properties intact: idle eviction now only
shrinks the surplus above `minConnectionsPerHost` (bursts still drain back down), while the
base stock is recycled solely via `maxConnectionLifeTime` (10min default). Secondaries were
hit hardest because primaries stay warm through real borrows - matching the measured
primary/secondary asymmetry.

#### Container fields of scalar-mapped types (BigDecimal, Character, Atomic*, LocalDate, ...) now deserialize correctly (#334)
`List`/array/`Map` fields whose element type has a custom mapper with a scalar `marshall()`
result (`BigDecimal`, `Character`, `AtomicBoolean`/`AtomicInteger`/`AtomicLong`, `LocalDate`,
`LocalTime`, `Timestamp`, ...) are stored element-wise as a `{"value": <scalar>}` wrapper map
without `class_name`. The read path had no branch that recognised this shape: the raw wrapper
`Map` survived into the loaded container, so the first typed access
(`BigDecimal.compareTo(...)`) threw a `ClassCastException` — and typed arrays like
`BigDecimal[]` failed the whole entity read outright with `array element type mismatch`.

The fix is deliberately **read-side only — the on-disk write format is bit-for-bit
unchanged**. A write-side fix (dropping the wrapper, adding `class_name`) was tried in
PR #333 and measurably changed the stored document shape, which breaks rollbacks,
mixed-version operation against a shared collection, and indexes on `field.value`; a
read-side unwrap is purely additive: existing documents load correctly, new documents look
exactly like before, and older Morphium versions keep reading them. A new format-stability
test pins the written raw shape so any future write-side change fails loudly.

Unwrapping is generic over the registered custom mappers, not a hardcoded type list, and
deliberately narrow: a map is only treated as a wrapper if the declared element type has a
registered custom mapper and the map carries exactly the key `value` (plus at most a
`class_name`). Documents that legitimately contain a field named `value` — embedded objects,
untyped `Map<String, Object>` content — are left untouched, and if the mapper was
deregistered at runtime the read falls back to the previous behavior instead of throwing.


## [6.3.6] - 2026-08-21

### Fixed

#### PoppyDB: ordinary client disconnects no longer flood the log with ERROR lines (#331)
All three Netty `exceptionCaught` handlers (decoder, encoder, command handler) logged every
exception unconditionally at ERROR - including a plain `Connection reset by peer` whenever a
client dropped its connection. Deploys, restarts and load balancers do that all day: on the ACC
acceptance cluster a single reconnect-looping client produced 140 ERROR lines in 40 minutes, and
unconditional ERROR logging is a good part of how a poppy.log grew into the gigabytes. The three
sites now share one rule: the IOException family (reset by peer, broken pipe, timeouts) is
logged at DEBUG, everything else stays at ERROR with the full stack trace. Close behaviour per
handler is unchanged.

#### PooledDriver: a rolling restart can no longer erode the topology into permanent silence (#330)
During the ACC rolling restart exactly one of ~30 clients ended up permanently bus-dead while
looking perfectly healthy: zero heartbeat threads, zero log lines, HTTP alive. The chain behind
it: the membership-removal path compared the hosts map's NORMALIZED keys against
UN-normalized names from the hello - so a hello advertising a member in a different case
(SERV-MSG1 vs serv-msg1, the exact constellation `normalizeHostKey`'s own comment documents)
removed the very host it had just added. A few such hellos during the takeover window eroded the
hosts map AND the running host seed to empty - and an empty seed made `reseedIfAllHostsEvicted`
a silent no-op: the heartbeat kept cycling over nothing, spawning nothing, logging nothing,
forever.

Four layers of fix, innermost first: the removal comparison now uses the exact same
normalization as the add path; membership REMOVAL is only accepted from the PRIMARY's hello
(the code comment always claimed this, the code never checked - secondaries and in-election
nodes answering with partial lists during a restart can no longer eat the topology; additions
stay accepted from every hello); the originally configured host seed is captured at connect
and restored - loudly - when the running seed has been eroded to empty; and the heartbeat
itself became self-rescheduling with a watchdog (silent-cycle detection with forced reseed,
dead-task revival, orphaned per-host bookkeeping cleanup), so even an unforeseen way of
stalling discovery now logs and recovers instead of freezing silently.


## [6.3.5] - 2026-08-21

### Fixed

#### ChangeStreamMonitor: a discarded resume token could be resurrected — clients hammered `ChangeStreamHistoryLost` resumes forever (#329)
When the server ends a change stream with 286 `ChangeStreamHistoryLost` ("resume window
lost"), the monitor's error classifier correctly discards its resume token and restarts
fresh. But `run()`'s finally-block adoption then read the token back off the dead
`WatchCommand` — the very token `run()` itself had set at watch construction — and
resurrected it, so every retry resumed with the exact token the server had just declared
dead. Against PoppyDB, whose in-memory sequence space used to reset on every restart, this
turned **every** connected client into a resume-hammering loop the moment the server came
back (the 2026-08-21 ACC bus outage: ~3.3k errors/s on the primary until every client
process was restarted by hand). Against real MongoDB the same loop starts once a consumer's
resume point falls off the oplog. The deliberate discard now suppresses exactly one
finally-adoption; ordinary errors keep the gap-protection adoption unchanged. Covered by
red-green unit tests and an end-to-end PoppyDB restart test that was verified to fail
against the pre-fix code.

### Added

#### PoppyDB: the change-stream sequence survives restarts (`sequence-state.properties`) (#329)
A restarted server used to issue tokens from 0 again, which made every client's resume token
"foreign or reset sequence space" and — worse — blinded the destructive-resync guard's
sequence comparison: a healthy restarted primary was indistinguishable from a stale one, so
the ACC secondaries livelocked in a 2s refuse/re-register cycle instead of resyncing. The
sequence is now persisted next to the dumps with every dump (periodic, on-demand and the
final dump on shutdown) and restored monotonically in `restoreFromDump()` with 10M headroom
for increments a crash may have left unpersisted. Stale client tokens thereby land in the
well-defined behind-the-replay-window case, and peer sequence comparisons stay meaningful
across restarts. Without a dump directory nothing changes.


## [6.3.4] - 2026-08-21

> **Defective release — do not use in production, upgrade to 6.3.5.** The server-side strict
> resume-window guard added here, combined with the client-side resume-token resurrection bug
> (#329, present in 6.3.4 and every earlier version), turns every connected client into an
> endless `ChangeStreamHistoryLost` resume loop after a PoppyDB restart (or, on real MongoDB,
> once a consumer's resume point falls off the oplog). Fixed in 6.3.5.

### Added

#### PoppyDB: `dumpNow` returns immediately, and every dump write is crash-safe (#317)
Two things that only look related until you trigger a dump on a node with real data in it.

`dumpNow` no longer keeps the client (or the server's I/O thread) waiting for the whole dump.
It starts one and answers right away with `status: "started"`, or - if a dump is already
running - `status: "alreadyRunning"`, without queuing anything. That "already running" is not
just about two admins racing each other: the periodic dump scheduler, the on-demand command
and the final dump on shutdown now share **one** guard, so an automatic dump can never overlap
a manual one either. A scheduled tick that finds the guard taken skips (the next one is due
anyway); shutdown waits a bounded 10s for a running dump before writing its final one, and
says so in the log if it gives up. Whether the started dump then succeeded is visible in the
server log and, for the completion timestamp, in `dumpStatus` - the command itself is done
once the dump is under way. The programmatic `PoppyDB.dumpNow()` stays synchronous but is
guarded the same way; it now returns `-1` when it skipped because another dump was running.

The dump *write* changed underneath all of that: `InMemoryDriver` no longer writes straight
into `<db>.morphium.gz` (which truncated the last good dump the moment a new one started).
Each database is written to a sibling `<db>.morphium.gz.tmp`, forced to storage, and only then
moved over the final name - atomically where the filesystem supports it, with a best-effort
fsync of the directory afterwards, the same sequence `ElectionManager` already uses for the
election state. A process or machine death mid-write now leaves the previous dump completely
intact instead of destroying it before the replacement exists. This applies to every dump -
scheduled, manual and the one on shutdown.

#### `sendMessages()` / `sendAnswers()` — genuine client-side batching for Messaging
Prompted directly by the "Batch Send Throughput" benchmark (see below): `@WriteBuffer`,
tried as a shortcut to Kafka-style batching, turned out to be the wrong tool for messaging —
it's a poll-and-WAIT mechanism that becomes a throughput *ceiling* under load, not a booster.
The thing that actually worked in that benchmark was a plain client-driven bulk insert, so
`MorphiumMessaging` now has that as a first-class API: `sendMessages(List<? extends Msg>)`
sends a batch as one or more real bulk-insert wire calls — grouped by whatever target
collection each implementation's routing needs (one call for all broadcasts;
`DualChannelMessaging`/`MultiCollectionMessaging` additionally group directed messages by
recipient, and `MultiCollectionMessaging` groups broadcasts by topic collection) — instead of
one insert per message. No annotation, no housekeeping thread, no tuning: the caller decides
the batch, one call carries it.

A default `sendAnswers(Msg answerOf, List<T> answers)` builds on top of it, replicating what
`Msg#sendAnswer()` does per message (`inAnswerTo`, recipient, a fresh `msgId`) before sending
the whole list in one batch. Aimed at a single thread that wants to send many answers to one
request — a chunked or streamed response, for instance — rather than at fanning out many
independent requests, since that's where a caller naturally already has a batch in hand
without any restructuring.

The single-message send path (`sendMessage()`) is unchanged; the per-message registry check
and sender/senderHost/TTL-default logic it relies on were factored into shared private helpers
so both paths apply the exact same policy instead of two copies drifting apart.

### Changed

#### `dumpNow` reply and completion semantics (#317) — **behavior change**
The `dumpNow` admin command shipped in 6.3.0 answered `{ok: 1, databases: N}` *after* the dump
had been written; it now answers immediately with `{ok: 1, status: "started"|"alreadyRunning"}`
and the `databases` count is gone — the command no longer knows it when it returns. Anything
that read `databases`, or treated a successful reply as "the dump is on disk" (a
snapshot-before-maintenance script, for example), has to change: trigger, then poll
`db.adminCommand({dumpStatus: 1}).lastDumpMs` until it advances. `alreadyRunning` means a dump
was already in flight and nothing was queued. The programmatic `PoppyDB.dumpNow()` keeps its
synchronous contract and its database count, but now returns `-1` when it skipped because
another dump held the guard.

### Fixed

#### Change stream: a resume is now verified inside the replay, not just before it (#320)
`canResumeChangeStream` was evaluated when the watch registered, but the actual replay ran later on
another thread - and eviction runs concurrently on every write. A resume validated as clean could
therefore still lose events in that gap, and the consumer had no way to notice: it received the
surviving suffix plus the live stream, with the hole in the middle invisible. Worse, that gate only
ever covered PoppyDB's replication resumes - ordinary resumes (`ChangeStreamMonitor`, i.e.
messaging) were not checked at all, so any burst past the replay-buffer limit silently truncated
their replay.

`replayHistory` now verifies the window itself, after the replay, when it knows what was actually
delivered: every token of the resume window must either still have been buffered or have already
reached this consumer live; a resume token beyond the driver's own sequence (a restarted primary,
or a failover to a different node's sequence space) fails immediately instead of silently starting
"from now"; and a drop of a namespace the stream covers, after the resume point, ends the stream so
the consumer actually learns of the drop. The drop rule is namespace-fair: an unrelated
collection's drop no longer matters to a collection-scoped stream, while the cluster-wide
replication watch keeps the strict global boundary it had. Every failure is loud, through the
existing terminal-error channel with the `ChangeStreamHistoryLost` marker both known consumers
already key their recovery on - `ChangeStreamMonitor` discards its token and restarts fresh, a
PoppyDB secondary falls back to a full re-sync. A silently gapped stream is the one outcome that
no longer exists.

#### Change stream: live events can no longer overtake a resume's history replay (#319)
A resumed watch registers its subscription before replaying history (the reverse order would drop
live events), which meant live dispatch raced the replay into the same consumer: a live event with
token 105 could arrive before the replayed 101-104 - and, past the 8192-token duplicate-suppression
window, even twice. For a PoppyDB secondary - which applies updates as `_id`-keyed full-document
upserts with no already-applied check - that inversion silently overwrites a newer document with an
older one, or lets a replayed insert resurrect a document a live delete already removed; both
persist until something forces a re-sync. The trigger is any replication reconnect while writes
continue, i.e. routine under load.

The subscription now carries an ordering barrier, armed before it is registered: while the replay
runs, live events are staged in a bounded per-subscription buffer instead of being delivered; when
the replay completes, the staging is drained in token order and only released once a drain finds it
empty, so nothing slips between flush and release. Writers never block on any of this. The staging
capacity is half the dedup window, which keeps that window provably sufficient instead of
guesswork; if live writes outrun the replay past that bound, the stream ends loud with
`ChangeStreamHistoryLost` and the consumer re-syncs - recoverable, unlike silent reordering.

#### PoppyDB: watch-cursor queues are byte-bounded - one slow consumer can no longer pin gigabytes (#321)
The per-cursor event queue was bounded by count only (10,000 events). Each queued event shares its
`fullDocument` payload with the replay-buffer entry, so replay-buffer byte eviction frees nothing
while a stalled cursor still references the payloads: with ~300KB documents, a single slow or
blocked consumer pinned about 3GB on the primary - the node whose OOM takes the whole cluster down.
This is the same failure family as the 2026-08-14 ACC incident, one layer up: that incident
produced byte budgets for the replay buffer and for the secondary's replication event queue, but
the cursor queue sitting between them stayed unbounded in bytes.

Each cursor's queue now has a byte budget (default 64m, `--cursor-queue-budget`, same size syntax
and same size estimate as the two sibling budgets). Overflow kills the cursor through the same
centralized path as the count cap - on the primary, kill is the only viable policy: server-mode
delivery runs synchronously on the writer thread, so blocking would stall the node's entire write
path for one slow consumer, and dropping oldest would silently lose events, the exact bug family
this project has been eliminating. A single event larger than the whole budget is still delivered
while the queue is empty, so the budget never imposes a document-size cap. The accounting adds at
offer time and subtracts the identical estimate at drain time, so the counter cannot drift and
quietly disable the bound.

#### Messaging: the polling path no longer dies on int64 message fields
The poll in `SingleCollectionMessaging` (and its twin in `DualChannelMessaging`) cast `priority`
hard to `Integer` and `timestamp` to `Long`. A message document whose numeric fields arrive as
the other boxed type - int64 over the wire, which real MongoDB may produce at any time and which
demonstrably occurs after a PoppyDB failover - killed every poll with a ClassCastException. The
poll is exactly the path that recovers the backlog after a changestream outage, so the receiver
silently never delivered again ("no messages within 15s of the fault", the
DriverFailoverProxyTest flake). The changestream path of the very same class has always handled
this tolerantly (`((Number) prio).intValue()`) - a classic two-paths drift; both poll paths now
follow the same rule.

#### PoppyDB: a stopped sync thread no longer writes into its successor's data (#323, part 1)
`ReplicationManager.stop()` joins its initial-sync thread with a 5s bound - but the sync
connection reads with a 60s timeout, socket reads ignore `Thread.interrupt()`, and the copy loop
checked neither `running` nor interruption anywhere. So the join lost routinely, and the abandoned
thread resurfaced later with a complete collection read in hand and inserted it - documents from
the OLD primary - into local data that by then belonged to the replacement ReplicationManager's
own sync: stale foreign documents, silent divergence until some later resync.

Two of the issue's three parts land here (the cheap, independently valuable half): the copy loop
is now cooperatively cancellable - checked between databases, between collections and, decisively,
between a completed read and its local insert, which is the exact position an abandoned straggler
resurfaces in - and `stop()` closes the tracked in-flight sync connection, which is the only thing
that ends a socket read blocked on a slow primary before its 60s timeout. A stopped cycle now ends
with one INFO line instead of an error-and-retry. The remaining part (a generation check before
every local write, plus the convergence-after-chaos test with a real slow-primary read seam) stays
with the issue.

#### PoppyDB: the initial sync no longer declares success over a dead watch (#322)
While a secondary's initial-sync snapshot runs, its apply gate is closed and replication events
pile up in the event queue until the byte budget blocks the watch reader - deliberate
backpressure. The primary however never blocks: it kills the cursor when the per-cursor buffer
overflows. The secondary had no way to notice, because the only thread that maintains its
watch-health flags is the very reader that is parked - so the post-snapshot guard trusted a stale
"watch is live" and opened the gate over a provably dead stream with a real event gap. Under
sustained load that became a self-sustaining loop: gap → window lost → full re-sync → same
overflow again, the node stuck in RECOVERING - and in the other branch (replay buffer still
covering the gap) it silently self-healed without the gap ever being visible at all, briefly
reporting a gapped state as healthy either way.

The guard now validates the one signal the blocked reader cannot make stale: after the snapshot
it asks the primary itself whether this cycle's watch cursor still exists (new
`poppyCursorAlive` command). If not, the snapshot is discarded and redone under a fresh watch,
and the dead session is retired: its buffered events are dropped and any events its
just-unblocked reader still delivers afterwards are discarded too, instead of being applied as
stale upserts over the freshly-copied data. The same late-event leak existed in the resume-window
resync path and is closed the same way. The probe deliberately runs unconditionally (not gated on
a "was the reader blocked" heuristic, which misses a reader that was already blocked before the
snapshot started), fails open toward an older primary that does not know the command, and fails
closed when the primary cannot be reached at all.

#### InMemoryDriver: the 21st change stream never started, and TTL expiry stopped with it (#325)
Server-side change streams - the ones PoppyDB opens for its clients and for replication - parked a
thread of the driver's shared scheduler for the entire lifetime of the stream, as did tailable
cursors. That pool is a `ScheduledThreadPoolExecutor` sized `max(20, 2*cores)`, and a
`ScheduledThreadPoolExecutor` never grows past its core size — so once that many streams were open,
the next watch's task simply never ran. It never replayed its history, never stamped its liveness
heartbeat, and never reached the `finally` that unregisters the subscription and releases its
connection, so the leak kept growing instead of stopping at the ceiling.

Nothing about that was visible from the outside. Live events kept flowing (they are delivered on the
writer and dispatcher threads, not on the parked one), so the stream looked healthy while its resume
replay had silently produced nothing — and to `ChangeStreamMonitor.isStreamLive` the same stream
looked *dead*, because the heartbeat lives in the loop that never started. Worse, the TTL sweep
shares that pool: with enough watches open, expiry stopped for the whole node, so `deleteAt`
documents — messages, locks — stopped expiring. For a PoppyDB node serving a message bus, where
there is one change stream per messaging client plus one per replicating secondary, twenty is not a
large number.

(Morphium's own `ChangeStreamMonitor`, and with it Messaging, was never affected: it calls `watch()`
synchronously and parks its own thread, not a pooled one.)

Watch loops now have their own executor that grows with the number of streams instead of capping
them, and the TTL sweep runs on a scheduler of its own so expiry can no longer be held up by
anything else that is queued. **This costs one thread per open server-side change stream**, so a
node carrying many concurrent streams uses noticeably more memory than before — the previous behaviour was cheaper
only because it stopped working past twenty. Removing the thread-per-stream shape itself (virtual
threads or event-driven delivery) is #328.

Also fixed alongside it: a watch whose registration did not confirm within 5s used to return a
perfectly normal-looking cursor for a stream that was never registered. It now says so in the log.

#### InMemoryDriver: two executor-lifecycle fixes found alongside #325
The TTL sweep was scheduled twice. The interval can only be set before `connect()` — the period is
fixed when the task is scheduled — and `connect()` schedules again, so the first task stayed alive
with nothing referencing it any more: unstoppable for the life of the driver, and sweeping in
parallel with its own replacement.

The change-stream event dispatcher was the one executor that could not come back. `shutdown()`
stops it when no subscription is active, but the field was `final` and `connect()` never re-created
it, so a driver instance that was shut down and reconnected — the documented cleanup path for tests
— dropped every client-mode change stream event from then on, with one warning per lost event.

#### PoppyDB never emitted `lock_released` — exclusive messages waited for the poll interval
`MultiCollectionMessaging` deliberately runs **without** its own lock-monitor change stream on
PoppyDB ("server pushes lock_released events directly … 0 extra connections") and depends on the
server emitting a synthetic `lock_released` event when a lock document is deleted. The only
producer of that event sat in the generic command path — and direct dispatch took `delete` over
in 16355e3c2 (March), making that path unreachable for deletes. Releasing a lock has since woken
nobody: the freed exclusive message was only picked up by the next poll round, capping throughput
at poll cadence and producing the msg_lck stall shape under contention. The notification now
happens in the direct `delete` path itself.

#### Replay-buffer accounting drifted, silently disabling the byte budget
An entry can be removed from the change-stream history by two independent parties at once — the
eviction loop (`pollFirst`) and a drop's purge (`removeIf`) — and neither learned whether it had
actually won: `ConcurrentLinkedDeque.removeIf` evaluates its predicate *before* the CAS that
unlinks the node, so a predicate that decremented the counters had already done so when it lost
the race. The counters drifted permanently below the buffer's real weight (reproduced: size
counter at **-19**), and once they do, `bytes > budget` stops firing and the byte budget no longer
bounds memory at all — the exact regression the budget exists to prevent. Every removal path now
books through one exactly-once guard on the entry itself. `getChangeStreamHistoryActualCount()` /
`getChangeStreamHistoryActualBytes()` expose the buffer's real content (O(n)) so the invariant is
assertable — and diagnosable on a live node.

#### Watch-cursor bookkeeping leaked, and a failed watch start was answered `ok: 1` (#326)
Three defects in the cursor delivery path, all with the same shape — state outliving its cursor,
or a client believing it has a stream it does not have:

- Two of the five cursor-removal paths (the terminal-error check in getMore, and `failUnservable`)
  removed the cursor without unregistering its messaging registration. Since nothing else ever
  removes an id from that set, every terminated cursor stayed in it forever, the set never
  emptied, so its key was never removed either — and each dead id cost a lookup in every later
  fast-path notification, on the event loop. Exactly the reconnect churn after
  `ChangeStreamHistoryLost` that those paths exist for made it grow. All removals now go through
  one path that takes the registration with it.
- `createWatchCursor` swallowed a failure to start the watch and returned the cursor id anyway,
  so the client was answered `ok: 1` with a dead cursor, missed every event, and learned about it
  only as a confusing "unknown cursor" error on its first getMore. It now throws, and the command
  is answered as failed.
- The parked-getMore fix from 6.3.3 survived as a race: the terminal state was checked *before*
  the request was parked, so a stream dying in between left the request orphaned to be answered
  after the full `maxTimeMS` with an empty, successful-looking batch. The state is re-checked
  after parking, mirroring the existing re-check for events.

#### The replication watermark could move backwards
`applyChangeEvent` advanced `lastAppliedSequence` with a plain `set`, while the batch paths used
`Math.max`. Events do not always arrive in sequence order — on a resume, the primary's history
replay runs concurrently with live dispatch — so an older event arriving after a newer one dragged
the watermark back. The node then asked the primary to resume from a point it was already past and
re-applied stale full documents over newer ones: silent divergence. Only non-insert events were
affected (inserts go through the bulk path, whose flush re-applied `Math.max` afterwards). All
advances are now monotonic; the deliberate reseed after a full sync stays a plain set.

#### Messages for topics without a listener starved the messaging poll window
`getMessagesForProcessing()` (SingleCollectionMessaging and DualChannelMessaging, plus the
latter's DM-lane fallback poll) fetched candidate messages sorted by `(priority, timestamp)`
with `limit(windowSize)`. A message for a topic this instance has no listener for is skipped
during processing *without* a `processed_by` mark — deliberately, because a listener registered
later (via `addListenerForTopic()`) must still receive it. But that meant the skipped message
re-entered every subsequent poll, and being older than any new arrival it sorted ahead of them
and permanently occupied a slot of the poll window. The trigger is per-instance, not global: it only takes *this* instance not listening to a
topic that has concurrent traffic elsewhere in the collection — the normal case wherever
several message types share one collection and each service instance listens to its own
subset. What actually determines severity is volume: with `windowSize` (default 100) or more
such messages pending within one TTL window (default 300s) — plausible under moderate-to-high
multi-topic traffic, unlikely on a quiet single-topic setup — messages for topics *with* a
listener were starved until the blockers expired via TTL.

The fix filters the poll query itself: it only fetches messages whose topic currently has a
registered listener (plus the status-info topic and the V5-legacy `name` field), while answers
pass regardless of topic since they target waiters/callbacks, not listeners. This mirrors the
server-side relevance filter the change-stream pipeline already applied. Crucially, skipped
messages are still *not* marked processed — they simply stay pending in the collection without
blocking the window, and because `addListenerForTopic()` bumps the poll trigger, the first poll
after a late listener registration picks up the backlog. Regression tests cover both the
starvation and the listener-registered-later delivery guarantee (broadcast and directed).
MultiCollectionMessaging is structurally immune (it polls per-topic collections only for
registered listeners).

#### PoppyDB change-stream cursors silently dropped events under burst load
`WatchCursorManager.drainEvents()` capped a batch at 100 events with
`while ((event = queue.poll()) != null && count < 100)` — when the queue held more than 100
pending events, the 101st was polled off the queue *before* the count check short-circuited
the loop, and the already-removed event was then dropped on the floor instead of being
returned or requeued. Under a burst that pushes the cursor's queue depth above 100 between two
`getMore`s (found via a Morphium Messaging benchmark that bulk-inserts 5000 documents in
~150ms), this lost roughly 1% of all change-stream events per cursor, silently — messages
never went missing loudly, they just took until their TTL expired to surface via the
(also affected) polling fallback, or never surfaced at all outside of one. This is general
change-stream/tailable-cursor infrastructure, not Messaging-specific: any bulk import,
migration, or ETL job writing into a collection an application is watching can trigger it.
Reaching it *through* Messaging specifically needed calling `morphium.insert(List<Msg>, ...)`
directly, bypassing `sendMessage()` — the public Messaging API has no bulk-send call that
could hit this on its own today.

Fixed by checking the count bound before polling
(`while (count < 100 && (event = queue.poll()) != null)`), so a 101st event stays in the queue
for the next drain instead of being discarded. `drainEvents()` backs both watch and tailable
cursors, so this likely also explains (and fixes) the long-standing `TailableQueryTests`
flakiness on PoppyDB noted as "known non-code flakiness" in the homelab test matrix — that
"getMore doesn't always see inserts from another connection" symptom is exactly this event
loss, not a genuine ordering/visibility gap.

#### GitHub releases carried neither the binaries nor a word of prose
Everything `release.sh` ever put on a GitHub release was the test-results table: the release
body came from `--notes-file <test report>` and nothing else, and no asset was ever uploaded.
The prose on 6.3.0-6.3.2 and the `poppydb-*-cli.jar` attached to them were typed in and
dragged there by hand afterwards — so v6.3.3, where that manual step was forgotten, shipped
with an empty description and no downloadable artifact at all, while the CHANGELOG's
`[Unreleased]` block still held exactly the text that release was missing.

Three changes close the loop:

- **The CHANGELOG is stamped at release time.** Before `release:prepare`, `[Unreleased]` is
  rolled over into `## [X.Y.Z] - <date>` with a fresh empty `[Unreleased]` opened above it, so
  the section is on the release commit and quotable from the tag. It stays hands-off when a
  section for the version already exists, when there is no `[Unreleased]` heading, or when the
  block is empty — a documentation gap is not a reason to abort a release.
- **The release body is rebuilt, not appended to.** Prose (everything outside the test-report
  markers) and the report are treated as two independent halves: hand-written prose always
  wins and the CHANGELOG section only fills in an empty one, while the report block is
  replaced rather than stacked. Re-running the step is therefore a no-op instead of producing
  a second table.
- **Every module jar is attached as a release asset**, including `poppydb-*-cli.jar`. Sources
  and javadoc stay out — those are on Maven Central, and thirty assets on a release page help
  nobody. Artifacts come from this run's bundle staging dir, or the zipped
  `target/bundle-<version>.jar` (`--skip-to-upload`), or straight from Maven Central.

The Maven Central fallback is what makes the new `./release.sh --github-assets [version]` mode
work on releases that were cut long ago: it attaches the jars and fills in a missing body for
any existing tag, which is how v6.3.3 was repaired retroactively.


## [6.3.3] - 2026-08-18

### Fixed

#### Replica-set node cut off for good after a restart, with nothing in the leader's log
A node that restarted could stay outside its replica set indefinitely: the leader kept
"sending" heartbeats to it but never opened a socket to it again, while continuing to serve
the other peers normally. The cut-off node saw no leader, ran PreVote rounds forever without
ever winning (a node the healthy leader is still in contact with is denied by every voter),
and could therefore neither lead nor follow. Restarting the affected node did not help —
only restarting the *leader* did, which is the opposite of where the symptom appeared.

The cause was a chain of four layers that each swallowed the problem. `ElectionNetworkClient`
caches one driver per peer and handed the cached one out without checking whether it still
worked. Such a driver cannot recover on its own, because `SingleMongoConnectDriver.close()`
nulls the connection *and* cancels the driver's own heartbeat. `getConnection()` then returned
a `ConnectionWrapper` around `null` — an object that is not `null`, so the client's null check
passed it through — whose first use threw a plain `RuntimeException`, which the
`MorphiumDriverException`-only eviction did not catch. The failure was finally logged at TRACE.

Peer connections are now validated before reuse and evicted on any exception, one dial
attempt per tick replaces the driver's own retry loop (which only piled up blocked threads
against a peer that is simply down), concurrent dials no longer leak the loser, and an
unreachable peer is reported at WARN with a matching INFO once contact returns — a leader that
cannot reach a follower now says so instead of failing silently.

#### SingleMongoConnectDriver could end up permanently dead (#310)
The same driver defect, fixed at its source, because it is reachable for any consumer:
`Morphium` selects this driver whenever no driver name is configured. After a connection loss
the driver's own recovery path ran `close(); connect();` — and since `close()` cancels the
heartbeat (interrupting the very thread performing the recovery), a failed `connect()` left
the driver with no connection, no scheduled repair and no way back, handing out unusable
connection wrappers from then on. The self-repair that exists in `getConnection()` could not
help: it requires a non-null connection, and the fatal state is precisely a null one.

`getConnection()` no longer returns a wrapper around `null`; it reconnects or throws
`MorphiumDriverException`. The recovery path closes only the connection and keeps the
heartbeat scheduled, so the driver retries on every tick, and a released connection now
reports `MorphiumDriverException` instead of a plain `RuntimeException`, so callers keying
their retry and failover handling on the driver exception type actually see it.

**Behaviour change:** a closed driver is now revivable — `getConnection()` on it connects
again instead of returning a broken wrapper.

#### Discarded drivers kept a scheduler thread and could revive themselves (#311)
Every driver owns a private scheduler whose threads are named `SCCon_*`, and `close()` never
shut it down, so each discarded driver cost one idle daemon thread for the lifetime of the
process — a node that repeatedly redials a flapping peer accumulates one per cycle. `close()`
now shuts the scheduler down, and a driver that is used again builds a fresh one.

Peer connections in `ElectionNetworkClient` additionally run with the driver's own heartbeat
switched off, and their host seed is pinned back to the one peer they were dialed for.
Connection liveness belongs to the election client now, which probes and redials on every tick;
the driver's recovery task, running on its own schedule, could otherwise reconnect a driver
that had already been evicted and leave it behind as an orphan holding an open socket.

The seed pinning closes a sharper edge of the same mechanism. A successful connect enlarges the
seed to every replica-set member — the responder's own address first — and a failed connection
attempt walks to the next seed entry, accepting whatever answers. A driver dialed for a peer
that happened to be down therefore attached to a different node while the caller went on
believing it had reached the peer. In the 2026-08-18 incident this turned a candidate's vote
request for a restarting peer into a request answered by the candidate *itself*, counted under
the peer's name: a 2/3 majority that no peer had granted, which made a node with an empty log
primary against the explicit denial of the only up-to-date node.

#### An election could deadlock with no electable node at all (#312)
The priority check in `ElectionManager` was an absolute veto: a voter that could lead itself
denied every lower-priority candidate. Together with the log-recency veto this can leave a
replica set where every candidate is denied by someone, permanently — as happened on
2026-08-18, when the only node with real log state held the lowest priority while the
higher-priority nodes were fresh restores reporting index 0. Neither veto is a race; both are
stable properties, so no amount of retrying dissolves the situation. It took restarting all
three nodes at once, which works only because they then all report index 0.

Priority is now a preference with a time budget rather than a veto: after the cluster has been
leaderless for three of the voter's own maximum election timeouts, priority alone no longer
denies a candidate, and an INFO line records that the escape hatch fired. A healthy cluster is
unaffected — an election completes well inside that window — and the window re-arms on every
heartbeat, so repeated failovers each get the full preference. Priority takeover continues to
hand leadership back once the preferred node becomes electable again.

#### Test-results report: skipped tests were invisible, and no record ever qualified for a tag
Two independent defects in the test-results reporting made the release table and the README
badge misleading.

**Skipped tests had no column.** The record builder computes `passed = methods - failed -
skipped` and stores `skipped` in every record, but `test_report.py` rendered only `Tests` and
`Passed` — so a release table showed e.g. 2114 tests and 2100 passed with nothing accounting
for the difference, reading like 14 silently lost tests. The rendered table now carries a
`Skipped` column, so `Tests = Passed + Skipped` is visible on its face.

**No test record could ever qualify for a tag commit.** A record counts for a target commit
only if every path in the diff between them is allowlisted as "does not change the released
artifact". The maven-release-plugin rewrites the project version in every `pom.xml` when it
cuts a tag, and `pom.xml` is deliberately not path-allowlisted — a changed dependency or
plugin version there absolutely does change the artifact. The consequence was that *every*
tag commit disqualified *every* record: the badge sat at `0/5 phases, 0 passed` in red no
matter how green the matrix was, and `updateReleaseReport.sh` hit its "no qualifying results"
guard on every run, so the release notes never refreshed — the "living report" was live in
name only. (Release notes still looked right at release time purely because `release.sh`
renders them against `HEAD` *before* the version bump.)

`pom.xml` is now judged by content rather than by path: its canonical XML is compared with the
project's own `version`, the `parent` version and the `scm` tag blanked out, so a pure release
bump no longer disqualifies a record. Only those three fields are blanked — a `<version>`
inside a `<dependency>` or `<plugin>` still disqualifies, as it must. The comparison strips
whitespace-only text, because the release plugin also reflows the `<project>` element's
namespace attributes onto one line. Anything unparsable, added or removed fails closed and
disqualifies: the check hands out permission to *ignore* a diff, so uncertainty must never
mean "ignore it".

#### Coverage badge removed from the READMEs
It rendered as `custom badge | resource not found`: `badges/coverage.json` is only written when
a test-results record carries coverage data, and `runtests.sh` invokes the record builder
without `--coverage-xml`, so no record ever has any. The badge is removed from both READMEs
until coverage is actually collected in the test runs — a broken badge in the header is worse
than no badge. The `--coverage-xml` path in `test_results_record.py` and the badge writer in
`test_report.py` are unchanged and ready for the day coverage data starts flowing.

## [6.3.2] - 2026-08-18

### Added

#### PoppyDB: byte budget for the secondary-side replication event queue (`--event-queue-budget`)
The queue a secondary buffers incoming change events in before applying them was count-capped
(100k events) but unbounded by bytes — with ~300KB bulk-export messages on a busy message bus,
100k queued events blow any heap, the same failure family as the replay-buffer incident the
`--replay-buffer` budget fixed. Unlike the replay buffer, evicting is not an option here:
queued events have not been applied yet, dropping one would be silent data loss on that
secondary. The byte budget therefore extends the queue's existing count backpressure to bytes:
once the estimated queued bytes (same `estimateBsonSize` estimate as the replay buffer) would
exceed the budget, the change-stream reader blocks until the apply side drains — exactly the
semantics the count capacity always had, including during initial sync (the snapshot runs on
its own thread and connections, so blocking the watch reader cannot deadlock it). An event
larger than the whole budget is always admitted into an empty queue, so it can never block
forever. Configured via `--event-queue-budget` / config key `event-queue-budget` with the same
size syntax as `--replay-buffer` (`k`/`m`/`g`, percent of max heap, `0` = byte cap off;
default 256m); replication stats report `eventQueueBytes`, `eventQueueByteBudget` and
`eventQueueBytePressureCount` (how often the reader had to wait).

#### `usersInfo` — `db.getUsers()` now works against PoppyDB
Listing users in mongosh failed with "no such command: 'usersInfo'": the in-memory driver
implemented `createUser`/`updateUser`/`dropUser`, but not the command every user-listing helper
sends. It reads the same `admin.system.users` documents and answers in mongod's shape,
supporting the argument forms mongod takes (`1`, a name, a `{user, db}` document, a list of
those) plus `forAllDBs`. Stored credentials stay out of the answer unless `showCredentials` is
requested — listing users must not hand out password material — while the available SCRAM
mechanisms are always reported, since clients need them to authenticate. An unknown user
yields an empty list rather than an error, as mongod does.

#### Decoupled test-results store, release report and badges
Test runs (full CI phases as well as partial developer runs) can now publish a JSON record
of their results to the append-only `test-results` orphan branch via
`runtests.sh --publish-results` — decoupled from the machine that produced them, so any
contributor can supply results without homelab infrastructure. `release.sh` aggregates the
records per (commit, phase) — newest run wins, only complete phase runs qualify, results
from earlier commits stay valid when only docs/tests/tooling changed since — and posts the
honest result table to the GitHub release notes, missing or broken phases included, plus
optional JaCoCo coverage (from `-Pcoverage`). The report is a *living* one: it is not frozen
at release time. The markdown section is wrapped in `<!-- morphium-test-report:start/end -->`
markers, and `scripts/updateReleaseReport.sh` — called best-effort after every
`runtests.sh --publish-results` — resolves the latest (or a given `--tag`) release, replaces
that marked section in its GitHub notes with a report for the *tag's* commit, and regenerates
the `tests`/`coverage` badges into the `test-results` store branch, so both the release notes
and the README badges (now served from `.../test-results/badges/*.json` instead of `master`)
keep refreshing automatically as new results come in, without another release being cut.
This is a report, not a gate: `release.sh` never aborts on an incomplete or red matrix, it
just says so in the release notes ("Transparenz statt Türsteher"). The aggregator itself
(`scripts/test_report.py`) still exits 0/1/3 for complete-and-green / gaps-or-broken /
store-unreachable, so a future caller or CI job that *does* want to gate on the matrix can
build that policy on top without changing the tool. Coverage records themselves are produced
by whatever runs `-Pcoverage` and passes `--coverage-xml` to `runtests.sh --publish-results`
— the CI orchestrator wiring for that is a follow-up; for now it's manual runs.

#### PoppyDB: honest capability advertisement in the hello reply (`poppyCapabilities`)
The hello reply advertises replica-set topology and logical sessions, which makes modern
drivers enable retryable writes by default — a capability PoppyDB does not have (no
`(lsid, txnNumber)` deduplication; the road to real support is specced in #293). There is no
standard hello field to say "sessions yes, retryable writes no", so the reply now carries an
explicit `poppyCapabilities` document (`retryableWrites: false`, `journal: false`,
`durability: "snapshot"`, `readConcern: "local"`, `transactions: "partial"`,
`textSearch: "simplified"`). Non-Morphium clients should connect with `retryWrites=false`;
documented in `docs/poppydb.md` together with the other honesty changes below.

#### PoppyDB: mongodump/mongorestore work against PoppyDB (mongo-tools compatibility)
`mongorestore` against a PoppyDB used to die at the handshake, and dumps of real-world schemas
could not be loaded at all. A restore is the natural way to seed a PoppyDB from an existing
MongoDB (and a dump the natural way to persist one), so the whole tool chain was fixed
end-to-end; a full dump → restore → dump round trip including secondary indexes now passes.
Individual fixes, each observable on its own:

- The legacy `isMaster` (OP_QUERY) reply carried the `QueryFailure` flag, making strict drivers
  (mongo-tools' Go driver) treat the hello document as an error and drop the connection.
  Lenient drivers (Node, morphium) ignore OP_REPLY flags, which is why this never surfaced.
- `buildInfo` now reports a `versionArray` — mongorestore refuses servers announcing fewer
  than 3 version components.
- OP_MSG kind-1 document sequences (how mongo-tools ship bulk inserts; morphium clients only
  ever send kind 0) are now merged into the command body per wire spec. The kind-1 *writer*
  in `OpMsg.getPayload` was rewritten as well — it never emitted the section content.
- `OpMsg.parsePayload` bounds parsing by the wire-header message size instead of the buffer
  length: with PoppyDB's zero-copy Netty path, a pipelining client (mongo-tools) made the
  parser run into the next message's bytes.
- BSON type 0x13 (Decimal128) is now encoded and decoded (`BigDecimal`, NaN/Infinity as
  `Decimal128`) — previously any document containing a `NumberDecimal` was unparsable.
- A message that fails to decode now gets an error reply instead of being silently skipped,
  which left clients hanging until their timeout.

### Changed

#### Object mapper: type-id class resolution and no-arg-constructor lookup cached
An in-JVM mapping benchmark (POJO with a `List<List<Map<String,Customer>>>` payload, no
network) showed `ObjectMapperImpl` roundtrips at ~100µs/op — 3.3x slower than the official
driver's `PojoCodecProvider`. Profiling (JFR, 1ms sampling) put the single biggest avoidable
cost in `AnnotationAndReflectionHelper.getClassForTypeId()`, which ran
`Class.forName()` on every call — once per embedded object carrying a `class_name`
attribute, i.e. dozens of times per deserialized document. That lookup is now cached per
helper instance (typeId → Class, successful lookups only, so hot-reload scenarios get a
fresh cache with a fresh helper). In addition, `deserialize()` now caches the resolved
no-arg constructor per class (with a sentinel for classes without one, so the
exception-based probe runs once instead of per call — measured at ~0.4µs per miss), and the
hot `customMappers` checks use a single `get()` instead of `containsKey()`+`get()`.
Deserialization of the benchmark payload drops from ~57µs to ~38µs (−34%), full roundtrip
from ~100µs to ~76µs; the remaining gap to `PojoCodecProvider` (~2.5x) is structural —
per-value map lookups against per-class precompiled codecs. Behavior is unchanged.

#### Test suite: timing-sensitive sleep+assert patterns replaced with condition waits (#292)
A `BulkInsertTest` flake on the CI matrix (count asserted immediately after `storeList`) turned
out to be one instance of a suite-wide pattern: `Thread.sleep` followed by an assertion on DB or
messaging state. The nine files with the highest density — BulkInsertTest, MorphiumTest,
MapListTest, DataTypeTests, QueryUpdateOperatorsTest, UpdateTest, CacheSyncTest and the two
(class-level disabled) ncmessaging suites — now use bounded
`TestUtils.waitForConditionToBecomeTrue` waits instead; unbounded poll loops got bounds too.
Sleeps that are load-bearing (negative "must-NOT-arrive" windows, exactly-once settle windows,
TTL waits, pause-semantics and throughput measurements) were deliberately kept. No production
code affected; the remaining sleep+assert files are tracked in #292.

#### Test suite: retired the ncmessaging (polling-only) test package (#292)
The `ncmessaging` suites were aging copies of the regular messaging tests with
`setUseChangeStream(false)` hard-coded — mostly class-level `@Disabled` and drifting. The
polling-only mode itself stays fully supported (it is what morphium auto-selects on standalone
MongoDB, where change streams don't exist) and remains tested: the MongoDB-Single CI phase runs
the entire messaging test set in exactly that mode. The one scenario without a counterpart —
request/reply round trips forced to polling on a replica set — moved to
`AnsweringTests.waitForAnswerPollingOnlyTest`.

#### Test suite: all bare `assert` statements migrated to JUnit assertions (#292)
1114 bare Java `assert` statements across 97 test files only ever ran because surefire enables
`-ea` by default — as `assertTrue(...)` they are independent of JVM flags and produce proper
assertion errors. Messages are preserved; dynamic messages keep the `assert` statement's lazy
evaluation via supplier arguments (except where a lambda could not capture the local, which use
eager `String.valueOf`). Behavior-preserving by construction: assertions were already enabled in
the test JVMs.

#### Messaging: "CHANGESTREAM DUPLICATE CAUGHT" dropped from WARN to DEBUG
The guard fires whenever the change stream and the fallback poll both find the same message,
which at a 10s fallback interval is simply normal operation — production logs showed ~135 lines
a day of it, burying the handful of warnings that actually matter (found during the #285
analysis). The deduplication behavior is unchanged, only the log level.

### Fixed

#### PoppyDB election: a failed retry-persist forgot a durably granted vote (#306 review round 2)
The persist-failure rollback in `handleVoteRequest` reset `votedFor` to null unconditionally.
On a *retry* from the candidate the node had already durably voted for (Raft standard: the
response got lost, the candidate asks again), a transient persist failure therefore erased the
earlier, still-durable vote from memory — and a *second* candidate asking next could be
granted the same term: two votes in one term, two leaders. The rollback now restores the
previous `votedFor` (the same pattern `becomeCandidate` already used), so a failed persist
denies the retry without forgetting the vote that actually stands.

#### PoppyDB election: a leader demoted by a straggling higher-term vote response went permanently silent (#306 review round 2)
The higher-term check in `handleVoteResponse` runs before round correlation (correct — a
higher term is authoritative whatever RPC carried it), but demoted with `resetTimer` only for
candidates. A node that had already *won* (its election timer cancelled by `becomeLeader()`)
ended up as a follower with neither heartbeats to receive nor an election timer to fire: if
the higher-term peer never made contact (died, partitioned), the node never campaigned again.
Every other leader-demotion path re-arms the timer; this one now does too.

#### PoppyDB election: the partial-restore guard deadlocked peer-less nodes, and only the CLI ever armed it (#306 review round 2)
Two halves. First, the guard's only release path is a completed initial sync *from a primary*
— which a single-node replica set can never have: one broken dump file and the node held back
candidacy forever, with no runtime override. A node without peers now skips the hold-back
(there is no one to sync from and no intact peer a partial primary could overwrite), and the
CLI's PARTIAL-RESTORE warning explains the manual way out (restore or delete the broken dump
files, restart). Second, only `PoppyDBCLI` called `setLocalDataComplete(false)` — an embedder
following the documented pattern (`restoreFromDump()`, check `isComplete()`, `start()`) booted
a gutted node that still considered itself electable, recreating the empty-node-wipe after a
cluster-wide restart. `restoreFromDump()` itself now drops the guard on a partial result.

#### PoppyDB: the replication-manager field was assigned after start(), losing fast sync-complete notifications (#306 review round 2)
The initial-sync completion notification is one-shot (`maybeFireSyncCompleteNotify` consumes
its flag via CAS). On a fast sync — e.g. the consistency shortcut against a loopback peer —
the batch processor could fire it before `startReplicationToLeader` assigned the new manager
to the field; the receiver then discarded the release as coming from a superseded manager, and
the partial-restore guard stayed stuck until some unrelated resync. The field is now assigned
before `start()` (as the static-mode path already did), with the assignment rolled back if
`start()` throws.

#### PoppyDB election: the state-file quarantine bricked every upgrade from a pre-checksum build (#306 follow-up)
The mandatory three-key state-file schema (currentTerm, explicitly-empty votedFor, CRC32
checksum) quarantined *every* file written by the immediately preceding builds — which wrote
no checksum at all and omitted votedFor when null. On upgrade, all nodes of an RS therefore
came up "holding back candidacy: persisted election state exists but is unreadable" at once:
no candidate, no primary, every client failing with "No primary node found" (observed
cluster-wide on the testrunner RS, 2026-08-17). A missing checksum *key* is now recognized as
the legacy signature — checksum-era files are written atomically (tmp+move) and cannot lose
single lines undetected, so "no checksum key" means an older build's complete write, not a
truncation. Legacy files are restored (votedFor optional, exactly as the legacy writer
produced them) and immediately rewritten in the current format, closing the unprotected
window; empty files, files without currentTerm, checksum mismatches and checksum-era files
missing votedFor are still quarantined as before.

#### PoppyDB election: a vote could be granted without being durable, and a broken state file reset the node to term 0 (#306)
`persistElectionState()` swallowed every write failure, yet the voter confirmed the vote (and
a candidate its self-vote) anyway — despite the "votedFor must be durable before the response
leaves" contract at exactly that call site. A crash after such a phantom persist lets the
restarted node forget its vote and vote a second time in the same term: two leaders in one
term, the one failure mode Raft's persistence rule exists to prevent. A failed persist now
turns the grant into a denial (votedFor rolled back in memory too) and aborts a candidacy
outright, term increment included — the node simply retries on the next election timeout.
Relatedly, `loadPersistedState()` treated an *existing but unreadable* state file like a
missing one and restarted at term 0, arguing PreVote makes that safe — which holds for term
inflation, not for double voting: an unreadable file means the node may have voted at any
term. The two cases are now distinguished: a missing file (first start, persistence newly
enabled) still starts clean and participates normally, while an unreadable one keeps the node
out of elections entirely (no votes, no PreVote grants, no candidacy — it still starts and
serves data) until the operator restores the file or deliberately deletes it; the condition is
logged on ERROR with those instructions and exposed as `stateFileUnreadable` in the election
stats. The quarantine is also self-preserving: while it holds, nothing writes the state file —
without that, the next higher-term heartbeat would run through `becomeFollower()` →
`persistElectionState()` and overwrite the broken file with the made-up in-memory state,
perfectly readable on the next restart, silently lifting the quarantine while the unknown
earlier vote stays lost (the untouched file is also the operator's evidence). And "durable"
now means durable across power/kernel failures too, not just JVM crashes: the state write
fsyncs the tmp file before the atomic rename and the parent directory after it (directory
fsync best-effort, since not every platform supports it). The persisted state also carries a
mandatory schema now — `currentTerm`, an *explicitly empty* `votedFor` when no vote is held,
and a CRC32 checksum over both — and a file missing any key or failing the checksum is
quarantined like an unparsable one: `Properties.load()` happily parses an empty or truncated
file, and the old `getProperty("currentTerm", "0")` default would have quietly turned "file
lost its content" (possibly including the votedFor line for a term whose vote is already
given away) into "term 0, never voted". Bare term adoption stays best-effort by design:
losing an adopted term to a crash costs no safety, because every grant re-persists both
values or is denied.

#### PoppyDB election: the partial-restore candidacy guard was never released in election mode (#306)
A node that starts with an incomplete dump restore is barred from candidacy until an
authoritative initial sync has replaced its local state (it would otherwise win a
cluster-wide-restart election — where every node reports index 0 — and push its gutted
dataset onto the intact peers). But the release of that guard lived only in the static-mode
replication path (`startReplication()`, with its synchronous `waitForInitialSync`); election
mode replicates through `startReplicationToLeader()`, which had no sync-completion hook at
all. So the guarded node synced fine and then stayed barred forever: with an intact primary A
and partially-restored B and C, everything worked until A died — then B and C refused every
candidacy and the cluster stayed without a primary despite both holding full authoritative
copies. `ReplicationManager` now exposes an `onInitialSyncComplete` hook wired by both
replication paths to the release — which also fixes the static path's own gap of a sync
finishing only after the bounded 30s wait had given up. The hook's firing point is
deliberately *not* the gate-opening moment: "initial sync complete" there only means the
snapshot is copied, while the change events buffered during it (up to 100k) are still queued
— a guard released that early hands candidacy back to a node that is measurably behind, and
if the primary dies inside that window the node can win the election while its stop()-time
flush only applies a single further batch. The sync thread therefore only *arms* the
notification and the batch processor fires it once the backlog has actually drained — and
only while that manager is still running: a superseded `ReplicationManager`'s sync thread can
outlive `stop()` by design (bounded 5s join) and complete its snapshot against a primary that
no longer leads, so a stopped manager never fires, and the receiving side additionally
ignores completions from any manager instance that is no longer the current one.

#### PoppyDB election: vote responses from earlier rounds were credited to the current PreVote round (#306)
`handleVoteResponse()` tallied every incoming grant into whatever round happened to be open —
no round correlation, no request-type check. In a three-node set, the self-vote plus one
grant straggling in from an *earlier* PreVote round already forms a "majority", starting a
real election (term bump included) that no current peer agreed to — under network latency the
livelock PreVote was built to end could return through this side door. Every outgoing batch
of (Pre)Vote requests now carries a sender-local round id (never serialized: the response
travels back on the same code path that sent the request, so `ElectionNetworkClient` simply
hands the original request back with the answer), and responses whose request is not of the
current round — or answers the wrong kind of request — are discarded. Higher response terms
are still honored before correlation, from any round: discovering a higher term is
authoritative cluster news whatever RPC delivered it. Within the current PreVote round, lower
response terms remain deliberately acceptable — a voter whose term is behind may legitimately
pre-grant, since PreVote adopts no terms on either side.

#### InMemory dump restore: ORM-written documents made a whole database unrestorable (#306)
On the customer acceptance environment, 4 of 8 databases could not be restored from freshly
written dumps — exactly the ones containing ORM-written documents. Those carry a `class_name`,
which made the restore path run them through the entity-aware `ObjectMapperImpl`
deserialization: any entity field of a dump-marked type (`Date`, `UUID`, `byte[]`, ids) that
was *absent* from a document handed `null` to the restore type mapper and NPE'd the entire
database ("Parsing failed … 'd' is null") — and even without absent fields, entity resolution
would have replaced the stored document maps with entity objects and dropped their
`class_name`. The restore now converts the dump payload at the dump boundary itself, without
any entity resolution: documents stay plain maps (`class_name` preserved as an ordinary
field), and only the `{class_name, value}` marker maps the dump writer emits are turned back
into `Date`/`UUID`/`byte[]`/ids. Id markers are read tolerantly — both the
`org.bson.types.ObjectId` form that every existing dump on production machines contains and
the `de.caluga.morphium.driver.MorphiumId` form are accepted, and both restore as
`MorphiumId`, matching what the wire path delivers. The wire and store paths are untouched
(the store legitimately holds both id types; the translation happens only at the dump
boundary). Restore failures now name the offending field path and marker type instead of a
bare "Parsing failed", and `ObjectMapperImpl` itself no longer hands `null` to custom field
mappers for absent fields (the standard null handling applies instead) and wraps mapper
failures with the field name, field type and entity class.

#### PoppyDB replication: a freshly-synced node could report log index 0 forever (seed race)
On a loaded host, a secondary whose initial sync finished fast (consistency shortcut, tiny
dataset) could permanently report replication position 0 to the election layer despite holding
the primary's complete dataset. Root cause is a race between two one-shot reporters on
different threads: the watch's registration callback flipped `watchLive` — which is what
releases the initial-sync thread — *before* recording the primary's sequence seed, so a sync
that outran that gap re-based `lastAppliedSequence` from the still-stale (0) seed and its
single end-of-sync election report read 0 and was skipped; when the seed then landed, nobody
reported it anymore, because the only remaining reporter (`processBatch`) fires solely when
live events are actually drained — a quiet primary means never. Recent sync speedups made the
sync win this race often enough to surface as the CI-only `InitialSyncElectionSeedTest`
failure. The consequence is severe since #306: its candidacy restraint treats index 0 as "empty
node, must not campaign", so the raced node locked itself out of every election — if the other
nodes fail, the cluster stays leaderless while the one node with the full data sits it out,
the same damage class #306 closed, from the other side. Fixed structurally, not at one spot:
the position now *catches up* instead of being reported exactly once — the registration seed
reports itself when it lands after sync success (gated on `initialSyncComplete`, so a node
that does not yet hold the data still never claims a position), the batch processor's flush
tick reconciles the election view periodically even with nothing to drain (safe because
`ElectionManager#updateLogIndex` is monotonic-max, so re-reporting can only ever raise), and
the registration callback now records the seed *before* flipping `watchLive`, closing the race
window at its source. `InitialSyncElectionSeedTest` gained a deterministic reproduction of the
losing interleaving, so the regression no longer needs CI load to become visible.

#### PoppyDB election: PreVote stops empty/syncing nodes from dethroning a healthy primary (#306)
A rolling upgrade on a 3-node replica set ended in a permanent leaderless livelock: a freshly
restarted, still-empty node could never *win* an election — the log-recency vote veto worked —
but it kept *campaigning* every election timeout, each campaign bumping the term, and every
higher-term RequestVote forced the healthy primary to step down (~15 terms/min, primary
flapping, finally no writable primary at all). This is Raft's textbook "disruptive server"
problem: the vote veto prevents the wrong winner, not the disruption. The election now
implements PreVote (Raft §4.2.3/§9.6): before any real election, the node asks its peers
"would you grant me a vote?" *without touching any term*; only a pre-granted majority starts
the real election. The PreVote answer is strictly read-only on the responder (no term
adoption, no votedFor, no timer reset) and applies the same log-recency, term and priority
checks as a real vote — so an empty or log-behind candidate fails the round every time and
retries forever without inflating a single term. Three companion changes close the remaining
gaps: **leader stickiness** — a voter that is the leader with a live lease, or heard a leader
heartbeat within the last minimum election timeout, ignores higher-term (Pre)Vote requests
*without adopting their term* (the term adoption on denial was exactly the dethroning lever,
and this also shields new nodes from old, PreVote-unaware campaigners during rolling
upgrades); **candidacy restraint hardening** — a peer's advertised log index is now recorded
even from heartbeats rejected as stale-term, so a node whose own term got inflated can no
longer blind itself to the existence of data-bearing peers (pre-PreVote, a candidate rejected
all heartbeats as stale and thus never learned it should hold back); and **term/votedFor
persistence** — Raft-required, opt-in via `morphiumserver.electionStatePath` (a properties
file written atomically on every term/votedFor change), because a node that came back at
`term=0` during the incident added to the churn; a node without (or with a corrupt) state
file still starts cleanly, which PreVote now makes safe. Wire-compatible with old nodes: the
PreVote probe rides as an extra `preVote` field on the existing `requestVote` command,
carrying the sender's *current* term, so an old node misreads it as a harmless same-term vote
request and its plain grant/deny counts toward the PreVote majority — a new node in an old
cluster is never blocked.

#### PoppyDB: restore-on-startup silently aborted on the first broken dump file, starting the node near-empty (#306)
During a rolling upgrade, a node whose shutdown had correctly dumped all 8 databases came back
with only 2 of them: `restoreAllFromDirectory` looped over the dump files without any per-file
error handling, so the first file that failed to parse threw straight out of the loop — the
databases already restored stayed, everything after the broken file was silently skipped, and
the node joined the replica set as a near-empty (and, per #306, election-disrupting) member.
The failure was invisible three times over: the loop never logged which file broke, the
summary line lived *after* the call and thus never appeared, and the CLI's catch logged only
`e.getMessage()` — which for the actual `RuntimeException("Parsing failed")` says nothing, and
for an NPE is literally `null`. Losing 6 databases because 1 file is broken is the wrong
trade for a startup restore, so each dump file is now restored under its own try/catch: a
broken file is logged on ERROR with its name and full stack trace, all remaining dumps are
still attempted, and a summary line is *always* emitted — INFO (`Restored N of N`) when
complete, an unmissable WARN with restored/total counts and the failed file names when
partial. `PoppyDB.restoreFromDump()` now returns that result instead of a bare count, and the
CLI uses it to log its own PARTIAL-RESTORE warning (with stack traces in the residual failure
path) rather than treating any non-exception as success. Note the restore itself always ran
synchronously *before* `start()` wires up replication and election — the suspected race with
the ElectionManager did not exist; the node joined empty purely because the aborted loop
reported nothing.

#### InMemoryDriver/PoppyDB: dumps of any database with real data were unrestorable — "Parsing failed" (#306)
The fault-tolerant restore above immediately surfaced the bug it had been hiding: on the
customer acceptance environment 5 of 8 databases failed to restore with
`RuntimeException: Parsing failed` — exactly the data-bearing ones, while the quasi-empty ones
went through. The dump writer (`Utils.writeJson`) never produced parseable JSON for real
content: strings were written verbatim (one quote, backslash or control character in a news
text and the JSON is broken — json-simple: `Unexpected character (S) at position 60`), and
`Date`/`UUID` values were written as bare unquoted `toString()` tokens (`Mon Aug 17 ...` —
`Unexpected character (M) at position 40`), so a single timestamp field was enough to lose the
whole database. On top, `byte[]` silently came back as a `List<Long>` and
`MorphiumId`/`ObjectId` ids as plain `String`s — documents unfindable by id after a restart —
and both sides of the roundtrip used the platform default charset (`new OutputStreamWriter(gzip)`
/ `new InputStreamReader(bin)`), so a dump written under one default and read under another
mojibake'd every umlaut without any error. Dumps are now written as UTF-8 with proper JSON
string escaping, and Date/UUID/ObjectId/byte[] as `class_name`-marked maps the restore converts
back into the exact storage types (ids restore as `MorphiumId`, which is what the wire protocol
stores and queries compare against). The restore side reads UTF-8 strictly and falls back to
ISO-8859-1 with a WARN for legacy dumps written under a non-UTF-8 platform default, and decodes
the stream as a whole instead of the old `readLine()` join that silently deleted raw newlines
inside string values. `Utils.writeJson` itself now escapes strings too (it also feeds
`@Encrypted` field serialization and log output), and `ObjectMapperImpl.deserialize` includes
the wrapped cause plus the JSON context around the parse position in its message — a bare
"Parsing failed" through a `getMessage()`-only log line is how this bug stayed invisible in the
first place. Honest limits for existing dump files: legacy dumps restore as far as they ever
could — content whose strings contain quotes/backslashes or that carries `Date`/`UUID` values
was written as structurally broken JSON by the old code and cannot be recovered; legacy dumps
without those (plain text, numbers, ids) restore fine, now even with correct umlauts across
platform-default changes and with raw newlines preserved.

#### PoppyDB CLI: election-state persistence was silently inactive — dump directory was set after `configureReplicaSet()` (#306)
The term/votedFor persistence introduced for the #306 election churn never engaged on the
customer environment: no `election-state.properties`, not even the "Election state persisted
to" log line. `configureReplicaSet()` is the place that derives the state-file path from the
dump directory and bakes it into the `ElectionConfig`, but the CLI set the dump directory only
afterwards, in the persistence/restore block — so the config never got a path and neither
persisting nor loading ever ran. The CLI now sets the dump directory before configuring the
replica set (the restore itself still runs synchronously before `start()`, unchanged), and
`PoppyDB.setDumpDirectory()` logs an unmissable WARN when it is called after an
election-enabled `configureReplicaSet()` without persistence, so embedded users cannot fall
into the same silent ordering trap.

#### PooledDriver: a client could stay stuck on "No primary node found" forever after a replica-set restart sequence (#304)
Nine service instances kept failing every operation for 30+ minutes after their PoppyDB
replica set had been restarted node by node, and only an application restart brought them
back — while other instances of the same services recovered on their own. Nothing but the
heartbeat ever sets `primaryNode`, so anything that stops the heartbeat from probing turns a
temporary outage into a permanent one, and two independent defects could do exactly that.
First, the heartbeat's whole cycle ran unguarded inside `scheduleWithFixedDelay`, which
cancels a periodic task for good the moment one execution throws — one unexpected failure
(creating a platform thread can fail with an `Error` under load) and discovery was over.
Second, the per-host check registered its bookkeeping entry *after* starting the thread,
while the thread removes its own entry when it finishes: against a host that refuses
connections the check completes in microseconds, so its removal could run before the
registration, leaving an entry that no later cycle ever clears — and every later cycle skips
a host it believes is already being checked. The cycle is now wrapped so nothing escapes it,
the claim is written before the thread starts (and removed again if the start fails), a stale
claim whose thread is no longer alive heals itself on the next cycle, `close()` clears the
bookkeeping, and asking for the primary restarts a heartbeat that is no longer scheduled —
so discovery can resume from every state, which is what a driver must guarantee.

#### InMemoryDriver/PoppyDB: index buckets leaked every deleted document whose indexed array or sub-document had been updated (#303)
A PoppyDB message bus ran its 12 GB heap over the watermark and rejected all writes for ~36h;
the dump showed a single `CollectionIndexStore` retaining 10.5 GB in 34,859 long-deleted
documents. The cause was an index key that keeps changing after it has been filed: `IndexKey`
stored the document's own `List`/`Map` instance as the key's value while `equals`/`hashCode`
are content-based, and the driver mutates documents in place — `$push`/`$addToSet` append to
that very list, a dotted-path `$set` writes into that very map. The key is a `HashMap` key in
the bucket map, so the moment the document is updated, the filed key no longer matches: the
lookup either misses the bin or fails `equals` against the mutated stored key, removal
silently no-ops, and the bucket keeps the document forever. Messaging is the perfect trigger —
every message is inserted with an empty `processed_by` list (part of five of `Msg`'s indexes)
and gets a push on it before being deleted, so every processed message leaked its full
payload. Keys now snapshot mutable container values deeply when they are extracted, which
keeps hash, `equals` and the comparator consistent for the key's whole lifetime. Note this was
*not* the reference-identity removal suspected in the issue: every caller of `onRemove` passes
the live document, and the bucket iteration that compares by identity is never even reached.

#### Messaging listener registration could silently drop listeners (and throw an NPE)
`SingleCollectionMessaging` and `DualChannelMessaging` published their topic→listener map
lock-free to the poll thread, which is why it was only ever written by clone-and-swap on a
`volatile` field. That read-modify-write was unsynchronized, though: two writers cloning the
same map made the later swap discard the other's entry. The visible symptom was an NPE in
`addListenerForTopic` when a just-created entry vanished between the `contains()` check and
the `add()` — the flaky `MessagingRequeueEventTest`, where the application thread registers
its listener while the freshly started messaging thread installs the status-info listener as
its first action in `run()`. The silent variant is worse and was never diagnosed as such: no
exception, the listener is simply gone and its messages are never delivered. Any application
registering listeners from more than one thread, or registering right after `start()`, could
hit it. The map is now a `ConcurrentHashMap` of `CopyOnWriteArrayList`s mutated under the
map's per-key lock, so concurrent registration composes and the lock-free iteration in the
poll thread stays safe without the clone-and-swap discipline that was easy to violate.
One deliberate behaviour change came out of this: installing the status-info listener now
*adds* it to whatever is registered under its name instead of replacing that entry, and
disabling it removes only the status-info listener instead of the whole topic — an
application listener that happens to use the status-info name is no longer silently thrown
away on `start()`, and `isStatusInfoListenerEnabled()` can no longer report `true` while no
status listener is installed. `setStatusInfoListenerName()` is in exchange no longer atomic —
it now removes under the old name and installs under the new one in two steps, so a status
query hitting the nanosecond-wide gap between them goes unanswered.

#### `MultiCollectionMessaging.removeListenerForTopic()` removed the wrong listener
The lookup walked the topic's entries with an index that kept counting when no match was
found, so removing a listener that was never registered for that topic silently evicted the
*last* one instead — including terminating its change stream monitor, leaving the topic
subscribed-but-deaf. Removing from a topic with no listeners at all threw an NPE. The lookup
now matches by identity or does nothing, and — like the listener maps in the other two
messaging implementations — runs under the map's per-key lock with a `CopyOnWriteArrayList`
behind it, so a concurrent registration cannot land in an entry that is about to be dropped.

#### PoppyDB: a restarted empty node could wipe the whole replica set
Reproduced kill chain: kill one node of a 3-node RS, restart it empty (fresh data dir), and it
could both win the next election and cause the surviving, data-bearing followers to drop their
local databases to match it. Two independent holes made this possible. First,
`ElectionManager`'s Raft log-recency check existed but was vacuous — `lastLogIndex` had no
production writer, so it stayed 0 on every node and an empty restarted candidate compared as
"at least as up to date" as a voter sitting on real data. Second, on the follower side, a
replication resume that finds its window already gone falls back to a full resync, and that
fallback trusted whatever the primary reported unconditionally — reconnecting to a now-empty
primary meant "wipe local data to match" with no discriminator between a legitimately empty
primary (post-`dropDatabase`) and a stale one that had simply forgotten everything.

The fix has three parts, each closing a different leg:
- **Vote safety**: the log-recency check now enforces the one invariant that can be honestly
  made without a real replicated log — a candidate reporting index 0 never wins against a voter
  sitting above 0; three empty nodes still elect cleanly on cold start. A related hole let a
  freshly-synced node still report index 0 to the election (initial sync suppresses the change
  stream, so the normal live-write feed never fired) — such nodes now seed their true position
  right after sync completes, so they neither wrongly grant votes to an empty candidate nor get
  wrongly denied candidacy themselves.
- **Candidacy restraint**: an empty node now holds off campaigning for as long as it can see a
  data-bearing peer, preventing the term churn an empty node's repeated candidacies would
  otherwise cause even after vote safety alone denies it the win.
- **Fail-closed resync**: a follower now refuses a destructive drop-to-match resync whenever
  the primary's replication sequence at registration is *behind* the sequence the follower's own
  data was last known to reflect — the discriminator that tells a restarted/stale primary apart
  from a legitimately empty one, since a real primary's sequence only ever advances, including
  across a replicated `dropDatabase`. The refusal logs an ERROR, keeps local data intact, and
  retries with watch re-registration paced at 2s and sync-loop retry backing off exponentially
  from 1s to 30s, until a genuinely caught-up primary answers or an operator intervenes; the
  replication stats now expose `refusingDestructiveResync` / `refusedResyncCount` so this state
  is observable rather than silent. Sequence knowledge now also carries over across leader
  changes — a freshly constructed replication manager used to start its own sequence at 0 and
  immediately self-seed from whatever the new leader reported, which made the guard structurally
  unable to fire on that path. Change-stream sequences are primary-local: after a successful
  sync/shortcut against a primary, a follower now *adopts* that primary's own counter as its new
  base rather than keeping the higher of the two — the old and new primaries' counters are
  unrelated numbers, and keeping a stale, inflated one made every later reconnect to that (still
  perfectly healthy) primary look like a resume-window loss, which then tripped the guard against
  the new primary's own honest, lower counter and refused every subsequent legitimate resync.

Composition note, stated plainly: the resync guard is a sequence-height heuristic, not a
lineage check. A wrongly-promoted empty primary that manages to take on enough fresh writes
before a follower reconnects could, in principle, still pass it — the guard alone is not the
safety boundary. The actual barrier against that scenario is the election-side fix: an empty
node must never be able to win the election in the first place, which is what vote safety and
candidacy restraint together guarantee — guaranteed for the single-restart case; if a majority
of nodes restart empty simultaneously, an empty node can still be elected (the fail-closed resync
then still protects each surviving node's local data, but the cluster serves empty until a
data-bearing node takes over). The resync guard is defense in depth on top of that, not a
substitute for it.

Operator note: if the *last* data-bearing node in a cluster dies permanently, the surviving
empty nodes deliberately hold back candidacy indefinitely rather than elect one of themselves —
restarting any one of the survivors clears its peer-index memory and lets the cluster elect
again, so recovery is "restart one node", not "restart the cluster".

Regression coverage: `EmptyNodeRestartWipeTest` reproduces both directions of the original bug
(empty node restarted as would-be primary, and as a would-be follower reconnecting to an empty
primary) against a real in-process 3-node replica set.

#### PoppyDB: j:true write concern no longer promises durability that does not exist
A `j: true` write concern was silently accepted and acknowledged although PoppyDB has no
journal (persistence is periodic snapshots). Like mongod running without journaling, the
write is still executed but the answer now carries `writeConcernError` code 2 (`BadValue`),
so clients relying on journal durability learn the truth instead of getting a hollow
acknowledgement.

#### PoppyDB: secondaries no longer serve reads that defaulted to primary read preference
MongoDB's default read preference *is* `primary`, but only an explicit `mode: "primary"` was
rejected on secondaries — a read without `$readPreference` was silently served, returning
possibly-stale data to a client that (by default) asked for primary consistency. Such reads
now get `NotPrimaryNoSecondaryOk` (13435), matching mongod's handling of a direct secondary
connection without `secondaryOk`. Morphium's own wire commands always send a read preference
(default `primaryPreferred`) and are unaffected.

#### InMemoryDriver: MongoDB collation strength mapped to the wrong Java collator level
MongoDB collation strength (1=primary..5=identical) was passed straight to
`java.text.Collator.setStrength()`, whose constants are 0-3. Every level was silently shifted
by one — `strength: 1` behaved as SECONDARY (diacritics significant) instead of PRIMARY — and
`strength: 4`/`5` threw an `IllegalArgumentException` instead of working at all. The values are
now mapped explicitly; Java has no quaternary level, so 4 and 5 both map to IDENTICAL, the
closest level at least as strong as what mongo promises.

#### PoppyDB: find fast path ignored the client's collation (#252 follow-up)
The #252 fix wired the request's `collation` through the update/delete/count/distinct wire
fast paths but missed `find`: a collation-aware find matched differently depending on which
internal dispatch path the request happened to take. The collation now reaches the driver on
both the single-shot and the cursor-window path, and the server-side find cursor carries it so
`getMore` refills re-execute the query with the same collation as the first batch.

#### InMemoryDriver: bulk-insert writeErrors pointed at the wrong batch positions, n overcounted
The insert path removes failed documents from its working list between its three
error-detection passes (oversize, duplicate against committed docs, intra-batch duplicate), so
every `writeErrors.index` reported after an earlier removal referred to the shrunken working
list — but clients resolve those indexes against the batch *they* sent. A parallel
original-index list now keeps the reported indexes stable; removal is position-based, which
also stops an equal-but-different document elsewhere in the batch from being dropped
collaterally. In addition, `n` was computed as `batchSize - writeErrors.size()` on both the
generic and the PoppyDB fast path — correct for unordered inserts only. An ordered insert
stops at the first error, so the never-attempted tail was counted as inserted; both paths now
derive the committed count from the first error's batch index.

#### PoppyDB: commitTransaction/abortTransaction failures were swallowed
A `commitTransaction`/`abortTransaction` that threw was only logged — the client received an
unconditional `ok:1` and believed its transaction was committed. Failures are now answered as
a mongo-shaped error (code 8 `UnknownError`, or the driver's mongo code if it attached one).
Commit/abort without an active transaction remains a lenient `ok:1` no-op; a full per-session
transaction state machine (txnNumber validation, `NoSuchTransaction`) is deliberately out of
scope here.

#### Write buffer: remove-by-query deleted only a single document
`BufferedMorphiumWriterImpl.remove(Query, multiple, callback)` accepted the `multiple` flag but
never passed it on to the queued `DeleteBulkRequest`, whose default is `multiple = false`. All
drivers translate that faithfully into `delete ... limit: 1` — so for any `@WriteBuffer` entity,
`morphium.remove(query)` and `clearCollection()` silently deleted exactly one matching document
and left the rest in place. The bug had been masked for years because the InMemoryDriver bypasses
the buffered writer entirely (`getWriterForClass`), so no in-memory test could see it, and the
one test that exercised the path against real servers (`CacheSyncTest.idCacheTest`) tolerated
lost objects until the #292 sleep→condition hardening turned its settle sleep into a hard count
assertion — which then failed on all four CI server phases and exposed the root cause. The flag
is now propagated; a regression test (`BufferedWriterTest.testWriteBufferRemoveByQuery`) covers
partial and full remove-by-query on a write-buffered entity. The dead skeleton
`driver/wire/BulkContext` (every driver call commented out, no remaining references) was removed
in the same change.

#### Messaging: legacy documents with processed_by: null are deliverable again (#291)
A stored message whose `processed_by` is an explicit `null` made the pre-exec marking fail on
mongod ("Cannot apply $addToSet to non-array field … has non-array type null") — and since
6.3.x requires exclusive messages to be marked *before* the listener runs, that turned into a
hard non-delivery: no listener call, no answer, `sendAndAwait` timeout. Morphium senders can't
produce such documents (Msg's `@PreStore` initializes the field), but foreign writers mapping
the same collection without that guard, raw-driver writers and restored dumps can — observed in
production against a consumer upgraded from 6.2.4, where the same failed write had merely been
log noise after processing. All marking sites in all three implementations (plus the rejection
handler) now fall back to an atomic repair: `{processed_by: null}` → `{$set: [own id]}`,
guarded so an existing array is never clobbered. The InMemoryDriver previously masked the whole
class by treating explicit null like a missing field for `$addToSet`/`$push` (creating the
array); it now rejects it exactly like mongod, so the scenario is testable in-memory.
`getIndexStore()` is reachable without the collection lock (explain and slow-query logging), so
its from-scratch build could race any write that invalidates the store — most visibly
`createUser`: the build snapshots the documents, the write lands and invalidates, and the build
then publishes its pre-mutation snapshot anyway. That store passed the provenance check for
every later reader and stayed authoritative until the next invalidate; in the worst case the
duplicate-`_id` check ran against it and admitted a second document with the same `_id`. Every
invalidation now bumps a per-collection epoch *before* removing the store, builds sample it
before snapshotting, and a build whose epoch moved is not published (checked again after the
publish, so a full invalidate landing between check and publish is undone too). Whole-DB drops
and `resetData()`, which discard stores in bulk without `invalidateIndexStore()`, get the same
fencing via a global drop epoch — a build racing a `dropDatabase` could previously resurrect
the dropped collection's index store, pre-drop documents included. The explain/slow-query paths
stay lock-free: a refused build is still returned to its caller for that one read, it just
never becomes visible to anyone else.

#### InMemoryDriver: literal array queries support whole-array equality ({field: []} et al.)
A literal query with an array operand only ever matched via the multikey "array contains the
operand as an element" rule; MongoDB additionally matches when the document's array *is* the
operand (order-sensitive). Most visibly, `{processed_by: []}` — the empty-array form services
use against messaging collections — matched nothing at all, and on dotted paths the resolver
flattened leaf arrays into their elements so an empty array contributed no match candidates
whatsoever. Both query engines (interpreter and compiled) now check whole-array equality with
the same id/number normalization as scalar comparison ([1, 2] matches [1L, 2.0]), on plain and
dotted paths. Found during the mongorestore rehearsal for the acceptance drop-in test.

#### InMemoryDriver: unique+sparse indexes no longer throw false duplicate-key errors
A `unique: true, sparse: true` index (the classic optional-email pattern) rejected the second
document that lacked the indexed field with E11000 — both the index store and the insert-path
pre-check treated the missing key as a colliding value. Per MongoDB semantics, documents
containing none of a sparse index's fields are not part of the index and cannot collide; the
uniqueness check now skips them (documents with present fields are still enforced). Also fixed
in passing: decoding a BSON MaxKey threw "unknown data type" due to a missing `break`.

#### InMemoryDriver: unique partial indexes enforced uniqueness over the whole collection
A `unique` index with a `partialFilterExpression` was created and reported with its filter, but
the filter was never evaluated: uniqueness was enforced against every document, so a schema like
JEF's task queue (`{msg_id:1}, unique, partialFilterExpression {msg_id:{$type:"objectId"}}`)
rejected the *second* document without an `msg_id` — or with a non-ObjectId one — with E11000,
where mongod accepts any number of them. Documents outside the filter are not part of a partial
index in MongoDB and cannot collide in it; the index store now honours that. The filter cuts both
ways: a stored document that does not match no longer counts as a collision partner either, which
matters when the filter selects on a field outside the index key (uncovered and covered documents
then share a key bucket). Found during the PoppyDB drop-in rehearsal for the acceptance messageBus
cluster, verified against mongod 8.0.

The follow-up review of this fix surfaced three more gaps, all closed:
- `insert()`'s legacy O(collection)-scan unique pre-check had gotten the cuts-both-ways half
  wrong (it exempted only the incoming document, still raising the false E11000 the store fix
  removed). It re-implemented the index-membership rules separately from the store, which its own
  comments already declared the single uniqueness authority — deleted outright; committed and
  intra-batch conflicts alike now surface via `CollectionIndexStore.onInsert`, with mongod's
  actual ordered semantics (stop at the first error).
- An update that leaves the index key untouched but moves a document *into* the partial filter
  now runs the uniqueness check too — before, it silently created two covered documents on one
  unique key, a state mongod rejects with E11000 and the store's own rebuild would refuse.
- TTL expiry honours `partialFilterExpression`: a TTL index with a filter no longer deletes
  uncovered documents (mongod's TTL monitor never touches them). The partial filter is also
  compiled once per index definition now instead of being re-interpreted through the global
  query cache on every write.

## [6.3.1] - 2026-08-11

### Added

#### Messaging: implementation mismatches between queue participants are detected (#280)
All three messaging implementations use incompatible collection layouts, and a mixed queue used
to fail *silently* in the worst direction: broadcasts kept flowing while answers landed in a
collection the other side never reads. Every messaging instance now announces its implementation
on startup in a layout-independent `<queue>_participants` collection (heartbeat on the
`messagingRegistryUpdateInterval`, stale entries pruned, withdrawn on `terminate()`) and checks
what the other participants run. The channel is deliberately *not* the messaging itself — between
two implementations without a shared collection, a messaging-based warning would never arrive.
On a mismatch the default is a WARN log; `MessagingSettings.ImplementationCheck.THROW` makes a
mismatched instance refuse startup with an `IllegalStateException`, `IGNORE` disables
announcement and check entirely. Detection and diagnostics only — no bridging. The participants
entity reads from the primary on purpose: under replication lag a secondary read could miss an
announcement made moments ago (seen as exactly that on the loaded replica-set test phase).

### Changed

#### Messaging: the main change stream filters server-side (#283)
Every consumer's change-stream cursor used to receive every insert into the messaging
collection — including messages addressed to other recipients, full payloads of large foreign
answers included. Under high traffic the cursor fell behind and delivery degraded to
fallback-poll latency. The main change stream is now built with a server-side `$match` restricted
to what the instance can actually process: messages addressed to it, broadcasts for topics with a
registered listener, and answers (broadcast answers bypass the topic clause). The stream is
rebuilt when the registered topic set changes. V5-legacy senders store only `name` instead of
`topic` — the filter matches both, so legacy documents keep flowing.

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
Load measurements showed that request/reply throughput on MongoDB is *delivery*-bound rather than
write-bound: a single change-stream cursor hands out majority-committed events at a fixed cadence,
which caps sustained request/reply throughput regardless of the offered rate.
`MultiCollectionMessaging` did better in those runs — but not because of its per-topic collection
split (on mongod every cursor tails the whole oplog anyway); the effective mechanism was its
*second* cursor for answers and DMs. `DualChannelMessaging` ports exactly that one mechanism onto
the Standard layout: identical single collection and cursor for broadcast/topic traffic, plus a
dedicated per-recipient collection `<queue>_dm_<senderId>` with its own change-stream cursor and
dispatcher thread for directed messages and answers. Select it with
`cfg.messagingSettings().setMessagingImplementation("DualChannelMessaging")`. **Every participant
on a given queue must run the same messaging implementation** — there is no dual-read/dual-write
bridge between the collection layouts, and a mismatch fails silently: a `SingleCollectionMessaging`
node awaiting an answer from a `DualChannelMessaging` responder times out forever, because the
answer is written to the requester's DM collection, which the other implementation never reads.
The same applies to `MultiCollectionMessaging`, whose per-topic layout shares no collection with
the other two. Every `DualChannelMessaging` instance logs a WARN on startup restating this.
Marked **beta**: the measured benefit is smaller
and more nuanced than the original motivation suggested — past saturation it trades a little
throughput against markedly better tail latency (p99 519 ms vs 723 ms for Standard and 2044 ms
for MultiCollection in the steady-state window) — so it is opt-in while it gathers real-world
mileage. See `docs/howtos/messaging-implementations.md` for the full comparison.

#### `dropUser` — the user lifecycle is complete (InMemoryDriver + PoppyDB)
The in-memory driver (and with it PoppyDB) now implements mongod-compatible `dropUser`: the user
document is removed and a delete event is emitted on `admin.system.users` under the same
ordering lock as `createUser`/`updateUser`, so PoppyDB secondaries replicate the drop exactly
like creates and updates (documentKey-keyed delete). On a replica set the command is
primary-only like every other write - a secondary answers `NotWritablePrimary`. Previously the
only way to remove a user was a raw delete on `admin.system.users`, which bypassed the
event-ordering guarantee and was not wired into any command surface.

#### `customData` support in `createUser`/`updateUser`
`createUser` stores an optional `customData` document on the user (mongod's shape);
`updateUser` accepts `customData` — replaced wholesale when given (including as the only field,
which previously returned `BadValue`), preserved when omitted. A password change no longer
silently discards stored `customData`. `authenticationRestrictions` remains unmodeled.

#### Driver: automated failover test via wire-rewriting proxy, replaces manual `FailoverReproTest`
`FailoverReproTest` reproduced the 6.2.6 failover regressions but required a hand-built local
replica set and process kills (`kill -9`, SIGSTOP) run by hand — it was tagged `manual` and never
ran in CI. `DriverFailoverProxyTest` reproduces the same client-visible failure modes — clean
stepdown, hard kill, and the critical frozen-socket case (TCP connection alive but silent, the one
a driver can't distinguish from a slow server without a timeout) — plus read/write/messaging
recovery, through a reusable wire-level fault-injection proxy that sits between the driver and a
real replica set instead of killing processes. Tagged `wire-failover`, it runs automatically
against both MongoDB and PoppyDB replica sets in the normal test matrix. `FailoverReproTest` is
removed.

#### `morphium-jakarta-data` — optional Jakarta Data 1.0 runtime module
A new optional module, `morphium-jakarta-data`, brings a [Jakarta Data 1.0](https://jakarta.ee/specifications/data/1.0/)
provider implementation on top of Morphium's existing query engine: `@Repository`-based
`CrudRepository`/`MorphiumRepository` interfaces with query derivation from method names
(`findByCategory`, `countByStatus`, `deleteByX`, `And`/`Or`/`Between`/`In`/`Like`/`OrderBy`
and the rest of the standard keyword set), JDQL via `@Query` (including `GROUP BY`/`HAVING`
aggregates compiled into a Morphium aggregation pipeline), `@Find`/`@Delete` with explicit
`@By` parameter binding, offset pagination (`Page<T>`) and cursor/keyset pagination
(`CursoredPage<T>`), and both static (`@OrderBy`) and dynamic (`Sort`/`Order`) sorting. The
module depends on Morphium core and on `jakarta.data:jakarta.data-api`; the dependency
direction is strictly one-way — core has no knowledge of Jakarta Data and no dependency on
this module, so an application declaring only `de.caluga:morphium` does not get
`jakarta.data-api` on its classpath and none of these annotations or types become available.
Building the reactor with `-DskipExtensions` produces a core-only build (core + PoppyDB, no
extension modules) exactly as before this change. `morphium-jakarta-data` is deliberately
framework-agnostic — plain Java classes with zero dependencies on Quarkus, Spring, or any DI
container — because it is meant to be consumed transitively by framework integrations, not
added directly by most applications: `quarkus-morphium` (build-time Gizmo bytecode
generation) and `spring-boot-morphium` (JDK dynamic proxies) build on top of this module and
will follow in subsequent PRs. The code originates from
[Bardioc1977/morphium-jakarta-data](https://github.com/Bardioc1977/morphium-jakarta-data),
which is being archived now that its content has moved into the main Morphium repository.
See [Jakarta Data](docs/jakarta-data.md).

#### `quarkus-morphium` — optional Quarkus extension for CDI integration
A new optional module, `quarkus-morphium`, integrates Morphium into
[Quarkus](https://quarkus.io) applications: a CDI producer for `Morphium`, type-safe
runtime configuration via `@ConfigMapping` (`quarkus.morphium.*`), declarative
`@MorphiumTransactional` transactions with `MorphiumTransactionEvent` CDI events
(graceful degradation on Azure CosmosDB, auto-detected), MicroProfile liveness/readiness/
startup health checks via SmallRye Health, Dev Services (an automatically-started MongoDB
container, optionally as a single-node replica set), a Dev UI card with live connection
info, build-time Jakarta Data `@Repository` implementations generated via Gizmo bytecode
(no runtime reflection, no dynamic proxies — see [Jakarta Data](docs/jakarta-data.md) for
the underlying query-derivation, JDQL, and pagination feature set), GraalVM native-image
support (automatic reflection registration for every `@Entity`/`@Embedded` class), default
`MorphiumId` JSON serialization as its canonical 24-character hex string (both Jackson and
JSON-B, in both directions), and a MongoDB-backed migration runner with a distributed lock.
The module publishes three artifacts — `quarkus-morphium` (runtime), `quarkus-morphium-deployment`
(build-time processing), and `quarkus-morphium-testing` (test support) — plus an
`integration-tests` submodule that is built and run but never published. Like
`morphium-jakarta-data`, the core has zero compile- or runtime dependency on this module;
building the reactor with `-DskipExtensions` produces an unchanged core-only build. The
integration tests spin up a real MongoDB via Testcontainers and therefore need a running
Docker daemon — when Docker is unavailable, they detect this and skip themselves rather than
failing the build. **groupId migration:** this extension previously published under
`io.quarkiverse.morphium` as part of the Quarkiverse organization; because it does not
actually live in the [Quarkiverse](https://quarkiverse.github.io) GitHub organization,
Maven coordinates now follow Morphium's own groupId, `de.caluga:quarkus-morphium`, and
version in lockstep with the Morphium reactor. **Existing users of
`io.quarkiverse.morphium:quarkus-morphium:1.2.0` must update their dependency's groupId to
`de.caluga` and its version to the Morphium version they adopt (currently `6.3.x`)** — no
package renames, no API changes, only the Maven coordinates move. The code originates from
[Bardioc1977/quarkus-morphium](https://github.com/Bardioc1977/quarkus-morphium), which is
being archived now that its content has moved into the main Morphium repository. See
[Quarkus Extension](docs/quarkus-extension.md).

#### `spring-boot-morphium` — optional Spring Boot integration module
A new optional module, `spring-boot-morphium`, integrates Morphium into
[Spring Boot](https://spring.io/projects/spring-boot) applications: `MorphiumAutoConfiguration`
creates the application's `Morphium` bean from `morphium.*` properties (type-safe
`@ConfigurationProperties`, with `spring-boot-configuration-processor`-generated metadata for
IDE autocompletion), and connection retry with linear backoff on transient failures.
Jakarta Data `@Repository`
interfaces (`CrudRepository`/`MorphiumRepository` from `morphium-jakarta-data`) are wired via
`MorphiumRepositoryRegistrar` at Spring context-startup time, backed by a JDK dynamic proxy
(`java.lang.reflect.Proxy`) per repository interface — in contrast to `quarkus-morphium`, which
generates repository implementations as Gizmo bytecode at build time; here everything is
runtime reflection, no annotation processor or build-time codegen involved. Declarative
`@MorphiumTransactional` transactions wrap the annotated method body in
`startTransaction()`/`commitTransaction()`/`abortTransaction()` via an AspectJ `@Around` advice,
active only when `spring-boot-starter-aop` is on the classpath. An Actuator `HealthIndicator`
reports live MongoDB connection status (database, driver, replica-set state) under
`/actuator/health`, active only when `spring-boot-actuator` is present and a `Morphium` bean
already exists; a user-defined bean named `morphiumHealthIndicator` correctly overrides the
auto-configured one. The module publishes three artifacts — `morphium-spring-boot-starter`,
`morphium-spring-boot-autoconfigure`, and `morphium-spring-boot-test` (a `@MorphiumTest`
composite annotation that wires `InMemDriver` into a `@SpringBootTest`, so repository tests run
without a MongoDB instance or container) — and, unlike `quarkus-morphium/integration-tests`,
`morphium-spring-boot-test` is a genuine end-user artifact, not an internal test suite, and is
published to Central like the other two. Like `morphium-jakarta-data` and `quarkus-morphium`,
the core has zero compile- or runtime dependency on this module; building the reactor with
`-DskipExtensions` produces an unchanged core-only build. No Docker/Testcontainers dependency
anywhere in the module — all tests run against Morphium's `InMemDriver`, unlike
`quarkus-morphium`'s integration tests, which need a running Docker daemon.
**Two coordinate/naming corrections made during the pre-integration conversion:** the three
modules were renamed from `spring-boot-morphium-*` to `morphium-spring-boot-*`, following the
Spring Boot starter naming convention (the `spring-boot-` prefix is reserved for Spring's own
starters); and the configuration property prefix was renamed from `spring.morphium.*` to
`morphium.*`, since the `spring.*` namespace is reserved for Spring Boot's own configuration
keys. Both renames happened before any Maven Central release of this module existed, so they
carry zero breaking-change cost. **Existing users of the pre-integration
`de.caluga:spring-boot-morphium-starter:1.0.0-SNAPSHOT`** must update their dependency's
artifactId to `morphium-spring-boot-starter`, its version to the Morphium version they adopt
(currently `6.3.x`), and rename every `spring.morphium.*` key in their
`application.properties`/`.yml` to `morphium.*` (e.g. `spring.morphium.database` →
`morphium.database`) — no Java API changes; `MorphiumProperties`, `@EnableMorphiumRepositories`,
`@MorphiumTransactional`, and all other public types are unaffected. The code originates from
[Bardioc1977/spring-boot-morphium](https://github.com/Bardioc1977/spring-boot-morphium), which
is being archived now that its content has moved into the main Morphium repository. See
[Spring Boot](docs/spring-boot.md).

#### PoppyDB: `--users-file` — declarative user provisioning (bootstrap, upsert, version-gated)
Builds on user replication: `--rootUser`/`--rootPassword` only ever provisioned one admin user,
so any real application user set still had to be created by hand (a shell script running
`createUser` against a live server, or worse, a manual `mongosh` session) — not something you can
put in version control or a config-management run. `--users-file <path>` (config key
`users-file`) now points at a JSON file — either a bare array of users, or `{"version": N,
"users": [...]}` — applied as an idempotent `createUser`/on-51003-fallback-`updateUser` upsert
wherever `ensureRootUser` already runs: once at startup for a static-mode primary (a broken file
aborts startup, fail-fast like any other bad config), and on every leadership-hook run for an
election-mode primary (a failure there can only be logged — a running server cannot abort
mid-failover). A static-mode secondary never applies the file itself, even if one is configured
on it too; it receives the result through the same `admin.system.users` replication that already
carries `createUser`/`updateUser`. An optional `version` field in the file gates re-application
against a small replicated meta document (`admin.system.version {_id: "poppydb.usersFile",
appliedVersion: N}`), which prevents a straggler node from rolling credentials back on failback
with an older copy of the file on disk — only a strictly higher version re-applies — provided the
node is not elected primary while still mid-resync: the vote's Raft log check
(`ElectionManager.isLogAtLeastAsUpToDate`) is currently dead code (nothing calls
`updateLogIndex`, so it can never deny a vote for being behind), so a mid-resync node with an
empty local log is exactly as electable as a fully caught-up peer — pre-existing, honestly named
here rather than implied, tracked as a follow-up (see docs). Unknown
fields (top-level or per-entry) are a hard error naming the field, and two entries naming the
same `(user, db)` pair are now a hard error too (previously silent last-entry-wins, since later
entries' `createUser`/`updateUser` simply overwrote earlier ones with no diagnostic — a
copy-paste typo in the file could drop a user's intended password/roles unnoticed). Like every
other secret file in PoppyDB's config surface, the file's POSIX permissions are checked
(group/other-readable warns, group/other-writable refuses to start); its content is never
logged, including in error messages, even for a malformed-JSON parse failure. `--check-config`
validates the file (parse, validation, permissions) the same way, without starting a server. See
[PoppyDB § Bootstrapping users](docs/poppydb.md#bootstrapping-users---users-file).

#### PoppyDB: `admin.system.users` replicates across the replica set — users survive failover
Users were node-local: `createUser` only ever wrote to whichever node's own `admin.system.users`,
so a secondary never had the same login-able users as the primary, and a failover — or a dump
taken on a priority-0 backup node — silently lost them. `admin.system.users` is now the one system
collection that replicates (live change-stream events, the initial-sync snapshot, and resync-clear
all carry it, same as ordinary user data), and it gained a proper `updateUser` command (mongod-
shaped, previously missing) alongside `createUser` for in-place password/role rotation. Both
commands, like all writes, are now primary-only — a secondary answers them with
`NotWritablePrimary` instead of silently accepting a write that would only ever apply locally,
which was the underlying cause of the replication gap. `ensureRootUser` follows the same rule in
election mode: only the current primary's leadership hook (re-)creates the initial admin user;
secondaries never self-create it and only ever receive it via replication. Two follow-up fixes
round out the failover path: a demoted-but-still-running leader now resumes replication toward the
new primary immediately instead of waiting for an unrelated later leader change, and a leader
change with byte-for-byte identical data (verified per-namespace via `dbHash`) takes a consistency
shortcut that skips the clear-and-full-resnapshot entirely. See
[PoppyDB § Authentication — User replication](docs/poppydb.md#authentication---auth).

#### PoppyDB: configuration file support (`--cfg`/`-f`, `--no-config`), secrets kept off the command line
Production deployment (systemd, Docker, config management) needed a config file — every setting
was CLI-only, and passwords (`--rootPassword`, `--sslKeystorePassword`) on the command line are
readable by any local user via `ps aux`/`/proc/<pid>/cmdline` for the life of the process. PoppyDB
now optionally reads a `java.util.Properties`-format file (`key=value`), discovered in order
(first match wins, files are never merged) from `--cfg`/`-f`, `$POPPYDB_CONF`,
`${XDG_CONFIG_HOME:-~/.config}/poppydb/config`, `~/.config/poppydb.conf`, `/etc/poppydb/config`,
then `/etc/poppydb.conf`; `--no-config` skips the four default locations. Precedence is uniform
for every single setting: command line argument wins, then the config file, then the built-in
default — `--no-ssl`/`--no-auth` were added so a config file's `ssl=true`/`auth=true` can still be
switched back off from the command line, closing the precedence chain for both boolean flags.
Keys are matched case/separator-insensitively (`max-bson-size` ≡ `maxBsonSize` ≡ `MAX_BSON_SIZE`),
an optional `poppydb.` prefix is stripped, and an unknown key (typo) aborts startup with a "did you
mean" suggestion instead of being silently ignored — a config that starts wrong is worse than one
that doesn't start. `root-password`/`ssl-keystore-password` each gained a `*-file` counterpart
(`root-password-file`, `ssl-keystore-password-file`) that reads the secret from a separate file
(compatible with Docker secrets, Kubernetes secret mounts, and systemd's `LoadCredential=`), and
any file carrying a secret — the main config or a referenced `*-file` — has its POSIX permissions
checked: group/other-readable warns, group/other-writable refuses to start (a world-writable
config holding secrets is a privilege escalation, not a style issue). Deliberately **not** built:
`#include`/`conf.d` directory merging — `#` is a comment character in `.properties` files, which
makes a `#include` directive collide with ordinary commented-out lines, and the one real
motivating use case (secrets separation) is better served by the `*-file` indirection above.
`scripts/poppydb.sh`/`scripts/startPoppyDB.sh` always pass `--no-config` now, so a developer's
private config can never silently change what a local test run connects to. See
[PoppyDB § Configuration File](docs/poppydb.md#configuration-file) and the
[Production Deployment Playbook](docs/howtos/poppydb-deployment.md).

#### PoppyDB: `--print-config`/`--check-config` CLI modes
PoppyDB CLI: `--print-config` prints the effective configuration (defaults + config file +
command line, secrets redacted, per-key source annotations) as a reusable config file;
`--check-config` validates syntax, semantics and deep checks (keystore loadable, dump-dir
usable) without starting the server — exit code 0/1 like `nginx -t`. See
[PoppyDB § Inspecting and validating the configuration](docs/poppydb.md#inspecting-and-validating-the-configuration).
Startup itself now validates option ranges and cross-option consistency that were previously
unchecked (e.g. `port` in range, `memory-warn` <= `memory-reject`) and reports every
configuration error at once instead of stopping at the first.

#### PoppyDB: DevOps command surface — live currentOp/killOp, rs.conf(), listCommands, hostInfo, real connection gauges
Closes the gaps that made mongosh's admin helpers fail against PoppyDB. A server-wide **op registry** tracks every command for the duration of its dispatch: `db.currentOp()` (mongosh's `{aggregate: 1, pipeline: [{$currentOp: {}}]}` shape, `$match` filters included) and the `currentOp` command answer from it with mongod-shaped op documents (opid, ns, command, secs_running, client, killPending — SASL/createUser payloads redacted); `killOp` marks an op kill-pending and best-effort interrupts its thread, cooperatively like mongod (never a Netty event loop; write-concern waits on the executor are interruptible). New commands: `listCommands` (generated from the real command surface — the wire handlers plus the driver's registered command classes), `hostInfo`, `connectionStatus` (reports the connection's SCRAM user under `--auth`), `whatsmyuri`, and `replSetGetConfig` — `rs.conf()` now works, reconstructed from `--rs-seed`/`--rs-priorities`. `serverStatus.connections` reports the server's **real** client-socket gauges (Netty channel group) instead of the in-memory driver's internal connection borrows. The embedded InMemoryDriver answers the `$currentOp` stage with an honest empty set (commands execute synchronously — there is never a concurrent op to report).

#### InMemoryDriver/PoppyDB: memory watermark — writes are rejected before the heap dies
An in-memory store dies of OOM when producers outrun consumers — and a replica set dies *completely*, because replication copies the data volume to every node. Two watermarks (percent of max heap) now guard the write path centrally in the driver: crossing the **warn** threshold (default 75%) logs a WARN once per crossing; above the **reject** threshold (default 90%) document-creating writes (insert/store) are refused with a mongod-shaped `ExceededMemoryLimit` error (code 146) that clients should treat as retryable backpressure. **Updates, deletes and TTL expiry stay allowed** — the drain paths (messaging processed-marks, lock releases, cleanup) must keep working or the system could never get back under the watermark. Replication applies and the initial sync bypass the guard (the primary is the gate; a secondary refusing what the primary accepted would silently diverge), so all members of a replica set stop accepting new data at the same bound instead of failing together. Both stages decide on the **post-GC live set** (per-pool collection usage, `heapUsedAfterGcPercent` in `serverStatus`), not on raw heap occupancy — with `-Xms` == `-Xmx` the raw `used/max` gauge routinely reads above 90% under allocation-heavy load even when the next GC would free most of it, and the first overnight replica-set CI run proved it: the raw-gauge version rejected the writes of 8 green messaging test classes on a heap that GC promptly dropped to 46%. The raw gauge stays as a cheap precheck on the hot write path (the live set can never exceed it). Configurable via `--memory-warn`/`--memory-reject` (100 = off), `PoppyDB.setMemoryWatermarks(...)` or `InMemoryDriver.setMemoryWatermarks(...)`; state is visible in `db.serverStatus().memoryWatermark`. On the way, the wire insert fast path stopped labelling every driver exception as a duplicate-key error (11000) — typed codes like 146 now pass through. The counterpart feature — per-collection LRU eviction for cache-style collections — is sketched in planned_features.md for 7.0.

#### Driver/PoppyDB: `maxMessageSizeBytes` respected end-to-end — byte-aware write splitting, hello limits adopted, reply batches capped
The 48MB wire message bound was advertised but ignored: batching was count-based only (`cursorBatchSize`, `maxWriteBatchSize`), so 1000 × 1MB documents went out as one ~1GB OP_MSG that any real MongoDB — and PoppyDB's own decoder — answers by closing the connection. Three fixes: **(1)** The PooledDriver now adopts `maxMessageSizeBytes`, `maxWriteBatchSize` and `maxBsonObjectSize` from the hello handshake (previously only SingleMongoConnectDriver did; the pool kept DriverBase's field defaults — a 16MB message bound, batch size 1000 and a `12*1025*1024` typo for the BSON limit — which now default to MongoDB's real 48MB/100000/16MB). **(2)** Write commands split oversized payloads like the official drivers: `WriteMongoCommand.execute()` cuts the `documents`/`updates`/`deletes` arrays into chunks under `maxMessageSize` minus envelope slack (and under `maxWriteBatchSize`), runs them through the normal single-message retry path and folds the results into one mongod-shaped answer — counters summed, `writeErrors`/`upserted` indices shifted to the caller's original statement positions, ordered writes stopping at the first sub-batch with write errors (`WriteBatchSplitter`). **(3)** In server mode the InMemoryDriver caps **reply** batches (find/aggregate/getMore cursors) by bytes at `maxBsonObjectSize` per batch like mongod, pushing the remainder back onto the cursor — embedded use is untouched, replies never become wire messages there. Also fixed on the way: the second `getClass().getDeclaredMethod` dispatch-lookup in `sendCommand` (subclassed drivers broke generic command dispatch, same bug as in `runCommand`).

#### InMemoryDriver/PoppyDB: BSON document size limit enforced like mongod — configurable, default 16MB
The 16MB limit was only ever *advertised* (and by the embedded driver as a fantasy 128MB), never enforced — clients that respected the handshake stayed compatible by accident, and updates could grow documents without any bound, which no real MongoDB would accept. Measured against a real 8.0.26: a `$set`/`$push` whose **result** exceeds the limit fails server-side with `BSONObjectTooLarge` (10334) and the message `BSONObj size: N (0x..) is invalid. Size must be between 0 and 16793600(16MB) …` — 16793600 being the user limit plus mongod's 16KB internal margin (`BSONObjMaxInternalSize`). The InMemoryDriver now does the same: inserts/stores are checked against the plain limit (ordered inserts throw, unordered ones report a per-document writeError), update/replacement/upsert **results** against limit+16KB — atomically, the in-place mutation is rolled back like a unique-violation. `hello` advertises the configured value (embedded and over PoppyDB's wire, which previously hardcoded 16MB), so drivers enforce it client-side exactly as against mongod. Configurable: `--max-bson-size <bytes>` (0 = off), `PoppyDB.setMaxBsonObjectSize(...)`, `InMemoryDriver.setMaxBsonObjectSize(...)`; `BsonEncoder.documentSize(Map)` measures without materializing the encoded copy. On the way, PoppyDB's `hello` no longer pays a ~30s reverse-DNS lookup on hosts without working rDNS when the RS seed list already names the member, and the InMemoryDriver's generic command dispatch now resolves handler methods against `InMemoryDriver.class` (a subclass previously broke the `getDeclaredMethod` lookup).

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


#### InMemoryDriver: the `$merge` aggregation stage is implemented (#241)
`$merge` previously reported success and wrote nothing at all — every persistence call was commented-out dead code — so pipelines materialising results (rollups, denormalised views, ETL-style flows) silently produced no data. It now works: `whenMatched` `merge` (default, incoming fields win) / `replace` / `keepExisting` / `fail`, `whenNotMatched` `insert` (default) / `discard` / `fail`, `on` defaulting to `_id` and accepting a single field or a list, and `into` as a collection name or `{db, coll}`. `merge` and `replace` preserve the target document's `_id`; ambiguous `on` matches and documents missing an `on` field are refused rather than silently guessed; `$merge` is terminal and yields no documents. Writes go through the driver's `find()`/`store()`, so index maintenance, capped/TTL bookkeeping, locking and watcher events all happen. `whenMatched` may also be a custom update pipeline: it runs per match with the existing target document as input and the incoming document bound to `$$new`, supports the stages mongod allows there (`$addFields`/`$set`, `$project`/`$unset`, `$replaceRoot`/`$replaceWith` — anything else is refused), and honours `let` (which, as in mongod, *replaces* the default `{new: "$$ROOT"}`, is evaluated against the incoming document, and is rejected when `whenMatched` is not a pipeline). References to undefined `$$variables` fail up front instead of evaluating to null; the pipeline result keeps the target document's `_id`.

### Changed

#### InMemoryDriver: the change-stream before-image is no longer deep-copied twice per watched update (#274)
With a change-stream subscriber on the namespace, `updateInternal` already takes a full `deepClone` of the document before mutating it — and then handed that clone to `notifyWatchers`, which deep-copied it a *second* time when building the event. The second copy existed only because `buildChangeStreamEvent` treated both images the same way, not because anything needed it: once the notification is queued, nothing in the update path reads or mutates that clone again, so the change-stream path is its sole owner and all the second copy contributed was another full recursive walk of the document plus a duplicate of its entire nested structure. The before-image is now adopted as-is on exactly that path, with only the `_id` normalization still applied. On a deeply-nested document (~580 nested maps/lists) with an active watcher this removes ~163 KiB of allocation per update, about 7% of the whole update's allocation — the wall-clock effect stays inside run-to-run noise, since the remaining traversals (after-image copy, `updatedFields`/`removedFields` flattening, `updateLookup`) dominate.

Deliberately narrow, and gated by an explicit `beforeDocumentIsExclusiveCopy` flag rather than applied to `buildChangeStreamEvent` as a whole, because on every other path the before-image is *not* exclusively owned: the delete paths pass the live stored document as both after- and before-image, `store()`'s replace branch passes the document it just unlinked, and an update without subscribers or transaction passes a `buildPartialBeforeImage` result that still shares untouched nested containers with the live document. Those all keep the real deep copy. The **after**-image keeps its unconditional deep copy on every path without exception — it references the live, in-place-mutated stored document, and a shallow variant of that copy was already tried once and reverted the same day (cf3e9cace).

#### InMemoryDriver: insert's duplicate-`_id` pre-check is an O(1) index lookup instead of an O(N) collection scan
Every `insert()` call built a `HashSet` of all existing `_id`s by iterating the entire collection — under the exclusive write lock. For single-document inserts into large collections (the messaging workload) that scan was the dominant per-insert cost, and it was redundant: the per-collection `CollectionIndexStore` always carries a unique `_id_` index that reflects exactly the committed documents. The pre-check now asks that index directly (new `CollectionIndexStore.containsId`, a single hash lookup). Semantics are unchanged: ordered inserts still throw on a committed duplicate, unordered ones still collect a code-11000 writeError, and duplicates *within* one batch still surface at the per-document index insert, as before. As a side effect the check now uses the index's `MorphiumId`/`ObjectId` normalization, so a duplicate no longer slips past the pre-check just because caller and store hold the same id in different wrapper types.

#### PoppyDB: dead `locked_by`/`locked` messaging index removed
`MessagingOptimizer` created a `msg_locked_by_1_locked_1` index on every registered messaging collection, but those fields no longer exist on `Msg` — locking moved to the separate `MsgLock` collection long ago. Nothing ever queried the index; it only added per-insert maintenance cost on the hottest collection. Removed.

#### Messaging: non-exclusive messages are processed from the change-stream `fullDocument` — one DB roundtrip less per message
`SingleCollectionMessaging` re-read every message by `_id` (PRIMARY read preference) before processing, although the insert event already carried the complete document. For the safe case — non-exclusive messages arriving via an insert event with a `fullDocument` — the change-stream handler now attaches the event snapshot to the processing queue element and the processing runnable deserializes it directly; all skip checks (listener existence, sender==self, processed-by, recipients, answer matching) run unchanged against the deserialized message. Everything with staleness risk deliberately keeps the re-fetch: exclusive messages (the `processed_by` re-check after claiming the lock is correctness, not overhead), requeue updates, poll pickups, and any snapshot that fails to deserialize. The decision trace records which path was taken.

#### InMemoryDriver/PoppyDB: dbStats and collStats report real sizes instead of zeros
`db.stats()` answered all byte-size fields with 0, and `collStats` reported jol's *shallow* `sizeOf` — the ArrayList object header, not the data (and NPE'd on a missing collection). Both now compute real values: `dataSize`/`size` is the actual BSON size of every document (mongod's definition; computed on demand, O(data) — fine for a diagnostic command), `storageSize` equals it (no padding or compression in memory), `avgObjSize` follows, and index sizes are estimates proportional to the entry count (64 bytes per document per index). New fields: `totalSize`, and on dbStats `fsUsedSize`/`fsTotalSize` reporting the JVM heap — the "filesystem" an in-memory database actually lives on. Index counts now include the implicit `_id` index like mongod. The `$collStats` aggregation stage's `storageStats` uses the same computation; `collStats` on a missing collection answers zeros instead of failing.

#### PoppyDB: reports its real version instead of "5.0.0-ALPHA" / "PoppyDB V0.1ALPHA"
`buildInfo.version` and `serverStatus.version` were hardcoded to `5.0.0-ALPHA` (mongosh greeted every connect with `Using MongoDB: 5.0.0-ALPHA`), and the hello `msg` field still said `PoppyDB V0.1ALPHA (Netty)`. All three now carry the actual product version from the Maven build (via `MorphiumVersion`, shared constant `InMemoryDriver.REPORTED_SERVER_VERSION`) — PoppyDB releases in lockstep with morphium, so mongosh now shows `Using MongoDB: 6.3.0`. Deliberately the PoppyDB version, not a MongoDB compatibility version: protocol capabilities are negotiated via `maxWireVersion`, not this string.

#### InMemoryDriver: O(1) change-stream replay-buffer bound
The ring-buffer bound check in `notifyWatchers` used `ConcurrentLinkedDeque.size()` — O(n), ~200k node traversals per write at PoppyDB's 100k-event replay bound. The deque size is now tracked in an `AtomicInteger`; eviction semantics are unchanged.

### Fixed

#### InMemoryDriver: a single insert after a TTL-queue invalidation stopped every older document from ever expiring (#269)
The TTL sweep is queue-driven, and `invalidateTtlQueue()` discards a collection's queue
outright at every structural change (drop, clear, rename, transaction commit/abort), relying
on a lazy rebuild-on-miss - the same discard-and-rebuild contract the persistent index store
uses. But only one of the two code paths that can find the queue missing actually rebuilt it:
`sweepTtlQueue()` bootstrapped from a full scan, while `ttlEnqueue()` used `computeIfAbsent`
and put a fresh, otherwise-EMPTY queue in place holding nothing but the one document it was
called for. That queue is no longer absent, so the sweep's bootstrap-on-miss never fired
again and every document that existed before the invalidation permanently lost its expiry
tracking - it would only ever come back through another structural event that happened to
invalidate the queue again at a quieter moment.

Why it matters beyond the in-memory driver: `Msg.deleteAt` carries
`@Index(options = "expireAfterSeconds:0")`, so this is the exact mechanism Morphium's
messaging relies on to clean up processed messages, and PoppyDB runs on this driver. A
messaging node starting against a PoppyDB that already holds messages opens precisely this
window - the `MessagingOptimizer` registers the messaging collection (structural index work)
and the first message inserted afterwards lands before the next sweep tick - after which the
pre-existing messages were never expired again and the `msg` collection grew without bound.

`ttlEnqueue()` now bootstraps on miss exactly like the sweep does. Two details this needed
care with: every call site runs *after* its document is physically in the collection and in
the index store, so the bootstrap scan has normally already queued it and re-adding it would
double-enqueue - guarded by an explicit check rather than an assumption, since the bootstrap
can legitimately miss it (a renamed collection carries no index definitions over, leaving
nothing to scan). And the bootstrap requires the collection's write lock, which all five
`ttlEnqueue()` call sites (`insert`, `storeInternal`, `updateInternal`) already hold, so no
new lock is taken and no ordering is introduced.

#### InMemoryDriver: index-store provenance mismatch evicted the entry, causing a rebuild ping-pong between a transaction and concurrent readers
Follow-up to the provenance fix. On a mismatch, `getIndexStore()` evicted the offending
entry before rebuilding, and a transaction whose entry got evicted then lost the race to
publish its own store forever: the surviving entry kept winning `putIfAbsent`, so that
transaction rebuilt its index store on every single operation for its whole lifetime. A
first attempt removed the eviction but left the mismatching entry in place unowned, which
fixed the rebuild storm but left a leftover foreign entry sitting in the map. The entry now
instead changes owner atomically once the rebuild finishes, via a compare-and-swap keyed on
the exact entry this call observed - a same-key swap rather than a remove-then-publish, so
there is never a moment with no entry for the key. Measured on 5000 documents and 20
operations inside a transaction that runs against a pre-existing store: 20 `buildIndexStore`
passes with the entry evicted, 1 with the CAS; a purely non-transactional caller (no
transaction open at all) sees 0 either way. Same numbers for one secondary index and for
two. Since `buildIndexStore` is O(documents x indexes) this worked against the "cost
proportional to what a transaction touches" property the lazy rebuild was introduced for.
The swap also never creates a "no entry present" window, which two lock-free callers (the
`ExplainCommand` path in `runCommand`, and `recordAggregateSlowQueryIfNeeded`) could
otherwise use to publish a store built from a document list another thread is mutating.

#### Messaging: change-stream fullDocument fast path skipped `@PostLoad`, silently dropping V5-legacy messages that only carry a `name` field
The non-exclusive fast path introduced with the fullDocument optimization deserialized the
change-stream snapshot via the raw `ObjectMapper`, which - unlike the query path - fires no
entity lifecycle callbacks. `Msg.postLoad()` is exactly where the V5→V6 compatibility
migration lives (`topic = name` when only the legacy `name` field is set), so a message
inserted externally in V5 format without a `topic` field (e.g. via `storeMap()`, as
`V5V6CompatibilityTest` simulates) arrived with `topic == null` and was silently discarded by
the "no listener registered for this topic" check - no exception, no fallback, on every
backend. The fast path now fires `firePostLoadEvent()` right after a successful deserialize,
matching the query path; if the callback throws, the message falls back to the pre-existing
re-fetch path.

#### InMemoryDriver: aborted/committed transactions could leave stale `CollectionIndexStore` entries, causing false duplicate-key errors on a provably empty collection
A persistent `CollectionIndexStore` lazily built while a transaction is open is built from
the transaction's private snapshot, i.e. from structurally-cloned document instances rather
than the live ones. Those clones were registered into the store's unique-index buckets same
as any real document. `commitTransaction()` already invalidated the store for every
collection the transaction touched, but `abortTransaction()` did not - so on abort the store
kept referencing the orphaned clones forever, since removal matches only by reference
identity and can never match a clone against the real document it was copied from. Every
later insert under that same unique-index key was then rejected as a duplicate, even after
the live collection had been cleared to zero documents. Both `abortTransaction()` and
`commitTransaction()` now invalidate the index store (and TTL queue) for every collection
whose store was actually built while the transaction was open, not merely the ones it wrote
to, since a read-only indexed query can trigger that same lazy rebuild without ever writing.

#### InMemoryDriver: a `CollectionIndexStore` built before a transaction started stayed stale for the whole transaction, silently losing an update on commit
The previous fix only covers a store built DURING a transaction. A store built BEFORE one -
the common case, since most collections already have a store from earlier reads or writes -
was never touched by that invalidation at all. Such a store was built by reading through the
live database and holds live document instances; a transaction's writes then mutate its
private cloned snapshot instead, without that pre-existing store ever finding out. An
index-backed read inside the transaction (an equality lookup on a secondary index) kept
returning the pre-transaction live instance, diverging from a full scan of the same
collection, which does read through the transaction's snapshot. Worse, an update whose
candidate document came from that stale index-backed lookup mutated the live object instead
of the snapshot clone the commit actually merges back, so the write was silently lost after
commit even though it succeeded without error inside the transaction. `getIndexStore()` now
records which transaction context (if any) each persistent store was built from and reuses a
store only for the caller it was built for - rebuilding lazily on first access rather than
eagerly discarding every collection's store at transaction start. Keying this by context
identity rather than by build order matters because `currentTransaction` is thread-local and
transactions genuinely overlap: it stops two concurrent transactions from borrowing each
other's store (which would let one transaction's index-backed update land in the other's
snapshot) and stops a reader outside any transaction from observing an open transaction's
uncommitted writes through a store seeded with that transaction's clones.

#### PoppyDB: a re-syncing secondary broadcast its own initial-sync wipe as change-stream drop events, letting stale watchers destroy `admin.system.users` cluster-wide during a stepdown
The initial sync's `clearLocalDatabases()` wipe and snapshot copy ran as regular commands and
therefore emitted live change-stream events on the syncing node - including
`drop admin.system.users`. During a live stepdown that is catastrophic: the demoted ex-primary
immediately starts re-sync attempts toward the presumed new leader (each failed retry wiping
again), while the other nodes' OLD ReplicationManagers are still watching the demoted node
(they only tear down once their own ElectionManager delivers the leader change) and faithfully
apply those wipe-drops to their own data. The drops then ricochet through every node's own
re-emission, and even the freshly promoted primary applied the demoted node's wipe-drop right
at its promotion (its stopping ReplicationManager flushes queued events) - so whether a user
created on the new primary survived on any given node was pure timing (the
`StepdownReplicationTest` ~40% flake, and a real data-loss window on production failovers).
Initial-sync writes are now performed inside a new
`InMemoryDriver.suppressChangeStreamEvents()` scope - mirroring MongoDB, where initial-sync
writes are never oplogged - so the wipe + snapshot are invisible to change-stream watchers;
steady-state replication applies still emit events as before (a promoted secondary must be
able to serve resumable streams).

#### Driver: failover read path could throw a raw NPE past every retry; stale `getLastConnectFailure()` after recovery
The read-preference fallback chain read the volatile `primaryNode` field multiple times; the
heartbeat nulls that field on stepdown or connection error - exactly while the fallback code
runs - so `hosts.get(null)` could throw a `NullPointerException` that, not being a
`MorphiumDriverException`, escaped every retry-catch on the read path and aborted a read the
fallback was built to save. Both fallback sites now work on a local snapshot. Additionally,
`getLastConnectFailure()` is cleared when a connect succeeds, so a caller polling after
recovery no longer sees the pre-recovery error as if it were current.

#### InMemoryDriver: `updateUser` reset the user's SCRAM mechanism set on every password change; malformed field types escaped as ClassCastException
A password change without an explicit `mechanisms` field rebuilt the credentials with the
both-mechanisms default, silently re-arming SCRAM-SHA-1 for a user deliberately created
SHA-256-only; mongod preserves the existing mechanism set, and now the in-memory driver does
too. `mechanisms` without `pwd` is now supported with mongod's subset-only semantics (stored
credentials of the named mechanisms are kept verbatim, the rest dropped; non-subset requests
are `BadValue`). All optional fields are shape-checked before casting, so `roles: "foo"` &co.
produce a `BadValue` command error instead of an uncaught `ClassCastException`.

#### PoppyDB: demoted leader could keep `primary==true` forever after a rapid leadership flap
`onLeadershipChange` incremented the leadership epoch and then wrote the `primary` flag
unsynchronized: a preempted stale dispatch could re-assert its outdated flag value AFTER a
newer transition had written the current one. A node stuck with `primary==true` as a follower
silently never replicates - `startReplicationToLeader`, the liveness probe and the retry chain
all no-op on `primary`. Epoch bump and flag flip are now one atomic unit, making a stale
overwrite structurally impossible. Related hardening in the same area: the post-start
replication liveness probe now checks "watch never registered" (`watchGeneration`) instead of
the instantaneous `isWatchLive()`, so it no longer tears down a healthy `ReplicationManager`
it happens to sample during a routine watch-reconnect gap; and a late election callback can no
longer install a `ReplicationManager` after `shutdown()` that nothing ever stops.

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
Each of `--auth` and `--ssl`, independently, made a multi-node PoppyDB replica set completely non-functional: `ElectionNetworkClient` (vote requests, heartbeats) and `ReplicationManager` (the sync connection to the primary) connected to peers as a plain, unauthenticated, unencrypted client, regardless of the server's own `--auth`/`--ssl` configuration. With `--ssl=true` every internal connection was rejected by the peer's TLS-only listener (`NotSslRecordException`); with `--auth=true` the election RPCs (`requestVote`/`appendEntries`) aren't on the pre-auth command whitelist, so every one was rejected as unauthorized - either way, no leader could ever be elected. Single-node PoppyDB with `--auth`/`--ssl` was unaffected; the client-facing enforcement itself was never the problem. The internal channel now authenticates as the configured root user and, when TLS is on, trusts exactly the server's own configured certificate (`ssl-keystore`, reused as the internal client's pinned truststore) - no new config keys, no change to auth enforcement.


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
`create` with a `timeseries` spec used to log a WARN and create a **plain** collection — a silent divergence: no `timeField` enforcement, no retention, `listCollections` reporting the wrong type. It now returns a proper command error (code 115 `CommandNotSupported`) over the wire and raises a `MorphiumDriverException` for embedded users. On the way, `CreateCommand.execute()` was switched from cursor-style reading to `readSingleAnswer` — mongod's create reply is a plain document, and the cursor path silently swallowed cursor-less replies (including error documents) on the in-memory connection. Real time-series support is tracked in #261 (API) and #262 (in-memory emulation), both scheduled for 7.0.0.

#### InMemoryDriver/PoppyDB: resumed change streams could deliver an event twice
A watch resuming with `resumeAfter` registers its subscription *before* replaying the event history (the reverse order would lose events written between history snapshot and live stream). An event written exactly in that window was delivered twice — once by the asynchronous live dispatch to the already-registered subscription, once by the replay — and, because the live dispatch can overtake the replay, in arbitrary order. Resumed subscriptions now suppress exact duplicates by resume token (a bounded recent-token window; a monotonic guard would have turned the reordering into losses). Fresh watches have no replay and are unaffected — no overhead on the messaging path. Real MongoDB never had this problem (oplog-cursor resume is snapshot-consistent); morphium's own consumers (messaging, PoppyDB replication) were already idempotent, so this mainly protects custom `ChangeStreamListener`s running against InMemoryDriver/PoppyDB.

#### PoppyDB: the wire fast path dropped `arrayFilters` (#256 follow-up)
`processUpdateDirect` — PoppyDB's direct dispatch for plain `update` commands — passed the request's per-update `collation` but not its `arrayFilters` to the driver, so a `$[<identifier>]` update sent over the wire (mongosh, any standard client) failed with "No array filter found" while the identical update worked against the InMemoryDriver directly. Third instance of the fast-path-drops-request-options bug class (#252: `ordered`/`collation`, createIndexes: index specs); covered by a `FastPathOptionsTest` seam test like the others.

#### InMemoryDriver/PoppyDB: auth commands no longer pretend to succeed (#245)
The entire server-side authentication surface — `saslStart`, X.509 `authenticate`, `createUser`, `createRole` — consisted of empty stubs that queued no result, which the command-dispatch machinery resolved to `{ok:1.0}`: every client "authenticated" successfully with any or no credentials, and `createUser`/`createRole` reported success while creating nothing. These commands now fail loudly (`AuthenticationFailed`/`NotImplemented` with an unmistakable message) until real SCRAM verification and a user/role store exist. InMemoryDriver/PoppyDB still perform **no** authentication — do not expose them to untrusted networks.

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
