# Messaging Implementations

Morphium provides three messaging implementations that share the same API (`MorphiumMessaging`) but differ in storage layout and scalability characteristics.

## Summary

- Standard (`StdMessaging`, name: `StandardMessaging`)
  - Single queue collection per queue name (e.g., `msg`).
  - Direct messages (recipient‑targeted) are stored in the same collection.
  - One lock collection per queue (e.g., `msg_lck`) for exclusive messages.
  - Simpler layout, good default for small to medium setups or few topics.

- Advanced (`AdvancedSplitCollectionMessaging`, name: `AdvMessaging`)
  - One collection per topic: `<queue>_<topic>` (spaces, dashes, slashes removed).
  - Direct messages use a dedicated collection per recipient: `dm_<senderIdCamelCase>`.
  - Lock collections per topic: `<queue>_lck_<topic>`.
  - Optimized change stream efficiency and reduced contention on busy/many‑topic systems.

- **Dual Channel (`DualChannelMessaging`, BETA, since 6.4.0)**
  - A complete fork of Standard: identical single-collection layout and change-stream cursor for
    broadcast/topic traffic - bit-for-bit the same backpressure/window behavior as Standard.
  - Adds a *second* delivery lane purely for directed messages and answers: each participant gets
    its own per-recipient collection `<queue>_dm_<senderIdCamelCase>` with its own dedicated
    change-stream cursor and dispatcher thread.
  - Rationale: load testing showed the throughput ceiling on MongoDB is delivery-bound - a single
    change-stream cursor delivers at a fixed cadence regardless of collection layout. Advanced's
    capacity edge over Standard comes from exactly this second cursor, not from the per-topic
    split. Dual Channel isolates that one idea on top of an otherwise unmodified Standard core.
  - Marked `@Beta` (see `de.caluga.morphium.annotations.Beta`): behavior, collection layout, or API
    surface may change without a deprecation cycle. See
    [GitHub issue #265](https://github.com/sboesebeck/morphium/issues/265).
  - **Mixed-cluster requirement:** every participant on a given queue must run
    `DualChannelMessaging` for DM/answer delivery to work both ways - see the dedicated section
    below.

Standard and Advanced support:
- Change Streams vs polling (`useChangeStream`), window size, multithreading, answers, exclusive vs broadcast.

Dual Channel supports all of the above too, plus its own DM lane settings (see Configuration below).

## Choosing an Implementation

- Start with Standard. If you have many topics, high listener fan‑out, or change streams getting noisy, move to Advanced.
- If your bottleneck is specifically request/reply (answer) throughput and you can run a
  homogeneous cluster (see below), try the beta Dual Channel implementation instead of Advanced -
  it keeps Standard's simpler single-collection layout for broadcast/topic traffic and only adds
  the second cursor where it actually helps.
- Names used for selection:
  - `StandardMessaging` → `StdMessaging`
  - `AdvMessaging` → `AdvancedSplitCollectionMessaging`
  - `DualChannelMessaging` (beta) → `DualChannelMessaging`

## Dual Channel Messaging (Beta)

Activate via configuration only - no code changes beyond the implementation name:

```java
cfg.messagingSettings().setMessagingImplementation("DualChannelMessaging");
```

Mechanism:
- Broadcast/topic messages (no recipients set) use the same collection and change-stream cursor
  as Standard, with identical window-limited polling and backpressure guards.
- Directed messages (recipients set - both explicit point-to-point requests and answers created via
  `Msg#sendAnswer`/`setRecipient`) are routed into the *recipient's own* per-recipient collection:
  `<queue>_dm_<senderIdCamelCase>`. That collection has its own change-stream cursor and a
  dedicated dispatcher thread, so answer/DM delivery is no longer serialized behind the main
  broadcast cursor.
- Answers are dispatched to a waiting `sendAndAwaitFirstAnswer`/`sendAndAwaitAsync` caller *before*
  the `processed_by` write is persisted (same pattern as the perf fix in 6.3.0 for Standard/
  Advanced), further cutting request/reply latency.
- The DM collection (incl. its TTL index) is created on `start()` and dropped again on
  `terminate()`. A registry-gated periodic sweep
  (`MessagingSettings#setMessagingDmCleanupOrphansOnStartup`, default `true`, requires
  `messagingRegistryEnabled=true`) drops OTHER empty, inactive participants' DM collections left
  behind by crashed/removed nodes - it never touches a non-empty collection or one belonging to a
  currently active participant.

**Mixed-cluster requirement (read this before enabling in production):** there is no dual-read/
dual-write bridge between the collection layouts.
- A `DualChannelMessaging` node CAN still receive and answer requests from legacy
  `StandardMessaging` nodes on the same queue (the forked main lane keeps Standard's answer-handling
  path as a compatibility side effect).
- The reverse does NOT work: a `StandardMessaging` node awaiting an answer from a
  `DualChannelMessaging` responder will time out, because the answer is written to the requester's
  DM collection, which legacy code never reads.
- Practically: **all messaging participants on a given queue must run `DualChannelMessaging`**
  for DM/answer delivery to work in both directions. Every `DualChannelMessaging` instance logs a
  `WARN` on startup restating this. Migrate with the same big-bang or bridge approach described
  under "Migrating Standard → Advanced" below (the same caveats apply).

## Measured Behavior Under Load (July 2026)

Request/reply load tests with the [Morpheus](https://github.com/sboesebeck/morpheus) latency tool
(`sendAndAwaitAsync` against a pong responder, 15 sender threads, 5 s warmup; everything on one
host — Apple Silicon macOS, MongoDB as a local 3-node replica set vs. a local 3-node PoppyDB
replica set; Morphium 6.3.0-SNAPSHOT). Two rounds were run; the numbers below are from the second,
slower ramp (`50:25:20` — start 50 msg/s, +25 msg/s every 20 s, 150 s total), which gives each rate
step enough dwell time to reach a real steady state before the next increase. All three
implementations (Standard, MultiCollection, Dual Channel) were measured side by side against both
backends.

### Latency floor (below saturation, 50–100 msg/s)

| | MongoDB | PoppyDB |
|---|---|---|
| RTT p50 | ~27 ms | ~3 ms |
| outbound / return leg | 13.3 / 13.5 ms | 1.6 / 1.2 ms |

The MongoDB floor is durability-bound, not implementation-bound: `Msg` writes use
`@WriteSafety(MAJORITY)`, and change streams only ever deliver majority-committed events, so every
hop pays the majority-commit cadence. On macOS this includes an F_FULLFSYNC per journaled write
(~10–15 ms on Apple SSDs) — on Linux the same setup lands in the low single-digit milliseconds, so
absolute numbers from macOS overstate the gap to in-memory servers considerably.

Since 6.3.0 the answer path delivers the answer *before* persisting the `processed_by` mark.
Before that reorder the return leg carried an extra majority-acked write, making it twice as
expensive as the outbound leg (measured 2.0× → 1.0× after the fix, ~40% lower request/reply RTT
on MongoDB).

### Throughput ceiling and overload behavior — MongoDB

Steady-state window (offered rate 175–225 msg/s, well past every implementation's knee):

| Implementation | Throughput | RTT p50 | RTT p95 |
|---|---|---|---|
| Standard | 121.1 msg/s | 410 ms | 723 ms |
| MultiCollection | 139.4 msg/s | 305 ms | **2044 ms** |
| **Dual Channel (beta)** | **131.7 msg/s** | **274 ms** | **519 ms** |

Dual Channel beats Standard on *both* axes — +9% throughput, −33% p50, −28% p95 — delivering on
its design goal: a second delivery lane without inheriting MultiCollection's runaway behavior.
MultiCollection does reach a higher raw number, but by accepting more than it can reliably
deliver: RTT repeatedly climbs into the multi-second range (up to ~2 s, near the answer timeout)
and back down rather than settling on a stable plateau — the same "accepts more than it delivers"
pattern the original motivation for this work identified. The +9% throughput gain is more modest
than the ~30% (~145→190 msg/s) implied by that original motivation; a slower/longer ramp than the
one used in the initial measurement was needed to even see a clean, reproducible edge over
Standard, since both implementations self-limit toward the same ~120–145 msg/s range unless the
offered load is sustained well past that knee for tens of seconds.

Two findings worth keeping in mind when choosing an implementation:

- **The ceiling on MongoDB is delivery-bound, not write-bound.** Lowering the write concern to
  w:1 would not lift it: change-stream visibility still waits for the majority commit point.
  It would only weaken the exactly-once guarantees for exclusive messages (processed_by marks and
  locks can roll back on failover).
- **MultiCollection's capacity edge comes from its second cursor** (answers/DMs through the
  dedicated DM collection), not from the per-topic split: on mongod every change-stream cursor
  tails the whole oplog regardless of collection layout, and Standard already filters relevance
  server-side in its pipeline. `DualChannelMessaging` isolates exactly that one mechanism on top of
  an unmodified Standard core — see [#265](https://github.com/sboesebeck/morphium/issues/265).

### Throughput ceiling — PoppyDB (no differentiation, and why)

| Implementation | Throughput | RTT p50 | RTT p95 |
|---|---|---|---|
| Standard | 186.6 msg/s | 22.6 ms | 176.3 ms |
| MultiCollection | 186.8 msg/s | 20.9 ms | 157.1 ms |
| Dual Channel (beta) | 186.9 msg/s | **17.4 ms** | 158.0 ms |

Throughput is statistically identical across all three (differences are noise) — the second
cursor buys nothing on PoppyDB, even measured over a ~50 s steady-state window well past PoppyDB's
own knee (~250–300 msg/s, notably higher than MongoDB's ~145 msg/s ceiling). This was verified
against the actual code, not just inferred from the numbers: PoppyDB's `InMemoryDriver` delivers
change-stream events **push-based, synchronously, immediately after the write** (`PoppyDB.java`
sets `serverMode=true`, which makes `dispatchEvent` run inline on the writer thread instead of via
polling or a fixed cadence), and a single `getMore` round-trip batches up to 100 events. There is no
"a cursor can only deliver events at a fixed cadence" bottleneck for a second cursor to
parallelize — that bottleneck is specific to mongod's oplog-tailing change streams, which is
exactly what motivated this feature. PoppyDB's own ceiling is write-path bound instead (per-
collection write lock, synchronous per-insert subscriber dispatch), not cursor-count bound, so a
second cursor doesn't move it. **No architectural changes are planned for PoppyDB on account of
this feature.** The one real, if small, side effect: Dual Channel's steady-state p50 is lowest of
the three despite equal throughput, plausibly because its dedicated DM collection has fewer
subscribers/irrelevant events to match per write than the shared main collection.

## Configuration and Usage

Always instantiate via `Morphium.createMessaging()` so configuration and implementation selection are handled correctly.

```java
// Configure implementation by name
cfg.messagingSettings().setMessagingImplementation("AdvMessaging"); // or "StandardMessaging" (default), or "DualChannelMessaging" (beta)

Morphium morphium = new Morphium(cfg);
MorphiumMessaging messaging = morphium.createMessaging();
messaging.start();

messaging.addListenerForTopic("orders.created", (mm, msg) -> {
  // ...
  return null;
});
```

Relevant settings are the same across all three implementations (see the Messaging page):
- `messageQueueName`, `messagingWindowSize`, `messagingMultithreadded`, `useChangeStream`, `messagingPollPause`.

Dual Channel additionally supports `messagingDmCleanupOrphansOnStartup` (see above).

## Storage Layout Details

- Standard
  - Queue: `<queue>` (default `msg`)
  - Locks: `<queue>_lck`
  - Direct messages: `<queue>`

- Advanced
  - Topic queue: `<queue>_<topic>`
  - Locks: `<queue>_lck_<topic>`
  - Direct messages: `dm_<recipientSenderIdCamelCase>`

- Dual Channel (beta)
  - Broadcast/topic queue: `<queue>` (identical to Standard)
  - Locks: `<queue>_lck` (identical to Standard)
  - Direct messages / answers: `<queue>_dm_<recipientSenderIdCamelCase>` (queue-prefixed, unlike
    Advanced's shared `dm_<sender>` — so multiple queues never collide on the same DM collection
    for a given sender id)

## Migrating Standard → Advanced

Because the storage layout changes, do not mix Standard and Advanced nodes for the same application at the same time.

Recommended approaches
- Big‑bang switch (simplest):
  - Drain or pause message producers.
  - Stop all consumers (nodes).
  - Update config: `cfg.messagingSettings().setMessagingImplementation("AdvMessaging")`.
  - Start all nodes; verify listeners on expected topics; resume producers.

- Transitional bridge (optional):
  - If you must avoid downtime, temporarily run a small bridge process that reads from Standard (`msg`) and republishes into Advanced per‑topic collections using the same `Msg` payloads. Switch all nodes to Advanced, then remove the bridge.

Notes
- Producers and consumers use the same `MorphiumMessaging` API. The change is purely implementation/configuration.
- Topic names do not change. Advanced derives collection names from topics automatically.
- Indexes are ensured automatically by both implementations on startup.
- Clean‑up: once fully migrated, you may drop the old Standard queue collections (`<queue>`, `<queue>_lck`) after verifying they’re unused.

## Migrating Standard → Dual Channel (Beta)

Same big-bang approach as Standard → Advanced: stop all consumers, switch every node's config to
`DualChannelMessaging`, restart. Since Dual Channel's broadcast/topic collection layout is
identical to Standard's, an even simpler option exists for this specific migration: a rolling
restart is tolerable for broadcast/topic traffic (both implementations read/write the same main
collection compatibly), but request/reply (answers, directed messages) will silently fail for any
`StandardMessaging` requester still waiting on a `DualChannelMessaging` responder mid-rollout (see
the Mixed-Cluster Requirement above) — so requests/replies should still be drained or paused
during the switch, even though broadcast/topic traffic tolerates a rolling restart.

## Cache Synchronization

All three implementations work with `MessagingCacheSynchronizer`. No code changes needed—only the implementation selection via config.

## Troubleshooting

### Messages Not Being Processed

If messages are piling up in MongoDB but not being processed:

1. **Check if ChangeStreamMonitor is running:**
   ```bash
   jstack <pid> | grep "changeStream"
   ```
   If no changeStream thread exists, the monitor may have died.

2. **Check logs for ChangeStream errors:**
   ```bash
   grep -i "changestream\|connection closed" /path/to/logs/*.log
   ```
   
3. **Common causes:**
   - "connection closed" - Network issues or MongoDB failover. Fixed in 6.1.4+ to auto-retry.
   - Logging level too high - Set `logging.level.de.caluga.morphium=WARN` to see ChangeStream logs.

### Duplicate Message Processing

If messages are being processed multiple times:

1. **Check lock TTL:**
   Messages with `timingOut=false` had a bug where lock TTL was 0, causing locks to expire immediately. Fixed in 6.1.4+.

2. **Verify exclusive flag:**
   Ensure `msg.setExclusive(true)` for messages that should only be processed once.

### Debugging Checklist

```bash
# 1. Thread dump - check for blocked threads
jstack <pid> | grep -A 20 "sendAndAwaitFirstAnswer"

# 2. Check if ChangeStream is alive
jstack <pid> | grep "changeStream"

# 3. Check MongoDB message queue
mongosh --eval "db.msg.countDocuments({})"

# 4. Check for unprocessed answers
mongosh --eval "db.msg.countDocuments({in_answer_to: {\$ne: null}})"

# 5. Check recipients of pending messages
mongosh --eval "db.msg.distinct('recipients')"
```

### Logging Configuration

To see ChangeStream-related logs, ensure your logging level is at least WARN:

```properties
# application.properties (Spring Boot)
logging.level.de.caluga.morphium=WARN
logging.level.de.caluga.morphium.changestream=INFO
logging.level.de.caluga.morphium.messaging=INFO
```

