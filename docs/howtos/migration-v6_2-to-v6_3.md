# Migration v6.2.x → v6.3.0

This guide covers breaking changes, deprecations, and the headline new features when upgrading
from Morphium 6.2.x to 6.3.0. 6.3.0 is a large release, dominated by InMemoryDriver/PoppyDB
correctness and production-readiness work; if you only use Morphium against real MongoDB and
never touch the embedded driver or PoppyDB, most of this guide does not apply to you — skip to
[Breaking Changes That Affect Real MongoDB Users](#breaking-changes-that-affect-real-mongodb-users)
and [New: DualChannelMessaging](#new-dualchannelmessaging-beta).

No dependency version bumps in this release (Netty/BSON/SLF4J/Logback are unchanged from 6.2.10).

## Breaking Changes That Affect Real MongoDB Users

### Mid-message read timeouts now close the connection instead of silently reusing it

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

## Deprecations — the 7.0-removal wave (#218)

Members confirmed for removal in 7.0 now carry `@Deprecated(since = "6.3", forRemoval = true)`,
so IDEs flag every usage a full minor release ahead of time. This is a pure annotation/Javadoc
change — nothing behaves differently in 6.3.0, and everything listed still works. Covered:

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

## New: PoppyDB — production-readiness features

- **DevOps command surface**: `db.currentOp()`/`killOp` (with a real op registry), `rs.conf()`,
  `listCommands`, `hostInfo`, `connectionStatus`, `whatsmyuri`, real `serverStatus.connections`.
  Fixes mongosh's admin helpers, which previously failed against PoppyDB.
- **Opt-in auth enforcement** (`--auth`) with real server-side SCRAM-SHA-1/SHA-256 verification
  (RFC 5802/7677) and a working `createUser`. Without `--auth`, behavior is unchanged (fully open).
  Authorization is authentication-only for now — roles are stored but not evaluated.
- **TLS actually works now** — it was silently broken (NPE on startup) whenever an `SSLContext` was
  configured.
- **`--log-level` option** — the CLI jar no longer floods disks by logging everything at DEBUG by
  default (root now defaults to `INFO`).
- **Replication correctness**: index definitions are now replicated (previously documents only —
  unique constraints, TTL, and index-backed queries silently didn't work on secondaries or after
  failover); replication is now lossless and order-preserving; a long-standing election bug that
  kept followers from ever starting replication is fixed.
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
- `dbHash`, `validate`, `currentOp`, real `serverStatus`, and the MongoDB-8.0-style top-level
  `bulkWrite` command.

If any of your tests were relying on a previously-stubbed or silently-wrong behavior in this area
(several dozen correctness fixes shipped alongside the new features — see `CHANGELOG.md`'s
`[Unreleased]`/6.3.0 section for the full list), re-run your suite against InMemoryDriver/PoppyDB
after upgrading.

## New: Messaging improvements (all implementations)

- Configurable `messagingDefaultTtl` (default 30s, was hardcoded) and `messagingFallbackPollInterval`
  (default 10s) — tune the poll interval below your shortest message TTL.
- Requeued messages (cleared `processedBy` via a plain DB update) are now picked up event-driven
  instead of waiting for the next fallback poll.
- Change-stream liveness now drives the fallback poll directly — a silent stream triggers an
  immediate poll instead of waiting for the next timer tick.
- A bounded processing-decision trace aids answer-timeout diagnostics (dumped only on timeout, not
  during normal operation).

## Migration Checklist

1. [ ] **Search for `forRemoval = true` candidates** (see Deprecations above) and migrate
   opportunistically — not urgent for 6.3.0, but IDEs will now flag them.
2. [ ] **If you test against InMemoryDriver/PoppyDB**, re-run your suite — several dozen
   correctness fixes may surface previously-masked test bugs (see the two Breaking Changes
   sections above).
3. [ ] **If you run PoppyDB in production**, review the new `--auth`, `--memory-warn`/
   `--memory-reject`, and `--log-level` options — defaults preserve prior (open, unbounded, DEBUG)
   behavior, so nothing changes unless you opt in.
4. [ ] **If you store documents that could exceed 16MB** or write batches that could exceed 48MB
   against InMemoryDriver/PoppyDB, verify you're within the now-enforced limits (or raise them).
5. [ ] **Optional:** if request/reply throughput is your bottleneck on real MongoDB and you can run
   a homogeneous cluster, evaluate the beta `DualChannelMessaging` implementation.
6. [ ] No dependency version changes — nothing to reconcile in your own `pom.xml`.
