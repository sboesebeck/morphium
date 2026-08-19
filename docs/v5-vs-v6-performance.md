# Performance Comparison: v5.1.x vs v6.x

*Benchmarked on MongoDB 8.2, 3-node replica set, January 2026*

---

## Executive Summary

| Aspect | v5.1.x → v6.x | Improvement |
|--------|---------------|-------------|
| **Connection Pool** | Global lock → Per-host locking | **+38%** throughput |
| **Messaging** | Improved threading & lock handling | Better under load |
| **$in Queries** | Same (MongoDB indexed) | ~8% faster |
| **SSL/TLS** | Not available → Full support | ✅ New feature |

---

## Real Benchmark Results

### v5.1.9 vs v6.x (MongoDB Cluster)

| Benchmark | v5.1.9 | v6.x | Improvement |
|-----------|--------|------|-------------|
| **Connection Pool** (20 threads × 100 ops) | 22,869 ops/sec | 31,642 ops/sec | **+38%** |
| **Messaging** (500 msgs, default settings) | 10 msgs/sec* | 21 msgs/sec | +110% |
| **$in Query** (500 values, indexed) | 3.40 ms | 3.14 ms | +8% |
| **Bulk Writes** (10K docs) | 43,544 docs/sec | 38,219 docs/sec | -12%** |

*\*v5 messaging hit timeout (345/500 received) — may indicate stability issues under load.*
*\*\*Bulk write difference under investigation.*

### Messaging Performance by Backend

| Backend | Throughput | Latency | vs MongoDB |
|---------|------------|---------|------------|
| **MongoDB** (3-node replica set) | 89 msgs/sec | 11.28 ms | 1x |
| **PoppyDB** | 223 msgs/sec | 4.47 ms | **2.5x faster** |
| **InMemory Driver** (direct) | 281 msgs/sec | 3.56 ms | **3.2x faster** |

> **Key insight:** PoppyDB is 2.5x faster than real MongoDB for messaging tests!

These are **round-trip** numbers: complete ping-pongs (request out, response received).
PoppyDB's edge here is latency — with less than half the per-message round-trip time, the
same workload completes 2.5x faster.

> **Re-measured 2026-08-07** (Morpheus `latency --headless`, 100 msg/s fixed rate, 5 sender
> threads, 30 s measured after 10 s warmup, Mac Studio M1 Ultra client; PoppyDB = local
> 3-node replica set, MongoDB = the 3-node homelab replica set): median RTT **2.4 ms**
> against PoppyDB vs **5.7 ms** against MongoDB; averages 2.5 ms vs 7.9 ms — MongoDB's mean
> carries a fat majority-fsync tail (p99 70–128 ms), PoppyDB's p99 stays under 5 ms. A
> same-session A/B against the pre-optimization baseline attributes **8–18 % lower median
> RTT** to the 2026-08 messaging optimizations (answers dispatched before the
> `processed_by` write; non-exclusive messages processed straight from the change-stream
> `fullDocument`): PoppyDB p50 2.89 → 2.42 ms, MongoDB p50 6.23 → 5.1–5.7 ms. Beware the
> cold-start trap when reproducing: the very first run after server start measures JIT, not
> the code — discard it (ours read 2× slower than the warm steady state). The table above
> keeps the original serial-ping-pong figures; both setups measure the same path under
> different load profiles, so compare within a vintage, not across.
>
> **Topology caveat for the 2026-08-07 run:** it is not like-for-like. PoppyDB ran locally
> on the client machine while MongoDB was reached over the network (homelab, via VPN), so
> the ratio carries a network component that is not attributable to the broker. See the
> symmetric re-measurement below.

> **Symmetric re-measurement 2026-08-11** — same Morpheus parameters (100 msg/s fixed rate,
> 5 sender threads, 30 s after 10 s warmup), but with every known bias removed: the client
> runs *inside* the homelab network on its own host (4 cores, no other load), and **both**
> backends are separate processes on dedicated hosts at equal network distance — PoppyDB as
> a 3-node replica set (`poppydb.fritz.box:17017-19`, no in-process advantage), MongoDB as
> the 2-node homelab replica set (`mongo1/mongo2:27017`). Two consecutive runs, 3001 pings
> each, zero loss:
>
> | | MongoDB (run 1 / 2) | PoppyDB (run 1 / 2) |
> |---|---|---|
> | p50 | 4.97 / 5.12 ms | **2.13 / 2.06 ms** |
> | avg | 6.10 / 8.34 ms | 2.41 / 2.40 ms |
> | min | 3.89 / 3.91 ms | 1.33 / 1.35 ms |
> | p90 | 6.80 / 7.24 ms | 2.91 / 2.63 ms |
> | p99 | 42.3 / 129.4 ms | **5.5 / 6.7 ms** |
> | max | 86.0 / 214.6 ms | 40.8 / 55.4 ms |
> | jitter | 1.28 / 1.55 ms | 0.54 / 0.52 ms |
>
> The ratio comes out at **2.34× and 2.49×**, confirming the ~2.5× of the earlier runs — the
> asymmetric topology of 2026-08-07 did not manufacture the advantage. Two things the median
> hides: the tail differs by an order of magnitude (MongoDB p99 42–129 ms at a mere 100 msg/s
> on an idle cluster, PoppyDB under 7 ms), and jitter differs by 2.5–3×. For latency-critical
> request/reply the tail is the more relevant figure.

### Exclusive request/reply — the production profile (measured 2026-08-12)

All numbers above ride the **broadcast (non-exclusive) path**: any listener may answer, no
lock traffic. Production request/reply between services typically uses **exclusive** messages
— exactly-once processing, which costs the responder side the full lock/claim machinery
(claim write, re-fetch, `processed_by` mark, each majority-acked on MongoDB). Measured with
Morpheus `latency --exclusive` against `pong --work 5` (5 ms simulated handler work, modeling
a real consumer), same parameters as the symmetric run above (100 msg/s, 5 sender threads,
30 s recorded after 10 s warmup, two consecutive runs, ~4,000 pings each, zero loss
everywhere). Client: Mac Studio (M1 Ultra) on the same LAN segment, 0.5–0.6 ms ICMP RTT to
both brokers — equal network distance, but a different client host than the 2026-08-11 run,
so compare ratios, not absolutes, across the two sections. morphium 6.3.1 client (with the
6.3.1 topic-filter and lock-callback fixes), PoppyDB 3-node RS on a 6.3.1-era build, MongoDB
8.0.26 as the 2-data-node + arbiter homelab RS.

| Profile | | MongoDB (run 1 / 2) | PoppyDB (run 1 / 2) |
|---|---|---|---|
| broadcast ping | p50 | 4.43 / 4.36 ms | 2.83 / 2.71 ms |
| | p99 | 88.7 / 48.1 ms | 8.0 / 7.0 ms |
| exclusive | p50 | 11.81 / 12.83 ms | **3.87 / 3.93 ms** |
| | p99 | 807.8 / 1004.9 ms | **12.2 / 15.0 ms** |
| exclusive + 5 ms work | p50 | 18.45 / 18.47 ms | **11.02 / 11.57 ms** |
| | p99 | 726.0 / 2209.3 ms | **22.5 / 23.6 ms** |

Three observations:

- **The exclusive flag is nearly free on PoppyDB and expensive on MongoDB.** Going from
  broadcast to exclusive costs PoppyDB ~1.1 ms at the median (claim round-trip against an
  in-memory server); MongoDB pays ~8 ms — the claim/mark writes are majority-acked, so the
  exclusive path stacks additional majority-commit cadences on top of the delivery floor.
  Median ratio between the backends grows from ~1.6× (broadcast) to ~3.2× (exclusive).
- **The exclusive tail on MongoDB is a different regime, not a bigger number.** At a mere
  100 msg/s on an otherwise idle cluster, exclusive p99 lands at 0.7–2.2 **seconds** (p90 up
  to 630 ms, max 2.7 s), and the tail is unstable between consecutive runs. PoppyDB's p99
  stays at 22–24 ms with run-to-run stability. For burst-shaped incident patterns (callers
  waiting hundreds of ms for tens of ms of work) the exclusive tail is the number to watch.
- **The 5 ms simulated handler work adds more than 5 ms** (PoppyDB +7 ms, MongoDB +6 ms at
  the median): a busy handler delays subsequent claims of the single consumer, so queueing
  briefly appears even below nominal capacity. Real deployments spread this across more
  consumers.

Topology caveat: with 2 data nodes + arbiter, the majority commit needs *both* data nodes —
a 3-data-node set can acknowledge with the faster secondary, which may soften (not remove)
the MongoDB tails. The broadcast rows are consistent with the 2026-08-11 symmetric run
above; the exclusive rows measure the same path that production sync request/reply uses.

### Messaging One-Way Throughput (send → receipt, no replies)

Measured 2026-08-06 with `MessagingOneWayThroughputBenchmark` (poppydb module, tag `manual`):
5000 messages, 4 sender threads, one listening receiver, clock from first send to last
receipt. Same 4-CPU test-runner LXC as the CI matrix; MongoDB is the 3-node homelab replica
set on separate hosts, PoppyDB runs in-process.

| Backend | Host | One-way throughput |
|---------|------|--------------------|
| **MongoDB** (3-node replica set, external hosts) | 4-CPU test runner | 868 msg/s |
| **MongoDB** (3-node replica set, external hosts) | Mac Studio (M1 Ultra, 64GB) | 1100–1250 msg/s (2026-08-07) |
| **PoppyDB** (in-process) | 4-CPU test runner | 769 msg/s |
| **PoppyDB** (in-process) | MacBook Pro (M1 Max, 32GB) | 2101 msg/s |
| **PoppyDB** (in-process) | Mac Studio (M1 Ultra, 64GB) | 4300–4900 msg/s (2026-08-07) |

> **Honest reading:** one-way throughput is write-bound, and an in-process PoppyDB shares its
> host's CPU with sender and receiver — on a small 4-core host it lands slightly *below* an
> external replica set, while on a laptop-class CPU it is well above. PoppyDB's advantage is
> round-trip latency (table above), not raw one-way throughput on constrained hardware. A
> historic "~8K msg/s" one-way figure circulated in older READMEs; it most likely stemmed
> from plain document-write throughput (compare the bulk-write numbers above), not from
> messaging with a listening receiver, and is superseded by these measurements.
>
> The M1 Max and M1 Ultra rows are different machines — in-process throughput simply scales
> with the host. An A/B run on the M1 Ultra on 2026-08-07 (baseline vs. the 2026-08
> optimization round: O(1) duplicate-`_id` insert pre-check, dead messaging index removed,
> `fullDocument` fast path) showed **no** significant change on this benchmark — with a
> near-empty collection, throughput is bound by the per-collection write lock, exactly as
> the write-concurrency plateau predicts. The same A/B against the MongoDB replica set
> (Mac Studio client, 2026-08-07) is also flat: there the benchmark is sender-bound
> (sendRate ≈ endToEndRate — four threads doing synchronous majority-acked inserts over the
> network), and the receiver-side re-read the `fullDocument` fast path removes shows up as
> delivery latency, not one-way throughput. Its effect belongs to the round-trip table
> above — hence the re-measurement note there. What the optimization round *did* change:
> insert cost no longer grows with collection size. Single-document inserts into a
> collection pre-filled with 200K documents went from ~97 inserts/s (per-insert O(N) `_id`
> scan) to ~205,000 inserts/s (O(1) index lookup) in the same A/B setup.
>
> **Update 2026-08-19:** the unbatched baseline in the batch-throughput probe below measured
> 7054 msg/s on this same Mac Studio — noticeably above the 4300–4900 msg/s here. The gap is
> `79dc8da9b` (`perf(mapper): cache type-id class resolution and no-arg constructor lookup`,
> merged 2026-08-13, after this 2026-08-07 measurement) — every `sendMessage()` goes through
> the object mapper, so its ~23% roundtrip improvement shows up here too.

### Batch Send Throughput: `@WriteBuffer` vs. Client-Side Bulk Insert

Prompted by a "could we batch messages like Kafka does" question. Measured 2026-08-19 with
`MessagingBatchSendThroughputBenchmark` (parked on branch
`bench/messaging-batch-send-throughput`, not merged into `develop` — see below): 5000
messages, 4 sender threads, one listening receiver, same clock methodology as the one-way
benchmark above (first send to last receipt), comparing three ways of getting those 5000
messages out: plain `sendMessage()` (unbatched baseline); routing through
`@WriteBuffer(size=10/100)` at two `writeBufferTimeGranularity` settings (Morphium's default
100ms poll, and a tightened 5ms poll); and genuine client-driven bulk insert —
`morphium.insert(List<Msg>, collection, null)`, a null callback runs it synchronously as one
real bulk-insert wire command per chunk, no `@WriteBuffer` or housekeeping thread involved at
all. PoppyDB in-process; MongoDB is a local 3-node replica set on the *same host*
(`mongotest.sh up --noauth`, MongoDB 8.3.7) — loopback, not the homelab RS used elsewhere in
this document, so don't compare these MongoDB numbers to the round-trip/one-way tables above.
Mac Studio (M1 Ultra, 64GB) throughout.

| Variant (PoppyDB, in-process) | sendRate | end-to-end rate | last-message latency |
|---|---|---|---|
| unbatched (`sendMessage()`) | 7054 msg/s | 7053 msg/s | 0.0 ms |
| bulk insert, chunks of 10 | 15,934 msg/s | 11,618 msg/s | 117 ms |
| bulk insert, chunks of 100 | 38,795 msg/s | **16,290 msg/s** | 178 ms |
| `@WriteBuffer(size=10)`, default 100ms poll | 86 msg/s | 64 msg/s | 20.1 s |
| `@WriteBuffer(size=100)`, default 100ms poll | 731 msg/s | 185 msg/s | 20.1 s |
| `@WriteBuffer(size=10)`, tuned 5ms poll | 724 msg/s | 186 msg/s | 20.0 s |
| `@WriteBuffer(size=100)`, tuned 5ms poll | 5130 msg/s | 5015 msg/s | 22 ms |

| Variant (MongoDB, local 3-node RS, same host) | sendRate | end-to-end rate | last-message latency |
|---|---|---|---|
| unbatched (`sendMessage()`) | 291 msg/s | 291 msg/s | 0.6 ms |
| bulk insert, chunks of 10 | 2720 msg/s | 1227 msg/s | 2.2 s |
| bulk insert, chunks of 100 | 18,313 msg/s | **1128 msg/s** | 4.2 s |
| `@WriteBuffer(size=10)`, default 100ms poll | 40 msg/s | 35 msg/s | 20.1 s |
| `@WriteBuffer(size=100)`, default 100ms poll | did not complete — see below | | |

**The genuine win is client-side bulk insert**, and it needs no annotation, no housekeeping
thread, no tuning: one list, one method call, one wire command. Against MongoDB it roughly
**quadruples** end-to-end throughput over unbatched (1128 vs. 291 msg/s) — exactly where
batching should pay off, since one bulk write amortizes the majority-ack/journal-fsync cost of
the write-concern round-trip across many documents instead of paying it once per message.
`sendMessage()` itself has no bulk variant today; this is a call to `Morphium.insert()`
directly, alongside the messaging layer rather than through it.

**`@WriteBuffer` underperforms — it was never built for this.** It predates Morphium
Messaging and flushes on a housekeeping thread that polls at a fixed interval
(`writeBufferTimeGranularity`, default 100ms). Once producers fill the buffer faster than that
thread drains it, the poll interval becomes a hard throughput *ceiling* of roughly
`size / interval` (10 msgs / 0.1s ≈ 100 msg/s — matching the measurements above almost
exactly), not a booster. Tightening the interval to 5ms narrows the gap substantially
(PoppyDB `size=100` goes from 731 to 5130 msg/s) but can't close it against bulk insert's
direct, single-round-trip path — a faster poll is still a poll. Against MongoDB, `size=100` at
the default interval didn't just underperform, it **stalled outright**: the housekeeping
thread flushes synchronously, one buffer at a time, and a majority-acked 100-document bulk
write occasionally took long enough (compounded by intermittent connection churn observed
against the local RS during this run) that the buffer sat pinned at capacity for 40+ seconds
straight — long enough for blocked producer threads to exceed even a 20-second wait-timeout
headroom and abort. That's a consequence of pushing `@WriteBuffer` well outside the workload it
was designed for (bounded-latency background writes, not synchronous-ack message delivery at
thousands of msg/s), not a MongoDB or Morphium reliability issue.

**Two real bugs came out of building this probe, both found and fixed the same day, on
`develop`:**
- PoppyDB's change-stream cursor silently dropped roughly 1% of events under a bulk-insert
  burst — an off-by-one batch-cap check in `WatchCursorManager.drainEvents()` polled the
  101st event off the queue before checking the 100-event cap, then discarded it (`4e8ccebf1`).
  The same drain path backs tailable cursors too, so this likely also explains the
  long-standing `TailableQueryTests` flakiness on PoppyDB noted in the homelab test matrix.
- Messaging's poll fallback let messages for topics without a registered listener — left
  unmarked on purpose, so a listener registered later still receives them — permanently occupy
  slots of the `limit(windowSize)` poll window, starving deliverable messages until the
  blockers expired via TTL (`20a8bf36a`).

The benchmark itself is a one-off exploratory probe, not an ongoing regression test — it lives
on branch `bench/messaging-batch-send-throughput` rather than `develop`.

### $in Query: Indexed vs Non-Indexed

| Field | MongoDB | InMemory |
|-------|---------|----------|
| Indexed (counter, 500 values) | 5.52 ms | 81.30 ms |
| Non-indexed (category, 50 values) | 10.39 ms | 16.60 ms |

MongoDB indexes make a huge difference. InMemory shows O(n×m) behavior without indexes.

---

## Architecture Improvements

### PooledDriver: Per-Host Locking

**v5.1.x:**
```java
// Global synchronized blocks - all hosts blocked
private synchronized MongoConnection borrowConnection(String host) {
    synchronized (connectionPool) { ... }
}
```

**v6.x:**
```java
// Per-host isolation with modern concurrency
private final Map<String, Host> hosts = new ConcurrentHashMap<>();

class Host {
    private final BlockingQueue<ConnectionContainer> pool;
    private final AtomicInteger borrowedConnections;
}
```

**Result:** Operations on different hosts don't block each other.

### Messaging Improvements

Both v5 and v6 use ChangeStream, but v6 has:

- **Better thread pool management** — Configurable core/max sizes
- **Improved resume token handling** — More reliable after disconnects  
- **Lock optimizations** — Less contention in message processing
- **Java 21 threading** — Ready for virtual threads

**Result:** Better throughput with optimized settings.

---

## PoppyDB for Testing

PoppyDB provides a MongoDB-compatible server backed by InMemoryDriver:

| Feature | Benefit |
|---------|---------|
| **No MongoDB required** | CI/CD without Docker |
| **2.5x faster messaging** | Faster test suites |
| **Full wire protocol** | Drop-in replacement |
| **Clustering support** | Test replica set scenarios |

### Quick Start

```bash
# Start server
mvn exec:java -Dexec.mainClass="de.caluga.poppydb.PoppyDBCLI" \
    -Dexec.args="-p 17017"

# Connect Morphium
MorphiumConfig cfg = new MorphiumConfig();
cfg.addHostToSeed("localhost:17017");
cfg.setDatabase("test");
Morphium m = new Morphium(cfg);
```

---

## InMemory vs MongoDB: When to Use What

| Use Case | Recommendation |
|----------|----------------|
| Unit tests | InMemory Driver (fastest) |
| Integration tests | PoppyDB (realistic + fast) |
| Load testing | Real MongoDB (production-like) |
| CI/CD pipelines | PoppyDB (no dependencies) |

### InMemory Driver Limitations

- No real indexes (full collection scan)
- $in queries are O(n×m) not O(n+m)
- No persistence across restarts

---

## Tuning Messaging Performance

Default settings are conservative. For high-throughput:

```java
// Optimized settings
SingleCollectionMessaging messaging = new SingleCollectionMessaging(
    morphium,
    10,      // pause: 10ms (default: 100ms)
    true,    // multithreaded
    100      // windowSize (default: 10)
);
```

| Setting | Default | Optimized | Effect |
|---------|---------|-----------|--------|
| pause | 100ms | 10ms | Lower latency |
| multithreaded | false | true | Parallel processing |
| windowSize | 10 | 100 | Batch efficiency |

---

## Migration Checklist

Upgrading from v5.1.x to v6.x:

- [ ] Java 21 required
- [ ] Update `Messaging` → `SingleCollectionMessaging`
- [ ] Review messaging settings for optimal performance
- [ ] Enable SSL/TLS for production (new in v6!)
- [ ] Consider PoppyDB for tests

See [Migration Guide v5→v6](./howtos/migration-v5-to-v6.md) for details.

---

*Benchmarks run on Mac Studio M2 Ultra, MongoDB 8.2.4, Morphium 6.1.8-SNAPSHOT*
