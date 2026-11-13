# Why Morphium?

*An honest comparison for experienced Java developers*

---

## The Problem with the Official MongoDB Driver

The official MongoDB Java Driver has **two faces**:

1. **Low-Level API:** Working with `Document` objects, manual mapping
2. **POJO Codec:** Built-in object mapping with its own Codec Registry

Sounds good in theory, but there are practical issues:

### The Official Driver's POJO Codec

```java
// Official Driver with POJO Codec
CodecRegistry pojoCodecRegistry = fromRegistries(
    MongoClientSettings.getDefaultCodecRegistry(),
    fromProviders(PojoCodecProvider.builder().automatic(true).build())
);

MongoCollection<User> collection = database
    .getCollection("users", User.class)
    .withCodecRegistry(pojoCodecRegistry);

User user = collection.find(eq("username", "alice")).first();
```

**Problems:**
- **Complex configuration** — Codec Registry setup is non-trivial
- **Limited control** — no first-class support for lifecycle hooks, lazy `@Reference` loading,
  field-level encryption, or custom name providers; you get whatever the codec conventions expose
  and no more
- **Conflicts with other mappers** — The driver "wants" to map itself, which can lead to **double mapping** when integrating with other frameworks
- **No caching integration — and that's a real gap, not a minor one** — the driver gives you no
  hook into the write path, no distributed invalidation mechanism, and no deterministic cache-key
  generation for queries. Replicating what Morphium gives you for free (`@Cache` per entity,
  `MessagingCacheSynchronizer`/`WatchingCacheSynchronizer` for cluster-wide invalidation, a
  query-result cache keyed by criteria+sort+projection+paging — see the
  [caching docs](./developer-guide.md#cache-synchronization)) means building, yourself: a wrapper
  around every store/update/delete to know when to invalidate, a way to propagate that across a
  cluster (a message queue you now also have to operate, or your own change-stream consumer with
  fan-out), and a stable cache-key scheme per query shape. Most teams never build this properly —
  they either accept "always hit the DB", or bolt on Redis as a second system where cache
  consistency can now break independently of the database.

### Why Morphium Has Its Own Driver (since v5.0)

Running the official driver's built-in POJO mapping *underneath* Morphium's own ODM mapping meant
mapping every document twice, with two independently-opinionated mappers fighting over the same
object graph:
- Double mapping (real work done twice, not just a "the codec itself is slow" issue)
- Unexpected type conversions where the two mappers disagreed
- Hard-to-debug errors from that disagreement

Note: this is an *integration* problem, not a claim that the official driver's own mapper is slow
in isolation — it isn't, and older claims here about generics support/mapping speed being weak
points of the official driver no longer hold and shouldn't be used as arguments.

**The solution:** A custom wire-protocol driver, **tailored exactly to Morphium's needs**, avoiding
the double-mapping problem entirely since there's only one mapper in the picture.

**Benefits of the custom driver:**
- **Failover, on our terms** — the official driver's failover behavior caused real production
  issues; owning the wire protocol means Morphium controls retry/reconnect/failover semantics
  directly instead of working around someone else's.
- **No double mapping** — a single object-mapping layer, tightly integrated with Morphium's
  lifecycle callbacks, `@Reference` lazy loading, `@Encrypted` fields, and custom type mappers,
  instead of two mappers fighting over the same document.
- **InMemoryDriver** — owning the driver abstraction (`MorphiumDriver`) made a pure-Java,
  no-network in-memory implementation practical; most of the test suite runs against it, no
  MongoDB or Testcontainers required.
- **PoppyDB** — a self-contained, wire-protocol-compatible alternative server exists only because
  Morphium isn't tied to the official driver's internals or assumptions.
- **One abstraction, three interchangeable backends** — the same `MorphiumDriver` interface runs
  against real MongoDB, PoppyDB, and the InMemoryDriver, which wouldn't be possible wrapping a
  driver designed around exactly one server implementation.
- **Wire-level control** — e.g. BSON's 16MB message limit is enforced end-to-end with a custom
  batch splitter; messaging (a MongoDB-collection-based pub/sub) is built directly on top of the
  same driver layer instead of bolted onto a black-box client.

---

## Morphium: The Same Code, Simplified

```java
// Morphium: The same user query
User user = morphium.createQueryFor(User.class)
    .f(User.Fields.username).eq("alice")
    .get();

// Save?
morphium.store(user);

// Done.
```

The entity:
```java
@Entity
public class User {
    @Id private MorphiumId id;
    @Index private String username;
    private String email;
    private Date createdAt;
    
    // Generated Fields enum for type-safe queries
    public enum Fields { id, username, email, createdAt }
}
```

---

## Morphium's Additional Features

*Beyond pure ODM, Morphium offers features you'd otherwise have to build separately:*

### 1. Built-in Messaging (MongoDB-based)

**Fun Fact:** The messaging system was originally created to **synchronize caches across a cluster**. It then evolved into a full-fledged, standalone feature.

Need messaging between services? Normally that means setting up RabbitMQ, Kafka, or similar. With Morphium, you just use MongoDB, which you already have.

**Traditional approach — extra infrastructure:**
```
┌─────────┐     ┌──────────┐     ┌─────────┐
│  App A  │────▶│ RabbitMQ │◀────│  App B  │
└────┬────┘     └──────────┘     └────┬────┘
     │                                │
     └────────────┬───────────────────┘
                  ▼
            ┌──────────┐
            │ MongoDB  │
            └──────────┘

= 2 systems to operate, 2 failure points
```

**With Morphium:**
```
┌─────────┐                    ┌─────────┐
│  App A  │◀──── Messaging ───▶│  App B  │
└────┬────┘                    └────┬────┘
     │                              │
     └────────────┬─────────────────┘
                  ▼
            ┌──────────┐
            │ MongoDB  │  ← Messages live here
            └──────────┘

= 1 system, you already have MongoDB anyway
```

**Messaging code:**
```java
// Producer
Messaging messaging = new Messaging(morphium, 100, true);
messaging.sendMessage(new Msg("order.created", "Order #12345"));

// Consumer (different instance)
messaging.addMessageListener((m, msg) -> {
    System.out.println("New order: " + msg.getValue());
    return null;
});
```

**Features you get "for free":**
- Message Priorities
- Request/Response Pattern
- Distributed Locks
- TTL & Timeouts
- Broadcast & Direct Messages

**The Killer Feature: Persistence & Replay**

Since messages live in MongoDB, they don't get lost. A service that wasn't running when the message was sent (restart, deployment, crash) can **process messages retroactively** once it's back.

```
Service A sends "order.created" at 10:00
Service B is restarting (10:00 - 10:02)
Service B starts at 10:02
→ Service B processes the message from 10:00 ✅
```

With classic message brokers (RabbitMQ, etc.), this "replay" is much more complex to implement — you need Dead Letter Queues, manual replay mechanisms, or additional persistence layers. With Morphium, it's just there.

**Bonus: Messages are queryable!**

Since messages are regular MongoDB documents, you can **search, filter, and analyze** them:

```java
// How many orders were processed today?
long todayOrders = morphium.createQueryFor(Msg.class)
    .f(Msg.Fields.topic).eq("order.created")
    .f(Msg.Fields.timestamp).gte(todayMidnight)
    .countAll();

// Average processing time?
// → Aggregation pipeline over processed_at - timestamp
```

Statistics, dashboards, debugging — all with standard MongoDB queries. With RabbitMQ/Kafka, you need separate monitoring tools or have to export messages to a database first.

---

### 2. Multi-Level Caching (Cluster-aware)

```java
@Entity
@Cache(timeout = 60_000, maxEntries = 1000, strategy = CacheStrategy.LRU)
public class Product {
    // ...
}
```

Morphium caches automatically locally. For **cluster-wide synchronization**, attach a cache synchronizer:

```java
// Enable cache synchronization in cluster
MessagingCacheSynchronizer cacheSynchronizer = new MessagingCacheSynchronizer(messaging, morphium);
```

`MessagingCacheSynchronizer` uses Morphium's own messaging to propagate cache invalidations to all instances — no Redis/Memcached setup needed. There's also a `WatchingCacheSynchronizer`, which watches the underlying collections directly via MongoDB Change Streams instead of relying on messaging (trade-offs and a "which one" guide are in the [Developer Guide](./developer-guide.md#cache-synchronization)).

---

### 3. InMemory Driver for Tests

**Without Morphium:**
- Spin up Testcontainers (slow)
- Or: Write mocks (tedious)
- Or: Embedded MongoDB (deprecated, fragile)

**With Morphium:**
```java
@BeforeEach
void setup() {
    MorphiumConfig cfg = new MorphiumConfig();
    cfg.setDatabase("test");
    cfg.setDriverName(InMemoryDriver.class.getName());
    morphium = new Morphium(cfg);
}
```

- **Starts in milliseconds**
- **~93% MongoDB feature coverage**
- **Aggregation pipelines work**
- **Change streams work**
- **No Docker, no external process**

Need the same in-memory engine reachable over the network — for multi-language integration tests,
CI pipelines, or as a lightweight production message broker/cache (no Docker or MongoDB install
required)? That's **[PoppyDB](poppydb.md)**: the InMemory Driver exposed behind the real
MongoDB wire protocol, so any MongoDB client (Python, Node.js, Go, ...) can connect to it directly.
See the [Production Deployment Playbook](./howtos/poppydb-deployment.md) if you're running it as
more than a test fixture.

---

### 4. Fluent Query API with Type Safety

**Error-prone:**
```java
// Typo? Still compiles!
collection.find(eq("usernmae", "alice"));
```

**Type-safe with Morphium:**
```java
// Compile error on typo!
morphium.createQueryFor(User.class)
    .f(User.Fields.username).eq("alice")
    .get();
```

**Complex queries stay readable:**
```java
List<Order> orders = morphium.createQueryFor(Order.class)
    .f(Order.Fields.status).in(List.of("pending", "processing"))
    .f(Order.Fields.total).gte(100.0)
    .f(Order.Fields.createdAt).gt(lastWeek)
    .sort("-createdAt")
    .limit(50)
    .asList();
```

---

## When Is the Official Driver Better?

Let's be honest: Morphium isn't always the best choice.

| Scenario | Recommendation |
|----------|----------------|
| Need the official driver's full feature surface on day one (GridFS, every admin/aggregation operator) | **Official Driver** — Morphium's own wire-protocol driver covers a subset, see [SSL/TLS guide](./ssl-tls.md) and driver docs for what's supported |
| Team only knows Spring Data | **Spring Data MongoDB** (lower learning curve) |
| No messaging needed, simple CRUD | **Official Driver** is sufficient |
| Already have RabbitMQ/Kafka in stack | Messaging advantage disappears |

---

## When Is Morphium the Better Choice?

| Scenario | Why Morphium |
|----------|--------------|
| Messaging + Persistence in one | No extra infrastructure needed |
| Many tests, fast CI/CD | InMemory Driver saves minutes |
| Cluster-wide caching | Built-in, no Redis |
| Complex domain objects | ODM saves boilerplate |
| Distributed locks | Built-in |
| Team productivity > Raw performance | Less code = fewer bugs |

---

## Conclusion

Morphium is **not a replacement** for the Official Driver — it's an **abstraction layer above it** (or rather, with its own wire-protocol driver).

If you:
- **Already use MongoDB or plan to**
- **Need messaging** (and not Kafka-scale)
- **Want fast tests**
- **Hate boilerplate**

...then Morphium will save you weeks of development time.

---

*Next step: [Quick Start Tutorial](./quickstart-tutorial.md)*
