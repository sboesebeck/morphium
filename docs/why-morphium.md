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
- **Limited control** — Little influence over mapping behavior
- **Conflicts with other mappers** — The driver "wants" to map itself, which can lead to **double mapping** when integrating with other frameworks
- **No caching integration** — You have to build caching yourself

### Why Morphium Has Its Own Driver (since v5.0)

The official driver's built-in mapping conflicted with Morphium's mapping:
- Double mapping (performance loss)
- Unexpected type conversions
- Hard-to-debug errors

**The solution:** A custom wire-protocol driver, **tailored exactly to Morphium's needs**.

**Benefits of the custom driver:**
- **Lightweight** — Only what Morphium needs, no overhead
- **Full control** — Mapping, retry, failover by our rules
- **InMemory Driver possible** — The lean driver made a complete in-memory implementation practical

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

Morphium caches automatically locally. For **cluster-wide synchronization**, you need a `CacheSynchronizer`:

```java
// Enable cache synchronization in cluster
CacheSynchronizer cacheSynchronizer = new CacheSynchronizer(messaging, morphium);
```

The CacheSynchronizer uses the messaging system to propagate cache invalidations to all instances. No Redis/Memcached setup needed — just Morphium's own messaging.

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
| MongoDB Atlas (sharded clusters) | **Official Driver** (Morphium supports Atlas replica sets via `mongodb+srv://`, but not sharded clusters) |
| Maximum throughput (>50K ops/sec) | **Official Driver** (less overhead) |
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
