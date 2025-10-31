# Messaging

Morphium provides a MongoDB‑backed message queue with topic‑based listeners.

Concepts

- Topic: string category for messages (e.g., `user.created`)
- Exclusive vs non‑exclusive:
  - Exclusive (`Msg.setExclusive(true)`): exactly one listener processes the message (one‑of‑n); implemented using a lock collection
  - Non‑exclusive (default): every registered listener for the topic processes the message (broadcast)
- Answers: listeners may return a `Msg` as response; senders can wait synchronously or asynchronously
- Implementations: choose between Standard and Advanced; see [Messaging Implementations](./howtos/messaging-implementations.md) for differences and migration.

Setup
```java
import de.caluga.morphium.messaging.*;

// Create via Morphium factory (preferred)
MorphiumMessaging messaging = morphium.createMessaging();
// Or with overrides: morphium.createMessaging(cfg.messagingSettings())
messaging.start();

// Listen to a topic
messaging.addListenerForTopic("user.created", (mm, m) -> {
  System.out.println("Got: " + m.getMsg());
  return null; // no answer
});

// Send a message
Msg msg = new Msg("user.created", "User alice created", "userId:123");
messaging.sendMessage(msg);
```

Request/Response
```java
// Listener answering a request
messaging.addListenerForTopic("user.lookup", (mm, m) -> {
  var response = new Msg(m.getTopic(), "ok", "");
  response.setMapValue(Map.of("userId", m.getValue()));
  return response;
});

// Sender waiting for first answer
Msg req = new Msg("user.lookup", "find", "user123");
Msg resp = messaging.sendAndAwaitFirstAnswer(req, 5000);
```

Configuration (via `MessagingSettings`)

- Queue name: `setMessageQueueName(String)`: collection suffix used for the queue.
- Window size: `setMessagingWindowSize(int)`: number of messages processed per batch. Messaging marks up to this many messages and processes them as one window.
- Multithreading: `setMessagingMultithreadded(boolean)`: process multiple messages in parallel using (virtual) threads; `false` enforces single‑threaded, sequential handling.
- Change streams: `setUseChangeStream(boolean)`: use MongoDB Change Streams to get push‑style notifications for new messages; when `false`, messaging uses polling. Requires a replica set for Change Streams.
- Poll pause: `setMessagingPollPause(int)`: pause (in ms) between polling requests when not using Change Streams. Also used as a heartbeat to check for messages outside the current processing window (e.g., if new messages arrive and the queue holds more than `windowSize`, a poll is triggered once after this pause).

Example
```java
var ms = new MessagingSettings();
ms.setMessageQueueName("default");
ms.setMessagingWindowSize(100);
ms.setMessagingMultithreadded(true);
ms.setUseChangeStream(true);
ms.setMessagingPollPause(250);

MorphiumMessaging mq = morphium.createMessaging(ms);
mq.start();
```

Examples and behavior

- Sequential processing: `multithreadded=false`, `windowSize=1` → exactly one message is processed at a time, in order.
- Batched parallelism: `multithreadded=true`, `windowSize=100` → up to 100 messages are fetched and processed concurrently per window.

Notes

- When Change Streams are disabled, polling respects `messagingPollPause` to reduce load but still peeks for messages beyond the current window so bursts are noticed promptly.


## Benefits & Trade‑offs

Benefits

- Persistent queue: messages are stored in MongoDB by default (durability across restarts); use in‑memory storage only when persistence is not needed.
- Queryable messages: run ad‑hoc queries for statistics, audits, or status checks without interfering with processing.
- Change streams: combine with MongoDB change streams to react to new messages transparently (no polling; requires replica set).
- No extra infrastructure: reuse your existing MongoDB setup—no separate broker or runtime dependency when you already operate a replica set.

Trade‑offs

- Throughput: slower than purpose‑built brokers; every message is a document write/read.
- Load: very high message rates will add notable database load—plan capacity accordingly or choose a different transport when ultra‑high throughput is critical.

## V6.0 Improvements

### Change Stream Reliability
Morphium 6.0 significantly improved change stream handling in messaging:

**No More Re-reads**
- v5: messaging layer re-read documents after change stream events
- v6: uses `evt.getFullDocument()` directly from change stream snapshots
- More efficient, no dirty reads, no race conditions

**Document Snapshots**
```java
// Change stream events now contain immutable snapshots
// Messages are processed from the exact state at insert time
messaging.addListenerForTopic("events", (m, msg) -> {
    // msg is from immutable snapshot, safe to process
    // No concurrent modifications possible
    return null;
});
```

### InMemoryDriver Support
Full messaging support with InMemoryDriver for testing:

```java
MorphiumConfig cfg = new MorphiumConfig();
cfg.driverSettings().setDriverName("InMemDriver");
cfg.connectionSettings().setDatabase("testdb");

try (Morphium morphium = new Morphium(cfg)) {
    MorphiumMessaging sender = morphium.createMessaging();
    MorphiumMessaging receiver = morphium.createMessaging();

    receiver.addListenerForTopic("test", (m, msg) -> {
        // Process message
        return null;
    });

    sender.start();
    receiver.start();

    sender.sendMessage(new Msg("test", "Hello", "World", 30000));
}
```

**Multi-Instance Support**
```java
// Multiple Morphium instances sharing same database
Morphium m1 = new Morphium(cfg);
Morphium m2 = new Morphium(cfg);

MorphiumMessaging msg1 = m1.createMessaging();
MorphiumMessaging msg2 = m2.createMessaging();

// Both share the same InMemoryDriver
// Change streams work correctly
// Exclusive messages properly distributed
// Broadcast messages delivered to all
```

### Virtual Threads
Java 21 virtual threads for lightweight concurrency:
- Change stream callbacks run on virtual threads
- Each change stream watcher has its own virtual thread executor
- Minimal memory overhead for thousands of concurrent listeners

## Built-in Status Monitoring

**Every Morphium messaging instance automatically includes a status listener** that responds to queries on the `morphium_status` topic. This provides comprehensive monitoring without any configuration.

### Status Query Levels

Send a message to the `morphium_status` topic (or use `messaging.getStatusInfoListenerName()`) with one of these levels:

- **`PING`** - Simple health check (empty response, minimal overhead)
- **`MESSAGING_ONLY`** - Messaging system stats only (default)
- **`MORPHIUM_ONLY`** - Morphium cache and driver stats only
- **`ALL`** - Complete system information (JVM, messaging, Morphium, driver)

### Basic Example

```java
MorphiumMessaging sender = morphium.createMessaging();
sender.start();

// Query all instances for status
List<Msg> responses = sender.sendAndAwaitAnswers(
    new Msg(sender.getStatusInfoListenerName(), "status", "ALL"),
    10,     // Wait for up to 10 responses
    2000    // 2 second timeout
);

// Process responses
for (Msg response : responses) {
    Map<String, Object> stats = response.getMapValue();

    System.out.println("=== Instance: " + response.getSender() + " ===");

    // JVM metrics
    System.out.println("JVM Version: " + stats.get("jvm.version"));
    System.out.println("Heap Used: " + stats.get("jvm.heap.used") + " bytes");
    System.out.println("Active Threads: " + stats.get("jvm.threads.active"));

    // Messaging metrics
    System.out.println("Messages Processing: " + stats.get("messaging.processing"));
    System.out.println("Messages In Progress: " + stats.get("messaging.in_progress"));
    System.out.println("Waiting for Answers: " + stats.get("messaging.waiting_for_answers"));

    // Driver metrics
    System.out.println("Driver Stats: " + stats.get("morphium.driver.stats"));
    System.out.println("Connections: " + stats.get("morphium.driver.connections"));
}
```

### Available Metrics

**JVM Metrics (all levels except PING):**
- `jvm.version` - Java version
- `jvm.free_mem`, `jvm.total_mem`, `jvm.max_mem` - Memory stats
- `jvm.heap.init`, `jvm.heap.used`, `jvm.heap.committed`, `jvm.heap.max` - Heap memory
- `jvm.nonheap.init`, `jvm.nonheap.used`, `jvm.nonheap.committed`, `jvm.nonheap.max` - Non-heap memory
- `jvm.threads.active`, `jvm.threads.deamons`, `jvm.threads.peak`, `jvm.threads.total_started` - Thread stats

**Messaging Metrics (ALL or MESSAGING_ONLY):**
- `messaging.multithreadded` - Thread pool enabled
- `messaging.threadpoolstats` - Thread pool statistics
- `message_listeners_by_name` - Registered listeners
- `messaging.changestream` - Change stream enabled
- `messaging.window_size` - Message window size
- `messaging.pause` - Poll pause duration
- `messaging.processing` - Currently processing messages
- `messaging.in_progress` - Messages in progress
- `messaging.waiting_for_answers` - Pending responses count
- `messaging.waiting_for_answers_total` - Total waiting for answers
- `messaging.time_till_recieved` - Message transit time (ms)

**Morphium/Driver Metrics (ALL or MORPHIUM_ONLY):**
- `morphium.cachestats` - Cache statistics
- `morphium.config` - Configuration properties
- `morphium.driver.stats` - Driver statistics
- `morphium.driver.connections` - Connection count per host
- `morphium.driver.replicaset_status` - Replica set status

### Enable/Disable Status Listener

The status listener is enabled by default but can be controlled:

```java
MorphiumMessaging messaging = morphium.createMessaging();
messaging.start();

// Disable status responses (for security or performance)
messaging.disableStatusInfoListener();

// Re-enable status responses
messaging.enableStatusInfoListener();
```

### Health Checks and Monitoring

**Simple Health Check (PING):**
```java
// Just check if instances are alive
List<Msg> responses = sender.sendAndAwaitAnswers(
    new Msg(sender.getStatusInfoListenerName(), "status", "PING"),
    5,      // Expected instances
    1000    // Quick timeout
);

if (responses.size() >= 5) {
    System.out.println("All instances healthy");
} else {
    System.out.println("WARNING: Only " + responses.size() + " instances responded");
}
```

**Periodic Monitoring:**
```java
// Monitor every 30 seconds
ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
scheduler.scheduleAtFixedRate(() -> {
    try {
        List<Msg> responses = sender.sendAndAwaitAnswers(
            new Msg(sender.getStatusInfoListenerName(), "status", "MESSAGING_ONLY"),
            10, 2000
        );

        for (Msg r : responses) {
            Map<String, Object> stats = r.getMapValue();
            int inProgress = (Integer) stats.get("messaging.in_progress");

            if (inProgress > 1000) {
                System.out.println("WARNING: " + r.getSender() +
                                 " has " + inProgress + " messages in progress");
            }
        }
    } catch (Exception e) {
        System.err.println("Monitoring failed: " + e.getMessage());
    }
}, 0, 30, TimeUnit.SECONDS);
```

### Cross-Language Monitoring

Status queries work from any MongoDB client since it's just messaging:

**From Python:**
```python
from pymongo import MongoClient
import time

client = MongoClient('mongodb://localhost:27017/')
collection = client.morphium_messaging.msg

# Send status query
timestamp = int(time.time() * 1000)
status_msg = {
    'name': 'morphium_status',
    'msg': 'status',
    'value': 'ALL',
    'timestamp': timestamp,
    'sender': 'python-monitor',
    'msg_id': f'status-{timestamp}'
}
collection.insert_one(status_msg)

# Wait and collect responses
time.sleep(1)
answers = collection.find({
    'in_answer_to': status_msg['msg_id'],
    'timestamp': {'$gt': timestamp}
})

for answer in answers:
    stats = answer.get('map_value', {})
    print(f"Instance {answer['sender']}:")
    print(f"  Heap: {stats.get('jvm.heap.used')} / {stats.get('jvm.heap.max')}")
    print(f"  Processing: {stats.get('messaging.processing')}")
```

### Security Considerations

- **Disable in production** if exposing sensitive JVM/configuration data is a concern
- Status responses include configuration properties which may contain sensitive information
- Consider using `PING` or `MESSAGING_ONLY` instead of `ALL` to limit exposed data
- Status listener respects the same security/authentication as other messaging

See also

- [In‑Memory Driver](./howtos/inmemory-driver.md)
- [Messaging Implementations](./howtos/messaging-implementations.md)
- [Migration Guide v5 → v6](./howtos/migration-v5-to-v6.md)


Notes and best practices

- No wildcard/global listeners: register explicit topics via `addListenerForTopic(topic, listener)`
- Non‑exclusive messages are broadcast to all listeners of a topic
- For delayed/scheduled handling, add your own not‑before timestamp field and have the listener re‑queue or skip until due; `Msg.timestamp` is used for ordering, not scheduling
- For retries and DLQ, implement logic in listeners (inspect payload, track retry count, re‑queue or redirect to a DLQ topic)
- For distributed cache synchronization, see [Caching Examples](./howtos/caching-examples.md) and [Cache Patterns](./howtos/cache-patterns.md); Morphium provides `MessagingCacheSynchronizer`.
