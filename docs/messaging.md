# Messaging

Morphium provides a MongoDB‑backed message queue with topic‑based listeners.

Concepts
- Topic: string category for messages (e.g., `user.created`)
- Exclusive vs non‑exclusive:
  - Exclusive (`Msg.setExclusive(true)`): exactly one listener processes the message (one‑of‑n); implemented using a lock collection
  - Non‑exclusive (default): every registered listener for the topic processes the message (broadcast)
- Answers: listeners may return a `Msg` as response; senders can wait synchronously or asynchronously

Setup
```java
import de.caluga.morphium.messaging.*;

// Construct and initialize
StdMessaging messaging = new StdMessaging();
messaging.init(morphium);            // or messaging.init(morphium, cfg.messagingSettings())
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
- Queue name: `setMessageQueueName(String)`
- Window size (IDs marked per fetch): `setMessagingWindowSize(int)`
- Multithreading: `setMessagingMultithreadded(boolean)`
- Change streams: `setUseChangeStream(boolean)` (requires replica set)
- Poll pause (ms, when polling): `setMessagingPollPause(int)`

Example
```java
var ms = new MessagingSettings();
ms.setMessageQueueName("default");
ms.setMessagingWindowSize(100);
ms.setMessagingMultithreadded(true);
ms.setUseChangeStream(true);
ms.setMessagingPollPause(250);

StdMessaging mq = new StdMessaging();
mq.init(morphium, ms);
mq.start();
```


## Benefits & Trade‑offs

Benefits
- Persistent queue: messages are stored in MongoDB by default (durability across restarts); use in‑memory storage only when persistence is not needed.
- Queryable messages: run ad‑hoc queries for statistics, audits, or status checks without interfering with processing.
- Change streams: combine with MongoDB change streams to react to new messages transparently (no polling; requires replica set).
- No extra infrastructure: reuse your existing MongoDB setup—no separate broker or runtime dependency when you already operate a replica set.

Trade‑offs
- Throughput: slower than purpose‑built brokers; every message is a document write/read.
- Load: very high message rates will add notable database load—plan capacity accordingly or choose a different transport when ultra‑high throughput is critical.

See also
- In‑memory driver: ./howtos/inmemory-driver.md


Notes and best practices
- No wildcard/global listeners: register explicit topics via `addListenerForTopic(topic, listener)`
- Non‑exclusive messages are broadcast to all listeners of a topic
- For delayed/scheduled handling, add your own not‑before timestamp field and have the listener re‑queue or skip until due; `Msg.timestamp` is used for ordering, not scheduling
- For retries and DLQ, implement logic in listeners (inspect payload, track retry count, re‑queue or redirect to a DLQ topic)
- For distributed cache synchronization, see Caching Examples and Cache Patterns; Morphium provides `MessagingCacheSynchronizer`
