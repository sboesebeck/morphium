# Messaging Implementations

Morphium provides two messaging implementations that share the same API (`MorphiumMessaging`) but differ in storage layout and scalability characteristics.

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

Both support:
- Change Streams vs polling (`useChangeStream`), window size, multithreading, answers, exclusive vs broadcast.

## Choosing an Implementation

- Start with Standard. If you have many topics, high listener fan‑out, or change streams getting noisy, move to Advanced.
- Names used for selection:
  - `StandardMessaging` → `StdMessaging`
  - `AdvMessaging` → `AdvancedSplitCollectionMessaging`

## Configuration and Usage

Always instantiate via `Morphium.createMessaging()` so configuration and implementation selection are handled correctly.

```java
// Configure implementation by name
cfg.messagingSettings().setMessagingImplementation("AdvMessaging"); // or "StandardMessaging" (default)

Morphium morphium = new Morphium(cfg);
MorphiumMessaging messaging = morphium.createMessaging();
messaging.start();

messaging.addListenerForTopic("orders.created", (mm, msg) -> {
  // ...
  return null;
});
```

Relevant settings are the same for both implementations (see the Messaging page):
- `messageQueueName`, `messagingWindowSize`, `messagingMultithreadded`, `useChangeStream`, `messagingPollPause`.

## Storage Layout Details

- Standard
  - Queue: `<queue>` (default `msg`)
  - Locks: `<queue>_lck`
  - Direct messages: `<queue>`

- Advanced
  - Topic queue: `<queue>_<topic>`
  - Locks: `<queue>_lck_<topic>`
  - Direct messages: `dm_<recipientSenderIdCamelCase>`

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

## Cache Synchronization

Both implementations work with `MessagingCacheSynchronizer`. No code changes needed—only the implementation selection via config.

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

