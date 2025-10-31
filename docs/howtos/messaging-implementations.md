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

