package de.caluga.morphium.server.messaging;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import de.caluga.morphium.server.netty.WatchCursorManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Optimizes MorphiumServer for messaging workloads.
 *
 * When a messaging collection is registered, the server can:
 * - Create optimized indexes for messaging queries
 * - Use fast-path change stream notifications
 * - Filter by sender server-side (reduce network traffic)
 * - Process index updates asynchronously
 */
public class MessagingOptimizer {
    private static final Logger log = LoggerFactory.getLogger(MessagingOptimizer.class);

    // Key: db.collection -> MessagingCollectionInfo
    private final ConcurrentHashMap<String, MessagingCollectionInfo> messagingCollections = new ConcurrentHashMap<>();

    // Key: db.lockCollection -> parent messaging collection key
    private final ConcurrentHashMap<String, String> lockCollectionMapping = new ConcurrentHashMap<>();

    // Standard indexes for messaging - field name -> direction (1 or -1)
    public static final List<Map<String, Object>> MESSAGING_INDEXES = List.of(
                        Doc.of("key", Doc.of("timestamp", 1), "name", "msg_timestamp_1"),
                        Doc.of("key", Doc.of("sender", 1), "name", "msg_sender_1"),
                        Doc.of("key", Doc.of("locked_by", 1, "locked", 1), "name", "msg_locked_by_1_locked_1"),
                        Doc.of("key", Doc.of("processed_by", 1), "name", "msg_processed_by_1")
        );

    private final InMemoryDriver driver;
    private WatchCursorManager cursorManager;

    public MessagingOptimizer(InMemoryDriver driver) {
        this.driver = driver;
    }

    /**
     * Set the WatchCursorManager for fast-path notifications.
     * Called after both MessagingOptimizer and WatchCursorManager are created.
     */
    public void setWatchCursorManager(WatchCursorManager cursorManager) {
        this.cursorManager = cursorManager;
    }

    /**
     * Register a collection as a messaging collection.
     * This enables optimizations for that collection.
     *
     * @param db Database name
     * @param collection Message collection name
     * @param lockCollection Lock collection name
     * @param senderId The subscriber's sender ID
     * @return Registration result
     */
    public Map<String, Object> registerMessagingCollection(String db, String collection,
            String lockCollection, String senderId) {
        String key = db + "." + collection;

        MessagingCollectionInfo info = messagingCollections.computeIfAbsent(key,
        k -> {
            log.info("Registering new messaging collection: {}", key);
            MessagingCollectionInfo newInfo = new MessagingCollectionInfo(db, collection, lockCollection);

            // Map lock collection to parent
            if (lockCollection != null && !lockCollection.isEmpty()) {
                lockCollectionMapping.put(db + "." + lockCollection, key);
            }

            // Create optimized indexes asynchronously
            createMessagingIndexesAsync(db, collection);

            return newInfo;
        });

        // Add subscriber
        if (senderId != null && !senderId.isEmpty()) {
            info.addSubscriber(senderId);
            log.debug("Added subscriber {} to messaging collection {}", senderId, key);
        }

        return Doc.of(
                               "ok", 1.0,
                               "registered", true,
                               "collection", key,
                               "lockCollection", info.getLockNamespaceKey(),
                               "subscriberCount", info.getSubscriberIds().size(),
                               "optimizations", List.of("fastChangeStream", "serverSideFiltering", "asyncIndexUpdates")
               );
    }

    /**
     * Unregister a subscriber from a messaging collection.
     */
    public Map<String, Object> unregisterMessagingSubscriber(String db, String collection, String senderId) {
        String key = db + "." + collection;
        MessagingCollectionInfo info = messagingCollections.get(key);

        if (info == null) {
            return Doc.of("ok", 0.0, "errmsg", "Collection not registered as messaging: " + key);
        }

        info.removeSubscriber(senderId);
        log.debug("Removed subscriber {} from messaging collection {}", senderId, key);

        // If no more subscribers, we could clean up, but keep the registration
        // in case the collection is still used for messaging

        return Doc.of(
                               "ok", 1.0,
                               "unregistered", true,
                               "collection", key,
                               "remainingSubscribers", info.getSubscriberIds().size()
               );
    }

    /**
     * Check if a collection is registered as a messaging collection.
     */
    public boolean isMessagingCollection(String db, String collection) {
        return messagingCollections.containsKey(db + "." + collection);
    }

    /**
     * Check if a collection is a lock collection for messaging.
     */
    public boolean isLockCollection(String db, String collection) {
        return lockCollectionMapping.containsKey(db + "." + collection);
    }

    /**
     * Get messaging collection info.
     */
    public MessagingCollectionInfo getMessagingInfo(String db, String collection) {
        return messagingCollections.get(db + "." + collection);
    }

    /**
     * Get all subscriber IDs for a messaging collection.
     * Used for server-side sender filtering.
     */
    public Set<String> getSubscribers(String db, String collection) {
        MessagingCollectionInfo info = messagingCollections.get(db + "." + collection);
        return info != null ? info.getSubscriberIds() : Set.of();
    }

    /**
     * Check if server-side sender filtering should exclude this message
     * from being sent to a specific subscriber.
     */
    public boolean shouldFilterForSubscriber(String db, String collection,
            String messageSender, String subscriberId) {
        // Don't send messages to their own sender
        return messageSender != null && messageSender.equals(subscriberId);
    }

    /**
     * Fast-path: Notify messaging cursors about a new message insert.
     * This bypasses the normal change stream subscription mechanism for lower latency.
     *
     * @param db Database name
     * @param collection Collection name
     * @param document The inserted document
     * @return true if fast-path was used, false otherwise
     */
    @SuppressWarnings("unchecked")
    public boolean notifyMessageInsert(String db, String collection, Map<String, Object> document) {
        if (cursorManager == null) {
            return false;
        }

        if (!isMessagingCollection(db, collection)) {
            return false;
        }

        // Check if there are any cursors waiting
        if (!cursorManager.hasMessagingCursors(db, collection)) {
            return false;
        }

        // Extract sender from the document for server-side filtering
        String sender = (String) document.get("sender");

        // Build a minimal change stream event for the insert
        Map<String, Object> event = Doc.of(
                "operationType", "insert",
                "fullDocument", document,
                "ns", Doc.of("db", db, "coll", collection),
                "documentKey", Doc.of("_id", document.get("_id"))
                                    );

        // Use fast-path notification
        int notified = cursorManager.notifyMessagingEvent(db, collection, event, sender);

        if (notified > 0) {
            log.debug("Fast-path: notified {} cursors for message insert in {}.{}", notified, db, collection);
            return true;
        }

        return false;
    }

    /**
     * Register a cursor as a messaging cursor for fast-path delivery.
     */
    public void registerMessagingCursor(long cursorId, String db, String collection, String subscriberId) {
        if (cursorManager != null && isMessagingCollection(db, collection)) {
            cursorManager.registerMessagingCursor(cursorId, db, collection, subscriberId);
        }
    }

    /**
     * Create messaging indexes asynchronously.
     */
    private void createMessagingIndexesAsync(String db, String collection) {
        // Run index creation in background to not block the registration
        Thread.ofPlatform().name("msg-index-" + collection).start(() -> {
            try {
                log.debug("Creating messaging indexes for {}.{}", db, collection);
                for (Map<String, Object> indexDef : MESSAGING_INDEXES) {
                    try {
                        @SuppressWarnings("unchecked")
                        Map<String, Object> key = (Map<String, Object>) indexDef.get("key");
                        String indexName = (String) indexDef.get("name");
                        driver.createIndex(db, collection, key, Doc.of("name", indexName));
                    } catch (Exception e) {
                        // Index might already exist, that's fine
                        log.trace("Index creation note for {}.{}: {}", db, collection, e.getMessage());
                    }
                }
                log.debug("Messaging indexes created for {}.{}", db, collection);
            } catch (Exception e) {
                log.warn("Error creating messaging indexes for {}.{}: {}", db, collection, e.getMessage());
            }
        });
    }

    /**
     * Get statistics about registered messaging collections.
     */
    public Map<String, Object> getStats() {
        return Doc.of(
                               "registeredCollections", messagingCollections.size(),
                               "lockCollections", lockCollectionMapping.size(),
                               "collections", messagingCollections.entrySet().stream()
                               .map(e -> Doc.of(
                                       "namespace", e.getKey(),
                                       "subscribers", e.getValue().getSubscriberIds().size(),
                                       "registeredAt", e.getValue().getRegisteredAt()
                                    ))
                               .toList()
               );
    }

    /**
     * Shutdown and cleanup.
     */
    public void shutdown() {
        log.info("Shutting down MessagingOptimizer with {} registered collections",
                 messagingCollections.size());
        messagingCollections.clear();
        lockCollectionMapping.clear();
    }
}
