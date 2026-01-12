package de.caluga.morphium.server.messaging;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Information about a registered messaging collection.
 * Tracks subscribers and their sender IDs for server-side filtering.
 */
public class MessagingCollectionInfo {
    private final String db;
    private final String collection;
    private final String lockCollection;
    private final Set<String> subscriberIds = ConcurrentHashMap.newKeySet();
    private final long registeredAt;

    public MessagingCollectionInfo(String db, String collection, String lockCollection) {
        this.db = db;
        this.collection = collection;
        this.lockCollection = lockCollection;
        this.registeredAt = System.currentTimeMillis();
    }

    public String getDb() {
        return db;
    }

    public String getCollection() {
        return collection;
    }

    public String getLockCollection() {
        return lockCollection;
    }

    public Set<String> getSubscriberIds() {
        return subscriberIds;
    }

    public void addSubscriber(String senderId) {
        subscriberIds.add(senderId);
    }

    public void removeSubscriber(String senderId) {
        subscriberIds.remove(senderId);
    }

    public boolean hasSubscriber(String senderId) {
        return subscriberIds.contains(senderId);
    }

    public long getRegisteredAt() {
        return registeredAt;
    }

    public String getNamespaceKey() {
        return db + "." + collection;
    }

    public String getLockNamespaceKey() {
        return db + "." + lockCollection;
    }

    @Override
    public String toString() {
        return "MessagingCollectionInfo{" +
                "db='" + db + '\'' +
                ", collection='" + collection + '\'' +
                ", lockCollection='" + lockCollection + '\'' +
                ", subscribers=" + subscriberIds.size() +
                '}';
    }
}
