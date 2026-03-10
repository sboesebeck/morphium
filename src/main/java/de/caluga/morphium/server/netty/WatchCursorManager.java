package de.caluga.morphium.server.netty;

import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.caluga.morphium.driver.DriverTailableIterationCallback;
import de.caluga.morphium.driver.commands.WatchCommand;
import de.caluga.morphium.driver.inmem.InMemoryDriver;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Manages watch cursors (change streams) and tailable cursors asynchronously.
 *
 * Instead of blocking threads with queue.poll(timeout), this manager uses
 * CompletableFuture to handle getMore requests asynchronously. When an event
 * arrives, all pending getMore requests for that cursor are completed.
 */
public class WatchCursorManager {

    private static final Logger log = LoggerFactory.getLogger(WatchCursorManager.class);

    private final AtomicLong cursorIdGenerator = new AtomicLong(1000);
    private final ConcurrentMap<Long, WatchCursorState> watchCursors = new ConcurrentHashMap<>();
    private final ConcurrentMap<Long, TailableCursorState> tailableCursors = new ConcurrentHashMap<>();
    private final ScheduledExecutorService scheduler;
    private volatile boolean running = true;

    // Fast-path: Track cursors watching messaging collections
    // Key: "db.collection" -> Set of cursorIds
    private final ConcurrentMap<String, Set<Long>> messagingCursors = new ConcurrentHashMap<>();

    public WatchCursorManager() {
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "WatchCursorManager-Scheduler");
            t.setDaemon(true);
            return t;
        });
    }

    /**
     * Generate a new cursor ID.
     */
    public long nextCursorId() {
        return cursorIdGenerator.incrementAndGet();
    }

    /**
     * Create a new watch cursor for a change stream.
     */
    public long createWatchCursor(InMemoryDriver driver, WatchCommand wcmd) {
        long cursorId = nextCursorId();
        WatchCursorState state = new WatchCursorState(cursorId, wcmd.getDb(), wcmd.getColl());
        watchCursors.put(cursorId, state);

        log.debug("Created watch cursor {} for {}.{}", cursorId, wcmd.getDb(), wcmd.getColl());

        // Set up callback to queue events
        wcmd.setCb(new DriverTailableIterationCallback() {
            @Override
            public void incomingData(Map<String, Object> data, long dur) {
                log.debug("Watch callback: received event for cursor {} - data: {}", cursorId, data);
                onWatchEvent(cursorId, data);
            }

            @Override
            public boolean isContinued() {
                return running && watchCursors.containsKey(cursorId);
            }
        });

        // Start the watch in the driver - it will register the subscription and return immediately
        // The async loop in InMemoryDriver will handle calling the callback when events arrive
        try {
            log.debug("Starting watch for cursor {}", cursorId);
            driver.runCommand(wcmd);
            log.debug("Watch started for cursor {}", cursorId);
        } catch (Exception e) {
            log.error("Watch command error for cursor {}", cursorId, e);
            watchCursors.remove(cursorId);
        }

        return cursorId;
    }

    /**
     * Register a cursor as watching a messaging collection for fast-path delivery.
     *
     * @param cursorId The cursor ID
     * @param db Database name
     * @param collection Collection name
     * @param subscriberId The subscriber's sender ID (for server-side filtering)
     */
    public void registerMessagingCursor(long cursorId, String db, String collection, String subscriberId) {
        String key = db + "." + collection;
        messagingCursors.computeIfAbsent(key, k -> ConcurrentHashMap.newKeySet()).add(cursorId);

        // Store subscriberId in the cursor state for sender filtering
        WatchCursorState state = watchCursors.get(cursorId);
        if (state != null && subscriberId != null) {
            state.subscriberId = subscriberId;
        }

        log.debug("Registered messaging cursor {} for {} (subscriber: {})", cursorId, key, subscriberId);
    }

    /**
     * Unregister a messaging cursor.
     */
    public void unregisterMessagingCursor(long cursorId, String db, String collection) {
        String key = db + "." + collection;
        Set<Long> cursors = messagingCursors.get(key);
        if (cursors != null) {
            cursors.remove(cursorId);
            if (cursors.isEmpty()) {
                messagingCursors.remove(key);
            }
        }
    }

    /**
     * Fast-path: Directly notify messaging cursors about a new event.
     * This bypasses the normal change stream subscription mechanism for lower latency.
     *
     * @param db Database name
     * @param collection Collection name
     * @param event The change stream event
     * @param senderToExclude Sender ID to exclude (don't notify sender about their own messages)
     * @return Number of cursors notified
     */
    public int notifyMessagingEvent(String db, String collection, Map<String, Object> event, String senderToExclude) {
        String key = db + "." + collection;
        Set<Long> cursors = messagingCursors.get(key);
        if (cursors == null || cursors.isEmpty()) {
            return 0;
        }

        int notified = 0;
        for (Long cursorId : cursors) {
            WatchCursorState state = watchCursors.get(cursorId);
            if (state == null) {
                continue;
            }

            // Server-side sender filtering: don't notify the sender about their own message
            if (senderToExclude != null && state.subscriberId != null
                    && senderToExclude.equals(state.subscriberId)) {
                log.trace("Fast-path: skipping cursor {} (sender {} excluded)", cursorId, senderToExclude);
                continue;
            }

            // Directly deliver the event
            deliverEventToCursor(state, event);
            notified++;
        }

        if (notified > 0) {
            log.debug("Fast-path: notified {} messaging cursors for {}", notified, key);
        }
        return notified;
    }

    /**
     * Check if a collection has any messaging cursors waiting.
     */
    public boolean hasMessagingCursors(String db, String collection) {
        String key = db + "." + collection;
        Set<Long> cursors = messagingCursors.get(key);
        return cursors != null && !cursors.isEmpty();
    }

    /**
     * Direct event delivery to a cursor (fast-path).
     */
    private void deliverEventToCursor(WatchCursorState state, Map<String, Object> event) {
        state.events.offer(event);

        // Complete any pending getMore request immediately
        PendingGetMore pending;
        while ((pending = state.pendingGetMores.poll()) != null) {
            List<Map<String, Object>> batch = drainEvents(state.events);
            log.trace("Fast-path: completing pending getMore for cursor {} with {} events", state.cursorId, batch.size());
            pending.future.complete(batch);
        }
    }

    /**
     * Called when a watch event arrives. Notifies any pending getMore requests.
     */
    private void onWatchEvent(long cursorId, Map<String, Object> event) {
        log.trace("onWatchEvent: cursorId={}, event operationType={}", cursorId, event != null ? event.get("operationType") : "null");
        WatchCursorState state = watchCursors.get(cursorId);
        if (state == null) {
            log.warn("onWatchEvent: cursor {} not found, event will be lost!", cursorId);
            return;
        }

        state.events.offer(event);
        log.trace("onWatchEvent: queued event for cursor {}, queue size now: {}", cursorId, state.events.size());

        // Complete any pending getMore request
        PendingGetMore pending;
        int completedCount = 0;
        while ((pending = state.pendingGetMores.poll()) != null) {
            List<Map<String, Object>> batch = drainEvents(state.events);
            log.debug("onWatchEvent: completing pending getMore for cursor {} with {} events", cursorId, batch.size());
            pending.future.complete(batch);
            completedCount++;
        }
    }

    /**
     * Handle a getMore request asynchronously.
     *
     * @param cursorId The cursor ID
     * @param maxTimeMs Maximum time to wait in milliseconds
     * @return CompletableFuture that completes with the batch of events
     */
    public CompletableFuture<List<Map<String, Object>>> getMore(long cursorId, int maxTimeMs) {
        WatchCursorState state = watchCursors.get(cursorId);
        if (state != null) {
            return getMoreWatch(state, maxTimeMs);
        }

        TailableCursorState tailableState = tailableCursors.get(cursorId);
        if (tailableState != null) {
            return getMoreTailable(tailableState, maxTimeMs);
        }

        // Cursor not found
        CompletableFuture<List<Map<String, Object>>> future = new CompletableFuture<>();
        future.complete(Collections.emptyList());
        return future;
    }

    private CompletableFuture<List<Map<String, Object>>> getMoreWatch(WatchCursorState state, int maxTimeMs) {
        log.debug("getMoreWatch: cursorId={}, maxTimeMs={}, events in queue={}", state.cursorId, maxTimeMs, state.events.size());

        // Check if there are already events available
        if (!state.events.isEmpty()) {
            List<Map<String, Object>> batch = drainEvents(state.events);
            log.debug("getMoreWatch: returning {} existing events for cursor {}", batch.size(), state.cursorId);
            return CompletableFuture.completedFuture(batch);
        }

        // If not running, return empty immediately
        if (!running) {
            log.debug("getMoreWatch: not running, returning empty for cursor {}", state.cursorId);
            return CompletableFuture.completedFuture(Collections.emptyList());
        }

        // No events available - set up async wait
        log.debug("getMoreWatch: no events available, setting up async wait for cursor {} ({}ms)", state.cursorId, maxTimeMs);
        CompletableFuture<List<Map<String, Object>>> future = new CompletableFuture<>();
        PendingGetMore pending = new PendingGetMore(future);
        state.pendingGetMores.offer(pending);

        // CRITICAL: Re-check events after adding to pendingGetMores to avoid race condition
        // Events might have arrived between the initial isEmpty check and adding the pending
        if (!state.events.isEmpty()) {
            // Try to remove our pending entry and return events immediately
            if (state.pendingGetMores.remove(pending)) {
                List<Map<String, Object>> batch = drainEvents(state.events);
                log.debug("getMoreWatch: race avoided - found {} events after adding pending for cursor {}", batch.size(), state.cursorId);
                future.complete(batch);
                return future;
            }
            // If remove failed, onWatchEvent already completed our future - just return it
        }

        // Schedule timeout - handle rejected execution during shutdown
        try {
            scheduler.schedule(() -> {
                if (state.pendingGetMores.remove(pending)) {
                    // Timeout - return whatever events are available (may be empty)
                    List<Map<String, Object>> batch = drainEvents(state.events);
                    log.debug("getMoreWatch: timeout for cursor {}, returning {} events", state.cursorId, batch.size());
                    future.complete(batch);
                }
            }, maxTimeMs, TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException e) {
            // Scheduler is shutting down - complete immediately
            state.pendingGetMores.remove(pending);
            future.complete(Collections.emptyList());
        }

        return future;
    }

    private CompletableFuture<List<Map<String, Object>>> getMoreTailable(TailableCursorState state, int maxTimeMs) {
        // Check if there are already documents available
        if (!state.documents.isEmpty()) {
            List<Map<String, Object>> batch = drainEvents(state.documents);
            return CompletableFuture.completedFuture(batch);
        }

        // If not running, return empty immediately
        if (!running) {
            return CompletableFuture.completedFuture(Collections.emptyList());
        }

        // No documents available - set up async wait
        CompletableFuture<List<Map<String, Object>>> future = new CompletableFuture<>();
        PendingGetMore pending = new PendingGetMore(future);
        state.pendingGetMores.offer(pending);

        // CRITICAL: Re-check documents after adding to pendingGetMores to avoid race condition
        if (!state.documents.isEmpty()) {
            if (state.pendingGetMores.remove(pending)) {
                List<Map<String, Object>> batch = drainEvents(state.documents);
                future.complete(batch);
                return future;
            }
        }

        // Schedule timeout - handle rejected execution during shutdown
        try {
            scheduler.schedule(() -> {
                if (state.pendingGetMores.remove(pending)) {
                    List<Map<String, Object>> batch = drainEvents(state.documents);
                    future.complete(batch);
                }
            }, maxTimeMs, TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException e) {
            // Scheduler is shutting down - complete immediately
            state.pendingGetMores.remove(pending);
            future.complete(Collections.emptyList());
        }

        return future;
    }

    private List<Map<String, Object>> drainEvents(Queue<Map<String, Object>> queue) {
        List<Map<String, Object>> batch = new ArrayList<>();
        Map<String, Object> event;
        int count = 0;
        while ((event = queue.poll()) != null && count < 100) {
            batch.add(event);
            count++;
        }
        return batch;
    }

    /**
     * Check if a cursor exists (watch or tailable).
     */
    public boolean hasCursor(long cursorId) {
        return watchCursors.containsKey(cursorId) || tailableCursors.containsKey(cursorId);
    }

    /**
     * Kill a cursor.
     */
    public boolean killCursor(long cursorId) {
        WatchCursorState watchState = watchCursors.remove(cursorId);
        if (watchState != null) {
            // Complete any pending getMore requests with empty results
            PendingGetMore pending;
            while ((pending = watchState.pendingGetMores.poll()) != null) {
                pending.future.complete(Collections.emptyList());
            }
            // Clean up messaging cursor tracking
            unregisterMessagingCursor(cursorId, watchState.db, watchState.collection);
            log.debug("Killed watch cursor {}", cursorId);
            return true;
        }

        TailableCursorState tailableState = tailableCursors.remove(cursorId);
        if (tailableState != null) {
            tailableState.active = false;
            PendingGetMore pending;
            while ((pending = tailableState.pendingGetMores.poll()) != null) {
                pending.future.complete(Collections.emptyList());
            }
            log.debug("Killed tailable cursor {}", cursorId);
            return true;
        }

        return false;
    }

    /**
     * Create a tailable cursor for a capped collection.
     */
    public long createTailableCursor(String db, String collection, Map<String, Object> filter) {
        long cursorId = nextCursorId();
        TailableCursorState state = new TailableCursorState(cursorId, db, collection, filter);
        tailableCursors.put(cursorId, state);
        log.debug("Created tailable cursor {} for {}.{}", cursorId, db, collection);
        return cursorId;
    }

    /**
     * Register an external cursor ID as a tailable cursor.
     * Used when the driver creates the cursor but we need to track it for notifications.
     */
    public void registerTailableCursor(long cursorId, String db, String collection, Map<String, Object> filter) {
        if (tailableCursors.containsKey(cursorId)) {
            return; // Already registered
        }
        TailableCursorState state = new TailableCursorState(cursorId, db, collection, filter);
        tailableCursors.put(cursorId, state);
        log.debug("Registered external tailable cursor {} for {}.{}", cursorId, db, collection);
    }

    /**
     * Add a document to a tailable cursor (called when new documents are inserted).
     */
    public void onTailableDocument(long cursorId, Map<String, Object> document) {
        TailableCursorState state = tailableCursors.get(cursorId);
        if (state == null || !state.active) {
            return;
        }

        state.documents.offer(document);

        // Complete any pending getMore request
        PendingGetMore pending;
        while ((pending = state.pendingGetMores.poll()) != null) {
            List<Map<String, Object>> batch = drainEvents(state.documents);
            pending.future.complete(batch);
        }
    }

    /**
     * Notify all tailable cursors watching a specific collection about new documents.
     * Called when documents are inserted into a capped collection.
     */
    public void notifyTailableCursors(String db, String collection, List<Map<String, Object>> documents) {
        for (TailableCursorState state : tailableCursors.values()) {
            if (!state.active) {
                continue;
            }
            if (!db.equals(state.db) || !collection.equals(state.collection)) {
                continue;
            }

            // Add all documents to the cursor's queue
            for (Map<String, Object> doc : documents) {
                // Check if document matches the cursor's filter (if any)
                if (state.filter == null || state.filter.isEmpty() || matchesFilter(doc, state.filter)) {
                    state.documents.offer(doc);
                }
            }

            // Complete any pending getMore requests
            PendingGetMore pending;
            while ((pending = state.pendingGetMores.poll()) != null) {
                List<Map<String, Object>> batch = drainEvents(state.documents);
                pending.future.complete(batch);
            }
        }
    }

    /**
     * Simple filter matching for tailable cursor queries.
     */
    @SuppressWarnings("unchecked")
    private boolean matchesFilter(Map<String, Object> doc, Map<String, Object> filter) {
        if (filter == null || filter.isEmpty()) {
            return true;
        }
        for (Map.Entry<String, Object> entry : filter.entrySet()) {
            String key = entry.getKey();
            Object filterValue = entry.getValue();
            Object docValue = doc.get(key);

            if (filterValue == null) {
                if (docValue != null) return false;
            } else if (!filterValue.equals(docValue)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Shutdown the cursor manager.
     */
    public void shutdown() {
        running = false;

        // Kill all cursors
        for (Long cursorId : new ArrayList<>(watchCursors.keySet())) {
            killCursor(cursorId);
        }
        for (Long cursorId : new ArrayList<>(tailableCursors.keySet())) {
            killCursor(cursorId);
        }

        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Get statistics for monitoring.
     */
    public Map<String, Object> getStats() {
        return Map.of(
            "watchCursors", watchCursors.size(),
            "tailableCursors", tailableCursors.size()
        );
    }

    // Internal state classes

    private static class WatchCursorState {
        final long cursorId;
        final String db;
        final String collection;
        final Queue<Map<String, Object>> events = new ConcurrentLinkedQueue<>();
        final Queue<PendingGetMore> pendingGetMores = new ConcurrentLinkedQueue<>();
        // For messaging fast-path: subscriber ID for sender filtering
        volatile String subscriberId;

        WatchCursorState(long cursorId, String db, String collection) {
            this.cursorId = cursorId;
            this.db = db;
            this.collection = collection;
        }
    }

    private static class TailableCursorState {
        final long cursorId;
        final String db;
        final String collection;
        final Map<String, Object> filter;
        final Queue<Map<String, Object>> documents = new ConcurrentLinkedQueue<>();
        final Queue<PendingGetMore> pendingGetMores = new ConcurrentLinkedQueue<>();
        volatile boolean active = true;

        TailableCursorState(long cursorId, String db, String collection, Map<String, Object> filter) {
            this.cursorId = cursorId;
            this.db = db;
            this.collection = collection;
            this.filter = filter;
        }
    }

    private static class PendingGetMore {
        final CompletableFuture<List<Map<String, Object>>> future;

        PendingGetMore(CompletableFuture<List<Map<String, Object>>> future) {
            this.future = future;
        }
    }
}
