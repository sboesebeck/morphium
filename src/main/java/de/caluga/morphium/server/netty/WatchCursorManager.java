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

        log.info("Created watch cursor {} for {}.{}", cursorId, wcmd.getDb(), wcmd.getColl());

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
            log.info("Starting watch for cursor {}", cursorId);
            driver.runCommand(wcmd);
            log.info("Watch started for cursor {}", cursorId);
        } catch (Exception e) {
            log.error("Watch command error for cursor {}", cursorId, e);
            watchCursors.remove(cursorId);
        }

        return cursorId;
    }

    /**
     * Called when a watch event arrives. Notifies any pending getMore requests.
     */
    private void onWatchEvent(long cursorId, Map<String, Object> event) {
        WatchCursorState state = watchCursors.get(cursorId);
        if (state == null) {
            return;
        }

        state.events.offer(event);

        // Complete any pending getMore request
        PendingGetMore pending;
        while ((pending = state.pendingGetMores.poll()) != null) {
            List<Map<String, Object>> batch = drainEvents(state.events);
            pending.future.complete(batch);
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
        // Check if there are already events available
        if (!state.events.isEmpty()) {
            List<Map<String, Object>> batch = drainEvents(state.events);
            return CompletableFuture.completedFuture(batch);
        }

        // If not running, return empty immediately
        if (!running) {
            return CompletableFuture.completedFuture(Collections.emptyList());
        }

        // No events available - set up async wait
        CompletableFuture<List<Map<String, Object>>> future = new CompletableFuture<>();
        PendingGetMore pending = new PendingGetMore(future);
        state.pendingGetMores.offer(pending);

        // Schedule timeout - handle rejected execution during shutdown
        try {
            scheduler.schedule(() -> {
                if (state.pendingGetMores.remove(pending)) {
                    // Timeout - return whatever events are available (may be empty)
                    List<Map<String, Object>> batch = drainEvents(state.events);
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
        log.info("Created tailable cursor {} for {}.{}", cursorId, db, collection);
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
        log.info("Registered external tailable cursor {} for {}.{}", cursorId, db, collection);
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
