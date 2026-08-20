package de.caluga.poppydb.netty;

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

    // Per-cursor buffer cap. A change stream / tailable cursor whose client stops reading
    // must not accumulate events without bound. On overflow the cursor is killed with a
    // CursorKilled-style error rather than risking OOM.
    static final int MAX_CURSOR_QUEUE_SIZE = 10_000;

    // #321: per-cursor byte budget on top of the count cap. The count cap alone lets a slow
    // consumer of ~300KB documents pin ~3GB (10,000 x 300KB) on the primary - each queued event
    // shares its fullDocument payload with the replay-buffer entry, so replay-buffer byte
    // eviction frees nothing while the cursor still references it. 0 disables the byte bound.
    private volatile long cursorQueueByteBudget = DEFAULT_CURSOR_QUEUE_BYTE_BUDGET;
    static final long DEFAULT_CURSOR_QUEUE_BYTE_BUDGET = 64L * 1024 * 1024;

    // #321/#322: injectable count cap - MAX_CURSOR_QUEUE_SIZE stays the default, but tests (and
    // the two-node scenarios of #322) need to shrink it without buffering 10,000 real events.
    private volatile int maxCursorQueueSize = MAX_CURSOR_QUEUE_SIZE;

    /** Per-cursor byte budget for buffered, undelivered events; 0 disables the byte bound. */
    public void setCursorQueueByteBudget(long bytes) {
        if (bytes < 0) {
            throw new IllegalArgumentException("cursorQueueByteBudget must be >= 0 (0 = disabled)");
        }

        this.cursorQueueByteBudget = bytes;
    }

    public long getCursorQueueByteBudget() {
        return cursorQueueByteBudget;
    }

    /** Count cap for newly created cursors' event queues (existing cursors keep theirs). */
    void setMaxCursorQueueSizeForTest(int maxEvents) {
        if (maxEvents < 1) {
            throw new IllegalArgumentException("maxCursorQueueSize must be >= 1");
        }

        this.maxCursorQueueSize = maxEvents;
    }

    /** Estimated bytes a watch cursor currently holds undelivered; -1 if it no longer exists. */
    long queuedByteCount(long cursorId) {
        WatchCursorState state = watchCursors.get(cursorId);
        return state == null ? -1 : state.queuedBytes.get();
    }

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
     *
     * @return the id of a cursor that is registered and live
     * @throws IllegalStateException if the change stream could not be started - deliberately
     *         unchecked and deliberately NOT swallowed: a caller must not hand a cursor id to a
     *         client for a stream that does not exist. The caller answers this as a failed
     *         command (see {@code MongoCommandHandler.processChangeStream}), which is also where
     *         it is logged - this method does not log it a second time.
     */
    public long createWatchCursor(InMemoryDriver driver, WatchCommand wcmd) {
        long cursorId = nextCursorId();
        WatchCursorState state = new WatchCursorState(cursorId, wcmd.getDb(), wcmd.getColl(), maxCursorQueueSize);
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

        state.wcmd = wcmd;
        // Woken the moment the watch dies, not on the next poll: a parked getMore would
        // otherwise sit out its maxTimeMS and answer empty - a successful-looking reply for a
        // stream already known to be unservable.
        wcmd.setOnTerminalError(reason -> failUnservable(cursorId, reason));

        // Start the watch in the driver - it will register the subscription and return immediately
        // The async loop in InMemoryDriver will handle calling the callback when events arrive
        try {
            log.debug("Starting watch for cursor {}", cursorId);
            driver.runCommand(wcmd);
            log.debug("Watch started for cursor {}", cursorId);
        } catch (Exception e) {
            // Do NOT hand back a cursor id for a stream that never started: the client would be
            // answered ok:1 with a live-looking cursor, miss every event, and only find out on
            // its first getMore - as an "unknown cursor" error from the generic path, which says
            // nothing about the actual failure. The caller turns this into an ok:0 reply and logs
            // it there, so this stays at debug rather than logging the same failure twice.
            log.debug("Watch command error for cursor {}: {}", cursorId, e.toString());
            removeWatchCursor(cursorId);
            throw new IllegalStateException("could not start change stream on "
                    + wcmd.getDb() + "." + wcmd.getColl() + ": " + e.getMessage(), e);
        }

        return cursorId;
    }

    /** How many events a watch cursor currently holds undelivered; -1 if it no longer exists.
     * Diagnostics, and the only way for a test to observe the buffered state without draining it. */
    int bufferedEventCount(long cursorId) {
        WatchCursorState state = watchCursors.get(cursorId);
        return state == null ? -1 : state.events.size();
    }

    /**
     * Drop a watch cursor and everything that tracks it. Every removal goes through here: a
     * cursor removed without unregistering its messaging registration leaves a dead id in
     * {@code messagingCursors} forever - the set then never empties, so its key is never removed
     * either, and every later fast-path notification pays a lookup for it on the event loop.
     *
     * @return the removed state, or null if the cursor was already gone
     */
    private WatchCursorState removeWatchCursor(long cursorId) {
        WatchCursorState removed = watchCursors.remove(cursorId);

        if (removed != null) {
            unregisterMessagingCursor(cursorId, removed.db, removed.collection);
        }

        return removed;
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
        // Look the state up FIRST: registering an id whose cursor is already gone (a watch that
        // failed to start, or one killed between creation and registration) leaks it - nothing
        // ever unregisters an id that has no cursor.
        WatchCursorState state = watchCursors.get(cursorId);

        if (state == null) {
            log.debug("Not registering messaging cursor {} for {}.{} - the cursor no longer exists",
                      cursorId, db, collection);
            return;
        }

        String key = db + "." + collection;
        messagingCursors.computeIfAbsent(key, k -> ConcurrentHashMap.newKeySet()).add(cursorId);

        // The cursor can be killed between the lookup above and the add - the kill's
        // unregistration would then run before this registration exists, leaving a dead id
        // behind that nothing ever removes. Re-check and undo, so the invariant "every id in
        // messagingCursors has a live cursor" holds however the two interleave.
        if (!watchCursors.containsKey(cursorId)) {
            unregisterMessagingCursor(cursorId, db, collection);
            log.debug("Rolled back messaging registration for cursor {} - it was killed while registering",
                      cursorId);
            return;
        }

        if (subscriberId != null) {
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
        if (!offerWatchEvent(state, event)) {
            return; // cursor overflowed and was killed
        }

        // Complete any pending getMore request immediately
        PendingGetMore pending;
        while ((pending = state.pendingGetMores.poll()) != null) {
            List<Map<String, Object>> batch = drainWatchEvents(state);
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

        if (!offerWatchEvent(state, event)) {
            return; // cursor overflowed and was killed
        }
        log.trace("onWatchEvent: queued event for cursor {}, queue size now: {}", cursorId, state.events.size());

        // Complete any pending getMore request
        PendingGetMore pending;
        int completedCount = 0;
        while ((pending = state.pendingGetMores.poll()) != null) {
            List<Map<String, Object>> batch = drainWatchEvents(state);
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
            List<Map<String, Object>> batch = drainWatchEvents(state);
            log.debug("getMoreWatch: returning {} existing events for cursor {}", batch.size(), state.cursorId);
            return CompletableFuture.completedFuture(batch);
        }

        // A stream that ENDED because it cannot be served must not answer like an idle one.
        // Checked after the buffered events above, so whatever was already delivered still
        // reaches the client before the error does.
        if (state.wcmd != null && state.wcmd.getTerminalError() != null) {
            log.warn("Cursor {} ended unservable: {}", state.cursorId, state.wcmd.getTerminalError());
            removeWatchCursor(state.cursorId);
            return CompletableFuture.failedFuture(
                new ChangeStreamHistoryLostException(state.wcmd.getTerminalError()));
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
                List<Map<String, Object>> batch = drainWatchEvents(state);
                log.debug("getMoreWatch: race avoided - found {} events after adding pending for cursor {}", batch.size(), state.cursorId);
                future.complete(batch);
                return future;
            }
            // If remove failed, onWatchEvent already completed our future - just return it
        }

        // Same race, other cause: the stream can die between the terminal check above and the
        // offer. failUnservable would then have drained a still-EMPTY pending queue and dropped
        // the cursor, leaving this request orphaned - and the timeout below would answer it after
        // the FULL maxTimeMS with an empty ok:1 batch carrying a live-looking cursor id, which is
        // exactly the "unservable stream looks idle" bug this check exists to prevent.
        String terminal = state.wcmd == null ? null : state.wcmd.getTerminalError();

        if (terminal != null || !watchCursors.containsKey(state.cursorId)) {
            if (state.pendingGetMores.remove(pending)) {
                if (!state.events.isEmpty()) {
                    future.complete(drainWatchEvents(state));
                } else if (terminal != null) {
                    future.completeExceptionally(new ChangeStreamHistoryLostException(terminal));
                } else {
                    // Killed cursor - same answer killCursor gives its pending requests.
                    future.complete(Collections.emptyList());
                }
            }

            return future;
        }

        // Schedule timeout - handle rejected execution during shutdown
        try {
            scheduler.schedule(() -> {
                if (state.pendingGetMores.remove(pending)) {
                    // Timeout - return whatever events are available (may be empty)
                    List<Map<String, Object>> batch = drainWatchEvents(state);
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
        // The count bound MUST be checked BEFORE polling: with the conditions the other way
        // round, the batch-capping drain polled a 101st event off the queue and then dropped
        // it on the floor when the count check short-circuited the loop - silently losing one
        // event per full batch. Under a bulk-insert burst (queue depth > 100 between getMores)
        // that lost ~1% of all change stream events per cursor, which is exactly how the
        // bulk100 messaging benchmark lost ~46 of 5000 messages.
        while (count < 100 && (event = queue.poll()) != null) {
            batch.add(event);
            count++;
        }
        return batch;
    }

    /**
     * Watch-cursor drain: same batch-capping rules as {@link #drainEvents} (count checked
     * BEFORE polling - see the comment there), plus the #321 byte accounting - each event
     * subtracts exactly the bytes it added at offer time.
     */
    private List<Map<String, Object>> drainWatchEvents(WatchCursorState state) {
        List<Map<String, Object>> batch = new ArrayList<>();
        QueuedEvent qe;
        int count = 0;
        while (count < 100 && (qe = state.events.poll()) != null) {
            state.queuedBytes.addAndGet(-qe.bytes());
            batch.add(qe.event());
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
        WatchCursorState watchState = removeWatchCursor(cursorId);
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
     * Enqueue a change stream event, killing the cursor if its buffer is full.
     *
     * @return true if the event was buffered, false if the cursor overflowed and was killed.
     */
    private boolean offerWatchEvent(WatchCursorState state, Map<String, Object> event) {
        long bytes = InMemoryDriver.estimateBsonSize(event);
        long budget = cursorQueueByteBudget;

        // #321: byte bound on top of the count cap, same estimate the replay-buffer and
        // replication-queue budgets use. Kill is the only viable overflow policy here: in
        // server mode delivery runs synchronously on the WRITER thread, so blocking would
        // stall the whole node's write path for one slow consumer, and dropping would
        // silently lose events. A single event larger than the whole budget is still
        // accepted while the queue is empty (newest-event-survives, like the replay buffer) -
        // killing there would impose a document-size cap MongoDB does not have.
        // check-then-add, not atomic: racing writers can overshoot by at most one in-flight
        // event each, which is noise against an estimated budget.
        if (budget > 0 && !state.events.isEmpty() && state.queuedBytes.get() + bytes > budget) {
            log.warn("Change stream cursor {} exceeded its byte budget ({} bytes buffered + {} incoming > {}) — killing cursor (slow/absent consumer)",
                    state.cursorId, state.queuedBytes.get(), bytes, budget);
            WatchCursorState removed = removeWatchCursor(state.cursorId);
            if (removed != null) {
                failPending(removed.pendingGetMores,
                        "cursor killed: event buffer byte budget exceeded (" + budget + " bytes)");
            }
            return false;
        }

        if (state.events.offer(new QueuedEvent(event, bytes))) {
            state.queuedBytes.addAndGet(bytes);
            return true;
        }
        log.warn("Change stream cursor {} exceeded max buffer size {} — killing cursor (slow/absent consumer)",
                state.cursorId, maxCursorQueueSize);
        WatchCursorState removed = removeWatchCursor(state.cursorId);
        if (removed != null) {
            failPending(removed.pendingGetMores,
                    "cursor killed: event buffer overflow (max " + maxCursorQueueSize + " buffered events)");
        }
        return false;
    }

    /**
     * Enqueue a tailable document, killing the cursor if its buffer is full.
     *
     * @return true if the document was buffered, false if the cursor overflowed and was killed.
     */
    private boolean offerTailableDocument(TailableCursorState state, Map<String, Object> document) {
        if (state.documents.offer(document)) {
            return true;
        }
        log.warn("Tailable cursor {} exceeded max buffer size {} — killing cursor (slow/absent consumer)",
                state.cursorId, MAX_CURSOR_QUEUE_SIZE);
        TailableCursorState removed = tailableCursors.remove(state.cursorId);
        if (removed != null) {
            removed.active = false;
            failPending(removed.pendingGetMores,
                    "cursor killed: event buffer overflow (max " + MAX_CURSOR_QUEUE_SIZE + " buffered events)");
        }
        return false;
    }

    /**
     * A change stream that can no longer be served from the replay buffer - the wire answer is
     * MongoDB's ChangeStreamHistoryLost (286), the same code the replication resume gate in
     * MongoCommandHandler already uses, so a client tells "window lost, resync" apart from an
     * ordinary error.
     */
    public static class ChangeStreamHistoryLostException extends RuntimeException {
        public ChangeStreamHistoryLostException(String message) {
            super(message);
        }
    }

    /**
     * The watch ended unservable. Anything already buffered is still handed over first - it was
     * legitimately delivered before the stream broke - and only once the queue is empty do the
     * waiting requests fail. The cursor is dropped either way: it cannot be served again.
     */
    private void failUnservable(long cursorId, String reason) {
        WatchCursorState state = watchCursors.get(cursorId);

        if (state == null) {
            return;
        }

        PendingGetMore p;

        while (!state.events.isEmpty() && (p = state.pendingGetMores.poll()) != null) {
            p.future.complete(drainWatchEvents(state));
        }

        while ((p = state.pendingGetMores.poll()) != null) {
            p.future.completeExceptionally(new ChangeStreamHistoryLostException(reason));
        }

        // Kept only while it still holds undelivered events, so the next getMore can collect
        // them and then hit the terminal check.
        if (state.events.isEmpty()) {
            removeWatchCursor(cursorId);
        }
    }

    /** Fail all pending getMore requests with a CursorKilled-style error. */
    private void failPending(Queue<PendingGetMore> pending, String reason) {
        PendingGetMore p;
        while ((p = pending.poll()) != null) {
            p.future.completeExceptionally(new IllegalStateException(reason));
        }
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

        if (!offerTailableDocument(state, document)) {
            return; // cursor overflowed and was killed
        }

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
            boolean alive = true;
            for (Map<String, Object> doc : documents) {
                // Check if document matches the cursor's filter (if any)
                if (state.filter == null || state.filter.isEmpty() || matchesFilter(doc, state.filter)) {
                    if (!offerTailableDocument(state, doc)) {
                        alive = false; // cursor overflowed and was killed
                        break;
                    }
                }
            }
            if (!alive) {
                continue;
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

    /**
     * A buffered event with its size, estimated ONCE at offer time - the drain subtracts the
     * exact same number it added, so the per-cursor byte counter cannot drift (#321).
     */
    private record QueuedEvent(Map<String, Object> event, long bytes) {}

    private static class WatchCursorState {
        final long cursorId;
        final String db;
        final String collection;
        // The command the watch runs under: it carries the reason when the stream ended because
        // it could not be served, which getMore turns into an error instead of an empty batch.
        volatile WatchCommand wcmd;
        // Bounded so a slow/absent consumer cannot grow the buffer without limit.
        // offer() returns false at capacity; the caller then kills the cursor.
        final Queue<QueuedEvent> events;
        // #321: estimated bytes of everything in `events` - offer-time add, drain-time subtract.
        final AtomicLong queuedBytes = new AtomicLong(0);
        final Queue<PendingGetMore> pendingGetMores = new ConcurrentLinkedQueue<>();
        // For messaging fast-path: subscriber ID for sender filtering
        volatile String subscriberId;

        WatchCursorState(long cursorId, String db, String collection, int queueCapacity) {
            this.cursorId = cursorId;
            this.db = db;
            this.collection = collection;
            this.events = new LinkedBlockingQueue<>(queueCapacity);
        }
    }

    private static class TailableCursorState {
        final long cursorId;
        final String db;
        final String collection;
        final Map<String, Object> filter;
        // Bounded — see WatchCursorState.events.
        final Queue<Map<String, Object>> documents = new LinkedBlockingQueue<>(MAX_CURSOR_QUEUE_SIZE);
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
