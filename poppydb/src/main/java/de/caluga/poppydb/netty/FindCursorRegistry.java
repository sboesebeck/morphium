package de.caluga.poppydb.netty;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Per-PoppyDB-instance registry of server-side find cursors (see {@link MongoCommandHandler}'s
 * batched-find handling in {@code processFindDirect}/{@code processGetMore}).
 *
 * <p>Owned by exactly one {@code PoppyDB} instance and handed to every {@link MongoCommandHandler}
 * created for that instance's connections — this mirrors how {@code PoppyDB} already threads a
 * per-instance {@link WatchCursorManager} and a live {@code Supplier<ReplicationCoordinator>} into
 * each handler (see {@code PoppyDB}'s {@code commandHandler} wiring). Before this class existed,
 * the find-cursor map and its sweeper were JVM-static on {@code MongoCommandHandler}, so multiple
 * {@code PoppyDB} instances running in one test JVM shared cursor state — a cursor opened on one
 * instance was visible to another instance's {@link #openFindCursors()} count. Making this a plain
 * per-instance object (constructed once per {@code PoppyDB}, like {@link WatchCursorManager})
 * fixes that without inventing a new threading mechanism.
 *
 * <p>Owns its own idle-cursor-sweeping daemon thread, started at construction and stopped by
 * {@link #shutdown()} — mirrors {@link WatchCursorManager}'s own dedicated scheduler rather than
 * sharing one sweeper across instances, which is the simpler correct design here (one extra daemon
 * thread per running {@code PoppyDB} instance is cheap, and it avoids having to make the sweeper
 * itself aware of a dynamic set of registries to iterate).
 */
public class FindCursorRegistry {

    private static final Logger log = LoggerFactory.getLogger(FindCursorRegistry.class);

    /** Idle time after which an unexhausted find cursor is reaped (MongoDB default cursorTimeoutMillis = 10 min). */
    static final long FIND_CURSOR_TTL_MS = 10 * 60 * 1000L;

    private final AtomicLong cursorIdSeq = new AtomicLong(1_000_000);
    private final ConcurrentHashMap<Long, FindCursorState> findCursors = new ConcurrentHashMap<>();
    private final ScheduledExecutorService sweeper;

    public FindCursorRegistry() {
        sweeper = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "PoppyDB-cursor-sweeper");
            t.setDaemon(true);
            return t;
        });
        sweeper.scheduleWithFixedDelay(this::sweepIdleFindCursors, 1, 1, TimeUnit.MINUTES);
    }

    /** Allocates the next server-side find-cursor id for this instance. */
    long nextCursorId() {
        return cursorIdSeq.incrementAndGet();
    }

    void put(long cursorId, FindCursorState state) {
        findCursors.put(cursorId, state);
    }

    FindCursorState get(long cursorId) {
        return findCursors.get(cursorId);
    }

    boolean containsKey(long cursorId) {
        return findCursors.containsKey(cursorId);
    }

    FindCursorState remove(long cursorId) {
        return findCursors.remove(cursorId);
    }

    private void sweepIdleFindCursors() {
        long cutoff = System.currentTimeMillis() - FIND_CURSOR_TTL_MS;
        int removed = 0;
        for (var e : findCursors.entrySet()) {
            if (e.getValue().lastAccessed < cutoff && findCursors.remove(e.getKey(), e.getValue())) {
                removed++;
            }
        }
        if (removed > 0) {
            log.debug("Cursor sweeper reaped {} idle find cursor(s)", removed);
        }
    }

    /** Test/monitoring hook: number of currently open server-side find cursors on this instance. */
    public int openFindCursors() {
        return findCursors.size();
    }

    /**
     * Test/monitoring hook: number of documents currently retained in memory for a server-side
     * find cursor's bounded window on this instance (never the full remainder — see
     * {@link FindCursorState}). Returns -1 if no such cursor is open.
     */
    public int retainedFindCursorDocs(long cursorId) {
        FindCursorState state = findCursors.get(cursorId);
        return state == null ? -1 : state.remaining.size();
    }

    /**
     * Stops this instance's sweeper thread. Called from {@code PoppyDB#shutdown()} — must be
     * called on every instance shutdown so sweeper threads don't leak across test instances.
     */
    public void shutdown() {
        sweeper.shutdown();
        try {
            if (!sweeper.awaitTermination(5, TimeUnit.SECONDS)) {
                sweeper.shutdownNow();
            }
        } catch (InterruptedException e) {
            sweeper.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    /**
     * A server-side find cursor no longer pins the full (possibly huge) result remainder in
     * memory. Instead it retains a bounded window of at most {@code MAX_RETAINED_BATCHES ×
     * batchSize} documents, plus the query/sort/projection and a skip offset. When the window
     * drains, {@code MongoCommandHandler#refillFindCursorWindow} RE-EXECUTES the find with an
     * advanced skip/limit to fetch the next window (see the original design brief: the
     * InMemoryDriver itself only returns full lists, so a truly lazy driver-level cursor is out
     * of scope here — this is the pragmatic bound at the handler level).
     *
     * <p>Correctness caveat: because each refill is a fresh query rather than a continuation of
     * a stable snapshot, concurrent writes between getMore calls can shift the skip window —
     * documents inserted/deleted ahead of the cursor's position can cause a later window to
     * skip or (rarely) repeat documents. This mirrors MongoDB's own cursor semantics on
     * unclustered collections, which are likewise not snapshot-isolated against concurrent
     * writes; it is not a new weakness introduced by bounding retention.
     */
    static final class FindCursorState {
        final String db;
        final String collection;
        final Map<String, Object> filter;
        final Map<String, Object> sort;
        final Map<String, Object> projection;
        final int batchSize;
        // true if the original find had a positive (non-zero) limit; caps how many more
        // documents may ever be pulled in via refills, independent of what's left to match.
        final boolean hasLimit;
        // Bounded window of not-yet-delivered documents, refilled on demand. Never exceeds
        // MAX_RETAINED_BATCHES * batchSize documents.
        final List<Map<String, Object>> remaining;
        // Offset to resume the query from on the next refill.
        int nextSkip;
        // Remaining document budget when hasLimit is true; refills stop once this hits zero.
        int remainingLimit;
        volatile long lastAccessed;

        FindCursorState(String db, String collection, Map<String, Object> filter, Map<String, Object> sort,
                         Map<String, Object> projection, List<Map<String, Object>> remaining, int batchSize,
                         int nextSkip, boolean hasLimit, int remainingLimit) {
            this.db = db;
            this.collection = collection;
            this.filter = filter;
            this.sort = sort;
            this.projection = projection;
            this.remaining = remaining;
            this.batchSize = batchSize;
            this.nextSkip = nextSkip;
            this.hasLimit = hasLimit;
            this.remainingLimit = remainingLimit;
            this.lastAccessed = System.currentTimeMillis();
        }
    }
}
