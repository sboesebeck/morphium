package de.caluga.morphium.writer;

import static de.caluga.morphium.annotations.caching.WriteBuffer.STRATEGY.JUST_WARN;

import de.caluga.morphium.*;
import de.caluga.morphium.annotations.Capped;
import de.caluga.morphium.annotations.caching.WriteBuffer;
import de.caluga.morphium.async.AsyncOperationCallback;
import de.caluga.morphium.async.AsyncOperationType;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.driver.bulk.BulkRequestContext;
import de.caluga.morphium.driver.bulk.DeleteBulkRequest;
import de.caluga.morphium.driver.bulk.InsertBulkRequest;
import de.caluga.morphium.driver.bulk.UpdateBulkRequest;
import de.caluga.morphium.driver.commands.CreateCommand;
import de.caluga.morphium.driver.commands.ExplainCommand.ExplainVerbosity;
import de.caluga.morphium.driver.wire.MongoConnection;
import de.caluga.morphium.query.Query;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.bson.types.ObjectId;

import java.lang.reflect.Field;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

/**
 * User: Stephan Bösebeck Date: 11.03.13 Time: 11:41
 *
 * <p>
 * Buffered Writer buffers all write requests (store, update, remove...) to mongo for a certain
 * time. After that time the requests are issued en block to mongo. Attention: this is not using
 * BULK-Requests yet!
 */
@SuppressWarnings({"EmptyCatchBlock", "SynchronizeOnNonFinalField", "WeakerAccess", "BusyWait"})
public class BufferedMorphiumWriterImpl implements MorphiumWriter, ShutdownListener {

    private static final Logger logger = LoggerFactory.getLogger(BufferedMorphiumWriterImpl.class);
    // needs to be securely stored
    private final Map < Class<?>, List<WriteBufferEntry>> opLog = new ConcurrentHashMap<>(); // synced
    private final List<UnresolvedWrite> unresolvedWrites = Collections.synchronizedList(new ArrayList<>());
    private final Map < Class<?>, Long> lastRun = new ConcurrentHashMap<>();
    private Morphium morphium;
    private MorphiumWriter directWriter;
    private Thread housekeeping;
    private volatile boolean running = true;
    private boolean orderedExecution = false;

    public BufferedMorphiumWriterImpl() {
    }

    public void close() {
        running = false;

        // Wait for the housekeeping thread to finish current operations
        // This prevents "Driver is shutting down" errors when the thread
        // tries to flush pending writes after the driver has been closed
        if (housekeeping != null && housekeeping.isAlive()) {
            housekeeping.interrupt(); // Wake up if sleeping
            try {
                // Wait up to 5 seconds for the thread to finish
                housekeeping.join(5000);
                if (housekeeping.isAlive()) {
                    logger.warn("Housekeeping thread did not stop in time, proceeding with shutdown");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @SuppressWarnings("unused")
    public boolean isOrderedExecution() {
        return orderedExecution;
    }

    @SuppressWarnings("unused")
    public void setOrderedExecution(boolean orderedExecution) {
        this.orderedExecution = orderedExecution;
    }


    private void createCappedColl(Class c, String n) {
        if (logger.isDebugEnabled()) {
            logger.debug("Collection does not exist - ensuring indices / capped status");
        }

        Capped capped = morphium.getARHelper().getAnnotationFromHierarchy(c, Capped.class);

        if (capped == null) {
            return;
        }

        CreateCommand cmd = null;
        MongoConnection primaryConnection = null;

        try {
            primaryConnection = morphium.getDriver().getPrimaryConnection(null);
            cmd = new CreateCommand(primaryConnection);
            cmd.setCapped(true);
            cmd.setSize(capped.maxSize()).setMax(capped.maxEntries());
            cmd.setDb(morphium.getConfig().connectionSettings().getDatabase());
            //primaryConnection.sendCommand(cmd);
            cmd.execute();
            cmd.releaseConnection();
            cmd = null;
            primaryConnection = null;
        } finally {
            if (cmd != null) {
                cmd.releaseConnection();
            } else if (primaryConnection != null) {
                morphium.getDriver().releaseConnection(primaryConnection);
            }
        }
    }

    @SuppressWarnings({"CommentedOutCode", "unchecked"})
    private void flushQueueToMongo(List<WriteBufferEntry> q) {
        if (q == null) {
            return;
        }

        List<WriteBufferEntry> localQueue;
        synchronized (q) {
            localQueue = new ArrayList<>(q);
        }
        Map<String, BulkRequestContext> bulkByCollectionName = new HashMap<>();
        Map<BulkRequestContext, List<WriteBufferEntry>> entriesByContext = new IdentityHashMap<>();
        Set<BulkRequestContext> failedContexts = Collections.newSetFromMap(new IdentityHashMap<>());

        for (WriteBufferEntry entry : localQueue) {
            if (entry.isFailed()) {
                continue;
            }

            try {
                if (morphium.getConfig().isAutoIndexAndCappedCreationOnWrite()
                        && !morphium.exists(morphium.getConfig().connectionSettings().getDatabase(), entry.getCollectionName())) {
                    createCappedColl(entry.getEntityType(), entry.getCollectionName());
                    morphium.ensureIndicesFor(entry.getEntityType(), entry.getCollectionName(), entry.getCb(), directWriter);
                }

                BulkRequestContext context = bulkByCollectionName.get(entry.getCollectionName());
                if (context == null) {
                    WriteBuffer w = morphium.getARHelper().getAnnotationFromHierarchy(entry.getEntityType(), WriteBuffer.class);
                    boolean ordered = w != null && w.ordered();
                    context = morphium.getDriver().createBulkContext(
                            morphium,
                            morphium.getConfig().connectionSettings().getDatabase(),
                            entry.getCollectionName(),
                            ordered,
                            morphium.getWriteConcernForClass(entry.getEntityType()));
                    bulkByCollectionName.put(entry.getCollectionName(), context);
                }

                if (failedContexts.contains(context)) {
                    failEntry(entry, new IllegalStateException("Bulk context was not executed after an earlier enqueue failure"));
                    continue;
                }

                entry.getToRun().queue(context);
                entriesByContext.computeIfAbsent(context, ignored -> new ArrayList<>()).add(entry);
            } catch (Exception e) {
                logger.error("Could not enqueue buffered write; retaining it as unresolved", e);
                failEntry(entry, e);
                BulkRequestContext context = bulkByCollectionName.get(entry.getCollectionName());
                if (context != null) {
                    failedContexts.add(context);
                    for (WriteBufferEntry queued : entriesByContext.getOrDefault(context, Collections.emptyList())) {
                        failEntry(queued, e);
                    }
                }
            }
        }

        for (Map.Entry<BulkRequestContext, List<WriteBufferEntry>> bulk : entriesByContext.entrySet()) {
            BulkRequestContext context = bulk.getKey();
            List<WriteBufferEntry> entries = bulk.getValue();

            if (!failedContexts.contains(context)) {
                try {
                    checkBulkResult(context.execute());
                    for (WriteBufferEntry entry : entries) {
                        reportSuccess(entry);
                    }
                } catch (Exception e) {
                    logger.error("Bulk execution outcome is unknown; retaining entries without automatic retry", e);
                    for (WriteBufferEntry entry : entries) {
                        failEntry(entry, e);
                    }
                }
            }
        }

        for (WriteBufferEntry entry : localQueue) {
            try {
                if (morphium.getCache() != null) {
                    morphium.clearCacheforClassIfNecessary(entry.getEntityType());
                }
            } catch (RuntimeException e) {
                logger.warn("Could not clear cache after buffered write processing", e);
            }

            q.remove(entry);
        }
    }

    private void failEntry(WriteBufferEntry entry, Throwable error) {
        if (!entry.isFailed()) {
            entry.markFailed(error);
            unresolvedWrites.add(new UnresolvedWrite(entry));
            reportFailure(entry, error);
        }
    }

    /**
     * Snapshot of unresolved writes, held in memory only. Reconcile each outcome with the
     * database/application before discarding it or submitting a new operation. Neither flush
     * nor close retries these entries; retain the writer reference to inspect them after close.
     * Entries and captured operation state occupy memory until explicitly discarded. The public
     * view deliberately does not expose captured query/document state: it may be mutable or
     * contain sensitive values. To correlate a quarantined write with application intent, pass a
     * unique callback instance per logical operation and match it by object identity using
     * {@link UnresolvedWrite#getCallback()}; callers that do not supply a unique callback cannot
     * reliably distinguish otherwise identical writes from this view.
     */
    public List<UnresolvedWrite> getUnresolvedWrites() {
        synchronized (unresolvedWrites) {
            return List.copyOf(unresolvedWrites);
        }
    }

    /** Forget an inspected entry after external reconciliation; this does not execute it. */
    public boolean discardUnresolvedWrite(UnresolvedWrite write) {
        return unresolvedWrites.remove(write);
    }

    public static final class UnresolvedWrite {
        private final WriteBufferEntry entry;

        private UnresolvedWrite(WriteBufferEntry entry) {
            this.entry = entry;
        }

        public Class<?> getEntityType() { return entry.getEntityType(); }
        public String getCollectionName() { return entry.getCollectionName(); }
        public AsyncOperationType getType() { return entry.getType(); }
        public long getTimestamp() { return entry.getTimestamp(); }
        public Throwable getFailure() { return entry.failure; }
        /**
         * Returns the exact callback instance supplied with this operation. Use a unique callback
         * instance for each logical write and compare by identity as the caller correlation handle.
         * Query/document payload is intentionally not exposed; reconcile from caller-owned state
         * rather than from mutable captured writer internals.
         */
        public AsyncOperationCallback<?> getCallback() { return entry.getCb(); }
    }

    private void reportFailure(WriteBufferEntry entry, Throwable error) {
        try {
            entry.getCb().onOperationError(entry.getType(), null, 0,
                    "Buffered write failed; outcome may be unknown and will not be retried automatically",
                    error, null);
        } catch (RuntimeException callbackError) {
            logger.warn("Buffered write failure callback threw an exception", callbackError);
        }
    }

    private void reportSuccess(WriteBufferEntry entry) {
        try {
            entry.getCb().onOperationSucceeded(entry.getType(), null, 0, null, null);
        } catch (RuntimeException callbackError) {
            logger.warn("Buffered write success callback threw an exception", callbackError);
        }
    }

    private void checkBulkResult(Map<String, Object> result) {
        if (result == null || (result.get("ok") instanceof Number ok && ok.doubleValue() != 1.0)
                || (result.get("writeErrors") instanceof List<?> errors && !errors.isEmpty())
                || result.get("writeConcernError") != null
                || (result.get("writeConcernErrors") instanceof List<?> errors && !errors.isEmpty())) {
            throw new MorphiumDriverException("Bulk write was not confirmed: " + result);
        }
    }

    private void executeImmediately(WriteBufferEntry entry, boolean ordered) {
        try {
            BulkRequestContext context = morphium.getDriver().createBulkContext(morphium,
                    morphium.getConfig().connectionSettings().getDatabase(), entry.getCollectionName(),
                    ordered, morphium.getWriteConcernForClass(entry.getEntityType()));
            entry.getToRun().queue(context);
            checkBulkResult(context.execute());
            reportSuccess(entry);
        } catch (Exception e) {
            failEntry(entry, e);
        }
    }

    public void addToWriteQueue(Class<?> type, String collectionName, BufferedBulkOp r, AsyncOperationCallback c, AsyncOperationType t) {
        if (collectionName == null) {
            collectionName = morphium.getMapper().getCollectionName(type);
        }

        WriteBufferEntry wb = new WriteBufferEntry(type, collectionName, r, System.currentTimeMillis(), c, t);
        WriteBuffer w = morphium.getARHelper().getAnnotationFromHierarchy(type, WriteBuffer.class);
        int size = 0;
        //        int timeout = morphium.getConfig().getWriteBufferTime();
        WriteBuffer.STRATEGY strategy = JUST_WARN;
        boolean ordered = false;

        if (w != null) {
            ordered = w.ordered();
            size = w.size();
            strategy = w.strategy();
        }

        if (size == 0) {
            // No @WriteBuffer on this class → execute immediately, do not buffer.
            executeImmediately(wb, ordered);
            return;
        }

        List<WriteBufferEntry> buffer;
        synchronized (opLog) {
            buffer = opLog.computeIfAbsent(type, k -> Collections.synchronizedList(new ArrayList<>()));
            if (size < 0 || buffer.size() < size) {
                buffer.add(wb);
                return;
            }
        }

        if (size > 0) {
            logger.warn("WARNING: Write buffer for type " + type.getName() + " maximum exceeded: " + buffer.size() + " entries now, max is " + size);

            switch (strategy) {
                case WAIT:
                    long start = System.currentTimeMillis();

                    while (true) {
                        int timeout = w.timeout();

                        if (morphium.getConfig().connectionSettings().getMaxWaitTime() > 0 && morphium.getConfig().connectionSettings().getMaxWaitTime() > timeout) {
                            timeout = morphium.getConfig().connectionSettings().getMaxWaitTime();
                        }

                        if (timeout > 0 && System.currentTimeMillis() - start > timeout) {
                            logger.error("Could not write - maxWaitTime/timeout exceeded!");
                            throw new RuntimeException("could now write - maxWaitTimeExceded " + morphium.getConfig().connectionSettings().getMaxWaitTime() + "ms");
                        }

                        synchronized (opLog) {
                            List<WriteBufferEntry> current = opLog.computeIfAbsent(type,
                                    k -> Collections.synchronizedList(new ArrayList<>()));
                            if (current.size() < size) {
                                current.add(wb);
                                break;
                            }
                        }

                        try {
                            Thread.sleep(morphium.getConfig().driverSettings().getIdleSleepTime());
                        } catch (InterruptedException e1) {
                        }
                    }
                    break;

                case JUST_WARN:
                    synchronized (opLog) {
                        opLog.computeIfAbsent(type, k -> Collections.synchronizedList(new ArrayList<>())).add(wb);
                    }
                    break;

                case IGNORE_NEW:
                    logger.warn("ignoring new incoming...");
                    return;

                case WRITE_NEW:
                    logger.warn("directly writing data... due to strategy setting");
                    executeImmediately(wb, ordered);
                    return;

                case WRITE_OLD:
                    // could have been written in the meantime
                    WriteBufferEntry old = null;
                    synchronized (opLog) {
                        List<WriteBufferEntry> current = opLog.computeIfAbsent(type, k -> Collections.synchronizedList(new ArrayList<>()));
                        current.sort(Comparator.comparingLong(WriteBufferEntry::getTimestamp));

                        if (!current.isEmpty()) {
                            old = current.remove(0);
                        }

                        current.add(wb);
                    }

                    if (old != null) {
                        executeImmediately(old, ordered);
                    }
                    return;

                case DEL_OLD:
                    if (logger.isDebugEnabled()) {
                        logger.debug("Deleting oldest entry");
                    }

                    synchronized (opLog) {
                        List<WriteBufferEntry> current = opLog.computeIfAbsent(type, k -> Collections.synchronizedList(new ArrayList<>()));
                        current.sort(Comparator.comparingLong(WriteBufferEntry::getTimestamp));

                        if (!current.isEmpty()) {
                            current.remove(0);
                        }

                        current.add(wb);
                    }

                    return;
            }
        }

        //        }
    }

    @Override
    public <T> void insert(T o, String collection, AsyncOperationCallback<T> c) {
        if (c == null) {
            c = new AsyncOpAdapter<>();
        }

        morphium.inc(StatisticKeys.WRITES_CACHED);
        addToWriteQueue(o.getClass(), collection, ctx -> {
            // do nothing
            morphium.firePreStore(o, true);
            ArrayList<Map<String, Object>> objToInsert = new ArrayList<>();

            try {
                setIdIfNull(o);
            } catch (IllegalAccessException e) {
            }
            objToInsert.add(Doc.of(morphium.getMapper().serialize(o)));
            InsertBulkRequest ins = ctx.addInsertBulkRequest(objToInsert);

            morphium.clearCacheforClassIfNecessary(o.getClass());
            morphium.firePostStore(o, true);
        }, c, AsyncOperationType.WRITE);
    }

    private <T> void setIdIfNull(T o) throws IllegalAccessException {
        Field idf = morphium.getARHelper().getIdField(o);

        if (idf.get(o) != null) {
            return;
        }

        if (idf.getType().equals(MorphiumId.class)) {
            idf.set(o, new MorphiumId());
        } else if (idf.getType().equals(String.class)) {
            idf.set(o, new MorphiumId().toString());
        } else if (idf.getType().equals(UUID.class)) {
            idf.set(o, UUID.randomUUID());
        } else if (idf.getType().equals(ObjectId.class)) {
            idf.set(o, new ObjectId());
        } else {
            throw new RuntimeException("Cannot auto-generate ID for type "
                    + idf.getType().getSimpleName());
        }
    }

    @Override
    public <T> void insert(List<T> o, AsyncOperationCallback<T> callback) {
        store(o, null, callback);
    }

    @Override
    public <T> void insert(List<T> lst, String collectionName, AsyncOperationCallback<T> c) {
        if (lst == null || lst.isEmpty()) {
            if (c != null) {
                c.onOperationSucceeded(AsyncOperationType.WRITE, null, 0, lst, null);
            }

            return;
        }

        final List<T> lstSnapshot = new ArrayList<>(lst);
        if (c == null) {
            c = new AsyncOpAdapter<>();
        }

        morphium.inc(StatisticKeys.WRITES_CACHED);

        if (morphium.isAutoValuesEnabledForThread()) {
            try {
                morphium.setAutoValuesBatch(lstSnapshot);
            } catch (IllegalAccessException e) {
                logger.error("Could not set @AutoSequence batch values", e);
            }
            for (T obj : lstSnapshot) {
                try {
                    morphium.setAutoValues(obj);
                } catch (IllegalAccessException e) {
                    logger.error("Could not set auto variables", e);
                }
            }
        }

        final AsyncOperationCallback<T> finalC = c;
        addToWriteQueue(lstSnapshot.get(0).getClass(), collectionName, ctx -> {
            Map<Object, Boolean> map = new HashMap<>();
            List<Map<String, Object>> marshalled = new ArrayList<>();

            for (T o : lstSnapshot) {
                map.put(o, true);

                try {
                    setIdIfNull(o);
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                }

                marshalled.add(Doc.of(morphium.getMapper().serialize(o)));
            }
            morphium.firePreStore(map);

            if (!marshalled.isEmpty()) {
                ctx.addInsertBulkRequest(marshalled);
            }
            morphium.clearCachefor(lstSnapshot.get(0).getClass());
            morphium.firePostStore(map);
        }, c, AsyncOperationType.WRITE);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> void store(final T o, final String collection, AsyncOperationCallback<T> c) {
        if (c == null) {
            c = new AsyncOpAdapter<>();
        }

        morphium.inc(StatisticKeys.WRITES_CACHED);
        addToWriteQueue(o.getClass(), collection, ctx -> {
            // do nothing
            boolean isNew = morphium.getARHelper().getId(o) == null;

            if (!isNew && !morphium.getARHelper().getIdField(o).getType().equals(MorphiumId.class)) {
                // need to check if type is not MongoId
                isNew = (morphium.createQueryFor(o.getClass()).f("_id").eq(morphium.getId(o)).countAll() == 0);
            }
            morphium.firePreStore(o, isNew);

            if (isNew) {
                ArrayList<Map<String, Object>> objToInsert = new ArrayList<>();

                try {
                    setIdIfNull(o);
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                }

                objToInsert.add(Doc.of(morphium.getMapper().serialize(o)));
                InsertBulkRequest ins = ctx.addInsertBulkRequest(objToInsert);
            } else {
                UpdateBulkRequest up = ctx.addUpdateBulkRequest();
                up.setMultiple(false);
                up.setUpsert(true); // insert for non-objectId
                up.setQuery(Doc.of((morphium.createQueryFor(o.getClass()).f(morphium.getARHelper().getIdFieldName(o)).eq(morphium.getARHelper().getId(o))).toQueryObject()));
                Doc cmd = new Doc();
                up.setCmd(Doc.of("$set", cmd));

                //noinspection unchecked
                for (String f : morphium.getARHelper().getFields(o.getClass())) {
                    try {
                        Object serialize = null;
                        Field field = morphium.getARHelper().getField(o.getClass(), f);

                        if (field.getType().getName().startsWith("java.lang") || field.getType().isPrimitive() || MorphiumId.class.isAssignableFrom(field.getType())) {
                            if (!(Map.class.isAssignableFrom(field.getType())) && !(Map.class.isAssignableFrom(field.getType())) && !field.getType().isArray()) {
                                serialize = field.get(o);
                            }
                        } else if (field.get(o) instanceof Collection) {
                            serialize = handleList((Collection) field.get(o));
                        }

                        if (serialize == null) {
                            serialize = morphium.getMapper().serialize(field.get(o));
                        }

                        cmd.put(morphium.getARHelper().getMongoFieldName(o.getClass(), f), serialize);
                    } catch (IllegalAccessException e) {
                        //swallow
                    }
                }
            }
            //                morphium.clearCacheforClassIfNecessary(o.getClass());
            morphium.firePostStore(o, isNew);
        }, c, AsyncOperationType.WRITE);
    }

    @SuppressWarnings("unchecked")
    private List handleList(Collection o) {
        List lst = new ArrayList();

        for (Object el : o) {
            if (el instanceof Collection) {
                //noinspection unchecked
                lst.add(handleList((Collection) el));
            } else if (el instanceof MorphiumId) {
                Map m = UtilsMap.of("morphium id", el.toString());
                //noinspection unchecked
                lst.add(m);
            } else {
                //noinspection unchecked
                lst.add(morphium.getMapper().serialize(el));
            }
        }

        return lst;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> void store(final List<T> lst, final String collectionName, AsyncOperationCallback<T> c) {
        if (lst == null || lst.isEmpty()) {
            if (c != null) {
                c.onOperationSucceeded(AsyncOperationType.WRITE, null, 0, lst, null);
            }

            return;
        }

        final List<T> lstSnapshot = new ArrayList<>(lst);
        if (c == null) {
            c = new AsyncOpAdapter<>();
        }

        morphium.inc(StatisticKeys.WRITES_CACHED);

        if (morphium.isAutoValuesEnabledForThread()) {
            try {
                morphium.setAutoValuesBatch(lstSnapshot);
            } catch (IllegalAccessException e) {
                logger.error("Could not set @AutoSequence batch values", e);
            }
            for (T obj : lstSnapshot) {
                try {
                    morphium.setAutoValues(obj);
                } catch (IllegalAccessException e) {
                    logger.error("Could not set auto variables", e);
                }
            }
        }

        final AsyncOperationCallback<T> finalC = c;
        addToWriteQueue(lstSnapshot.get(0).getClass(), collectionName, ctx -> {
            Map<Object, Boolean> map = new HashMap<>();

            for (T o : lstSnapshot) {
                map.put(o, morphium.getARHelper().getId(o) == null);
            }
            morphium.firePreStore(map);
            List<Map<String, Object>> toInsert = new ArrayList<>();

            for (Map.Entry<Object, Boolean> entry : map.entrySet()) {
                if (entry.getValue()) {
                    try {
                        setIdIfNull(entry.getKey());
                    } catch (IllegalAccessException e) {
                        throw new RuntimeException(e);
                    }

                    toInsert.add(Doc.of(morphium.getMapper().serialize(entry.getKey())));
                } else {
                    //noinspection unchecked
                    store((T) entry.getKey(), morphium.getMapper().getCollectionName(entry.getKey().getClass()), finalC);
                }
            }
            if (!toInsert.isEmpty()) {
                ctx.addInsertBulkRequest(toInsert);
            }
            morphium.firePostStore(map);
        }, c, AsyncOperationType.WRITE);
    }

    @Override
    public <T> void updateUsingFields(final T ent, final String collection, AsyncOperationCallback<T> c, final String... fields) {
        if (c == null) {
            c = new AsyncOpAdapter<>();
        }

        //        final AsyncOperationCallback<T> callback = c;
        morphium.inc(StatisticKeys.WRITES_CACHED);
        addToWriteQueue(ent.getClass(), collection, ctx -> {
            //                directWriter.updateUsingFields(ent, collection, callback,
            // fields);
            morphium.firePreUpdateEvent(ent.getClass(), MorphiumStorageListener.UpdateTypes.SET);
            @SuppressWarnings("unchecked")
            Query<Object> query = (Query<Object>) morphium.createQueryFor(ent.getClass()).f(morphium.getARHelper().getIdFieldName(ent)).eq(morphium.getARHelper().getId(ent));

            if (collection != null) {
                query.setCollectionName(collection);
            }
            //                BulkRequestWrapper r = ctx.addFind(query);
            String[] flds = fields;

            if (flds.length == 0) {
                Stream<Field> stream = morphium.getARHelper().getAllFields(ent.getClass()).stream();
                Stream<String> m = stream.map(Field::getName);
                flds = m.toArray(String[]::new);
            }

            UpdateBulkRequest r = ctx.addUpdateBulkRequest();
            r.setMultiple(false);
            r.setUpsert(false);
            r.setQuery(Doc.of(query.toQueryObject()));
            Doc set = new Doc();
            r.setCmd(Doc.of("$set", set));

            for (String f : flds) {
                String fld = morphium.getARHelper().getMongoFieldName(query.getType(), f);
                set.put(fld, morphium.getARHelper().getValue(ent, f));
            }
            morphium.getCache().clearCacheIfNecessary(ent.getClass());
            morphium.firePostUpdateEvent(ent.getClass(), MorphiumStorageListener.UpdateTypes.SET);
        }, c, AsyncOperationType.UPDATE);
    }

    @Override
    public <T> Map<String, Object> set(final Query<T> query, final Map<String, Object> values, final boolean upsert, final boolean multiple, AsyncOperationCallback<T> c) {
        if (c == null) {
            c = new AsyncOpAdapter<>();
        }

        //        final AsyncOperationCallback<T> callback = c;
        morphium.inc(StatisticKeys.WRITES_CACHED);
        addToWriteQueue(query.getType(), query.getCollectionName(), ctx -> {
            //                directWriter.set(toSet, collection, field, value, upsert,
            // multiple, callback);
            morphium.firePreUpdateEvent(query.getType(), MorphiumStorageListener.UpdateTypes.SET);

            UpdateBulkRequest wr = ctx.addUpdateBulkRequest();
            wr.setUpsert(upsert);
            wr.setMultiple(multiple);
            morphium.getCache().clearCacheIfNecessary(query.getType());
            wr.setQuery(Doc.of(query.toQueryObject()));
            Doc set = new Doc();
            wr.setCmd(Doc.of("$set", set));

            for (Map.Entry kv : values.entrySet()) {
                String fld = morphium.getARHelper().getMongoFieldName(query.getType(), kv.getKey().toString());
                set.put(fld, kv.getValue());
            }
            morphium.firePostUpdateEvent(query.getType(), MorphiumStorageListener.UpdateTypes.SET);
        }, c, AsyncOperationType.SET);
        return null;
    }

    @Override
    public <T> Map<String, Object> inc(final Query<T> query, final Map<String, Number> fieldsToInc, final boolean upsert, final boolean multiple, AsyncOperationCallback<T> c) {
        if (c == null) {
            c = new AsyncOpAdapter<>();
        }

        //        final AsyncOperationCallback<T> callback = c;
        morphium.inc(StatisticKeys.WRITES_CACHED);
        addToWriteQueue(query.getType(), query.getCollectionName(), ctx -> {
            //                directWriter.set(toSet, collection, field, value, upsert,
            // multiple, callback);
            morphium.firePreUpdateEvent(query.getType(), MorphiumStorageListener.UpdateTypes.INC);
            UpdateBulkRequest wr = ctx.addUpdateBulkRequest();
            wr.setQuery(Doc.of(query.toQueryObject()));
            wr.setUpsert(upsert);
            wr.setMultiple(multiple);
            Doc inc = new Doc();
            wr.setCmd(Doc.of("$inc", inc));
            morphium.getCache().clearCacheIfNecessary(query.getType());

            for (Map.Entry kv : fieldsToInc.entrySet()) {
                String fld = morphium.getARHelper().getMongoFieldName(query.getType(), kv.getKey().toString());
                inc.put(fld, kv.getValue());
            }
            morphium.firePostUpdateEvent(query.getType(), MorphiumStorageListener.UpdateTypes.INC);
        }, c, AsyncOperationType.INC);
        return null;
    }

    @SuppressWarnings("CommentedOutCode")
    @Override
    public <T> Map<String, Object> inc(final Query<T> query, final String field, final Number amount, final boolean upsert, final boolean multiple, AsyncOperationCallback<T> c) {
        if (c == null) {
            c = new AsyncOpAdapter<>();
        }

        morphium.inc(StatisticKeys.WRITES_CACHED);
        addToWriteQueue(query.getType(), query.getCollectionName(), ctx -> {
            //                directWriter.set(toSet, collection, field, value, upsert,
            // multiple, callback);
            UpdateBulkRequest wr = ctx.addUpdateBulkRequest();
            String fieldName = morphium.getARHelper().getMongoFieldName(query.getType(), field);
            wr.setCmd(Doc.of("$inc", Doc.of(fieldName, amount)));
            wr.setUpsert(upsert);
            wr.setMultiple(multiple);
            wr.setQuery(Doc.of(query.toQueryObject()));
            morphium.firePreUpdateEvent(query.getType(), MorphiumStorageListener.UpdateTypes.INC);
            morphium.getCache().clearCacheIfNecessary(query.getType());
            //                wr.inc(fieldName, amount, multiple);
            //                ctx.addRequest(wr);
            morphium.firePostUpdateEvent(query.getType(), MorphiumStorageListener.UpdateTypes.INC);
        }, c, AsyncOperationType.INC);
        return null;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> void inc(final T obj, final String collection, final String field, final Number amount, AsyncOperationCallback<T> c) {
        if (c == null) {
            c = new AsyncOpAdapter<>();
        }

        morphium.inc(StatisticKeys.WRITES_CACHED);
        addToWriteQueue(obj.getClass(), collection, ctx -> {
            //                directWriter.set(toSet, collection, field, value, upsert,
            // multiple, callback);
            morphium.firePreUpdateEvent(obj.getClass(), MorphiumStorageListener.UpdateTypes.INC);
            Query q = morphium.createQueryFor(obj.getClass()).f(morphium.getARHelper().getIdFieldName(obj)).eq(morphium.getARHelper().getId(obj));
            q.setCollectionName(collection);
            UpdateBulkRequest wr = ctx.addUpdateBulkRequest();
            String fieldName = morphium.getARHelper().getMongoFieldName(obj.getClass(), field);
            wr.setCmd(Doc.of("$inc", UtilsMap.of(fieldName, amount)));
            wr.setUpsert(false);
            wr.setMultiple(false);
            //noinspection unchecked
            wr.setQuery(Doc.of(q.toQueryObject()));
            morphium.getCache().clearCacheIfNecessary(obj.getClass());
            //                ctx.addRequest(wr);
            morphium.firePostUpdateEvent(obj.getClass(), MorphiumStorageListener.UpdateTypes.INC);
        }, c, AsyncOperationType.INC);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> void pop(final T obj, final String collection, final String field, final boolean first, AsyncOperationCallback<T> c) {
        if (c == null) {
            c = new AsyncOpAdapter<>();
        }

        morphium.inc(StatisticKeys.WRITES_CACHED);
        addToWriteQueue(obj.getClass(), collection, ctx -> {
            //                directWriter.set(toSet, collection, field, value, upsert,
            // multiple, callback);
            morphium.firePreUpdateEvent(obj.getClass(), MorphiumStorageListener.UpdateTypes.POP);
            Query q = morphium.createQueryFor(obj.getClass()).f(morphium.getARHelper().getIdFieldName(obj)).eq(morphium.getARHelper().getId(obj));
            UpdateBulkRequest wr = ctx.addUpdateBulkRequest();
            //noinspection unchecked
            wr.setQuery(Doc.of(q.toQueryObject()));
            wr.setUpsert(false);
            wr.setMultiple(false);
            morphium.getCache().clearCacheIfNecessary(obj.getClass());
            String fld = morphium.getARHelper().getMongoFieldName(obj.getClass(), field);
            wr.setCmd(Doc.of("$pop", UtilsMap.of(fld, first)));
            morphium.firePostUpdateEvent(obj.getClass(), MorphiumStorageListener.UpdateTypes.POP);
        }, c, AsyncOperationType.WRITE);
    }

    @Override
    public void setMorphium(Morphium m) {
        morphium = m;
        directWriter = m.getConfig().writerSettings().getWriter();

        if (housekeeping == null) {
            housekeeping = new Thread() {
                @SuppressWarnings("SynchronizeOnNonFinalField")
                @Override
                public void run() {
                    setName("BufferedWriter_thread");

                    while (running) {
                        try {
                            // processing and clearing write cache...
                            //                        synchronized (opLog) {
                            List < Class<?>> localBuffer = new ArrayList<>(opLog.keySet());

                            for (Class<?> clz : localBuffer) {
                                // single snapshot - a concurrent flush can remove the entry
                                // between two get() calls (same race as in addToWriteQueue)
                                List<WriteBufferEntry> queued = opLog.get(clz);

                                if (queued == null || queued.isEmpty()) {
                                    continue;
                                }

                                WriteBuffer w = morphium.getARHelper().getAnnotationFromHierarchy(clz, WriteBuffer.class);
                                int size = 0;
                                int timeout = morphium.getConfig().writerSettings().getWriteBufferTime();
                                //                                WriteBuffer.STRATEGY
                                // strategy = WriteBuffer.STRATEGY.JUST_WARN;

                                if (w != null) {
                                    size = w.size();
                                    timeout = w.timeout();
                                    //                                    strategy =
                                    // w.strategy();
                                }

                                if (lastRun.get(clz) != null && System.currentTimeMillis() - lastRun.get(clz) > timeout) {
                                    // timeout reached....
                                    runIt(clz);
                                    continue;
                                }

                                if (size > 0 && queued.size() >= size) {
                                    // size reached!
                                    runIt(clz);
                                    continue;
                                }

                                lastRun.putIfAbsent(clz, System.currentTimeMillis());
                            }

                            //                        }
                        } catch (Exception e) {
                            logger.info("Got exception during write buffer handling!", e);
                        }

                        try {
                            if (morphium != null) {
                                if (morphium.getConfig() == null) {
                                    running = false;
                                    break;
                                }

                                //noinspection BusyWait
                                Thread.sleep(morphium.getConfig().writerSettings().getWriteBufferTimeGranularity());
                            } else {
                                logger.warn("Morphium not set - assuming timeout of 1sec");
                                //noinspection BusyWait
                                Thread.sleep(1000);
                            }
                        } catch (InterruptedException e) {
                            // Interrupted - check running flag and exit if needed
                        }
                    }

                    // Flush any remaining data on shutdown
                    try {
                        List<Class<?>> remainingClasses = new ArrayList<>(opLog.keySet());
                        for (Class<?> clz : remainingClasses) {
                            List<WriteBufferEntry> remaining = opLog.get(clz);

                            if (remaining != null && !remaining.isEmpty()) {
                                logger.debug("Flushing remaining {} entries for {} on shutdown",
                                    remaining.size(), clz.getSimpleName());
                                runIt(clz);
                            }
                        }
                    } catch (Exception e) {
                        // Driver may already be closed, log and continue
                        logger.debug("Could not flush remaining entries on shutdown: {}", e.getMessage());
                    }
                }

                private void runIt(Class<?> clz) {
                    lastRun.put(clz, System.currentTimeMillis());
                    List<WriteBufferEntry> localQueue;
                    synchronized (opLog) {
                        localQueue = opLog.remove(clz);
                    }
                    flushQueueToMongo(localQueue);

                }
            };
            housekeeping.setDaemon(true);
            housekeeping.start();
        }

        m.addShutdownListener(m1 -> close());
    }

    @Override
    public <T> void remove(final List<T> lst, AsyncOperationCallback<T> c) {
        if (c == null) {
            c = new AsyncOpAdapter<>();
        }

        //        final AsyncOperationCallback<T> callback = c;
        morphium.inc(StatisticKeys.WRITES_CACHED);

        for (final T obj : lst) {
            remove(obj, null, c);
        }
    }

    @Override
    public <T> Map<String, Object> remove(final Query<T> q, boolean multiple, AsyncOperationCallback<T> c) {
        if (c == null) {
            c = new AsyncOpAdapter<>();
        }

        //        final AsyncOperationCallback<T> cb = c;
        morphium.inc(StatisticKeys.WRITES_CACHED);
        addToWriteQueue(q.getType(), q.getCollectionName(), ctx -> {
            morphium.firePreRemoveEvent(q);
            DeleteBulkRequest r = ctx.addDeleteBulkRequest();
            r.setQuery(Doc.of(q.toQueryObject()));
            r.setMultiple(multiple);
            morphium.firePostRemoveEvent(q);
        }, c, AsyncOperationType.REMOVE);
        return null;
    }

    @Override
    public <T> void remove(final T o, final String collection, AsyncOperationCallback<T> c) {
        if (c == null) {
            c = new AsyncOpAdapter<>();
        }

        morphium.inc(StatisticKeys.WRITES_CACHED);
        addToWriteQueue(o.getClass(), collection, ctx -> {
            Query<?> q = morphium.createQueryFor(o.getClass()).f(morphium.getARHelper().getIdFieldName(o)).eq(morphium.getARHelper().getId(o));

            if (collection != null) {
                q.setCollectionName(collection);
            }
            morphium.firePreRemoveEvent(q);
            DeleteBulkRequest r = ctx.addDeleteBulkRequest();
            //noinspection unchecked
            r.setQuery(Doc.of(q.toQueryObject()));
            //                ctx.addRequest(r);
            morphium.firePostRemoveEvent(q);
        }, c, AsyncOperationType.REMOVE);
    }

    @Override
    public <T> Map<String, Object> remove(final Query<T> q, AsyncOperationCallback<T> c) {
        remove(q, true, c);
        return null;
    }

    @Override
    public <T> Map<String, Object> pushPull(final MorphiumStorageListener.UpdateTypes type, final Query<T> q, final String field, final Object value, final boolean upsert, final boolean multiple,
                                            AsyncOperationCallback<T> c) {
        if (c == null) {
            c = new AsyncOpAdapter<>();
        }

        morphium.inc(StatisticKeys.WRITES_CACHED);
        addToWriteQueue(q.getType(), q.getCollectionName(), ctx -> {
            UpdateBulkRequest r = ctx.addUpdateBulkRequest();
            r.setQuery(Doc.of(q.toQueryObject()));
            r.setUpsert(upsert);
            r.setMultiple(multiple);

            morphium.getCache().clearCacheIfNecessary(q.getType());
            // String fld = morphium.getARHelper().getMongoFieldName(q.getType(), field);
            switch (type) {
                case PUSH:
                    morphium.firePreUpdateEvent(q.getType(), MorphiumStorageListener.UpdateTypes.PUSH);
                    r.setCmd(Doc.of("$push", Doc.of(field, value)));
                    morphium.firePostUpdateEvent(q.getType(), MorphiumStorageListener.UpdateTypes.PUSH);
                    break;

                case PULL:
                    morphium.firePreUpdateEvent(q.getType(), MorphiumStorageListener.UpdateTypes.PULL);
                    r.setCmd(Doc.of("$pull", Doc.of(field, value)));
                    morphium.firePostUpdateEvent(q.getType(), MorphiumStorageListener.UpdateTypes.PULL);
                    break;

                case ADD_TO_SET:
                    morphium.firePreUpdateEvent(q.getType(), MorphiumStorageListener.UpdateTypes.ADD_TO_SET);
                    r.setCmd(Doc.of("$addToSet", Doc.of(field, value)));
                    morphium.firePostUpdateEvent(q.getType(), MorphiumStorageListener.UpdateTypes.ADD_TO_SET);
                    break;

                default:
                    throw new IllegalArgumentException("Unsupported push type " + type.name());
            }

            //                ctx.addRequest(r);
        }, c, type.equals(MorphiumStorageListener.UpdateTypes.PUSH) ? AsyncOperationType.PUSH : AsyncOperationType.PULL);
        return null;
    }

    @Override
    public <T> Map<String, Object> pushPullAll(final MorphiumStorageListener.UpdateTypes type, final Query<T> q, final String field, final List<?> value, final boolean upsert, final boolean multiple,
            AsyncOperationCallback<T> c) {
        if (c == null) {
            c = new AsyncOpAdapter<>();
        }

        morphium.inc(StatisticKeys.WRITES_CACHED);
        addToWriteQueue(q.getType(), q.getCollectionName(), ctx -> {
            //                directWriter.pushPull(push, query, field, value, upsert,
            // multiple, callback);
            morphium.getCache().clearCacheIfNecessary(q.getType());
            String fld = morphium.getARHelper().getMongoFieldName(q.getType(), field);
            morphium.firePreUpdateEvent(q.getType(), type);

            for (Object o : value) {
                UpdateBulkRequest r = ctx.addUpdateBulkRequest();
                r.setQuery(Doc.of(q.toQueryObject()));
                r.setUpsert(upsert);
                r.setMultiple(multiple);

                switch (type) {
                    case PUSH:
                        r.setCmd(Doc.of("$push", Doc.of(fld, o)));
                        break;

                    case PULL:
                        r.setCmd(Doc.of("$push", Doc.of(fld, o)));
                        break;

                    case ADD_TO_SET:
                        r.setCmd(Doc.of("$addToSet", Doc.of(fld, o)));
                        break;

                    default:
                        throw new IllegalArgumentException("Unsupported operation " + type.name());
                }

                //                        ctx.addRequest(r);
            }
            morphium.firePostUpdateEvent(q.getType(), type);
        }, c, type.equals(MorphiumStorageListener.UpdateTypes.PULL) ? AsyncOperationType.PULL : AsyncOperationType.PUSH);
        return null;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> void unset(final T obj, final String collection, final String field, AsyncOperationCallback<T> c) {
        if (c == null) {
            c = new AsyncOpAdapter<>();
        }

        morphium.inc(StatisticKeys.WRITES_CACHED);
        addToWriteQueue(obj.getClass(), collection, ctx -> {
            //                directWriter.set(toSet, collection, field, value, upsert,
            // multiple, callback);
            Query q = morphium.createQueryFor(obj.getClass()).f(morphium.getARHelper().getIdFieldName(obj)).eq(morphium.getARHelper().getId(obj));

            if (collection != null) {
                q.setCollectionName(collection);
            }
            UpdateBulkRequest wr = ctx.addUpdateBulkRequest();
            //noinspection unchecked
            wr.setQuery(Doc.of(q.toQueryObject()));
            wr.setMultiple(false);
            wr.setUpsert(false);
            String fld = morphium.getARHelper().getMongoFieldName(obj.getClass(), field);
            wr.setCmd(Doc.of("$unset", Doc.of(fld, "")));
            morphium.getCache().clearCacheIfNecessary(obj.getClass());
            //                ctx.addRequest(wr);
        }, c, AsyncOperationType.UNSET);
    }

    @Override
    public <T> Map<String, Object> unset(final Query<T> query, final String field, final boolean multiple, AsyncOperationCallback<T> c) {
        if (c == null) {
            c = new AsyncOpAdapter<>();
        }

        //        final AsyncOperationCallback<T> callback = c;
        morphium.inc(StatisticKeys.WRITES_CACHED);
        addToWriteQueue(query.getType(), query.getCollectionName(), ctx -> {
            //                directWriter.set(toSet, collection, field, value, upsert,
            // multiple, callback);
            morphium.firePreUpdateEvent(query.getType(), MorphiumStorageListener.UpdateTypes.UNSET);
            UpdateBulkRequest wr = ctx.addUpdateBulkRequest();
            wr.setQuery(Doc.of(query.toQueryObject()));
            wr.setMultiple(multiple);
            wr.setUpsert(false);
            String fld = morphium.getARHelper().getMongoFieldName(query.getType(), field);
            wr.setCmd(Doc.of("$unset", Doc.of(fld, "")));
            //                ctx.addRequest(wr);
            morphium.getCache().clearCacheIfNecessary(query.getType());
            morphium.firePostUpdateEvent(query.getType(), MorphiumStorageListener.UpdateTypes.UNSET);
        }, c, AsyncOperationType.UNSET);
        return null;
    }

    @SuppressWarnings("CommentedOutCode")
    @Override
    public <T> Map<String, Object> unset(final Query<T> query, AsyncOperationCallback<T> c, final boolean multiple, final String... fields) {
        if (c == null) {
            c = new AsyncOpAdapter<>();
        }

        morphium.inc(StatisticKeys.WRITES_CACHED);
        addToWriteQueue(query.getType(), query.getCollectionName(), ctx -> {
            //                directWriter.set(toSet, collection, field, value, upsert,
            // multiple, callback);
            morphium.firePreUpdateEvent(query.getType(), MorphiumStorageListener.UpdateTypes.UNSET);
            UpdateBulkRequest wr = ctx.addUpdateBulkRequest();
            wr.setQuery(Doc.of(query.toQueryObject()));
            wr.setMultiple(multiple);
            wr.setUpsert(false);
            Doc unset = new Doc();
            wr.setCmd(Doc.of("$unset", unset));
            //                ctx.addRequest(wr);

            //                BulkRequestWrapper wr = ctx.addFind(query);
            for (String f : fields) {
                String fld = morphium.getARHelper().getMongoFieldName(query.getType(), f);
                unset.put(fld, "");
            }
            morphium.getCache().clearCacheIfNecessary(query.getType());
            morphium.firePostUpdateEvent(query.getType(), MorphiumStorageListener.UpdateTypes.UNSET);
        }, c, AsyncOperationType.UNSET);
        return null;
    }

    @Override
    public <T> Map<String, Object> unset(final Query<T> query, AsyncOperationCallback<T> c, final boolean multiple, final Enum... fields) {
        String[] flds = new String[fields.length];
        int i = 0;

        for (Enum e : fields) {
            flds[i++] = e.name();
        }

        unset(query, c, multiple, flds);
        return null;
    }

    @Override
    public <T> void dropCollection(final Class<T> cls, final String collection, AsyncOperationCallback<T> c) {
        if (c == null) {
            c = new AsyncOpAdapter<>();
        }

        final AsyncOperationCallback<T> callback = c;
        morphium.inc(StatisticKeys.WRITES_CACHED);
        addToWriteQueue(cls, collection, ctx -> {
            directWriter.dropCollection(cls, collection, callback);
            morphium.getCache().clearCacheIfNecessary(cls);
        }, c, AsyncOperationType.REMOVE);
    }

    @SuppressWarnings("unused")
    public <T> void ensureIndex(final Class<T> cls, final String collection, final Map<String, Object> index, AsyncOperationCallback<T> c) {
        createIndex(cls, collection, IndexDescription.fromMaps(index, null), c);
    }

    @Override
    public <T> void createIndex(final Class<T> cls, final String collection, final IndexDescription index, AsyncOperationCallback<T> c) {
        // Index creation is a schema operation; buffering it can deadlock under load (it may enqueue work
        // that in turn needs writer threads to progress, while tests wait for the buffer to drain).
        // Run it directly through the underlying writer.
        directWriter.createIndex(cls, collection, index, c);
    }

    @Override
    public int writeBufferCount() {
        int cnt = 0;

        //        synchronized (opLog) {
        for (List<WriteBufferEntry> lst : opLog.values()) {
            cnt += lst.size();
        }

        //        }
        return cnt;
    }

    @Override
    public <T> void store(final List<T> lst, AsyncOperationCallback<T> c) {
        store(lst, null, c);
    }

    @Override
    public void flush() {
        List<Class<?>> localBuffer = new ArrayList<>(opLog.keySet());

        for (Class<?> clz : localBuffer) {
            // Atomically take ownership of the queue for this class.
            // ConcurrentHashMap.remove() is atomic: if two threads call flush()
            // concurrently (e.g. explicit saveList flush + background housekeeping thread),
            // only one will receive the list — the other gets null and skips.
            // This prevents both threads from processing the same entries and causing
            // duplicate-key errors on insert.
            List<WriteBufferEntry> queue;
            synchronized (opLog) {
                queue = opLog.remove(clz);
            }
            if (queue == null || queue.isEmpty()) {
                continue;
            }

            flushQueueToMongo(queue);

        }
    }

    @Override
    public void flush(Class type) {
        List<WriteBufferEntry> queue;
        synchronized (opLog) {
            queue = opLog.remove(type);
        }
        if (queue == null || queue.isEmpty()) {
            return;
        }

        flushQueueToMongo(queue);

    }

    @Override
    public void onShutdown(Morphium m) {
        logger.debug("Stopping housekeeping thread");
        running = false;
        flush();
    }

    @Override
    public void setMaximumQueingTries(int n) {
        directWriter.setMaximumQueingTries(n);
    }

    @Override
    public void setPauseBetweenTries(int p) {
        directWriter.setPauseBetweenTries(p);
    }

    private interface BufferedBulkOp {
        void queue(BulkRequestContext ctx);
    }

    private static class WriteBufferEntry {
        private final String collection;
        private BufferedBulkOp toRun;
        private AsyncOperationCallback cb;
        private AsyncOperationType type;
        private long timestamp;
        private Class entityType;
        private volatile Throwable failure;

        private WriteBufferEntry(Class entitiyType, String collectionName, BufferedBulkOp toRun, long timestamp, AsyncOperationCallback c, AsyncOperationType t) {
            this.toRun = toRun;
            this.timestamp = timestamp;
            this.cb = c;
            this.type = t;
            this.entityType = entitiyType;
            this.collection = collectionName;
        }

        public String getCollectionName() {
            return collection;
        }

        public Class getEntityType() {
            return entityType;
        }

        @SuppressWarnings("unused")
        public void setEntityType(Class entityType) {
            this.entityType = entityType;
        }

        public AsyncOperationType getType() {
            return type;
        }

        @SuppressWarnings("unused")
        public void setType(AsyncOperationType type) {
            this.type = type;
        }

        public AsyncOperationCallback getCb() {
            return cb;
        }

        @SuppressWarnings("unused")
        public void setCb(AsyncOperationCallback cb) {
            this.cb = cb;
        }

        public BufferedBulkOp getToRun() {
            return toRun;
        }

        private boolean isFailed() {
            return failure != null;
        }

        private void markFailed(Throwable failure) {
            this.failure = failure;
        }

        @SuppressWarnings("unused")
        public void setToRun(BufferedBulkOp toRun) {
            this.toRun = toRun;
        }

        public long getTimestamp() {
            return timestamp;
        }

        @SuppressWarnings("unused")
        public void setTimestamp(long timestamp) {
            this.timestamp = timestamp;
        }
    }

    @Override
    public <T> void set(T toSet, String collection, Map<String, Object> values, boolean upsert, AsyncOperationCallback<T> callback) {
        @SuppressWarnings("unchecked")
        Query<T> id = morphium.createQueryFor((Class<T>) toSet.getClass()).f("_id").eq(morphium.getId(toSet));
        set(id, values, upsert, false, callback);
    }

    private static class AsyncOpAdapter<T> implements AsyncOperationCallback<T> {

        @Override
        public void onOperationSucceeded(AsyncOperationType type, Query<T> q, long duration, List<T> result, T entity, Object... param) {
        }

        @Override
        public void onOperationError(AsyncOperationType type, Query<T> q, long duration, String error, Throwable t, T entity, Object... param) {
        }
    }

    @Override
    public <T> List<FailedStore<T>> storeList(List<T> lst, String collectionName, boolean continueOnError) {
        return directWriter.storeList(lst, collectionName, continueOnError);
    }

    @Override
    public <T> Map<String, Object> explainRemove(ExplainVerbosity verbosity, T o, String Collection) {
        return directWriter.explainRemove(verbosity, o, Collection);
    }

    @Override
    public <T> Map<String, Object> explainRemove(ExplainVerbosity verbosity, Query<T> q) {
        return directWriter.explainRemove(verbosity, q);
    }
}
