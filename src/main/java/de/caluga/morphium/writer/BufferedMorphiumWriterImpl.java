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

import java.lang.reflect.Field;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.stream.Stream;

/**
 * User: Stephan Bösebeck Date: 11.03.13 Time: 11:41
 *
 * <p>Buffered Writer buffers all write requests (store, update, remove...) to mongo for a certain
 * time. After that time the requests are issued en block to mongo. Attention: this is not using
 * BULK-Requests yet!
 */
@SuppressWarnings({"EmptyCatchBlock", "SynchronizeOnNonFinalField", "WeakerAccess", "BusyWait"})
public class BufferedMorphiumWriterImpl implements MorphiumWriter, ShutdownListener {

    private static final Logger logger = LoggerFactory.getLogger(BufferedMorphiumWriterImpl.class);
    // needs to be securely stored
    private final Map<Class<?>, List<WriteBufferEntry>> opLog = new ConcurrentHashMap<>(); // synced
    private final Map<Class<?>, Long> lastRun = new ConcurrentHashMap<>();
    private Morphium morphium;
    private MorphiumWriter directWriter;
    private Thread housekeeping;
    private boolean running = true;
    private boolean orderedExecution = false;

    public BufferedMorphiumWriterImpl() {
    }

    public void close() {
        running = false;

        try {
            long start = System.currentTimeMillis();

            while (housekeeping.isAlive()) {
                if (System.currentTimeMillis() - start > 1000) {
                    //noinspection deprecation
                    housekeeping.stop();
                    break;
                }

                //noinspection BusyWait
                Thread.sleep(50);
            }
        } catch (Exception e) {
            // swallow on shutdown
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

    private void createCappedColl(Class c) {
        createCappedColl(c, null);
    }

    private void createCappedColl(Class c, String n) {
        if (logger.isDebugEnabled()) {
            logger.debug("Collection does not exist - ensuring indices / capped status");
        }

        Capped capped = morphium.getARHelper().getAnnotationFromHierarchy(c, Capped.class);

        if (capped == null) { return; }

        MongoConnection primaryConnection = null;

        try {
            primaryConnection = morphium.getDriver().getPrimaryConnection(null);
            CreateCommand cmd = new CreateCommand(primaryConnection);
            cmd.setCapped(true);
            cmd.setSize(capped.maxSize()).setMax(capped.maxEntries());
            cmd.setDb(morphium.getConfig().getDatabase());
            primaryConnection.sendCommand(cmd);
        } catch (MorphiumDriverException e) {
            // TODO: Implement Handling
            throw new RuntimeException(e);
        } finally {
            if (primaryConnection != null) { primaryConnection.release(); }
        }
    }

    @SuppressWarnings("CommentedOutCode")
    private void flushQueueToMongo(List<WriteBufferEntry> q) {
        if (q == null) {
            return;
        }

        List<WriteBufferEntry> localQueue = new ArrayList<>(q);
        // either buffer size reached, or time is up => queue writes
        List<WriteBufferEntry> didNotWrite = new ArrayList<>();
        Map<String, BulkRequestContext> bulkByCollectionName = new HashMap<>();

        try {
            for (WriteBufferEntry entry : localQueue) {
                if (morphium.getConfig().isAutoIndexAndCappedCreationOnWrite()
                 && !morphium.exists(morphium.getConfig().getDatabase(), entry.getCollectionName())) {
                    createCappedColl(entry.getEntityType(), entry.getCollectionName());
                    //noinspection unchecked,unchecked
                    morphium.ensureIndicesFor(entry.getEntityType(),
                     entry.getCollectionName(),
                     entry.getCb(),
                     directWriter);
                }

                try {
                    if (bulkByCollectionName.get(entry.getCollectionName()) == null) {
                        WriteBuffer w =
                         morphium.getARHelper()
                         .getAnnotationFromHierarchy(entry.getEntityType(), WriteBuffer.class);
                        bulkByCollectionName.put(entry.getCollectionName(),
                         morphium.getDriver()
                         .createBulkContext(morphium,
                         morphium.getConfig().getDatabase(),
                         entry.getCollectionName(),
                         w.ordered(),
                         morphium.getWriteConcernForClass(entry.getEntityType())));
                    }

                    //                logger.info("Queueing a request of type "+entry.getType());
                    entry.getToRun().queue(bulkByCollectionName.get(entry.getCollectionName()));
                    //noinspection unchecked
                    entry.getCb().onOperationSucceeded(entry.getType(), null, 0, null, null);
                } catch (RejectedExecutionException e) {
                    logger.info("too much load - add write to next run");
                    didNotWrite.add(entry);
                } catch (Exception e) {
                    logger.error("could not write", e);
                }
            }
        } catch (MorphiumDriverException ex) {
            logger.error("Got error during write!", ex);
            throw new RuntimeException(ex);
        }

        try {
            for (BulkRequestContext ctx : bulkByCollectionName.values()) {
                if (ctx != null) {
                    ctx.execute();
                }
            }
        } catch (Exception e) {
            logger.error("Error during exeecution of unordered bulk", e);
        }

        for (WriteBufferEntry entry : localQueue) {
            morphium.clearCacheforClassIfNecessary(entry.getEntityType());

            if (!didNotWrite.contains(entry)) {
                q.remove(entry);
            }
        }
    }

    public void addToWriteQueue(
        Class<?> type,
        String collectionName,
        BufferedBulkOp r,
        AsyncOperationCallback c,
        AsyncOperationType t) {
        if (collectionName == null) {
            collectionName = morphium.getMapper().getCollectionName(type);
        }

        WriteBufferEntry wb =
         new WriteBufferEntry(type, collectionName, r, System.currentTimeMillis(), c, t);
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

        //        synchronized (opLog) {
        if (opLog.get(type) == null) {
            synchronized (opLog) {
                opLog.putIfAbsent(type, new Vector<>());
            }
        }

        if (size > 0 && opLog.get(type) != null && opLog.get(type).size() > size) {
            logger.warn("WARNING: Write buffer for type " + type.getName() + " maximum exceeded: " + opLog.get(type).size() + " entries now, max is " + size);
            BulkRequestContext ctx =
             morphium.getDriver()
             .createBulkContext(morphium,
              morphium.getConfig().getDatabase(),
              collectionName,
              ordered,
              morphium.getWriteConcernForClass(type));

            synchronized (opLog) {
                if (opLog.get(type) == null) {
                    opLog.putIfAbsent(type, Collections.synchronizedList(new ArrayList<>()));
                }

                switch (strategy) {
                case WAIT:
                    long start = System.currentTimeMillis();

                    while (true) {
                        int timeout = w.timeout();

                        if (morphium.getConfig().getMaxWaitTime() > 0
                         && morphium.getConfig().getMaxWaitTime() > timeout) {
                            timeout = morphium.getConfig().getMaxWaitTime();
                        }

                        if (timeout > 0 && System.currentTimeMillis() - start > timeout) {
                            logger.error("Could not write - maxWaitTime/timeout exceeded!");
                            throw new RuntimeException(
                                "could now write - maxWaitTimeExceded "
                                + morphium.getConfig().getMaxWaitTime()
                                + "ms");
                        }

                        try {
                            Thread.sleep(morphium.getConfig().getIdleSleepTime());
                        } catch (InterruptedException e1) {
                        }

                        try {
                            if (opLog.get(type) == null || opLog.get(type).size() < size) {
                                break;
                            }
                        } catch (NullPointerException e) {
                            // Can happen - Multithreadded acces...
                            break;
                        }
                    }

                    if (opLog.get(type) == null) {
                        opLog.putIfAbsent(type, Collections.synchronizedList(new ArrayList<>()));
                    }

                case JUST_WARN:
                    opLog.get(type).add(wb);
                    break;

                case IGNORE_NEW:
                    logger.warn("ignoring new incoming...");
                    return;

                case WRITE_NEW:
                    logger.warn("directly writing data... due to strategy setting");
                    r.queue(ctx);
                    //                        ctx.execute();
                    break;

                case WRITE_OLD:
                    opLog.get(type)
                    .sort(Comparator.comparingLong(WriteBufferEntry::getTimestamp));

                    // could have been written in the meantime
                    synchronized (opLog) {
                        if (opLog.get(type) != null && !opLog.get(type).isEmpty()) {
                            WriteBufferEntry e = opLog.get(type).remove(0);
                            e.getToRun().queue(ctx);
                        }

                        opLog.putIfAbsent(type, new ArrayList<>());
                        opLog.get(type).add(wb);
                    }

                    break;

                case DEL_OLD:
                    opLog.get(type).sort(Comparator.comparingLong(WriteBufferEntry::getTimestamp));

                    if (logger.isDebugEnabled()) {
                        logger.debug("Deleting oldest entry");
                    }

                    synchronized (opLog) {
                        if (opLog.get(type) != null && !opLog.get(type).isEmpty()) {
                            opLog.get(type).remove(0);
                        }

                        opLog.putIfAbsent(type, new ArrayList<>());
                        opLog.get(type).add(wb);
                    }

                    return;
                }
            }

            try {
                ctx.execute();
            } catch (MorphiumDriverException e) {
                throw new RuntimeException(e);
            }
        } else {
            opLog.get(type).add(wb);
        }

        //        }
    }

    @Override
    public <T> void insert(T o, String collection, AsyncOperationCallback<T> c) {
        if (c == null) {
            c = new AsyncOpAdapter<>();
        }

        morphium.inc(StatisticKeys.WRITES_CACHED);
        addToWriteQueue(o.getClass(),
         collection,
         ctx->{
            // do nothing
            morphium.firePreStore(o, true);
            ArrayList<Map<String, Object>> objToInsert = new ArrayList<>();

            try {
                setIdIfNull(o);
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }
            objToInsert.add(Doc.of(morphium.getMapper().serialize(o)));
            InsertBulkRequest ins = ctx.addInsertBulkRequest(objToInsert);

            //                morphium.clearCacheforClassIfNecessary(o.getClass());
            morphium.firePostStore(o, true);
        },
         c,
         AsyncOperationType.WRITE);
    }

    private <T> void setIdIfNull(T o) throws IllegalAccessException {
        Field idf = morphium.getARHelper().getIdField(o);

        if (idf.get(o) != null) { return; }

        if (idf.get(o) == null && idf.getType().equals(MorphiumId.class)) {
            idf.set(o, new MorphiumId());
        } else if (idf.get(o) == null && idf.getType().equals(String.class)) {
            idf.set(o, new MorphiumId().toString());
        } else {
            throw new RuntimeException("Cannot set ID");
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

        if (c == null) {
            c = new AsyncOpAdapter<>();
        }

        morphium.inc(StatisticKeys.WRITES_CACHED);

        if (morphium.isAutoValuesEnabledForThread()) {
            for (T obj : lst) {
                try {
                    morphium.setAutoValues(obj);
                } catch (IllegalAccessException e) {
                    logger.error("Could not set auto variables", e);
                }
            }
        }

        final AsyncOperationCallback<T> finalC = c;
        addToWriteQueue(lst.get(0).getClass(),
         collectionName,
         ctx->{
            Map<Object, Boolean> map = new HashMap<>();
            List<Map<String, Object>> marshalled = new ArrayList<>();

            for (T o : lst) {
                map.put(o, true);

                try {
                    setIdIfNull(o);
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                }

                marshalled.add(Doc.of(morphium.getMapper().serialize(o)));
            }
            morphium.firePreStore(map);

            ctx.addInsertBulkRequest(marshalled);
            morphium.firePostStore(map);
        },
         c,
         AsyncOperationType.WRITE);
    }

    @Override
    public <T> void store(final T o, final String collection, AsyncOperationCallback<T> c) {
        if (c == null) {
            c = new AsyncOpAdapter<>();
        }

        morphium.inc(StatisticKeys.WRITES_CACHED);
        addToWriteQueue(o.getClass(),
         collection,
         ctx->{
            // do nothing
            boolean isNew = morphium.getARHelper().getId(o) == null;

            if (!isNew
            && !morphium.getARHelper()
            .getIdField(o)
            .getType()
            .equals(MorphiumId.class)) {
                // need to check if type is not MongoId
                isNew =
                (morphium.createQueryFor(o.getClass())
                .f("_id")
                .eq(morphium.getId(o))
                .countAll()
                == 0);
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
                up.setQuery(Doc.of((morphium.createQueryFor(o.getClass())
                .f(morphium.getARHelper().getIdFieldName(o))
                .eq(morphium.getARHelper().getId(o)))
                .toQueryObject()));
                Doc cmd = new Doc();
                up.setCmd(Doc.of("$set", cmd));

                //noinspection unchecked
                for (String f : morphium.getARHelper().getFields(o.getClass())) {
                    try {
                        Object serialize = null;
                        Field field = morphium.getARHelper().getField(o.getClass(), f);

                        if (field.getType().getName().startsWith("java.lang")
                        || field.getType().isPrimitive()
                        || MorphiumId.class.isAssignableFrom(field.getType())) {
                            if (!(Map.class.isAssignableFrom(field.getType()))
                            && !(Map.class.isAssignableFrom(field.getType()))
                            && !field.getType().isArray()) {
                                serialize = field.get(o);
                            }
                        } else if (field.get(o) instanceof Collection) {
                            serialize = handleList((Collection) field.get(o));
                        }

                        if (serialize == null) {
                            serialize = morphium.getMapper().serialize(field.get(o));
                        }

                        cmd.put(morphium.getARHelper().getMongoFieldName(o.getClass(), f),
                        serialize);
                    } catch (IllegalAccessException e) {
                        e.printStackTrace();
                    }
                }
            }
            //                morphium.clearCacheforClassIfNecessary(o.getClass());
            morphium.firePostStore(o, isNew);
        },
         c,
         AsyncOperationType.WRITE);
    }

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

    @Override
    public <T> void store(
        final List<T> lst, final String collectionName, AsyncOperationCallback<T> c) {
        if (lst == null || lst.isEmpty()) {
            if (c != null) {
                c.onOperationSucceeded(AsyncOperationType.WRITE, null, 0, lst, null);
            }

            return;
        }

        if (c == null) {
            c = new AsyncOpAdapter<>();
        }

        morphium.inc(StatisticKeys.WRITES_CACHED);

        if (morphium.isAutoValuesEnabledForThread()) {
            for (T obj : lst) {
                try {
                    morphium.setAutoValues(obj);
                } catch (IllegalAccessException e) {
                    logger.error("Could not set auto variables", e);
                }
            }
        }

        final AsyncOperationCallback<T> finalC = c;
        addToWriteQueue(lst.get(0).getClass(),
         collectionName,
         ctx->{
            Map<Object, Boolean> map = new HashMap<>();

            for (T o : lst) {
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
                    store((T) entry.getKey(),
                    morphium.getMapper()
                    .getCollectionName(entry.getKey().getClass()),
                    finalC);
                }
            }
            ctx.addInsertBulkRequest(toInsert);
            morphium.firePostStore(map);
        },
         c,
         AsyncOperationType.WRITE);
    }

    @Override
    public <T> void updateUsingFields(
        final T ent,
        final String collection,
        AsyncOperationCallback<T> c,
        final String... fields) {
        if (c == null) {
            c = new AsyncOpAdapter<>();
        }

        //        final AsyncOperationCallback<T> callback = c;
        morphium.inc(StatisticKeys.WRITES_CACHED);
        addToWriteQueue(ent.getClass(),
         collection,
         ctx->{
            //                directWriter.updateUsingFields(ent, collection, callback,
            // fields);
            morphium.firePreUpdateEvent(ent.getClass(), MorphiumStorageListener.UpdateTypes.SET);
            @SuppressWarnings("unchecked")
            Query<Object> query =
            (Query<Object>)
            morphium.createQueryFor(ent.getClass())
            .f(morphium.getARHelper().getIdFieldName(ent))
            .eq(morphium.getARHelper().getId(ent));

            if (collection != null) {
                query.setCollectionName(collection);
            }
            //                BulkRequestWrapper r = ctx.addFind(query);
            String[] flds = fields;

            if (flds.length == 0) {
                Stream<Field> stream =
                morphium.getARHelper().getAllFields(ent.getClass()).stream();
                Stream<String> m = stream.map(Field::getName);
                flds = m.toArray(String[] ::new);
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
        },
         c,
         AsyncOperationType.UPDATE);
    }

    @Override
    public <T> Map<String, Object> set(final Query<T> query, final Map<String, Object> values, final boolean upsert, final boolean multiple,
     AsyncOperationCallback<T> c) {
        if (c == null) {
            c = new AsyncOpAdapter<>();
        }

        //        final AsyncOperationCallback<T> callback = c;
        morphium.inc(StatisticKeys.WRITES_CACHED);
        addToWriteQueue(query.getType(),
         query.getCollectionName(),
         ctx->{
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
        addToWriteQueue(query.getType(),
         query.getCollectionName(),
         ctx->{
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
                String fld =
                morphium.getARHelper()
                .getMongoFieldName(query.getType(), kv.getKey().toString());
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
        addToWriteQueue(query.getType(),
         query.getCollectionName(),
         ctx->{
            //                directWriter.set(toSet, collection, field, value, upsert,
            // multiple, callback);
            UpdateBulkRequest wr = ctx.addUpdateBulkRequest();
            String fieldName =
            morphium.getARHelper().getMongoFieldName(query.getType(), field);
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

    @Override
    public <T> void inc(final T obj, final String collection, final String field, final Number amount,
     AsyncOperationCallback<T> c) {
        if (c == null) {
            c = new AsyncOpAdapter<>();
        }

        morphium.inc(StatisticKeys.WRITES_CACHED);
        addToWriteQueue(obj.getClass(),
         collection,
         ctx->{
            //                directWriter.set(toSet, collection, field, value, upsert,
            // multiple, callback);
            morphium.firePreUpdateEvent(obj.getClass(), MorphiumStorageListener.UpdateTypes.INC);
            Query q =
            morphium.createQueryFor(obj.getClass())
            .f(morphium.getARHelper().getIdFieldName(obj))
            .eq(morphium.getARHelper().getId(obj));
            q.setCollectionName(collection);
            UpdateBulkRequest wr = ctx.addUpdateBulkRequest();
            String fieldName =
            morphium.getARHelper().getMongoFieldName(obj.getClass(), field);
            wr.setCmd(Doc.of("$inc", UtilsMap.of(fieldName, amount)));
            wr.setUpsert(false);
            wr.setMultiple(false);
            //noinspection unchecked
            wr.setQuery(Doc.of(q.toQueryObject()));
            morphium.getCache().clearCacheIfNecessary(obj.getClass());
            //                ctx.addRequest(wr);
            morphium.firePostUpdateEvent(obj.getClass(), MorphiumStorageListener.UpdateTypes.INC);
        },
         c,
         AsyncOperationType.INC);
    }

    @Override
    public <T> void pop(final T obj, final String collection, final String field, final boolean first, AsyncOperationCallback<T> c) {
        if (c == null) {
            c = new AsyncOpAdapter<>();
        }

        morphium.inc(StatisticKeys.WRITES_CACHED);
        addToWriteQueue(obj.getClass(),
         collection,
         ctx->{
            //                directWriter.set(toSet, collection, field, value, upsert,
            // multiple, callback);
            morphium.firePreUpdateEvent(obj.getClass(), MorphiumStorageListener.UpdateTypes.POP);
            Query q =
            morphium.createQueryFor(obj.getClass())
            .f(morphium.getARHelper().getIdFieldName(obj))
            .eq(morphium.getARHelper().getId(obj));
            UpdateBulkRequest wr = ctx.addUpdateBulkRequest();
            //noinspection unchecked
            wr.setQuery(Doc.of(q.toQueryObject()));
            wr.setUpsert(false);
            wr.setMultiple(false);
            morphium.getCache().clearCacheIfNecessary(obj.getClass());
            String fld = morphium.getARHelper().getMongoFieldName(obj.getClass(), field);
            wr.setCmd(Doc.of("$pop", UtilsMap.of(fld, first)));
            morphium.firePostUpdateEvent(obj.getClass(), MorphiumStorageListener.UpdateTypes.POP);
        },
         c,
         AsyncOperationType.WRITE);
    }

    @Override
    public void setMorphium(Morphium m) {
        morphium = m;
        directWriter = m.getConfig().getWriter();

        if (housekeeping == null) {
            housekeeping =
             new Thread() {
                @SuppressWarnings("SynchronizeOnNonFinalField")
                @Override
                public void run() {
                    setName("BufferedWriter_thread");

                    while (running) {
                        try {
                            // processing and clearing write cache...
                            //                        synchronized (opLog) {
                            List<Class<?>> localBuffer = new ArrayList<>(opLog.keySet());

                            for (Class<?> clz : localBuffer) {
                                if (opLog.get(clz) == null || opLog.get(clz).isEmpty()) {
                                    continue;
                                }

                                WriteBuffer w =
                                 morphium.getARHelper()
                                 .getAnnotationFromHierarchy(clz, WriteBuffer.class);
                                int size = 0;
                                int timeout = morphium.getConfig().getWriteBufferTime();
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

                                // can't be null
                                if (size > 0 && opLog.get(clz) != null && opLog.get(clz).size() >= size) {
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
                                Thread.sleep(morphium.getConfig()
                                 .getWriteBufferTimeGranularity());
                            } else {
                                logger.warn("Morphium not set - assuming timeout of 1sec");
                                //noinspection BusyWait
                                Thread.sleep(1000);
                            }
                        } catch (InterruptedException e) {
                        }
                    }
                }
                private void runIt(Class<?> clz) {
                    lastRun.put(clz, System.currentTimeMillis());
                    List<WriteBufferEntry> localQueue;
                    localQueue = opLog.remove(clz);
                    //                            opLog.put(clz, new
                    // Vector<WriteBufferEntry>());
                    flushQueueToMongo(localQueue);

                    if (localQueue != null && !localQueue.isEmpty()) {
                        if (opLog.get(clz) == null) {
                            synchronized (opLog) {
                                opLog.putIfAbsent(clz,
                                 Collections.synchronizedList(new ArrayList<>()));
                            }
                        }

                        opLog.get(clz).addAll(localQueue);
                    }
                }
            };
            housekeeping.setDaemon(true);
            housekeeping.start();
        }

        m.addShutdownListener(m1->close());
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
        addToWriteQueue(q.getType(),
         q.getCollectionName(),
         ctx->{
            morphium.firePreRemoveEvent(q);
            DeleteBulkRequest r = ctx.addDeleteBulkRequest();
            r.setQuery(Doc.of(q.toQueryObject()));
            //                    ctx.addRequest(r);
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
        addToWriteQueue(o.getClass(),
         collection,
         ctx->{
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
        },
         c,
         AsyncOperationType.REMOVE);
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
        addToWriteQueue(q.getType(),
         q.getCollectionName(),
         ctx->{
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
        addToWriteQueue(q.getType(),
         q.getCollectionName(),
         ctx->{
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

    @Override
    public <T> void unset(final T obj, final String collection, final String field, AsyncOperationCallback<T> c) {
        if (c == null) {
            c = new AsyncOpAdapter<>();
        }

        morphium.inc(StatisticKeys.WRITES_CACHED);
        addToWriteQueue(obj.getClass(),
         collection,
         ctx->{
            //                directWriter.set(toSet, collection, field, value, upsert,
            // multiple, callback);
            Query q =
            morphium.createQueryFor(obj.getClass())
            .f(morphium.getARHelper().getIdFieldName(obj))
            .eq(morphium.getARHelper().getId(obj));

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
        },
         c,
         AsyncOperationType.UNSET);
    }

    @Override
    public <T> Map<String, Object> unset(final Query<T> query, final String field, final boolean multiple, AsyncOperationCallback<T> c) {
        if (c == null) {
            c = new AsyncOpAdapter<>();
        }

        //        final AsyncOperationCallback<T> callback = c;
        morphium.inc(StatisticKeys.WRITES_CACHED);
        addToWriteQueue(query.getType(),
         query.getCollectionName(),
         ctx->{
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
    public <T> Map<String, Object> unset(final Query<T> query, AsyncOperationCallback<T> c, final boolean multiple, final String ... fields) {
        if (c == null) {
            c = new AsyncOpAdapter<>();
        }

        morphium.inc(StatisticKeys.WRITES_CACHED);
        addToWriteQueue(query.getType(),
         query.getCollectionName(),
         ctx->{
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
    public <T> Map<String, Object> unset(final Query<T> query, AsyncOperationCallback<T> c, final boolean multiple, final Enum ... fields) {
        String[] flds = new String[fields.length];
        int i = 0;

        for (Enum e : fields) {
            flds[i++] = e.name();
        }

        unset(query, c, multiple, flds);
        return null;
    }

    @Override
    public <T> void dropCollection(
        final Class<T> cls, final String collection, AsyncOperationCallback<T> c) {
        if (c == null) {
            c = new AsyncOpAdapter<>();
        }

        final AsyncOperationCallback<T> callback = c;
        morphium.inc(StatisticKeys.WRITES_CACHED);
        addToWriteQueue(cls,
         collection,
         ctx->{
            directWriter.dropCollection(cls, collection, callback);
            morphium.getCache().clearCacheIfNecessary(cls);
        },
         c,
         AsyncOperationType.REMOVE);
    }

    @SuppressWarnings("unused")
    public <T> void ensureIndex(
        final Class<T> cls,
        final String collection,
        final Map<String, Object> index,
        AsyncOperationCallback<T> c) {
        createIndex(cls, collection, IndexDescription.fromMaps(index, null), c);
    }

    @Override
    public <T> void createIndex(
        final Class<T> cls,
        final String collection,
        final IndexDescription index,
        AsyncOperationCallback<T> c) {
        if (c == null) {
            c = new AsyncOpAdapter<>();
        }

        final AsyncOperationCallback<T> callback = c;
        morphium.inc(StatisticKeys.WRITES_CACHED);
        addToWriteQueue(cls,
         collection,
         ctx->directWriter.createIndex(cls, collection, index, callback),
         c,
         AsyncOperationType.ENSURE_INDICES);
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
        //        synchronized (opLog) {
        List<Class<?>> localBuffer = new ArrayList<>(opLog.keySet());

        for (Class<?> clz : localBuffer) {
            if (opLog.get(clz) == null || opLog.get(clz).isEmpty()) {
                continue;
            }

            flushQueueToMongo(opLog.get(clz));
        }

        //        }
    }

    @Override
    public void flush(Class type) {
        if (opLog.get(type) == null || opLog.get(type).isEmpty()) {
            return;
        }

        flushQueueToMongo(opLog.get(type));
    }

    @Override
    public void onShutdown(Morphium m) {
        logger.debug("Stopping housekeeping thread");
        running = false;
        flush();

        try {
            Thread.sleep((morphium.getConfig().getWriteBufferTimeGranularity()));

            if (housekeeping != null) {
                //noinspection deprecation
                housekeeping.stop();
            }
        } catch (Throwable e) {
        }
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

        private WriteBufferEntry(Class entitiyType,
         String collectionName,
         BufferedBulkOp toRun,
         long timestamp,
         AsyncOperationCallback c,
         AsyncOperationType t) {
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
    public <T> void set(
        T toSet,
        String collection,
        Map<String, Object> values,
        boolean upsert,
        AsyncOperationCallback<T> callback) {
        @SuppressWarnings("unchecked")
        Query<T> id =
         morphium.createQueryFor((Class<T>) toSet.getClass())
         .f("_id")
         .eq(morphium.getId(toSet));
        set(id, values, upsert, false, callback);
    }

    private static class AsyncOpAdapter<T> implements AsyncOperationCallback<T> {

        @Override
        public void onOperationSucceeded(
            AsyncOperationType type,
            Query<T> q,
            long duration,
            List<T> result,
            T entity,
            Object... param) {
        }

        @Override
        public void onOperationError(
            AsyncOperationType type,
            Query<T> q,
            long duration,
            String error,
            Throwable t,
            T entity,
            Object... param) {
        }
    }

    @Override
    public <T> Map<String, Object> explainRemove(ExplainVerbosity verbosity, T o, String Collection) {
        return directWriter.explainRemove(verbosity,o,Collection);
    }

    @Override
    public <T> Map<String, Object> explainRemove(ExplainVerbosity verbosity, Query<T> q) {
        return directWriter.explainRemove(verbosity,q);
    }
}
