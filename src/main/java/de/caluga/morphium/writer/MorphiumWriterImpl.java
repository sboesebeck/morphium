package de.caluga.morphium.writer;

import de.caluga.morphium.*;
import de.caluga.morphium.Collation;
import de.caluga.morphium.annotations.*;
import de.caluga.morphium.async.AsyncOperationCallback;
import de.caluga.morphium.async.AsyncOperationType;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.driver.WriteConcern;
import de.caluga.morphium.driver.bulk.BulkRequestContext;
import de.caluga.morphium.driver.commands.*;
import de.caluga.morphium.driver.wire.MongoConnection;
import de.caluga.morphium.query.Query;

import org.bson.types.ObjectId;
import org.json.simple.parser.ContainerFactory;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * User: Stephan Bösebeck Date: 30.08.12 Time: 14:38
 *
 * <p>default writer implementation - uses a ThreadPoolExecutor for execution of asynchornous calls
 * maximum Threads are limited to 0.9* MaxConnections configured in MorphiumConfig
 *
 * @see MorphiumWriter
 */
@SuppressWarnings({ "ConstantConditions", "unchecked", "ConfusingArgumentToVarargsMethod", "WeakerAccess" })
public class MorphiumWriterImpl implements MorphiumWriter, ShutdownListener {
    private static final Logger logger = LoggerFactory.getLogger(MorphiumWriterImpl.class);
    private Morphium morphium;
    private int maximumRetries = 10;
    private int pause = 250;
    private ThreadPoolExecutor executor = null;

    @Override
    public void setMaximumQueingTries(int n) {
        maximumRetries = n;
    }

    @Override
    public void setPauseBetweenTries(int p) {
        pause = p;
    }

    @SuppressWarnings("CommentedOutCode")
    @Override
    public void setMorphium(Morphium m) {
        morphium = m;

        if (m != null) {
            BlockingQueue<Runnable> queue =
            new LinkedBlockingQueue<Runnable>() {
                @Override
                public boolean offer(Runnable e) {
                    /*
                     * Offer it to the queue if there is 0 items already queued, else
                     * return false so the TPE will add another thread. If we return false
                     * and max threads have been reached then the RejectedExecutionHandler
                     * will be called which will do the put into the queue.
                     */
                    int poolSize = executor.getPoolSize();
                    int maximumPoolSize = executor.getMaximumPoolSize();

                    if (poolSize >= maximumPoolSize || poolSize > executor.getActiveCount()) {
                        return super.offer(e);
                    } else {
                        return false;
                    }
                }
            };
            int core = m.getConfig().getMaxConnections() / 2;

            if (core <= 1) {
                core = 1;
            }

            int max = m.getConfig().getMaxConnections() * m.getConfig().getThreadConnectionMultiplier();

            if (max <= core) {
                max = 2 * core;
            }

            executor = new ThreadPoolExecutor(core, max, 60L, TimeUnit.SECONDS, queue);
            executor.setRejectedExecutionHandler(
            (r, executor) -> {
                try {
                    /*
                     * This does the actual put into the queue. Once the max threads
                     * have been reached, the tasks will then queue up.
                     */
                    executor.getQueue().put(r);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
            //            executor.setRejectedExecutionHandler(new RejectedExecutionHandler() {
            //                @Override
            //                public void rejectedExecution(Runnable r, ThreadPoolExecutor executor)
            // {
            //                    logger.warn("Could not schedule...");
            //                }
            //            });
            executor.setThreadFactory(
            new ThreadFactory() {
                private final AtomicInteger num = new AtomicInteger(1);
                @Override
                public Thread newThread(Runnable r) {
                    Thread ret = new Thread(r, "writer " + num);
                    num.set(num.get() + 1);
                    ret.setDaemon(true);
                    return ret;
                }
            });
            m.addShutdownListener(this);
        }
    }

    public void close() {
        if (executor != null) {
            executor.shutdownNow();
        }
    }

    @Override
    public <T> void insert(List<T> lst, AsyncOperationCallback<T> callback) {
        if (!lst.isEmpty()) {
            WriterTask r =
            new WriterTask() {
                private AsyncOperationCallback<T> callback;
                @Override
                public void setCallback(AsyncOperationCallback cb) {
                    callback = cb;
                }
                @Override
                public void run() {
                    //                    System.out.println(System.currentTimeMillis()+" -
                    // storing" );
                    HashMap<Class, List<Object>> sorted = new HashMap<>();

                    for (Object o : lst) {
                        Class type = morphium.getARHelper().getRealClass(o.getClass());

                        if (!morphium.getARHelper().isAnnotationPresentInHierarchy(type, Entity.class)) {
                            logger.error("Not an entity! Storing not possible! Even not in" + " list!");
                            continue;
                        }

                        morphium.inc(StatisticKeys.WRITES);
                        o = morphium.getARHelper().getRealObject(o);

                        if (o == null) {
                            logger.warn("Illegal Reference? - cannot store Lazy-Loaded /" + " Partial Update Proxy without delegate!");
                            return;
                        }

                        sorted.putIfAbsent(o.getClass(), new ArrayList<>());
                        sorted.get(o.getClass()).add(o);

                        if (morphium.getARHelper().isAnnotationPresentInHierarchy(o.getClass(), CreationTime.class)) {
                            try {
                                morphium.setAutoValues(o);
                            } catch (IllegalAccessException e) {
                                logger.error(e.getMessage(), e);
                            }
                        }

                        morphium.firePreStore(o, true);
                    }

                    long allStart = System.currentTimeMillis();

                    try {
                        for (Map.Entry<Class, List<Object>> es : sorted.entrySet()) {
                            Class c = es.getKey();
                            List<Map<String, Object>> dbLst = new ArrayList<>();
                            // bulk insert... check if something already exists
                            WriteConcern wc = morphium.getWriteConcernForClass(c);
                            String coll = morphium.getMapper().getCollectionName(c);
                            checkIndexAndCaps(c, coll, callback);

                            // HashMap<Integer, Object> mapMarshalledNewObjects = new
                            // HashMap<>();
                            for (Object record : es.getValue()) {
                                setIdIfNull(record);
                                Map<String, Object> marshall = morphium.getMapper().serialize(record);
                                dbLst.add(Doc.of(marshall));
                                // mapMarshalledNewObjects.put(dbLst.size() - 1, record);
                            }

                            long start = System.currentTimeMillis();

                            if (!dbLst.isEmpty()) {
                                var con = morphium.getDriver().getPrimaryConnection(wc);
                                InsertMongoCommand settings = new InsertMongoCommand(con)
                                .setDb(morphium.getDatabase())
                                .setColl(coll)
                                .setDocuments(dbLst)
                                .setOrdered(false)
                                //.setBypassDocumentValidation(true)
                                ;

                                if (wc != null) {
                                    settings.setWriteConcern(wc.asMap());
                                }

                                settings.execute();
                                int idx = 0;
                                con.release();
                                //
                                // System.out.println(System.currentTimeMillis()+" -
                                // finish" );
                            }

                            morphium.getCache().clearCacheIfNecessary(c);
                            long dur = System.currentTimeMillis() - start;
                            // bulk insert
                            morphium.fireProfilingWriteEvent(c, dbLst, dur, true, WriteAccessType.BULK_INSERT);
                            // null because key changed => mongo _id
                            es.getValue().forEach(record -> {  // null because key changed => mongo _id
                                morphium.firePostStore(record, true);
                            });
                        }

                        if (callback != null) {
                            callback.onOperationSucceeded(AsyncOperationType.WRITE, null, System.currentTimeMillis() - allStart, null, null, lst);
                        }
                    } catch (Exception e) {
                        if (e instanceof RuntimeException) {
                            throw(RuntimeException) e;
                        }

                        if (callback == null) {
                            throw new RuntimeException(e);
                        }

                        callback.onOperationError(AsyncOperationType.WRITE, null, System.currentTimeMillis() - allStart, e.getMessage(), e, null, lst);
                    }

                    //                    System.out.println(System.currentTimeMillis()+" -
                    // finish" );
                }
            };
            submitAndBlockIfNecessary(callback, r);
        }
    }

    private void setIdIfNull(Object record) throws IllegalAccessException {
        Field idf = morphium.getARHelper().getIdField(record);

        if (idf.get(record) != null) {
            return;
        }

        if (idf.get(record) == null && idf.getType().equals(MorphiumId.class)) {
            idf.set(record, new MorphiumId());
        } else if (idf.get(record) == null && idf.getType().equals(ObjectId.class)) {
            idf.set(record, new ObjectId());
        } else if (idf.get(record) == null && idf.getType().equals(String.class)) {
            idf.set(record, new MorphiumId().toString());
        } else if (idf.getType().isAssignableFrom(MorphiumId.class)) {
            idf.set(record, new MorphiumId());
        } else {
            throw new IllegalArgumentException("Cannot set ID of non-ID-Type");
        }
    }

    @Override
    public <T> void insert(List<T> lst, String cn, AsyncOperationCallback<T> callback) {
        if (!lst.isEmpty()) {
            WriterTask r =
            new WriterTask() {
                private AsyncOperationCallback<T> callback;
                @Override
                public void setCallback(AsyncOperationCallback cb) {
                    callback = cb;
                }
                @SuppressWarnings("CommentedOutCode")
                @Override
                public void run() {
                    try {
                        String collectionName = cn;

                        if (lst == null || lst.isEmpty()) {
                            return;
                        }

                        if (collectionName == null) {
                            collectionName =
                                morphium.getMapper()
                                .getCollectionName(lst.get(0).getClass());
                        }

                        List<Map<String, Object>> dbLst = new ArrayList<>();
                        //        DBCollection collection =
                        // morphium.getDbName().getCollection(collectionName);
                        WriteConcern wc =
                            morphium.getWriteConcernForClass(lst.get(0).getClass());
                        HashMap<Object, Boolean> isNew = new HashMap<>();

                        for (Object o : lst) {
                            isNew.put(o, true);

                            try {
                                setIdIfNull(o);
                            } catch (IllegalAccessException e) {
                                throw new RuntimeException(e);
                            }

                            dbLst.add(Doc.of(morphium.getMapper().serialize(o)));
                        }

                        checkIndexAndCaps(lst.get(0).getClass(), collectionName, callback);
                        long start = System.currentTimeMillis();
                        //                        int cnt = 0;
                        morphium.firePreStore(isNew);
                        long dur = System.currentTimeMillis() - start;
                        morphium.fireProfilingWriteEvent(lst.get(0).getClass(), lst, dur, true, WriteAccessType.BULK_UPDATE);
                        start = System.currentTimeMillis();
                        MongoConnection con = morphium.getDriver().getPrimaryConnection(wc);
                        InsertMongoCommand settings =
                            new InsertMongoCommand(con)
                        .setDb(morphium.getDatabase())
                        .setColl(collectionName)
                        .setDocuments(dbLst);

                        if (wc != null) {
                            settings.setWriteConcern(wc.asMap());
                        }

                        var writeResult = settings.execute();
                        con.release();

                        if (writeResult.containsKey("writeErrors")) {
                            int failedWrites =
                                ((List) writeResult.get("writeErrors")).size();
                            int success = (int) writeResult.get("n");
                            throw new RuntimeException(
                                "Failed to write: "
                                + failedWrites
                                + " - succeeded: "
                                + success);
                        }

                        dur = System.currentTimeMillis() - start;
                        List<Class> cleared = new ArrayList<>();

                        for (Object o : lst) {
                            if (cleared.contains(o.getClass())) {
                                continue;
                            }

                            cleared.add(o.getClass());
                            morphium.getCache().clearCacheIfNecessary(o.getClass());
                        }

                        // bulk insert
                        morphium.fireProfilingWriteEvent(
                            lst.get(0).getClass(),
                            dbLst,
                            dur,
                            true,
                            WriteAccessType.BULK_INSERT);
                        morphium.firePostStore(isNew);
                    } catch (MorphiumDriverException e) {
                        // TODO: Implement Handling
                        throw new RuntimeException(e);
                    }
                }
            };
            submitAndBlockIfNecessary(callback, r);
        }
    }

    @Override
    public <T> void insert(
        final T obj, final String collection, AsyncOperationCallback<T> callback) {
        if (obj instanceof List) {
            insert((List) obj, callback);
            return;
        }

        WriterTask r =
        new WriterTask() {
            private AsyncOperationCallback<T> callback;
            @Override
            public void setCallback(AsyncOperationCallback cb) {
                callback = cb;
            }
            @Override
            public void run() {
                long start = System.currentTimeMillis();

                try {
                    T o = obj;
                    Class type = morphium.getARHelper().getRealClass(o.getClass());

                    if (!morphium.getARHelper()
                        .isAnnotationPresentInHierarchy(type, Entity.class)) {
                        throw new RuntimeException("Not an entity: " + type.getSimpleName() + " Storing not possible!");
                    }

                    morphium.inc(StatisticKeys.WRITES);
                    o = morphium.getARHelper().getRealObject(o);

                    if (o == null) {
                        logger.warn(
                            "Illegal Reference? - cannot store Lazy-Loaded / Partial"
                            + " Update Proxy without delegate!");
                        return;
                    }

                    if (morphium.isAutoValuesEnabledForThread()) {
                        morphium.setAutoValues(o);
                    }

                    morphium.firePreStore(o, true);
                    setIdIfNull(o);
                    Entity en = morphium.getARHelper().getAnnotationFromHierarchy(type, Entity.class);
                    Map<String, Object> marshall = morphium.getMapper().serialize(o);
                    String coll = collection;

                    if (coll == null) {
                        coll = morphium.getMapper().getCollectionName(type);
                    }

                    checkIndexAndCaps(type, coll, callback);
                    WriteConcern wc = morphium.getWriteConcernForClass(type);
                    List<Map<String, Object>> objs = new ArrayList<>();
                    objs.add(Doc.of(marshall));
                    MongoConnection primaryConnection = null;

                    try {
                        primaryConnection = morphium.getDriver().getPrimaryConnection(wc);
                        InsertMongoCommand settings =
                            new InsertMongoCommand(primaryConnection)
                        .setDb(getDbName())
                        .setColl(coll)
                        .setDocuments(objs);

                        if (wc != null) {
                            settings.setWriteConcern(wc.asMap());
                        }

                        var writeResult = settings.execute();

                        if (writeResult.containsKey("writeErrors")) {
                            int failedWrites =
                                ((List) writeResult.get("writeErrors")).size();
                            int success = (int) writeResult.get("n");
                            throw new RuntimeException(
                                "Failed to write: "
                                + failedWrites
                                + " - succeeded: "
                                + success);
                        }
                    } catch (Throwable t) {
                        throw new RuntimeException(t);
                    } finally {
                        if (primaryConnection != null) {
                            primaryConnection.release();
                        }
                    }

                    long dur = System.currentTimeMillis() - start;
                    morphium.fireProfilingWriteEvent(o.getClass(), marshall, dur, true, WriteAccessType.SINGLE_INSERT);
                    morphium.getCache().clearCacheIfNecessary(o.getClass());
                    morphium.firePostStore(o, true);

                    if (callback != null) {
                        callback.onOperationSucceeded(AsyncOperationType.WRITE, null, System.currentTimeMillis() - start, null, obj);
                    }
                } catch (Exception e) {
                    checkViolations(e);

                    if (callback == null) {
                        if (e instanceof RuntimeException) {
                            throw((RuntimeException) e);
                        }

                        throw new RuntimeException(e);
                    }

                    callback.onOperationError(AsyncOperationType.WRITE, null, System.currentTimeMillis() - start, e.getMessage(), e, obj);
                }
            }
        };
        submitAndBlockIfNecessary(callback, r);
    }

    private <T> void checkIndexAndCaps(Class type, String coll, AsyncOperationCallback<T> callback)
    throws MorphiumDriverException {
        if (coll == null) {
            coll = morphium.getMapper().getCollectionName(type);
        }

        if (!morphium.getDriver().isTransactionInProgress()
            && !morphium.getDriver().exists(getDbName(), coll)) {
            switch (morphium.getConfig().getIndexCheck()) {
                case CREATE_ON_WRITE_NEW_COL:
                    if (logger.isDebugEnabled()) {
                        logger.debug("Collection " + coll + " does not exist - ensuring indices");
                    }

                    createCappedCollection(type, coll);
                    morphium.ensureIndicesFor(type, coll, callback);
                    break;

                case CREATE_ON_STARTUP:
                case WARN_ON_STARTUP:
                case NO_CHECK:
                default:
                    // nothing
            }

            //
            //
        }
    }

    /**
     * @param obj - object to store
     */
    @Override
    public <T> void store(
        final T obj, final String collection, AsyncOperationCallback<T> callback) {
        if (obj instanceof List) {
            store((List) obj, collection, callback);
            return;
        }

        WriterTask r =
        new WriterTask() {
            private AsyncOperationCallback<T> callback;
            @Override
            public void setCallback(AsyncOperationCallback cb) {
                callback = cb;
            }
            @SuppressWarnings("CommentedOutCode")
            @Override
            public void run() {
                long start = System.currentTimeMillis();

                try {
                    T o = obj;
                    Class type = morphium.getARHelper().getRealClass(o.getClass());

                    if (!morphium.getARHelper()
                        .isAnnotationPresentInHierarchy(type, Entity.class)) {
                        throw new RuntimeException("Not an entity: " + type.getSimpleName() + " Storing not possible!");
                    }

                    // Entity en = morphium.getARHelper().getAnnotationFromHierarchy(o.getClass(), Entity.class);
                    morphium.inc(StatisticKeys.WRITES);
                    Object id = morphium.getARHelper().getId(o);
                    o = morphium.getARHelper().getRealObject(o);

                    if (o == null) {
                        logger.warn("Illegal Reference? - cannot store Lazy-Loaded / Partial Update Proxy without delegate!");
                        return;
                    }

                    boolean isNew = id == null;

                    if (morphium.isAutoValuesEnabledForThread()) {
                        isNew = morphium.setAutoValues(o);
                    }

                    morphium.firePreStore(o, isNew);
                    setIdIfNull(o);
                    Map<String, Object> marshall = morphium.getMapper().serialize(o);
                    String coll = collection;

                    if (coll == null) {
                        coll = morphium.getMapper().getCollectionName(type);
                    }

                    checkIndexAndCaps(type, coll, callback);
                    WriteConcern wc = morphium.getWriteConcernForClass(type);
                    List<Map<String, Object>> objs = new ArrayList<>();
                    objs.add(marshall);
                    Map<String, Object> ret;
                    MongoConnection con = null;

                    try {
                        con = morphium.getDriver().getPrimaryConnection(wc);
                        StoreMongoCommand settings =
                            new StoreMongoCommand(con)
                        .setColl(coll)
                        .setDb(getDbName())
                        .setDocs(objs);

                        if (wc != null) {
                            settings.setWriteConcern(wc.asMap());
                        }

                        ret = settings.execute();
                    } catch (MorphiumDriverException mde) {
                        throw mde;
                    } catch (Throwable t) {
                        throw new RuntimeException(t);
                    } finally {
                        if (con != null) {
                            con.release();
                        }
                    }

                    long dur = System.currentTimeMillis() - start;
                    morphium.fireProfilingWriteEvent(
                        o.getClass(),
                        marshall,
                        dur,
                        true,
                        WriteAccessType.SINGLE_INSERT);

                    //                    if (logger.isDebugEnabled()) {
                    //                        String n = "";
                    //                        if (isNew) {
                    //                            n = "NEW ";
                    //                        }
                    //                        logger.debug(n + "stored " +
                    // type.getSimpleName() + " after " + dur + " ms length:" +
                    // serialize.toString().length());
                    //                    }
                    if (isNew) {
                        List<String> flds =
                            morphium.getARHelper().getFields(o.getClass(), Id.class);

                        if (flds == null) {
                            throw new RuntimeException("Object does not have an ID field!");
                        }
                    }

                    morphium.getCache().clearCacheIfNecessary(o.getClass());
                    morphium.firePostStore(o, isNew);

                    if (callback != null) {
                        callback.onOperationSucceeded(AsyncOperationType.WRITE, null, System.currentTimeMillis() - start, null, obj);
                    }
                } catch (Exception e) {
                    checkViolations(e);

                    if (callback == null) {
                        if (e instanceof RuntimeException) {
                            throw((RuntimeException) e);
                        }

                        throw new RuntimeException(e);
                    }

                    callback.onOperationError(AsyncOperationType.WRITE, null, System.currentTimeMillis() - start, e.getMessage(), e, obj);
                }
            }
        };
        submitAndBlockIfNecessary(callback, r);
    }

    private void checkViolations(Exception e) {
        if (e instanceof RuntimeException) {
            if (e.getClass().getName().equals("javax.validation.ConstraintViolationException")) {
                // using reflection to get fields etc... in order to remove strong dependency
                try {
                    Method m = e.getClass().getMethod("getConstraintViolations");
                    Set violations = (Set) m.invoke(e);

                    for (Object v : violations) {
                        m = v.getClass().getMethod("getMessage");
                        String msg = (String) m.invoke(v);
                        m = v.getClass().getMethod("getRootBean");
                        Object bean = m.invoke(v);
                        String s = Utils.toJsonString(bean);
                        String type = bean.getClass().getName();
                        m = v.getClass().getMethod("getInvalidValue");
                        Object invalidValue = m.invoke(v);
                        m = v.getClass().getMethod("getPropertyPath");
                        Iterable<?> pth = (Iterable<?>) m.invoke(v);
                        StringBuilder stringBuilder = new StringBuilder();

                        for (Object p : pth) {
                            m = p.getClass().getMethod("getName");
                            String name = (String) m.invoke(p);
                            stringBuilder.append(".");
                            stringBuilder.append(name);
                        }

                        logger.error("Validation of " + type + " failed: " + msg + " - Invalid Value: " + invalidValue + " for path: " + stringBuilder + "\n Tried to store: " + s);
                    }
                } catch (Exception e1) {
                    logger.error("Could not get more information about validation error ", e1);
                }
            }
        }
    }

    @Override
    public <T> void store(final List<T> lst, String cln, AsyncOperationCallback<T> callback) {
        if (!lst.isEmpty()) {
            WriterTask r =
            new WriterTask() {
                private AsyncOperationCallback<T> callback;
                @Override
                public void setCallback(AsyncOperationCallback cb) {
                    callback = cb;
                }
                @SuppressWarnings("CommentedOutCode")
                @Override
                public void run() {
                    long allStart = System.currentTimeMillis();

                    try {
                        //
                        // System.out.println(System.currentTimeMillis()+" -  storing" );
                        Map<Class, List<Map<String, Object>>> toUpdate = new HashMap<>();
                        Map<Class, List<Map<String, Object>>> newElementsToInsert =
                            new HashMap<>();

                        // HashMap<Object, Boolean> isNew = new HashMap<>();
                        for (int i = 0; i < lst.size(); i++) {
                            Object o = lst.get(i);
                            Class type = morphium.getARHelper().getRealClass(o.getClass());

                            if (!morphium.getARHelper()
                                .isAnnotationPresentInHierarchy(type, Entity.class)) {
                                logger.error(
                                    "Not an entity! Storing not possible! Even not in"
                                    + " list!");
                                continue;
                            }

                            morphium.inc(StatisticKeys.WRITES);
                            o = morphium.getARHelper().getRealObject(o);

                            if (o == null) {
                                logger.warn(
                                    "Illegal Reference? - cannot store Lazy-Loaded /"
                                    + " Partial Update Proxy without delegate!");
                                return;
                            }

                            boolean isn = morphium.getId(o) == null;

                            if (morphium.isAutoValuesEnabledForThread()) {
                                isn = morphium.setAutoValues(o);
                            }

                            if (isn) {
                                setIdIfNull(o);
                                newElementsToInsert.putIfAbsent(
                                    o.getClass(), new ArrayList<>());
                                newElementsToInsert.get(o.getClass()).add(morphium.getMapper().serialize(o));
                            } else {
                                toUpdate.putIfAbsent(o.getClass(), new ArrayList<>());
                                toUpdate.get(o.getClass()).add(morphium.getMapper().serialize(o));
                            }

                            morphium.firePreStore(o, isn);
                        }

                        for (Map.Entry<Class, List<Map<String, Object>>> es :
                             toUpdate.entrySet()) {
                            Class c = es.getKey();
                            // bulk insert... check if something already exists
                            WriteConcern wc = morphium.getWriteConcernForClass(c);
                            String coll =
                                cln != null
                                ? cln
                                : morphium.getMapper().getCollectionName(c);
                            checkIndexAndCaps(c, coll, callback);
                            Entity en =
                                morphium.getARHelper()
                                .getAnnotationFromHierarchy(c, Entity.class);
                            long start = System.currentTimeMillis();
                            List<Map<String, Object>> lst = new ArrayList<>();
                            lst.addAll(es.getValue());
                            MongoConnection con =
                                morphium.getDriver().getPrimaryConnection(wc);

                            try {
                                StoreMongoCommand settings =
                                    new StoreMongoCommand(con)
                                .setDb(morphium.getConfig().getDatabase())
                                .setDocs(lst)
                                .setColl(coll);

                                if (wc != null) {
                                    settings.setWriteConcern(wc.asMap());
                                }

                                Map<String, Object> ret = settings.execute();
                                morphium.getCache().clearCacheIfNecessary(c);
                                long dur = System.currentTimeMillis() - start;
                                // bulk insert
                                morphium.fireProfilingWriteEvent(
                                    c,
                                    toUpdate,
                                    dur,
                                    true,
                                    WriteAccessType.BULK_INSERT);
                                // null because key changed => mongo _id
                                es.getValue()
                                .forEach(
                                record -> { // null because key changed =>
                                    // mongo _id
                                    morphium.firePostStore(record, true);
                                });
                            } finally {
                                con.release();
                            }
                        }

                        for (Map.Entry<Class, List<Map<String, Object>>> es :
                             newElementsToInsert.entrySet()) {
                            Class c = es.getKey();
                            // bulk insert... check if something already exists
                            WriteConcern wc = morphium.getWriteConcernForClass(c);
                            String coll =
                                cln != null
                                ? cln
                                : morphium.getMapper().getCollectionName(c);
                            //                            if
                            // (morphium.getConfig().isAutoIndexAndCappedCreationOnWrite()
                            // &&
                            // !morphium.getDriver().getConnection().exists(morphium.getConfig().getDatabase(), coll)) {
                            //                                createCappedCollationColl(c,
                            // coll);
                            //                                morphium.ensureIndicesFor(c,
                            // coll, callback);
                            //                            }
                            checkIndexAndCaps(c, coll, null);
                            long start = System.currentTimeMillis();
                            // morphium.getDriver().getConnection().insert(morphium.getConfig().getDatabase(), coll, es.getValue(), wc);
                            morphium.getCache().clearCacheIfNecessary(c);
                            long dur = System.currentTimeMillis() - start;
                            // bulk insert
                            morphium.fireProfilingWriteEvent(
                                c, toUpdate, dur, true, WriteAccessType.BULK_INSERT);
                            // null because key changed => mongo _id
                            es.getValue()
                            .forEach(
                            record -> { // null because key changed => mongo
                                // _id
                                morphium.firePostStore(record, true);
                            });
                        }

                        if (callback != null) {
                            callback.onOperationSucceeded(
                                AsyncOperationType.WRITE,
                                null,
                                System.currentTimeMillis() - allStart,
                                null,
                                null,
                                lst);
                        }
                    } catch (Exception e) {
                        if (e instanceof RuntimeException) {
                            throw(RuntimeException) e;
                        }

                        if (callback == null) {
                            throw new RuntimeException(e);
                        }

                        callback.onOperationError(
                            AsyncOperationType.WRITE,
                            null,
                            System.currentTimeMillis() - allStart,
                            e.getMessage(),
                            e,
                            null,
                            lst);
                    }

                    //                    System.out.println(System.currentTimeMillis()+" -
                    // finish" );
                }
            };
            submitAndBlockIfNecessary(callback, r);
        }
    }

    @Override
    public void flush() {
        // nothing to do
    }

    @Override
    public void flush(Class type) {
        // nothing to do here
    }

    @Override
    public <T> void store(final List<T> lst, AsyncOperationCallback<T> callback) {
        store(lst, null, callback);
    }

    private void createCappedCollection(Class c) {
        createCappedCollection(c, null);
    }

    @SuppressWarnings("SameParameterValue")
    private void createCappedCollection(Class c, String collectionName) {
        if (logger.isDebugEnabled()) {
            logger.debug(
                "Collection does not exist - ensuring indices / capped status / Schema"
                + " validation");
        }

        // checking if collection exists
        MongoConnection con = null;

        try {
            con = morphium.getDriver().getPrimaryConnection(morphium.getWriteConcernForClass(c));
            ListCollectionsCommand lcmd =
                new ListCollectionsCommand(con)
            .setDb(getDbName())
            .setFilter(Doc.of("name", collectionName));
            var result = lcmd.execute();

            if (result.size() > 0) {
                logger.info("collection already exists");
                return;
            }

            Entity e = morphium.getARHelper().getAnnotationFromHierarchy(c, Entity.class);
            CreateCommand cmd = new CreateCommand(con);
            cmd.setDb(morphium.getDatabase());
            cmd.setColl(morphium.getMapper().getCollectionName(c));
            Capped capped = morphium.getARHelper().getAnnotationFromHierarchy(c, Capped.class);

            if (capped != null) {
                cmd.setCapped(true);
                cmd.setSize(capped.maxSize());
                cmd.setMax(capped.maxEntries());
            }

            // cmd.put("autoIndexId",
            // (morphium.getARHelper().getIdField(c).getType().equals(MorphiumId.class)));

            if (!e.schemaDef().equals("")) {
                JSONParser jsonParser = new JSONParser();

                try {
                    Map<String, Object> def =
                        (Map<String, Object>)
                        jsonParser.parse(
                            e.schemaDef(),
                    new ContainerFactory() {
                        @Override
                        public Map createObjectContainer() {
                            return new HashMap<>();
                        }
                        @Override
                        public List creatArrayContainer() {
                            return new ArrayList();
                        }
                    });
                    cmd.setValidator(def);
                    cmd.setValidationLevel(e.validationLevel().name());
                    cmd.setValidationAction(e.validationAction().name());
                } catch (ParseException parseException) {
                    parseException.printStackTrace();
                }
            }

            if (!e.comment().equals("")) {
                cmd.setComment(e.comment());
            }

            de.caluga.morphium.annotations.Collation collation =
                morphium.getARHelper()
                .getAnnotationFromHierarchy(
                    c, de.caluga.morphium.annotations.Collation.class);

            if (collation != null) {
                Collation collation1 = new Collation();
                collation1.locale(collation.locale());

                if (!collation.alternate().equals("")) {
                    collation1.alternate(collation.alternate());
                }

                if (!collation.caseFirst().equals("")) {
                    collation1.caseFirst(collation.caseFirst());
                }

                collation1.backwards(collation.backwards());
                collation1.caseLevel(collation.caseLevel());
                collation1.numericOrdering(collation.numericOrdering());
                collation1.strength(collation.strength());
                cmd.setCollation(collation1.toQueryObject());
            }

            cmd.execute();
        } catch (MorphiumDriverException ex) {
            if (ex.getMessage()
                .startsWith(
                    "internal error: Command failed with error 48 (NamespaceExists):"
                    + " 'Collection already exists. NS:")) {
                LoggerFactory.getLogger(MorphiumWriterImpl.class)
                .error("Collection already exists...?");
            } else {
                throw new RuntimeException(ex);
            }
        } finally {
            con.release();
        }
    }

    /**
     * automatically convert the collection for the given type to a capped collection only works
     * if @Capped annotation is given for type
     *
     * @param c - type
     */
    @SuppressWarnings("unused")
    public <T> void convertToCapped(final Class<T> c) {
        convertToCapped(c, null);
    }

    public <T> void convertToCapped(final Class<T> c, final AsyncOperationCallback<T> callback) {
        convertToCapped(c, null, callback);
    }

    public <T> void convertToCapped(
        final Class<T> c, String collectionName, final AsyncOperationCallback<T> callback) {
        Runnable r =
        () -> {
            WriteConcern wc = morphium.getWriteConcernForClass(c);
            String coll =
            collectionName == null
            ? morphium.getMapper().getCollectionName(c)
            : collectionName;

            try {
                if (!morphium.exists(morphium.getConfig().getDatabase(), coll)) {
                    if (logger.isDebugEnabled()) {
                        logger.debug(
                            "Collection does not exist - creating collection with"
                            + " capped status");
                    }

                    Map<String, Object> cmd = new LinkedHashMap<>();
                    cmd.put("create", coll);
                    Capped capped =
                        morphium.getARHelper()
                        .getAnnotationFromHierarchy(c, Capped.class);

                    if (capped != null) {
                        cmd.put("capped", true);
                        cmd.put("size", capped.maxSize());
                        cmd.put("max", capped.maxEntries());
                    }

                    // cmd.put("autoIndexId",
                    // (morphium.getARHelper().getIdField(c).getType().equals(MorphiumId.class)));
                    //                    morphium.getDriver().runCommand(getDbName(),
                    // Doc.of(cmd));
                    throw new RuntimeException("not implemented yet");
                } else {
                    Capped capped =
                        morphium.getARHelper()
                        .getAnnotationFromHierarchy(c, Capped.class);

                    if (capped != null) {
                        Map<String, Object> cmd = new HashMap<>();
                        cmd.put("convertToCapped", coll);
                        cmd.put("size", capped.maxSize());
                        cmd.put("max", capped.maxEntries());
                        //
                        // morphium.getDriver().runCommand(getDbName(), Doc.of(cmd));
                        throw new RuntimeException("not implemented yet");
                        // Indexes are not available after converting - recreate them
                        //                        morphium.ensureIndicesFor(c, callback);
                    }
                }
            } catch (MorphiumDriverException e) {
                // TODO: Implement Handling
                throw new RuntimeException(e);
            }
        };

        if (callback == null) {
            r.run();
        } else {
            morphium.getAsyncOperationsThreadPool().execute(r);
            //            new Thread(r).start();
        }
    }

    private String getDbName() {
        return morphium.getConfig().getDatabase();
    }

    @SuppressWarnings({"unused", "UnusedParameters"})
    private void executeWriteBatch(
        List<Object> es, Class c, WriteConcern wc, BulkRequestContext bulkCtx, long start) {
        try {
            bulkCtx.execute();
        } catch (MorphiumDriverException e) {
            // TODO: Implement Handling
            throw new RuntimeException(e);
        }

        long dur = System.currentTimeMillis() - start;
        morphium.fireProfilingWriteEvent(c, es, dur, false, WriteAccessType.BULK_UPDATE);
        morphium.getCache().clearCacheIfNecessary(c);
        morphium.firePostStore(es, false);
    }

    @Override
    public <T> void set(
        final T toSet,
        final String col,
        final Map<String, Object> values,
        final boolean upsert,
        AsyncOperationCallback<T> callback) {
        Query<T> q =
            (Query<T>)
            morphium.createQueryFor(toSet.getClass())
            .f("_id")
            .eq(morphium.getId(toSet));
        q.setCollectionName(col);
        set(q, values, upsert, false, callback);

        for (Map.Entry<String, Object> entry : values.entrySet()) {
            Field fld = morphium.getARHelper().getField(toSet.getClass(), entry.getKey());

            try {
                fld.set(toSet, entry.getValue());
            } catch (IllegalAccessException e) {
                throw new RuntimeException("could not set value to field: " + entry.getKey());
            }
        }
    }

    @SuppressWarnings("CatchMayIgnoreException")
    public <T> void submitAndBlockIfNecessary(AsyncOperationCallback<T> callback, WriterTask<T> r) {
        if (callback == null) {
            r.run();
        } else {
            r.setCallback(callback);
            int tries = 0;
            boolean retry = true;

            while (retry) {
                try {
                    tries++;
                    executor.execute(r);
                    retry = false;
                } catch (OutOfMemoryError ignored) {
                    logger.error(tries + " - Got OutOfMemory Erro, retrying...", ignored);
                } catch (java.util.concurrent.RejectedExecutionException e) {
                    if (tries > maximumRetries) {
                        throw new RuntimeException(
                            "Could not write - not even after "
                            + maximumRetries
                            + " retries and pause of "
                            + pause
                            + "ms",
                            e);
                    }

                    if (logger.isDebugEnabled()) {
                        logger.warn(
                            "thread pool exceeded - waiting "
                            + pause
                            + " ms for the "
                            + tries
                            + ". time");
                    }

                    try {
                        Thread.sleep(pause);
                    } catch (InterruptedException ignored) {
                        // swallow
                    }
                }
            }
        }
    }

    @Override
    public <T> void updateUsingFields(
        final T ent,
        final String collection,
        AsyncOperationCallback<T> callback,
        final String... fields) {
        if (ent == null) {
            return;
        }

        WriterTask r =
        new WriterTask() {
            private AsyncOperationCallback<T> callback;
            @Override
            public void setCallback(AsyncOperationCallback cb) {
                callback = cb;
            }
            @SuppressWarnings("CommentedOutCode")
            @Override
            public void run() {
                Object id = morphium.getARHelper().getId(ent);

                if (id == null) {
                    // new object - update not working
                    logger.warn(
                        "trying to partially update new object - storing it in full!");
                    store(ent, collection, callback);
                    return;
                }

                morphium.firePreStore(ent, false);
                morphium.inc(StatisticKeys.WRITES);
                Entity entityDefinition =
                    morphium.getARHelper()
                    .getAnnotationFromHierarchy(ent.getClass(), Entity.class);
                boolean convertCamelCase =
                    entityDefinition != null && entityDefinition.translateCamelCase()
                    || entityDefinition == null
                    && morphium.getConfig()
                    .isCamelCaseConversionEnabled();
                Map<String, Object> find = new HashMap<>();
                find.put("_id", id);
                Map<String, Object> update = new HashMap<>();

                for (String f : fields) {
                    try {
                        Object value = morphium.getARHelper().getValue(ent, f);

                        if (value != null) {
                            Entity en =
                                morphium.getARHelper()
                                .getAnnotationFromHierarchy(
                                    value.getClass(), Entity.class);

                            if (morphium.getARHelper()
                                .isAnnotationPresentInHierarchy(
                                    value.getClass(), Entity.class)) {
                                if (morphium.getARHelper()
                                    .getField(ent.getClass(), f)
                                    .getAnnotation(Reference.class)
                                    != null) {
                                    // need to store reference
                                    value = morphium.getARHelper().getId(ent);
                                } else {
                                    value = morphium.getMapper().serialize(value);
                                }
                            }

                            //
                            //                            if (convertCamelCase) {
                            //                                f =
                            // morphium.getARHelper().convertCamelCase(f);
                            //                            }
                        }

                        update.put(f, value);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }

                Class<?> type = morphium.getARHelper().getRealClass(ent.getClass());
                LastChange t =
                    morphium.getARHelper()
                    .getAnnotationFromHierarchy(
                        type, LastChange.class); // (StoreLastChange)

                // type.getAnnotation(StoreLastChange.class);
                if (t != null) {
                    List<String> lst =
                        morphium.getARHelper()
                        .getFields(ent.getClass(), LastChange.class);
                    long now = System.currentTimeMillis();

                    for (String ctf : lst) {
                        Field f = morphium.getARHelper().getField(type, ctf);

                        if (f != null) {
                            try {
                                f.set(ent, now);
                            } catch (IllegalAccessException e) {
                                logger.error("Could not set modification time", e);
                            }
                        }

                        update.put(ctf, now);
                    }
                }

                update = UtilsMap.of("$set", update);
                WriteConcern wc = morphium.getWriteConcernForClass(type);
                long start = System.currentTimeMillis();

                try {
                    String collectionName = collection;

                    if (collectionName == null) {
                        collectionName =
                            morphium.getMapper().getCollectionName(ent.getClass());
                    }

                    checkIndexAndCaps(ent.getClass(), collectionName, callback);
                    Entity en =
                        morphium.getARHelper()
                        .getAnnotationFromHierarchy(
                            ent.getClass(), Entity.class);
                    var con = morphium.getDriver().getPrimaryConnection(wc);

                    try {
                        UpdateMongoCommand up =
                            new UpdateMongoCommand(con)
                        .setDb(getDbName())
                        .setColl(collectionName)
                        .setUpdates(
                            Arrays.asList(
                                Doc.of(
                                    "q",
                                    Doc.of(find),
                                    "u",
                                    Doc.of(update),
                                    "multi",
                                    false,
                                    "upsert",
                                    false)));

                        if (wc != null) {
                            up.setWriteConcern(wc.asMap());
                        }

                        up.execute();
                    } finally {
                        con.release();
                    }

                    long dur = System.currentTimeMillis() - start;
                    morphium.fireProfilingWriteEvent(
                        ent.getClass(),
                        update,
                        dur,
                        false,
                        WriteAccessType.SINGLE_UPDATE);
                    morphium.getCache()
                    .clearCacheIfNecessary(
                        morphium.getARHelper().getRealClass(ent.getClass()));
                    morphium.firePostStore(ent, false);

                    if (callback != null) {
                        callback.onOperationSucceeded(
                            AsyncOperationType.UPDATE,
                            null,
                            System.currentTimeMillis() - start,
                            null,
                            ent,
                            (Object) fields);
                    }
                } catch (Exception e) {
                    if (callback == null) {
                        throw new RuntimeException(e);
                    }

                    callback.onOperationError(
                        AsyncOperationType.UPDATE,
                        null,
                        System.currentTimeMillis() - start,
                        e.getMessage(),
                        e,
                        ent,
                        (Object) fields);
                }
            }
        };
        submitAndBlockIfNecessary(callback, r);
    }

    @SuppressWarnings("SuspiciousMethodCalls")
    @Override
    public <T> void remove(final List<T> lst, AsyncOperationCallback<T> callback) {
        WriterTask r =
        new WriterTask() {
            private AsyncOperationCallback<T> callback;
            @Override
            public void setCallback(AsyncOperationCallback cb) {
                callback = cb;
            }
            @Override
            public void run() {
                HashMap<Class<T>, List<Query<T>>> sortedMap = new HashMap<>();

                for (T o : lst) {
                    if (sortedMap.get(o.getClass()) == null) {
                        @SuppressWarnings("MismatchedQueryAndUpdateOfCollection")
                        List<Query<T>> queries = new ArrayList<>();
                        sortedMap.put((Class<T>) o.getClass(), queries);
                    }

                    Query<T> q = (Query<T>) morphium.createQueryFor(o.getClass());
                    q.f(morphium.getARHelper().getIdFieldName(o))
                    .eq(morphium.getARHelper().getId(o));
                    sortedMap.get(o.getClass()).add(q);
                }

                morphium.firePreRemove(lst);
                long start = System.currentTimeMillis();

                try {
                    for (Class<T> cls : sortedMap.keySet()) {
                        Query<T> orQuery = morphium.createQueryFor(cls);
                        orQuery = orQuery.or(sortedMap.get(cls));
                        remove(orQuery, null); // sync call
                    }

                    morphium.inc(StatisticKeys.WRITES);
                    morphium.firePostRemove(lst);

                    if (callback != null) {
                        callback.onOperationSucceeded(
                            AsyncOperationType.REMOVE,
                            null,
                            System.currentTimeMillis() - start,
                            null,
                            null,
                            lst);
                    }
                } catch (Exception e) {
                    if (callback == null) {
                        throw new RuntimeException(e);
                    }

                    callback.onOperationError(
                        AsyncOperationType.REMOVE,
                        null,
                        System.currentTimeMillis() - start,
                        e.getMessage(),
                        e,
                        null,
                        lst);
                }
            }
        };
        submitAndBlockIfNecessary(callback, r);
    }

    /**
     * deletes all objects matching the given query
     *
     * @param q - query
     */
    @Override
    public <T> void remove(final Query<T> q, AsyncOperationCallback<T> callback) {
        remove(q, true, callback);
    }

    @Override
    public <T> void remove(final Query<T> q, boolean multiple, AsyncOperationCallback<T> callback) {
        WriterTask r =
        new WriterTask() {
            private AsyncOperationCallback<T> callback;
            @Override
            public void setCallback(AsyncOperationCallback cb) {
                callback = cb;
            }
            @Override
            public void run() {
                morphium.firePreRemoveEvent(q);
                WriteConcern wc = morphium.getWriteConcernForClass(q.getType());
                long start = System.currentTimeMillis();
                MongoConnection con = null;

                try {
                    con = morphium.getDriver().getPrimaryConnection(wc);
                    String collectionName = q.getCollectionName();
                    DeleteMongoCommand settings =
                        new DeleteMongoCommand(con)
                    .setColl(collectionName)
                    .setDb(getDbName())
                    .setDeletes(
                        Arrays.asList(
                            Doc.of(
                                "q",
                                q.toQueryObject(),
                                "limit",
                                multiple ? 0 : 1,
                                "collation",
                                q.getCollation() == null
                                ? null
                                : q.getCollation()
                                .toQueryObject())));

                    if (wc != null) {
                        settings.setWriteConcern(wc.asMap());
                    }

                    settings.execute();
                    long dur = System.currentTimeMillis() - start;
                    morphium.fireProfilingWriteEvent(
                        q.getType(),
                        q.toQueryObject(),
                        dur,
                        false,
                        WriteAccessType.BULK_DELETE);
                    morphium.inc(StatisticKeys.WRITES);
                    morphium.getCache().clearCacheIfNecessary(q.getType());
                    morphium.firePostRemoveEvent(q);

                    if (callback != null) {
                        callback.onOperationSucceeded(
                            AsyncOperationType.REMOVE,
                            q,
                            System.currentTimeMillis() - start,
                            null,
                            null);
                    }
                } catch (Exception e) {
                    if (callback == null) {
                        throw new RuntimeException(e);
                    }

                    callback.onOperationError(
                        AsyncOperationType.REMOVE,
                        q,
                        System.currentTimeMillis() - start,
                        e.getMessage(),
                        e,
                        null);
                } finally {
                    if (con != null) {
                        con.release();
                    }
                }
            }
        };
        submitAndBlockIfNecessary(callback, r);
    }

    @Override
    public <T> void remove(final T o, final String collection, AsyncOperationCallback<T> callback) {
        WriterTask r =
        new WriterTask() {
            private AsyncOperationCallback<T> callback;
            @Override
            public void setCallback(AsyncOperationCallback cb) {
                callback = cb;
            }
            @Override
            public void run() {
                Object id = morphium.getARHelper().getId(o);
                morphium.firePreRemove(o);
                Map<String, Object> db = new HashMap<>();
                db.put("_id", id);
                WriteConcern wc = morphium.getWriteConcernForClass(o.getClass());
                long start = System.currentTimeMillis();
                MongoConnection con = null;

                try {
                    if (collection == null) {
                        morphium.getMapper().getCollectionName(o.getClass());
                    }

                    con = morphium.getDriver().getPrimaryConnection(wc);
                    DeleteMongoCommand settings =
                        new DeleteMongoCommand(con)
                    .setDb(getDbName())
                    .setColl(collection)
                    .setDeletes(Arrays.asList(Doc.of("q", db, "limit", 1)));

                    if (wc != null) {
                        settings.setWriteConcern(wc.asMap());
                    }

                    var res = settings.execute();
                    long dur = System.currentTimeMillis() - start;
                    morphium.fireProfilingWriteEvent(
                        o.getClass(), o, dur, false, WriteAccessType.SINGLE_DELETE);
                    morphium.clearCachefor(o.getClass());
                    morphium.inc(StatisticKeys.WRITES);
                    morphium.firePostRemoveEvent(o);

                    if (callback != null) {
                        callback.onOperationSucceeded(
                            AsyncOperationType.REMOVE,
                            null,
                            System.currentTimeMillis() - start,
                            null,
                            o);
                    }
                } catch (Exception e) {
                    if (callback == null) {
                        throw new RuntimeException(e);
                    }

                    callback.onOperationError(
                        AsyncOperationType.REMOVE,
                        null,
                        System.currentTimeMillis() - start,
                        e.getMessage(),
                        e,
                        o);
                } finally {
                    if (con != null) {
                        con.release();
                    }
                }
            }
        };
        submitAndBlockIfNecessary(callback, r);
    }

    /**
     * Increases a value in an existing mongo collection entry - no reading necessary. Object is
     * altered in place db.collection.update({"_id":toInc.id},{$inc:{field:amount}}
     * <b>attention</b>: this alteres the given object toSet in a similar way
     *
     * @param toInc: object to set the value in (or better - the corresponding entry in mongo)
     * @param field: the field to change
     * @param amount: the value to set
     */
    @Override
    public <T> void inc(
        final T toInc,
        final String collection,
        final String field,
        final Number amount,
        AsyncOperationCallback<T> callback) {
        WriterTask r =
        new WriterTask() {
            private AsyncOperationCallback<T> callback;
            @Override
            public void setCallback(AsyncOperationCallback cb) {
                callback = cb;
            }
            @SuppressWarnings("CommentedOutCode")
            @Override
            public void run() {
                Class cls = toInc.getClass();
                morphium.firePreUpdateEvent(
                    morphium.getARHelper().getRealClass(cls),
                    MorphiumStorageListener.UpdateTypes.INC);
                String coll = collection;

                if (coll == null) {
                    coll = morphium.getMapper().getCollectionName(cls);
                }

                Map<String, Object> query = new HashMap<>();
                query.put("_id", morphium.getId(toInc));
                Field f = morphium.getARHelper().getField(cls, field);

                if (f == null) {
                    throw new RuntimeException("Cannot inc unknown field: " + field);
                }

                String fieldName = morphium.getARHelper().getMongoFieldName(cls, field);
                Map<String, Object> update =
                    UtilsMap.of("$inc", UtilsMap.of(fieldName, amount));
                WriteConcern wc = morphium.getWriteConcernForClass(toInc.getClass());
                long start = System.currentTimeMillis();
                MongoConnection con = null;

                try {
                    checkIndexAndCaps(cls, coll, callback);
                    //                    if
                    // (morphium.getConfig().isAutoIndexAndCappedCreationOnWrite() &&
                    // !morphium.getDriver().getConnection().exists(getDbName(), coll)) {
                    //                        createCappedCollationColl(cls, coll);
                    //                        morphium.ensureIndicesFor(cls, coll,
                    // callback);
                    //                    }
                    Entity en =
                        morphium.getARHelper()
                        .getAnnotationFromHierarchy(cls, Entity.class);
                    con = morphium.getDriver().getPrimaryConnection(wc);
                    handleLastChange(cls, update);
                    UpdateMongoCommand settings =
                        new UpdateMongoCommand(con)
                    .setColl(coll)
                    .setDb(getDbName())
                    .setWriteConcern(wc.asMap())
                    .addUpdate(
                        Doc.of(query),
                        Doc.of(update),
                        null,
                        false,
                        false,
                        null,
                        null,
                        null);
                    ;
                    Map<String, Object> res = settings.execute();
                    morphium.getCache().clearCacheIfNecessary(cls);

                    if (f.getType().equals(Integer.class)
                        || f.getType().equals(int.class)) {
                        try {
                            f.set(toInc, ((Integer) f.get(toInc)) + amount.intValue());
                        } catch (IllegalAccessException e) {
                            throw new RuntimeException(e);
                        }
                    } else if (f.getType().equals(Double.class)
                               || f.getType().equals(double.class)) {
                        try {
                            f.set(toInc, ((Double) f.get(toInc)) + amount.doubleValue());
                        } catch (IllegalAccessException e) {
                            throw new RuntimeException(e);
                        }
                    } else if (f.getType().equals(Float.class)
                               || f.getType().equals(float.class)) {
                        try {
                            f.set(toInc, ((Float) f.get(toInc)) + amount.floatValue());
                        } catch (IllegalAccessException e) {
                            throw new RuntimeException(e);
                        }
                    } else if (f.getType().equals(Long.class)
                               || f.getType().equals(long.class)) {
                        try {
                            f.set(toInc, ((Long) f.get(toInc)) + amount.longValue());
                        } catch (IllegalAccessException e) {
                            throw new RuntimeException(e);
                        }
                    } else {
                        logger.error(
                            "Could not set increased value - unsupported type "
                            + cls.getName());
                    }

                    morphium.firePostUpdateEvent(
                        morphium.getARHelper().getRealClass(cls),
                        MorphiumStorageListener.UpdateTypes.INC);
                    morphium.fireProfilingWriteEvent(
                        toInc.getClass(),
                        toInc,
                        System.currentTimeMillis() - start,
                        false,
                        WriteAccessType.SINGLE_UPDATE);

                    if (callback != null) {
                        callback.onOperationSucceeded(
                            AsyncOperationType.INC,
                            null,
                            System.currentTimeMillis() - start,
                            null,
                            toInc,
                            field,
                            amount);
                    }
                } catch (Exception e) {
                    if (callback == null) {
                        throw new RuntimeException(e);
                    }

                    callback.onOperationError(
                        AsyncOperationType.INC,
                        null,
                        System.currentTimeMillis() - start,
                        e.getMessage(),
                        e,
                        toInc,
                        field,
                        amount);
                } finally {
                    if (con != null) {
                        con.release();
                    }
                }
            }
        };
        submitAndBlockIfNecessary(callback, r);
    }

    @Override
    public <T> void inc(
        final Query<T> query,
        final Map<String, Number> fieldsToInc,
        final boolean upsert,
        final boolean multiple,
        AsyncOperationCallback<T> callback) {
        WriterTask r =
        new WriterTask() {
            private AsyncOperationCallback<T> callback;
            @Override
            public void setCallback(AsyncOperationCallback cb) {
                callback = cb;
            }
            @SuppressWarnings("CommentedOutCode")
            @Override
            public void run() {
                Class<? extends T> cls = query.getType();
                morphium.firePreUpdateEvent(
                    morphium.getARHelper().getRealClass(cls),
                    MorphiumStorageListener.UpdateTypes.INC);
                String coll = query.getCollectionName();
                Map<String, Object> update = new HashMap<>();
                update.put("$inc", new HashMap<String, Object>(fieldsToInc));
                handleLastChange((Class<? extends T>) cls, update);
                Map<String, Object> qobj = query.toQueryObject();

                if (upsert) {
                    qobj = morphium.simplifyQueryObject(qobj);
                }

                long start = System.currentTimeMillis();
                MongoConnection con = null;

                try {
                    if (upsert) {
                        checkIndexAndCaps(cls, coll, callback);
                    }

                    //                    if (upsert &&
                    // morphium.getConfig().isAutoIndexAndCappedCreationOnWrite() &&
                    // !morphium.getDriver().getConnection().exists(getDbName(), coll)) {
                    //                        createCappedCollationColl(cls, coll);
                    //                        morphium.ensureIndicesFor((Class<T>) cls,
                    // coll, callback);
                    //                    }
                    WriteConcern wc = morphium.getWriteConcernForClass(cls);
                    con = morphium.getDriver().getPrimaryConnection(wc);
                    UpdateMongoCommand settings =
                        new UpdateMongoCommand(con)
                    .setDb(getDbName())
                    .setColl(coll)
                    .addUpdate(
                        Doc.of(qobj),
                        Doc.of(update),
                        null,
                        upsert,
                        multiple,
                        query.getCollation(),
                        null,
                        null);

                    if (query.getSort() != null) {
                        logger.warn("Sorting is not supported on updates!");
                    }

                    settings.execute();
                    long dur = System.currentTimeMillis() - start;
                    morphium.fireProfilingWriteEvent(
                        cls,
                        update,
                        dur,
                        upsert,
                        multiple
                        ? WriteAccessType.BULK_UPDATE
                        : WriteAccessType.SINGLE_UPDATE);
                    morphium.getCache().clearCacheIfNecessary(cls);
                    morphium.firePostUpdateEvent(
                        morphium.getARHelper().getRealClass(cls),
                        MorphiumStorageListener.UpdateTypes.INC);

                    if (callback != null) {
                        callback.onOperationSucceeded(
                            AsyncOperationType.INC,
                            query,
                            System.currentTimeMillis() - start,
                            null,
                            null,
                            fieldsToInc);
                    }
                } catch (Exception e) {
                    if (callback == null) {
                        throw new RuntimeException(e);
                    }

                    callback.onOperationError(
                        AsyncOperationType.INC,
                        query,
                        System.currentTimeMillis() - start,
                        e.getMessage(),
                        e,
                        null,
                        fieldsToInc);
                } finally {
                    if (con != null) {
                        con.release();
                    }
                }
            }
        };
        submitAndBlockIfNecessary(callback, r);
    }

    /**
     * @param cls
     * @param update: Content will be changed!!!!!
     * @param <T>
     */
    private <T> void handleLastChange(Class<? extends T> cls, Map<String, Object> update) {
        if (!morphium.isAutoValuesEnabledForThread()) {
            return;
        }

        LastChange lc = morphium.getARHelper().getAnnotationFromHierarchy(cls, LastChange.class);

        if (lc != null) {
            List<String> latChangeFlds = morphium.getARHelper().getFields(cls, LastChange.class);

            if (latChangeFlds != null && !latChangeFlds.isEmpty()) {
                for (String fL : latChangeFlds) {
                    Field fld = morphium.getARHelper().getField(cls, fL);
                    Class<?> type = fld.getType();
                    update.putIfAbsent("$set", new HashMap<>());

                    if (type.equals(Long.class) || type.equals(long.class)) {
                        ((Map) update.get("$set"))
                        .put(
                            morphium.getARHelper()
                            .getMongoFieldName(cls, fld.getName()),
                            System.currentTimeMillis());
                    } else if (type.equals(Date.class)) {
                        ((Map) update.get("$set"))
                        .put(
                            morphium.getARHelper()
                            .getMongoFieldName(cls, fld.getName()),
                            new Date());
                    } else if (type.equals(String.class)) {
                        ((Map) update.get("$set"))
                        .put(
                            morphium.getARHelper()
                            .getMongoFieldName(cls, fld.getName()),
                            new Date().toString());
                    } else {
                        ((Map) update.get("$set"))
                        .put(
                            morphium.getARHelper()
                            .getMongoFieldName(cls, fld.getName()),
                            System.currentTimeMillis());
                    }
                }
            }
        }
    }

    @Override
    public <T> void inc(
        final Query<T> query,
        final String field,
        final Number amount,
        final boolean upsert,
        final boolean multiple,
        AsyncOperationCallback<T> callback) {
        WriterTask r =
        new WriterTask() {
            private AsyncOperationCallback<T> callback;
            @Override
            public void setCallback(AsyncOperationCallback cb) {
                callback = cb;
            }
            @SuppressWarnings("CommentedOutCode")
            @Override
            public void run() {
                Class cls = query.getType();
                morphium.firePreUpdateEvent(
                    morphium.getARHelper().getRealClass(cls),
                    MorphiumStorageListener.UpdateTypes.INC);
                String coll = query.getCollectionName();
                String fieldName = morphium.getARHelper().getMongoFieldName(cls, field);
                Map<String, Object> update =
                    UtilsMap.of("$inc", UtilsMap.of(fieldName, amount));
                Map<String, Object> qobj = query.toQueryObject();

                if (upsert) {
                    qobj = morphium.simplifyQueryObject(qobj);
                }

                long start = System.currentTimeMillis();
                MongoConnection con = null;

                try {
                    if (upsert) {
                        checkIndexAndCaps(cls, coll, callback);
                    }

                    //                    if (upsert &&
                    // morphium.getConfig().isAutoIndexAndCappedCreationOnWrite() &&
                    // !morphium.getDriver().getConnection().exists(getDbName(), coll)) {
                    //                        createCappedCollationColl(cls, coll);
                    //                        morphium.ensureIndicesFor(cls, coll,
                    // callback);
                    //
                    //                    }
                    handleLastChange(cls, update);
                    WriteConcern wc = morphium.getWriteConcernForClass(cls);
                    con = morphium.getDriver().getPrimaryConnection(wc);
                    UpdateMongoCommand settings =
                        new UpdateMongoCommand(con)
                    .setDb(getDbName())
                    .setColl(coll)
                    .addUpdate(
                        Doc.of(qobj),
                        Doc.of(update),
                        null,
                        upsert,
                        multiple,
                        query.getCollation(),
                        null,
                        null);

                    if (query.getSort() != null) {
                        logger.warn("Sorting not supported in update query!");
                    }

                    settings.execute();
                    long dur = System.currentTimeMillis() - start;
                    morphium.fireProfilingWriteEvent(
                        cls,
                        update,
                        dur,
                        upsert,
                        multiple
                        ? WriteAccessType.BULK_UPDATE
                        : WriteAccessType.SINGLE_UPDATE);
                    morphium.getCache().clearCacheIfNecessary(cls);
                    morphium.firePostUpdateEvent(
                        morphium.getARHelper().getRealClass(cls),
                        MorphiumStorageListener.UpdateTypes.INC);

                    if (callback != null) {
                        callback.onOperationSucceeded(
                            AsyncOperationType.INC,
                            query,
                            System.currentTimeMillis() - start,
                            null,
                            null,
                            field,
                            amount);
                    }
                } catch (Exception e) {
                    if (callback == null) {
                        throw new RuntimeException(e);
                    }

                    callback.onOperationError(
                        AsyncOperationType.INC,
                        query,
                        System.currentTimeMillis() - start,
                        e.getMessage(),
                        e,
                        null,
                        field,
                        amount);
                } finally {
                    if (con != null) {
                        con.release();
                    }
                }
            }
        };
        submitAndBlockIfNecessary(callback, r);
    }

    /**
     * @param cls
     * @param coll
     * @param query
     * @param update: the update commmand - will be changed!
     * @param upsert
     */
    private void handleCreationTimeOnUpsert(
        Class cls,
        String coll,
        Map<String, Object> query,
        Map<String, Object> update,
        boolean upsert) {
        if (upsert
            && morphium.getARHelper().isAnnotationPresentInHierarchy(cls, CreationTime.class)
            && morphium.isAutoValuesEnabledForThread()) {
            Map<String, Object> qobj = morphium.simplifyQueryObject(query);

            if (coll == null) {
                coll = morphium.getMapper().getCollectionName(cls);
            }

            long cnt = 1;
            MongoConnection con = null;

            try {
                con = morphium.getDriver().getReadConnection(null);
                CountMongoCommand settings =
                    new CountMongoCommand(con)
                .setQuery(Doc.of(qobj))
                .setDb(getDbName())
                .setColl(coll);
                cnt = settings.getCount();
            } catch (MorphiumDriverException e) {
                logger.error("Error counting", e);
            } finally {
                if (con != null) {
                    con.release();
                }
            }

            if (cnt == 0) {
                List<String> flds = morphium.getARHelper().getFields(cls, CreationTime.class);

                for (String creationTimeField : flds) {
                    Class<?> type =
                        morphium.getARHelper().getField(cls, creationTimeField).getType();

                    if (type.equals(Date.class)) {
                        qobj.put(creationTimeField, new Date());
                    } else if (type.equals(Long.class) || type.equals(long.class)) {
                        qobj.put(creationTimeField, System.currentTimeMillis());
                    } else if (type.equals(String.class)) {
                        qobj.put(creationTimeField, new Date().toString());
                    } else {
                        logger.error("Could not set CreationTime.... wrong type " + type.getName());
                    }
                }
            }

            update.putIfAbsent("$set", new HashMap<>());

            // Remove values, that are part of the query, but also in the update part
            // keep only the updates
            for (Map.Entry e : update.entrySet()) {
                for (Map.Entry<String, Object> f :
                     ((Map<String, Object>) e.getValue()).entrySet()) {
                    qobj.remove(f.getKey());
                }
            }

            ((Map) update.get("$set")).putAll(qobj);
        }
    }

    /**
     * will change an entry in mongodb-collection corresponding to given class object if query is
     * too complex, upsert might not work! Upsert should consist of single and-queries, which will
     * be used to generate the object to create, unless it already exists. look at Mongodb-query
     * documentation as well
     *
     * @param query - query to specify which objects should be set
     * @param values - map fieldName->Value, which values are to be set!
     * @param upsert - insert, if it does not exist (query needs to be simple!)
     * @param multiple - update several documents, if false, only first hit will be updated
     */
    @Override
    public <T> void set(
        final Query<T> query,
        final Map<String, Object> values,
        final boolean upsert,
        final boolean multiple,
        AsyncOperationCallback<T> callback) {
        submitAndBlockIfNecessary(
            callback,
        new WriterTask() {
            private AsyncOperationCallback<T> callback;
            @Override
            public void setCallback(AsyncOperationCallback cb) {
                callback = cb;
            }
            @SuppressWarnings("CommentedOutCode")
            @Override
            public void run() {
                Class<?> cls = query.getType();
                String coll = query.getCollectionName();
                morphium.firePreUpdateEvent(
                    morphium.getARHelper().getRealClass(cls),
                    MorphiumStorageListener.UpdateTypes.SET);
                Map<String, Object> toSet = new HashMap<>();

                for (Map.Entry<String, Object> ef : values.entrySet()) {
                    String fieldName =
                        morphium.getARHelper().getMongoFieldName(cls, ef.getKey());
                    toSet.put(fieldName, marshallIfNecessary(ef.getValue()));
                }

                Map<String, Object> update = UtilsMap.of("$set", toSet);
                Map<String, Object> qobj = query.toQueryObject();
                Entity en =
                    morphium.getARHelper()
                    .getAnnotationFromHierarchy(cls, Entity.class);
                handleLastChange(cls, update);
                handleCreationTimeOnUpsert(
                    cls, coll, query.toQueryObject(), update, upsert);
                WriteConcern wc = morphium.getWriteConcernForClass(cls);
                long start = System.currentTimeMillis();
                MongoConnection con = null;

                try {
                    if (upsert) {
                        checkIndexAndCaps(cls, coll, callback);
                    }

                    //                    if (upsert &&
                    // morphium.getConfig().isAutoIndexAndCappedCreationOnWrite() &&
                    // !morphium.getDriver().getConnection().exists(getDbName(), coll)) {
                    //                        createCappedCollationColl(cls, coll);
                    //                        morphium.ensureIndicesFor((Class<T>) cls,
                    // coll, callback);
                    //                    }
                    con = morphium.getDriver().getPrimaryConnection(wc);
                    UpdateMongoCommand settings =
                        new UpdateMongoCommand(con)
                    .setDb(getDbName())
                    .setColl(coll)
                    .setWriteConcern(wc != null ? wc.asMap() : null)
                    .addUpdate(
                        Doc.of(qobj),
                        Doc.of(update),
                        null,
                        upsert,
                        multiple,
                        query.getCollation(),
                        null,
                        null);
                    Map<String, Object> daa = settings.execute();
                    long dur = System.currentTimeMillis() - start;
                    morphium.fireProfilingWriteEvent(
                        cls,
                        update,
                        dur,
                        upsert,
                        multiple
                        ? WriteAccessType.BULK_UPDATE
                        : WriteAccessType.SINGLE_UPDATE);
                    morphium.inc(StatisticKeys.WRITES);
                    morphium.getCache().clearCacheIfNecessary(cls);
                    morphium.firePostUpdateEvent(
                        morphium.getARHelper().getRealClass(cls),
                        MorphiumStorageListener.UpdateTypes.SET);

                    if (callback != null) {
                        callback.onOperationSucceeded(
                            AsyncOperationType.SET,
                            query,
                            System.currentTimeMillis() - start,
                            null,
                            null,
                            values,
                            upsert,
                            multiple);
                    }
                } catch (Exception e) {
                    if (callback == null) {
                        throw new RuntimeException(e);
                    }

                    callback.onOperationError(
                        AsyncOperationType.SET,
                        query,
                        System.currentTimeMillis() - start,
                        e.getMessage(),
                        e,
                        null,
                        values,
                        upsert,
                        multiple);
                } finally {
                    if (con != null) {
                        con.release();
                    }
                }
            }
        });
    }

    @Override
    public <T> void unset(
        final Query<T> query,
        AsyncOperationCallback<T> callback,
        final boolean multiple,
        final Enum... fields) {
        ArrayList<String> flds = new ArrayList<>();

        for (Enum e : fields) {
            flds.add(morphium.getARHelper().getMongoFieldName(query.getType(), e.name()));
        }

        unset(query, callback, multiple, flds.toArray(new String[fields.length]));
    }

    @Override
    public <T> void unset(
        final Query<T> query,
        AsyncOperationCallback<T> callback,
        final boolean multiple,
        final String... fields) {
        WriterTask r =
        new WriterTask() {
            private AsyncOperationCallback<T> callback;
            @Override
            public void setCallback(AsyncOperationCallback cb) {
                callback = cb;
            }
            @Override
            public void run() {
                Class<?> cls = query.getType();
                String coll = query.getCollectionName();
                morphium.firePreUpdateEvent(
                    morphium.getARHelper().getRealClass(cls),
                    MorphiumStorageListener.UpdateTypes.SET);
                Map<String, Object> qobj = query.toQueryObject();
                Map<String, String> toSet = new HashMap<>();

                for (String f : fields) {
                    toSet.put(
                        morphium.getARHelper().getMongoFieldName(cls, f),
                        ""); // value is ignored
                }

                Map<String, Object> update = UtilsMap.of("$unset", toSet);
                handleLastChange(cls, update);
                WriteConcern wc = morphium.getWriteConcernForClass(cls);
                long start = System.currentTimeMillis();
                MongoConnection con = null;

                try {
                    con = morphium.getDriver().getPrimaryConnection(wc);
                    UpdateMongoCommand settings =
                        new UpdateMongoCommand(con)
                    .setDb(getDbName())
                    .setColl(coll)
                    .setWriteConcern(wc.asMap())
                    .addUpdate(
                        Doc.of(qobj),
                        Doc.of(update),
                        null,
                        false,
                        multiple,
                        query.getCollation(),
                        null,
                        null);

                    if (query.getSort() != null) {
                        logger.warn("Sort not supported when updating");
                    }

                    settings.execute();
                    long dur = System.currentTimeMillis() - start;
                    morphium.fireProfilingWriteEvent(
                        cls,
                        update,
                        dur,
                        false,
                        multiple
                        ? WriteAccessType.BULK_UPDATE
                        : WriteAccessType.SINGLE_UPDATE);
                    morphium.getCache().clearCacheIfNecessary(cls);
                    morphium.firePostUpdateEvent(
                        morphium.getARHelper().getRealClass(cls),
                        MorphiumStorageListener.UpdateTypes.SET);

                    if (callback != null) {
                        callback.onOperationSucceeded(
                            AsyncOperationType.SET,
                            query,
                            System.currentTimeMillis() - start,
                            null,
                            null,
                            fields,
                            false,
                            multiple);
                    }
                } catch (Exception e) {
                    if (callback == null) {
                        throw new RuntimeException(e);
                    }

                    callback.onOperationError(
                        AsyncOperationType.SET,
                        query,
                        System.currentTimeMillis() - start,
                        e.getMessage(),
                        e,
                        null,
                        fields,
                        false,
                        multiple);
                } finally {
                    if (con != null) {
                        con.release();
                    }
                }
            }
        };
        submitAndBlockIfNecessary(callback, r);
    }

    /**
     * Un-setting a value in an existing mongo collection entry - no reading necessary. Object is
     * altered in place db.collection.update({"_id":toSet.id},{$unset:{field:1}} <b>attention</b>:
     * this alteres the given object toSet in a similar way
     *
     * @param toSet: object to set the value in (or better - the corresponding entry in mongo)
     * @param field: field to remove from document
     */
    @Override
    public <T> void unset(
        final T toSet,
        final String collection,
        final String field,
        AsyncOperationCallback<T> callback) {
        if (toSet == null) {
            throw new RuntimeException("Cannot update null!");
        }

        if (morphium.getARHelper().getId(toSet) == null) {
            logger.info("just storing object as it is new...");
            store(toSet, collection, callback);
        }

        WriterTask r =
        new WT() {
            @Override
            public void run() {
                Class cls = toSet.getClass();
                morphium.firePreUpdateEvent(
                    morphium.getARHelper().getRealClass(cls),
                    MorphiumStorageListener.UpdateTypes.UNSET);
                String coll = collection;

                if (coll == null) {
                    coll = morphium.getMapper().getCollectionName(cls);
                }

                Map<String, Object> query = new HashMap<>();
                query.put("_id", morphium.getId(toSet));
                Field f = morphium.getARHelper().getField(cls, field);

                if (f == null
                    && !morphium.getARHelper()
                    .isAnnotationPresentInHierarchy(
                        cls, AdditionalData.class)) {
                    throw new RuntimeException("Unknown field: " + field);
                }

                String fieldName = morphium.getARHelper().getMongoFieldName(cls, field);
                Map<String, Object> update =
                    UtilsMap.of("$unset", UtilsMap.of(fieldName, 1));
                WriteConcern wc = morphium.getWriteConcernForClass(toSet.getClass());
                doUpdate(cls, toSet, coll, field, query, f, update, wc);
            }
        };
        submitAndBlockIfNecessary(callback, r);
    }

    @Override
    public <T> void pop(
        final T obj,
        final String collection,
        final String field,
        final boolean first,
        AsyncOperationCallback<T> callback) {
        if (obj == null) {
            throw new RuntimeException("Cannot update null!");
        }

        if (morphium.getARHelper().getId(obj) == null) {
            logger.info("just storing object as it is new...");
            store(obj, collection, callback);
        }

        WriterTask r =
        new WT() {
            @Override
            public void run() {
                Class cls = obj.getClass();
                morphium.firePreUpdateEvent(
                    morphium.getARHelper().getRealClass(cls),
                    MorphiumStorageListener.UpdateTypes.UNSET);
                String coll = collection;

                if (coll == null) {
                    coll = morphium.getMapper().getCollectionName(cls);
                }

                Map<String, Object> query = new HashMap<>();
                query.put("_id", morphium.getId(obj));
                Field f = morphium.getARHelper().getField(cls, field);

                if (f == null) {
                    throw new RuntimeException("Unknown field: " + field);
                }

                String fieldName = morphium.getARHelper().getMongoFieldName(cls, field);
                Map<String, Object> update =
                    UtilsMap.of("$pop", UtilsMap.of(fieldName, first ? -1 : 1));
                WriteConcern wc = morphium.getWriteConcernForClass(obj.getClass());
                doUpdate(cls, obj, coll, field, query, f, update, wc);
            }
        };
        submitAndBlockIfNecessary(callback, r);
    }

    @Override
    public <T> void unset(
        Query<T> query, String field, boolean multiple, AsyncOperationCallback<T> callback) {
        unset(query, callback, multiple, field);
    }

    @Override
    public <T> void pushPull(
        final MorphiumStorageListener.UpdateTypes type,
        final Query<T> query,
        final String field,
        final Object value,
        final boolean upsert,
        final boolean multiple,
        AsyncOperationCallback<T> callback) {
        WriterTask r =
        new WriterTask() {
            private AsyncOperationCallback<T> callback;
            @Override
            public void setCallback(AsyncOperationCallback cb) {
                callback = cb;
            }
            @Override
            public void run() {
                Class<?> cls = query.getType();
                morphium.firePreUpdateEvent(morphium.getARHelper().getRealClass(cls), type);
                String coll = query.getCollectionName();
                Map<String, Object> qobj = query.toQueryObject();

                if (upsert) {
                    qobj = morphium.simplifyQueryObject(qobj);
                }

                Object v = marshallIfNecessary(value);
                String fieldName = morphium.getARHelper().getMongoFieldName(cls, field);
                Map<String, Object> set =
                    Doc.of(fieldName, v instanceof Enum ? ((Enum) v).name() : v);
                Map<String, Object> update = null;

                switch (type) {
                    case PUSH:
                        update = Doc.of("$push", set);
                        break;

                    case PULL:
                        update = Doc.of("$pull", set);
                        break;

                    case ADD_TO_SET:
                        update = Doc.of("$add_to_set", set);
                        break;

                    default:
                        throw new IllegalArgumentException(
                            "Unsupported type " + type.name());
                }

                // Doc.of(push ? "$push" : "$pull", set);
                long start = System.currentTimeMillis();

                try {
                    boolean push =
                        type.equals(MorphiumStorageListener.UpdateTypes.PUSH)
                        || type.equals(
                            MorphiumStorageListener.UpdateTypes.ADD_TO_SET);
                    pushIt(
                        push,
                        upsert,
                        multiple,
                        cls,
                        coll,
                        qobj,
                        update,
                        query.getCollation());
                    morphium.firePostUpdateEvent(
                        query.getType(), MorphiumStorageListener.UpdateTypes.PUSH);

                    if (callback != null) {
                        callback.onOperationSucceeded(
                            AsyncOperationType.PUSH,
                            query,
                            System.currentTimeMillis() - start,
                            null,
                            null,
                            field,
                            value,
                            upsert,
                            multiple);
                    }
                } catch (RuntimeException e) {
                    if (callback == null) {
                        throw new RuntimeException(e);
                    }

                    callback.onOperationError(
                        AsyncOperationType.PUSH,
                        query,
                        System.currentTimeMillis() - start,
                        e.getMessage(),
                        e,
                        null,
                        field,
                        value,
                        upsert,
                        multiple);
                }
            }
        };
        submitAndBlockIfNecessary(callback, r);
    }

    public Object marshallIfNecessary(Object value) {
        if (value != null) {
            if (value instanceof Enum) {
                return ((Enum) value).name();
            }

            if (morphium.getARHelper()
                .isAnnotationPresentInHierarchy(value.getClass(), Entity.class)
                || morphium.getARHelper()
                .isAnnotationPresentInHierarchy(value.getClass(), Embedded.class)) {
                // need to serialize...
                Map<String, Object> marshall = morphium.getMapper().serialize(value);
                marshall.put(
                    "class_name", morphium.getARHelper().getTypeIdForClass(value.getClass()));
                value = marshall;
            } else if (List.class.isAssignableFrom(value.getClass())) {
                List lst = new ArrayList();

                for (Object o : (List) value) {
                    if (morphium.getARHelper()
                        .isAnnotationPresentInHierarchy(o.getClass(), Embedded.class)
                        || morphium.getARHelper()
                        .isAnnotationPresentInHierarchy(o.getClass(), Entity.class)) {
                        Map<String, Object> marshall = morphium.getMapper().serialize(o);
                        marshall.put(
                            "class_name",
                            morphium.getARHelper().getTypeIdForClass(o.getClass()));
                        lst.add(marshall);
                    } else {
                        lst.add(o);
                    }
                }

                value = lst;
            } else if (Map.class.isAssignableFrom(value.getClass())) {
                value = new LinkedHashMap((Map) value);

                for (Object e : ((Map) value).entrySet()) {
                    Map.Entry en = (Map.Entry) e;

                    if (!String.class.isAssignableFrom(((Map.Entry) e).getKey().getClass())) {
                        throw new IllegalArgumentException(
                            "Can't push maps with Key not of type String!");
                    }

                    if (en.getValue() != null) {
                        if (morphium.getARHelper()
                            .isAnnotationPresentInHierarchy(
                                en.getValue().getClass(), Entity.class)
                            || morphium.getARHelper()
                            .isAnnotationPresentInHierarchy(
                                en.getValue().getClass(), Embedded.class)) {
                            Map<String, Object> marshall =
                                morphium.getMapper().serialize(en.getValue());
                            marshall.put(
                                "class_name",
                                morphium.getARHelper()
                                .getTypeIdForClass(en.getValue().getClass()));
                            ((Map) value).put(en.getKey(), marshall);
                        }
                    } else {
                        ((Map) value).put(en.getKey(), null);
                    }
                }
            }
        }

        return value;
    }

    @SuppressWarnings("CommentedOutCode")
    private void pushIt(
        boolean push,
        boolean upsert,
        boolean multiple,
        Class<?> cls,
        String coll,
        Map<String, Object> qobj,
        Map<String, Object> update,
        Collation collation) {
        morphium.firePreUpdateEvent(
            morphium.getARHelper().getRealClass(cls),
            push
            ? MorphiumStorageListener.UpdateTypes.PUSH
            : MorphiumStorageListener.UpdateTypes.PULL);
        Entity en = morphium.getARHelper().getAnnotationFromHierarchy(cls, Entity.class);

        if (coll == null) {
            coll = morphium.getMapper().getCollectionName(cls);
        }

        handleLastChange(cls, update);
        handleCreationTimeOnUpsert(cls, coll, qobj, update, upsert);
        WriteConcern wc = morphium.getWriteConcernForClass(cls);
        long start = System.currentTimeMillis();
        MongoConnection con = null;

        try {
            checkIndexAndCaps(cls, coll, null);
            con = morphium.getDriver().getPrimaryConnection(wc);
            UpdateMongoCommand settings =
                new UpdateMongoCommand(con)
            .setColl(coll)
            .setDb(getDbName())
            .setWriteConcern(wc != null ? wc.asMap() : null)
            .addUpdate(
                Doc.of(qobj),
                Doc.of(update),
                null,
                upsert,
                multiple,
                collation,
                null,
                null);
            settings.execute();
            morphium.inc(StatisticKeys.WRITES);
        } catch (MorphiumDriverException e) {
            // TODO: Implement Handling
            throw new RuntimeException(e);
        } finally {
            if (con != null) {
                con.release();
            }
        }

        long dur = System.currentTimeMillis() - start;
        morphium.fireProfilingWriteEvent(
            cls,
            update,
            dur,
            upsert,
            multiple ? WriteAccessType.BULK_UPDATE : WriteAccessType.SINGLE_UPDATE);
        morphium.getCache().clearCacheIfNecessary(cls);
        morphium.firePostUpdateEvent(
            morphium.getARHelper().getRealClass(cls),
            push
            ? MorphiumStorageListener.UpdateTypes.PUSH
            : MorphiumStorageListener.UpdateTypes.PULL);
    }

    @Override
    public <T> void pushPullAll(
        final MorphiumStorageListener.UpdateTypes type,
        final Query<T> query,
        final String f,
        final List<?> v,
        final boolean upsert,
        final boolean multiple,
        AsyncOperationCallback<T> callback) {
        WriterTask r =
        new WriterTask() {
            private AsyncOperationCallback<T> callback;
            @Override
            public void setCallback(AsyncOperationCallback cb) {
                callback = cb;
            }
            @SuppressWarnings("CommentedOutCode")
            @Override
            public void run() {
                List<?> value = v;
                String field = f;
                Class<?> cls = query.getType();
                String coll = query.getCollectionName();
                morphium.firePreUpdateEvent(morphium.getARHelper().getRealClass(cls), type);
                long start = System.currentTimeMillis();
                value =
                    value.stream()
                    .map(o -> marshallIfNecessary(o))
                    .collect(Collectors.toList());

                try {
                    Map<String, Object> qobj = query.toQueryObject();

                    if (upsert) {
                        qobj = morphium.simplifyQueryObject(qobj);
                    }

                    field = morphium.getARHelper().getMongoFieldName(cls, field);
                    Map<String, Object> update;

                    switch (type) {
                        case PULL:
                            update = UtilsMap.of("$pullAll", UtilsMap.of(field, value));
                            break;

                        case ADD_TO_SET:
                            Map<String, Object> set =
                                UtilsMap.of(field, UtilsMap.of("$each", value));
                            update = UtilsMap.of("$add_to_set", set);
                            break;

                        case PUSH:
                            set = UtilsMap.of(field, UtilsMap.of("$each", value));
                            update = UtilsMap.of("$push", set);
                            break;

                        default:
                            throw new IllegalArgumentException(
                                "Unsupported update type " + type.name());
                    }

                    if (type.equals(MorphiumStorageListener.UpdateTypes.PULL)) {
                    } else {
                    }

                    handleLastChange(cls, update);
                    handleCreationTimeOnUpsert(
                        cls, coll, query.toQueryObject(), update, upsert);
                    WriteConcern wc = morphium.getWriteConcernForClass(cls);
                    MongoConnection con = null;

                    try {
                        if (upsert) {
                            checkIndexAndCaps(cls, coll, callback);
                        }

                        con = morphium.getDriver().getPrimaryConnection(wc);
                        UpdateMongoCommand settings =
                            new UpdateMongoCommand(con)
                        .setColl(coll)
                        .setDb(getDbName())
                        .setWriteConcern(wc.asMap())
                        .addUpdate(
                            Doc.of(qobj),
                            Doc.of(update),
                            null,
                            upsert,
                            multiple,
                            query.getCollation(),
                            null,
                            null);

                        if (query.getSort() != null) {
                            logger.warn("Sort is not supported for updates!!!");
                        }

                        settings.execute();
                        morphium.inc(StatisticKeys.WRITES);
                    } catch (MorphiumDriverException e) {
                        // TODO: Implement Handling
                        throw new RuntimeException(e);
                    } finally {
                        if (con != null) {
                            con.release();
                        }
                    }

                    long dur = System.currentTimeMillis() - start;
                    morphium.fireProfilingWriteEvent(
                        cls,
                        update,
                        dur,
                        upsert,
                        multiple
                        ? WriteAccessType.BULK_UPDATE
                        : WriteAccessType.SINGLE_UPDATE);
                    morphium.getCache().clearCacheIfNecessary(cls);
                    morphium.firePostUpdateEvent(
                        query.getType(), MorphiumStorageListener.UpdateTypes.PUSH);

                    if (callback != null) {
                        callback.onOperationSucceeded(
                            type.equals(MorphiumStorageListener.UpdateTypes.PULL) ? AsyncOperationType.PULL : AsyncOperationType.PUSH,
                            query,
                            System.currentTimeMillis() - start,
                            null,
                            null,
                            field,
                            value,
                            upsert,
                            multiple);
                    }
                } catch (RuntimeException e) {
                    if (callback == null) {
                        throw new RuntimeException(e);
                    }

                    callback.onOperationError(
                        type.equals(MorphiumStorageListener.UpdateTypes.PULL) ? AsyncOperationType.PULL : AsyncOperationType.PUSH,
                        query,
                        System.currentTimeMillis() - start,
                        e.getMessage(),
                        e,
                        null,
                        field,
                        value,
                        upsert,
                        multiple);
                }
            }
        };
        submitAndBlockIfNecessary(callback, r);
    }

    @Override
    public <T> void dropCollection(
        final Class<T> cls, final String collection, AsyncOperationCallback<T> callback) {
        if (morphium.getARHelper().isAnnotationPresentInHierarchy(cls, Entity.class)) {
            WriterTask r =
            new WriterTask() {
                @Override
                public void setCallback(AsyncOperationCallback cb) {}
                public void run() {
                    morphium.firePreDrop(cls);
                    long start = System.currentTimeMillis();
                    String co = collection;

                    if (co == null) {
                        co = morphium.getMapper().getCollectionName(cls);
                    }

                    MongoConnection con = null;

                    try {
                        con = morphium.getDriver().getPrimaryConnection(null);
                        DropMongoCommand settings =
                            new DropMongoCommand(con).setColl(co).setDb(getDbName());
                        settings.execute();
                    } catch (MorphiumDriverException e) {
                        if (e.getMessage().endsWith("error: 26 - ns not found")) {
                            LoggerFactory.getLogger(MorphiumWriterImpl.class)
                            .warn(
                                "NS not found: "
                                + morphium.getMapper()
                                .getCollectionName(cls));
                        } else {
                            throw new RuntimeException(e);
                        }
                    } finally {
                        if (con != null) {
                            con.release();
                        }
                    }

                    long dur = System.currentTimeMillis() - start;
                    morphium.fireProfilingWriteEvent(
                        cls, null, dur, false, WriteAccessType.DROP);
                    morphium.firePostDropEvent(cls);
                }
            };
            submitAndBlockIfNecessary(callback, r);
        } else {
            throw new RuntimeException("No entity class: " + cls.getName());
        }
    }

    @Override
    public <T> void createIndex(
        final Class<T> cls,
        final String collection,
        final IndexDescription idesc,
        AsyncOperationCallback<T> callback) {
        WriterTask r =
        new WriterTask() {
            @Override
            public void setCallback(AsyncOperationCallback cb) {}
            @Override
            public void run() {
                List<String> fields = morphium.getARHelper().getFields(cls);
                // replace keys with matching fieldnames
                Map<String, Object> idx = new LinkedHashMap<>();

                for (Map.Entry<String, Object> es : idesc.getKey().entrySet()) {
                    String k = es.getKey();

                    if (!k.contains(".") && !fields.contains(k) && !fields.contains( morphium.getARHelper().convertCamelCase(k))) {
                        throw new IllegalArgumentException(
                            "Field unknown for type " + cls.getSimpleName() + ": " + k);
                    }

                    String fn = morphium.getARHelper().getMongoFieldName(cls, k);
                    idx.put(fn, es.getValue());
                }

                long start = System.currentTimeMillis();
                Map<String, Object> keys = new LinkedHashMap<>(idx);
                String coll = collection;

                if (coll == null) {
                    coll = morphium.getMapper().getCollectionName(cls);
                }

                MongoConnection con = null;

                try {
                    con = morphium.getDriver().getPrimaryConnection(null);
                    CreateIndexesCommand cmd = new CreateIndexesCommand(con);
                    cmd.setDb(getDbName()).setColl(coll);
                    idesc.setKey(keys);
                    cmd.addIndex(idesc);
                    var res = cmd.execute();

                    if (res.containsKey("ok") && res.get("ok").equals(Double.valueOf(0))) {
                        if (((String) res.get("errmsg")).contains("Index already exists with a different name")) {
                            logger.warn("could not create index - already exists");
                        } else {
                            throw new MorphiumDriverException((String) res.get("errmsg"));
                        }
                    }
                } catch (MorphiumDriverException e) {
                    throw new RuntimeException(e);
                } finally {
                    if (con != null) {
                        con.release();
                    }
                }

                long dur = System.currentTimeMillis() - start;
                morphium.fireProfilingWriteEvent(
                    cls, keys, dur, false, WriteAccessType.ENSURE_INDEX);
            }
        };
        submitAndBlockIfNecessary(callback, r);
    }

    @Override
    public int writeBufferCount() {
        return executor.getActiveCount();
    }

    @Override
    public void onShutdown(Morphium m) {
        if (executor != null) {
            try {
                executor.shutdownNow();
            } catch (Exception e) {
                // swallow
            }
        }
    }

    public abstract class WT<T> implements WriterTask<T> {
        private AsyncOperationCallback<T> callback;

        @Override
        public void setCallback(AsyncOperationCallback cb) {
            callback = cb;
        }

        @SuppressWarnings("CommentedOutCode")
        public void doUpdate(
            Class cls,
            T toSet,
            String coll,
            String field,
            Map<String, Object> query,
            Field f,
            Map<String, Object> update,
            WriteConcern wc) {
            long start = System.currentTimeMillis();

            if (coll == null) {
                coll = morphium.getMapper().getCollectionName(cls);
            }

            MongoConnection con = null;

            try {
                checkIndexAndCaps(cls, coll, callback);
                con = morphium.getDriver().getPrimaryConnection(null);
                UpdateMongoCommand settings = new UpdateMongoCommand(con).setColl(coll).setDb(getDbName()).addUpdate(
                    Doc.of(query), Doc.of(update), null, false, false, null, null, null);

                if (wc != null) {
                    settings.setWriteConcern(wc.asMap());
                }

                settings.execute();
                morphium.inc(StatisticKeys.WRITES);
                handleLastChange(cls, update);
                long dur = System.currentTimeMillis() - start;
                morphium.fireProfilingWriteEvent(toSet.getClass(), update, dur, false, WriteAccessType.SINGLE_UPDATE);
                morphium.getCache().clearCacheIfNecessary(cls);

                try {
                    if (f != null) {
                        f.set(toSet, null);
                    }
                } catch (IllegalAccessException e) {
                    // May happen, if null is not allowed for example
                } finally {
                    if (con != null) {
                        con.release();
                    }
                }

                morphium.firePostUpdateEvent(
                    morphium.getARHelper().getRealClass(cls),
                    MorphiumStorageListener.UpdateTypes.UNSET);

                if (callback != null) {
                    callback.onOperationSucceeded(AsyncOperationType.UNSET, null, System.currentTimeMillis() - start, null, toSet, field);
                }
            } catch (Exception e) {
                if (callback == null) {
                    throw new RuntimeException(e);
                }

                callback.onOperationError(AsyncOperationType.UNSET, null, System.currentTimeMillis() - start, e.getMessage(), e, toSet, field);
            }
        }
    }
}