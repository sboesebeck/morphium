package de.caluga.morphium.writer;

import de.caluga.morphium.*;
import de.caluga.morphium.Collation;
import de.caluga.morphium.annotations.*;
import de.caluga.morphium.async.AsyncOperationCallback;
import de.caluga.morphium.async.AsyncOperationType;
import de.caluga.morphium.bulk.MorphiumBulkContext;
import de.caluga.morphium.driver.MorphiumDriver;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.driver.WriteConcern;
import de.caluga.morphium.driver.bulk.BulkRequestContext;
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
 * User: Stephan Bösebeck
 * Date: 30.08.12
 * Time: 14:38
 * <p>
 * default writer implementation - uses a ThreadPoolExecutor for execution of asynchornous calls
 * maximum Threads are limited to 0.9* MaxConnections configured in MorphiumConfig
 *
 * @see MorphiumWriter
 */
@SuppressWarnings({"ConstantConditions", "unchecked", "ConfusingArgumentToVarargsMethod", "WeakerAccess"})
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

    @Override
    public void setMorphium(Morphium m) {
        morphium = m;
        if (m != null) {
            BlockingQueue<Runnable> queue = new LinkedBlockingQueue<Runnable>() {
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
            if (core <= 1) core = 1;
            int max = m.getConfig().getMaxConnections() * m.getConfig().getThreadConnectionMultiplier();
            if (max <= core) max = 2 * core;

            executor = new ThreadPoolExecutor(core, max,
                    60L, TimeUnit.SECONDS,
                    queue);

            executor.setRejectedExecutionHandler((r, executor) -> {
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
            //                public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
            //                    logger.warn("Could not schedule...");
            //                }
            //            });
            executor.setThreadFactory(new ThreadFactory() {
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
        executor.shutdownNow();
    }


    @Override
    public <T> void insert(List<T> lst, AsyncOperationCallback<T> callback) {

        if (!lst.isEmpty()) {
            WriterTask r = new WriterTask() {
                private AsyncOperationCallback<T> callback;

                @Override
                public void setCallback(AsyncOperationCallback cb) {
                    callback = cb;
                }

                @Override
                public void run() {
                    //                    System.out.println(System.currentTimeMillis()+" -  storing" );
                    HashMap<Class, List<Object>> sorted = new HashMap<>();
                    for (Object o : lst) {
                        Class type = morphium.getARHelper().getRealClass(o.getClass());
                        if (!morphium.getARHelper().isAnnotationPresentInHierarchy(type, Entity.class)) {
                            logger.error("Not an entity! Storing not possible! Even not in list!");
                            continue;
                        }
                        morphium.inc(StatisticKeys.WRITES);

                        o = morphium.getARHelper().getRealObject(o);
                        if (o == null) {
                            logger.warn("Illegal Reference? - cannot store Lazy-Loaded / Partial Update Proxy without delegate!");
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

                        handleStringIds(o);

                        morphium.firePreStore(o, true);
                    }
                    long allStart = System.currentTimeMillis();
                    try {
                        for (Map.Entry<Class, List<Object>> es : sorted.entrySet()) {
                            Class c = es.getKey();
                            ArrayList<Map<String, Object>> dbLst = new ArrayList<>();
                            //bulk insert... check if something already exists
                            WriteConcern wc = morphium.getWriteConcernForClass(c);
                            String coll = morphium.getMapper().getCollectionName(c);

                            createIndexAndCaps(c, coll, callback);

                            //HashMap<Integer, Object> mapMarshalledNewObjects = new HashMap<>();
                            for (Object record : es.getValue()) {
                                setIdIfNull(record);
                                Map<String, Object> marshall = morphium.getMapper().serialize(record);
                                dbLst.add(marshall);
                                //mapMarshalledNewObjects.put(dbLst.size() - 1, record);
                            }

                            long start = System.currentTimeMillis();
                            if (!dbLst.isEmpty()) {

                                morphium.getDriver().insert(morphium.getConfig().getDatabase(), coll, dbLst, wc);
                                int idx = 0;

                                //                                System.out.println(System.currentTimeMillis()+" -  finish" );
                            }
                            morphium.getCache().clearCacheIfNecessary(c);
                            long dur = System.currentTimeMillis() - start;
                            //bulk insert
                            morphium.fireProfilingWriteEvent(c, dbLst, dur, true, WriteAccessType.BULK_INSERT);
                            //null because key changed => mongo _id
                            es.getValue().forEach(record -> { //null because key changed => mongo _id
                                morphium.firePostStore(record, true);
                            });
                        }

                        if (callback != null) {
                            callback.onOperationSucceeded(AsyncOperationType.WRITE, null, System.currentTimeMillis() - allStart, null, null, lst);
                        }
                    } catch (Exception e) {
                        if (e instanceof RuntimeException) {
                            throw (RuntimeException) e;
                        }
                        if (callback == null) {
                            throw new RuntimeException(e);
                        }
                        callback.onOperationError(AsyncOperationType.WRITE, null, System.currentTimeMillis() - allStart, e.getMessage(), e, null, lst);
                    }
                    //                    System.out.println(System.currentTimeMillis()+" -  finish" );
                }
            };
            submitAndBlockIfNecessary(callback, r);
        }

    }

    private void setIdIfNull(Object record) throws IllegalAccessException {
        Field idf = morphium.getARHelper().getIdField(record);
        if (idf.get(record) != null) return;
        if (idf.get(record) == null && idf.getType().equals(MorphiumId.class)) {
            idf.set(record, new MorphiumId());
        } else if (idf.get(record) == null && idf.getType().equals(ObjectId.class)) {
            idf.set(record, new ObjectId());
        } else if (idf.get(record) == null && idf.getType().equals(String.class)) {
            idf.set(record, new MorphiumId().toString());
        } else {
            throw new IllegalArgumentException("Cannot set ID of non-ID-Type");
        }
    }


    private void handleStringIds(Object o) {
        if (!morphium.getARHelper().getIdField(o).getType().equals(MorphiumId.class) && morphium.getId(o) == null) {
            if (morphium.getARHelper().getIdField(o).getType().equals(String.class)) {
                try {
                    morphium.getARHelper().getIdField(o).set(o, new MorphiumId());
                } catch (IllegalAccessException e) {
                    throw new RuntimeException("Could not sett id-string", e);
                }
            } else {
                throw new IllegalArgumentException("Cannot automatically set non -MongoId IDs");
            }
        }
    }


    @Override
    public <T> void insert(List<T> lst, String cn, AsyncOperationCallback<T> callback) {
        if (!lst.isEmpty()) {
            WriterTask r = new WriterTask() {

                private AsyncOperationCallback<T> callback;

                @Override
                public void setCallback(AsyncOperationCallback cb) {
                    callback = cb;
                }

                @Override
                public void run() {
                    try {
                        String collectionName = cn;
                        if (lst == null || lst.isEmpty()) {
                            return;
                        }
                        if (collectionName == null) {
                            collectionName = morphium.getMapper().getCollectionName(lst.get(0).getClass());
                        }
                        ArrayList<Map<String, Object>> dbLst = new ArrayList<>();
                        //        DBCollection collection = morphium.getDbName().getCollection(collectionName);
                        WriteConcern wc = morphium.getWriteConcernForClass(lst.get(0).getClass());

                        //        BulkWriteOperation bulkWriteOperation = collection.initializeUnorderedBulkOperation();
                        //                        BulkRequestContext bulk = morphium.getDriver().createBulkContext(morphium, morphium.getConfig().getDatabase(), collectionName, false, wc);
                        HashMap<Object, Boolean> isNew = new HashMap<>();
                        for (Object o : lst) {
                            isNew.put(o, true);
                            try {
                                setIdIfNull(o);
                            } catch (IllegalAccessException e) {
                                throw new RuntimeException(e);
                            }
                            dbLst.add(morphium.getMapper().serialize(o));
                        }
                        if (morphium.getConfig().isAutoIndexAndCappedCreationOnWrite() && !morphium.getDriver().exists(morphium.getConfig().getDatabase(), collectionName)) {
                            logger.warn("collection does not exist while storing list -  taking first element of list to ensure indices");
                            createCappedCollationColl(lst.get(0).getClass());
                            morphium.ensureIndicesFor((Class<T>) lst.get(0).getClass(), collectionName, callback);
                        }
                        long start = System.currentTimeMillis();
                        //                        int cnt = 0;
                        morphium.firePreStore(isNew);


                        long dur = System.currentTimeMillis() - start;
                        morphium.fireProfilingWriteEvent(lst.get(0).getClass(), lst, dur, true, WriteAccessType.BULK_UPDATE);

                        start = System.currentTimeMillis();

                        morphium.getDriver().insert(morphium.getConfig().getDatabase(), collectionName, dbLst, wc);
                        dur = System.currentTimeMillis() - start;
                        List<Class> cleared = new ArrayList<>();
                        for (Object o : lst) {
                            if (cleared.contains(o.getClass())) {
                                continue;
                            }
                            cleared.add(o.getClass());
                            morphium.getCache().clearCacheIfNecessary(o.getClass());
                        }
                        //bulk insert
                        morphium.fireProfilingWriteEvent(lst.get(0).getClass(), dbLst, dur, true, WriteAccessType.BULK_INSERT);
                        morphium.firePostStore(isNew);
                    } catch (MorphiumDriverException e) {
                        //TODO: Implement Handling
                        throw new RuntimeException(e);
                    }
                }
            };
            submitAndBlockIfNecessary(callback, r);
        }
    }


    @Override
    public <T> void insert(final T obj, final String collection, AsyncOperationCallback<T> callback) {
        if (obj instanceof List) {
            insert((List) obj, callback);
            return;
        }
        WriterTask r = new WriterTask() {
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
                    if (!morphium.getARHelper().isAnnotationPresentInHierarchy(type, Entity.class)) {
                        throw new RuntimeException("Not an entity: " + type.getSimpleName() + " Storing not possible!");
                    }
                    morphium.inc(StatisticKeys.WRITES);


                    o = morphium.getARHelper().getRealObject(o);
                    if (o == null) {
                        logger.warn("Illegal Reference? - cannot store Lazy-Loaded / Partial Update Proxy without delegate!");
                        return;
                    }
                    if (morphium.isAutoValuesEnabledForThread()) {
                        morphium.setAutoValues(o);
                    }
                    morphium.firePreStore(o, true);

                    setIdIfNull(o);
                    Entity en = morphium.getARHelper().getAnnotationFromHierarchy(type, Entity.class);
                    if (en.autoVersioning()) {
                        morphium.getARHelper().setValue(o, 1, morphium.getARHelper().getFields(type, Version.class).get(0));
                    }

                    Map<String, Object> marshall = morphium.getMapper().serialize(o);

                    String coll = collection;
                    if (coll == null) {
                        coll = morphium.getMapper().getCollectionName(type);
                    }
                    createIndexAndCaps(type, coll, callback);

                    WriteConcern wc = morphium.getWriteConcernForClass(type);
                    List<Map<String, Object>> objs = new ArrayList<>();
                    objs.add(marshall);
                    try {
                        morphium.getDriver().insert(morphium.getConfig().getDatabase(), coll, objs, wc);
                    } catch (Throwable t) {
                        throw new RuntimeException(t);
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
                            throw ((RuntimeException) e);
                        }
                        throw new RuntimeException(e);
                    }
                    callback.onOperationError(AsyncOperationType.WRITE, null, System.currentTimeMillis() - start, e.getMessage(), e, obj);
                }
            }


        };
        submitAndBlockIfNecessary(callback, r);
    }

    private <T> void createIndexAndCaps(Class type, String coll, AsyncOperationCallback<T> callback) throws MorphiumDriverException {
        if (morphium.getConfig().isAutoIndexAndCappedCreationOnWrite() && !morphium.getDriver().exists(getDbName(), coll)) {
            if (logger.isDebugEnabled()) {
                logger.debug("Collection " + coll + " does not exist - ensuring indices");
            }
            createCappedCollationColl(type);
            morphium.ensureIndicesFor(type, coll, callback);
        }
    }


    /**
     * @param obj - object to store
     */
    @Override
    public <T> void store(final T obj, final String collection, AsyncOperationCallback<T> callback) {
        if (obj instanceof List) {
            store((List) obj, collection, callback);
            return;
        }
        WriterTask r = new WriterTask() {
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
                    if (!morphium.getARHelper().isAnnotationPresentInHierarchy(type, Entity.class)) {
                        throw new RuntimeException("Not an entity: " + type.getSimpleName() + " Storing not possible!");
                    }
                    Entity en = morphium.getARHelper().getAnnotationFromHierarchy(o.getClass(), Entity.class);
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
                    createIndexAndCaps(type, coll, callback);

                    WriteConcern wc = morphium.getWriteConcernForClass(type);
                    List<Map<String, Object>> objs = new ArrayList<>();
                    objs.add(marshall);
                    Map<String, Object> ret;
                    try {
                        ret = morphium.getDriver().store(morphium.getConfig().getDatabase(), coll, objs, wc);
                    } catch (MorphiumDriverException mde) {
                        if (mde.getMessage().contains("duplicate key") && mde.getMessage().contains("_id") && en.autoVersioning()) {
                            throw new ConcurrentModificationException("Versioning / upsert failure - concurrent modification!");
                        } else {
                            throw mde;
                        }
                    } catch (Throwable t) {
                        throw new RuntimeException(t);
                    }
                    if (en.autoVersioning()) {
                        if (((Integer) ret.get("total")) < ((Integer) ret.get("modified"))) {
                            throw new ConcurrentModificationException("versioning failure");
                        }

                        if (en.autoVersioning()) {
                            String fld = morphium.getARHelper().getFields(type, Version.class).get(0);
                            Long v = morphium.getARHelper().getLongValue(o, fld);
                            v = v + 1;
                            morphium.getARHelper().setValue(o, v, fld);
                        }
                    }
                    long dur = System.currentTimeMillis() - start;
                    morphium.fireProfilingWriteEvent(o.getClass(), marshall, dur, true, WriteAccessType.SINGLE_INSERT);
                    //                    if (logger.isDebugEnabled()) {
                    //                        String n = "";
                    //                        if (isNew) {
                    //                            n = "NEW ";
                    //                        }
                    //                        logger.debug(n + "stored " + type.getSimpleName() + " after " + dur + " ms length:" + serialize.toString().length());
                    //                    }
                    if (isNew) {
                        List<String> flds = morphium.getARHelper().getFields(o.getClass(), Id.class);
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
                            throw ((RuntimeException) e);
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
                //using reflection to get fields etc... in order to remove strong dependency
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
                        Iterable<?> pth = (Iterable) m.invoke(v);
                        StringBuilder stringBuilder = new StringBuilder();
                        for (Object p : pth) {
                            m = p.getClass().getMethod("getName");
                            String name = (String) m.invoke(p);
                            stringBuilder.append(".");
                            stringBuilder.append(name);
                        }
                        logger.error("Validation of " + type + " failed: " + msg + " - Invalid Value: " + invalidValue + " for path: " + stringBuilder.toString() + "\n Tried to store: " + s);
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
            WriterTask r = new WriterTask() {

                private AsyncOperationCallback<T> callback;

                @Override
                public void setCallback(AsyncOperationCallback cb) {
                    callback = cb;
                }

                @Override
                public void run() {
                    try {
                        String collectionName = cln;
                        if (lst == null || lst.isEmpty()) {
                            return;
                        }
                        ArrayList<Map<String, Object>> dbLst = new ArrayList<>();
                        //        DBCollection collection = morphium.getDbName().getCollection(collectionName);
                        WriteConcern wc = morphium.getWriteConcernForClass(lst.get(0).getClass());

                        //        BulkWriteOperation bulkWriteOperation = collection.initializeUnorderedBulkOperation();
                        //                        BulkRequestContext bulk = morphium.getDriver().createBulkContext(morphium, morphium.getConfig().getDatabase(), collectionName, false, wc);
                        HashMap<Object, Boolean> isNew = new HashMap<>();
                        if (morphium.getConfig().isAutoIndexAndCappedCreationOnWrite() && !morphium.getDriver().exists(morphium.getConfig().getDatabase(), collectionName)) {
                            logger.warn("collection does not exist while storing list -  taking first element of list to ensure indices");
                            createCappedCollationColl(lst.get(0).getClass());
                            morphium.ensureIndicesFor((Class<T>) lst.get(0).getClass(), collectionName, callback);
                        }
                        long start = System.currentTimeMillis();
                        //                        int cnt = 0;
                        List<Class<?>> types = new ArrayList<>();
                        MorphiumBulkContext ctx = morphium.createBulkRequestContext(collectionName, false);
                        for (Object record : lst) {

                            Object id = morphium.getARHelper().getId(record);
                            boolean isn = id == null;
                            Object reread;
                            if (morphium.isAutoValuesEnabledForThread()) {
                                CreationTime creationTime = morphium.getARHelper().getAnnotationFromHierarchy(record.getClass(), CreationTime.class);
                                if (!isn && (morphium.getConfig().isCheckForNew() || (creationTime != null && creationTime.checkForNew()))
                                        && !morphium.getARHelper().getIdField(record).getType().equals(MorphiumId.class)) {
                                    //check if it exists
                                    reread = morphium.findById(record.getClass(), id);
                                    isn = reread == null;
                                }
                                try {
                                    isn = morphium.setAutoValues(record);
                                } catch (IllegalAccessException e) {
                                    logger.error(e.getMessage(), e);
                                }
                            }
                            isNew.put(record, isn);

                            if (!types.contains(record.getClass())) {
                                types.add(record.getClass());
                            }
                            try {
                                setIdIfNull(record);
                            } catch (IllegalAccessException e) {
                                throw new RuntimeException(e);
                            }

                            boolean av = false;
                            Map<String, Object> marshall = morphium.getMapper().serialize(record);
                            Entity en = morphium.getARHelper().getAnnotationFromHierarchy(record.getClass(), Entity.class);
                            if (en != null) {
                                av = en.autoVersioning();
                            }
                            if (av) {
                                Long version = morphium.getARHelper().getVersion(record);
                                marshall.put(MorphiumDriver.VERSION_NAME, version + 1);
                                ctx.addSetRequest(morphium.createQueryFor(record.getClass()).f("_id").eq(morphium.getARHelper().getId(record)).f(MorphiumDriver.VERSION_NAME).eq(version), marshall, false, false);
                            } else {

                                dbLst.add(marshall);
                            }
                        }

                        if (ctx.getNumberOfRequests() > 0) {
                            Map result = ctx.runBulk();
                            /**
                             * res.put("num_del", delCount);
                             *         res.put("num_matched", matchedCount);
                             *         res.put("num_insert", insertCount);
                             *         res.put("num_modified", modifiedCount);
                             *         res.put("num_upserts", upsertCount);
                             */
                            if (!result.get("num_modified").equals((long) ctx.getNumberOfRequests())) {
                                throw new RuntimeException("modification failed - concurrent modification?");
                            }
                            for (Object record : lst) {
                                boolean av = false;
                                Entity en = morphium.getARHelper().getAnnotationFromHierarchy(record.getClass(), Entity.class);
                                if (en != null) {
                                    av = en.autoVersioning();
                                }
                                if (av) {
                                    try {
                                        morphium.getARHelper().getVersionField(record).set(record, morphium.getARHelper().getVersion(record) + 1);
                                    } catch (IllegalAccessException e) {
                                        throw new RuntimeException(e);
                                    }
                                }
                            }
                        }
                        morphium.firePreStore(isNew);


                        long dur = System.currentTimeMillis() - start;
                        morphium.fireProfilingWriteEvent(lst.get(0).getClass(), lst, dur, false, WriteAccessType.BULK_UPDATE);

                        start = System.currentTimeMillis();
                        if (collectionName == null) {
                            collectionName = morphium.getMapper().getCollectionName(lst.get(0).getClass());
                        }


                        morphium.getDriver().store(morphium.getConfig().getDatabase(), collectionName, dbLst, wc);
                        dur = System.currentTimeMillis() - start;
                        for (Class<?> c : types) {
                            morphium.getCache().clearCacheIfNecessary(c);
                        }
                        //bulk insert
                        morphium.fireProfilingWriteEvent(lst.get(0).getClass(), dbLst, dur, true, WriteAccessType.BULK_INSERT);
                        morphium.firePostStore(isNew);
                    } catch (MorphiumDriverException e) {
                        //TODO: Implement Handling
                        throw new RuntimeException(e);
                    }
                }
            };
            submitAndBlockIfNecessary(callback, r);
        }
    }

    @Override
    public void flush() {
        //nothing to do
    }

    @Override
    public void flush(Class type) {
        //nothing to do here
    }

    @Override
    public <T> void store(final List<T> lst, AsyncOperationCallback<T> callback) {

        if (!lst.isEmpty()) {
            WriterTask r = new WriterTask() {
                private AsyncOperationCallback<T> callback;

                @Override
                public void setCallback(AsyncOperationCallback cb) {
                    callback = cb;
                }

                @Override
                public void run() {
                    //                    System.out.println(System.currentTimeMillis()+" -  storing" );
                    HashMap<Class, List<Object>> sorted = new HashMap<>();
                    HashMap<Object, Boolean> isNew = new HashMap<>();
                    for (Object o : lst) {
                        Class type = morphium.getARHelper().getRealClass(o.getClass());
                        if (!morphium.getARHelper().isAnnotationPresentInHierarchy(type, Entity.class)) {
                            logger.error("Not an entity! Storing not possible! Even not in list!");
                            continue;
                        }
                        morphium.inc(StatisticKeys.WRITES);
                        o = morphium.getARHelper().getRealObject(o);
                        if (o == null) {
                            logger.warn("Illegal Reference? - cannot store Lazy-Loaded / Partial Update Proxy without delegate!");
                            return;
                        }

                        sorted.putIfAbsent(o.getClass(), new ArrayList<>());
                        sorted.get(o.getClass()).add(o);
                        boolean isn = morphium.getId(o) == null;
                        if (morphium.getARHelper().isAnnotationPresentInHierarchy(o.getClass(), CreationTime.class)) {
                            try {
                                isn = morphium.setAutoValues(o);
                            } catch (IllegalAccessException e) {
                                logger.error(e.getMessage(), e);
                            }
                        }
                        if (isn) {
                            handleStringIds(o);
                            isNew.put(o, true);
                        } else {
                            isNew.put(o, false);
                        }
                        morphium.firePreStore(o, isNew.get(o));
                    }
                    long allStart = System.currentTimeMillis();
                    try {
                        for (Map.Entry<Class, List<Object>> es : sorted.entrySet()) {
                            Class c = es.getKey();
                            ArrayList<Map<String, Object>> dbLst = new ArrayList<>();
                            //bulk insert... check if something already exists
                            WriteConcern wc = morphium.getWriteConcernForClass(c);
                            String coll = morphium.getMapper().getCollectionName(c);

                            if (morphium.getConfig().isAutoIndexAndCappedCreationOnWrite() && !morphium.getDriver().exists(morphium.getConfig().getDatabase(), coll)) {
                                createCappedCollationColl(c, coll);
                                morphium.ensureIndicesFor(c, coll, callback);
                            }

                            Entity en = morphium.getARHelper().getAnnotationFromHierarchy(c, Entity.class);
                            //HashMap<Integer, Object> mapMarshalledNewObjects = new HashMap<>();
                            for (Object record : es.getValue()) {
                                setIdIfNull(record);
                                Map<String, Object> marshall = morphium.getMapper().serialize(record);
                                dbLst.add(marshall);
                                //mapMarshalledNewObjects.put(dbLst.size() - 1, record);
                            }

                            long start = System.currentTimeMillis();
                            if (!dbLst.isEmpty()) {
                                //                                System.out.println(System.currentTimeMillis()+" -  driver call" );

                                Map<String, Object> ret = morphium.getDriver().store(morphium.getConfig().getDatabase(), coll, dbLst, wc);
                                if (en.autoVersioning()) {
                                    if (((Integer) ret.get("total")) < ((Integer) ret.get("modified"))) {
                                        throw new ConcurrentModificationException("versioning failure");
                                    }
                                }
                                //                                System.out.println(System.currentTimeMillis()+" -  driver finish" );
                                //                                doStoreList(dbLst, wc, coll);
                                //                                System.out.println(System.currentTimeMillis()+" -  updating ids" );
                                int idx = 0;
                                //                                System.out.println(System.currentTimeMillis()+" -  finish" );
                            }
                            morphium.getCache().clearCacheIfNecessary(c);
                            long dur = System.currentTimeMillis() - start;
                            //bulk insert
                            morphium.fireProfilingWriteEvent(c, dbLst, dur, true, WriteAccessType.BULK_INSERT);
                            //null because key changed => mongo _id
                            es.getValue().forEach(record -> { //null because key changed => mongo _id
                                morphium.firePostStore(record, true);
                            });
                        }

                        if (callback != null) {
                            callback.onOperationSucceeded(AsyncOperationType.WRITE, null, System.currentTimeMillis() - allStart, null, null, lst);
                        }
                    } catch (Exception e) {
                        if (e instanceof RuntimeException) {
                            throw (RuntimeException) e;
                        }
                        if (callback == null) {
                            throw new RuntimeException(e);
                        }
                        callback.onOperationError(AsyncOperationType.WRITE, null, System.currentTimeMillis() - allStart, e.getMessage(), e, null, lst);
                    }
                    //                    System.out.println(System.currentTimeMillis()+" -  finish" );
                }
            };
            submitAndBlockIfNecessary(callback, r);
        }


    }

    private void createCappedCollationColl(Class c) {
        createCappedCollationColl(c, null);
    }

    private void createCappedCollationColl(Class c, String n) {
        if (logger.isDebugEnabled()) {
            logger.debug("Collection does not exist - ensuring indices / capped status / Schema validation");
        }


        Entity e = morphium.getARHelper().getAnnotationFromHierarchy(c, Entity.class);


        Map<String, Object> cmd = new LinkedHashMap<>();
        cmd.put("create", n != null ? n : morphium.getMapper().getCollectionName(c));
        Capped capped = morphium.getARHelper().getAnnotationFromHierarchy(c, Capped.class);
        if (capped != null) {
            cmd.put("capped", true);
            cmd.put("size", capped.maxSize());
            cmd.put("max", capped.maxEntries());
        }

        //cmd.put("autoIndexId", (morphium.getARHelper().getIdField(c).getType().equals(MorphiumId.class)));

        if (!e.schemaDef().equals("")) {
            JSONParser jsonParser = new JSONParser();
            try {
                Map<String, Object> def = (Map<String, Object>) jsonParser.parse(e.schemaDef(), new ContainerFactory() {
                    @Override
                    public Map createObjectContainer() {
                        return new HashMap<>();
                    }

                    @Override
                    public List creatArrayContainer() {
                        return new ArrayList();
                    }
                });
                cmd.put("validator", def);
                cmd.put("validationLevel", e.validationLevel().name());
                cmd.put("validationAction", e.validationAction().name());
            } catch (ParseException parseException) {
                parseException.printStackTrace();
            }

        }
        if (!e.comment().equals("")) {
            cmd.put("comment", e.comment());
        }
        de.caluga.morphium.annotations.Collation collation = morphium.getARHelper().getAnnotationFromHierarchy(c, de.caluga.morphium.annotations.Collation.class);
        if (collation != null) {
            Map<String, Object> map = new LinkedHashMap<>();

            map.put("locale", collation.locale());

            if (!collation.alternate().equals("")) {
                map.put("alternate", collation.alternate());
            }
            if (!collation.caseFirst().equals("")) {
                map.put("caseFirst", collation.caseFirst());
            }
            map.put("backwards", collation.backwards());
            map.put("caseLevel", collation.caseLevel());
            map.put("numericOrdering", collation.numericOrdering());
            map.put("strength", collation.strength());
            cmd.put("collation", map);
        }

        try {
            morphium.getDriver().runCommand(morphium.getConfig().getDatabase(), cmd);
        } catch (MorphiumDriverException ex) {
            //TODO: Implement Handling
            throw new RuntimeException(ex);
        }
    }


    /**
     * automatically convert the collection for the given type to a capped collection
     * only works if @Capped annotation is given for type
     *
     * @param c - type
     */
    @SuppressWarnings("unused")
    public <T> void convertToCapped(final Class<T> c) {
        convertToCapped(c, null);
    }

    public <T> void convertToCapped(final Class<T> c, final AsyncOperationCallback<T> callback) {
        Runnable r = () -> {
            WriteConcern wc = morphium.getWriteConcernForClass(c);
            String coll = morphium.getMapper().getCollectionName(c);
            try {
                if (morphium.getConfig().isAutoIndexAndCappedCreationOnWrite() && !morphium.getDriver().exists(morphium.getConfig().getDatabase(), coll)) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Collection does not exist - ensuring indices / capped status");
                    }
                    Map<String, Object> cmd = new LinkedHashMap<>();
                    cmd.put("create", coll);
                    Capped capped = morphium.getARHelper().getAnnotationFromHierarchy(c, Capped.class);
                    if (capped != null) {
                        cmd.put("capped", true);
                        cmd.put("size", capped.maxSize());
                        cmd.put("max", capped.maxEntries());
                    }
                    //cmd.put("autoIndexId", (morphium.getARHelper().getIdField(c).getType().equals(MorphiumId.class)));
                    morphium.getDriver().runCommand(getDbName(), cmd);
                } else {
                    Capped capped = morphium.getARHelper().getAnnotationFromHierarchy(c, Capped.class);
                    if (capped != null) {
                        Map<String, Object> cmd = new HashMap<>();
                        cmd.put("convertToCapped", coll);
                        cmd.put("size", capped.maxSize());
                        cmd.put("max", capped.maxEntries());
                        morphium.getDriver().runCommand(getDbName(), cmd);
                        //Indexes are not available after converting - recreate them
                        morphium.ensureIndicesFor(c, callback);
                    }
                }
            } catch (MorphiumDriverException e) {
                //TODO: Implement Handling
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
    private void executeWriteBatch(List<Object> es, Class c, WriteConcern wc, BulkRequestContext bulkCtx, long start) {
        try {
            bulkCtx.execute();
        } catch (MorphiumDriverException e) {
            //TODO: Implement Handling
            throw new RuntimeException(e);
        }
        long dur = System.currentTimeMillis() - start;
        morphium.fireProfilingWriteEvent(c, es, dur, false, WriteAccessType.BULK_UPDATE);
        morphium.getCache().clearCacheIfNecessary(c);
        morphium.firePostStore(es, false);
    }

    @Override
    public <T> void set(final T toSet, final String collection, final String field, final Object v, final boolean upsert, final boolean multiple, AsyncOperationCallback<T> callback) {
        WriterTask<T> r = new WriterTask<T>() {
            private AsyncOperationCallback<T> callback;

            @Override
            public void setCallback(AsyncOperationCallback cb) {
                callback = cb;
            }

            @Override
            public void run() {
                Class cls = toSet.getClass();
                Object value = v;
                morphium.firePreUpdateEvent(morphium.getARHelper().getRealClass(cls), MorphiumStorageListener.UpdateTypes.SET);
                value = marshallIfNecessary(value);
                if (collection == null) {
                    morphium.getMapper().getCollectionName(cls);
                }
                Map<String, Object> query = new HashMap<>();
                query.put("_id", morphium.getId(toSet));
                Field f = morphium.getARHelper().getField(cls, field);
                if (f == null) {
                    throw new RuntimeException("Unknown field: " + field);
                }
                String fieldName = morphium.getARHelper().getFieldName(cls, field);

                Map<String, Object> update;
                if (value != null) {
                    update = Utils.getMap("$set", Utils.getMap(fieldName, value.getClass().isEnum() ? value.toString() : value));
                } else {
                    update = Utils.getMap("$set", Utils.getMap(fieldName, null));
                }
                List<String> lastChangeFields = morphium.getARHelper().getFields(cls, LastChange.class);
                if (lastChangeFields != null && !lastChangeFields.isEmpty() && morphium.isAutoValuesEnabledForThread()) {
                    updateField(cls, update, lastChangeFields);
                }

                List<String> creationTimeFields = morphium.getARHelper().getFields(cls, CreationTime.class);
                if (upsert && creationTimeFields != null && !creationTimeFields.isEmpty()) {
                    long cnt;
                    try {
                        cnt = morphium.getDriver().count(getDbName(), collection, query, null, null);
                    } catch (MorphiumDriverException e) {
                        //TODO: Implement Handling
                        throw new RuntimeException(e);
                    }
                    if (cnt == 0) {
                        //not found, would insert
                        updateField(cls, update, creationTimeFields);
                    }
                }

                Entity en = morphium.getARHelper().getAnnotationFromHierarchy(toSet.getClass(), Entity.class);

                WriteConcern wc = morphium.getWriteConcernForClass(cls);
                long start = System.currentTimeMillis();

                try {
                    if (upsert && morphium.getConfig().isAutoIndexAndCappedCreationOnWrite() && !morphium.getDriver().exists(getDbName(), collection)) {
                        createCappedCollationColl(cls, collection);
                        morphium.ensureIndicesFor(cls, collection, callback);
                    }
                    if (en != null && en.autoVersioning()) {
                        List<String> versionFields = morphium.getARHelper().getFields(cls, Version.class);
                        query.put(MorphiumDriver.VERSION_NAME, morphium.getARHelper().getValue(toSet, versionFields.get(0)));
                        update.put(MorphiumDriver.VERSION_NAME, ((Long) morphium.getARHelper().getValue(toSet, versionFields.get(0))) + 1L);
                    }
                    Map<String, Object> res = morphium.getDriver().update(getDbName(), collection, query, update, multiple, upsert, null, wc);
                    if (en != null && en.autoVersioning() && res.get("modified").equals(0L)) {
                        throw new ConcurrentModificationException("could not modify");
                    }
                    long dur = System.currentTimeMillis() - start;
                    morphium.fireProfilingWriteEvent(cls, update, dur, false, WriteAccessType.SINGLE_UPDATE);
                    morphium.getCache().clearCacheIfNecessary(cls);
                    morphium.inc(StatisticKeys.WRITES);
                    try {
                        f.set(toSet, v);
                    } catch (IllegalAccessException e) {
                        throw new RuntimeException(e);
                    }
                    if (callback != null) {
                        callback.onOperationSucceeded(AsyncOperationType.SET, null, System.currentTimeMillis() - start, null, toSet, field, v);
                    }
                } catch (Exception e) {
                    if (callback == null) {
                        throw new RuntimeException(e);
                    }
                    callback.onOperationError(AsyncOperationType.SET, null, System.currentTimeMillis() - start, e.getMessage(), e, toSet, field, v);
                }
                morphium.firePostUpdateEvent(morphium.getARHelper().getRealClass(cls), MorphiumStorageListener.UpdateTypes.SET);
            }
        };
        submitAndBlockIfNecessary(callback, r);
    }

    private void updateField(Class cls, Map<String, Object> update, List<String> lastChangeFields) {
        for (String fL : lastChangeFields) {
            Field fld = morphium.getARHelper().getField(cls, fL);
            if (fld.getType().equals(Date.class)) {
                ((Map<String, Object>) update.get("$set")).put(fL, new Date());
            } else {
                ((Map<String, Object>) update.get("$set")).put(fL, System.currentTimeMillis());
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
                        throw new RuntimeException("Could not write - not even after " + maximumRetries + " retries and pause of " + pause + "ms", e);
                    }
                    if (logger.isDebugEnabled()) {
                        logger.warn("thread pool exceeded - waiting " + pause + " ms for the " + tries + ". time");
                    }
                    try {
                        Thread.sleep(pause);
                    } catch (InterruptedException ignored) {
                        //swallow
                    }

                }
            }
        }
    }

    @Override
    public <T> void updateUsingFields(final T ent, final String collection, AsyncOperationCallback<T> callback, final String... fields) {
        if (ent == null) {
            return;
        }
        WriterTask r = new WriterTask() {
            private AsyncOperationCallback<T> callback;

            @Override
            public void setCallback(AsyncOperationCallback cb) {
                callback = cb;
            }

            @Override
            public void run() {
                Object id = morphium.getARHelper().getId(ent);
                if (id == null) {
                    //new object - update not working
                    logger.warn("trying to partially update new object - storing it in full!");
                    store(ent, collection, callback);
                    return;
                }

                morphium.firePreStore(ent, false);
                morphium.inc(StatisticKeys.WRITES);

                Map<String, Object> find = new HashMap<>();

                find.put("_id", id);
                Map<String, Object> update = new HashMap<>();
                for (String f : fields) {
                    try {
                        Object value = morphium.getARHelper().getValue(ent, f);
                        Entity en = morphium.getARHelper().getAnnotationFromHierarchy(value.getClass(), Entity.class);
                        if (morphium.getARHelper().isAnnotationPresentInHierarchy(value.getClass(), Entity.class)) {
                            if (morphium.getARHelper().getField(ent.getClass(), f).getAnnotation(Reference.class) != null) {
                                //need to store reference
                                value = morphium.getARHelper().getId(ent);
                            } else {
                                value = morphium.getMapper().serialize(value);
                            }
                        }
                        if (en != null && en.translateCamelCase() || en != null && morphium.getConfig().isCamelCaseConversionEnabled()) {
                            f = morphium.getARHelper().convertCamelCase(f);
                        }
                        update.put(f, value);

                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
                Class<?> type = morphium.getARHelper().getRealClass(ent.getClass());

                LastChange t = morphium.getARHelper().getAnnotationFromHierarchy(type, LastChange.class); //(StoreLastChange) type.getAnnotation(StoreLastChange.class);
                if (t != null) {
                    List<String> lst = morphium.getARHelper().getFields(ent.getClass(), LastChange.class);

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


                update = Utils.getMap("$set", update);
                WriteConcern wc = morphium.getWriteConcernForClass(type);
                long start = System.currentTimeMillis();
                try {
                    String collectionName = collection;
                    if (collectionName == null) {
                        collectionName = morphium.getMapper().getCollectionName(ent.getClass());
                    }

                    if (morphium.getConfig().isAutoIndexAndCappedCreationOnWrite() && !morphium.getDriver().exists(getDbName(), collectionName)) {
                        createCappedCollationColl(ent.getClass(), collectionName);
                        morphium.ensureIndicesFor((Class<T>) ent.getClass(), collectionName, callback);
                    }
                    Entity en = morphium.getARHelper().getAnnotationFromHierarchy(ent.getClass(), Entity.class);
                    if (en != null && en.autoVersioning()) {
                        List<String> fl = morphium.getARHelper().getFields(ent.getClass(), Version.class);

                        find.put(MorphiumDriver.VERSION_NAME, morphium.getARHelper().getValue(ent, fl.get(0)));
                        update.put(MorphiumDriver.VERSION_NAME, ((Long) morphium.getARHelper().getValue(ent, fl.get(0))) + 1);
                    }
                    morphium.getDriver().update(getDbName(), collectionName, find, update, false, false, null, wc);

                    long dur = System.currentTimeMillis() - start;
                    morphium.fireProfilingWriteEvent(ent.getClass(), update, dur, false, WriteAccessType.SINGLE_UPDATE);
                    morphium.getCache().clearCacheIfNecessary(morphium.getARHelper().getRealClass(ent.getClass()));
                    morphium.firePostStore(ent, false);
                    if (callback != null) {
                        callback.onOperationSucceeded(AsyncOperationType.UPDATE, null, System.currentTimeMillis() - start, null, ent, (Object) fields);
                    }
                } catch (Exception e) {
                    if (callback == null) {
                        throw new RuntimeException(e);
                    }
                    callback.onOperationError(AsyncOperationType.UPDATE, null, System.currentTimeMillis() - start, e.getMessage(), e, ent, (Object) fields);
                }
            }
        };
        submitAndBlockIfNecessary(callback, r);
    }

    @SuppressWarnings("SuspiciousMethodCalls")
    @Override
    public <T> void remove(final List<T> lst, AsyncOperationCallback<T> callback) {
        WriterTask r = new WriterTask() {
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
                        @SuppressWarnings("MismatchedQueryAndUpdateOfCollection") List<Query<T>> queries = new ArrayList<>();
                        sortedMap.put((Class<T>) o.getClass(), queries);
                    }
                    Query<T> q = (Query<T>) morphium.createQueryFor(o.getClass());
                    q.f(morphium.getARHelper().getIdFieldName(o)).eq(morphium.getARHelper().getId(o));
                    sortedMap.get(o.getClass()).add(q);
                }
                morphium.firePreRemove(lst);
                long start = System.currentTimeMillis();
                try {
                    for (Class<T> cls : sortedMap.keySet()) {
                        Query<T> orQuery = morphium.createQueryFor(cls);
                        orQuery = orQuery.or(sortedMap.get(cls));
                        remove(orQuery, null); //sync call
                    }
                    morphium.inc(StatisticKeys.WRITES);
                    morphium.firePostRemove(lst);
                    if (callback != null) {
                        callback.onOperationSucceeded(AsyncOperationType.REMOVE, null, System.currentTimeMillis() - start, null, null, lst);
                    }
                } catch (Exception e) {
                    if (callback == null) {
                        throw new RuntimeException(e);
                    }
                    callback.onOperationError(AsyncOperationType.REMOVE, null, System.currentTimeMillis() - start, e.getMessage(), e, null, lst);
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
        WriterTask r = new WriterTask() {
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
                try {
                    String collectionName = q.getCollectionName();

                    morphium.getDriver().delete(getDbName(), collectionName, q.toQueryObject(), multiple, q.getCollation(), wc);
                    long dur = System.currentTimeMillis() - start;
                    morphium.fireProfilingWriteEvent(q.getType(), q.toQueryObject(), dur, false, WriteAccessType.BULK_DELETE);
                    morphium.inc(StatisticKeys.WRITES);
                    morphium.getCache().clearCacheIfNecessary(q.getType());
                    morphium.firePostRemoveEvent(q);
                    if (callback != null) {
                        callback.onOperationSucceeded(AsyncOperationType.REMOVE, q, System.currentTimeMillis() - start, null, null);
                    }
                } catch (Exception e) {
                    if (callback == null) {
                        throw new RuntimeException(e);
                    }
                    callback.onOperationError(AsyncOperationType.REMOVE, q, System.currentTimeMillis() - start, e.getMessage(), e, null);
                }
            }
        };
        submitAndBlockIfNecessary(callback, r);


    }

    @Override
    public <T> void remove(final T o, final String collection, AsyncOperationCallback<T> callback) {

        WriterTask r = new WriterTask() {
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
                try {
                    if (collection == null) {
                        morphium.getMapper().getCollectionName(o.getClass());
                    }
                    morphium.getDriver().delete(getDbName(), collection, db, false, null, wc);
                    long dur = System.currentTimeMillis() - start;
                    morphium.fireProfilingWriteEvent(o.getClass(), o, dur, false, WriteAccessType.SINGLE_DELETE);
                    morphium.clearCachefor(o.getClass());
                    morphium.inc(StatisticKeys.WRITES);
                    morphium.firePostRemoveEvent(o);
                    if (callback != null) {
                        callback.onOperationSucceeded(AsyncOperationType.REMOVE, null, System.currentTimeMillis() - start, null, o);
                    }
                } catch (Exception e) {
                    if (callback == null) {
                        throw new RuntimeException(e);
                    }
                    callback.onOperationError(AsyncOperationType.REMOVE, null, System.currentTimeMillis() - start, e.getMessage(), e, o);
                }
            }
        };
        submitAndBlockIfNecessary(callback, r);

    }

    /**
     * Increases a value in an existing mongo collection entry - no reading necessary. Object is altered in place
     * db.collection.update({"_id":toInc.id},{$inc:{field:amount}}
     * <b>attention</b>: this alteres the given object toSet in a similar way
     *
     * @param toInc:  object to set the value in (or better - the corresponding entry in mongo)
     * @param field:  the field to change
     * @param amount: the value to set
     */
    @Override
    public <T> void inc(final T toInc, final String collection, final String field, final Number amount, AsyncOperationCallback<T> callback) {
        WriterTask r = new WriterTask() {
            private AsyncOperationCallback<T> callback;

            @Override
            public void setCallback(AsyncOperationCallback cb) {
                callback = cb;
            }

            @Override
            public void run() {
                Class cls = toInc.getClass();
                morphium.firePreUpdateEvent(morphium.getARHelper().getRealClass(cls), MorphiumStorageListener.UpdateTypes.INC);
                String coll = collection;
                if (coll == null) {
                    coll = morphium.getMapper().getCollectionName(cls);
                }
                Map<String, Object> query = new HashMap<>();
                query.put("_id", morphium.getId(toInc));
                Field f = morphium.getARHelper().getField(cls, field);
                if (f == null) {
                    throw new RuntimeException("Unknown field: " + field);
                }
                String fieldName = morphium.getARHelper().getFieldName(cls, field);

                Map<String, Object> update = Utils.getMap("$inc", Utils.getMap(fieldName, amount));
                WriteConcern wc = morphium.getWriteConcernForClass(toInc.getClass());

                long start = System.currentTimeMillis();
                try {
                    if (morphium.getConfig().isAutoIndexAndCappedCreationOnWrite() && !morphium.getDriver().exists(getDbName(), coll)) {
                        createCappedCollationColl(cls, coll);
                        morphium.ensureIndicesFor(cls, coll, callback);
                    }
                    Entity en = morphium.getARHelper().getAnnotationFromHierarchy(cls, Entity.class);
                    Long currentVersion = morphium.getARHelper().getLongValue(toInc, MorphiumDriver.VERSION_NAME);
                    if (en != null && en.autoVersioning()) {
                        query.put(MorphiumDriver.VERSION_NAME, currentVersion);
                        ((Map) update.get("$inc")).put(MorphiumDriver.VERSION_NAME, currentVersion + 1L);
                    }
                    Map<String, Object> res = morphium.getDriver().update(getDbName(), coll, query, update, false, false, null, wc);
                    if (en != null && en.autoVersioning() && res.get("modified").equals(0L)) {
                        throw new ConcurrentModificationException("Versioning error? Could not update");
                    }
                    morphium.getCache().clearCacheIfNecessary(cls);
                    if (en != null && en.autoVersioning()) {
                        morphium.getARHelper().setValue(toInc, currentVersion + 1L, morphium.getARHelper().getFields(cls, Version.class).get(0));
                    }
                    if (f.getType().equals(Integer.class) || f.getType().equals(int.class)) {
                        try {
                            f.set(toInc, ((Integer) f.get(toInc)) + amount.intValue());
                        } catch (IllegalAccessException e) {
                            throw new RuntimeException(e);
                        }
                    } else if (f.getType().equals(Double.class) || f.getType().equals(double.class)) {
                        try {
                            f.set(toInc, ((Double) f.get(toInc)) + amount.doubleValue());
                        } catch (IllegalAccessException e) {
                            throw new RuntimeException(e);
                        }
                    } else if (f.getType().equals(Float.class) || f.getType().equals(float.class)) {
                        try {
                            f.set(toInc, ((Float) f.get(toInc)) + amount.floatValue());
                        } catch (IllegalAccessException e) {
                            throw new RuntimeException(e);
                        }
                    } else if (f.getType().equals(Long.class) || f.getType().equals(long.class)) {
                        try {
                            f.set(toInc, ((Long) f.get(toInc)) + amount.longValue());
                        } catch (IllegalAccessException e) {
                            throw new RuntimeException(e);
                        }
                    } else {
                        logger.error("Could not set increased value - unsupported type " + cls.getName());
                    }
                    morphium.firePostUpdateEvent(morphium.getARHelper().getRealClass(cls), MorphiumStorageListener.UpdateTypes.INC);
                    morphium.fireProfilingWriteEvent(toInc.getClass(), toInc, System.currentTimeMillis() - start, false, WriteAccessType.SINGLE_UPDATE);
                    if (callback != null) {
                        callback.onOperationSucceeded(AsyncOperationType.INC, null, System.currentTimeMillis() - start, null, toInc, field, amount);
                    }
                } catch (Exception e) {
                    if (callback == null) {
                        throw new RuntimeException(e);
                    }
                    callback.onOperationError(AsyncOperationType.INC, null, System.currentTimeMillis() - start, e.getMessage(), e, toInc, field, amount);
                }
            }
        };
        submitAndBlockIfNecessary(callback, r);
    }

    @Override
    public <T> void inc(final Query<T> query, final Map<String, Number> fieldsToInc, final boolean upsert, final boolean multiple, AsyncOperationCallback<T> callback) {
        WriterTask r = new WriterTask() {
            private AsyncOperationCallback<T> callback;

            @Override
            public void setCallback(AsyncOperationCallback cb) {
                callback = cb;
            }

            @Override
            public void run() {
                Class<? extends T> cls = query.getType();
                morphium.firePreUpdateEvent(morphium.getARHelper().getRealClass(cls), MorphiumStorageListener.UpdateTypes.INC);
                String coll = query.getCollectionName();

                Map<String, Object> update = new HashMap<>();
                update.put("$inc", new HashMap<String, Object>(fieldsToInc));
                Map<String, Object> qobj = query.toQueryObject();
                if (upsert) {
                    qobj = morphium.simplifyQueryObject(qobj);
                }

                long start = System.currentTimeMillis();
                try {
                    if (upsert && morphium.getConfig().isAutoIndexAndCappedCreationOnWrite() && !morphium.getDriver().exists(getDbName(), coll)) {
                        createCappedCollationColl(cls, coll);
                        morphium.ensureIndicesFor((Class<T>) cls, coll, callback);
                    }
                    WriteConcern wc = morphium.getWriteConcernForClass(cls);
                    morphium.getDriver().update(getDbName(), coll, qobj, update, multiple, upsert, query.getCollation(), wc);
                    long dur = System.currentTimeMillis() - start;
                    morphium.fireProfilingWriteEvent(cls, update, dur, upsert, multiple ? WriteAccessType.BULK_UPDATE : WriteAccessType.SINGLE_UPDATE);
                    morphium.getCache().clearCacheIfNecessary(cls);
                    morphium.firePostUpdateEvent(morphium.getARHelper().getRealClass(cls), MorphiumStorageListener.UpdateTypes.INC);
                    if (callback != null) {
                        callback.onOperationSucceeded(AsyncOperationType.INC, query, System.currentTimeMillis() - start, null, null, fieldsToInc);
                    }
                } catch (Exception e) {
                    if (callback == null) {
                        throw new RuntimeException(e);
                    }
                    callback.onOperationError(AsyncOperationType.INC, query, System.currentTimeMillis() - start, e.getMessage(), e, null, fieldsToInc);

                }

            }
        };
        submitAndBlockIfNecessary(callback, r);
    }

    @Override
    public <T> void inc(final Query<T> query, final String field, final Number amount, final boolean upsert, final boolean multiple, AsyncOperationCallback<T> callback) {
        WriterTask r = new WriterTask() {
            private AsyncOperationCallback<T> callback;

            @Override
            public void setCallback(AsyncOperationCallback cb) {
                callback = cb;
            }

            @Override
            public void run() {
                Class cls = query.getType();
                morphium.firePreUpdateEvent(morphium.getARHelper().getRealClass(cls), MorphiumStorageListener.UpdateTypes.INC);
                String coll = query.getCollectionName();
                String fieldName = morphium.getARHelper().getFieldName(cls, field);
                Map<String, Object> update = Utils.getMap("$inc", Utils.getMap(fieldName, amount));
                Map<String, Object> qobj = query.toQueryObject();
                if (upsert) {
                    qobj = morphium.simplifyQueryObject(qobj);
                }

                long start = System.currentTimeMillis();
                try {
                    if (upsert && morphium.getConfig().isAutoIndexAndCappedCreationOnWrite() && !morphium.getDriver().exists(getDbName(), coll)) {
                        createCappedCollationColl(cls, coll);
                        morphium.ensureIndicesFor(cls, coll, callback);

                    }
                    WriteConcern wc = morphium.getWriteConcernForClass(cls);
                    morphium.getDriver().update(getDbName(), coll, qobj, update, multiple, upsert, query.getCollation(), wc);
                    long dur = System.currentTimeMillis() - start;
                    morphium.fireProfilingWriteEvent(cls, update, dur, upsert, multiple ? WriteAccessType.BULK_UPDATE : WriteAccessType.SINGLE_UPDATE);
                    morphium.getCache().clearCacheIfNecessary(cls);
                    morphium.firePostUpdateEvent(morphium.getARHelper().getRealClass(cls), MorphiumStorageListener.UpdateTypes.INC);
                    if (callback != null) {
                        callback.onOperationSucceeded(AsyncOperationType.INC, query, System.currentTimeMillis() - start, null, null, field, amount);
                    }
                } catch (Exception e) {
                    if (callback == null) {
                        throw new RuntimeException(e);
                    }
                    callback.onOperationError(AsyncOperationType.INC, query, System.currentTimeMillis() - start, e.getMessage(), e, null, field, amount);

                }
            }
        };
        submitAndBlockIfNecessary(callback, r);
    }

    /**
     * will change an entry in mongodb-collection corresponding to given class object
     * if query is too complex, upsert might not work!
     * Upsert should consist of single and-queries, which will be used to generate the object to create, unless
     * it already exists. look at Mongodb-query documentation as well
     *
     * @param query    - query to specify which objects should be set
     * @param values   - map fieldName->Value, which values are to be set!
     * @param upsert   - insert, if it does not exist (query needs to be simple!)
     * @param multiple - update several documents, if false, only first hit will be updated
     */
    @Override
    public <T> void set(final Query<T> query, final Map<String, Object> values, final boolean upsert, final boolean multiple, AsyncOperationCallback<T> callback) {
        submitAndBlockIfNecessary(callback, new WriterTask() {
            private AsyncOperationCallback<T> callback;

            @Override
            public void setCallback(AsyncOperationCallback cb) {
                callback = cb;
            }

            @Override
            public void run() {
                Class<?> cls = query.getType();
                String coll = query.getCollectionName();
                morphium.firePreUpdateEvent(morphium.getARHelper().getRealClass(cls), MorphiumStorageListener.UpdateTypes.SET);
                Map<String, Object> toSet = new HashMap<>();
                for (Map.Entry<String, Object> ef : values.entrySet()) {
                    String fieldName = morphium.getARHelper().getFieldName(cls, ef.getKey());
                    toSet.put(fieldName, marshallIfNecessary(ef.getValue()));
                }
                Map<String, Object> update = Utils.getMap("$set", toSet);
                Map<String, Object> qobj = query.toQueryObject();
                if (upsert) {
                    qobj = morphium.simplifyQueryObject(qobj);
                    List<String> creationTimeFlds = morphium.getARHelper().getFields(cls, CreationTime.class);
                    try {
                        if (creationTimeFlds != null && !creationTimeFlds.isEmpty() && morphium.getDriver().count(getDbName(), coll, qobj, query.getCollation(), null) == 0) {
                            if (creationTimeFlds != null && !creationTimeFlds.isEmpty() && (morphium.isAutoValuesEnabledForThread())) {
                                updateField(cls, update, creationTimeFlds);
                            }
                        }
                    } catch (MorphiumDriverException e) {
                        //TODO: Implement Handling
                        throw new RuntimeException(e);
                    }
                }
                Entity en = morphium.getARHelper().getAnnotationFromHierarchy(cls, Entity.class);
                if (en.autoVersioning()) {
                    update.put("$inc", Utils.getMap(MorphiumDriver.VERSION_NAME, 1));
                }

                List<String> latChangeFlds = morphium.getARHelper().getFields(cls, LastChange.class);
                if (latChangeFlds != null && !latChangeFlds.isEmpty() && (morphium.isAutoValuesEnabledForThread())) {
                    updateField(cls, update, latChangeFlds);
                }


                WriteConcern wc = morphium.getWriteConcernForClass(cls);
                long start = System.currentTimeMillis();
                try {
                    if (upsert && morphium.getConfig().isAutoIndexAndCappedCreationOnWrite() && !morphium.getDriver().exists(getDbName(), coll)) {
                        createCappedCollationColl(cls, coll);
                        morphium.ensureIndicesFor((Class<T>) cls, coll, callback);
                    }
                    Map<String, Object> daa = morphium.getDriver().update(getDbName(), coll, qobj, update, multiple, upsert, query.getCollation(), wc);
                    long dur = System.currentTimeMillis() - start;
                    morphium.fireProfilingWriteEvent(cls, update, dur, upsert, multiple ? WriteAccessType.BULK_UPDATE : WriteAccessType.SINGLE_UPDATE);
                    morphium.inc(StatisticKeys.WRITES);
                    morphium.getCache().clearCacheIfNecessary(cls);
                    morphium.firePostUpdateEvent(morphium.getARHelper().getRealClass(cls), MorphiumStorageListener.UpdateTypes.SET);
                    if (callback != null) {
                        callback.onOperationSucceeded(AsyncOperationType.SET, query, System.currentTimeMillis() - start, null, null, values, upsert, multiple);
                    }
                } catch (Exception e) {
                    if (callback == null) {
                        throw new RuntimeException(e);
                    }
                    callback.onOperationError(AsyncOperationType.SET, query, System.currentTimeMillis() - start, e.getMessage(), e, null, values, upsert, multiple);
                }
            }
        });
    }

    @Override
    public <T> void unset(final Query<T> query, AsyncOperationCallback<T> callback, final boolean multiple, final Enum... fields) {
        ArrayList<String> flds = new ArrayList<>();
        for (Enum e : fields) {
            flds.add(e.name());
        }
        unset(query, callback, multiple, flds.toArray(new String[fields.length]));
    }

    @Override
    public <T> void unset(final Query<T> query, AsyncOperationCallback<T> callback, final boolean multiple, final String... fields) {
        WriterTask r = new WriterTask() {
            private AsyncOperationCallback<T> callback;

            @Override
            public void setCallback(AsyncOperationCallback cb) {
                callback = cb;
            }

            @Override
            public void run() {
                Class<?> cls = query.getType();
                String coll = query.getCollectionName();
                morphium.firePreUpdateEvent(morphium.getARHelper().getRealClass(cls), MorphiumStorageListener.UpdateTypes.SET);
                Map<String, Object> qobj = query.toQueryObject();

                Map<String, String> toSet = new HashMap<>();
                for (String f : fields) {
                    toSet.put(f, ""); //value is ignored
                }
                Map<String, Object> update = Utils.getMap("$unset", toSet);
                WriteConcern wc = morphium.getWriteConcernForClass(cls);
                long start = System.currentTimeMillis();
                try {
                    morphium.getDriver().update(getDbName(), coll, qobj, update, multiple, false, query.getCollation(), wc);
                    long dur = System.currentTimeMillis() - start;
                    morphium.fireProfilingWriteEvent(cls, update, dur, false, multiple ? WriteAccessType.BULK_UPDATE : WriteAccessType.SINGLE_UPDATE);
                    morphium.getCache().clearCacheIfNecessary(cls);
                    morphium.firePostUpdateEvent(morphium.getARHelper().getRealClass(cls), MorphiumStorageListener.UpdateTypes.SET);
                    if (callback != null) {
                        callback.onOperationSucceeded(AsyncOperationType.SET, query, System.currentTimeMillis() - start, null, null, fields, false, multiple);
                    }
                } catch (Exception e) {
                    if (callback == null) {
                        throw new RuntimeException(e);
                    }
                    callback.onOperationError(AsyncOperationType.SET, query, System.currentTimeMillis() - start, e.getMessage(), e, null, fields, false, multiple);
                }
            }
        };
        submitAndBlockIfNecessary(callback, r);
    }

    /**
     * Un-setting a value in an existing mongo collection entry - no reading necessary. Object is altered in place
     * db.collection.update({"_id":toSet.id},{$unset:{field:1}}
     * <b>attention</b>: this alteres the given object toSet in a similar way
     *
     * @param toSet: object to set the value in (or better - the corresponding entry in mongo)
     * @param field: field to remove from document
     */
    @Override
    public <T> void unset(final T toSet, final String collection, final String field, AsyncOperationCallback<T> callback) {
        if (toSet == null) {
            throw new RuntimeException("Cannot update null!");
        }
        if (morphium.getARHelper().getId(toSet) == null) {
            logger.info("just storing object as it is new...");
            store(toSet, collection, callback);
        }

        WriterTask r = new WT() {

            @Override
            public void run() {
                Class cls = toSet.getClass();
                morphium.firePreUpdateEvent(morphium.getARHelper().getRealClass(cls), MorphiumStorageListener.UpdateTypes.UNSET);

                String coll = collection;
                if (coll == null) {
                    coll = morphium.getMapper().getCollectionName(cls);
                }
                Map<String, Object> query = new HashMap<>();
                query.put("_id", morphium.getId(toSet));
                Field f = morphium.getARHelper().getField(cls, field);
                if (f == null) {
                    throw new RuntimeException("Unknown field: " + field);
                }
                String fieldName = morphium.getARHelper().getFieldName(cls, field);

                Map<String, Object> update = Utils.getMap("$unset", Utils.getMap(fieldName, 1));
                WriteConcern wc = morphium.getWriteConcernForClass(toSet.getClass());

                doUpdate(cls, toSet, coll, field, query, f, update, wc);

            }


        };
        submitAndBlockIfNecessary(callback, r);
    }

    @Override
    public <T> void pop(final T obj, final String collection, final String field, final boolean first, AsyncOperationCallback<T> callback) {
        if (obj == null) {
            throw new RuntimeException("Cannot update null!");
        }
        if (morphium.getARHelper().getId(obj) == null) {
            logger.info("just storing object as it is new...");
            store(obj, collection, callback);
        }

        WriterTask r = new WT() {

            @Override
            public void run() {
                Class cls = obj.getClass();
                morphium.firePreUpdateEvent(morphium.getARHelper().getRealClass(cls), MorphiumStorageListener.UpdateTypes.UNSET);

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
                String fieldName = morphium.getARHelper().getFieldName(cls, field);

                Map<String, Object> update = Utils.getMap("$pop", Utils.getMap(fieldName, first ? -1 : 1));
                WriteConcern wc = morphium.getWriteConcernForClass(obj.getClass());


                doUpdate(cls, obj, coll, field, query, f, update, wc);


            }
        };
        submitAndBlockIfNecessary(callback, r);
    }

    @Override
    public <T> void unset(Query<T> query, String field, boolean multiple, AsyncOperationCallback<T> callback) {
        unset(query, callback, multiple, field);
    }

    @Override
    public <T> void pushPull(final boolean push, final Query<T> query, final String field, final Object value, final boolean upsert, final boolean multiple, AsyncOperationCallback<T> callback) {
        WriterTask r = new WriterTask() {
            private AsyncOperationCallback<T> callback;

            @Override
            public void setCallback(AsyncOperationCallback cb) {
                callback = cb;
            }

            @Override
            public void run() {
                Class<?> cls = query.getType();
                morphium.firePreUpdateEvent(morphium.getARHelper().getRealClass(cls), push ? MorphiumStorageListener.UpdateTypes.PUSH : MorphiumStorageListener.UpdateTypes.PULL);

                String coll = query.getCollectionName();

                Map<String, Object> qobj = query.toQueryObject();
                if (upsert) {
                    qobj = morphium.simplifyQueryObject(qobj);
                }
                Object v = marshallIfNecessary(value);

                String fieldName = morphium.getARHelper().getFieldName(cls, field);
                Map<String, Object> set = Utils.getMap(fieldName, v.getClass().isEnum() ? v.toString() : v);
                Map<String, Object> update = Utils.getMap(push ? "$push" : "$pull", set);

                long start = System.currentTimeMillis();

                try {
                    pushIt(push, upsert, multiple, cls, coll, qobj, update, query.getCollation());
                    morphium.firePostUpdateEvent(query.getType(), MorphiumStorageListener.UpdateTypes.PUSH);
                    if (callback != null) {
                        callback.onOperationSucceeded(AsyncOperationType.PUSH, query, System.currentTimeMillis() - start, null, null, field, value, upsert, multiple);
                    }
                } catch (RuntimeException e) {
                    if (callback == null) {
                        throw new RuntimeException(e);
                    }
                    callback.onOperationError(AsyncOperationType.PUSH, query, System.currentTimeMillis() - start, e.getMessage(), e, null, field, value, upsert, multiple);
                }
            }
        };

        submitAndBlockIfNecessary(callback, r);
    }

    private Object marshallIfNecessary(Object value) {
        if (value != null) {
            if (value.getClass().isEnum()) {
                return ((Enum) value).name();
            }
            if (morphium.getARHelper().isAnnotationPresentInHierarchy(value.getClass(), Entity.class)
                    || morphium.getARHelper().isAnnotationPresentInHierarchy(value.getClass(), Embedded.class)) {
                //need to serialize...
                Map<String, Object> marshall = morphium.getMapper().serialize(value);
                marshall.put("class_name", morphium.getARHelper().getRealClass(value.getClass()).getName());
                value = marshall;
            } else if (List.class.isAssignableFrom(value.getClass())) {
                List lst = new ArrayList();
                for (Object o : (List) value) {
                    if (morphium.getARHelper().isAnnotationPresentInHierarchy(o.getClass(), Embedded.class) ||
                            morphium.getARHelper().isAnnotationPresentInHierarchy(o.getClass(), Entity.class)
                    ) {
                        Map<String, Object> marshall = morphium.getMapper().serialize(o);
                        marshall.put("class_name", morphium.getARHelper().getRealClass(o.getClass()).getName());

                        lst.add(marshall);
                    } else {
                        lst.add(o);
                    }
                }
                value = lst;
            } else if (Map.class.isAssignableFrom(value.getClass())) {
                for (Object e : ((Map) value).entrySet()) {
                    Map.Entry en = (Map.Entry) e;
                    if (!String.class.isAssignableFrom(((Map.Entry) e).getKey().getClass())) {
                        throw new IllegalArgumentException("Can't push maps with Key not of type String!");
                    }
                    if (morphium.getARHelper().isAnnotationPresentInHierarchy(en.getValue().getClass(), Entity.class) ||
                            morphium.getARHelper().isAnnotationPresentInHierarchy(en.getValue().getClass(), Embedded.class)
                    ) {
                        Map<String, Object> marshall = morphium.getMapper().serialize(en.getValue());
                        marshall.put("class_name", morphium.getARHelper().getRealClass(en.getValue().getClass()).getName());
                        ((Map) value).put(en.getKey(), marshall);
                    }
                }

            }
        }
        return value;
    }

    private void pushIt(boolean push, boolean upsert, boolean multiple, Class<?> cls, String coll, Map<String, Object> qobj, Map<String, Object> update, Collation collation) {
        morphium.firePreUpdateEvent(morphium.getARHelper().getRealClass(cls), push ? MorphiumStorageListener.UpdateTypes.PUSH : MorphiumStorageListener.UpdateTypes.PULL);

        List<String> lastChangeFields = morphium.getARHelper().getFields(cls, LastChange.class);
        if (lastChangeFields != null && !lastChangeFields.isEmpty()) {
            update.put("$set", new HashMap<String, Object>());
            updateField(cls, update, lastChangeFields);
        }
        if (upsert) {
            doUpsert(upsert, cls, coll, qobj, update, collation);
        }
        Entity en = morphium.getARHelper().getAnnotationFromHierarchy(cls, Entity.class);
        if (en.autoVersioning()) {
            update.put("$inc", Utils.getMap(MorphiumDriver.VERSION_NAME, 1));
        }

        WriteConcern wc = morphium.getWriteConcernForClass(cls);
        long start = System.currentTimeMillis();
        try {
            if (morphium.getConfig().isAutoIndexAndCappedCreationOnWrite() && !morphium.getDriver().exists(getDbName(), coll) && upsert) {
                createCappedCollationColl(cls, coll);
                morphium.ensureIndicesFor(cls, coll);
            }
            morphium.getDriver().update(getDbName(), coll, qobj, update, multiple, upsert, collation, wc);
            morphium.inc(StatisticKeys.WRITES);

        } catch (MorphiumDriverException e) {
            //TODO: Implement Handling
            throw new RuntimeException(e);
        }

        long dur = System.currentTimeMillis() - start;
        morphium.fireProfilingWriteEvent(cls, update, dur, upsert, multiple ? WriteAccessType.BULK_UPDATE : WriteAccessType.SINGLE_UPDATE);
        morphium.getCache().clearCacheIfNecessary(cls);
        morphium.firePostUpdateEvent(morphium.getARHelper().getRealClass(cls), push ? MorphiumStorageListener.UpdateTypes.PUSH : MorphiumStorageListener.UpdateTypes.PULL);
    }

    private void doUpsert(boolean upsert, Class<?> cls, String coll, Map<String, Object> qobj, Map<String, Object> update, Collation collation) {
        List<String> creationTimeFields = morphium.getARHelper().getFields(cls, CreationTime.class);
        if (upsert && creationTimeFields != null && !creationTimeFields.isEmpty()) {
            long cnt;
            try {
                cnt = morphium.getDriver().count(getDbName(), coll, qobj, collation, null);
                morphium.inc(StatisticKeys.WRITES);
            } catch (MorphiumDriverException e) {
                //TODO: Implement Handling
                throw new RuntimeException(e);
            }
            if (cnt == 0) {
                //not found, would insert
                update.putIfAbsent("$set", new HashMap<String, Object>());
                updateField(cls, update, creationTimeFields);
            }
        }
    }

    @Override
    public <T> void pushPullAll(final boolean push, final Query<T> query, final String f, final List<?> v, final boolean upsert, final boolean multiple, AsyncOperationCallback<T> callback) {
        WriterTask r = new WriterTask() {
            private AsyncOperationCallback<T> callback;

            @Override
            public void setCallback(AsyncOperationCallback cb) {
                callback = cb;
            }

            @Override
            public void run() {
                List<?> value = v;
                String field = f;
                Class<?> cls = query.getType();
                String coll = query.getCollectionName();
                morphium.firePreUpdateEvent(morphium.getARHelper().getRealClass(cls), push ? MorphiumStorageListener.UpdateTypes.PUSH : MorphiumStorageListener.UpdateTypes.PULL);
                long start = System.currentTimeMillis();
                value = value.stream().map(o -> marshallIfNecessary(o)).collect(Collectors.toList());
                try {
                    Map<String, Object> qobj = query.toQueryObject();
                    if (upsert) {
                        qobj = morphium.simplifyQueryObject(qobj);
                    }

                    field = morphium.getARHelper().getFieldName(cls, field);
                    Map<String, Object> update;
                    if (push) {
                        Map<String, Object> set = Utils.getMap(field, Utils.getMap("$each", value));
                        update = Utils.getMap("$push", set);

                    } else {
                        update = Utils.getMap("$pullAll", Utils.getMap(field, value));
                    }

                    List<String> lastChangeFields = morphium.getARHelper().getFields(cls, LastChange.class);
                    if (lastChangeFields != null && !lastChangeFields.isEmpty()) {
                        update.put("$set", new HashMap<String, Object>());
                        updateField(cls, update, lastChangeFields);
                    }
                    if (upsert) {
                        doUpsert(upsert, cls, coll, qobj, update, query.getCollation());
                    }

                    WriteConcern wc = morphium.getWriteConcernForClass(cls);
                    try {
                        if (upsert && morphium.getConfig().isAutoIndexAndCappedCreationOnWrite() && !morphium.getDriver().exists(getDbName(), coll)) {
                            createCappedCollationColl(cls, coll);
                            morphium.ensureIndicesFor((Class<T>) cls, coll, callback);
                        }
                        morphium.getDriver().update(getDbName(), coll, qobj, update, multiple, upsert, query.getCollation(), wc);
                        morphium.inc(StatisticKeys.WRITES);
                    } catch (MorphiumDriverException e) {
                        //TODO: Implement Handling
                        throw new RuntimeException(e);
                    }
                    long dur = System.currentTimeMillis() - start;
                    morphium.fireProfilingWriteEvent(cls, update, dur, upsert, multiple ? WriteAccessType.BULK_UPDATE : WriteAccessType.SINGLE_UPDATE);
                    morphium.getCache().clearCacheIfNecessary(cls);
                    morphium.firePostUpdateEvent(query.getType(), MorphiumStorageListener.UpdateTypes.PUSH);
                    if (callback != null) {
                        callback.onOperationSucceeded(push ? AsyncOperationType.PUSH : AsyncOperationType.PULL, query, System.currentTimeMillis() - start, null, null, field, value, upsert, multiple);
                    }
                } catch (RuntimeException e) {
                    if (callback == null) {
                        throw new RuntimeException(e);
                    }
                    callback.onOperationError(push ? AsyncOperationType.PUSH : AsyncOperationType.PULL, query, System.currentTimeMillis() - start, e.getMessage(), e, null, field, value, upsert, multiple);
                }
            }

        };
        submitAndBlockIfNecessary(callback, r);
    }

    @Override
    public <T> void dropCollection(final Class<T> cls, final String collection, AsyncOperationCallback<T> callback) {
        if (morphium.getARHelper().isAnnotationPresentInHierarchy(cls, Entity.class)) {
            WriterTask r = new WriterTask() {
                @Override
                public void setCallback(AsyncOperationCallback cb) {
                }

                public void run() {
                    morphium.firePreDrop(cls);
                    long start = System.currentTimeMillis();
                    String co = collection;
                    if (co == null) {
                        co = morphium.getMapper().getCollectionName(cls);
                    }
                    try {
                        morphium.getDriver().drop(getDbName(), co, null);
                    } catch (MorphiumDriverException e) {
                        //TODO: Implement Handling
                        throw new RuntimeException(e);
                    }
                    long dur = System.currentTimeMillis() - start;
                    morphium.fireProfilingWriteEvent(cls, null, dur, false, WriteAccessType.DROP);
                    morphium.firePostDropEvent(cls);
                }
            };
            submitAndBlockIfNecessary(callback, r);
        } else {
            throw new RuntimeException("No entity class: " + cls.getName());
        }
    }

    @Override
    public <T> void ensureIndex(final Class<T> cls, final String collection, final Map<String, Object> index, final Map<String, Object> options, AsyncOperationCallback<T> callback) {
        WriterTask r = new WriterTask() {

            @Override
            public void setCallback(AsyncOperationCallback cb) {
            }

            @Override
            public void run() {
                List<String> fields = morphium.getARHelper().getFields(cls);

                Map<String, Object> idx = new LinkedHashMap<>();
                for (Map.Entry<String, Object> es : index.entrySet()) {
                    String k = es.getKey();
                    if (!k.contains(".") && !fields.contains(k) && !fields.contains(morphium.getARHelper().convertCamelCase(k))) {
                        throw new IllegalArgumentException("Field unknown for type " + cls.getSimpleName() + ": " + k);
                    }
                    String fn = morphium.getARHelper().getFieldName(cls, k);
                    idx.put(fn, es.getValue());
                }
                long start = System.currentTimeMillis();
                Map<String, Object> keys = new LinkedHashMap<>(idx);
                String coll = collection;
                if (coll == null) {
                    coll = morphium.getMapper().getCollectionName(cls);
                }
                try {
                    morphium.getDriver().createIndex(getDbName(), coll, keys, options);
                } catch (MorphiumDriverException e) {
                    //TODO: Implement Handling
                    throw new RuntimeException(e);
                }
                long dur = System.currentTimeMillis() - start;
                morphium.fireProfilingWriteEvent(cls, keys, dur, false, WriteAccessType.ENSURE_INDEX);
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
                //swallow
            }
        }
    }

    public abstract class WT<T> implements WriterTask<T> {
        private AsyncOperationCallback<T> callback;

        @Override
        public void setCallback(AsyncOperationCallback cb) {
            callback = cb;
        }

        public void doUpdate(Class cls, T toSet, String coll, String field, Map<String, Object> query, Field f, Map<String, Object> update, WriteConcern wc) {
            long start = System.currentTimeMillis();

            try {
                if (morphium.getConfig().isAutoIndexAndCappedCreationOnWrite() && !morphium.getDriver().exists(getDbName(), coll)) {
                    createCappedCollationColl(cls, coll);
                    morphium.ensureIndicesFor(cls, coll, callback);
                }
                morphium.getDriver().update(getDbName(), coll, query, update, false, false, null, wc);
                morphium.inc(StatisticKeys.WRITES);
                List<String> lastChangeFields = morphium.getARHelper().getFields(cls, LastChange.class);
                if (lastChangeFields != null && !lastChangeFields.isEmpty()) {
                    update = Utils.getMap("$set", new HashMap<String, Object>());
                    updateField(cls, update, lastChangeFields);

                    morphium.getDriver().update(getDbName(), coll, query, update, false, false, null, wc);
                }

                long dur = System.currentTimeMillis() - start;
                morphium.fireProfilingWriteEvent(toSet.getClass(), update, dur, false, WriteAccessType.SINGLE_UPDATE);
                morphium.getCache().clearCacheIfNecessary(cls);

                try {
                    f.set(toSet, null);
                } catch (IllegalAccessException e) {
                    //May happen, if null is not allowed for example
                }
                morphium.firePostUpdateEvent(morphium.getARHelper().getRealClass(cls), MorphiumStorageListener.UpdateTypes.UNSET);
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
