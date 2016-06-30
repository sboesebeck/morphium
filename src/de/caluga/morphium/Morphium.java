/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.caluga.morphium;

import de.caluga.morphium.aggregation.Aggregator;
import de.caluga.morphium.annotations.*;
import de.caluga.morphium.annotations.lifecycle.*;
import de.caluga.morphium.async.AsyncOperationCallback;
import de.caluga.morphium.bulk.MorphiumBulkContext;
import de.caluga.morphium.cache.MorphiumCache;
import de.caluga.morphium.cache.MorphiumCacheImpl;
import de.caluga.morphium.driver.MorphiumDriver;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.ReadPreference;
import de.caluga.morphium.driver.WriteConcern;
import de.caluga.morphium.driver.bson.MorphiumId;
import de.caluga.morphium.query.MongoField;
import de.caluga.morphium.query.MongoFieldImpl;
import de.caluga.morphium.query.Query;
import de.caluga.morphium.replicaset.RSMonitor;
import de.caluga.morphium.replicaset.ReplicaSetNode;
import de.caluga.morphium.replicaset.ReplicaSetStatus;
import de.caluga.morphium.validation.JavaxValidationStorageListener;
import de.caluga.morphium.writer.BufferedMorphiumWriterImpl;
import de.caluga.morphium.writer.MorphiumWriter;
import de.caluga.morphium.writer.MorphiumWriterImpl;
import net.sf.cglib.proxy.Enhancer;

import java.io.Serializable;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This is the single access point for accessing MongoDB. This should
 *
 * @author stephan
 */

@SuppressWarnings("WeakerAccess")
public class Morphium {

    /**
     * singleton is usually not a good idea in j2ee-Context, but as we did it on
     * several places in the Application it's the easiest way Usage:
     * <code>
     * MorphiumConfig cfg=new MorphiumConfig("testdb",false,false,10,5000,2500);
     * cfg.addAddress("localhost",27017);
     * Morphium.config=cfg;
     * Morphium l=Morphium.get();
     * if (l==null) {
     * System.out.println("Error establishing connection!");
     * System.exit(1);
     * }
     * </code>
     *
     * @see MorphiumConfig
     */
    private static final Logger logger = new Logger(Morphium.class);
    private final ThreadLocal<Boolean> enableAutoValues = new ThreadLocal<>();
    private final ThreadLocal<Boolean> enableReadCache = new ThreadLocal<>();
    private final ThreadLocal<Boolean> disableWriteBuffer = new ThreadLocal<>();
    private final ThreadLocal<Boolean> disableAsyncWrites = new ThreadLocal<>();
    private final List<ProfilingListener> profilingListeners;
    private final List<ShutdownListener> shutDownListeners = new CopyOnWriteArrayList<>();
    private final List<DereferencingListener> lazyDereferencingListeners = new CopyOnWriteArrayList<>();
    private MorphiumConfig config;
    private Map<StatisticKeys, StatisticValue> stats = new ConcurrentHashMap<>();
    private List<MorphiumStorageListener> listeners = new CopyOnWriteArrayList<>();
    private AnnotationAndReflectionHelper annotationHelper;
    private ObjectMapper objectMapper;
    private RSMonitor rsMonitor;
    private ThreadPoolExecutor asyncOperationsThreadPool;
    private MorphiumDriver morphiumDriver;

    public Morphium() {
        profilingListeners = new CopyOnWriteArrayList<>();

    }

    public Morphium(String host, String db) {
        this();
        MorphiumConfig cfg = new MorphiumConfig(db, 100, 5000, 5000);
        try {
            cfg.addHostToSeed(host);
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
        cfg.setReplicasetMonitoring(false);
        setConfig(cfg);

    }

    public Morphium(String host, int port, String db) {
        this();
        MorphiumConfig cfg = new MorphiumConfig(db, 100, 5000, 5000);
        try {
            cfg.addHostToSeed(host, port);
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
        setConfig(cfg);
    }


    /**
     * init the MongoDbLayer. Uses Morphium-Configuration Object for Configuration.
     * Needs to be set before use or RuntimeException is thrown!
     * us used for de-referencing and automatical save of referenced entities
     * all logging is done in INFO level
     *
     * @see MorphiumConfig
     */
    public Morphium(MorphiumConfig cfg) {
        this();
        setConfig(cfg);
    }

    public MorphiumConfig getConfig() {
        return config;
    }

    public void setConfig(MorphiumConfig cfg) {
        if (config != null) {
            throw new RuntimeException("Cannot change config!");
        }
        config = cfg;
        annotationHelper = new AnnotationAndReflectionHelper(cfg.isCamelCaseConversionEnabled());
        asyncOperationsThreadPool = new ThreadPoolExecutor(getConfig().getThreadPoolAsyncOpCoreSize(), getConfig().getThreadPoolAsyncOpMaxSize(),
                getConfig().getThreadPoolAsyncOpKeepAliveTime(), TimeUnit.MILLISECONDS,
                new SynchronousQueue<>());
        asyncOperationsThreadPool.setThreadFactory(new ThreadFactory() {
            private final AtomicInteger num = new AtomicInteger(1);

            @Override
            public Thread newThread(Runnable r) {
                Thread ret = new Thread(r, "asyncOp " + num);
                num.set(num.get() + 1);
                ret.setDaemon(true);
                return ret;
            }
        });
        initializeAndConnect();
    }

    public ThreadPoolExecutor getAsyncOperationsThreadPool() {
        return asyncOperationsThreadPool;
    }

    public void registerTypeMapper(Class c, TypeMapper m) {
        getMapper().registerCustomTypeMapper(c, m);
    }

    public void deregisterTypeMapper(Class c) {
        getMapper().deregisterTypeMapper(c);
    }

    private void initializeAndConnect() {
        if (config == null) {
            throw new RuntimeException("Please specify configuration!");
        }
        for (StatisticKeys k : StatisticKeys.values()) {
            stats.put(k, new StatisticValue());
        }

        if (morphiumDriver == null) {
            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    try {
                        close();
                    } catch (Exception e) {
                        //swallow
                    }
                }
            });


            try {
                morphiumDriver = (MorphiumDriver) Class.forName(config.getDriverClass()).newInstance();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            morphiumDriver.setSocketTimeout(config.getSocketTimeout());
            morphiumDriver.setConnectionTimeout(config.getConnectionTimeout());
            morphiumDriver.setMaxConnectionsPerHost(config.getMaxConnections());
            morphiumDriver.setSocketKeepAlive((config.isSocketKeepAlive()));
            morphiumDriver.setMaxBlockingThreadMultiplier(config.getBlockingThreadsMultiplier());
            //            drv.cursorFinalizerEnabled(config.isCursorFinalizerEnabled());
            //            drv.alwaysUseMBeans(config.isAlwaysUseMBeans());
            morphiumDriver.setHeartbeatConnectTimeout(config.getHeartbeatConnectTimeout());
            morphiumDriver.setHeartbeatFrequency(config.getHeartbeatFrequency());
            morphiumDriver.setHeartbeatSocketTimeout(config.getHeartbeatSocketTimeout());
            morphiumDriver.setMinConnectionsPerHost(config.getMinConnectionsPerHost());
            //            drv.setMinminHeartbeatFrequency(config.getMinHearbeatFrequency());
            morphiumDriver.setLocalThreshold(config.getLocalThreashold());
            morphiumDriver.setMaxConnectionIdleTime(config.getMaxConnectionIdleTime());
            morphiumDriver.setMaxConnectionLifetime(config.getMaxConnectionLifeTime());
            morphiumDriver.setMaxWaitTime(config.getMaxWaitTime());

            System.getProperties().put("morphium.log.level", "" + config.getGlobalLogLevel());
            System.getProperties().put("morphium.log.synced", "" + config.isGlobalLogSynced());
            if (config.getGlobalLogFile() != null) {
                System.getProperties().put("morphium.log.file", config.getGlobalLogFile());
            }
            if (config.getHostSeed().isEmpty()) {
                throw new RuntimeException("Error - no server address specified!");
            }

            if (config.getMongoLogin() != null && config.getMongoPassword() != null) {
                morphiumDriver.setCredentials(config.getMongoLogin(), config.getDatabase(), config.getMongoPassword().toCharArray());
            }
            if (config.getMongoAdminUser() != null && config.getMongoAdminPwd() != null) {
                morphiumDriver.setCredentials(config.getMongoAdminUser(), "admin", config.getMongoAdminPwd().toCharArray());
            }
            String[] seed = new String[config.getHostSeed().size()];
            for (int i = 0; i < seed.length; i++) {
                seed[i] = config.getHostSeed().get(i);
            }
            morphiumDriver.setHostSeed(seed);

            morphiumDriver.setDefaultReadPreference(config.getDefaultReadPreference());
            try {
                morphiumDriver.connect(config.getRequiredReplicaSetName());
            } catch (MorphiumDriverException e) {
                throw new RuntimeException(e);
            }
        }

        if (config.getWriter() == null) {
            config.setWriter(new MorphiumWriterImpl());
        }
        if (config.getBufferedWriter() == null) {
            config.setBufferedWriter(new BufferedMorphiumWriterImpl());
        }
        config.getWriter().setMorphium(this);
        config.getWriter().setMaximumQueingTries(config.getMaximumRetriesWriter());
        config.getWriter().setPauseBetweenTries(config.getRetryWaitTimeWriter());
        config.getBufferedWriter().setMorphium(this);
        config.getBufferedWriter().setMaximumQueingTries(config.getMaximumRetriesBufferedWriter());
        config.getBufferedWriter().setPauseBetweenTries(config.getRetryWaitTimeBufferedWriter());
        config.getAsyncWriter().setMorphium(this);
        config.getAsyncWriter().setMaximumQueingTries(config.getMaximumRetriesAsyncWriter());
        config.getAsyncWriter().setPauseBetweenTries(config.getRetryWaitTimeAsyncWriter());

        if (config.getCache() == null) {
            config.setCache(new MorphiumCacheImpl());
        }
        config.getCache().setAnnotationAndReflectionHelper(getARHelper());
        config.getCache().setGlobalCacheTimeout(config.getGlobalCacheValidTime());
        config.getCache().setHouskeepingIntervalPause(config.getHousekeepingTimeout());

        if (hasValidationSupport()) {
            logger.info("Adding javax.validation Support...");
            addListener(new JavaxValidationStorageListener());
        }
        try {
            objectMapper = config.getOmClass().newInstance();
            objectMapper.setMorphium(this);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }


        if (isReplicaSet()) {
            rsMonitor = new RSMonitor(this);
            rsMonitor.start();
            rsMonitor.getReplicaSetStatus(false);
        }
        logger.info("Initialization successful...");
    }

    public MorphiumCache getCache() {
        return config.getCache();
    }

    /**
     * Checks if javax.validation is available and enables validation support.
     *
     * @return true, if validation is supported
     */

    @SuppressWarnings("UnusedDeclaration")
    private boolean hasValidationSupport() {
        try {
            getClass().getClassLoader().loadClass("javax.validation.ValidatorFactory");

        } catch (ClassNotFoundException cnf) {
            return false;
        }
        return true;
    }

    public void addListener(MorphiumStorageListener lst) {
        List<MorphiumStorageListener> newList = new ArrayList<>();
        newList.addAll(listeners);
        newList.add(lst);
        listeners = newList;
    }

    public void removeListener(MorphiumStorageListener lst) {
        List<MorphiumStorageListener> newList = new ArrayList<>();
        newList.addAll(listeners);
        newList.remove(lst);
        listeners = newList;
    }

    public MorphiumDriver getDriver() {
        return morphiumDriver;
    }

    @SuppressWarnings("unused")
    public void setDriver(MorphiumDriver drv) {
        morphiumDriver = drv;
    }


    /**
     * search for objects similar to template concerning all given fields.
     * If no fields are specified, all NON Null-Fields are taken into account
     * if specified, field might also be null
     *
     * @param template - what to search for
     * @param fields   - fields to use for searching
     * @param <T>      - type
     * @return result of search
     */
    @SuppressWarnings({"unchecked", "UnusedDeclaration"})
    public <T> List<T> findByTemplate(T template, String... fields) {
        Class cls = template.getClass();
        List<String> flds = new ArrayList<>();
        if (fields.length > 0) {
            flds.addAll(Arrays.asList(fields));
        } else {
            flds = annotationHelper.getFields(cls);
        }
        Query<T> q = createQueryFor((Class<T>) cls);
        for (String f : flds) {
            try {
                q.f(f).eq(annotationHelper.getValue(template, f));
            } catch (Exception e) {
                logger.error("Could not read field " + f + " of object " + cls.getName());
            }
        }
        return q.asList();
    }

    @SuppressWarnings({"unchecked", "UnusedDeclaration"})
    public <T> void unset(T toSet, Enum field) {
        unset(toSet, field.name(), (AsyncOperationCallback) null);
    }

    public <T> void unset(final T toSet, final String field) {
        //noinspection unchecked
        unset(toSet, field, (AsyncOperationCallback) null);
    }

    public <T> void unset(final T toSet, final Enum field, final AsyncOperationCallback<T> callback) {
        unset(toSet, field.name(), callback);
    }

    public <T> void unset(final T toSet, String collection, final Enum field) {
        unset(toSet, collection, field.name(), null);
    }

    @SuppressWarnings("unused")
    public <T> void unset(final T toSet, String collection, final Enum field, final AsyncOperationCallback<T> callback) {
        unset(toSet, collection, field.name(), callback);
    }

    public <T> void unset(final T toSet, final String field, final AsyncOperationCallback<T> callback) {
        unset(toSet, getMapper().getCollectionName(toSet.getClass()), field, callback);
    }


    public <T> void unset(final T toSet, String collection, final String field, final AsyncOperationCallback<T> callback) {
        if (toSet == null) {
            throw new RuntimeException("Cannot update null!");
        }
        MorphiumWriter wr = getWriterForClass(toSet.getClass());
        wr.unset(toSet, collection, field, callback);
    }

    public <T> void unsetQ(Query<T> q, String... field) {
        getWriterForClass(q.getType()).unset(q, null, false, field);
    }

    public <T> void unsetQ(Query<T> q, boolean multiple, String... field) {
        getWriterForClass(q.getType()).unset(q, null, multiple, field);
    }

    public <T> void unsetQ(Query<T> q, Enum... field) {
        getWriterForClass(q.getType()).unset(q, null, false, field);
    }

    public <T> void unsetQ(Query<T> q, boolean multiple, Enum... field) {
        getWriterForClass(q.getType()).unset(q, null, multiple, field);
    }

    public <T> void unsetQ(Query<T> q, AsyncOperationCallback<T> cb, String... field) {
        getWriterForClass(q.getType()).unset(q, cb, false, field);
    }

    @SuppressWarnings({"unused", "UnusedParameters"})
    public <T> void unsetQ(Query<T> q, AsyncOperationCallback<T> cb, boolean multiple, String... field) {
        getWriterForClass(q.getType()).unset(q, cb, false, field);
    }

    public <T> void unsetQ(Query<T> q, AsyncOperationCallback<T> cb, Enum... field) {
        getWriterForClass(q.getType()).unset(q, cb, false, field);
    }

    public <T> void unsetQ(Query<T> q, boolean multiple, AsyncOperationCallback<T> cb, Enum... field) {
        getWriterForClass(q.getType()).unset(q, cb, multiple, field);
    }


    /**
     * can be called for autmatic index ensurance. Attention: might cause heavy load on mongo
     * will be called automatically if a new collection is created
     *
     * @param type type to ensure indices for
     */
    @SuppressWarnings("unchecked")
    public <T> void ensureIndicesFor(Class<T> type) {
        ensureIndicesFor(type, getMapper().getCollectionName(type), null);
    }

    public <T> void ensureIndicesFor(Class<T> type, String onCollection) {
        ensureIndicesFor(type, onCollection, null);
    }


    public <T> void ensureIndicesFor(Class<T> type, AsyncOperationCallback<T> callback) {
        ensureIndicesFor(type, getMapper().getCollectionName(type), callback);
    }

    public <T> void ensureIndicesFor(Class<T> type, String onCollection, AsyncOperationCallback<T> callback) {
        if (annotationHelper.isAnnotationPresentInHierarchy(type, Index.class)) {
            @SuppressWarnings("unchecked") List<Annotation> lst = annotationHelper.getAllAnnotationsFromHierachy(type, Index.class);
            for (Annotation a : lst) {
                Index i = (Index) a;
                if (i.value().length > 0) {
                    List<Map<String, Object>> options = null;
                    if (i.options().length > 0) {
                        //options set
                        options = createIndexMapFrom(i.options());
                    }
                    List<Map<String, Object>> idx = createIndexMapFrom(i.value());
                    int cnt = 0;
                    for (Map<String, Object> m : idx) {
                        Map<String, Object> optionsMap = null;
                        if (options != null && options.size() > cnt) {
                            optionsMap = options.get(cnt);
                        }
                        getWriterForClass(type).ensureIndex(type, onCollection, m, optionsMap, callback);
                        cnt++;
                    }
                }
            }
        }

        @SuppressWarnings("unchecked") List<String> flds = annotationHelper.getFields(type, Index.class);
        if (flds != null && !flds.isEmpty()) {

            for (String f : flds) {
                Index i = annotationHelper.getField(type, f).getAnnotation(Index.class);
                Map<String, Object> idx = new LinkedHashMap<>();
                if (i.decrement()) {
                    idx.put(f, -1);
                } else {
                    idx.put(f, 1);
                }
                Map<String, Object> optionsMap = null;
                if (createIndexMapFrom(i.options()) != null) {
                    optionsMap = createIndexMapFrom(i.options()).get(0);
                }
                getWriterForClass(type).ensureIndex(type, onCollection, idx, optionsMap, callback);
            }
        }
    }


    /**
     * converts the given type to capped collection in Mongo, even if no @capped is defined!
     * <b>Warning:</b> depending on size this might take some time!
     *
     * @param c    entity type
     * @param size size of capped collection
     * @param cb   callback
     * @param <T>  type
     */
    public <T> void convertToCapped(Class<T> c, int size, AsyncOperationCallback<T> cb) {
        convertToCapped(getMapper().getCollectionName(c), size, cb);
        //Indexes are not available after converting - recreate them
        ensureIndicesFor(c, cb);
    }

    @SuppressWarnings("UnusedParameters")
    public <T> void convertToCapped(String coll, int size, AsyncOperationCallback<T> cb) {
        Map<String, Object> cmd = new LinkedHashMap<>();
        cmd.put("convertToCapped", coll);
        cmd.put("size", size);
        //        cmd.put("max", max);
        try {
            morphiumDriver.runCommand(config.getDatabase(), cmd);
        } catch (MorphiumDriverException e) {
            throw new RuntimeException(e);
        }

    }

    public Map<String, Object> execCommand(String cmd) {
        Map<String, Object> map = new HashMap<>();
        map.put(cmd, "1");
        return execCommand(map);
    }

    public Map<String, Object> execCommand(Map<String, Object> command) {
        Map<String, Object> cmd = new LinkedHashMap<>(command);
        Map<String, Object> ret;
        try {
            ret = morphiumDriver.runCommand(config.getDatabase(), cmd);
        } catch (MorphiumDriverException e) {
            throw new RuntimeException(e);
        }
        return ret;
    }

    /**
     * automatically convert the collection for the given type to a capped collection
     * only works if @Capped annotation is given for type
     *
     * @param c - type
     */
    public <T> void ensureCapped(final Class<T> c) {
        ensureCapped(c, null);
    }

    public <T> void ensureCapped(final Class<T> c, final AsyncOperationCallback<T> callback) {
        Runnable r = () -> {
            String coll = getMapper().getCollectionName(c);
            //                DBCollection collection = null;

            try {
                boolean exists = morphiumDriver.exists(config.getDatabase(), coll);
                if (exists && morphiumDriver.isCapped(config.getDatabase(), coll)) {
                    return;
                }
                if (config.isAutoIndexAndCappedCreationOnWrite() && !exists) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Collection does not exist - ensuring indices / capped status");
                    }
                    Map<String, Object> cmd = new LinkedHashMap<>();
                    cmd.put("create", coll);
                    Capped capped = annotationHelper.getAnnotationFromHierarchy(c, Capped.class);
                    if (capped != null) {
                        cmd.put("capped", true);
                        cmd.put("size", capped.maxSize());
                        cmd.put("max", capped.maxEntries());
                    }
                    cmd.put("autoIndexId", (annotationHelper.getIdField(c).getType().equals(MorphiumId.class)));
                    morphiumDriver.runCommand(config.getDatabase(), cmd);
                } else {
                    Capped capped = annotationHelper.getAnnotationFromHierarchy(c, Capped.class);
                    if (capped != null) {

                        convertToCapped(c, capped.maxSize(), null);
                    }
                }
            } catch (Throwable t) {
                throw new RuntimeException(t);
            }

        };

        if (callback == null) {
            r.run();
        } else {
            asyncOperationsThreadPool.execute(r);
            //            new Thread(r).start();
        }
    }


    public Map<String, Object> simplifyQueryObject(Map<String, Object> q) {
        if (q.keySet().size() == 1 && q.get("$and") != null) {
            Map<String, Object> ret = new HashMap<>();
            @SuppressWarnings("unchecked") List<Map<String, Object>> lst = (List<Map<String, Object>>) q.get("$and");
            for (Object o : lst) {
                if (o instanceof Map) {
                    //noinspection unchecked
                    ret.putAll(((Map) o));
                } else {
                    //something we cannot handle
                    return q;
                }
            }
            return ret;
        }
        return q;
    }

    public <T> void set(Query<T> query, Enum field, Object val) {
        set(query, field, val, (AsyncOperationCallback<T>) null);
    }

    public <T> void set(Query<T> query, Enum field, Object val, AsyncOperationCallback<T> callback) {
        Map<String, Object> toSet = new HashMap<>();
        toSet.put(field.name(), val);
        getWriterForClass(query.getType()).set(query, toSet, false, false, callback);
    }

    public <T> void set(Query<T> query, String field, Object val) {
        set(query, field, val, (AsyncOperationCallback<T>) null);
    }

    public <T> void set(Query<T> query, String field, Object val, AsyncOperationCallback<T> callback) {
        Map<String, Object> toSet = new HashMap<>();
        toSet.put(field, val);
        getWriterForClass(query.getType()).set(query, toSet, false, false, callback);
    }

    @SuppressWarnings("unused")
    public void setEnum(Query<?> query, Map<Enum, Object> values, boolean upsert, boolean multiple) {
        HashMap<String, Object> toSet = new HashMap<>();
        for (Map.Entry<Enum, Object> est : values.entrySet()) {
            //noinspection SuspiciousMethodCalls,SuspiciousMethodCalls
            toSet.put(est.getKey().name(), values.get(est.getValue()));
        }
        set(query, toSet, upsert, multiple);
    }

    public void push(final Query<?> query, final Enum field, final Object value) {
        push(query, field, value, false, true);
    }

    @SuppressWarnings({"unchecked", "UnusedDeclaration"})
    public void pull(Query<?> query, Enum field, Object value) {
        pull(query, field.name(), value, false, true);
    }

    public void push(Query<?> query, String field, Object value) {
        push(query, field, value, false, true);
    }

    @SuppressWarnings("unused")
    public void pull(Query<?> query, String field, Object value) {
        pull(query, field, value, false, true);
    }


    public void push(Query<?> query, Enum field, Object value, boolean upsert, boolean multiple) {
        push(query, field.name(), value, upsert, multiple);
    }

    @SuppressWarnings({"unchecked", "UnusedDeclaration"})
    public void pull(Query<?> query, Enum field, Object value, boolean upsert, boolean multiple) {
        pull(query, field.name(), value, upsert, multiple);
    }

    @SuppressWarnings({"unchecked", "UnusedDeclaration"})
    public void pushAll(Query<?> query, Enum field, List<Object> value, boolean upsert, boolean multiple) {
        push(query, field.name(), value, upsert, multiple);
    }

    @SuppressWarnings({"unchecked", "UnusedDeclaration"})
    public void pullAll(Query<?> query, Enum field, List<Object> value, boolean upsert, boolean multiple) {
        pull(query, field.name(), value, upsert, multiple);
    }


    public <T> void push(final Query<T> query, final String field, final Object value, final boolean upsert, final boolean multiple) {
        push(query, field, value, upsert, multiple, null);
    }

    /**
     * asynchronous call to callback
     *
     * @param query    - the query
     * @param field    - field to push values to
     * @param value    - value to push
     * @param upsert   - insert object, if it does not exist
     * @param multiple - more than one
     * @param callback - will be called, when operation succeeds - synchronous call, if null
     * @param <T>      - the type
     */
    @SuppressWarnings("UnusedParameters")
    public <T> void push(final Query<T> query, final String field, final Object value, final boolean upsert, final boolean multiple, final AsyncOperationCallback<T> callback) {
        if (query == null || field == null) {
            throw new RuntimeException("Cannot update null!");
        }
        getWriterForClass(query.getType()).pushPull(true, query, field, value, upsert, multiple, null);

    }

    public <T> void pull(final Query<T> query, final String field, final Object value, final boolean upsert, final boolean multiple) {
        pull(query, field, value, upsert, multiple, null);
    }

    /**
     * Asynchronous call to pulll
     *
     * @param query    - query
     * @param field    - field to pull
     * @param value    - value to pull from field
     * @param upsert   - insert document unless it exists
     * @param multiple - more than one
     * @param callback -callback to call when operation succeeds - synchronous call, if null
     * @param <T>      - type
     */
    public <T> void pull(final Query<T> query, final String field, final Object value, final boolean upsert, final boolean multiple, final AsyncOperationCallback<T> callback) {
        if (query == null || field == null) {
            throw new RuntimeException("Cannot update null!");
        }
        MorphiumWriter wr = getWriterForClass(query.getType());
        wr.pushPull(false, query, field, value, upsert, multiple, callback);
    }

    public void pushAll(final Query<?> query, final String field, final List<?> value, final boolean upsert, final boolean multiple) {
        pushAll(query, field, value, upsert, multiple, null);
    }

    public <T> void pushAll(final Query<T> query, final String field, final List<?> value, final boolean upsert, final boolean multiple, final AsyncOperationCallback<T> callback) {
        if (query == null || field == null) {
            throw new RuntimeException("Cannot update null!");
        }
        MorphiumWriter wr = getWriterForClass(query.getType());
        wr.pushPullAll(true, query, field, value, upsert, multiple, callback);


    }

    /**
     * will change an entry in mongodb-collection corresponding to given class object
     * if query is too complex, upsert might not work!
     * Upsert should consist of single and-queries, which will be used to generate the object to create, unless
     * it already exists. look at Mongodb-query documentation as well
     *
     * @param query    - query to specify which objects should be set
     * @param field    - field to set
     * @param val      - value to set
     * @param upsert   - insert, if it does not exist (query needs to be simple!)
     * @param multiple - update several documents, if false, only first hit will be updated
     */
    @SuppressWarnings("unused")
    public <T> void set(Query<T> query, Enum field, Object val, boolean upsert, boolean multiple) {
        set(query, field.name(), val, upsert, multiple, (AsyncOperationCallback<Query<T>>) null);
    }

    @SuppressWarnings("unused")
    public <T> void set(Query<T> query, Enum field, Object val, boolean upsert, boolean multiple, AsyncOperationCallback<Query<T>> callback) {
        set(query, field.name(), val, upsert, multiple, callback);
    }


    @SuppressWarnings({"unchecked", "UnusedDeclaration"})
    public void pullAll(Query<?> query, String field, List<Object> value, boolean upsert, boolean multiple) {
        pull(query, field, value, upsert, multiple);
    }

    public <T> void set(Query<T> query, String field, Object val, boolean upsert, boolean multiple) {
        set(query, field, val, upsert, multiple, (AsyncOperationCallback<T>) null);
    }

    public <T> void set(Query<T> query, String field, Object val, boolean upsert, boolean multiple, AsyncOperationCallback<T> callback) {
        Map<String, Object> map = new HashMap<>();
        map.put(field, val);
        set(query, map, upsert, multiple, callback);
    }

    public void set(final Query<?> query, final Map<String, Object> map, final boolean upsert, final boolean multiple) {
        set(query, map, upsert, multiple, null);
    }

    public <T> void set(final Query<T> query, final Map<String, Object> map, final boolean upsert, final boolean multiple, AsyncOperationCallback<T> callback) {
        if (query == null) {
            throw new RuntimeException("Cannot update null!");
        }
        getWriterForClass(query.getType()).set(query, map, upsert, multiple, callback);
    }


    @SuppressWarnings("unused")
    public <T> void set(T toSet, Enum field, Object value, AsyncOperationCallback<T> callback) {
        set(toSet, field.name(), value, callback);
    }

    @SuppressWarnings("unused")
    public void set(Object toSet, Enum field, Object value) {
        set(toSet, field.name(), value, null);
    }

    /**
     * setting a value in an existing mongo collection entry - no reading necessary. Object is altered in place
     * db.collection.update({"_id":toSet.id},{$set:{field:value}}
     * <b>attention</b>: this alteres the given object toSet in a similar way
     *
     * @param toSet - object to set the value in (or better - the corresponding entry in mongo)
     * @param field - the field to change
     * @param value - the value to set
     */
    public void set(final Object toSet, final String field, final Object value) {
        set(toSet, field, value, null);
    }

    public <T> void set(final T toSet, final String field, final Object value, boolean upserts, boolean multiple, AsyncOperationCallback<T> callback) {
        set(toSet, getMapper().getCollectionName(toSet.getClass()), field, value, upserts, multiple, callback);
    }

    public <T> void set(final T toSet, String collection, final String field, final Object value, boolean upserts, boolean multiple, AsyncOperationCallback<T> callback) {
        if (toSet == null) {
            throw new RuntimeException("Cannot update null!");
        }

        if (getId(toSet) == null) {
            logger.info("just storing object as it is new...");
            store(toSet);
            return;
        }
        annotationHelper.callLifecycleMethod(PreUpdate.class, toSet);
        getWriterForClass(toSet.getClass()).set(toSet, collection, field, value, upserts, multiple, callback);
        annotationHelper.callLifecycleMethod(PostUpdate.class, toSet);
    }

    public <T> void set(final T toSet, final String field, final Object value, final AsyncOperationCallback<T> callback) {
        set(toSet, field, value, false, false, callback);

    }


    ////////// DEC and INC Methods

    @SuppressWarnings({"unchecked", "UnusedDeclaration"})
    public void dec(Query<?> query, Enum field, double amount, boolean upsert, boolean multiple) {
        dec(query, field.name(), -amount, upsert, multiple);
    }

    @SuppressWarnings("unused")
    public void dec(Query<?> query, Enum field, long amount, boolean upsert, boolean multiple) {
        dec(query, field.name(), -amount, upsert, multiple);
    }

    @SuppressWarnings("unused")
    public void dec(Query<?> query, Enum field, Number amount, boolean upsert, boolean multiple) {
        dec(query, field.name(), -amount.doubleValue(), upsert, multiple);
    }

    @SuppressWarnings("unused")
    public void dec(Query<?> query, Enum field, int amount, boolean upsert, boolean multiple) {
        dec(query, field.name(), -amount, upsert, multiple);
    }

    public void dec(Query<?> query, String field, double amount, boolean upsert, boolean multiple) {
        inc(query, field, -amount, upsert, multiple);
    }

    public void dec(Query<?> query, String field, long amount, boolean upsert, boolean multiple) {
        inc(query, field, -amount, upsert, multiple);
    }

    public void dec(Query<?> query, String field, int amount, boolean upsert, boolean multiple) {
        inc(query, field, -amount, upsert, multiple);
    }

    @SuppressWarnings("unused")
    public void dec(Query<?> query, String field, Number amount, boolean upsert, boolean multiple) {
        inc(query, field, -amount.doubleValue(), upsert, multiple);
    }

    @SuppressWarnings("unused")
    public void dec(Query<?> query, String field, double amount) {
        inc(query, field, -amount, false, false);
    }

    @SuppressWarnings("unused")
    public void dec(Query<?> query, String field, long amount) {
        inc(query, field, -amount, false, false);
    }

    public void dec(Query<?> query, String field, int amount) {
        inc(query, field, -amount, false, false);
    }

    @SuppressWarnings("unused")
    public void dec(Query<?> query, String field, Number amount) {
        inc(query, field, -amount.doubleValue(), false, false);
    }

    @SuppressWarnings({"unchecked", "UnusedDeclaration"})
    public void dec(Query<?> query, Enum field, double amount) {
        inc(query, field, -amount, false, false);
    }

    @SuppressWarnings("unused")
    public void dec(Query<?> query, Enum field, long amount) {
        inc(query, field, -amount, false, false);
    }

    @SuppressWarnings("unused")
    public void dec(Query<?> query, Enum field, int amount) {
        inc(query, field, -amount, false, false);
    }

    @SuppressWarnings("unused")
    public void dec(Query<?> query, Enum field, Number amount) {
        inc(query, field, -amount.doubleValue(), false, false);
    }


    @SuppressWarnings("unused")
    public void inc(Query<?> query, String field, long amount) {
        inc(query, field, amount, false, false);
    }

    public void inc(Query<?> query, String field, int amount) {
        inc(query, field, amount, false, false);
    }

    @SuppressWarnings("unused")
    public void inc(Query<?> query, String field, Number amount) {
        inc(query, field, amount, false, false);
    }

    @SuppressWarnings("unused")
    public void inc(Query<?> query, String field, double amount) {
        inc(query, field, amount, false, false);
    }

    @SuppressWarnings({"unchecked", "UnusedDeclaration"})
    public void inc(Query<?> query, Enum field, double amount) {
        inc(query, field, amount, false, false);
    }

    @SuppressWarnings("unused")
    public void inc(Query<?> query, Enum field, long amount) {
        inc(query, field, amount, false, false);
    }

    @SuppressWarnings("unused")
    public void inc(Query<?> query, Enum field, int amount) {
        inc(query, field, amount, false, false);
    }

    @SuppressWarnings("unused")
    public void inc(Query<?> query, Enum field, Number amount) {
        inc(query, field, amount, false, false);
    }

    public void inc(Query<?> query, Enum field, double amount, boolean upsert, boolean multiple) {
        inc(query, field.name(), amount, upsert, multiple);
    }

    public void inc(Query<?> query, Enum field, long amount, boolean upsert, boolean multiple) {
        inc(query, field.name(), amount, upsert, multiple);
    }

    public void inc(Query<?> query, Enum field, int amount, boolean upsert, boolean multiple) {
        inc(query, field.name(), amount, upsert, multiple);
    }

    public void inc(Query<?> query, Enum field, Number amount, boolean upsert, boolean multiple) {
        inc(query, field.name(), amount, upsert, multiple);
    }

    public <T> void inc(final Query<T> query, final Map<String, Number> toUptad, final boolean upsert, final boolean multiple, AsyncOperationCallback<T> callback) {
        if (query == null) {
            throw new RuntimeException("Cannot update null!");
        }
        getWriterForClass(query.getType()).inc(query, toUptad, upsert, multiple, callback);
    }

    public void inc(final Query<?> query, final String name, final long amount, final boolean upsert, final boolean multiple) {
        inc(query, name, amount, upsert, multiple, null);
    }

    public void inc(final Query<?> query, final String name, final int amount, final boolean upsert, final boolean multiple) {
        inc(query, name, amount, upsert, multiple, null);
    }

    public void inc(final Query<?> query, final String name, final double amount, final boolean upsert, final boolean multiple) {
        inc(query, name, amount, upsert, multiple, null);
    }

    public void inc(final Query<?> query, final String name, final Number amount, final boolean upsert, final boolean multiple) {
        inc(query, name, amount, upsert, multiple, null);
    }

    public <T> void inc(final Query<T> query, final String name, final long amount, final boolean upsert, final boolean multiple, final AsyncOperationCallback<T> callback) {
        if (query == null) {
            throw new RuntimeException("Cannot update null!");
        }
        getWriterForClass(query.getType()).inc(query, name, amount, upsert, multiple, callback);
    }

    public <T> void inc(final Query<T> query, final String name, final int amount, final boolean upsert, final boolean multiple, final AsyncOperationCallback<T> callback) {
        if (query == null) {
            throw new RuntimeException("Cannot update null!");
        }
        getWriterForClass(query.getType()).inc(query, name, amount, upsert, multiple, callback);
    }

    public <T> void inc(final Query<T> query, final String name, final double amount, final boolean upsert, final boolean multiple, final AsyncOperationCallback<T> callback) {
        if (query == null) {
            throw new RuntimeException("Cannot update null!");
        }
        getWriterForClass(query.getType()).inc(query, name, amount, upsert, multiple, callback);

    }

    public <T> void inc(final Query<T> query, final String name, final Number amount, final boolean upsert, final boolean multiple, final AsyncOperationCallback<T> callback) {
        if (query == null) {
            throw new RuntimeException("Cannot update null!");
        }
        getWriterForClass(query.getType()).inc(query, name, amount, upsert, multiple, callback);

    }


    public MorphiumWriter getWriterForClass(Class<?> cls) {

        if (annotationHelper.isBufferedWrite(cls) && isWriteBufferEnabledForThread()) {
            return config.getBufferedWriter();
        } else if (annotationHelper.isAsyncWrite(cls) && isAsyncWritesEnabledForThread()) {
            return config.getAsyncWriter();
        } else {
            return config.getWriter();
        }
    }

    /**
     * decreasing a value of a given object
     * calles <code>inc(toDec,field,-amount);</code>
     */
    @SuppressWarnings("unused")
    public void dec(Object toDec, String field, double amount) {
        inc(toDec, field, -amount);
    }

    public void dec(Object toDec, String field, int amount) {
        inc(toDec, field, -amount);
    }

    @SuppressWarnings("unused")
    public void dec(Object toDec, String field, long amount) {
        inc(toDec, field, -amount);
    }

    @SuppressWarnings("unused")
    public void dec(Object toDec, String field, Number amount) {
        inc(toDec, field, -amount.doubleValue());
    }

    public void inc(final Object toSet, final String field, final long i) {
        inc(toSet, field, i, null);
    }

    public void inc(final Object toSet, final String field, final int i) {
        inc(toSet, field, i, null);
    }

    public void inc(final Object toSet, final String field, final double i) {
        inc(toSet, field, i, null);
    }

    @SuppressWarnings("unused")
    public void inc(final Object toSet, final String field, final Number i) {
        inc(toSet, field, i, null);
    }

    public <T> void inc(final T toSet, final String field, final double i, final AsyncOperationCallback<T> callback) {
        inc(toSet, getMapper().getCollectionName(toSet.getClass()), field, i, callback);
    }

    public <T> void inc(final T toSet, final String field, final int i, final AsyncOperationCallback<T> callback) {
        inc(toSet, getMapper().getCollectionName(toSet.getClass()), field, i, callback);
    }

    public <T> void inc(final T toSet, final String field, final long i, final AsyncOperationCallback<T> callback) {
        inc(toSet, getMapper().getCollectionName(toSet.getClass()), field, i, callback);
    }

    public <T> void inc(final T toSet, final String field, final Number i, final AsyncOperationCallback<T> callback) {
        inc(toSet, getMapper().getCollectionName(toSet.getClass()), field, i, callback);
    }

    public <T> void inc(final T toSet, String collection, final String field, final double i, final AsyncOperationCallback<T> callback) {
        if (toSet == null) {
            throw new RuntimeException("Cannot update null!");
        }

        if (getId(toSet) == null) {
            logger.info("just storing object as it is new...");
            store(toSet);
            return;
        }
        getWriterForClass(toSet.getClass()).inc(toSet, collection, field, i, callback);
    }

    public <T> void inc(final T toSet, String collection, final String field, final int i, final AsyncOperationCallback<T> callback) {
        if (toSet == null) {
            throw new RuntimeException("Cannot update null!");
        }

        if (getId(toSet) == null) {
            logger.info("just storing object as it is new...");
            store(toSet);
            return;
        }
        getWriterForClass(toSet.getClass()).inc(toSet, collection, field, i, callback);
    }

    public <T> void inc(final T toSet, String collection, final String field, final long i, final AsyncOperationCallback<T> callback) {
        if (toSet == null) {
            throw new RuntimeException("Cannot update null!");
        }

        if (getId(toSet) == null) {
            logger.info("just storing object as it is new...");
            store(toSet);
            return;
        }
        getWriterForClass(toSet.getClass()).inc(toSet, collection, field, i, callback);
    }

    public <T> void inc(final T toSet, String collection, final String field, final Number i, final AsyncOperationCallback<T> callback) {
        if (toSet == null) {
            throw new RuntimeException("Cannot update null!");
        }

        if (getId(toSet) == null) {
            logger.info("just storing object as it is new...");
            store(toSet);
            return;
        }
        getWriterForClass(toSet.getClass()).inc(toSet, collection, field, i, callback);
    }

    public <T> void delete(List<T> lst, String forceCollectionName) {
        delete(lst, forceCollectionName, (AsyncOperationCallback<T>) null);
    }

    public <T> void delete(List<T> lst, String forceCollectionName, AsyncOperationCallback<T> callback) {
        ArrayList<T> directDel = new ArrayList<>();
        ArrayList<T> bufferedDel = new ArrayList<>();
        for (T o : lst) {
            if (annotationHelper.isBufferedWrite(o.getClass())) {
                bufferedDel.add(o);
            } else {
                directDel.add(o);
            }
        }

        for (T o : bufferedDel) {
            config.getBufferedWriter().remove(o, forceCollectionName, callback);
        }
        for (T o : directDel) {
            config.getWriter().remove(o, forceCollectionName, callback);
        }
    }

    @SuppressWarnings("unused")
    public <T> void delete(List<T> lst, AsyncOperationCallback<T> callback) {
        ArrayList<T> directDel = new ArrayList<>();
        ArrayList<T> bufferedDel = new ArrayList<>();
        for (T o : lst) {
            if (annotationHelper.isBufferedWrite(o.getClass())) {
                bufferedDel.add(o);
            } else {
                directDel.add(o);
            }
        }
        config.getBufferedWriter().remove(bufferedDel, callback);
        config.getWriter().remove(directDel, callback);
    }

    public void inc(StatisticKeys k) {
        stats.get(k).inc();
    }


    /**
     * updating an enty in DB without sending the whole entity
     * only transfers the fields to be changed / set
     *
     * @param ent    - entity to update
     * @param fields - fields to use
     */
    public void updateUsingFields(final Object ent, final String... fields) {
        updateUsingFields(ent, null, fields);
    }

    public <T> void updateUsingFields(final T ent, AsyncOperationCallback<T> callback, final String... fields) {
        updateUsingFields(ent, getMapper().getCollectionName(ent.getClass()), callback, fields);
    }

    @SuppressWarnings("UnusedParameters")
    public <T> void updateUsingFields(final T ent, String collection, AsyncOperationCallback<T> callback, final String... fields) {
        if (ent == null) {
            return;
        }
        if (fields.length == 0) {
            return; //not doing an update - no change
        }

        //        if (annotationHelper.isAnnotationPresentInHierarchy(ent.getClass(), NoCache.class)) {
        //            config.getWriter().updateUsingFields(ent, collection, null, fields);
        //            return;
        //        }
        getWriterForClass(ent.getClass()).updateUsingFields(ent, collection, null, fields);
    }


    public ObjectMapper getMapper() {
        return objectMapper;
    }

    public AnnotationAndReflectionHelper getARHelper() {
        return annotationHelper;
    }


    /**
     * careful this actually changes the parameter o!
     *
     * @param o   - object to read
     * @param <T> - tpye of the object
     * @return -  entity
     */
    public <T> T reread(T o) {
        return reread(o, objectMapper.getCollectionName(o.getClass()));
    }

    public <T> T reread(T o, String collection) {
        if (o == null) {
            return null;
        }
        Object id = getId(o);
        if (id == null) {
            return null;
        }
        //        DBCollection col = config.getDb().getCollection(collection);
        Map<String, Object> srch = new HashMap<>();
        srch.put("_id", id);
        //        List<Field> lst = annotationHelper.getAllFields(o.getClass());
        //        Map<String, Object> fields = new HashMap<>();
        //        for (Field f : lst) {
        //            if (f.isAnnotationPresent(WriteOnly.class) || f.isAnnotationPresent(Transient.class)) {
        //                continue;
        //            }
        //            String n = annotationHelper.getFieldName(o.getClass(), f.getName());
        //            fields.put(n, 1);
        //        }

        try {
            Map<String, Object> findMetaData = new HashMap<>();
            List<Map<String, Object>> found = morphiumDriver.find(config.getDatabase(), collection, srch, null, null, 0, 1, 1, null, findMetaData);
            if (found != null && !found.isEmpty()) {
                Map<String, Object> dbo = found.get(0);
                Object fromDb = objectMapper.unmarshall(o.getClass(), dbo);
                if (fromDb == null) {
                    throw new RuntimeException("could not reread from db");
                }
                @SuppressWarnings("unchecked") List<String> flds = annotationHelper.getFields(o.getClass());
                for (String f : flds) {
                    Field fld = annotationHelper.getField(o.getClass(), f);
                    if (java.lang.reflect.Modifier.isStatic(fld.getModifiers())) {
                        continue;
                    }
                    try {
                        fld.set(o, fld.get(fromDb));
                    } catch (IllegalAccessException e) {
                        logger.error("Could not set Value: " + fld);
                    }
                }
                firePostLoadEvent(o);
            } else {
                return null;
            }
        } catch (MorphiumDriverException e) {
            throw new RuntimeException(e);
        }
        return o;
    }

    ///Event handling
    public void firePreStore(Object o, boolean isNew) {
        if (o == null) {
            return;
        }
        for (MorphiumStorageListener l : listeners) {
            //noinspection unchecked
            l.preStore(this, o, isNew);
        }
        annotationHelper.callLifecycleMethod(PreStore.class, o);

    }

    public void firePostStore(Object o, boolean isNew) {
        for (MorphiumStorageListener l : listeners) {
            //noinspection unchecked
            l.postStore(this, o, isNew);
        }
        annotationHelper.callLifecycleMethod(PostStore.class, o);
        //existing object  => store last Access, if needed

    }

    public void firePreDrop(Class cls) {
        for (MorphiumStorageListener l : listeners) {
            //noinspection unchecked
            l.preDrop(this, cls);
        }

    }

    public <T> void firePostStore(Map<T, Boolean> isNew) {
        for (MorphiumStorageListener l : listeners) {
            //noinspection unchecked
            l.postStore(this, isNew);
        }
        for (Object o : isNew.keySet()) annotationHelper.callLifecycleMethod(PreStore.class, o);

    }

    public <T> void firePostRemove(List<T> toRemove) {
        for (MorphiumStorageListener l : listeners) {
            //noinspection unchecked
            l.postRemove(this, toRemove);
        }
        for (Object o : toRemove) annotationHelper.callLifecycleMethod(PostRemove.class, o);

    }

    public <T> void firePostLoad(List<T> loaded) {
        for (MorphiumStorageListener l : listeners) {
            //noinspection unchecked
            l.postLoad(this, loaded);
        }
        for (Object o : loaded) annotationHelper.callLifecycleMethod(PostLoad.class, o);

    }


    public void firePreStore(Map<Object, Boolean> isNew) {
        for (MorphiumStorageListener l : listeners) {
            //noinspection unchecked
            l.preStore(this, isNew);
        }
        for (Object o : isNew.keySet()) annotationHelper.callLifecycleMethod(PreStore.class, o);
    }

    public <T> void firePreRemove(List<T> lst) {
        for (MorphiumStorageListener l : listeners) {
            //noinspection unchecked
            l.preRemove(this, lst);
        }
        for (T o : lst) annotationHelper.callLifecycleMethod(PreRemove.class, o);
    }

    public void firePreRemove(Object o) {
        for (MorphiumStorageListener l : listeners) {
            //noinspection unchecked
            l.preRemove(this, o);
        }
        annotationHelper.callLifecycleMethod(PreRemove.class, o);
    }

    @SuppressWarnings("unchecked")
    public void firePostDropEvent(Class cls) {
        for (MorphiumStorageListener l : listeners) {
            l.postDrop(this, cls);
        }
    }

    @SuppressWarnings("unchecked")
    public void firePostUpdateEvent(Class cls, MorphiumStorageListener.UpdateTypes t) {
        for (MorphiumStorageListener l : listeners) {
            l.postUpdate(this, cls, t);
        }
    }

    @SuppressWarnings("unchecked")
    public void firePreUpdateEvent(Class cls, MorphiumStorageListener.UpdateTypes t) {
        for (MorphiumStorageListener l : listeners) {
            l.preUpdate(this, cls, t);
        }
    }

    @SuppressWarnings("unchecked")
    public void firePostRemoveEvent(Object o) {
        for (MorphiumStorageListener l : listeners) {
            l.postRemove(this, o);
        }
        annotationHelper.callLifecycleMethod(PostRemove.class, o);
    }

    @SuppressWarnings("unchecked")
    public void firePostRemoveEvent(Query q) {
        for (MorphiumStorageListener l : listeners) {
            l.postRemove(this, q);
        }
    }


    @SuppressWarnings("unchecked")
    public void firePreRemoveEvent(Query q) {
        for (MorphiumStorageListener l : listeners) {
            l.preRemove(this, q);
        }
    }

    /**
     * de-references the given object of type T. If itself or any of its members is a Proxy (PartiallyUpdateableProxy or LazyDeReferencingProxy), it'll be removed and replaced
     * by the real objet.
     * This is not recursive, only the members here are de-referenced
     *
     * @param obj - the object to replact
     * @param <T> - type
     * @return the dereferenced object
     */
    @SuppressWarnings("unused")
    public <T> T deReference(T obj) {
        if (obj instanceof LazyDeReferencingProxy) {
            //noinspection unchecked
            obj = ((LazyDeReferencingProxy<T>) obj).__getDeref();
        }
        if (obj instanceof PartiallyUpdateableProxy) {
            //noinspection unchecked
            obj = ((PartiallyUpdateableProxy<T>) obj).__getDeref();
        }
        List<Field> flds = getARHelper().getAllFields(obj.getClass());
        for (Field fld : flds) {
            fld.setAccessible(true);
            Reference r = fld.getAnnotation(Reference.class);
            if (r != null && r.lazyLoading()) {
                try {
                    LazyDeReferencingProxy v = (LazyDeReferencingProxy) fld.get(obj);
                    Object value = v.__getDeref();
                    fld.set(obj, value);
                } catch (IllegalAccessException e) {
                    logger.error("dereferencing of field " + fld.getName() + " failed", e);
                }
            }
            try {
                if (fld.get(obj) != null && getARHelper().isAnnotationPresentInHierarchy(fld.getType(), Entity.class) && fld.get(obj) instanceof PartiallyUpdateableProxy) {
                    fld.set(obj, ((PartiallyUpdateableProxy) fld.get(obj)).__getDeref());
                }
            } catch (IllegalAccessException ignored) {
            }
        }
        return obj;
    }

    /**
     * will be called by query after unmarshalling
     *
     * @param o - entitiy
     */
    @SuppressWarnings("unchecked")
    public void firePostLoadEvent(Object o) {
        for (MorphiumStorageListener l : listeners) {
            l.postLoad(this, o);
        }
        annotationHelper.callLifecycleMethod(PostLoad.class, o);
    }


    /**
     * same as retReplicaSetStatus(false);
     *
     * @return replica set status
     */
    @SuppressWarnings("unused")
    private de.caluga.morphium.replicaset.ReplicaSetStatus getReplicaSetStatus() {
        return rsMonitor.getReplicaSetStatus(false);
    }

    public ReplicaSetStatus getCurrentRSState() {
        if (rsMonitor == null) {
            return null;
        }
        return rsMonitor.getCurrentStatus();
    }


    public boolean isReplicaSet() {
        return config.isReplicaset();
    }

    //
    //    public void handleNetworkError(int i, Throwable e) {
    //        logger.info("Handling network error..." + e.getClass().getName());
    //        if (e.getClass().getName().equals("javax.validation.ConstraintViolationException")) {
    //            throw ((RuntimeException) e);
    //        }
    //        if (e instanceof DuplicateKeyException) {
    //            throw new RuntimeException(e);
    //        }
    //        if (e.getMessage() != null && (e.getMessage().equals("can't find a master")
    //                || e.getMessage().startsWith("No replica set members available in")
    //                || e.getMessage().equals("not talking to master and retries used up"))
    //                || (e instanceof WriteConcernException && e.getMessage() != null && e.getMessage().contains("not master"))
    //                || e instanceof MongoException) {
    //            if (i + 1 < getConfig().getRetriesOnNetworkError()) {
    //                logger.warn("Retry because of network error: " + e.getMessage());
    //                try {
    //                    Thread.sleep(getConfig().getSleepBetweenNetworkErrorRetries());
    //                } catch (InterruptedException ignored) {
    //                }
    //
    //            } else {
    //                logger.info("no retries left - re-throwing exception");
    //                if (e instanceof RuntimeException) {
    //                    throw ((RuntimeException) e);
    //                }
    //                throw (new RuntimeException(e));
    //            }
    //        } else {
    //            if (e instanceof RuntimeException) {
    //                throw ((RuntimeException) e);
    //            }
    //            throw (new RuntimeException(e));
    //        }
    //    }


    public ReadPreference getReadPreferenceForClass(Class<?> cls) {
        DefaultReadPreference rp = annotationHelper.getAnnotationFromHierarchy(cls, DefaultReadPreference.class);
        if (rp == null) {
            return config.getDefaultReadPreference();
        }
        return rp.value().getPref();
    }

    public MorphiumBulkContext createBulkRequestContext(Class<?> type, boolean ordered) {
        return new MorphiumBulkContext(getDriver().createBulkContext(this, config.getDatabase(), getMapper().getCollectionName(type), ordered, getWriteConcernForClass(type)));
    }

    @SuppressWarnings("unused")
    public MorphiumBulkContext createBulkRequestContext(String collection, boolean ordered) {
        return new MorphiumBulkContext(getDriver().createBulkContext(this, config.getDatabase(), collection, ordered, null));
    }


    @SuppressWarnings("ConstantConditions")
    public WriteConcern getWriteConcernForClass(Class<?> cls) {
        if (logger.isDebugEnabled()) {
            logger.debug("returning write concern for " + cls.getSimpleName());
        }
        WriteSafety safety = annotationHelper.getAnnotationFromHierarchy(cls, WriteSafety.class);  // cls.getAnnotation(WriteSafety.class);
        if (safety == null) {
            return null;
        }
        @SuppressWarnings("deprecation") boolean fsync = safety.waitForSync();
        boolean j = safety.waitForJournalCommit();

        if (j && fsync) {
            fsync = false;
        }
        int w = safety.level().getValue();
        if (!isReplicaSet() && w > 1) {
            w = 1;
        }
        long timeout = safety.timeout();
        if (isReplicaSet() && w > 2) {
            de.caluga.morphium.replicaset.ReplicaSetStatus s = rsMonitor.getCurrentStatus();

            if (getConfig().isReplicaset() && s == null || s.getActiveNodes() == 0) {
                logger.warn("ReplicaSet status is null or no node active! Assuming default write concern");
                return null;
            }
            if (logger.isDebugEnabled()) {
                logger.debug("Active nodes now: " + s.getActiveNodes());
            }
            int activeNodes = s.getActiveNodes();

            long masterOpTime = 0;
            long maxReplLag = 0;
            for (ReplicaSetNode node : s.getMembers()) {
                if (node.getState() == 1) {
                    //Master
                    masterOpTime = node.getOptimeDate().getTime();
                }
            }
            for (ReplicaSetNode node : s.getMembers()) {
                if (node.getState() == 2) {
                    //Master
                    long tm = node.getOptimeDate().getTime() - masterOpTime;
                    if (maxReplLag < tm) {
                        maxReplLag = tm;
                    }
                }
            }
            if (timeout < 0) {
                //set timeout to replication lag * 3 - just to be sure
                if (logger.isDebugEnabled()) {
                    logger.debug("Setting timeout to replication lag*3");
                }
                if (maxReplLag < 0) {
                    maxReplLag = -maxReplLag;
                }
                if (maxReplLag == 0) {
                    maxReplLag = 1;
                }
                timeout = maxReplLag * 3000;
                if (maxReplLag > 10) {
                    logger.warn("Warning: replication lag too high! timeout set to " + timeout + "ms - replication Lag is " + maxReplLag + "s - write should take place in Background!");
                }

            }
            //Wait for all active slaves (-1 for the timeout bug)
            w = activeNodes;
            if (timeout > 0 && timeout < maxReplLag * 1000) {
                logger.warn("Timeout is set smaller than replication lag - increasing to replication_lag time * 3");
                timeout = maxReplLag * 3000;
            }
        }

        return WriteConcern.getWc(w, fsync, j, (int) timeout);
    }

    public void addProfilingListener(ProfilingListener l) {
        profilingListeners.add(l);
    }

    public void removeProfilingListener(ProfilingListener l) {
        profilingListeners.remove(l);
    }

    public void fireProfilingWriteEvent(Class type, Object data, long time, boolean isNew, WriteAccessType wt) {
        for (ProfilingListener l : profilingListeners) {
            try {
                l.writeAccess(type, data, time, isNew, wt);
            } catch (Throwable e) {
                logger.error("Error during profiling: ", e);
            }
        }
    }

    public void fireProfilingReadEvent(Query q, long time, ReadAccessType t) {
        for (ProfilingListener l : profilingListeners) {
            try {
                l.readAccess(q, time, t);
            } catch (Throwable e) {
                logger.error("Error during profiling", e);
            }
        }
    }


    /**
     * issues a remove command - no lifecycle methods calles, no drop, keeps all indexec this way
     *
     * @param cls - class
     */
    @SuppressWarnings("unchecked")
    public void clearCollection(Class<?> cls) {
        delete(createQueryFor(cls));
    }

    /**
     * issues a remove command - no lifecycle methods calles, no drop, keeps all indexec this way
     * But uses sepcified collection name instead deriving name from class
     *
     * @param cls     - class
     * @param colName - CollectionName
     */
    public void clearCollection(Class<?> cls, String colName) {
        Query q = createQueryFor(cls);
        q.setCollectionName(colName);
        delete(q);
    }

    /**
     * clears every single object in collection - reads ALL objects to do so
     * this way Lifecycle methods can be called!
     *
     * @param cls -class
     */

    @SuppressWarnings("unused")
    public void clearCollectionOneByOne(Class<?> cls) {
        inc(StatisticKeys.WRITES);
        List<?> lst = readAll(cls);
        lst.forEach(this::delete);

        getCache().clearCacheIfNecessary(cls);


    }

    /**
     * return a list of all elements stored in morphium for this type
     *
     * @param cls - type to search for, needs to be an Property
     * @param <T> - Type
     * @return - list of all elements stored
     */
    public <T> List<T> readAll(Class<? extends T> cls) {
        inc(StatisticKeys.READS);
        Query<T> qu;
        qu = createQueryFor(cls);
        return qu.asList();
    }

    public <T> Query<T> createQueryFor(Class<? extends T> type, String usingCollectionName) {
        //noinspection unchecked
        return (Query<T>) createQueryFor(type).setCollectionName(usingCollectionName);
    }

    public <T> Query<T> createQueryFor(Class<? extends T> type) {
        Query<T> q = config.getQueryFact().createQuery(this, type);
        q.setMorphium(this);
        q.setAutoValuesEnabled(isAutoValuesEnabledForThread());
        return q;
    }

    public <T> List<T> find(Query<T> q) {
        return q.asList();
    }


    @SuppressWarnings("unused")
    public List<Object> distinct(Enum key, Class c) {
        return distinct(key.name(), c);
    }

    /**
     * returns a distinct list of values of the given collection
     * Attention: these values are not unmarshalled, you might get MongoMap<String,Object>s
     */
    @SuppressWarnings("unused")
    public List<Object> distinct(Enum key, Query q) {
        return distinct(key.name(), q);
    }

    /**
     * returns a distinct list of values of the given collection
     * Attention: these values are not unmarshalled, you might get MongoMap<String,Object>s
     */
    @SuppressWarnings("unchecked")
    public List<Object> distinct(String key, Query q) {
        try {
            return morphiumDriver.distinct(config.getDatabase(), q.getCollectionName(), key, q.toQueryObject(), getReadPreferenceForClass(q.getType()));
        } catch (MorphiumDriverException e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("unchecked")
    public List<Object> distinct(String key, Class cls) {
        try {
            return morphiumDriver.distinct(config.getDatabase(), objectMapper.getCollectionName(cls), key, new HashMap<>(), getReadPreferenceForClass(cls));
        } catch (MorphiumDriverException e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings({"unchecked", "unused"})
    public List<Object> distinct(String key, String collectionName) {
        try {
            return morphiumDriver.distinct(config.getDatabase(), collectionName, key, new HashMap<>(), config.getDefaultReadPreference());
        } catch (MorphiumDriverException e) {
            throw new RuntimeException(e);
        }
    }


    public Map<String, Object> group(Query q, Map<String, Object> initial, String jsReduce, String jsFinalize, ReadPreference rp, String... keys) {
        try {
            //noinspection unchecked
            return morphiumDriver.group(config.getDatabase(), objectMapper.getCollectionName(q.getType()), q.toQueryObject(), initial, jsReduce, jsFinalize, rp, keys);
        } catch (MorphiumDriverException e) {
            throw new RuntimeException(e);
        }
    }

    public Map<String, Object> group(Query q, Map<String, Object> initial, String jsReduce, String jsFinalize, String... keys) {
        return group(q, initial, jsReduce, jsFinalize, null, keys);
    }

    @SuppressWarnings("unchecked")
    public <T> T findById(Class<? extends T> type, Object id) {
        return findById(type, id, null);
    }

    public <T> T findById(Class<? extends T> type, Object id, String collection) {
        T ret = getCache().getFromIDCache(type, id);
        if (ret != null) {
            return ret;
        }
        @SuppressWarnings("unchecked") List<String> ls = annotationHelper.getFields(type, Id.class);
        if (ls.isEmpty()) {
            throw new RuntimeException("Cannot find by ID on non-Entity");
        }

        return createQueryFor(type).setCollectionName(collection).f(ls.get(0)).eq(id).get();
    }

    @SuppressWarnings("unchecked")
    public <T> List<T> findByField(Class<? extends T> cls, String fld, Object val) {
        Query<T> q = createQueryFor(cls);
        q = q.f(fld).eq(val);
        return q.asList();
    }

    public <T> List<T> findByField(Class<? extends T> cls, Enum fld, Object val) {
        return findByField(cls, fld.name(), val);
    }


    /**
     * Erase cache entries for the given type. is being called after every store
     * depending on cache settings!
     *
     * @param cls - class
     */
    public void clearCachefor(Class<?> cls) {
        getCache().clearCachefor(cls);
    }

    public void clearCacheforClassIfNecessary(Class<?> cls) {
        getCache().clearCacheIfNecessary(cls);
    }

    public <T> void storeNoCache(T lst) {
        storeNoCache(lst, getMapper().getCollectionName(lst.getClass()), null);
    }

    @SuppressWarnings("unused")
    public <T> void storeNoCache(T o, AsyncOperationCallback<T> callback) {
        storeNoCache(o, getMapper().getCollectionName(o.getClass()), callback);
    }

    public <T> void storeNoCache(T o, String collection) {
        storeNoCache(o, collection, null);
    }

    public <T> void storeNoCache(T o, String collection, AsyncOperationCallback<T> callback) {
        config.getWriter().store(o, collection, callback);
    }

    public <T> void storeBuffered(final T lst) {
        storeBuffered(lst, null);
    }

    public <T> void storeBuffered(final T lst, final AsyncOperationCallback<T> callback) {
        storeBuffered(lst, getMapper().getCollectionName(lst.getClass()), callback);
    }

    public <T> void storeBuffered(final T lst, String collection, final AsyncOperationCallback<T> callback) {

        config.getBufferedWriter().store(lst, collection, callback);
    }

    @SuppressWarnings("unused")
    public void flush() {
        config.getBufferedWriter().flush();
        config.getWriter().flush();
    }


    public Object getId(Object o) {
        return annotationHelper.getId(o);
    }

    public <T> void dropCollection(Class<T> cls, AsyncOperationCallback<T> callback) {
        dropCollection(cls, getMapper().getCollectionName(cls), callback);
    }

    public <T> void dropCollection(Class<T> cls, String collection, AsyncOperationCallback<T> callback) {
        getWriterForClass(cls).dropCollection(cls, collection, callback);
    }

    public void dropCollection(Class<?> cls) {
        getWriterForClass(cls).dropCollection(cls, getMapper().getCollectionName(cls), null);
    }

    @SuppressWarnings("unused")
    public <T> void ensureIndex(Class<T> cls, Map<String, Object> index, AsyncOperationCallback<T> callback) {
        ensureIndex(cls, getMapper().getCollectionName(cls), index, callback);
    }

    @SuppressWarnings("unused")
    public <T> void ensureIndex(Class<T> cls, String collection, Map<String, Object> index, Map<String, Object> options, AsyncOperationCallback<T> callback) {
        getWriterForClass(cls).ensureIndex(cls, collection, index, options, callback);
    }

    public <T> void ensureIndex(Class<T> cls, String collection, Map<String, Object> index, AsyncOperationCallback<T> callback) {
        getWriterForClass(cls).ensureIndex(cls, collection, index, null, callback);
    }

    @SuppressWarnings("unused")
    public int writeBufferCount() {
        return config.getWriter().writeBufferCount() + config.getBufferedWriter().writeBufferCount();
    }

    @SuppressWarnings("unused")
    public <T> void store(List<T> lst, String collectionName, AsyncOperationCallback<T> callback) {
        if (lst == null || lst.isEmpty()) {
            return;
        }
        getWriterForClass(lst.get(0).getClass()).store(lst, collectionName, callback);
    }


    public void ensureIndex(Class<?> cls, Map<String, Object> index) {
        getWriterForClass(cls).ensureIndex(cls, getMapper().getCollectionName(cls), index, null, null);
    }

    @SuppressWarnings("unused")
    public void ensureIndex(Class<?> cls, String collection, Map<String, Object> index, Map<String, Object> options) {
        getWriterForClass(cls).ensureIndex(cls, collection, index, options, null);
    }

    @SuppressWarnings("unused")
    public void ensureIndex(Class<?> cls, String collection, Map<String, Object> index) {
        getWriterForClass(cls).ensureIndex(cls, collection, index, null, null);
    }

    /**
     * ensureIndex(CachedObject.class,"counter","-value");
     * ensureIndex(CachedObject.class,"counter:2d","-value);
     * Similar to sorting
     *
     * @param cls    - class
     * @param fldStr - fields
     */
    public <T> void ensureIndex(Class<T> cls, AsyncOperationCallback<T> callback, Enum... fldStr) {
        ensureIndex(cls, getMapper().getCollectionName(cls), callback, fldStr);
    }

    public <T> void ensureIndex(Class<T> cls, String collection, AsyncOperationCallback<T> callback, Enum... fldStr) {
        Map<String, Object> m = new LinkedHashMap<>();
        for (Enum e : fldStr) {
            String f = e.name();
            m.put(f, 1);
        }
        getWriterForClass(cls).ensureIndex(cls, collection, m, null, callback);
    }

    public <T> void ensureIndex(Class<T> cls, AsyncOperationCallback<T> callback, String... fldStr) {
        ensureIndex(cls, getMapper().getCollectionName(cls), callback, fldStr);
    }

    public <T> void ensureIndex(Class<T> cls, String collection, AsyncOperationCallback<T> callback, String... fldStr) {
        List<Map<String, Object>> m = createIndexMapFrom(fldStr);
        for (Map<String, Object> idx : m) {
            getWriterForClass(cls).ensureIndex(cls, collection, idx, null, callback);
        }
    }

    public List<Map<String, Object>> createIndexMapFrom(String[] fldStr) {
        if (fldStr.length == 0) {
            return null;
        }
        List<Map<String, Object>> lst = new ArrayList<>();


        for (String f : fldStr) {
            Map<String, Object> m = new LinkedHashMap<>();
            for (String idx : f.split(",")) {
                if (idx.contains(":")) {
                    String i[] = idx.split(":");
                    String value = i[1].replaceAll(" ", "");
                    String key = i[0].replaceAll(" ", "");
                    if (value.matches("^['\"].*['\"]$") || value.equals("2d")) {
                        m.put(key, value);
                    } else {
                        try {
                            int v = Integer.parseInt(value);
                            m.put(key, v);
                        } catch (NumberFormatException e) {
                            try {
                                long l = Long.parseLong(value);
                                m.put(key, l);
                            } catch (NumberFormatException ex) {
                                try {
                                    double d = Double.parseDouble(value);
                                    m.put(key, d);
                                } catch (NumberFormatException e1) {
                                    m.put(key, value);
                                }
                            }
                        }
                    }

                } else {
                    idx = idx.replaceAll(" ", "");
                    if (idx.startsWith("-")) {
                        m.put(idx.substring(1), -1);
                    } else {
                        idx = idx.replaceAll("^\\+", "").replaceAll(" ", "");
                        m.put(idx, 1);
                    }
                }
            }
            lst.add(m);
        }
        return lst;
    }


    @SuppressWarnings("unused")
    public void ensureIndex(Class<?> cls, String... fldStr) {
        ensureIndex(cls, null, fldStr);
    }


    @SuppressWarnings("unused")
    public void ensureIndex(Class<?> cls, Enum... fldStr) {
        ensureIndex(cls, null, fldStr);
    }


    /**
     * Stores a single Object. Clears the corresponding cache
     *
     * @param o - Object to store
     */
    public <T> void store(T o) {
        if (o instanceof List) {
            storeList((List) o);
        } else if (o instanceof Collection) {
            //noinspection unchecked,unchecked
            storeList(new ArrayList<>((Collection) o));
        }
        store(o, null);
    }

    public <T> void store(T o, final AsyncOperationCallback<T> callback) {
        if (o instanceof List) {
            //noinspection unchecked
            storeList((List) o, callback);
        } else if (o instanceof Collection) {
            //noinspection unchecked,unchecked
            storeList(new ArrayList<>((Collection) o), callback);
        }
        store(o, getMapper().getCollectionName(o.getClass()), callback);
    }

    public <T> void store(T o, String collection, final AsyncOperationCallback<T> callback) {
        if (o instanceof List) {
            //noinspection unchecked
            storeList((List) o, collection, callback);
        } else if (o instanceof Collection) {
            //noinspection unchecked,unchecked
            storeList(new ArrayList<>((Collection) o), collection, callback);
        }

        getWriterForClass(o.getClass()).store(o, collection, callback);
    }

    public <T> void store(List<T> lst, AsyncOperationCallback<T> callback) {
        storeList(lst, callback);
    }


    /**
     * stores all elements of this list to the given collection
     *
     * @param lst        - list of objects to store
     * @param collection - collection name to use
     * @param <T>        - type of entity
     */
    public <T> void storeList(List<T> lst, String collection) {
        storeList(lst, collection, null);
    }

    public <T> void storeList(List<T> lst, String collection, AsyncOperationCallback<T> callback) {
        Map<Class<?>, MorphiumWriter> writers = new HashMap<>();
        Map<Class<?>, List<Object>> values = new HashMap<>();
        for (Object o : lst) {
            writers.putIfAbsent(o.getClass(), getWriterForClass(o.getClass()));
            values.putIfAbsent(o.getClass(), new ArrayList<>());
            values.get(o.getClass()).add(o);
        }
        for (Class cls : writers.keySet()) {
            try {
                //noinspection unchecked
                writers.get(cls).store((List<T>) values.get(cls), collection, callback);
            } catch (Exception e) {
                logger.error("Write failed for " + cls.getName() + " lst of size " + values.get(cls).size(), e);
            }
        }
    }


    /**
     * sorts elements in this list, whether to store in background or directly.
     *
     * @param lst - all objects are sorted whether to store in BG or direclty. All objects are stored in their corresponding collection
     * @param <T>
     */
    public <T> void storeList(List<T> lst) {
        storeList(lst, (AsyncOperationCallback<T>) null);
    }

    public <T> void storeList(List<T> lst, final AsyncOperationCallback<T> callback) {
        //have to sort list - might have different objects
        List<T> storeDirect = new ArrayList<>();
        final List<T> storeInBg = new ArrayList<>();

        //checking permission - might take some time ;-(
        for (T o : lst) {
            if (annotationHelper.isBufferedWrite(getARHelper().getRealClass(o.getClass()))) {
                storeInBg.add(o);
            } else {
                storeDirect.add(o);
            }
        }
        config.getBufferedWriter().store(storeInBg, callback);
        config.getWriter().store(storeDirect, callback);
    }


    public <T> void delete(Query<T> o) {
        getWriterForClass(o.getType()).remove(o, null);
    }

    public <T> void delete(Query<T> o, final AsyncOperationCallback<T> callback) {
        getWriterForClass(o.getType()).remove(o, callback);
    }

    @SuppressWarnings("unused")
    public <T> void pushPull(boolean push, Query<T> query, String field, Object value, boolean upsert, boolean multiple, AsyncOperationCallback<T> callback) {
        getWriterForClass(query.getType()).pushPull(push, query, field, value, upsert, multiple, callback);
    }

    @SuppressWarnings("unused")
    public <T> void pushPullAll(boolean push, Query<T> query, String field, List<?> value, boolean upsert, boolean multiple, AsyncOperationCallback<T> callback) {
        getWriterForClass(query.getType()).pushPullAll(push, query, field, value, upsert, multiple, callback);
    }

    @SuppressWarnings("unused")
    public <T> void pullAll(Query<T> query, String field, List<?> value, boolean upsert, boolean multiple, AsyncOperationCallback<T> callback) {
        getWriterForClass(query.getType()).pushPullAll(false, query, field, value, upsert, multiple, callback);
    }

    /**
     * deletes a single object from morphium backend. Clears cache
     *
     * @param o - entity
     */
    public void delete(Object o) {
        delete(o, getMapper().getCollectionName(o.getClass()));
    }

    public void delete(Object o, String collection) {
        getWriterForClass(o.getClass()).remove(o, collection, null);
    }


    public <T> void delete(final T lo, final AsyncOperationCallback<T> callback) {
        if (lo instanceof Query) {
            //noinspection unchecked
            delete((Query) lo, callback);
            return;
        }
        getWriterForClass(lo.getClass()).remove(lo, getMapper().getCollectionName(lo.getClass()), callback);
    }

    @SuppressWarnings("unused")
    public <T> void delete(final T lo, String collection, final AsyncOperationCallback<T> callback) {
        getWriterForClass(lo.getClass()).remove(lo, collection, callback);
    }


    //////////////////////////////////////////////////////////////////////
    //////////////////////////////////////////////////
    //////////////////////////////
    /////////////// Statistics
    /////////
    /////
    ///
    public Map<String, Double> getStatistics() {
        return new Statistics(this);
    }

    public void resetStatistics() {
        Map<StatisticKeys, StatisticValue> s = new HashMap<>();

        for (StatisticKeys k : StatisticKeys.values()) {
            s.put(k, new StatisticValue());
        }
        stats = s;
    }


    public Map<StatisticKeys, StatisticValue> getStats() {
        return stats;
    }


    public void addShutdownListener(ShutdownListener l) {
        shutDownListeners.add(l);
    }

    @SuppressWarnings("unused")
    public void removeShutdownListener(ShutdownListener l) {
        shutDownListeners.remove(l);
    }

    public void close() {
        asyncOperationsThreadPool.shutdownNow();

        for (ShutdownListener l : shutDownListeners) {
            l.onShutdown(this);
        }
        try {
            Thread.sleep(1000); //give it time to end ;-)
        } catch (Exception e) {
            logger.debug("Ignoring interrupted-exception");
        }
        if (rsMonitor != null) {
            rsMonitor.terminate();
        }

        config.getAsyncWriter().close();
        config.getBufferedWriter().close();
        config.getWriter().close();
        try {
            getDriver().close();
        } catch (MorphiumDriverException e) {
            e.printStackTrace();
        }
        config = null;
        //        MorphiumSingleton.reset();
    }

    @SuppressWarnings("unused")
    public String createCamelCase(String n) {
        return annotationHelper.createCamelCase(n, false);
    }


    /////////////////
    //// AGGREGATOR Support
    ///

    public <T, R> Aggregator<T, R> createAggregator(Class<? extends T> type, Class<? extends R> resultType) {
        Aggregator<T, R> aggregator = config.getAggregatorFactory().createAggregator(type, resultType);
        aggregator.setMorphium(this);
        return aggregator;
    }

    public <T, R> List<R> aggregate(Aggregator<T, R> a) {

        //        DBCollection coll = null;
        //        for (int i = 0; i < getConfig().getRetriesOnNetworkError(); i++) {
        //            try {
        //                String collectionName = a.getCollectionName();
        //                if (collectionName == null) collectionName = objectMapper.getCollectionName(a.getSearchType());
        //                coll = config.getDb().getCollection(collectionName);
        //                break;
        //            } catch (Throwable e) {
        //                handleNetworkError(i, e);
        //            }
        //        }
        List<Map<String, Object>> agList = a.toAggregationList();
        try {
            List<Map<String, Object>> ret = getDriver().aggregate(config.getDatabase(), a.getCollectionName(), agList, a.isExplain(), a.isUseDisk(), getReadPreferenceForClass(a.getSearchType()));
            List<R> result = new ArrayList<>();
            for (Map<String, Object> dbObj : ret) {
                result.add(getMapper().unmarshall(a.getResultType(), dbObj));
            }
            return result;
        } catch (MorphiumDriverException e) {
            throw new RuntimeException(e);
        }
        //        Map<String, Object> first = agList.get(0);
        //        agList.remove(0);
        //        AggregationOutput resp = null;
        //        for (int i = 0; i < getConfig().getRetriesOnNetworkError(); i++) {
        //            try {
        //                resp = coll.aggregate(first, agList.toArray(new Map<String, Object>[agList.size()]));
        //                break;
        //            } catch (Throwable t) {
        //                handleNetworkError(i, t);
        //            }
        //        }
        //        List<R> ret = new ArrayList<>();
        //        if (resp != null) {
        //            for (Map<String, Object> o : resp.results()) {
        //                R obj = getMapper().unmarshall(a.getResultType(), o);
        //                if (obj == null) continue;
        //                ret.add(obj);
        //            }
        //        }
        //        return ret;
    }

    /**
     * create a proxy object, implementing the ParitallyUpdateable Interface
     * these objects will be updated in mongo by only changing altered fields
     * <b>Attention:</b> the field name if determined by the setter name for now. That means, it does not honor the @Property-Annotation!!!
     * To make sure, you take the correct field - use the UpdatingField-Annotation for the setters!
     *
     * @param o   - entity
     * @param <T> - type
     * @return Type
     */
    @SuppressWarnings("unchecked")
    public <T> T createPartiallyUpdateableEntity(T o) {
        return (T) Enhancer.create(o.getClass(), new Class[]{PartiallyUpdateable.class, Serializable.class}, new PartiallyUpdateableProxy(this, o));
    }

    @SuppressWarnings("unchecked")
    public <T> T createLazyLoadedEntity(Class<? extends T> cls, Object id, Object container, String fieldName, String collectionName) {
        return (T) Enhancer.create(cls, new Class[]{Serializable.class}, new LazyDeReferencingProxy(this, cls, id, container, fieldName, collectionName));
    }

    @SuppressWarnings("unchecked")
    public <T> MongoField<T> createMongoField() {
        if (config == null) {
            return new MongoFieldImpl<>();
        }
        try {
            return (MongoField<T>) config.getFieldImplClass().newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }


    public int getWriteBufferCount() {
        return config.getBufferedWriter().writeBufferCount() + config.getWriter().writeBufferCount() + config.getAsyncWriter().writeBufferCount();
    }

    public int getBufferedWriterBufferCount() {
        return config.getBufferedWriter().writeBufferCount();
    }

    @SuppressWarnings("unused")
    public int getAsyncWriterBufferCount() {
        return config.getAsyncWriter().writeBufferCount();
    }

    public int getWriterBufferCount() {
        return config.getWriter().writeBufferCount();
    }

    @SuppressWarnings("unused")
    public void disableAutoValuesForThread() {
        enableAutoValues.set(false);
    }

    @SuppressWarnings("unused")
    public void enableAutoValuesForThread() {
        enableAutoValues.set(true);
    }

    public boolean isAutoValuesEnabledForThread() {
        return ((enableAutoValues.get() == null || enableAutoValues.get()) && config.isAutoValuesEnabled());
    }

    @SuppressWarnings("unused")
    public void disableReadCacheForThread() {
        enableReadCache.set(false);
    }

    @SuppressWarnings("unused")
    public void enableReadCacheForThread() {
        enableReadCache.set(true);
    }

    public boolean isReadCacheEnabledForThread() {
        return (enableReadCache.get() == null || enableReadCache.get()) && config.isReadCacheEnabled();
    }


    @SuppressWarnings("unused")
    public void disableWriteBufferForThread() {
        disableWriteBuffer.set(false);
    }

    @SuppressWarnings("unused")
    public void enableWriteBufferForThread() {
        disableWriteBuffer.set(true);
    }

    public boolean isWriteBufferEnabledForThread() {
        return (disableWriteBuffer.get() == null || disableWriteBuffer.get()) && config.isBufferedWritesEnabled();
    }


    @SuppressWarnings("unused")
    public void disableAsyncWritesForThread() {
        disableAsyncWrites.set(false);
    }

    @SuppressWarnings("unused")
    public void enableAsyncWritesForThread() {
        disableAsyncWrites.set(true);
    }

    public boolean isAsyncWritesEnabledForThread() {
        return (disableAsyncWrites.get() == null || disableAsyncWrites.get()) && config.isAsyncWritesEnabled();
    }


    public void queueTask(Runnable runnable) {
        boolean queued = false;
        do {
            try {
                asyncOperationsThreadPool.execute(runnable);
                queued = true;
            } catch (Exception e) {
                try {
                    Thread.sleep(100); //wait a moment, reduce load
                } catch (InterruptedException ignored) {
                }
            }
        } while (!queued);
    }

    public int getNumberOfAvailableThreads() {
        return asyncOperationsThreadPool.getMaximumPoolSize() - asyncOperationsThreadPool.getActiveCount();
    }

    public int getActiveThreads() {
        return asyncOperationsThreadPool.getActiveCount();
    }

    public <T, E, I> void fireWouldDereference(E container, String fieldname, I id, Class<? extends T> cls, boolean lazy) {
        for (DereferencingListener l : this.lazyDereferencingListeners) {
            //noinspection unchecked
            l.wouldDereference(container, fieldname, id, cls, lazy);
        }
    }

    public <T> void fireDidDereference(Object container, String fieldname, T deReferenced, boolean lazy) {
        for (DereferencingListener l : this.lazyDereferencingListeners) {
            //noinspection unchecked
            l.didDereference(container, fieldname, deReferenced, lazy);
        }
    }

    public void addDereferencingListener(DereferencingListener lst) {
        this.lazyDereferencingListeners.add(lst);
    }

    public void removeDerrferencingListener(DereferencingListener lst) {
        this.lazyDereferencingListeners.remove(lst);
    }
}
