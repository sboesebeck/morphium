/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.caluga.morphium;

import com.mongodb.event.ClusterListener;
import com.mongodb.event.CommandListener;
import com.mongodb.event.ConnectionPoolListener;
import de.caluga.morphium.aggregation.Aggregator;
import de.caluga.morphium.aggregation.Expr;
import de.caluga.morphium.annotations.*;
import de.caluga.morphium.annotations.caching.Cache;
import de.caluga.morphium.annotations.lifecycle.*;
import de.caluga.morphium.async.AsyncOperationCallback;
import de.caluga.morphium.async.AsyncOperationType;
import de.caluga.morphium.bulk.MorphiumBulkContext;
import de.caluga.morphium.cache.MorphiumCache;
import de.caluga.morphium.cache.MorphiumCacheImpl;
import de.caluga.morphium.changestream.ChangeStreamEvent;
import de.caluga.morphium.changestream.ChangeStreamListener;
import de.caluga.morphium.driver.*;
import de.caluga.morphium.encryption.EncryptionKeyProvider;
import de.caluga.morphium.query.MongoField;
import de.caluga.morphium.query.MongoFieldImpl;
import de.caluga.morphium.query.Query;
import de.caluga.morphium.replicaset.RSMonitor;
import de.caluga.morphium.replicaset.ReplicaSetNode;
import de.caluga.morphium.replicaset.ReplicaSetStatus;
import de.caluga.morphium.replicaset.ReplicasetStatusListener;
import de.caluga.morphium.validation.JavaxValidationStorageListener;
import de.caluga.morphium.writer.BufferedMorphiumWriterImpl;
import de.caluga.morphium.writer.MorphiumWriter;
import de.caluga.morphium.writer.MorphiumWriterImpl;
import io.github.classgraph.ClassGraph;
import io.github.classgraph.ClassInfoList;
import io.github.classgraph.ScanResult;
import net.sf.cglib.proxy.Enhancer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.lang.annotation.Annotation;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This is the single access point for accessing MongoDB. This should
 *
 * @author stephan
 */

@SuppressWarnings({"WeakerAccess", "unused"})
public class Morphium implements AutoCloseable {

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
    private static final Logger logger = LoggerFactory.getLogger(Morphium.class);
    private final ThreadLocal<Boolean> enableAutoValues = new ThreadLocal<>();
    private final ThreadLocal<Boolean> enableReadCache = new ThreadLocal<>();
    private final ThreadLocal<Boolean> disableWriteBuffer = new ThreadLocal<>();
    private final ThreadLocal<Boolean> disableAsyncWrites = new ThreadLocal<>();
    private final List<ProfilingListener> profilingListeners;
    private final List<ShutdownListener> shutDownListeners = new CopyOnWriteArrayList<>();
    private MorphiumConfig config;
    private Map<StatisticKeys, StatisticValue> stats = new ConcurrentHashMap<>();
    private List<MorphiumStorageListener> listeners = new CopyOnWriteArrayList<>();
    private AnnotationAndReflectionHelper annotationHelper;
    private MorphiumObjectMapper objectMapper;
    private EncryptionKeyProvider encryptionKeyProvider;
    private RSMonitor rsMonitor;
    private ThreadPoolExecutor asyncOperationsThreadPool;
    private MorphiumDriver morphiumDriver;

    private JavaxValidationStorageListener lst;

    public Morphium() {
        profilingListeners = new CopyOnWriteArrayList<>();

    }

    public Morphium(String host, String db) {
        this();
        MorphiumConfig cfg = new MorphiumConfig(db, 100, 5000, 5000);
        cfg.addHostToSeed(host);
        //cfg.setReplicasetMonitoring(false);
        setConfig(cfg);

    }

    public Morphium(String host, int port, String db) {
        this();
        MorphiumConfig cfg = new MorphiumConfig(db, 100, 5000, 5000);
        //cfg.setReplicasetMonitoring(false);
        cfg.addHostToSeed(host, port);

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

        BlockingQueue<Runnable> queue = new LinkedBlockingQueue<Runnable>() {
            private static final long serialVersionUID = -6903933921423432194L;

            @Override
            public boolean offer(Runnable e) {
                int poolSize = asyncOperationsThreadPool.getPoolSize();
                int maximumPoolSize = asyncOperationsThreadPool.getMaximumPoolSize();
                if (poolSize >= maximumPoolSize || poolSize > asyncOperationsThreadPool.getActiveCount()) {
                    return super.offer(e);
                } else {
                    return false;
                }
            }
        };
        asyncOperationsThreadPool = new ThreadPoolExecutor(getConfig().getThreadPoolAsyncOpCoreSize(), getConfig().getThreadPoolAsyncOpMaxSize(),
                getConfig().getThreadPoolAsyncOpKeepAliveTime(), TimeUnit.MILLISECONDS,
                queue);
        asyncOperationsThreadPool.setRejectedExecutionHandler((r, executor) -> {
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
                    setName("shutdown_hook");
                    try {
                        close();
                    } catch (Exception e) {
                        //swallow
                    }
                }
            });


            try {
                morphiumDriver = (MorphiumDriver) Class.forName(config.getDriverClass()).getDeclaredConstructor().newInstance();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            morphiumDriver.setConnectionTimeout(config.getConnectionTimeout());
            morphiumDriver.setMaxConnections(config.getMaxConnections());
            morphiumDriver.setMinConnections(config.getMinConnectionsHost());
            morphiumDriver.setReadTimeout(config.getReadTimeout());
            morphiumDriver.setRetryReads(config.isRetryReads());
            morphiumDriver.setRetryWrites(config.isRetryWrites());
            morphiumDriver.setHeartbeatFrequency(config.getHeartbeatFrequency());
            morphiumDriver.setMaxConnectionIdleTime(config.getMaxConnectionIdleTime());
            morphiumDriver.setMaxConnectionLifetime(config.getMaxConnectionLifeTime());
            morphiumDriver.setMaxWaitTime(config.getMaxWaitTime());
            morphiumDriver.setServerSelectionTimeout(config.getServerSelectionTimeout());
            morphiumDriver.setUuidRepresentation(config.getUuidRepresentation());

            morphiumDriver.setUseSSL(config.isUseSSL());
            morphiumDriver.setSslContext(config.getSslContext());
            morphiumDriver.setSslInvalidHostNameAllowed(config.isSslInvalidHostNameAllowed());

            if (config.getHostSeed().isEmpty()) {
                throw new RuntimeException("Error - no server address specified!");
            }

            if (config.getMongoLogin() != null && config.getMongoPassword() != null) {
                morphiumDriver.setCredentials(config.getDatabase(), config.getMongoLogin(), config.getMongoPassword().toCharArray());
            }
            if (config.getMongoAdminUser() != null && config.getMongoAdminPwd() != null) {
                morphiumDriver.setCredentials("admin", config.getMongoAdminUser(), config.getMongoAdminPwd().toCharArray());
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
            //Defaulting to standard Cache impl
            config.setCache(new MorphiumCacheImpl());
        }
        config.getCache().setAnnotationAndReflectionHelper(getARHelper());
        config.getCache().setGlobalCacheTimeout(config.getGlobalCacheValidTime());
        config.getCache().setHouskeepingIntervalPause(config.getHousekeepingTimeout());

        //        if (getConfig().isOplogMonitorEnabled()) {
        //            oplogMonitor = new OplogMonitor(this);
        //            oplogMonitorThread = new Thread(oplogMonitor);
        //            oplogMonitorThread.setDaemon(true);
        //            oplogMonitorThread.setName("oplogmonitor");
        //            oplogMonitorThread.start();
        //        }

        setValidationSupport();
        try {
            objectMapper = config.getOmClass().getDeclaredConstructor().newInstance();
            objectMapper.setMorphium(this);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        try {
            encryptionKeyProvider = config.getEncryptionKeyProviderClass().getDeclaredConstructor().newInstance();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        if (isReplicaSet()) {
            rsMonitor = new RSMonitor(this);
            rsMonitor.start();
            rsMonitor.getReplicaSetStatus(false);
        }
        if (!config.getIndexCappedCheck().equals(MorphiumConfig.IndexCappedCheck.NO_CHECK) &&
                config.getIndexCappedCheck().equals(MorphiumConfig.IndexCappedCheck.CREATE_ON_STARTUP)) {
            Map<Class<?>, List<Map<String, Object>>> missing = checkIndices(classInfo -> !classInfo.getPackageName().startsWith("de.caluga.morphium"));
            if (missing != null && !missing.isEmpty()) {
                for (Class cls : missing.keySet()) {
                    if (missing.get(cls).size() != 0) {
                        if (config.getIndexCappedCheck().equals(MorphiumConfig.IndexCappedCheck.WARN_ON_STARTUP)) {
                            logger.warn("Missing indices for entity " + cls.getName() + ": " + missing.get(cls).size());
                            if (cappedMissing(missing.get(cls))) {
                                logger.warn("No capped settings missing for " + cls.getName());
                            }
                        } else if (config.getIndexCappedCheck().equals(MorphiumConfig.IndexCappedCheck.CREATE_ON_STARTUP)) {
                            logger.warn("Creating missing indices for entity " + cls.getName());
                            ensureIndicesFor(cls);
                            if (cappedMissing(missing.get(cls))) {
                                logger.warn("applying capped settings for entity " + cls.getName());
                                ensureCapped(cls);
                            }
                        }
                    }
                }
            }
        }

        //logger.info("Initialization successful...");
    }

    private boolean cappedMissing(List<Map<String, Object>> lst) {
        for (Map<String, Object> idx : lst) {
            if (idx.containsKey("__capped_size")) {
                return true;
            }
        }
        return false;

    }


    public EncryptionKeyProvider getEncryptionKeyProvider() {
        return encryptionKeyProvider;
    }

    public MorphiumCache getCache() {
        return config.getCache();
    }

    /**
     * Checks if javax.validation is available and enables validation support.
     *
     * @return true, if validation is supported
     */


    public boolean isValidationEnabled() {
        return lst != null;
    }

    public void disableValidation() {
        if (lst == null) return;
        listeners.remove(lst);
        lst = null;
    }

    public void enableValidation() {
        setValidationSupport();
        if (lst == null) {
            throw new RuntimeException("Validation not possible - javax.validation implementation not in Classpath?");
        }
    }

    @SuppressWarnings("UnusedDeclaration")
    private void setValidationSupport() {
        if (lst != null) {
            if (!listeners.contains(lst)) {
                listeners.add(lst);
            }
            return;
        }
        try {

            getClass().getClassLoader().loadClass("javax.validation.ValidatorFactory");
            lst = new JavaxValidationStorageListener();
            addListener(lst);
            logger.debug("Adding javax.validation Support...");
        } catch (Exception cnf) {
            logger.debug("Validation disabled!");

        }
    }

    public List<String> listDatabases() {
        try {
            return getDriver().listDatabases();
        } catch (MorphiumDriverException e) {
            throw new RuntimeException("Could not list databases", e);
        }
    }


    public List<String> listCollections() {
        return listCollections(null);
    }

    public List<String> listCollections(String pattern) {
        try {
            return getDriver().listCollections(getConfig().getDatabase(), pattern);
        } catch (MorphiumDriverException e) {
            throw new RuntimeException(e);
        }
    }

    public void reconnectToDb(String db) {
        Properties prop = getConfig().asProperties();
        close();
        MorphiumConfig cfg = new MorphiumConfig(prop);
        //cfg.setDatabase(db);
        setConfig(cfg);
    }

    public void addListener(MorphiumStorageListener lst) {
        List<MorphiumStorageListener> newList = new ArrayList<>(listeners);
        newList.add(lst);
        listeners = newList;
    }

    public void removeListener(MorphiumStorageListener lst) {
        List<MorphiumStorageListener> newList = new ArrayList<>(listeners);
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


    public <T> Query<T> createQueryByTemplate(T template, Enum... fields) {
        String[] flds = new String[fields.length];
        for (int i = 0; i < fields.length; i++) {
            flds[i] = fields[i].name();
        }
        return createQueryByTemplate(template, flds);
    }

    public <T> Query<T> createQueryByTemplate(T template, String... fields) {
        Class cls = template.getClass();
        List<String> flds;
        if (fields.length > 0) {
            flds = new ArrayList<>(Arrays.asList(fields));
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
        return q;
    }

    public <T> List<T> findByTemplate(T template, Enum... fields) {
        return createQueryByTemplate(template, fields).asList();
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

        return createQueryByTemplate(template, fields).asList();
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
        ensureIndicesFor(type, onCollection, callback, getWriterForClass(type));
    }

    public void addReplicasetStatusListener(ReplicasetStatusListener lst) {
        if (rsMonitor != null) {
            rsMonitor.addListener(lst);
        }
    }

    public void removeReplicasetStatusListener(ReplicasetStatusListener lst) {
        if (rsMonitor != null) {
            rsMonitor.removeListener(lst);
        }
    }

    public <T> void ensureIndicesFor(Class<T> type, String onCollection, AsyncOperationCallback<T> callback, MorphiumWriter wr) {
        if (annotationHelper.isAnnotationPresentInHierarchy(type, Index.class)) {
            // List<Annotation> collations = annotationHelper.getAllAnnotationsFromHierachy(type, de.caluga.morphium.annotations.Collation.class);

            @SuppressWarnings("unchecked") List<Annotation> lst = annotationHelper.getAllAnnotationsFromHierachy(type, Index.class);
            for (Annotation a : lst) {
                Index i = (Index) a;
                if (i.value().length > 0) {
                    List<Map<String, Object>> options = null;

                    if (i.options().length > 0) {
                        //options set
                        options = createIndexMapFrom(i.options());
                    }
                    if (!i.locale().equals("")) {
                        Map<String, Object> collation = Utils.getMap("locale", i.locale());
                        collation.put("alternate", i.alternate().mongoText);
                        collation.put("backwards", i.backwards());
                        collation.put("caseFirst", i.caseFirst().mongoText);
                        collation.put("caseLevel", i.caseLevel());
                        collation.put("maxVariable", i.maxVariable().mongoText);
                        collation.put("strength", i.strength().mongoValue);
                        options.add(Utils.getMap("collation", collation));
                    }
                    List<Map<String, Object>> idx = createIndexMapFrom(i.value());
                    int cnt = 0;
                    for (Map<String, Object> m : idx) {
                        Map<String, Object> optionsMap = null;
                        if (options != null && options.size() > cnt) {
                            optionsMap = options.get(cnt);
                        }
                        wr.ensureIndex(type, onCollection, m, optionsMap, callback);
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
                wr.ensureIndex(type, onCollection, idx, optionsMap, callback);
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
                    //cmd.put("autoIndexId", (annotationHelper.getIdField(c).getType().equals(MorphiumId.class)));
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
            // noinspection SuspiciousMethodCalls
            toSet.put(est.getKey().name(), values.get(est.getValue()));
        }
        set(query, toSet, upsert, multiple);
    }

    public void push(final Query<?> query, final Enum field, final Object value) {
        push(query, field, value, false, true);
    }

    @SuppressWarnings({"UnusedDeclaration"})
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

    @SuppressWarnings({"UnusedDeclaration"})
    public void pull(Query<?> query, Enum field, Object value, boolean upsert, boolean multiple) {
        pull(query, field.name(), value, upsert, multiple);
    }

    @SuppressWarnings({"UnusedDeclaration"})
    public void pushAll(Query<?> query, Enum field, List<Object> value, boolean upsert, boolean multiple) {
        pushAll(query, field.name(), value, upsert, multiple);
    }

    public void push(Object entity, String field, Object value, boolean upsert) {
        push(entity, null, field, value, upsert);
    }

    public void push(Object entity, String collection, String field, Object value, boolean upsert) {
        Object id = getId(entity);
        if (!upsert || id != null) {
            Query<?> id1 = createQueryFor(entity.getClass()).f("_id").eq(id);
            if (collection != null) {
                id1.setCollectionName(collection);
            }
            push(id1, field, value, upsert, false);
        }
        Field fld = getARHelper().getField(entity.getClass(), field);
        try {
            if (fld.getType().isArray()) {
                //array handling
                Object a = fld.get(entity);
                Object arr = null;
                if (a == null) {
                    arr = Array.newInstance(((Class) fld.getGenericType()).getComponentType(), 1);
                    Array.set(arr, 0, value);
                } else {
                    arr = Array.newInstance(((Class) fld.getGenericType()).getComponentType(), Array.getLength(a) + 1);
                    for (int i = 0; i < Array.getLength(a); i++) {
                        Array.set(arr, i, Array.get(a, i));
                    }
                    Array.set(arr, Array.getLength(a), value);
                }
                fld.set(entity, arr);
            } else if (Collection.class.isAssignableFrom(fld.getType())) {
                //collection / List etc.
                Collection v = null;

                v = (Collection) fld.get(entity);

                if (v == null) {
                    v = new ArrayList();
                    v.add(value);
                    fld.set(entity, v);
                } else {
                    v.add(value);
                }


            }
        } catch (Exception e) {
            throw new RuntimeException("Could not update entity", e);
        }
        if (upsert && id == null) {
            store(entity);
        }
    }

    public void push(Object entity, String collection, Enum field, Object value, boolean upsert) {
        push(entity, collection, field.name(), value, upsert);
    }

    public void push(Object entity, Enum field, Object value, boolean upsert) {
        push(entity, field.name(), value, upsert);
    }

    @SuppressWarnings({"UnusedDeclaration"})
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

    public <T> void pull(final T entity, final String field, final Expr value, final boolean upsert, final boolean multiple, final AsyncOperationCallback<T> callback) {
        if (entity == null) throw new IllegalArgumentException("Null Entity cannot be pulled...");
        pull((Query<T>) createQueryFor(entity.getClass()).f("_id").eq(getId(entity)), field, value, upsert, multiple, callback);
    }

    public <T> void pull(final Query<T> query, final String field, final Expr value, final boolean upsert, final boolean multiple, final AsyncOperationCallback<T> callback) {
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

    ////////
    /////
    /// SET with Query
    //
    //

    /**
     * will change an entry in mongodb-collection corresponding to given class object
     * if query is too complex, upsert might not work!
     * Upsert should consist of single anueries, which will be used to generate the object to create, unless
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
        set(query, field.name(), val, upsert, multiple, null);
    }

    @SuppressWarnings("unused")
    public <T> void set(Query<T> query, Enum field, Object val, boolean upsert, boolean multiple, AsyncOperationCallback<T> callback) {
        set(query, field.name(), val, upsert, multiple, callback);
    }


    @SuppressWarnings({"UnusedDeclaration"})
    public void pullAll(Query<?> query, String field, List<Object> value, boolean upsert, boolean multiple) {
        pull(query, field, value, upsert, multiple);
    }

    public <T> void set(Query<T> query, String field, Object val, boolean upsert, boolean multiple) {
        set(query, field, val, upsert, multiple, null);
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

    /**
     * set current date into one field
     *
     * @param query
     * @param field
     * @param upsert
     * @param multiple
     * @param <T>
     */
    public <T> void currentDate(final Query<?> query, String field, boolean upsert, boolean multiple) {
        set(query, Utils.getMap("$currentDate", Utils.getMap(field, 1)), upsert, multiple);
    }

    public <T> void currentDate(final Query<?> query, Enum field, boolean upsert, boolean multiple) {
        set(query, Utils.getMap("$currentDate", Utils.getMap(field.name(), 1)), upsert, multiple);
    }

    ////////
    /////
    //
    // SET with object
    //
    @SuppressWarnings("unused")
    public <T> void set(T toSet, Enum field, Object value, AsyncOperationCallback<T> callback) {
        set(toSet, field.name(), value, callback);
    }

    public <T> void set(T toSet, Enum field, Object value) {
        set(toSet, field.name(), value, null);
    }

    @SuppressWarnings("unused")
    public <T> void set(T toSet, String collection, Enum field, Object value) {
        set(toSet, collection, field.name(), value, false, null);
    }

    public <T> void set(final T toSet, final Enum field, final Object value, boolean upserts, AsyncOperationCallback<T> callback) {
        set(toSet, field.name(), value, upserts, callback);
    }

    public <T> void set(final T toSet, final Map<Enum, Object> values) {
        set(toSet, getMapper().getCollectionName(toSet.getClass()), false, values, null);
    }

    public <T> void set(String inCollection, final Map<Enum, Object> values, final T into) {
        set(into, inCollection, false, values, null);
    }

    public <T> void set(final T toSet, String collection, boolean upserts, final Map<Enum, Object> values) {
        set(toSet, collection, upserts, values, null);

    }

    public <T> void set(final T toSet, String collection, boolean upserts, final Map<Enum, Object> values, AsyncOperationCallback<T> callback) {
        Map<String, Object> strValues = new HashMap<>();
        for (Map.Entry<Enum, Object> e : values.entrySet()) {
            strValues.put(e.getKey().name(), e.getValue());
        }
        set(toSet, collection, strValues, upserts, callback);
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
    public <T> void set(final T toSet, final String field, final Object value) {
        set(toSet, field, value, null);
    }

    public <T> void set(final T toSet, final String field, final Object value, boolean upserts, AsyncOperationCallback<T> callback) {
        set(toSet, getMapper().getCollectionName(toSet.getClass()), field, value, upserts, callback);
    }

    public <T> void set(final Map<String, Object> values, final T into) {
        set(into, getMapper().getCollectionName(into.getClass()), values, false, null);
    }

    public <T> void set(final T toSet, String collection, final Map<String, Object> values) {
        set(toSet, collection, values, false, null);
    }

    public <T> void set(final T toSet, String collection, final Map<String, Object> values, boolean upserts) {
        set(toSet, collection, values, upserts, null);
    }

    public <T> void set(final T toSet, String collection, final Map<String, Object> values, boolean upserts, AsyncOperationCallback<T> callback) {
        if (toSet == null) {
            throw new RuntimeException("Cannot update null!");
        }

        if (getId(toSet) == null) {
            logger.info("just storing object as it is new...");
            store(toSet);
            return;
        }
        annotationHelper.callLifecycleMethod(PreUpdate.class, toSet);
        getWriterForClass(toSet.getClass()).set(toSet, collection, values, upserts, callback);
        annotationHelper.callLifecycleMethod(PostUpdate.class, toSet);
    }

    public <T> void set(final T toSet, String collection, final String field, final Object value, boolean upserts, AsyncOperationCallback<T> callback) {
        set(toSet, collection, Utils.getMap(field, value), upserts, callback);
    }

    public <T> void set(final T toSet, final String field, final Object value, final AsyncOperationCallback<T> callback) {
        set(toSet, field, value, false, callback);

    }

    ///////////////////////////////
    //////////////////////
    ///////////////
    ////////////
    ////////// DEC and INC Methods
    /////
    //

    @SuppressWarnings({"UnusedDeclaration"})
    public void dec(Query<?> query, Enum field, double amount, boolean upsert, boolean multiple) {
        dec(query, field.name(), -amount, upsert, multiple);
    }

    @SuppressWarnings("unused")
    public void dec(Query<?> query, Enum field, long amount, boolean upsert, boolean multiple) {
        dec(query, field.name(), -amount, upsert, multiple);
    }

    @SuppressWarnings("unused")
    public void dec(Query<?> query, Enum field, Number amount, boolean upsert, boolean multiple) {
        dec(query, field.name(), amount.doubleValue(), upsert, multiple);
    }

    @SuppressWarnings("unused")
    public void dec(Query<?> query, Enum field, int amount, boolean upsert, boolean multiple) {
        dec(query, field.name(), amount, upsert, multiple);
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

    @SuppressWarnings({"UnusedDeclaration"})
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

    @SuppressWarnings({"UnusedDeclaration"})
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

    public <T> void inc(final Map<Enum, Number> fieldsToInc, final Query<T> matching, final boolean upsert, final boolean multiple, AsyncOperationCallback<T> callback) {
        Map<String, Number> toUpdate = new HashMap<>();
        for (Map.Entry<Enum, Number> e : fieldsToInc.entrySet()) {
            toUpdate.put(e.getKey().name(), e.getValue());
        }
        inc(matching, toUpdate, upsert, multiple, callback);
    }

    public <T> void inc(final Query<T> query, final Map<String, Number> toUpdate, final boolean upsert, final boolean multiple, AsyncOperationCallback<T> callback) {
        if (query == null) {
            throw new RuntimeException("Cannot update null!");
        }
        getWriterForClass(query.getType()).inc(query, toUpdate, upsert, multiple, callback);
    }

    public <T> void dec(final Map<Enum, Number> fieldsToInc, final Query<T> matching, final boolean upsert, final boolean multiple, AsyncOperationCallback<T> callback) {
        Map<String, Number> toUpdate = new HashMap<>();
        for (Map.Entry<Enum, Number> e : fieldsToInc.entrySet()) {
            toUpdate.put(e.getKey().name(), e.getValue());
        }
        dec(matching, toUpdate, upsert, multiple, callback);
    }

    public <T> void dec(final Query<T> query, final Map<String, Number> toUpdate, final boolean upsert, final boolean multiple, AsyncOperationCallback<T> callback) {
        if (query == null) {
            throw new RuntimeException("Cannot update null!");
        }
        getWriterForClass(query.getType()).inc(query, toUpdate, upsert, multiple, callback);
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

    public <T> void inc(final Query<T> query, final Enum field, final long amount, final boolean upsert, final boolean multiple, final AsyncOperationCallback<T> callback) {
        inc(query, field.name(), amount, upsert, multiple, callback);
    }

    public <T> void inc(final Query<T> query, final String name, final long amount, final boolean upsert, final boolean multiple, final AsyncOperationCallback<T> callback) {
        if (query == null) {
            throw new RuntimeException("Cannot update null!");
        }
        getWriterForClass(query.getType()).inc(query, name, amount, upsert, multiple, callback);
    }

    public <T> void inc(final Query<T> query, final Enum field, final int amount, final boolean upsert, final boolean multiple, final AsyncOperationCallback<T> callback) {
        inc(query, field.name(), amount, upsert, multiple, callback);
    }

    public <T> void inc(final Query<T> query, final String name, final int amount, final boolean upsert, final boolean multiple, final AsyncOperationCallback<T> callback) {
        if (query == null) {
            throw new RuntimeException("Cannot update null!");
        }
        getWriterForClass(query.getType()).inc(query, name, amount, upsert, multiple, callback);
    }

    public <T> void inc(final Query<T> query, final Enum field, final double amount, final boolean upsert, final boolean multiple, final AsyncOperationCallback<T> callback) {
        inc(query, field.name(), amount, upsert, multiple, callback);
    }

    public <T> void inc(final Query<T> query, final String name, final double amount, final boolean upsert, final boolean multiple, final AsyncOperationCallback<T> callback) {
        if (query == null) {
            throw new RuntimeException("Cannot update null!");
        }
        getWriterForClass(query.getType()).inc(query, name, amount, upsert, multiple, callback);

    }

    public <T> void inc(final Query<T> query, final Enum field, final Number amount, final boolean upsert, final boolean multiple, final AsyncOperationCallback<T> callback) {
        inc(query, field.name(), amount, upsert, multiple, callback);
    }

    public <T> void inc(final Query<T> query, final String name, final Number amount, final boolean upsert, final boolean multiple, final AsyncOperationCallback<T> callback) {
        if (query == null) {
            throw new RuntimeException("Cannot update null!");
        }
        getWriterForClass(query.getType()).inc(query, name, amount, upsert, multiple, callback);

    }

    public <T> void dec(final Query<T> query, final Enum field, final long amount, final boolean upsert, final boolean multiple, final AsyncOperationCallback<T> callback) {
        dec(query, field.name(), amount, upsert, multiple, callback);
    }

    public <T> void dec(final Query<T> query, final String name, final long amount, final boolean upsert, final boolean multiple, final AsyncOperationCallback<T> callback) {
        if (query == null) {
            throw new RuntimeException("Cannot update null!");
        }
        getWriterForClass(query.getType()).inc(query, name, -amount, upsert, multiple, callback);
    }

    public <T> void dec(final Query<T> query, final Enum field, final int amount, final boolean upsert, final boolean multiple, final AsyncOperationCallback<T> callback) {
        dec(query, field.name(), amount, upsert, multiple, callback);
    }

    public <T> void dec(final Query<T> query, final String name, final int amount, final boolean upsert, final boolean multiple, final AsyncOperationCallback<T> callback) {
        if (query == null) {
            throw new RuntimeException("Cannot update null!");
        }
        getWriterForClass(query.getType()).inc(query, name, -amount, upsert, multiple, callback);
    }

    public <T> void dec(final Query<T> query, final Enum field, final double amount, final boolean upsert, final boolean multiple, final AsyncOperationCallback<T> callback) {
        dec(query, field.name(), amount, upsert, multiple, callback);
    }

    public <T> void dec(final Query<T> query, final String name, final double amount, final boolean upsert, final boolean multiple, final AsyncOperationCallback<T> callback) {
        if (query == null) {
            throw new RuntimeException("Cannot update null!");
        }
        getWriterForClass(query.getType()).inc(query, name, -amount, upsert, multiple, callback);

    }

    public <T> void dec(final Query<T> query, final Enum field, final Number amount, final boolean upsert, final boolean multiple, final AsyncOperationCallback<T> callback) {
        dec(query, field.name(), amount, upsert, multiple, callback);
    }

    public <T> void dec(final Query<T> query, final String name, final Number amount, final boolean upsert, final boolean multiple, final AsyncOperationCallback<T> callback) {
        if (query == null) {
            throw new RuntimeException("Cannot update null!");
        }
        getWriterForClass(query.getType()).inc(query, name, -amount.doubleValue(), upsert, multiple, callback);

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
    public void dec(Object toDec, Enum field, double amount) {
        dec(toDec, field.name(), amount);
    }

    public void dec(Object toDec, String field, double amount) {
        inc(toDec, field, -amount);
    }

    public void dec(Object toDec, Enum field, int amount) {
        dec(toDec, field.name(), amount);
    }

    public void dec(Object toDec, String field, int amount) {
        inc(toDec, field, -amount);
    }

    @SuppressWarnings("unused")
    public void dec(Object toDec, Enum field, long amount) {
        dec(toDec, field.name(), amount);
    }

    public void dec(Object toDec, String field, long amount) {
        inc(toDec, field, -amount);
    }

    @SuppressWarnings("unused")
    public void dec(Object toDec, Enum field, Number amount) {
        dec(toDec, field.name(), amount);
    }

    public void dec(Object toDec, String field, Number amount) {
        inc(toDec, field, -amount.doubleValue());
    }

    public void inc(final Object toSet, final Enum field, final long i) {
        inc(toSet, field.name(), i, null);
    }

    public void inc(final Object toSet, final String field, final long i) {
        inc(toSet, field, i, null);
    }

    public void inc(final Object toSet, final Enum field, final int i) {
        inc(toSet, field.name(), i, null);

    }

    public void inc(final Object toSet, final String field, final int i) {
        inc(toSet, field, i, null);
    }

    public void inc(final Object toSet, final Enum field, final double i) {
        inc(toSet, field.name(), i, null);
    }

    public void inc(final Object toSet, final String field, final double i) {
        inc(toSet, field, i, null);
    }

    @SuppressWarnings("unused")
    public void inc(final Object toSet, final Enum field, final Number i) {
        inc(toSet, field.name(), i, null);
    }

    public void inc(final Object toSet, final String field, final Number i) {
        inc(toSet, field, i, null);
    }

    public <T> void inc(final T toSet, final Enum field, final double i, final AsyncOperationCallback<T> callback) {
        inc(toSet, field.name(), i, callback);
    }

    public <T> void inc(final T toSet, final String field, final double i, final AsyncOperationCallback<T> callback) {
        inc(toSet, getMapper().getCollectionName(toSet.getClass()), field, i, callback);
    }

    public <T> void inc(final T toSet, final Enum field, final int i, final AsyncOperationCallback<T> callback) {
        inc(toSet, field.name(), i, callback);
    }

    public <T> void inc(final T toSet, final String field, final int i, final AsyncOperationCallback<T> callback) {
        inc(toSet, getMapper().getCollectionName(toSet.getClass()), field, i, callback);
    }

    public <T> void inc(final T toSet, final Enum field, final long i, final AsyncOperationCallback<T> callback) {
        inc(toSet, field.name(), i, callback);
    }

    public <T> void inc(final T toSet, final String field, final long i, final AsyncOperationCallback<T> callback) {
        inc(toSet, getMapper().getCollectionName(toSet.getClass()), field, i, callback);
    }

    public <T> void inc(final T toSet, final Enum field, final Number i, final AsyncOperationCallback<T> callback) {
        inc(toSet, field.name(), i, callback);
    }

    public <T> void inc(final T toSet, final String field, final Number i, final AsyncOperationCallback<T> callback) {
        inc(toSet, getMapper().getCollectionName(toSet.getClass()), field, i, callback);
    }

    public <T> void inc(final T toSet, Enum collection, final Enum field, final double i, final AsyncOperationCallback<T> callback) {
        inc(toSet, field.name(), i, callback);
    }

    public <T> void inc(final T toSet, String collection, final Enum field, final double i, final AsyncOperationCallback<T> callback) {
        inc(toSet, collection, field.name(), i, callback);
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

    public <T> void inc(final T toSet, String collection, final Enum field, final int i, final AsyncOperationCallback<T> callback) {
        inc(toSet, collection, field.name(), i, callback);
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

    public <T> void inc(final T toSet, String collection, final Enum field, final long i, final AsyncOperationCallback<T> callback) {
        inc(toSet, collection, field.name(), i, callback);
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

    public <T> void inc(final T toSet, String collection, final Enum field, final Number i, final AsyncOperationCallback<T> callback) {
        inc(toSet, collection, field.name(), i, callback);
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


    public MorphiumObjectMapper getMapper() {
        return objectMapper;
    }

    public AnnotationAndReflectionHelper getARHelper() {
        if (annotationHelper == null) return new AnnotationAndReflectionHelper(true);
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
            List<Map<String, Object>> found = morphiumDriver.find(config.getDatabase(), collection, srch, null, null, 0, 1, 1, null, null, findMetaData);
            if (found != null && !found.isEmpty()) {
                Map<String, Object> dbo = found.get(0);
                Object fromDb = objectMapper.deserialize(o.getClass(), dbo);
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
        }
        return obj;
    }


    public void setNameProviderForClass(Class<?> cls, NameProvider pro) {
        getMapper().setNameProviderForClass(cls, pro);
    }

    public NameProvider getNameProviderForClass(Class<?> cls) {
        return getMapper().getNameProviderForClass(cls);
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
//        if (logger.isDebugEnabled()) {
//            logger.debug("returning write concern for " + cls.getSimpleName());
//        }
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
        if (!isReplicaSet() && timeout < 0) {
            timeout = 0;
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
        //noinspection unchecked
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
        //q.setMorphium(this);
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
            return morphiumDriver.distinct(config.getDatabase(), q.getCollectionName(), key, q.toQueryObject(), q.getCollation(), getReadPreferenceForClass(q.getType()));
        } catch (MorphiumDriverException e) {
            throw new RuntimeException(e);
        }
    }

    public List<Object> distinct(String key, Class cls) {
        return distinct(key, cls, null);
    }

    public List<Object> distinct(String key, Class cls, Collation collation) {
        try {
            return morphiumDriver.distinct(config.getDatabase(), objectMapper.getCollectionName(cls), key, new HashMap<>(), collation, getReadPreferenceForClass(cls));
        } catch (MorphiumDriverException e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings({"unused"})
    public List<Object> distinct(String key, String collectionName) {
        return distinct(key, collectionName, null);
    }

    public List<Object> distinct(String key, String collectionName, Collation collation) {
        try {
            return morphiumDriver.distinct(config.getDatabase(), collectionName, key, new HashMap<>(), collation, config.getDefaultReadPreference());
        } catch (MorphiumDriverException e) {
            throw new RuntimeException(e);
        }
    }


    public <T> T findById(Class<? extends T> type, Object id) {
        return findById(type, id, null);
    }

    public <T> void findById(Class<? extends T> type, Object id, String collection, AsyncOperationCallback callback) {
        createQueryFor(type).setCollectionName(collection).f(getARHelper().getIdFieldName(type)).eq(id).get(callback);
    }

    public <T> T findById(Class<? extends T> type, Object id, String collection) {
        inc(StatisticKeys.READS);
        Cache c = getARHelper().getAnnotationFromHierarchy(type, Cache.class); //type.getAnnotation(Cache.class);
        boolean useCache = c != null && c.readCache() && isReadCacheEnabledForThread();

        if (useCache) {
            if (getCache().getFromIDCache(type, id) != null) {
                inc(StatisticKeys.CHITS);
                return getCache().getFromIDCache(type, id);
            }
            inc(StatisticKeys.CMISS);
        } else {
            inc(StatisticKeys.NO_CACHED_READS);

        }


        @SuppressWarnings("unchecked") List<String> ls = annotationHelper.getFields(type, Id.class);
        if (ls.isEmpty()) {
            throw new RuntimeException("Cannot find by ID on non-Entity");
        }

        return createQueryFor(type).setCollectionName(collection).f(ls.get(0)).eq(id).get();
    }

    public <T> List<T> findByField(Class<? extends T> cls, String fld, Object val) {
        Query<T> q = createQueryFor(cls);
        q = q.f(fld).eq(val);
        return q.asList();
    }

    public <T> List<T> findByField(Class<? extends T> cls, Enum fld, Object val) {
        return findByField(cls, fld.name(), val);
    }


    public <T> boolean setAutoValues(T o) throws IllegalAccessException {
        Class type = o.getClass();
        Object id = getARHelper().getId(o);
        boolean aNew = id == null;
        if (!isAutoValuesEnabledForThread()) {
            return aNew;
        }
        Object reread = null;
        //new object - need to store creation time
        CreationTime ct = getARHelper().getAnnotationFromHierarchy(o.getClass(), CreationTime.class);
        if (ct != null && config.isCheckForNew() && ct.checkForNew() && !aNew) {
            //check if it is new or not
            reread = findById(getARHelper().getRealClass(o.getClass()), getARHelper().getId(o));//reread(o);
            aNew = reread == null;
        }
        if (getARHelper().isAnnotationPresentInHierarchy(type, CreationTime.class) && aNew) {
            boolean checkForNew = Objects.requireNonNull(ct).checkForNew() || getConfig().isCheckForNew();
            @SuppressWarnings("unchecked") List<String> lst = getARHelper().getFields(type, CreationTime.class);
            for (String fld : lst) {
                Field field = getARHelper().getField(o.getClass(), fld);
                if (id != null) {
                    if (checkForNew && reread == null) {
                        reread = findById(o.getClass(), id);
                        aNew = reread == null;
                        //read creation time
                        if (reread != null) {
                            Object value = field.get(reread);
                            field.set(o, value);
                            aNew = false;
                        }
                    } else {
                        if (reread == null) {
                            aNew = (id instanceof MorphiumId); //if id null, is new. if id!=null probably not, if type is objectId
                        } else {
                            Object value = field.get(reread);
                            field.set(o, value);
                            aNew = false;
                        }
                    }
                } else {
                    aNew = true;
                }
            }
            if (aNew) {
                if (lst.isEmpty()) {
                    logger.error("Unable to store creation time as @CreationTime for field is missing");
                } else {
                    long now = System.currentTimeMillis();
                    for (String ctf : lst) {
                        Object val = null;

                        Field f = getARHelper().getField(type, ctf);
                        if (f.getType().equals(long.class) || f.getType().equals(Long.class)) {
                            val = now;
                        } else if (f.getType().equals(Date.class)) {
                            val = new Date(now);
                        } else if (f.getType().equals(String.class)) {
                            CreationTime ctField = f.getAnnotation(CreationTime.class);
                            SimpleDateFormat df = new SimpleDateFormat(ctField.dateFormat());
                            val = df.format(now);
                        }

                        try {
                            f.set(o, val);
                        } catch (IllegalAccessException e) {
                            logger.error("Could not set creation time", e);

                        }

                    }

                }

            }
        }


        if (getARHelper().isAnnotationPresentInHierarchy(type, LastChange.class)) {
            @SuppressWarnings("unchecked") List<String> lst = getARHelper().getFields(type, LastChange.class);
            if (lst != null && !lst.isEmpty()) {
                long now = System.currentTimeMillis();
                for (String ctf : lst) {
                    Object val = null;

                    Field f = getARHelper().getField(type, ctf);
                    if (f.getType().equals(long.class) || f.getType().equals(Long.class)) {
                        val = now;
                    } else if (f.getType().equals(Date.class)) {
                        val = new Date(now);
                    } else if (f.getType().equals(String.class)) {
                        LastChange ctField = f.getAnnotation(LastChange.class);
                        SimpleDateFormat df = new SimpleDateFormat(ctField.dateFormat());
                        val = df.format(now);
                    }

                    try {
                        f.set(o, val);
                    } catch (IllegalAccessException e) {
                        logger.error("Could not set modification time", e);

                    }
                }
            } else {
                logger.warn("Could not store last change - @LastChange missing!");
            }

        }
        return aNew;
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

    public void flush(Class type) {
        config.getBufferedWriter().flush(type);
        config.getWriter().flush(type);
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
                    String[] i = idx.split(":");
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


    public <T> void insert(T o) {
        if (o instanceof List) {
            insertList((List) o, null);
        } else if (o instanceof Collection) {
            // noinspection unchecked
            insertList(new ArrayList<>((Collection) o), null);
        } else {
            insert(o, null);
        }
    }

    public <T> void insert(T o, AsyncOperationCallback<T> callback) {
        if (o instanceof List) {
            insertList((List) o, callback);
        } else if (o instanceof Collection) {
            // noinspection unchecked
            insertList(new ArrayList<>((Collection) o), callback);
        } else {
            insert(o, getMapper().getCollectionName(o.getClass()), callback);
        }
    }


    private <T> void insert(T o, String collection, AsyncOperationCallback<T> callback) {
        if (o instanceof List) {
            insertList((List) o, collection, callback);
        } else if (o instanceof Collection) {
            // noinspection unchecked
            insertList(new ArrayList<>((Collection) o), collection, callback);
        }

        getWriterForClass(o.getClass()).insert(o, collection, callback);
    }

    private <T> void insertList(List lst, String collection, AsyncOperationCallback<T> callback) {
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
                writers.get(cls).insert((List<T>) values.get(cls), collection, callback);
            } catch (Exception e) {
                logger.error("Write failed for " + cls.getName() + " lst of size " + values.get(cls).size(), e);
                throw new RuntimeException(e);
            }
        }
    }

    private <T> void insertList(List arrayList, AsyncOperationCallback<T> callback) {
        insertList(arrayList, null, callback);
    }

    private <T> void insertList(List arrayList) {
        insertList(arrayList, null, null);
    }

    /**
     * Stores a single Object. Clears the corresponding cache
     *
     * @param o - Object to store
     */
    public <T> void store(T o) {
        if (o instanceof List) {
            //noinspection unchecked
            storeList((List) o);
        } else if (o instanceof Collection) {
            // noinspection unchecked
            storeList(new ArrayList<>((Collection) o));
        } else {
            store(o, null);
        }
    }

    public <T> void store(T o, final AsyncOperationCallback<T> callback) {
        if (o instanceof List) {
            //noinspection unchecked
            storeList((List) o, callback);
        } else if (o instanceof Collection) {
            // noinspection unchecked
            storeList(new ArrayList<>((Collection) o), callback);
        } else {
            store(o, getMapper().getCollectionName(o.getClass()), callback);
        }
    }

    public <T> void store(T o, String collection, final AsyncOperationCallback<T> callback) {
        if (o instanceof List) {
            //noinspection unchecked
            storeList((List) o, collection, callback);
        } else if (o instanceof Collection) {
            // noinspection unchecked
            storeList(new ArrayList<>((Collection) o), collection, callback);
        }
        if (getARHelper().getId(o) != null) {
            getWriterForClass(o.getClass()).store(o, collection, callback);
        } else {
            getWriterForClass(o.getClass()).insert(o, collection, callback);
        }
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
     * @param <T> - type of list elements
     */
    public <T> void storeList(List<T> lst) {
        storeList(lst, (AsyncOperationCallback<T>) null);
    }

    public <T> void storeList(Set<T> set) {
        storeList(new ArrayList<>(set), (AsyncOperationCallback<T>) null);
    }

    public <T> void storeList(List<T> lst, final AsyncOperationCallback<T> callback) {
        //have to sort list - might have different objects
        List<T> storeDirect = new ArrayList<>();
        List<T> insertDirect = new ArrayList<>();
        if (isWriteBufferEnabledForThread()) {
            final List<T> storeInBg = new ArrayList<>();
            final List<T> insertInBg = new ArrayList<>();

            //checking permission - might take some time ;-(
            for (T o : lst) {
                if (annotationHelper.isBufferedWrite(getARHelper().getRealClass(o.getClass()))) {
                    if (getARHelper().getId(o) == null) {
                        insertInBg.add(o);
                    } else {
                        storeInBg.add(o);
                    }
                } else {
                    if (getARHelper().getId(o) == null) {
                        insertDirect.add(o);
                    } else {
                        storeDirect.add(o);
                    }
                }
            }
            config.getBufferedWriter().store(storeInBg, callback);
            config.getWriter().store(storeDirect, callback);
            config.getBufferedWriter().insert(insertInBg, callback);
            config.getWriter().insert(insertDirect, callback);
        } else {

            for (T o : lst) {
                if (getARHelper().getId(o) == null) {
                    insertDirect.add(o);
                } else {
                    storeDirect.add(o);
                }
            }
            config.getWriter().store(storeDirect, callback);
            config.getWriter().insert(insertDirect, callback);
        }
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


    public boolean exists(String db) throws MorphiumDriverException {
        return getDriver().exists(db);
    }

    public boolean exists(String db, String col) throws MorphiumDriverException {
        return getDriver().exists(db, col);
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


    public List<ShutdownListener> getShutDownListeners() {
        return shutDownListeners;
    }

    public void addShutdownListener(ShutdownListener l) {
        shutDownListeners.add(l);
    }

    @SuppressWarnings("unused")
    public void removeShutdownListener(ShutdownListener l) {
        shutDownListeners.remove(l);
    }

    public void close() {
        if (asyncOperationsThreadPool != null) {
            asyncOperationsThreadPool.shutdownNow();
        }
        asyncOperationsThreadPool = null;
        for (ShutdownListener l : shutDownListeners) {
            l.onShutdown(this);
        }

        if (rsMonitor != null) {
            rsMonitor.terminate();
            rsMonitor = null;
        }

        if (config != null) {
            config.getAsyncWriter().close();
            config.getBufferedWriter().close();
            config.getWriter().close();
        }
        if (morphiumDriver != null) {
            try {
                morphiumDriver.close();
            } catch (MorphiumDriverException e) {
                e.printStackTrace();
            }
            morphiumDriver = null;
        }
        // if (oplogMonitor != null) {
        // oplogMonitor.terminate();
        // try {
        // Thread.sleep(1000); //wait for it to finish...
        // } catch (InterruptedException e) {
        // //ignoring interrupted excepition
        // }
        // }
        // if (oplogMonitorThread != null) {
        // try {
        // oplogMonitorThread.interrupt();
        // } catch (Exception e) {
        // //ignoring
        // }
        // }
        if (config != null) {
            config.getCache().resetCache();
            config.getCache().close();
            config.setBufferedWriter(null);
            config.setAsyncWriter(null);
            config.setWriter(null);
            config = null;

        }
        // config.getCache().resetCache();
        // MorphiumSingleton.reset();
    }

    @SuppressWarnings("unused")
    public String createCamelCase(String n) {
        return annotationHelper.createCamelCase(n, false);
    }


    //    public void addOplogListener(OplogListener lst) {
    //        if (oplogMonitor != null) {
    //            oplogMonitor.addListener(lst);
    //        }
    //    }
    //
    //    public void removeOplogListener(OplogListener lst) {
    //        if (oplogMonitor != null) {
    //            oplogMonitor.removeListener(lst);
    //        }
    //    }
    ////////////////////////////////
    /////// MAP/REDUCE
    /////

    public <T> List<T> mapReduce(Class<? extends T> type, String map, String reduce) throws MorphiumDriverException {
        List<Map<String, Object>> result = getDriver().mapReduce(getConfig().getDatabase(), getMapper().getCollectionName(type), map, reduce);
        List<T> ret = new ArrayList<>();

        for (Map<String, Object> o : result) {
            ret.add(getMapper().deserialize(type, o));
        }
        return ret;

    }

    /////////////////
    //// AGGREGATOR Support
    ///

    public <T, R> Aggregator<T, R> createAggregator(Class<? extends T> type, Class<? extends R> resultType) {
        Aggregator<T, R> aggregator = config.getAggregatorFactory().createAggregator(type, resultType);
        aggregator.setMorphium(this);
        return aggregator;
    }


    @SuppressWarnings("unchecked")
    public <T> T createLazyLoadedEntity(Class<? extends T> cls, Object id, String collectionName) {
        return (T) Enhancer.create(cls, new Class[]{Serializable.class}, new LazyDeReferencingProxy(this, cls, id, collectionName));
    }

    @SuppressWarnings("unchecked")
    public <T> MongoField<T> createMongoField() {
        if (config == null) {
            return new MongoFieldImpl<>();
        }
        try {
            return (MongoField<T>) config.getFieldImplClass().getDeclaredConstructor().newInstance();
        } catch (InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
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

    public void startTransaction() {
        getDriver().startTransaction();
    }

    public MorphiumTransactionContext getTransaction() {
        return getDriver().getTransactionContext();
    }

    public void setTransaction(MorphiumTransactionContext ctx) {
        getDriver().setTransactionContext(ctx);
    }

    public void commitTransaction() {
        getDriver().commitTransaction();
    }

    public void abortTransaction() {
        getDriver().abortTransaction();
    }

    public <T> void watchAsync(String collectionName, boolean updateFull, ChangeStreamListener lst) {
        asyncOperationsThreadPool.execute(() -> watch(collectionName, updateFull, null, lst));
    }

    public <T> void watchAsync(String collectionName, boolean updateFull, List<Map<String, Object>> pipeline, ChangeStreamListener lst) {
        asyncOperationsThreadPool.execute(() -> watch(collectionName, updateFull, pipeline, lst));
    }

    public <T> void watchAsync(Class<T> entity, boolean updateFull, ChangeStreamListener lst) {
        asyncOperationsThreadPool.execute(() -> watch(entity, updateFull, null, lst));
    }

    public <T> void watchAsync(Class<T> entity, boolean updateFull, List<Map<String, Object>> pipeline, ChangeStreamListener lst) {
        asyncOperationsThreadPool.execute(() -> watch(entity, updateFull, pipeline, lst));
    }

    public <T> void watch(Class<T> entity, boolean updateFull, ChangeStreamListener lst) {
        watch(getMapper().getCollectionName(entity), updateFull, null, lst);
    }

    public <T> void watch(Class<T> entity, boolean updateFull, List<Map<String, Object>> pipeline, ChangeStreamListener lst) {
        watch(getMapper().getCollectionName(entity), updateFull, pipeline, lst);
    }

    public <T> void watch(String collectionName, boolean updateFull, ChangeStreamListener lst) {
        watch(collectionName, config.getMaxWaitTime(), updateFull, null, lst);
    }

    public <T> void watch(String collectionName, boolean updateFull, List<Map<String, Object>> pipeline, ChangeStreamListener lst) {
        watch(collectionName, config.getMaxWaitTime(), updateFull, pipeline, lst);
    }

    public <T> void watch(String collectionName, int maxWaitTime, boolean updateFull, List<Map<String, Object>> pipeline, ChangeStreamListener lst) {
        try {
            getDriver().watch(config.getDatabase(), collectionName, maxWaitTime, updateFull, pipeline, new DriverTailableIterationCallback() {
                boolean b = true;

                @Override
                public void incomingData(Map<String, Object> data, long dur) {
                    b = processEvent(lst, data);
                }

                @Override
                public boolean isContinued() {
                    return b;
                }
            });
        } catch (MorphiumDriverException e) {
            throw new RuntimeException(e);
        }
    }

    private boolean processEvent(ChangeStreamListener lst, Map<String, Object> doc) {
        MorphiumObjectMapper mapper = new ObjectMapperImpl();
        AnnotationAndReflectionHelper hlp = new AnnotationAndReflectionHelper(false);
        mapper.setAnnotationHelper(hlp);

        @SuppressWarnings("unchecked") Map<String, Object> obj = (Map<String, Object>) doc.get("fullDocument");
        doc.remove("fullDocument");
        ChangeStreamEvent evt = mapper.deserialize(ChangeStreamEvent.class, doc);

        evt.setFullDocument(obj);
        return lst.incomingData(evt);
    }


    public <T> void watchDbAsync(String dbName, boolean updateFull, List<Map<String, Object>> pipeline, ChangeStreamListener lst) {
        asyncOperationsThreadPool.execute(() -> {
            watchDb(dbName, updateFull, lst);
            logger.debug("watch async finished");
        });
    }


    public <T> void watchDbAsync(boolean updateFull, ChangeStreamListener lst) {
        watchDbAsync(config.getDatabase(), updateFull, null, lst);
    }

    public <T> void watchDbAsync(boolean updateFull, List<Map<String, Object>> pipeline, ChangeStreamListener lst) {
        watchDbAsync(config.getDatabase(), updateFull, pipeline, lst);
    }

    public <T> void watchDb(boolean updateFull, ChangeStreamListener lst) {
        watchDb(getConfig().getDatabase(), updateFull, lst);
    }

    public <T> void watchDb(String dbName, boolean updateFull, ChangeStreamListener lst) {
        watchDb(dbName, getConfig().getMaxWaitTime(), updateFull, null, lst);
    }

    public <T> void watchDb(String dbName, boolean updateFull, List<Map<String, Object>> pipeline, ChangeStreamListener lst) {
        watchDb(dbName, getConfig().getMaxWaitTime(), updateFull, pipeline, lst);
    }

    public <T> void watchDb(String dbName, int maxWaitTime, boolean updateFull, List<Map<String, Object>> pipeline, ChangeStreamListener lst) {
        try {
            getDriver().watch(dbName, maxWaitTime, updateFull, pipeline, new DriverTailableIterationCallback() {
                private boolean b = true;

                @Override
                public void incomingData(Map<String, Object> data, long dur) {
                    b = processEvent(lst, data);
                }

                @Override
                public boolean isContinued() {
                    return b;
                }
            });
        } catch (MorphiumDriverException e) {
            throw new RuntimeException(e);
        }
    }

    public void reset() {
        MorphiumConfig cfg = getConfig();
        if (lst != null) listeners.remove(lst);
        close();
        setConfig(cfg);
        initializeAndConnect();
    }


    public List<Map<String, Object>> getMissingIndicesFor(Class<?> entity) throws MorphiumDriverException {
        return getMissingIndicesFor(entity, objectMapper.getCollectionName(entity));
    }

    public List<Map<String, Object>> getMissingIndicesFor(Class<?> entity, String collection) throws MorphiumDriverException {
        List<Map<String, Object>> missingIndexDef = new ArrayList<>();
        Index i = annotationHelper.getAnnotationFromClass(entity, Index.class);
        List<Map<String, Object>> ind = morphiumDriver.getIndexes(getConfig().getDatabase(), collection);
        List<Map<String, Object>> indices = new ArrayList<>();
        for (Map<String, Object> m : ind) {
            Map<String, Object> indexKey = (Map<String, Object>) m.get("key");
            indices.add(indexKey);
        }
        if (indices.size() > 0 && indices.get(0) == null) {
            logger.error("Something is wrong!");
        }
        if (i != null) {
            if (i.value().length > 0) {
                List<Map<String, Object>> options = null;

                if (i.options().length > 0) {
                    //options se
                    options = createIndexMapFrom(i.options());
                }
                if (!i.locale().equals("")) {
                    Map<String, Object> collation = Utils.getMap("locale", i.locale());
                    collation.put("alternate", i.alternate().mongoText);
                    collation.put("backwards", i.backwards());
                    collation.put("caseFirst", i.caseFirst().mongoText);
                    collation.put("caseLevel", i.caseLevel());
                    collation.put("maxVariable", i.maxVariable().mongoText);
                    collation.put("strength", i.strength().mongoValue);
                    options.add(Utils.getMap("collation", collation));
                }
                List<Map<String, Object>> idx = createIndexMapFrom(i.value());
                if (!morphiumDriver.exists(config.getDatabase(), collection) || indices.size() == 0) {
                    logger.info("Collection '" + collection + "' for entity '" + entity.getName() + "' does not exist.");
                    return idx;
                }
                int cnt = 0;
                for (Map<String, Object> m : idx) {
                    Map<String, Object> optionsMap = null;
                    if (options != null && options.size() > cnt) {
                        optionsMap = options.get(cnt);

                        if (!indices.contains(m)) {
                            if (m.values().toArray()[0].equals("text")) {
                                //special text handling
                                logger.info("Handling text index");
                                boolean found = false;
                                for (Map<String, Object> indexDef : ind) {
                                    if (indexDef.containsKey("textIndexVersion")) {
                                        found = true;
                                        break;
                                    }
                                }
                                if (!found) {
                                    //assuming text index is missing

                                    missingIndexDef.add(m);
                                }
                            } else {
                                missingIndexDef.add(m);
                            }
                        }
                    }

                    cnt++;
                }
            }
        }

        @SuppressWarnings("unchecked") List<String> flds = annotationHelper.getFields(entity, Index.class);
        if (flds != null && !flds.isEmpty()) {

            for (String f : flds) {
                i = annotationHelper.getField(entity, f).getAnnotation(Index.class);
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
                if (!indices.contains(idx)) {
                    //special handling for text indices
                    if (idx.values().toArray()[0].equals("text")) {
                        logger.info("checking for text index...");
                        for (Map<String, Object> indexDef : ind) {
                            if (!indexDef.containsKey("textIndexVersion")) {
                                //assuming text index is missing
                                missingIndexDef.add(idx);
                            }
                        }
                    } else {
                        missingIndexDef.add(idx);
                    }
                }

            }
        }
        return missingIndexDef;
    }


    /**
     * run trhough classpath, find all Entities, check indices
     * returns a list of Entities, whos indices are missing or different
     */
    public Map<Class<?>, List<Map<String, Object>>> checkIndices() {
        return checkIndices(null);
    }

    public Map<Class<?>, List<Map<String, Object>>> checkIndices(ClassInfoList.ClassInfoFilter filter) {

        Map<Class<?>, List<Map<String, Object>>> missingIndicesByClass = new ConcurrentHashMap<>();
//initializing type IDs
        try (ScanResult scanResult =
                     new ClassGraph()
                             //                     .verbose()             // Enable verbose logging
                             .enableAnnotationInfo()
//                             .enableFieldInfo()
                             .enableClassInfo()       // Scan classes, methods, fields, annotations
                             .scan()) {
            ClassInfoList entities =
                    scanResult.getAllClasses();
            if (filter != null) {
                entities = entities.filter(filter);
            }
            //entities.addAll(scanResult.getClassesWithAnnotation(Embedded.class.getName()));

            for (String cn : entities.getNames()) {
                //ClassInfo ci = scanResult.getClassInfo(cn);

                try {
                    //if (param.getName().equals("index"))
                    //logger.info("Class " + cn + "   Param " + param.getName() + " = " + param.getValue());
                    if (cn.startsWith("sun.")) continue;
                    if (cn.startsWith("com.sun.")) continue;
                    if (cn.startsWith("org.assertj.")) continue;
                    //logger.info("Checking "+cn);
                    Class<?> entity = Class.forName(cn);
                    if (annotationHelper.getAnnotationFromHierarchy(entity, Entity.class) == null) {
                        continue;
                    }
                    List<Map<String, Object>> missing = getMissingIndicesFor(entity);

                    if (missing != null && !missing.isEmpty()) {
                        missingIndicesByClass.put(entity, missing);
                    }
                    if (annotationHelper.isAnnotationPresentInHierarchy(entity, Capped.class)) {
                        if (!morphiumDriver.isCapped(getConfig().getDatabase(), getMapper().getCollectionName(entity))) {
                            missingIndicesByClass.putIfAbsent(entity, new ArrayList<>());
                            Capped capped = annotationHelper.getAnnotationFromClass(entity, Capped.class);
                            missingIndicesByClass.get(entity).add(
                                    Utils.getMap("__capped_entries", (Object) capped.maxEntries())
                                            .add("__capped_size", capped.maxSize())
                            );
                        }
                    }
                } catch (Throwable e) {
                    //swallow
                    //logger.error("Could not check indices for " + cn, e);
                }
            }
        } catch (Exception e) {
            logger.error("error", e);
        }
        return missingIndicesByClass;
    }

    public void addCommandListener(CommandListener cmd) {
        morphiumDriver.addCommandListener(cmd);
    }

    public void removeCommandListener(CommandListener cmd) {
        morphiumDriver.removeCommandListener(cmd);
    }

    public void addClusterListener(ClusterListener cl) {
        morphiumDriver.addClusterListener(cl);
    }

    public void removeClusterListener(ClusterListener cl) {
        morphiumDriver.removeClusterListener(cl);
    }

    public void addConnectionPoolListener(ConnectionPoolListener cpl) {
        morphiumDriver.addConnectionPoolListener(cpl);
    }

    public void removeConnectionPoolListener(ConnectionPoolListener cpl) {
        morphiumDriver.removeConnectionPoolListener(cpl);
    }
}
