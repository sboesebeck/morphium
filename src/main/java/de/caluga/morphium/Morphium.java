/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.caluga.morphium;

import de.caluga.morphium.MorphiumStorageListener.UpdateTypes;
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
import de.caluga.morphium.driver.commands.*;
import de.caluga.morphium.driver.commands.ExplainCommand.ExplainVerbosity;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import de.caluga.morphium.driver.wire.MongoConnection;
import de.caluga.morphium.driver.wire.SingleMongoConnectDriver;
import de.caluga.morphium.encryption.EncryptionKeyProvider;
import de.caluga.morphium.encryption.ValueEncryptionProvider;
import de.caluga.morphium.messaging.Msg;
import de.caluga.morphium.objectmapping.MorphiumObjectMapper;
import de.caluga.morphium.ObjectMapperImpl;
import de.caluga.morphium.query.Query;
import de.caluga.morphium.query.QueryIterator;
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
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This is the single access point for accessing MongoDB. This should
 *
 * @author stephan
 */
@SuppressWarnings({ "WeakerAccess", "unused", "unchecked", "CommentedOutCode" })
public class Morphium implements AutoCloseable {

    /**
     * singleton is usually not a good idea in j2ee-Context, but as we did it on
     * several places in
     * the Application it's the easiest way Usage: <code>
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
    private static final Logger log = LoggerFactory.getLogger(Morphium.class);

    private final ThreadLocal<Boolean> enableAutoValues = new ThreadLocal<>();
    private final ThreadLocal<Boolean> enableReadCache = new ThreadLocal<>();
    private final ThreadLocal<Boolean> disableWriteBuffer = new ThreadLocal<>();
    private final ThreadLocal<Boolean> disableAsyncWrites = new ThreadLocal<>();
    // private final List<ProfilingListener> profilingListeners;
    private final List<ShutdownListener> shutDownListeners = new CopyOnWriteArrayList<>();
    private MorphiumConfig config;
    private Map<StatisticKeys, StatisticValue> stats = new ConcurrentHashMap<>();
    private List<MorphiumStorageListener<?>> listeners = new CopyOnWriteArrayList<>();
    private AnnotationAndReflectionHelper annotationHelper;
    private MorphiumObjectMapper objectMapper;
    private EncryptionKeyProvider encryptionKeyProvider;
    private RSMonitor rsMonitor;
    private ThreadPoolExecutor asyncOperationsThreadPool;
    private MorphiumDriver morphiumDriver;

    private JavaxValidationStorageListener lst;
    private ValueEncryptionProvider valueEncryptionProvider;
    private String CREDENTIAL_ENCRYPT_KEY_NAME;

    public Morphium() {
        // profilingListeners = new CopyOnWriteArrayList<>();
    }

    public Morphium(String host, String db) {
        this();
        MorphiumConfig cfg = new MorphiumConfig(db, 100, 5000, 5000);
        cfg.addHostToSeed(host);
        // cfg.setReplicasetMonitoring(false);
        setConfig(cfg);
    }

    public Morphium(String host, int port, String db) {
        this();
        MorphiumConfig cfg = new MorphiumConfig(db, 100, 5000, 5000);
        // cfg.setReplicasetMonitoring(false);
        cfg.addHostToSeed(host, port);
        setConfig(cfg);
    }

    /**
     * init the MongoDbLayer. Uses Morphium-Configuration Object for Configuration.
     * Needs to be set
     * before use or RuntimeException is thrown! us used for de-referencing and
     * automatical save of
     * referenced entities all logging is done in INFO level
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
        asyncOperationsThreadPool = new ThreadPoolExecutor(getConfig().getThreadPoolAsyncOpCoreSize(),
          getConfig().getThreadPoolAsyncOpMaxSize(), getConfig().getThreadPoolAsyncOpKeepAliveTime(),
          TimeUnit.MILLISECONDS, queue);
        asyncOperationsThreadPool.setRejectedExecutionHandler((r, executor)->{
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

    @SuppressWarnings("CommentedOutCode")
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
                        // swallow
                    }
                }
            });

            try (ScanResult scanResult = new ClassGraph().enableAllInfo() // Scan classes, methods, fields, annotations
             .scan()) {
                ClassInfoList entities = scanResult.getClassesImplementing(MorphiumDriver.class.getName());

                // entities.addAll(scanResult.getClassesWithAnnotation(Embedded.class.getName()));
                if (log.isDebugEnabled()) {
                    log.debug("Found " + entities.size() + " drivers in classpath");
                }

                if (config.getDriverName() == null) {
                    config.setDriverName(SingleMongoConnectDriver.driverName);
                    morphiumDriver = new SingleMongoConnectDriver();
                } else {
                    for (String cn : entities.getNames()) {
                        try {
                            Class c = Class.forName(cn);

                            if (Modifier.isAbstract(c.getModifiers())) {
                                continue;
                            }

                            var flds = annotationHelper.getAllFields(c);

                            for (var f : flds) {
                                if (Modifier.isStatic(f.getModifiers()) && Modifier.isFinal(f.getModifiers())
                                 && Modifier.isPublic(f.getModifiers()) && f.getName().equals("driverName")) {
                                    String dn = (String) f.get(c);
                                    log.debug("Found driverName: " + dn);

                                    if (dn.equals(config.getDriverName())) {
                                        morphiumDriver = (MorphiumDriver) c.getDeclaredConstructor().newInstance();
                                    }

                                    break;
                                }
                            }

                            if (morphiumDriver == null) {
                                var drv = (MorphiumDriver) c.getDeclaredConstructor().newInstance();

                                if (drv.getName().equals(config.getDriverName())) {
                                    morphiumDriver = drv;
                                    break;
                                }
                            }
                        } catch (Throwable e) {
                            log.error("Could not load driver " + config.getDriverName(), e);
                        }
                    }
                }

                if (morphiumDriver == null) {
                    morphiumDriver = new SingleMongoConnectDriver(); // default
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

            // TODO: add Settings
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
            morphiumDriver.setIdleSleepTime(config.getIdleSleepTime());
            //
            // morphiumDriver.setServerSelectionTimeout(config.getServerSelectionTimeout());
            // morphiumDriver.setUuidRepresentation(config.getUuidRepresentation());
            //
            morphiumDriver.setUseSSL(config.isUseSSL());
            // morphiumDriver.setSslContext(config.getSslContext());
            //
            // morphiumDriver.setSslInvalidHostNameAllowed(config.isSslInvalidHostNameAllowed());

            if (config.getHostSeed().isEmpty() && !(morphiumDriver instanceof InMemoryDriver)) {
                throw new RuntimeException("Error - no server address specified!");
            }

            setValidationSupport();

            try {
                objectMapper = new ObjectMapperImpl();
                objectMapper.setMorphium(this);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

            try {
                encryptionKeyProvider = config.getEncryptionKeyProviderClass().getDeclaredConstructor().newInstance();

                if (config.getCredentialsEncryptionKey() != null) {
                    encryptionKeyProvider.setEncryptionKey(CREDENTIAL_ENCRYPT_KEY_NAME,
                     config.getCredentialsEncryptionKey().getBytes(StandardCharsets.UTF_8));
                }

                if (config.getCredentialsDecryptionKey() != null) {
                    encryptionKeyProvider.setDecryptionKey(CREDENTIAL_ENCRYPT_KEY_NAME,
                     config.getCredentialsDecryptionKey().getBytes(StandardCharsets.UTF_8));
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

            try {
                valueEncryptionProvider = config.getValueEncryptionProviderClass().getDeclaredConstructor()
                 .newInstance();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

            if (config.getMongoLogin() != null && config.getMongoPassword() != null) {
                if (config.getCredentialsEncrypted() != null && config.getCredentialsEncrypted()) {
                    var key = getEncryptionKeyProvider().getDecryptionKey(CREDENTIAL_ENCRYPT_KEY_NAME);
                    valueEncryptionProvider
                    .setEncryptionKey(getEncryptionKeyProvider().getEncryptionKey(CREDENTIAL_ENCRYPT_KEY_NAME));
                    valueEncryptionProvider
                    .setDecryptionKey(getEncryptionKeyProvider().getDecryptionKey(CREDENTIAL_ENCRYPT_KEY_NAME));

                    if (key == null) {
                        log.error("Cannot decrypt - no key for mongodb_crendentials set!");
                    }

                    try {
                        var user = new String(getValueEncrpytionProvider()
                          .decrypt(Base64.getDecoder().decode(config.getMongoLogin())));
                        var passwd = new String(getValueEncrpytionProvider()
                          .decrypt(Base64.getDecoder().decode(config.getMongoPassword())));
                        var authdb = "admin";

                        if (config.getMongoAuthDb() != null) {
                            authdb = new String(getValueEncrpytionProvider()
                              .decrypt(Base64.getDecoder().decode(config.getMongoAuthDb())));
                        }

                        morphiumDriver.setCredentials(authdb, user, passwd);
                    } catch (Exception e) {
                        throw new IllegalArgumentException("Credential decryption failed", e);
                    }
                } else {
                    morphiumDriver.setCredentials(config.getMongoAuthDb(), config.getMongoLogin(),
                     config.getMongoPassword());
                }
            }

            // if (config.getMongoAdminUser() != null && config.getMongoAdminPwd() !=
            // null) {
            // morphiumDriver.setCredentials("admin", config.getMongoAdminUser(),
            // config.getMongoAdminPwd());
            // }
            String[] seed = new String[config.getHostSeed().size()];

            for (int i = 0; i < seed.length; i++) {
                seed[i] = config.getHostSeed().get(i);
            }

            morphiumDriver.setHostSeed(seed);
            // morphiumDriver.setAtlasUrl(config.getAtlasUrl());

            //
            // morphiumDriver.setDefaultReadPreference(config.getDefaultReadPreference());
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
            // Defaulting to standard Cache impl
            config.setCache(new MorphiumCacheImpl());
        }

        config.getCache().setAnnotationAndReflectionHelper(getARHelper());
        config.getCache().setGlobalCacheTimeout(config.getGlobalCacheValidTime());
        config.getCache().setHouskeepingIntervalPause(config.getHousekeepingTimeout());

        // if (getConfig().isOplogMonitorEnabled()) {
        // oplogMonitor = new OplogMonitor(this);
        // oplogMonitorThread = new Thread(oplogMonitor);
        // oplogMonitorThread.setDaemon(true);
        // oplogMonitorThread.setName("oplogmonitor");
        // oplogMonitorThread.start();
        // }

        if (isReplicaSet()) {
            rsMonitor = new RSMonitor(this);
            rsMonitor.start();
            rsMonitor.getReplicaSetStatus(false);
        }

        log.debug("Checking for capped collections...");
        // checking capped
        var capped = checkCapped();

        if (capped != null && !capped.isEmpty()) {
            for (Class cls : capped.keySet()) {
                switch (config.getCappedCheck()) {
                case WARN_ON_STARTUP:
                    log.warn("Collection for entity " + cls.getName() + " is not capped although configured!");
                    break;

                case CONVERT_EXISTING_ON_STARTUP:
                    try {
                        if (exists(getDatabase(), getMapper().getCollectionName(cls))) {
                            log.warn("Existing collection is not capped - ATTENTION!");
                            // convertToCapped(cls, capped.get(cls).get("size"), capped.get(cls).get("max"),
                            // null);
                        }
                    } catch (MorphiumDriverException e) {
                        throw new RuntimeException(e);
                    }

                    break;

                case CREATE_ON_STARTUP:
                    try {
                        if (!morphiumDriver.exists(getDatabase(), getMapper().getCollectionName(cls))) {
                            MongoConnection primaryConnection = null;

                            try {
                                primaryConnection = morphiumDriver.getPrimaryConnection(null);
                                CreateCommand cmd = new CreateCommand(primaryConnection);
                                cmd.setDb(getDatabase()).setColl(getMapper().getCollectionName(cls)).setCapped(true)
                                .setMax(capped.get(cls).get("max")).setSize(capped.get(cls).get("size"));
                                var ret = cmd.execute();
                                log.debug("Created capped collection");
                            } catch (MorphiumDriverException e) {
                                throw new RuntimeException(e);
                            } finally {
                                if (primaryConnection != null) {
                                    primaryConnection.release();
                                }
                            }
                        }
                    } catch (MorphiumDriverException e) {
                        throw new RuntimeException(e);
                    }

                case CREATE_ON_WRITE_NEW_COL:
                case NO_CHECK:
                    break;

                default:
                    throw new IllegalArgumentException("Unknow value for cappedcheck " + config.getCappedCheck());
                }
            }
        }

        if (!config.getIndexCheck().equals(MorphiumConfig.IndexCheck.NO_CHECK)
         && config.getIndexCheck().equals(MorphiumConfig.IndexCheck.CREATE_ON_STARTUP)) {
            Map<Class<?>, List<IndexDescription>> missing = checkIndices(classInfo->!classInfo.getPackageName().startsWith("de.caluga.morphium"));

            if (missing != null && !missing.isEmpty()) {
                for (Class<?> cls : missing.keySet()) {
                    if (missing.get(cls).size() != 0) {
                        if (Msg.class.isAssignableFrom(cls)) {
                            // ignoring message class, messaging creates indexes
                            continue;
                        }

                        try {
                            if (config.getIndexCheck().equals(MorphiumConfig.IndexCheck.WARN_ON_STARTUP)) {
                                log.warn("Missing indices for entity " + cls.getName() + ": " + missing.get(cls).size());
                            } else if (config.getIndexCheck().equals(MorphiumConfig.IndexCheck.CREATE_ON_STARTUP)) {
                                log.warn("Creating missing indices for entity " + cls.getName());
                                // noinspection unchecked
                                ensureIndicesFor(cls);
                            }
                        } catch (Exception e) {
                            log.error("Could not process indices for entity " + cls.getName(), e);
                        }
                    }
                }
            }
        }

        getCache().setValidCacheTime(CollectionInfo.class, 15000); // TODO: settings for collectionCache
        // logger.info("Initialization successful...");
    }

    public ValueEncryptionProvider getValueEncrpytionProvider() {
        return valueEncryptionProvider;
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
        if (lst == null) {
            return;
        }

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
            log.debug("Adding javax.validation Support...");
        } catch (Exception cnf) {
            log.debug("Validation disabled!");
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
        if (getCache().isCached(CollectionInfo.class, pattern)) {
            var lst = getCache().getFromCache(CollectionInfo.class, pattern);
            List<String> ret = new ArrayList<>();

            for (var n : lst) {
                ret.add(n.getName());
            }

            return ret;
        }

        try {
            var result = getDriver().listCollections(getConfig().getDatabase(), pattern);
            var lst = new ArrayList<CollectionInfo>();

            for (var r : result) {
                lst.add(new CollectionInfo().setName(r));
            }

            getCache().addToCache(pattern, CollectionInfo.class, lst);
            return result;
        } catch (MorphiumDriverException e) {
            throw new RuntimeException(e);
        }
    }

    public void reconnectToDb(String db) {
        Properties prop = getConfig().asProperties();
        var dec = getConfig().getCredentialsDecryptionKey();
        var enc = getConfig().getCredentialsEncryptionKey();
        close();
        MorphiumConfig cfg = new MorphiumConfig(prop);
        cfg.setCredentialsDecryptionKey(dec);
        cfg.setCredentialsEncryptionKey(enc);
        cfg.setDatabase(db);
        setConfig(cfg);
    }

    public void addListener(MorphiumStorageListener<?> lst) {
        List<MorphiumStorageListener<?>> newList = new ArrayList<>(listeners);
        newList.add(lst);
        listeners = newList;
    }

    public void removeListener(MorphiumStorageListener<?> lst) {
        List<MorphiumStorageListener<?>> newList = new ArrayList<>(listeners);
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

    public Query<Map<String, Object>> createMapQuery(String collection) {
        try {
            Query<Map<String, Object>> q = new Query<>(this, null, getAsyncOperationsThreadPool());
            q.setCollectionName(collection);
            return q;
        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }

    public <T> Query<T> createQueryByTemplate(T template, String ... fields) {
        Class cls = template.getClass();
        List<String> flds;

        if (fields.length > 0) {
            flds = new ArrayList<>(Arrays.asList(fields));
        } else {
            // noinspection unchecked
            flds = annotationHelper.getFields(cls);
        }

        @SuppressWarnings("unchecked")
        Query<T> q = createQueryFor((Class<T>) cls);

        for (String f : flds) {
            try {
                q.f(f).eq(annotationHelper.getValue(template, f));
            } catch (Exception e) {
                log.error("Could not read field " + f + " of object " + cls.getName());
            }
        }

        return q;
    }

    /**
     * search for objects similar to template concerning all given fields. If no
     * fields are
     * specified, all NON Null-Fields are taken into account if specified, field
     * might also be null
     *
     * @param template - what to search for
     * @param fields   - fields to use for searching
     * @param <T>      - type
     * @return result of search
     */
    @SuppressWarnings({ "unchecked", "UnusedDeclaration" })
    @Deprecated
    public <T> List<T> findByTemplate(T template, String ... fields) {
        return createQueryByTemplate(template, fields).asList();
    }

    @SuppressWarnings({ "unchecked", "UnusedDeclaration" })
    public <T> void unset(T toSet, Enum<?> field) {
        unset(toSet, field.name(), (AsyncOperationCallback) null);
    }

    public <T> void unset(final T toSet, final String field) {
        // noinspection unchecked
        unset(toSet, field, (AsyncOperationCallback) null);
    }

    public <T> void unset(final T toSet, final Enum<?> field, final AsyncOperationCallback<T> callback) {
        unset(toSet, field.name(), callback);
    }

    public <T> void unset(final T toSet, String collection, final Enum<?> field) {
        unset(toSet, collection, field.name(), null);
    }

    @SuppressWarnings("unused")
    public <T> void unset(final T toSet, String collection, final Enum<?> field,
     final AsyncOperationCallback<T> callback) {
        unset(toSet, collection, field.name(), callback);
    }

    public <T> void unset(final T toSet, final String field, final AsyncOperationCallback<T> callback) {
        unset(toSet, getMapper().getCollectionName(toSet.getClass()), field, callback);
    }

    public <T> void unset(final T toSet, String collection, final String field,
     final AsyncOperationCallback<T> callback) {
        if (toSet == null) {
            throw new RuntimeException("Cannot update null!");
        }

        MorphiumWriter wr = getWriterForClass(toSet.getClass());
        wr.unset(toSet, collection, field, callback);
    }

    public <T> Map<String, Object> unsetQ(Query<T> q, String ... field) {
        return getWriterForClass(q.getType()).unset(q, null, false, field);
    }

    public <T> Map<String, Object> unsetQ(Query<T> q, boolean multiple, String ... field) {
        return getWriterForClass(q.getType()).unset(q, null, multiple, field);
    }

    public <T> Map<String, Object> unsetQ(Query<T> q, Enum ... field) {
        return getWriterForClass(q.getType()).unset(q, null, false, field);
    }

    public <T> Map<String, Object> unsetQ(Query<T> q, boolean multiple, Enum ... field) {
        return getWriterForClass(q.getType()).unset(q, null, multiple, field);
    }

    public <T> Map<String, Object> unsetQ(Query<T> q, AsyncOperationCallback<T> cb, String ... field) {
        return getWriterForClass(q.getType()).unset(q, cb, false, field);
    }

    @SuppressWarnings({ "unused", "UnusedParameters" })
    public <T> Map<String, Object> unsetQ(Query<T> q, AsyncOperationCallback<T> cb, boolean multiple, String ... field) {
        return getWriterForClass(q.getType()).unset(q, cb, false, field);
    }

    public <T> Map<String, Object> unsetQ(Query<T> q, AsyncOperationCallback<T> cb, Enum ... field) {
        return getWriterForClass(q.getType()).unset(q, cb, false, field);
    }

    public <T> Map<String, Object> unsetQ(Query<T> q, boolean multiple, AsyncOperationCallback<T> cb, Enum ... field) {
        return getWriterForClass(q.getType()).unset(q, cb, multiple, field);
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

    public List<Map<String, Object>> runCommand(String command, String collection, Map<String, Object> cmdMap)
    throws MorphiumDriverException {
        GenericCommand cmd = new GenericCommand(getDriver().getPrimaryConnection(null));

        try {
            cmd.setDb(getDatabase());
            cmd.setColl(collection);
            cmd.setCmdData(cmdMap);
            cmd.setCommandName(command);
            int crs = cmd.executeAsync();
            return cmd.getConnection().readAnswerFor(crs);
        } finally {
            cmd.releaseConnection();
        }
    }

    /**
     * automatically convert the collection for the given type to a capped
     * collection only works
     * if @Capped annotation is given for type
     *
     * @param c - type
     */
    public <T> void ensureCapped(final Class<T> c) {
        ensureCapped(c, null);
    }

    public <T> void ensureCapped(final Class<T> c, final AsyncOperationCallback<T> callback) {
        Runnable r = ()->{
            String coll = getMapper().getCollectionName(c);
            // DBCollection collection = null;

            try {
                boolean exists = morphiumDriver.exists(config.getDatabase(), coll);

                if (exists && morphiumDriver.isCapped(config.getDatabase(), coll)) {
                    return;
                }

                if (!exists) {
                    if (log.isDebugEnabled()) {
                        log.debug("Collection does not exist - ensuring indices / capped" + " status");
                    }

                    MongoConnection primaryConnection = morphiumDriver.getPrimaryConnection(getWriteConcernForClass(c));
                    var create = new CreateCommand(primaryConnection);
                    create.setColl(getMapper().getCollectionName(c)).setDb(getDatabase());
                    Capped capped = annotationHelper.getAnnotationFromHierarchy(c, Capped.class);

                    if (capped != null) {
                        create.setSize(capped.maxSize()).setCapped(true).setMax(capped.maxEntries());
                    }

                    // cmd.put("autoIndexId",
                    // (annotationHelper.getIdField(c).getType().equals(MorphiumId.class)));
                    create.execute();
                    primaryConnection.release();
                } else {
                    Capped capped = annotationHelper.getAnnotationFromHierarchy(c, Capped.class);

                    if (capped != null) {
                        log.warn("Collection to be capped already exists! ATTENTION!");
                        // convertToCapped(c, capped.maxSize(), capped.maxEntries(), null);
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
            // new Thread(r).start();
        }
    }

    public Map<String, Object> simplifyQueryObject(Map<String, Object> q) {
        if (q.keySet().size() == 1 && q.get("$and") != null) {
            Map<String, Object> ret = new HashMap<>();
            List<Map<String, Object>> lst = (List<Map<String, Object>>) q.get("$and");

            for (Object o : lst) {
                if (o instanceof Map) {
                    // noinspection unchecked
                    ret.putAll(((Map) o));
                } else {
                    // something we cannot handle
                    return q;
                }
            }

            return ret;
        }

        return q;
    }

    public <T> Map<String, Object> set(Query<T> query, Enum<?> field, Object val) {
        return set(query, field, val, (AsyncOperationCallback<T>) null);
    }

    public <T> Map<String, Object> set(Query<T> query, Enum<?> field, Object val, AsyncOperationCallback<T> callback) {
        Map<String, Object> toSet = new HashMap<>();
        toSet.put(field.name(), val);
        return getWriterForClass(query.getType()).set(query, toSet, false, false, callback);
    }

    public <T> Map<String, Object> set(Query<T> query, String field, Object val) {
        return set(query, field, val, (AsyncOperationCallback<T>) null);
    }

    public <T> Map<String, Object> set(Query<T> query, String field, Object val, AsyncOperationCallback<T> callback) {
        Map<String, Object> toSet = new HashMap<>();
        toSet.put(field, val);
        return getWriterForClass(query.getType()).set(query, toSet, false, false, callback);
    }

    @SuppressWarnings("unused")
    public Map<String, Object> setEnum(Query<?> query, Map<Enum, Object> values, boolean upsert, boolean multiple) {
        HashMap<String, Object> toSet = new HashMap<>();

        for (Map.Entry<Enum, Object> est : values.entrySet()) {
            // noinspection SuspiciousMethodCalls
            toSet.put(est.getKey().name(), values.get(est.getValue()));
        }

        return set(query, toSet, upsert, multiple);
    }

    public Map<String, Object> push(final Query<?> query, final Enum<?> field, final Object value) {
        return push(query, field, value, false, true);
    }

    @SuppressWarnings({ "UnusedDeclaration" })
    public Map<String, Object> pull(Query<?> query, Enum<?> field, Object value) {
        return pull(query, field.name(), value, false, true);
    }

    public Map<String, Object> push(Query<?> query, String field, Object value) {
        return push(query, field, value, false, true);
    }

    @SuppressWarnings("unused")
    public Map<String, Object> pull(Query<?> query, String field, Object value) {
        return pull(query, field, value, false, true);
    }

    public Map<String, Object> push(Query<?> query, Enum<?> field, Object value, boolean upsert, boolean multiple) {
        return push(query, field.name(), value, upsert, multiple);
    }

    @SuppressWarnings({ "UnusedDeclaration" })
    public Map<String, Object> pull(Query<?> query, Enum<?> field, Object value, boolean upsert, boolean multiple) {
        return pull(query, field.name(), value, upsert, multiple);
    }

    @SuppressWarnings({ "UnusedDeclaration" })
    public Map<String, Object> pushAll(Query<?> query, Enum<?> field, List<Object> value, boolean upsert,
     boolean multiple) {
        return pushAll(query, field.name(), value, upsert, multiple);
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
                // array handling
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
                // collection / List etc.
                Collection v = null;
                v = (Collection) fld.get(entity);

                if (v == null) {
                    v = new ArrayList();
                    // noinspection unchecked
                    v.add(value);
                    fld.set(entity, v);
                } else {
                    // noinspection unchecked
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

    public void push(Object entity, String collection, Enum<?> field, Object value, boolean upsert) {
        push(entity, collection, field.name(), value, upsert);
    }

    public void push(Object entity, Enum<?> field, Object value, boolean upsert) {
        push(entity, field.name(), value, upsert);
    }

    @SuppressWarnings({ "UnusedDeclaration" })
    public Map<String, Object> pullAll(Query<?> query, Enum<?> field, List<Object> value, boolean upsert,
     boolean multiple) {
        return pull(query, field.name(), value, upsert, multiple);
    }

    public <T> Map<String, Object> push(final Query<T> query, final String field, final Object value,
     final boolean upsert, final boolean multiple) {
        return push(query, field, value, upsert, multiple, null);
    }

    /**
     * asynchronous call to callback
     *
     * @param query    - the query
     * @param field    - field to push values to
     * @param value    - value to push
     * @param upsert   - insert object, if it does not exist
     * @param multiple - more than one
     * @param callback - will be called, when operation succeeds - synchronous call,
     *                 if null
     * @param <T>      - the type
     */
    @SuppressWarnings("UnusedParameters")
    public <T> Map<String, Object> push(final Query<T> query, final String field, final Object value,
     final boolean upsert, final boolean multiple, final AsyncOperationCallback<T> callback) {
        if (query == null || field == null) {
            throw new RuntimeException("Cannot update null!");
        }

        return getWriterForClass(query.getType()).pushPull(MorphiumStorageListener.UpdateTypes.PUSH, query, field,
                value, upsert, multiple, null);
    }

    public <T> Map<String, Object> addToSet(final Query<T> query, final String field, final Object value) {
        return addToSet(query, field, value, false, false, null);
    }

    public <T> Map<String, Object> addToSet(final Query<T> query, final String field, final Object value,
     final boolean multiple) {
        return addToSet(query, field, value, false, multiple, null);
    }

    public <T> Map<String, Object> addToSet(final Query<T> query, final String field, final Object value,
     final boolean upsert, final boolean multiple) {
        return addToSet(query, field, value, upsert, multiple, null);
    }

    public <T> Map<String, Object> addToSet(final Query<T> query, final String field, final Object value,
     final boolean upsert, final boolean multiple, final AsyncOperationCallback<T> callback) {
        if (query == null || field == null) {
            throw new IllegalArgumentException("Cannot update null");
        }

        return getWriterForClass(query.getType()).pushPull(UpdateTypes.ADD_TO_SET, query, field, value, upsert,
                multiple, callback);
    }

    public <T> Map<String, Object> pull(final Query<T> query, final String field, final Object value,
     final boolean upsert, final boolean multiple) {
        return pull(query, field, value, upsert, multiple, null);
    }

    /**
     * Asynchronous call to pulll
     *
     * @param query    - query
     * @param field    - field to pull
     * @param value    - value to pull from field
     * @param upsert   - insert document unless it exists
     * @param multiple - more than one
     * @param callback -callback to call when operation succeeds - synchronous call,
     *                 if null
     * @param <T>      - type
     */
    public <T> Map<String, Object> pull(final Query<T> query, final String field, final Object value,
     final boolean upsert, final boolean multiple, final AsyncOperationCallback<T> callback) {
        if (query == null || field == null) {
            throw new RuntimeException("Cannot update null!");
        }

        MorphiumWriter wr = getWriterForClass(query.getType());
        return wr.pushPull(MorphiumStorageListener.UpdateTypes.PULL, query, field, value, upsert, multiple, callback);
    }

    public <T> Map<String, Object> pull(final T entity, final String field, final Expr value, final boolean upsert,
     final boolean multiple, final AsyncOperationCallback<T> callback) {
        if (entity == null) {
            throw new IllegalArgumentException("Null Entity cannot be pulled...");
        }

        // noinspection unchecked
        return pull((Query<T>)createQueryFor(entity.getClass()).f("_id").eq(getId(entity)), field, value, upsert,
                multiple, callback);
    }

    public <T> Map<String, Object> pull(final Query<T> query, final String field, final Expr value,
     final boolean upsert, final boolean multiple, final AsyncOperationCallback<T> callback) {
        if (query == null || field == null) {
            throw new RuntimeException("Cannot update null!");
        }

        MorphiumWriter wr = getWriterForClass(query.getType());
        return wr.pushPull(MorphiumStorageListener.UpdateTypes.PULL, query, field, value, upsert, multiple, callback);
    }

    public Map<String, Object> pushAll(final Query<?> query, final String field, final List<?> value,
     final boolean upsert, final boolean multiple) {
        return pushAll(query, field, value, upsert, multiple, null);
    }

    public <T> Map<String, Object> pushAll(final Query<T> query, final String field, final List<?> value,
     final boolean upsert, final boolean multiple, final AsyncOperationCallback<T> callback) {
        if (query == null || field == null) {
            throw new RuntimeException("Cannot update null!");
        }

        MorphiumWriter wr = getWriterForClass(query.getType());
        return wr.pushPullAll(UpdateTypes.PUSH, query, field, value, upsert, multiple, callback);
    }

    public <T> Map<String, Object> addAllToSet(final Query<T> query, final String field, final List<?> value,
     final boolean multiple) {
        return addAllToSet(query, field, value, false, multiple, null);
    }

    public <T> Map<String, Object> addAllToSet(final Query<T> query, final String field, final List<?> value,
     final boolean upsert, final boolean multiple) {
        return addAllToSet(query, field, value, upsert, multiple, null);
    }

    public <T> Map<String, Object> addAllToSet(final Query<T> query, final String field, final List<?> value,
     final boolean upsert, final boolean multiple, final AsyncOperationCallback callback) {
        if (query == null || field == null) {
            throw new RuntimeException("Cannot update null!");
        }

        MorphiumWriter wr = getWriterForClass(query.getType());
        return wr.pushPullAll(UpdateTypes.ADD_TO_SET, query, field, value, upsert, multiple, callback);
    }

    ////////
    /////
    /// SET with Query
    //
    //

    /**
     * will change an entry in mongodb-collection corresponding to given class
     * object if query is
     * too complex, upsert might not work! Upsert should consist of single anueries,
     * which will be
     * used to generate the object to create, unless it already exists. look at
     * Mongodb-query
     * documentation as well
     *
     * @param query    - query to specify which objects should be set
     * @param field    - field to set
     * @param val      - value to set
     * @param upsert   - insert, if it does not exist (query needs to be simple!)
     * @param multiple - update several documents, if false, only first hit will be
     *                 updated
     */
    @SuppressWarnings("unused")
    public <T> Map<String, Object> set(Query<T> query, Enum<?> field, Object val, boolean upsert, boolean multiple) {
        return set(query, field.name(), val, upsert, multiple, null);
    }

    @SuppressWarnings("unused")
    public <T> Map<String, Object> set(Query<T> query, Enum<?> field, Object val, boolean upsert, boolean multiple,
     AsyncOperationCallback<T> callback) {
        return set(query, field.name(), val, upsert, multiple, callback);
    }

    @SuppressWarnings({ "UnusedDeclaration" })
    public Map<String, Object> pullAll(Query<?> query, String field, List<Object> value, boolean upsert,
     boolean multiple) {
        return pull(query, field, value, upsert, multiple);
    }

    public <T> Map<String, Object> set(Query<T> query, String field, Object val, boolean upsert, boolean multiple) {
        return set(query, field, val, upsert, multiple, null);
    }

    public <T> Map<String, Object> set(Query<T> query, String field, Object val, boolean upsert, boolean multiple,
     AsyncOperationCallback<T> callback) {
        Map<String, Object> map = new HashMap<>();
        map.put(field, val);
        return set(query, map, upsert, multiple, callback);
    }

    public Map<String, Object> set(final Query<?> query, final Map<String, Object> map, final boolean upsert,
     final boolean multiple) {
        return set(query, map, upsert, multiple, null);
    }

    public <T> Map<String, Object> set(final Query<T> query, final Map<String, Object> map, final boolean upsert,
     final boolean multiple, AsyncOperationCallback<T> callback) {
        if (query == null) {
            throw new RuntimeException("Cannot update null!");
        }

        return getWriterForClass(query.getType()).set(query, map, upsert, multiple, callback);
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
    public <T> Map<String, Object> currentDate(final Query<?> query, String field, boolean upsert, boolean multiple) {
        return set(query, UtilsMap.of("$currentDate", UtilsMap.of(field, 1)), upsert, multiple);
    }

    public <T> Map<String, Object> currentDate(final Query<?> query, Enum field, boolean upsert, boolean multiple) {
        return set(query, UtilsMap.of("$currentDate", UtilsMap.of(field.name(), 1)), upsert, multiple);
    }

    ////////
    /////
    //
    // SET with object
    //
    @SuppressWarnings("unused")
    public <T> void set(T toSet, Enum<?> field, Object value, AsyncOperationCallback<T> callback) {
        set(toSet, field.name(), value, callback);
    }

    public <T> void set(T toSet, Enum<?> field, Object value) {
        set(toSet, field.name(), value, null);
    }

    @SuppressWarnings("unused")
    public <T> void set(T toSet, String collection, Enum<?> field, Object value) {
        set(toSet, collection, field.name(), value, false, null);
    }

    public <T> void set(final T toSet, final Enum<?> field, final Object value, boolean upserts,
     AsyncOperationCallback<T> callback) {
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

    public <T> void set(final T toSet, String collection, boolean upserts, final Map<Enum, Object> values,
     AsyncOperationCallback<T> callback) {
        Map<String, Object> strValues = new HashMap<>();

        for (Map.Entry<Enum, Object> e : values.entrySet()) {
            strValues.put(e.getKey().name(), e.getValue());
        }

        set(toSet, collection, strValues, upserts, callback);
    }

    /**
     * setting a value in an existing mongo collection entry - no reading necessary.
     * Object is
     * altered in place db.collection.update({"_id":toSet.id},{$set:{field:value}}
     * <b>attention</b>:
     * this alteres the given object toSet in a similar way
     *
     * @param toSet - object to set the value in (or better - the corresponding
     *              entry in mongo)
     * @param field - the field to change
     * @param value - the value to set
     */
    public <T> void set(final T toSet, final String field, final Object value) {
        set(toSet, field, value, null);
    }

    public <T> void set(final T toSet, final String field, final Object value, boolean upserts,
     AsyncOperationCallback<T> callback) {
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

    public <T> void set(final T toSet, String collection, final Map<String, Object> values, boolean upserts,
     AsyncOperationCallback<T> callback) {
        if (toSet == null) {
            throw new RuntimeException("Cannot update null!");
        }

        if (getId(toSet) == null) {
            log.debug("just storing object as it is new...");
            store(toSet);
            return;
        }

        annotationHelper.callLifecycleMethod(PreUpdate.class, toSet);
        getWriterForClass(toSet.getClass()).set(toSet, collection, values, upserts, callback);
        annotationHelper.callLifecycleMethod(PostUpdate.class, toSet);
    }

    //
    public Map<String, Object> getDbStats(String db) throws MorphiumDriverException {
        return getDriver().getDBStats(db);
    }

    public Map<String, Object> getDbStats() throws MorphiumDriverException {
        return getDriver().getDBStats(getConfig().getDatabase());
    }

    public Map<String, Object> getCollStats(Class<?> coll) throws MorphiumDriverException {
        return getDriver().getCollStats(getConfig().getDatabase(), getMapper().getCollectionName(coll));
    }

    public Map<String, Object> getCollStats(String coll) throws MorphiumDriverException {
        return getDriver().getCollStats(getConfig().getDatabase(), coll);
    }

    public <T> void set(final T toSet, String collection, final Enum field, final Object value, boolean upserts,
     AsyncOperationCallback<T> callback) {
        set(toSet, collection, UtilsMap.of(field.name(), value), upserts, callback);
    }

    public <T> void set(final T toSet, String collection, final String field, final Object value, boolean upserts,
     AsyncOperationCallback<T> callback) {
        set(toSet, collection, UtilsMap.of(field, value), upserts, callback);
    }

    public <T> void set(final T toSet, final String field, final Object value,
     final AsyncOperationCallback<T> callback) {
        set(toSet, field, value, false, callback);
    }

    ///////////////////////////////
    //////////////////////
    ///////////////
    ////////////
    ////////// DEC and INC Methods
    /////
    //

    @SuppressWarnings({ "UnusedDeclaration" })
    public Map<String, Object> dec(Query<?> query, Enum<?> field, double amount, boolean upsert, boolean multiple) {
        return dec(query, field.name(), -amount, upsert, multiple);
    }

    @SuppressWarnings("unused")
    public Map<String, Object> dec(Query<?> query, Enum<?> field, long amount, boolean upsert, boolean multiple) {
        return dec(query, field.name(), -amount, upsert, multiple);
    }

    @SuppressWarnings("unused")
    public Map<String, Object> dec(Query<?> query, Enum<?> field, Number amount, boolean upsert, boolean multiple) {
        return dec(query, field.name(), amount.doubleValue(), upsert, multiple);
    }

    @SuppressWarnings("unused")
    public Map<String, Object> dec(Query<?> query, Enum<?> field, int amount, boolean upsert, boolean multiple) {
        return dec(query, field.name(), amount, upsert, multiple);
    }

    public Map<String, Object> dec(Query<?> query, String field, double amount, boolean upsert, boolean multiple) {
        return inc(query, field, -amount, upsert, multiple);
    }

    public Map<String, Object> dec(Query<?> query, String field, long amount, boolean upsert, boolean multiple) {
        return inc(query, field, -amount, upsert, multiple);
    }

    public Map<String, Object> dec(Query<?> query, String field, int amount, boolean upsert, boolean multiple) {
        return inc(query, field, -amount, upsert, multiple);
    }

    @SuppressWarnings("unused")
    public Map<String, Object> dec(Query<?> query, String field, Number amount, boolean upsert, boolean multiple) {
        return inc(query, field, -amount.doubleValue(), upsert, multiple);
    }

    @SuppressWarnings("unused")
    public Map<String, Object> dec(Query<?> query, String field, double amount) {
        return inc(query, field, -amount, false, false);
    }

    @SuppressWarnings("unused")
    public Map<String, Object> dec(Query<?> query, String field, long amount) {
        return inc(query, field, -amount, false, false);
    }

    public Map<String, Object> dec(Query<?> query, String field, int amount) {
        return inc(query, field, -amount, false, false, null);
    }

    @SuppressWarnings("unused")
    public Map<String, Object> dec(Query<?> query, String field, Number amount) {
        return inc(query, field, -amount.doubleValue(), false, false);
    }

    @SuppressWarnings({ "UnusedDeclaration" })
    public Map<String, Object> dec(Query<?> query, Enum<?> field, double amount) {
        return inc(query, field, -amount, false, false);
    }

    @SuppressWarnings("unused")
    public Map<String, Object> dec(Query<?> query, Enum<?> field, long amount) {
        return inc(query, field, -amount, false, false);
    }

    @SuppressWarnings("unused")
    public Map<String, Object> dec(Query<?> query, Enum<?> field, int amount) {
        return inc(query, field, -amount, false, false);
    }

    @SuppressWarnings("unused")
    public Map<String, Object> dec(Query<?> query, Enum<?> field, Number amount) {
        return inc(query, field, -amount.doubleValue(), false, false);
    }

    @SuppressWarnings("unused")
    public Map<String, Object> inc(Query<?> query, String field, long amount) {
        return inc(query, field, amount, false, false);
    }

    public Map<String, Object> inc(Query<?> query, String field, int amount) {
        return inc(query, field, amount, false, false);
    }

    @SuppressWarnings("unused")
    public Map<String, Object> inc(Query<?> query, String field, Number amount) {
        return inc(query, field, amount, false, false);
    }

    @SuppressWarnings("unused")
    public Map<String, Object> inc(Query<?> query, String field, double amount) {
        return inc(query, field, amount, false, false);
    }

    @SuppressWarnings({ "UnusedDeclaration" })
    public Map<String, Object> inc(Query<?> query, Enum<?> field, double amount) {
        return inc(query, field, amount, false, false);
    }

    @SuppressWarnings("unused")
    public Map<String, Object> inc(Query<?> query, Enum<?> field, long amount) {
        return inc(query, field, amount, false, false);
    }

    @SuppressWarnings("unused")
    public Map<String, Object> inc(Query<?> query, Enum<?> field, int amount) {
        return inc(query, field, amount, false, false);
    }

    @SuppressWarnings("unused")
    public Map<String, Object> inc(Query<?> query, Enum<?> field, Number amount) {
        return inc(query, field, amount, false, false);
    }

    public Map<String, Object> inc(Query<?> query, Enum<?> field, double amount, boolean upsert, boolean multiple) {
        return inc(query, field.name(), amount, upsert, multiple);
    }

    public Map<String, Object> inc(Query<?> query, Enum<?> field, long amount, boolean upsert, boolean multiple) {
        return inc(query, field.name(), amount, upsert, multiple);
    }

    public Map<String, Object> inc(Query<?> query, Enum<?> field, int amount, boolean upsert, boolean multiple) {
        return inc(query, field.name(), amount, upsert, multiple);
    }

    public Map<String, Object> inc(Query<?> query, Enum<?> field, Number amount, boolean upsert, boolean multiple) {
        return inc(query, field.name(), amount, upsert, multiple);
    }

    public <T> Map<String, Object> inc(final Map<Enum, Number> fieldsToInc, final Query<T> matching,
     final boolean upsert, final boolean multiple, AsyncOperationCallback<T> callback) {
        Map<String, Number> toUpdate = new HashMap<>();

        for (Map.Entry<Enum, Number> e : fieldsToInc.entrySet()) {
            toUpdate.put(e.getKey().name(), e.getValue());
        }

        return inc(matching, toUpdate, upsert, multiple, callback);
    }

    public <T> Map<String, Object> inc(final Query<T> query, final Map<String, Number> toUpdate, final boolean upsert,
     final boolean multiple, AsyncOperationCallback<T> callback) {
        if (query == null) {
            throw new RuntimeException("Cannot update null!");
        }

        return getWriterForClass(query.getType()).inc(query, toUpdate, upsert, multiple, callback);
    }

    public <T> void dec(final Map<Enum, Number> fieldsToInc, final Query<T> matching, final boolean upsert,
     final boolean multiple, AsyncOperationCallback<T> callback) {
        Map<String, Number> toUpdate = new HashMap<>();

        for (Map.Entry<Enum, Number> e : fieldsToInc.entrySet()) {
            toUpdate.put(e.getKey().name(), e.getValue());
        }

        dec(matching, toUpdate, upsert, multiple, callback);
    }

    public <T> Map<String, Object> dec(final Query<T> query, final Map<String, Number> toUpdate, final boolean upsert,
     final boolean multiple, AsyncOperationCallback<T> callback) {
        if (query == null) {
            throw new RuntimeException("Cannot update null!");
        }

        return getWriterForClass(query.getType()).inc(query, toUpdate, upsert, multiple, callback);
    }

    public Map<String, Object> inc(final Query<?> query, final String name, final long amount, final boolean upsert,
     final boolean multiple) {
        return inc(query, name, amount, upsert, multiple, null);
    }

    public Map<String, Object> inc(final Query<?> query, final String name, final int amount, final boolean upsert,
     final boolean multiple) {
        return inc(query, name, amount, upsert, multiple, null);
    }

    public Map<String, Object> inc(final Query<?> query, final String name, final double amount, final boolean upsert,
     final boolean multiple) {
        return inc(query, name, amount, upsert, multiple, null);
    }

    public Map<String, Object> inc(final Query<?> query, final String name, final Number amount, final boolean upsert,
     final boolean multiple) {
        return inc(query, name, amount, upsert, multiple, null);
    }

    public <T> Map<String, Object> inc(final Query<T> query, final Enum<?> field, final long amount,
     final boolean upsert, final boolean multiple, final AsyncOperationCallback<T> callback) {
        return inc(query, field.name(), amount, upsert, multiple, callback);
    }

    public <T> Map<String, Object> inc(final Query<T> query, final String name, final long amount, final boolean upsert,
     final boolean multiple, final AsyncOperationCallback<T> callback) {
        if (query == null) {
            throw new RuntimeException("Cannot update null!");
        }

        return getWriterForClass(query.getType()).inc(query, name, amount, upsert, multiple, callback);
    }



    public <T> Map<String, Object> inc(final Query<T> query, final Enum<?> field, final int amount,
     final boolean upsert, final boolean multiple, final AsyncOperationCallback<T> callback) {
        return inc(query, field.name(), amount, upsert, multiple, callback);
    }

    public <T> Map<String, Object> inc(final Query<T> query, final String name, final int amount, final boolean upsert,
     final boolean multiple, final AsyncOperationCallback<T> callback) {
        if (query == null) {
            throw new RuntimeException("Cannot update null!");
        }

        return getWriterForClass(query.getType()).inc(query, name, amount, upsert, multiple, callback);
    }

    public <T> Map<String, Object> inc(final Query<T> query, final Enum<?> field, final double amount,
     final boolean upsert, final boolean multiple, final AsyncOperationCallback<T> callback) {
        return inc(query, field.name(), amount, upsert, multiple, callback);
    }

    public <T> Map<String, Object> inc(final Query<T> query, final String name, final double amount,
     final boolean upsert, final boolean multiple, final AsyncOperationCallback<T> callback) {
        if (query == null) {
            throw new RuntimeException("Cannot update null!");
        }

        return getWriterForClass(query.getType()).inc(query, name, amount, upsert, multiple, callback);
    }

    public <T> Map<String, Object> inc(final Query<T> query, final Enum<?> field, final Number amount,
     final boolean upsert, final boolean multiple, final AsyncOperationCallback<T> callback) {
        return inc(query, field.name(), amount, upsert, multiple, callback);
    }

    public <T> Map<String, Object> inc(final Query<T> query, final String name, final Number amount,
     final boolean upsert, final boolean multiple, final AsyncOperationCallback<T> callback) {
        if (query == null) {
            throw new RuntimeException("Cannot update null!");
        }

        return getWriterForClass(query.getType()).inc(query, name, amount, upsert, multiple, callback);
    }

    public <T> Map<String, Object> dec(final Query<T> query, final Enum<?> field, final long amount,
     final boolean upsert, final boolean multiple, final AsyncOperationCallback<T> callback) {
        return dec(query, field.name(), amount, upsert, multiple, callback);
    }

    public <T> Map<String, Object> dec(final Query<T> query, final String name, final long amount, final boolean upsert,
     final boolean multiple, final AsyncOperationCallback<T> callback) {
        if (query == null) {
            throw new RuntimeException("Cannot update null!");
        }

        return getWriterForClass(query.getType()).inc(query, name, -amount, upsert, multiple, callback);
    }

    public <T> Map<String, Object> dec(final Query<T> query, final Enum<?> field, final int amount,
     final boolean upsert, final boolean multiple, final AsyncOperationCallback<T> callback) {
        return dec(query, field.name(), amount, upsert, multiple, callback);
    }

    public <T> Map<String, Object> dec(final Query<T> query, final String name, final int amount, final boolean upsert,
     final boolean multiple, final AsyncOperationCallback<T> callback) {
        if (query == null) {
            throw new RuntimeException("Cannot update null!");
        }

        return getWriterForClass(query.getType()).inc(query, name, -amount, upsert, multiple, callback);
    }

    public <T> Map<String, Object> dec(final Query<T> query, final Enum<?> field, final double amount,
     final boolean upsert, final boolean multiple, final AsyncOperationCallback<T> callback) {
        return dec(query, field.name(), amount, upsert, multiple, callback);
    }

    public <T> Map<String, Object> dec(final Query<T> query, final String name, final double amount,
     final boolean upsert, final boolean multiple, final AsyncOperationCallback<T> callback) {
        if (query == null) {
            throw new RuntimeException("Cannot update null!");
        }

        return getWriterForClass(query.getType()).inc(query, name, -amount, upsert, multiple, callback);
    }

    public <T> Map<String, Object> dec(final Query<T> query, final Enum<?> field, final Number amount,
     final boolean upsert, final boolean multiple, final AsyncOperationCallback<T> callback) {
        return dec(query, field.name(), amount, upsert, multiple, callback);
    }

    public <T> Map<String, Object> dec(final Query<T> query, final String name, final Number amount,
     final boolean upsert, final boolean multiple, final AsyncOperationCallback<T> callback) {
        if (query == null) {
            throw new RuntimeException("Cannot update null!");
        }

        return getWriterForClass(query.getType()).inc(query, name, -amount.doubleValue(), upsert, multiple, callback);
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
     * decreasing a value of a given object calles
     * <code>inc(toDec,field,-amount);</code>
     */
    @SuppressWarnings("unused")
    public void dec(Object toDec, Enum<?> field, double amount) {
        dec(toDec, field.name(), amount);
    }

    public void dec(Object toDec, String field, double amount) {
        inc(toDec, field, -amount);
    }

    public void dec(Object toDec, Enum<?> field, int amount) {
        dec(toDec, field.name(), amount);
    }

    public void dec(Object toDec, String field, int amount) {
        inc(toDec, field, -amount);
    }

    @SuppressWarnings("unused")
    public void dec(Object toDec, Enum<?> field, long amount) {
        dec(toDec, field.name(), amount);
    }

    public void dec(Object toDec, String field, long amount) {
        inc(toDec, field, -amount);
    }

    @SuppressWarnings("unused")
    public void dec(Object toDec, Enum<?> field, Number amount) {
        dec(toDec, field.name(), amount);
    }

    public void dec(Object toDec, String field, Number amount) {
        inc(toDec, field, -amount.doubleValue());
    }

    public void inc(final Object toSet, final Enum<?> field, final long i) {
        inc(toSet, field.name(), i, null);
    }

    public void inc(final Object toSet, final String field, final long i) {
        inc(toSet, field, i, null);
    }

    public void inc(final Object toSet, final Enum<?> field, final int i) {
        inc(toSet, field.name(), i, null);
    }

    public void inc(final Object toSet, final String field, final int i) {
        inc(toSet, field, i, null);
    }

    public void inc(final Object toSet, final Enum<?> field, final double i) {
        inc(toSet, field.name(), i, null);
    }

    public void inc(final Object toSet, final String field, final double i) {
        inc(toSet, field, i, null);
    }

    @SuppressWarnings("unused")
    public void inc(final Object toSet, final Enum<?> field, final Number i) {
        inc(toSet, field.name(), i, null);
    }

    public void inc(final Object toSet, final String field, final Number i) {
        inc(toSet, field, i, null);
    }

    public <T> void inc(final T toSet, final Enum<?> field, final double i, final AsyncOperationCallback<T> callback) {
        inc(toSet, field.name(), i, callback);
    }

    public <T> void inc(final T toSet, final String field, final double i, final AsyncOperationCallback<T> callback) {
        inc(toSet, getMapper().getCollectionName(toSet.getClass()), field, i, callback);
    }

    public <T> void inc(final T toSet, final Enum<?> field, final int i, final AsyncOperationCallback<T> callback) {
        inc(toSet, field.name(), i, callback);
    }

    public <T> void inc(final T toSet, final String field, final int i, final AsyncOperationCallback<T> callback) {
        inc(toSet, getMapper().getCollectionName(toSet.getClass()), field, i, callback);
    }

    public <T> void inc(final T toSet, final Enum<?> field, final long i, final AsyncOperationCallback<T> callback) {
        inc(toSet, field.name(), i, callback);
    }

    public <T> void inc(final T toSet, final String field, final long i, final AsyncOperationCallback<T> callback) {
        inc(toSet, getMapper().getCollectionName(toSet.getClass()), field, i, callback);
    }

    public <T> void inc(final T toSet, final Enum<?> field, final Number i, final AsyncOperationCallback<T> callback) {
        inc(toSet, field.name(), i, callback);
    }

    public <T> void inc(final T toSet, final String field, final Number i, final AsyncOperationCallback<T> callback) {
        inc(toSet, getMapper().getCollectionName(toSet.getClass()), field, i, callback);
    }

    public <T> void inc(final T toSet, Enum<?> collection, final Enum<?> field, final double i,
     final AsyncOperationCallback<T> callback) {
        inc(toSet, field.name(), i, callback);
    }

    public <T> void inc(final T toSet, String collection, final Enum<?> field, final double i,
     final AsyncOperationCallback<T> callback) {
        inc(toSet, collection, field.name(), i, callback);
    }

    public <T> void inc(final T toSet, String collection, final String field, final double i,
     final AsyncOperationCallback<T> callback) {
        if (toSet == null) {
            throw new RuntimeException("Cannot update null!");
        }

        if (getId(toSet) == null) {
            log.debug("just storing object as it is new...");
            store(toSet);
            return;
        }

        getWriterForClass(toSet.getClass()).inc(toSet, collection, field, i, callback);
    }

    public <T> void inc(final T toSet, String collection, final Enum<?> field, final int i,
     final AsyncOperationCallback<T> callback) {
        inc(toSet, collection, field.name(), i, callback);
    }

    public <T> void inc(final T toSet, String collection, final String field, final int i,
     final AsyncOperationCallback<T> callback) {
        if (toSet == null) {
            throw new RuntimeException("Cannot update null!");
        }

        if (getId(toSet) == null) {
            log.debug("just storing object as it is new...");
            store(toSet);
            return;
        }

        getWriterForClass(toSet.getClass()).inc(toSet, collection, field, i, callback);
    }

    public <T> void inc(final T toSet, String collection, final Enum<?> field, final long i,
     final AsyncOperationCallback<T> callback) {
        inc(toSet, collection, field.name(), i, callback);
    }

    public <T> void inc(final T toSet, String collection, final String field, final long i,
     final AsyncOperationCallback<T> callback) {
        if (toSet == null) {
            throw new RuntimeException("Cannot update null!");
        }

        if (getId(toSet) == null) {
            log.debug("just storing object as it is new...");
            store(toSet);
            return;
        }

        getWriterForClass(toSet.getClass()).inc(toSet, collection, field, i, callback);
    }

    public <T> void inc(final T toSet, String collection, final Enum<?> field, final Number i,
     final AsyncOperationCallback<T> callback) {
        inc(toSet, collection, field.name(), i, callback);
    }

    public <T> void inc(final T toSet, String collection, final String field, final Number i,
     final AsyncOperationCallback<T> callback) {
        if (toSet == null) {
            throw new RuntimeException("Cannot update null!");
        }

        if (getId(toSet) == null) {
            log.debug("just storing object as it is new...");
            store(toSet);
            return;
        }

        getWriterForClass(toSet.getClass()).inc(toSet, collection, field, i, callback);
    }

    public <T> void remove(List<T> lst, String forceCollectionName) {
        remove(lst, forceCollectionName, (AsyncOperationCallback<T>) null);
    }

    /**
     * * Use remove instead, to make it more similar to mongosh
     **/
    @Deprecated
    public <T> void delete (List<T> lst, String forceCollectionName) {
        remove(lst, forceCollectionName, (AsyncOperationCallback<T>) null);
    }

    /**
     * * Use remove instead, to make it more similar to mongosh
     **/
    @Deprecated
    public <T> void delete (List<T> lst, String forceCollectionName, AsyncOperationCallback<T> callback) {
        remove(lst, forceCollectionName, callback);
    }

    public <T> void remove(List<T> lst, String forceCollectionName, AsyncOperationCallback<T> callback) {
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
    public <T> void delete (List<T> lst, AsyncOperationCallback<T> callback) {
        remove(lst, callback);
    }

    public <T> void remove(List<T> lst, AsyncOperationCallback<T> callback) {
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
     * updating an enty in DB without sending the whole entity only transfers the
     * fields to be
     * changed / set
     *
     * @param ent    - entity to update
     * @param fields - fields to use
     */
    public void updateUsingFields(final Object ent, final String... fields) {
        updateUsingFields(ent, null, fields);
    }

    public void updateUsingFields(final Object ent, final Enum... fieldNames) {
        updateUsingFields(ent, null, fieldNames);
    }

    public <T> void updateUsingFields(final T ent, AsyncOperationCallback<T> callback, final Enum... fields) {
        updateUsingFields(ent, getMapper().getCollectionName(ent.getClass()), callback, fields);
    }

    public <T> void updateUsingFields(final T ent, AsyncOperationCallback<T> callback, final String... fields) {
        updateUsingFields(ent, getMapper().getCollectionName(ent.getClass()), callback, fields);
    }

    public <T> void updateUsingFields(final T ent, String collection, AsyncOperationCallback<T> callback,
     final Enum... fields) {
        List<String> g = new ArrayList<>();

        for (Enum<?> e : fields) {
            g.add(e.name());
        }

        updateUsingFields(ent, collection, callback, g.toArray(new String[] {}));
    }

    @SuppressWarnings({ "UnusedParameters", "CommentedOutCode" })
    public <T> void updateUsingFields(final T ent, String collection, AsyncOperationCallback<T> callback,
     final String... fields) {
        if (ent == null) {
            return;
        }

        if (fields.length == 0) {
            return; // not doing an update - no change
        }

        for (int idx = 0; idx < fields.length; idx++) {
            fields[idx] = getARHelper().getMongoFieldName(ent.getClass(), fields[idx]);
        }

        getWriterForClass(ent.getClass()).updateUsingFields(ent, collection, null, fields);
    }

    public MorphiumObjectMapper getMapper() {
        return objectMapper;
    }

    public AnnotationAndReflectionHelper getARHelper() {
        if (annotationHelper == null) {
            return new AnnotationAndReflectionHelper(true);
        }

        return annotationHelper;
    }

    /**
     * careful this actually changes the parameter o!
     *
     * @param o   - object to read
     * @param <T> - tpye of the object
     * @return - entity
     */
    public <T> T reread(T o) {
        if (o == null) {
            return null;
        }

        return reread(o, objectMapper.getCollectionName(o.getClass()));
    }

    @SuppressWarnings("CommentedOutCode")
    public <T> T reread(T o, String collection) {
        if (o == null) {
            return null;
        }

        Object id = getId(o);

        if (id == null) {
            return null;
        }

        Map<String, Object> srch = new HashMap<>();
        srch.put("_id", id);

        try {
            MongoConnection con = morphiumDriver.getReadConnection(getReadPreferenceForClass(o.getClass()));
            FindCommand settings = new FindCommand(con).setDb(config.getDatabase()).setColl(collection)
             .setFilter(Doc.of(srch)).setBatchSize(1).setLimit(1);
            List<Map<String, Object>> found = settings.execute();
            con.release();
            // log.info("Reread took: "+settings.getMetaData().get("duration"));

            if (found != null && !found.isEmpty()) {
                Map<String, Object> dbo = found.get(0);
                Object fromDb = objectMapper.deserialize(o.getClass(), dbo);

                if (fromDb == null) {
                    throw new RuntimeException("could not reread from db");
                }

                @SuppressWarnings("unchecked")
                List<String> flds = annotationHelper.getFields(o.getClass());

                for (String f : flds) {
                    Field fld = annotationHelper.getField(o.getClass(), f);

                    if (java.lang.reflect.Modifier.isStatic(fld.getModifiers())) {
                        continue;
                    }

                    try {
                        fld.set(o, fld.get(fromDb));
                    } catch (IllegalAccessException e) {
                        log.error("Could not set Value: " + fld);
                    }
                }

                firePostLoadEvent(o);
            } else {
                return null;
            }
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }

        return o;
    }

    /// Event handling
    public void firePreStore(Object o, boolean isNew) {
        if (o == null) {
            return;
        }

        for (MorphiumStorageListener l : listeners) {
            // noinspection unchecked
            l.preStore(this, o, isNew);
        }

        annotationHelper.callLifecycleMethod(PreStore.class, o);
    }

    public void firePostStore(Object o, boolean isNew) {
        for (MorphiumStorageListener l : listeners) {
            // noinspection unchecked
            l.postStore(this, o, isNew);
        }

        annotationHelper.callLifecycleMethod(PostStore.class, o);
        // existing object => store last Access, if needed
    }

    public void firePreDrop(Class cls) {
        for (MorphiumStorageListener l : listeners) {
            // noinspection unchecked
            l.preDrop(this, cls);
        }
    }

    public <T> void firePostStore(Map<T, Boolean> isNew) {
        for (MorphiumStorageListener l : listeners) {
            // noinspection unchecked
            l.postStore(this, isNew);
        }

        for (Object o : isNew.keySet()) {
            annotationHelper.callLifecycleMethod(PostStore.class, o);
        }
    }

    public <T> void firePostRemove(List<T> toRemove) {
        for (MorphiumStorageListener l : listeners) {
            // noinspection unchecked
            l.postRemove(this, toRemove);
        }

        for (Object o : toRemove) {
            annotationHelper.callLifecycleMethod(PostRemove.class, o);
        }
    }

    public <T> void firePostLoad(List<T> loaded) {
        for (MorphiumStorageListener l : listeners) {
            // noinspection unchecked
            l.postLoad(this, loaded);
        }

        for (Object o : loaded) {
            annotationHelper.callLifecycleMethod(PostLoad.class, o);
        }
    }

    public void firePreStore(Map<Object, Boolean> isNew) {
        for (MorphiumStorageListener l : listeners) {
            // noinspection unchecked
            l.preStore(this, isNew);
        }

        for (Object o : isNew.keySet()) {
            annotationHelper.callLifecycleMethod(PreStore.class, o);
        }
    }

    public <T> void firePreRemove(List<T> lst) {
        for (MorphiumStorageListener l : listeners) {
            // noinspection unchecked
            l.preRemove(this, lst);
        }

        for (T o : lst) {
            annotationHelper.callLifecycleMethod(PreRemove.class, o);
        }
    }

    public void firePreRemove(Object o) {
        for (MorphiumStorageListener l : listeners) {
            // noinspection unchecked
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
     * de-references the given object of type T. If itself or any of its members is
     * a Proxy
     * (PartiallyUpdateableProxy or LazyDeReferencingProxy), it'll be removed and
     * replaced by the
     * real objet. This is not recursive, only the members here are de-referenced
     *
     * @param obj - the object to replact
     * @param <T> - type
     * @return the dereferenced object
     */
    @SuppressWarnings("unused")
    public <T> T deReference(T obj) {
        if (obj instanceof LazyDeReferencingProxy) {
            // noinspection unchecked
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
                    log.error("dereferencing of field " + fld.getName() + " failed", e);
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

    public ReadPreference getReadPreferenceForClass(Class<?> cls) {
        if (cls == null) {
            return config.getDefaultReadPreference();
        }

        DefaultReadPreference rp = annotationHelper.getAnnotationFromHierarchy(cls, DefaultReadPreference.class);

        if (rp == null) {
            return config.getDefaultReadPreference();
        }

        return rp.value().getPref();
    }

    public MorphiumBulkContext createBulkRequestContext(Class<?> type, boolean ordered) {
        return new MorphiumBulkContext(getDriver().createBulkContext(this, config.getDatabase(),
                getMapper().getCollectionName(type), ordered, getWriteConcernForClass(type)));
    }

    @SuppressWarnings("unused")
    public MorphiumBulkContext createBulkRequestContext(String collection, boolean ordered) {
        return new MorphiumBulkContext(
            getDriver().createBulkContext(this, config.getDatabase(), collection, ordered, null));
    }

    @SuppressWarnings({ "ConstantConditions", "CommentedOutCode" })
    public WriteConcern getWriteConcernForClass(Class<?> cls) {
        WriteSafety safety = annotationHelper.getAnnotationFromHierarchy(cls, WriteSafety.class); // cls.getAnnotation(WriteSafety.class);

        if (safety == null) {
            return null;
        }

        boolean j = safety.waitForJournalCommit();
        int w = safety.level().getValue();
        long timeout = safety.timeout();

        if (isReplicaSet() && w > 2) {
            de.caluga.morphium.replicaset.ReplicaSetStatus s = rsMonitor.getCurrentStatus();

            if (getConfig().isReplicaset() && s == null || s.getActiveNodes() == 0) {
                log.warn("ReplicaSet status is null or no node active! Assuming default write" + " concern");
                return null;
            }

            if (log.isDebugEnabled()) {
                log.debug("Active nodes now: " + s.getActiveNodes());
            }

            int activeNodes = s.getActiveNodes();

            if (activeNodes > 50) {
                activeNodes = 50;
            }

            long masterOpTime = 0;
            long maxReplLag = 0;

            for (ReplicaSetNode node : s.getMembers()) {
                if (node.getState() == 1) {
                    // Master
                    masterOpTime = node.getOptimeDate().getTime();
                }
            }

            for (ReplicaSetNode node : s.getMembers()) {
                if (node.getState() == 2) {
                    // Master
                    long tm = node.getOptimeDate().getTime() - masterOpTime;

                    if (maxReplLag < tm) {
                        maxReplLag = tm;
                    }
                }
            }

            if (timeout < 0) {
                // set timeout to replication lag * 3 - just to be sure
                if (log.isDebugEnabled()) {
                    log.debug("Setting timeout to replication lag*3");
                }

                if (maxReplLag < 0) {
                    maxReplLag = -maxReplLag;
                }

                if (maxReplLag == 0) {
                    maxReplLag = 1;
                }

                timeout = maxReplLag * 3000;

                if (maxReplLag > 10) {
                    log.warn("Warning: replication lag too high! timeout set to " + timeout + "ms - replication Lag is "
                     + maxReplLag + "s - write should take place in Background!");
                }
            }

            // Wait for all active slaves (-1 for the timeout bug)
            w = activeNodes;

            if (timeout > 0 && timeout < maxReplLag * 1000) {
                log.warn("Timeout is set smaller than replication lag - increasing to" + " replication_lag time * 3");
                timeout = maxReplLag * 3000;
            }
        }

        if (!isReplicaSet() && timeout < 0) {
            timeout = 0;
        }

        return WriteConcern.getWc(w, j, (int) timeout);
    }

    // public void addProfilingListener(ProfilingListener l) {
    // profilingListeners.add(l);
    // }
    //
    // public void removeProfilingListener(ProfilingListener l) {
    // profilingListeners.remove(l);
    // }

    // public void fireProfilingWriteEvent(Class type, Object data, long time,
    // boolean isNew, WriteAccessType wt) {
    // for (ProfilingListener l : profilingListeners) {
    // try {
    // l.writeAccess(type, data, time, isNew, wt);
    // } catch (Throwable e) {
    // log.error("Error during profiling: ", e);
    // }
    // }
    // }

    // public void fireProfilingReadEvent(Query q, long time, ReadAccessType t) {
    // for (ProfilingListener l : profilingListeners) {
    // try {
    // l.readAccess(q, time, t);
    // } catch (Throwable e) {
    // log.error("Error during profiling", e);
    // }
    // }
    // }

    /**
     * issues a remove command - no lifecycle methods calles, no drop, keeps all
     * indexec this way
     *
     * @param cls - class
     */
    public void clearCollection(Class<?> cls) {
        remove(createQueryFor(cls));
    }

    /**
     * issues a remove command - no lifecycle methods calles, no drop, keeps all
     * indexec this way
     * But uses sepcified collection name instead deriving name from class
     *
     * @param cls     - class
     * @param colName - CollectionName
     */
    public void clearCollection(Class<?> cls, String colName) {
        Query q = createQueryFor(cls);
        q.setCollectionName(colName);
        // noinspection unchecked
        remove(q);
    }

    /**
     * clears every single object in collection - reads ALL objects to do so this
     * way Lifecycle
     * methods can be called!
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
        // noinspection unchecked
        return (Query<T>)createQueryFor(type).setCollectionName(usingCollectionName);
    }

    public <T> Query<T> createQueryFor(Class<? extends T> type) {
        Query<T> q = new Query<>(this, type, getAsyncOperationsThreadPool());
        q.setAutoValuesEnabled(isAutoValuesEnabledForThread());
        return q;
    }

    public <T> QueryIterator<? extends T> iterateAll(Class<? extends T> type) {
        return iterateAll(type, null);
    }

    public <T> QueryIterator<? extends T> iterateAll(Class<? extends T> type, Map<?, ?> sort) {
        inc(StatisticKeys.READS);
        HashMap<String, Object> map = null;

        if (sort != null) {
            map = new LinkedHashMap<String, Object>();

            for (var e : sort.entrySet()) {
                map.put(e.getKey().toString(), e.getValue());
            }
        }

        var lst = createQueryFor(type).setSort(map).asIterable();
        return lst;
    }

    public <T> List<T> readAll(Class<? extends T> type, Map<?, ?> sort) {
        inc(StatisticKeys.READS);
        HashMap<String, Object> map = null;

        if (sort != null) {
            map = new LinkedHashMap<String, Object>();

            for (var e : sort.entrySet()) {
                map.put(e.getKey().toString(), e.getValue());
            }
        }

        List<T> lst = (List<T>)createQueryFor(type).setSort(map).asList();
        return lst;
    }

    public int getEstimatedCount(Class<?> type) {
        try {
            var stats = getCollStats(getMapper().getCollectionName(type));

            if (stats == null || stats.isEmpty()) {
                return 0;
            }

            return (Integer) stats.get("count");
        } catch (MorphiumDriverException e) {
            throw new RuntimeException(e);
        }
    }

    public long getCount(Class<?> type) {
        return createQueryFor(type).countAll();
    }

    /**
     * use query.asList() instead
     *
     * @param q
     * @param <T>
     * @return
     */
    @Deprecated
    public <T> List<T> find(Query<T> q) {
        return q.asList();
    }

    @SuppressWarnings("unused")
    public List<Object> distinct(Enum<?> key, Class c) {
        return distinct(key.name(), c);
    }

    /**
     * returns a distinct list of values of the given collection Attention: these
     * values are not
     * unmarshalled, you might get MongoMap<String,Object>s
     */
    @SuppressWarnings("unused")
    public List<Object> distinct(Enum<?> key, Query q) {
        return distinct(key.name(), q);
    }

    /**
     * returns a distinct list of values of the given collection Attention: these
     * values are not
     * unmarshalled, you might get MongoMap<String,Object>s
     */
    @SuppressWarnings("unchecked")
    public List<Object> distinct(String key, Query q) {
        MongoConnection con = null;

        try {
            con = getDriver().getPrimaryConnection(null);
            DistinctMongoCommand settings = new DistinctMongoCommand(con).setColl(q.getCollectionName())
             .setDb(config.getDatabase()).setQuery(Doc.of(q.toQueryObject())).setKey(key);

            if (q.getCollation() != null) {
                settings.setCollation(Doc.of(q.getCollation().toQueryObject()));
            }

            return settings.execute();
        } catch (MorphiumDriverException e) {
            throw new RuntimeException(e);
        } finally {
            getDriver().releaseConnection(con);
        }
    }

    public List<Object> distinct(String key, Class cls) {
        return distinct(key, cls, null);
    }

    public List<Object> distinct(String key, Class cls, Collation collation) {
        MongoConnection con = null;

        try {
            con = morphiumDriver.getPrimaryConnection(null);
            DistinctMongoCommand settings = new DistinctMongoCommand(con).setColl(objectMapper.getCollectionName(cls))
             .setDb(config.getDatabase()).setKey(getARHelper().getMongoFieldName(cls, key));

            if (collation != null) {
                settings.setCollation(Doc.of(collation.toQueryObject()));
            }

            return settings.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            getDriver().releaseConnection(con);
        }
    }

    @SuppressWarnings({ "unused" })
    public List<Object> distinct(String key, String collectionName) {
        return distinct(key, collectionName, null);
    }

    public List<Object> distinct(String key, String collectionName, Collation collation) {
        MongoConnection con = null;

        try {
            con = getDriver().getPrimaryConnection(null);
            DistinctMongoCommand cmd = new DistinctMongoCommand(con);
            cmd.setColl(collectionName).setDb(config.getDatabase()).setKey(key).setCollation(collation.toQueryObject());
            return cmd.execute();
            // return morphiumDriver.distinct(config.getDatabase(), collectionName, key, new
            // HashMap<>(), collation, config.getDefaultReadPreference());
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            getDriver().releaseConnection(con);
        }
    }

    public <T> T findById(Class<? extends T> type, Object id) {
        return findById(type, id, null);
    }

    public <T> void findById(Class<? extends T> type, Object id, String collection, AsyncOperationCallback callback) {
        // noinspection unchecked
        createQueryFor(type).setCollectionName(collection).f(getARHelper().getIdFieldName(type)).eq(id).get(callback);
    }

    public <T> T findById(Class<? extends T> type, Object id, String collection) {
        inc(StatisticKeys.READS);
        Cache c = getARHelper().getAnnotationFromHierarchy(type, Cache.class); // type.getAnnotation(Cache.class);
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

        @SuppressWarnings("unchecked")
        List<String> ls = annotationHelper.getFields(type, Id.class);

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

    public <T> List<T> findByField(Class<? extends T> cls, Enum<?> fld, Object val) {
        return findByField(cls, fld.name(), val);
    }

    public <T> boolean setAutoValues(T o) throws IllegalAccessException {
        Class type = getARHelper().getRealClass(o.getClass());
        Object id = getARHelper().getId(o);
        boolean aNew = id == null;

        if (!isAutoValuesEnabledForThread()) {
            return aNew;
        }

        // new object - need to store creation time
        // CreationTime ct = getARHelper().getAnnotationFromHierarchy(o.getClass(),
        // CreationTime.class);
        //
        // if (ct != null && config.isCheckForNew() && ct.checkForNew() && !aNew) {
        // // check if it is new or not
        // reread =
        // findById(getARHelper().getRealClass(o.getClhttps://tenor.com/bVYaH.gifass()),
        // getARHelper().getId(o)); // reread(o);
        // aNew = reread == null;
        // }
        //
        if (getARHelper().isAnnotationPresentInHierarchy(type, CreationTime.class)) {
            Object reread = null;
            CreationTime ct = getARHelper().getAnnotationFromHierarchy(o.getClass(), CreationTime.class);
            boolean checkForNew = Objects.requireNonNull(ct).checkForNew() && getConfig().isCheckForNew();
            @SuppressWarnings("unchecked")
            List<String> lst = getARHelper().getFields(type, CreationTime.class);

            if (id == null) {
                aNew = true;
            } else {
                if (checkForNew) {
                    reread = findById(type, id);
                    aNew = reread == null;

                    if (!aNew) {
                        if (lst.isEmpty()) {
                            log.error("Unable to copy @CreationTime - field missing");
                        } else {
                            for (String fld : lst) {
                                Field field = getARHelper().getField(type, fld);
                                Object value = field.get(reread);
                                field.set(o, value);
                            }
                        }
                    }
                } else {
                    aNew = false;
                }
            }

            if (aNew) {
                if (lst.isEmpty()) {
                    log.error("Unable to store creation time as @CreationTime for field is missing");
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
                            log.error("Could not set creation time", e);
                        }
                    }
                }
            }
        }

        if (getARHelper().isAnnotationPresentInHierarchy(type, LastChange.class)) {
            @SuppressWarnings("unchecked")
            List<String> lst = getARHelper().getFields(type, LastChange.class);

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
                        log.error("Could not set modification time", e);
                    }
                }
            } else {
                log.warn("Could not store last change - @LastChange missing!");
            }
        }

        return aNew;
    }

    /**
     * Erase cache entries for the given type. is being called after every store
     * depending on cache
     * settings!
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
        if (getARHelper().getId(o) == null) {
            config.getWriter().insert(o, collection, callback);
        } else {
            config.getWriter().store(o, collection, callback);
        }
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
        try {
            getWriterForClass(cls).dropCollection(cls, collection, callback);
        } catch (Exception e) {
            if (!e.getMessage().endsWith("Error: 26: ns not found")) {
                if (callback != null) {
                    callback.onOperationError(AsyncOperationType.WRITE, null, 0, e.getMessage(), e, null, cls);
                }
            } else {
                if (callback != null) {
                    callback.onOperationSucceeded(AsyncOperationType.WRITE, null, 0, null, null, cls);
                }
            }
        }
    }

    public void dropCollection(Class<?> cls) {
        try {
            getWriterForClass(cls).dropCollection(cls, getMapper().getCollectionName(cls), null);
        } catch (Exception e) {
            if (!e.getMessage().endsWith("ns not found")) {
                throw e;
            }
        }
    }

    ////////////////////////////////////////////////////////////////////////
    // __ .__ __. _______ __________ ___ _______ _______.
    // | | | \ | | | \ | ____\ \ / / | ____| / |
    // | | | \| | | .--. || |__ \ V / | |__ | (----`
    // | | | . ` | | | | || __| > < | __| \ \
    // | | | |\ | | '--' || |____ / . \ | |____.----) |
    // |__| |__| \__| |_______/ |_______/__/ \__\ |_______|_______/

    /**
     * can be called for autmatic index ensurance. Attention: might cause heavy load
     * on mongo
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

    @SuppressWarnings("ConstantConditions")
    public <T> void ensureIndicesFor(Class<T> type, String onCollection, AsyncOperationCallback<T> callback,
     MorphiumWriter wr) {
        if (annotationHelper.isAnnotationPresentInHierarchy(type, Index.class)) {
            // List<Annotation> collations =
            // annotationHelper.getAllAnnotationsFromHierachy(type,
            // de.caluga.morphium.annotations.Collation.class);
            @SuppressWarnings("unchecked")
            List<Annotation> lst = annotationHelper.getAllAnnotationsFromHierachy(type, Index.class);

            for (Annotation a : lst) {
                Index i = (Index) a;

                if (i.value().length > 0) {
                    List<Map<String, Object>> options = null;

                    if (i.options().length > 0) {
                        // options set
                        options = createIndexKeyMapFrom(i.options());
                    }

                    if (!i.locale().equals("")) {
                        Map<String, Object> collation = UtilsMap.of("locale", i.locale());
                        collation.put("alternate", i.alternate().mongoText);
                        collation.put("backwards", i.backwards());
                        collation.put("caseFirst", i.caseFirst().mongoText);
                        collation.put("caseLevel", i.caseLevel());
                        collation.put("maxVariable", i.maxVariable().mongoText);
                        collation.put("strength", i.strength().mongoValue);
                        options.add(UtilsMap.of("collation", collation));
                    }

                    List<Map<String, Object>> idx = createIndexKeyMapFrom(i.value());
                    int cnt = 0;

                    for (Map<String, Object> m : idx) {
                        Map<String, Object> optionsMap = null;

                        if (options != null && options.size() > cnt) {
                            optionsMap = options.get(cnt);
                        }

                        if (optionsMap != null && optionsMap.containsKey("")) {
                            optionsMap = null;
                        }

                        try {
                            wr.createIndex(type, onCollection, IndexDescription.fromMaps(m, optionsMap), callback);
                        } catch (Exception e) {
                            if (e.getMessage() != null
                             && e.getMessage().contains("Index already exists with a different name:")) {
                                log.debug("Index already exists");
                            } else {
                                throw e;
                            }
                        }

                        cnt++;
                    }
                }
            }
        }

        @SuppressWarnings("unchecked")
        List<String> flds = annotationHelper.getFields(type, Index.class);

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

                if (createIndexKeyMapFrom(i.options()) != null) {
                    optionsMap = createIndexKeyMapFrom(i.options()).get(0);
                }

                try {
                    wr.createIndex(type, onCollection, IndexDescription.fromMaps(idx, optionsMap), callback);
                } catch (Exception e) {
                    if (e.getMessage().contains("Index already exists with a different name:")) {
                        log.debug("Index already exists");
                    } else {
                        throw(e);
                    }
                }
            }
        }
    }

    @Deprecated
    public void ensureIndex(Class<?> cls, Map<String, Object> index) {
        try {
            getWriterForClass(cls).createIndex(cls, getMapper().getCollectionName(cls),
             IndexDescription.fromMaps(index, null), null);
        } catch (Exception e) {
            if (e.getMessage().contains("Index already exists with a different name:")) {
                log.debug("Index already exists");
            } else {
                throw(e);
            }
        }
    }

    @Deprecated
    @SuppressWarnings("unused")
    public void ensureIndex(Class<?> cls, String collection, Map<String, Object> index, Map<String, Object> options) {
        try {
            getWriterForClass(cls).createIndex(cls, collection, IndexDescription.fromMaps(index, options), null);
        } catch (Exception e) {
            if (e.getMessage().contains("Index already exists with a different name:")) {
                log.debug("Index already exists");
            } else {
                throw(e);
            }
        }
    }

    @Deprecated
    @SuppressWarnings("unused")
    public void ensureIndex(Class<?> cls, String collection, Map<String, Object> index) {
        try {
            getWriterForClass(cls).createIndex(cls, collection, IndexDescription.fromMaps(index, null), null);
        } catch (Exception e) {
            if (e.getMessage().contains("Index already exists with a different name:")) {
                log.debug("Index already exists");
            } else {
                throw(e);
            }
        }
    }

    /**
     * ensureIndex(CachedObject.class,"counter","-value");
     * ensureIndex(CachedObject.class,"counter:2d","-value); Similar to sorting
     *
     * @param cls    - class
     * @param fldStr - fields
     */
    @Deprecated
    public <T> void ensureIndex(Class<T> cls, AsyncOperationCallback<T> callback, Enum... fldStr) {
        ensureIndex(cls, getMapper().getCollectionName(cls), callback, fldStr);
    }

    @Deprecated
    public <T> void ensureIndex(Class<T> cls, String collection, AsyncOperationCallback<T> callback, Enum... fldStr) {
        Map<String, Object> m = new LinkedHashMap<>();

        for (Enum<?> e : fldStr) {
            String f = e.name();
            m.put(f, 1);
        }

        try {
            getWriterForClass(cls).createIndex(cls, collection, IndexDescription.fromMaps(m, null), callback);
        } catch (Exception e) {
            if (e.getMessage().contains("Index already exists with a different name:")) {
                log.debug("Index already exists");
            } else {
                throw(e);
            }
        }
    }

    @Deprecated
    public <T> void ensureIndex(Class<T> cls, AsyncOperationCallback<T> callback, String... fldStr) {
        ensureIndex(cls, getMapper().getCollectionName(cls), callback, fldStr);
    }

    @Deprecated
    public <T> void ensureIndex(Class<T> cls, String collection, AsyncOperationCallback<T> callback, String... fldStr) {
        List<Map<String, Object>> m = createIndexKeyMapFrom(fldStr);

        for (Map<String, Object> idx : m) {
            try {
                getWriterForClass(cls).createIndex(cls, collection, IndexDescription.fromMaps(idx, null), callback);
            } catch (Exception e) {
                if (e.getMessage().contains("Index already exists with a different name:")) {
                    log.debug("Index already exists");
                } else {
                    throw(e);
                }
            }
        }
    }

    @Deprecated
    public <T> void ensureIndex(Class<T> cls, Map<String, Object> index, AsyncOperationCallback<T> callback) {
        ensureIndex(cls, getMapper().getCollectionName(cls), index, callback);
    }

    @Deprecated
    public <T> void ensureIndex(Class<T> cls, String collection, Map<String, Object> index, Map<String, Object> options,
     AsyncOperationCallback<T> callback) {
        try {
            getWriterForClass(cls).createIndex(cls, collection, IndexDescription.fromMaps(index, options), callback);
        } catch (Exception e) {
            if (e.getMessage().contains("Index already exists with a different name:")) {
                log.debug("Index already exists");
            } else {
                throw(e);
            }
        }
    }

    @Deprecated
    public <T> void ensureIndex(Class<T> cls, String collection, Map<String, Object> index,
     AsyncOperationCallback<T> callback) {
        try {
            getWriterForClass(cls).createIndex(cls, collection, IndexDescription.fromMaps(index, null), callback);
        } catch (Exception e) {
            if (e.getMessage().contains("Index already exists with a different name:")) {
                log.debug("Index already exists");
            } else {
                throw(e);
            }
        }
    }

    public <T> void createIndex(Class<T> cls, String collection, IndexDescription index,
     AsyncOperationCallback<T> callback) {
        try {
            getWriterForClass(cls).createIndex(cls, collection, index, callback);
        } catch (Exception e) {
            if (e.getMessage().contains("Index already exists with a different name:")) {
                log.debug("Index already exists");
            } else {
                throw(e);
            }
        }
    }

    public List<IndexDescription> getIndexesFromMongo(Class cls) {
        return getIndexesFromMongo(getMapper().getCollectionName(cls));
    }

    public List<IndexDescription> getIndexesFromMongo(String collection) {
        MongoConnection readConnection = getDriver().getReadConnection(getConfig().getDefaultReadPreference());
        ListIndexesCommand cmd = new ListIndexesCommand(readConnection);
        cmd.setDb(getDatabase()).setColl(collection);

        try {
            return cmd.execute();
        } catch (MorphiumDriverException e) {
            throw new RuntimeException(e);
        } finally {
            getDriver().releaseConnection(readConnection);
        }
    }

    public <T> List<IndexDescription> getIndexesFromEntity(Class<T> type) {
        List<IndexDescription> ret = new ArrayList<>();

        if (annotationHelper.isAnnotationPresentInHierarchy(type, Index.class)) {
            // List<Annotation> collations =
            // annotationHelper.getAllAnnotationsFromHierachy(type,
            // de.caluga.morphium.annotations.Collation.class);
            // Indexes on class level, usually combined ones
            @SuppressWarnings("unchecked")
            List<Annotation> lst = annotationHelper.getAllAnnotationsFromHierachy(type, Index.class);

            for (Annotation a : lst) {
                Index i = (Index) a;

                if (i.value().length > 0) {
                    List<Map<String, Object>> options = null;

                    if (i.options().length > 0) {
                        // options set
                        options = createIndexKeyMapFrom(i.options());
                    }

                    if (!i.locale().equals("")) {
                        Map<String, Object> collation = UtilsMap.of("locale", i.locale());
                        collation.put("alternate", i.alternate().mongoText);
                        collation.put("backwards", i.backwards());
                        collation.put("caseFirst", i.caseFirst().mongoText);
                        collation.put("caseLevel", i.caseLevel());
                        collation.put("maxVariable", i.maxVariable().mongoText);
                        collation.put("strength", i.strength().mongoValue);
                        options.add(UtilsMap.of("collation", collation));
                    }

                    List<Map<String, Object>> idx = createIndexKeyMapFrom(i.value());
                    int cnt = 0;

                    for (Map<String, Object> m : idx) {
                        Map<String, Object> optionsMap = null;

                        if (options != null && options.size() > cnt) {
                            optionsMap = options.get(cnt);
                        }

                        if (optionsMap != null && optionsMap.containsKey("")) {
                            optionsMap = null;
                        }

                        if (optionsMap == null || !optionsMap.containsKey("weights")) {
                            if (m.containsValue("text")) {
                                if (optionsMap == null) {
                                    optionsMap = new HashMap<>();
                                }

                                var weights = Doc.of();

                                for (var k : m.keySet()) {
                                    weights.put(k, 1);
                                }

                                optionsMap.put("weights", weights);
                                optionsMap.put("textIndexVersion", 3);
                            }
                        }

                        ret.add(IndexDescription.fromMaps(m, optionsMap));
                        cnt++;
                    }
                }
            }
        }

        // Indexes on Field
        @SuppressWarnings("unchecked")
        List<String> flds = annotationHelper.getFields(type, Index.class);

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

                if (createIndexKeyMapFrom(i.options()) != null) {
                    optionsMap = createIndexKeyMapFrom(i.options()).get(0);
                }

                ret.add(IndexDescription.fromMaps(idx, optionsMap));
            }
        }

        return ret;
    }

    //////////////////////////////////////////////////////////////////////////////////

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

    public List<Map<String, Object>> createIndexKeyMapFrom(String[] fldStr) {
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
                                    if (value.equalsIgnoreCase("true") || value.equalsIgnoreCase("false")) {
                                        m.put(key, Boolean.parseBoolean(value));
                                    } else {
                                        m.put(key, value);
                                    }
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

    public String getDatabase() {
        return getConfig().getDatabase();
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

    public <T> void insert(T o, String collection, AsyncOperationCallback<T> callback) {
        if (o instanceof List) {
            insertList((List) o, collection, callback);
        } else if (o instanceof Collection) {
            // noinspection unchecked
            insertList(new ArrayList<>((Collection) o), collection, callback);
        } else {
            getWriterForClass(o.getClass()).insert(o, collection, callback);
        }
    }

    public <T> void insertList(List lst, String collection, AsyncOperationCallback<T> callback) {
        Map<Class<?>, MorphiumWriter> writers = new HashMap<>();
        Map<Class<?>, List<Object>> values = new HashMap<>();

        for (Object o : lst) {
            writers.putIfAbsent(o.getClass(), getWriterForClass(o.getClass()));
            values.putIfAbsent(o.getClass(), new ArrayList<>());
            values.get(o.getClass()).add(o);
        }

        for (Class cls : writers.keySet()) {
            try {
                // noinspection unchecked
                writers.get(cls).insert((List<T>) values.get(cls), collection, callback);
            } catch (Exception e) {
                // log.error("Write failed for " + cls.getName() + " lst of size " +
                // values.get(cls).size(), e);
                throw new RuntimeException(e);
            }
        }
    }

    public <T> void insertList(List arrayList, AsyncOperationCallback<T> callback) {
        insertList(arrayList, null, callback);
    }

    public <T> void insertList(List arrayList) {
        insertList(arrayList, null, null);
    }

    public <T> void insertAsync(T o, String collection){
        MongoConnection con = null;
        try {
            con = getDriver().getPrimaryConnection(getWriteConcernForClass(o.getClass()));
            InsertMongoCommand insert = new InsertMongoCommand(con);
            insert.setDb(getDatabase()).setColl(getMapper().getCollectionName(o.getClass()));
            insert.setDocuments(Arrays.asList(getMapper().serialize(o)));
            insert.executeAsync();
        } catch (Exception e) {
        } finally {
            if (con != null) {
                con.release();
            }
        }
    }

    /**
     * directly writes data to Mongo, no Mapper used use with caution, as caches are
     * not updated
     * also no checks for validity of fields, no references, no auto-variables no
     * async writing!
     *
     * @param type - type to write to (just for determining collection name)
     * @param lst  - list of entries to write
     * @return statistics
     * @throws MorphiumDriverException
     */
    public Map<String, Integer> storeMaps(Class type, List<Map<String, Object>> lst) throws MorphiumDriverException {
        return saveMaps(type, lst);
    }

    public Map<String, Integer> saveMaps(Class type, List<Map<String, Object>> lst) throws MorphiumDriverException {
        var con = getDriver().getPrimaryConnection(null);

        try {
            StoreMongoCommand settings = new StoreMongoCommand(con).setColl(getMapper().getCollectionName(type))
             .setDb(getDatabase()).setDocuments(lst);
            con.release();
            return null;
        } finally {
            con.release();
        }
    }

    /**
     * directly writes data to Mongo, no Mapper used use with caution, as caches are
     * not updated
     * also no checks for validity of fields, no references, no auto-variables no
     * async writing!
     *
     * @param collection - name of colleciton to write to
     * @param lst        - list of entries to write
     * @return statistics
     * @throws MorphiumDriverException
     */
    public Map<String, Integer> storeMaps(String collection, List<Map<String, Object>> lst)
    throws MorphiumDriverException {
        return saveMaps(collection, lst);
    }

    public Map<String, Integer> saveMaps(String collection, List<Map<String, Object>> lst)
    throws MorphiumDriverException {
        MongoConnection primaryConnection = getDriver().getPrimaryConnection(null);

        try {
            StoreMongoCommand settings = new StoreMongoCommand(primaryConnection).setColl(collection)
             .setDb(getDatabase()).setDocuments(lst);
            Map<String, Object> ret = settings.execute();
            Map<String, Integer> res = new HashMap<>();
            res.put("stored", (Integer) ret.get("stored"));
            return res;
        } finally {
            primaryConnection.release();
        }
    }

    /**
     * directly writes data to Mongo, no Mapper used use with caution, as caches are
     * not updated
     * also no checks for validity of fields, no references, no auto-variables no
     * async writing!
     *
     * @param collection collection name
     * @param m          data to write
     * @return statistics
     * @throws MorphiumDriverException
     */
    public Map<String, Integer> storeMap(String collection, Map<String, Object> m) throws MorphiumDriverException {
        return saveMap(collection, m);
    }

    public Map<String, Integer> saveMap(String collection, Map<String, Object> m) throws MorphiumDriverException {
        MongoConnection primaryConnection = getDriver().getPrimaryConnection(null);
        StoreMongoCommand settings = new StoreMongoCommand(primaryConnection).setDb(getDatabase()).setColl(collection)
         .setDocuments(Arrays.asList(Doc.of(m)));
        Map<String, Integer> res = new HashMap<>();
        Map<String, Object> result = settings.execute();
        getDriver().releaseConnection(primaryConnection);
        res.put("stored", (Integer) result.get("stored"));
        return res;
    }

    /**
     * directly writes data to Mongo, no Mapper used use with caution, as caches are
     * not updated
     * also no checks for validity of fields, no references, no auto-variables no
     * async writing!
     *
     * @param type - type, used to determine collection name
     * @param m    - data to write
     * @return statistics
     * @throws MorphiumDriverException
     */
    public Map<String, Object> storeMap(Class type, Map<String, Object> m) throws MorphiumDriverException {
        MongoConnection primaryConnection = getDriver().getPrimaryConnection(null);
        StoreMongoCommand settings = new StoreMongoCommand(primaryConnection).setDb(getDatabase())
         .setColl(getMapper().getCollectionName(type)).setDocuments(Arrays.asList(m));
        WriteConcern wc = getWriteConcernForClass(type);

        if (wc != null) {
            settings.setWriteConcern(wc.asMap());
        }

        var ret = settings.execute();
        getDriver().releaseConnection(primaryConnection);
        return ret;
    }

    /**
     * Stores a single Object. Clears the corresponding cache
     *
     * @param o - Object to store
     */
    public <T> void store(T o) {
        save(o);
    }

    public <T> void save(T o) {
        if (o instanceof List) {
            // noinspection unchecked
            saveList((List) o);
        } else if (o instanceof Collection) {
            // noinspection unchecked
            saveList(new ArrayList<>((Collection) o));
        } else {
            save(o, getMapper().getCollectionName(o.getClass()), null);
        }
    }

    public <T> void store(T o, final AsyncOperationCallback<T> callback) {
        save(o, callback);
    }

    public <T> void save(T o, final AsyncOperationCallback<T> callback) {
        if (o instanceof List) {
            // noinspection unchecked
            saveList((List) o, callback);
        } else if (o instanceof Collection) {
            // noinspection unchecked
            saveList(new ArrayList<>((Collection) o), callback);
        } else {
            save(o, getMapper().getCollectionName(o.getClass()), callback);
        }
    }

    public <T> void store(T o, String collection, final AsyncOperationCallback<T> callback) {
        save(o, collection, callback);
    }

    public <T> void save(T o, String collection, final AsyncOperationCallback<T> callback) {
        if (o instanceof List) {
            // noinspection unchecked
            saveList((List) o, collection, callback);
        } else if (o instanceof Collection) {
            // noinspection unchecked
            saveList(new ArrayList<>((Collection) o), collection, callback);
        }

        if (getARHelper().getId(o) != null) {
            getWriterForClass(o.getClass()).store(o, collection, callback);
        } else {
            getWriterForClass(o.getClass()).insert(o, collection, callback);
        }
    }

    public <T> void save(List<T> lst, AsyncOperationCallback<T> callback) {
        saveList(lst, callback);
    }

    public <T> void store(List<T> lst, AsyncOperationCallback<T> callback) {
        saveList(lst, callback);
    }

    /**
     * stores all elements of this list to the given collection
     *
     * @param lst        - list of objects to store
     * @param collection - collection name to use
     * @param <T>        - type of entity
     */
    public <T> void storeList(List<T> lst, String collection) {
        saveList(lst, collection, null);
    }

    public <T> void saveList(List<T> lst, String collection) {
        saveList(lst, collection, null);
    }

    public <T> void storeList(List<T> lst, String collection, AsyncOperationCallback<T> callback) {
        saveList(lst, collection, callback);
    }

    public <T> void saveList(List<T> lst, String collection, AsyncOperationCallback<T> callback) {
        Map<Class<?>, MorphiumWriter> writers = new HashMap<>();
        Map<Class<?>, List<Object>> values = new HashMap<>();

        for (Object o : lst) {
            writers.putIfAbsent(o.getClass(), getWriterForClass(o.getClass()));
            values.putIfAbsent(o.getClass(), new ArrayList<>());
            values.get(o.getClass()).add(o);
        }

        for (Class cls : writers.keySet()) {
            try {
                // noinspection unchecked
                writers.get(cls).store((List<T>) values.get(cls), collection, callback);
            } catch (Exception e) {
                log.error("Async Write failed for " + cls.getName() + " lst of size " + values.get(cls).size(), e);
                callback.onOperationError(AsyncOperationType.WRITE, null, 0, e.getMessage(), e, null, cls);
            }
        }
    }

    /**
     * sorts elements in this list, whether to store in background or directly.
     *
     * @param lst - all objects are sorted whether to store in BG or direclty. All
     *            objects are
     *            stored in their corresponding collection
     * @param <T> - type of list elements
     */
    public <T> void storeList(List<T> lst) {
        saveList(lst);
    }

    public <T> void saveList(List<T> lst) {
        saveList(lst, (AsyncOperationCallback<T>) null);
    }

    public <T> void storeList(Set<T> set) {
        saveList(set);
    }

    public <T> void saveList(Set<T> set) {
        saveList(new ArrayList<>(set), (AsyncOperationCallback<T>) null);
    }

    public <T> void storeList(List<T> lst, final AsyncOperationCallback<T> callback) {
        saveList(lst, callback);
    }

    public <T> void saveList(List<T> lst, final AsyncOperationCallback<T> callback) {
        // have to sort list - might have different objects
        List<T> storeDirect = new ArrayList<>();
        List<T> insertDirect = new ArrayList<>();

        if (isWriteBufferEnabledForThread()) {
            final List<T> storeInBg = new ArrayList<>();
            final List<T> insertInBg = new ArrayList<>();

            // checking permission - might take some time ;-(
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

    public <T> Map<String, Object> delete (Query<T> o) {
        return remove(o);
    }

    public <T> Map<String, Object> explainRemove(Query<T> q) {
        return config.getWriter().explainRemove(null, q);
    }

    public <T> Map<String, Object> remove(Query<T> o) {
        return getWriterForClass(o.getType()).remove(o, null);
    }

    public <T> Map<String, Object> delete (Query<T> o, final AsyncOperationCallback<T> callback) {
        return remove(o, callback);
    }

    public <T> Map<String, Object> remove(Query<T> o, final AsyncOperationCallback<T> callback) {
        return getWriterForClass(o.getType()).remove(o, callback);
    }

    @SuppressWarnings("unused")
    public <T> Map<String, Object> pushPull(boolean push, Query<T> query, String field, Object value, boolean upsert,
     boolean multiple, AsyncOperationCallback<T> callback) {
        return getWriterForClass(query.getType()).pushPull(push ? MorphiumStorageListener.UpdateTypes.PUSH : MorphiumStorageListener.UpdateTypes.PULL, query,
                field, value, upsert, multiple, callback);
    }

    @SuppressWarnings("unused")
    public <T> Map<String, Object> pushPullAll(boolean push, Query<T> query, String field, List<?> value,
     boolean upsert, boolean multiple, AsyncOperationCallback<T> callback) {
        return getWriterForClass(query.getType()).pushPullAll(push ? UpdateTypes.PUSH : UpdateTypes.PULL, query, field,
                value, upsert, multiple, callback);
    }

    @SuppressWarnings("unused")
    public <T> Map<String, Object> pullAll(Query<T> query, String field, List<?> value, boolean upsert,
     boolean multiple, AsyncOperationCallback<T> callback) {
        return getWriterForClass(query.getType()).pushPullAll(UpdateTypes.PULL, query, field, value, upsert, multiple,
                callback);
    }

    /**
     * deletes a single object from morphium backend. Clears cache
     *
     * @param o - entity
     */
    public void remove(Object o) {
        remove(o, getMapper().getCollectionName(o.getClass()));
    }

    public void delete (Object o) {
        remove(o, getMapper().getCollectionName(o.getClass()));
    }

    public void remove(Object o, String collection) {
        getWriterForClass(o.getClass()).remove(o, collection, null);
    }

    public void delete (Object o, String collection) {
        remove(o, collection);
    }

    public <T> void delete (final T lo, final AsyncOperationCallback<T> callback) {
        remove(lo, callback);
    }

    public <T> Map<String, Object> explainRemove(ExplainVerbosity verbosity, T o) {
        return config.getWriter().explainRemove(verbosity, o, getMapper().getCollectionName(o.getClass()));
    }

    public <T> void remove(final T lo, final AsyncOperationCallback<T> callback) {
        if (lo instanceof Query) {
            // noinspection unchecked
            remove((Query) lo, callback);
            return;
        }

        getWriterForClass(lo.getClass()).remove(lo, getMapper().getCollectionName(lo.getClass()), callback);
    }

    @SuppressWarnings("unused")
    public <T> void delete (final T lo, String collection, final AsyncOperationCallback<T> callback) {
        remove(lo, collection, callback);
    }

    public <T> void remove(final T lo, String collection, final AsyncOperationCallback<T> callback) {
        getWriterForClass(lo.getClass()).remove(lo, collection, callback);
    }

    public boolean exists(String db) throws MorphiumDriverException {
        MongoConnection primaryConnection = getDriver().getPrimaryConnection(null);
        ListDatabasesCommand cmd = new ListDatabasesCommand(primaryConnection);
        var dbs = cmd.getList();
        getDriver().releaseConnection(primaryConnection);

        // var ret = getDriver().runCommand("admin", Doc.of("listDatabasess", 1));
        for (Map<String, Object> l : dbs) {
            if (l.get("name").equals(db)) {
                return true;
            }
        }

        return false;
    }

    public boolean exists(String db, String col) throws MorphiumDriverException {
        return getDriver().listCollections(db, col).size() != 0;
    }

    public boolean exists(Class<?> cls) throws MorphiumDriverException {
        return exists(getDatabase(), getMapper().getCollectionName(cls));
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

    @SuppressWarnings("CommentedOutCode")
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
            } catch (Exception e) {
                e.printStackTrace();
            }

            morphiumDriver = null;
        }

        if (config != null) {
            if (config.getCache() != null) {
                config.getCache().resetCache();
                config.getCache().close();
            }

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

    ////////////////////////////////
    /////// MAP/REDUCE
    /////

    public <T> Map<String, Object> explainMapReduce(Class<? extends T> type, String map, String reduce,
     ExplainVerbosity verbose) throws MorphiumDriverException {
        MongoConnection readConnection = morphiumDriver.getReadConnection(getReadPreferenceForClass(type));

        try {
            MapReduceCommand mr = new MapReduceCommand(readConnection).setDb(getDatabase())
             .setColl(getMapper().getCollectionName(type)).setMap(map).setReduce(reduce);
            return mr.explain(verbose);
        } finally {
            getDriver().releaseConnection(readConnection);
        }
    }

    public <T> List<T> mapReduce(Class<? extends T> type, String map, String reduce) throws MorphiumDriverException {
        MongoConnection readConnection = morphiumDriver.getReadConnection(getReadPreferenceForClass(type));

        try {
            MapReduceCommand mr = new MapReduceCommand(readConnection).setDb(getDatabase())
             .setColl(getMapper().getCollectionName(type)).setMap(map).setReduce(reduce);
            List<Map<String, Object>> result = mr.execute();
            List<T> ret = new ArrayList<>();

            for (Map<String, Object> o : result) {
                ret.add(getMapper().deserialize(type, (Map<String, Object>) o.get("value")));
            }

            return ret;
        } finally {
            getDriver().releaseConnection(readConnection);
        }
    }

    /////////////////
    //// AGGREGATOR Support
    ///

    public <T, R> Aggregator<T, R> createAggregator(Class<? extends T> type, Class<? extends R> resultType) {
        Aggregator<T, R> aggregator = getDriver().createAggregator(this, type, resultType);
        return aggregator;
    }

    @SuppressWarnings("unchecked")
    public <T> T createLazyLoadedEntity(Class<? extends T> cls, Object id, String collectionName) {
        return (T) Enhancer.create(cls, new Class[] { Serializable.class },
                new LazyDeReferencingProxy(this, cls, id, collectionName));
    }

    public int getWriteBufferCount() {
        return config.getBufferedWriter().writeBufferCount() + config.getWriter().writeBufferCount()
               + config.getAsyncWriter().writeBufferCount();
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
                    Thread.sleep(100); // wait a moment, reduce load
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
        getDriver().startTransaction(false);
    }

    public MorphiumTransactionContext getTransaction() {
        return getDriver().getTransactionContext();
    }

    public void setTransaction(MorphiumTransactionContext ctx) {
        getDriver().setTransactionContext(ctx);
    }

    public void commitTransaction() {
        try {
            getDriver().commitTransaction();
        } catch (MorphiumDriverException e) {
            throw new RuntimeException(e);
        }
    }

    public void abortTransaction() {
        try {
            getDriver().abortTransaction();
        } catch (MorphiumDriverException e) {
            throw new RuntimeException(e);
        }
    }

    public <T> void watchAsync(String collectionName, boolean updateFull, ChangeStreamListener lst) {
        asyncOperationsThreadPool.execute(()->watch(collectionName, updateFull, null, lst));
    }

    public <T> void watchAsync(String collectionName, boolean updateFull, List<Map<String, Object>> pipeline,
     ChangeStreamListener lst) {
        asyncOperationsThreadPool.execute(()->watch(collectionName, updateFull, pipeline, lst));
    }

    public <T> void watchAsync(Class<T> entity, boolean updateFull, ChangeStreamListener lst) {
        asyncOperationsThreadPool.execute(()->watch(entity, updateFull, null, lst));
    }

    public <T> void watchAsync(Class<T> entity, boolean updateFull, List<Map<String, Object>> pipeline,
     ChangeStreamListener lst) {
        asyncOperationsThreadPool.execute(()->watch(entity, updateFull, pipeline, lst));
    }

    public <T> void watch(Class<T> entity, boolean updateFull, ChangeStreamListener lst) {
        watch(getMapper().getCollectionName(entity), updateFull, null, lst);
    }

    public <T> void watch(Class<T> entity, boolean updateFull, List<Map<String, Object>> pipeline,
     ChangeStreamListener lst) {
        watch(getMapper().getCollectionName(entity), updateFull, pipeline, lst);
    }

    public <T> void watch(String collectionName, boolean updateFull, ChangeStreamListener lst) {
        watch(collectionName, config.getMaxWaitTime(), updateFull, null, lst);
    }

    public <T> void watch(String collectionName, boolean updateFull, List<Map<String, Object>> pipeline,
     ChangeStreamListener lst) {
        watch(collectionName, config.getMaxWaitTime(), updateFull, pipeline, lst);
    }

    public <T> void watch(String collectionName, int maxWaitTime, boolean updateFull,
     List<Map<String, Object>> pipeline, ChangeStreamListener lst) {
        try {
            MongoConnection primaryConnection = getDriver().getPrimaryConnection(null);
            WatchCommand settings = new WatchCommand(primaryConnection).setDb(config.getDatabase())
             .setColl(collectionName).setMaxTimeMS(maxWaitTime).setPipeline(pipeline)
             .setFullDocument(updateFull ? WatchCommand.FullDocumentEnum.updateLookup
                : WatchCommand.FullDocumentEnum.defaultValue)
             .setCb(new DriverTailableIterationCallback() {
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
            getDriver().watch(settings);
            getDriver().releaseConnection(primaryConnection);
        } catch (MorphiumDriverException e) {
            throw new RuntimeException(e);
        }
    }

    private boolean processEvent(ChangeStreamListener lst, Map<String, Object> doc) {
        @SuppressWarnings("unchecked")
        Map<String, Object> obj = (Map<String, Object>) doc.get("fullDocument");
        doc.remove("fullDocument");
        ChangeStreamEvent evt = getMapper().deserialize(ChangeStreamEvent.class, doc);
        evt.setFullDocument(obj);
        return lst.incomingData(evt);
    }

    public <T> AtomicBoolean watchDbAsync(String dbName, boolean updateFull, List<Map<String, Object>> pipeline,
     ChangeStreamListener lst) {
        AtomicBoolean runningFlag = new AtomicBoolean(true);
        asyncOperationsThreadPool.execute(()->{
            watchDb(dbName, updateFull, null, runningFlag, lst);
            log.debug("watch async finished");
        });
        return runningFlag;
    }

    public <T> AtomicBoolean watchDbAsync(boolean updateFull, AtomicBoolean runningFlag, ChangeStreamListener lst) {
        return watchDbAsync(config.getDatabase(), updateFull, null, lst);
    }

    public <T> AtomicBoolean watchDbAsync(boolean updateFull, List<Map<String, Object>> pipeline,
     ChangeStreamListener lst) {
        return watchDbAsync(config.getDatabase(), updateFull, pipeline, lst);
    }

    public <T> void watchDb(boolean updateFull, ChangeStreamListener lst) {
        watchDb(getConfig().getDatabase(), updateFull, lst);
    }

    public <T> void watchDb(String dbName, boolean updateFull, ChangeStreamListener lst) {
        watchDb(dbName, getConfig().getMaxWaitTime(), updateFull, null, new AtomicBoolean(true), lst);
    }

    public <T> void watchDb(String dbName, boolean updateFull, List<Map<String, Object>> pipeline,
     AtomicBoolean runningFlag, ChangeStreamListener lst) {
        watchDb(dbName, getConfig().getMaxWaitTime(), updateFull, pipeline, runningFlag, lst);
    }

    public <T> void watchDb(String dbName, int maxWaitTime, boolean updateFull, List<Map<String, Object>> pipeline,
     AtomicBoolean runningFlag, ChangeStreamListener lst) {
        MongoConnection con = null;

        try {
            con = getDriver().getPrimaryConnection(null);
            WatchCommand cmd = new WatchCommand(con).setDb(dbName).setMaxTimeMS(maxWaitTime)
             .setFullDocument(updateFull ? WatchCommand.FullDocumentEnum.updateLookup
                : WatchCommand.FullDocumentEnum.defaultValue)
             .setPipeline(pipeline).setCb(new DriverTailableIterationCallback() {
                @Override
                public void incomingData(Map<String, Object> data, long dur) {
                    ChangeStreamEvent evt = getMapper().deserialize(ChangeStreamEvent.class, data);

                    if (!lst.incomingData(evt)) {
                        runningFlag.set(false);
                    }
                }
                @Override
                public boolean isContinued() {
                    return runningFlag.get();
                }
            });
            cmd.watch();
        } catch (MorphiumDriverException e) {
            throw new RuntimeException(e);
        } finally {
            if (con != null) {
                con.release();
            }
        }
    }

    public void reset() {
        MorphiumConfig cfg = getConfig();

        if (lst != null) {
            listeners.remove(lst);
        }

        close();
        setConfig(cfg);
        initializeAndConnect();
    }

    public List<IndexDescription> getMissingIndicesFor(Class<?> entity) throws MorphiumDriverException {
        // if (!morphiumDriver.exists(getDatabase(),
        // objectMapper.getCollectionName(entity)))
        // {
        // return new ArrayList<>(); //skip non existent collections
        // }
        return getMissingIndicesFor(entity, objectMapper.getCollectionName(entity));
    }

    @SuppressWarnings("ConstantConditions")
    public List<IndexDescription> getMissingIndicesFor(Class<?> entity, String collection)
    throws MorphiumDriverException {
        List<IndexDescription> missingIndexDef = new ArrayList<>();
        var fromMongo = getIndexesFromMongo(collection);
        var fromJava = getIndexesFromEntity(entity);

        for (IndexDescription idx : fromJava) {
            if (!fromMongo.contains(idx)) {
                missingIndexDef.add(idx);
            }
        }

        return missingIndexDef;
    }
    //
    // Index i = annotationHelper.getAnnotationFromClass(entity, Index.class);
    // ListIndexesCommand cmd = new
    // ListIndexesCommand(morphiumDriver).setColl(collection).setDb(getConfig().getDatabase());
    // List<IndexDescription> ind = cmd.execute();
    // List<Map<String, Object>> indices = new ArrayList<>();
    // for (IndexDescription m : ind) {
    // @SuppressWarnings("unchecked") Map<String, Object> indexKey = m.getKey();
    // indices.add(indexKey);
    // }
    // if (indices.size() > 0 && indices.get(0) == null) {
    // logger.error("Something is wrong! Index[0]==null");
    // }
    // if (i != null) {
    // if (i.value().length > 0) {
    // List<Map<String, Object>> options = null;
    //
    // if (i.options().length > 0) {
    // //options se
    // options = createIndexKeyMapFrom(i.options());
    // }
    // if (!i.locale().equals("")) {
    // Map<String, Object> collation = UtilsMap.of("locale", i.locale());
    // collation.put("alternate", i.alternate().mongoText);
    // collation.put("backwards", i.backwards());
    // collation.put("caseFirst", i.caseFirst().mongoText);
    // collation.put("caseLevel", i.caseLevel());
    // collation.put("maxVariable", i.maxVariable().mongoText);
    // collation.put("strength", i.strength().mongoValue);
    // options.add(UtilsMap.of("collation", collation));
    // }
    // List<Map<String, Object>> idx = createIndexKeyMapFrom(i.value());
    // if (!exists(config.getDatabase(), collection) || indices.size() == 0) {
    // logger.info("Collection '" + collection + "' for entity '" +
    // entity.getName() + "' does not exist.");
    // return null;
    // }
    // int cnt = 0;
    // for (Map<String, Object> m : idx) {
    // Map<String, Object> optionsMap = null;
    // if (options != null && options.size() > cnt) {
    // optionsMap = options.get(cnt);
    // if (optionsMap.containsKey("")) {//empty placeholder
    // optionsMap = null;
    // }
    // //TODO: check options
    // if (!indices.contains(m)) {
    // if (m.values().toArray()[0].equals("text")) {
    // //special text handling
    // logger.info("Handling text index");
    // boolean found = false;
    // for (IndexDescription indexDef : ind) {
    // if (indexDef.getTextIndexVersion() != null) {
    // found = true;
    // break;
    // }
    // }
    // if (!found) {
    // //assuming text index is missing
    // var ix = Doc.of("key", m);
    // if (optionsMap != null)
    // ix.putAll(optionsMap);
    // missingIndexDef.add(IndexDescription.fromMap(ix));
    // }
    // } else {
    // var ix = Doc.of("key", m);
    // if (optionsMap != null)
    // ix.putAll(optionsMap);
    // missingIndexDef.add(IndexDescription.fromMap(ix));
    // //missingIndexDef.add(IndexDescription.fromMap(m));
    // }
    // }
    // }
    //
    // cnt++;
    // }
    // }
    // }
    //
    // @SuppressWarnings("unchecked") List<String> flds =
    // annotationHelper.getFields(entity,
    // Index.class);
    // if (flds != null && !flds.isEmpty()) {
    //
    // for (String f : flds) {
    // i = annotationHelper.getField(entity, f).getAnnotation(Index.class);
    // Map<String, Object> key = new LinkedHashMap<>();
    // if (i.decrement()) {
    // key.put(f, -1);
    // } else {
    // key.put(f, 1);
    // }
    // Map<String, Object> optionsMap = null;
    // if (createIndexKeyMapFrom(i.options()) != null) {
    // optionsMap = createIndexKeyMapFrom(i.options()).get(0);
    //
    // }
    // if (!indices.contains(key)) {
    // //special handling for text indices
    // if (key.values().toArray()[0].equals("text")) {
    // logger.info("checking for text index...");
    // for (IndexDescription indexDef : ind) {
    // if (indexDef.getTextIndexVersion() != null) {
    // //assuming text index is missing
    // var idxMap = Doc.of("key", key);
    // if (optionsMap != null) idxMap.putAll(optionsMap);
    // missingIndexDef.add(IndexDescription.fromMap(idxMap));
    // }
    // }
    // } else {
    // missingIndexDef.add(IndexDescription.fromMaps(key,optionsMap));
    // }
    // }
    //
    // }
    // }
    // return missingIndexDef;
    // }

    /**
     * run trhough classpath, find all Entities, check indices returns a list of
     * Entities, whos
     * indices are missing or different
     */
    public Map<Class<?>, List<IndexDescription>> checkIndices() {
        return checkIndices(null);
    }

    public Map<Class<?>, Map<String, Integer>> checkCapped() {
        Map<Class<?>, Map<String, Integer>> uncappedCollections = new HashMap<>();

        try (ScanResult scanResult = new ClassGraph().enableAnnotationInfo().enableClassInfo().scan()) {
            ClassInfoList entities = scanResult.getClassesWithAnnotation(Capped.class.getName());

            for (String cn : entities.getNames()) {
                // ClassInfo ci = scanResult.getClassInfo(cn);
                try {
                    if (cn.startsWith("sun.")) {
                        continue;
                    }

                    if (cn.startsWith("com.sun.")) {
                        continue;
                    }

                    if (cn.startsWith("org.assertj.")) {
                        continue;
                    }

                    if (cn.startsWith("javax.")) {
                        continue;
                    }

                    log.debug("Cap-Checking " + cn);
                    Class<?> entity = Class.forName(cn);

                    if (annotationHelper.getAnnotationFromHierarchy(entity, Entity.class) == null) {
                        continue;
                    }

                    if (annotationHelper.isAnnotationPresentInHierarchy(entity, Capped.class)) {
                        if (!morphiumDriver.isCapped(getConfig().getDatabase(),
                         getMapper().getCollectionName(entity))) {
                            Capped capped = annotationHelper.getAnnotationFromClass(entity, Capped.class);
                            uncappedCollections.put(entity,
                             UtilsMap.of("max", capped.maxEntries(), "size", capped.maxSize()));
                        }
                    }
                } catch (Exception e) {
                    log.error("error", e);
                }
            }
        }

        return uncappedCollections;
    }

    @SuppressWarnings("CommentedOutCode")
    public Map<Class<?>, List<IndexDescription>> checkIndices(ClassInfoList.ClassInfoFilter filter) {
        Map<Class<?>, List<IndexDescription>> missingIndicesByClass = new HashMap<>();

        // initializing type IDs
        try (ScanResult scanResult = new ClassGraph()
         // .verbose() // Enable verbose logging
         .enableAnnotationInfo()
         // .enableFieldInfo()
         .enableClassInfo()    // Scan classes, methods, fields, annotations
         .scan()) {
            ClassInfoList entities = scanResult.getClassesWithAnnotation(Entity.class.getName());

            if (filter != null) {
                entities = entities.filter(filter);
            }

            for (String cn : entities.getNames()) {
                // ClassInfo ci = scanResult.getClassInfo(cn);
                try {
                    // if (param.getName().equals("index"))
                    // logger.info("Class " + cn + " Param " + param.getName() + " = " +
                    // param.getValue());
                    if (cn.startsWith("sun.")) {
                        continue;
                    }

                    if (cn.startsWith("com.sun.")) {
                        continue;
                    }

                    if (cn.startsWith("org.assertj.")) {
                        continue;
                    }

                    if (cn.startsWith("javax.")) {
                        continue;
                    }

                    // logger.info("Checking "+cn);
                    Class<?> entity = Class.forName(cn);

                    if (annotationHelper.getAnnotationFromHierarchy(entity, Entity.class) == null) {
                        continue;
                    }

                    if (exists(getDatabase(), getMapper().getCollectionName(entity))) {
                        List<IndexDescription> missing = getMissingIndicesFor(entity);

                        if (missing != null && !missing.isEmpty()) {
                            missingIndicesByClass.put(entity, missing);
                        }
                    } else {
                        // does not exists => create
                        missingIndicesByClass.put(entity, getIndexesFromEntity(entity));
                    }
                } catch (Throwable e) {
                    // swallow
                    if (!e.getMessage().contains("Error: 26 - ns does not exist:")) {
                        log.error("Could not check indices for " + cn, e);
                    }
                }
            }
        } catch (Exception e) {
            log.error("error", e);
        }

        return missingIndicesByClass;
    }

    @Override
    public String toString() {
        return "Morphium: Driver " + getDriver().getName() + " - " + String.join(",", getConfig().getHostSeed());
    }

    //
    // public void addCommandListener(CommandListener cmd) {
    // morphiumDriver.addCommandListener(cmd);
    // }
    //
    // public void removeCommandListener(CommandListener cmd) {
    // morphiumDriver.removeCommandListener(cmd);
    // }
    //
    // public void addClusterListener(ClusterListener cl) {
    // morphiumDriver.addClusterListener(cl);
    // }
    //
    // public void removeClusterListener(ClusterListener cl) {
    // morphiumDriver.removeClusterListener(cl);
    // }
    //
    // public void addConnectionPoolListener(ConnectionPoolListener cpl) {
    // morphiumDriver.addConnectionPoolListener(cpl);
    // }
    //
    // public void removeConnectionPoolListener(ConnectionPoolListener cpl) {
    // morphiumDriver.removeConnectionPoolListener(cpl);
    // }
}
