/*
 */
package de.caluga.morphium;

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
import de.caluga.morphium.config.CollectionCheckSettings.IndexCheck;
import de.caluga.morphium.driver.*;
import de.caluga.morphium.driver.commands.*;
import de.caluga.morphium.driver.commands.ExplainCommand.ExplainVerbosity;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import de.caluga.morphium.driver.wire.MongoConnection;
import de.caluga.morphium.driver.wire.SingleMongoConnectDriver;
import de.caluga.morphium.driver.wireprotocol.OpCompressed;
import de.caluga.morphium.encryption.EncryptionKeyProvider;
import de.caluga.morphium.encryption.ValueEncryptionProvider;
import de.caluga.morphium.messaging.MorphiumMessaging;
import de.caluga.morphium.messaging.Msg;
import de.caluga.morphium.config.MessagingSettings;
import de.caluga.morphium.objectmapping.MorphiumObjectMapper;
import de.caluga.morphium.query.Query;
import de.caluga.morphium.query.QueryIterator;
import de.caluga.morphium.validation.JavaxValidationStorageListener;
import de.caluga.morphium.writer.BufferedMorphiumWriterImpl;
import de.caluga.morphium.writer.MorphiumWriter;
import de.caluga.morphium.writer.MorphiumWriterImpl;
import io.github.classgraph.ClassGraph;
import io.github.classgraph.ClassInfoList;
import io.github.classgraph.ScanResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cglib.proxy.Enhancer;

import java.io.Serializable;
import java.lang.annotation.Annotation;
import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

/**
 * This is the single access point for accessing MongoDB. This conains a ton of convenience Methods
 * in a nutshell: all write access goes from Morphium->correct Writer
 * all read access done via query.
 *
 * @author stephan
 */
@SuppressWarnings({"WeakerAccess", "unused", "unchecked", "CommentedOutCode"})
public class Morphium extends MorphiumBase implements AutoCloseable {

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
    private List < MorphiumStorageListener<? >> listeners = new CopyOnWriteArrayList<>();
    private AnnotationAndReflectionHelper annotationHelper;
    private MorphiumObjectMapper objectMapper;
    private EncryptionKeyProvider encryptionKeyProvider;
    private ThreadPoolExecutor asyncOperationsThreadPool;
    private MorphiumDriver morphiumDriver;

    private JavaxValidationStorageListener lst;
    private ValueEncryptionProvider valueEncryptionProvider;
    private String CREDENTIAL_ENCRYPT_KEY_NAME;

    private Class <? extends MorphiumMessaging > messagingClass;

    private static Vector<Morphium> instances = new Vector<>();
    private static AtomicInteger maxInstances = new AtomicInteger();
    // Map to track InMemoryDriver instances by database name for sharing within a test scope
    private static final java.util.concurrent.ConcurrentHashMap<String, MorphiumDriver> inMemoryDriversByDatabase = new java.util.concurrent.ConcurrentHashMap<>();
    private static final java.util.concurrent.ConcurrentHashMap<String, java.util.concurrent.atomic.AtomicInteger> inMemoryDriverRefCounts = new java.util.concurrent.ConcurrentHashMap<>();
    public Morphium() {
        // profilingListeners = new CopyOnWriteArrayList<>();
        instances.add(this);

        if (maxInstances.get() < instances.size()) {
            maxInstances.set(instances.size());
            StatisticValue v = new StatisticValue();
            v.set(maxInstances.get());
            stats.putIfAbsent(StatisticKeys.INSTANCE_COUNT, v);
        }
    }

    public Morphium(String host, String db) {
        this();
        MorphiumConfig cfg = new MorphiumConfig(db, 100, 5000, 5000);
        cfg.clusterSettings().addHostToSeed(host);
        // cfg.setReplicasetMonitoring(false);
        setConfig(cfg);
    }

    public Morphium(String host, int port, String db) {
        this();
        MorphiumConfig cfg = new MorphiumConfig(db, 100, 5000, 5000);
        cfg.clusterSettings().addHostToSeed(host, port);
        setConfig(cfg);
    }

    public List<Morphium> getAlternativeMorphiums() {
        List<Morphium> alternativeInstances = new ArrayList<>();

        for (Morphium m : instances) {
            if (m == this) continue;

            alternativeInstances.add(m);
        }

        return alternativeInstances;
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
        annotationHelper = new AnnotationAndReflectionHelper(cfg.objectMappingSettings().isCamelCaseConversionEnabled());
        // BlockingQueue<Runnable> queue = new LinkedBlockingQueue<Runnable>() {
        //     private static final long serialVersionUID = -6903933921423432194L;
        //     @Override
        //     public boolean offer(Runnable e) {
        //         int poolSize = asyncOperationsThreadPool.getPoolSize();
        //         int maximumPoolSize = asyncOperationsThreadPool.getMaximumPoolSize();
        //
        //         if (poolSize >= maximumPoolSize || poolSize > asyncOperationsThreadPool.getActiveCount()) {
        //             return super.offer(e);
        //         } else {
        //             return false;
        //         }
        //     }
        // };
        asyncOperationsThreadPool = new ThreadPoolExecutor(
                        getConfig().threadPoolSettings().getThreadPoolAsyncOpCoreSize(),
                        getConfig().threadPoolSettings().getThreadPoolAsyncOpMaxSize(),
                        getConfig().threadPoolSettings().getThreadPoolAsyncOpKeepAliveTime(),
                        TimeUnit.MILLISECONDS,
                        new LinkedBlockingQueue<>(),
                        Thread.ofVirtual().name("asyncOp-", 0).factory()
        );
        // new ThreadPoolExecutor(getConfig().getThreadPoolAsyncOpCoreSize(), getConfig().getThreadPoolAsyncOpMaxSize(), getConfig().getThreadPoolAsyncOpKeepAliveTime(),
        //     TimeUnit.MILLISECONDS, queue);
        // asyncOperationsThreadPool.setRejectedExecutionHandler((r, executor)-> {
        //     try {
        //         /*
        //          * This does the actual put into the queue. Once the max threads
        //          * have been reached, the tasks will then queue up.
        //          */
        //         executor.getQueue().put(r);
        //     } catch (InterruptedException e) {
        //         Thread.currentThread().interrupt();
        //     }
        // });
        // asyncOperationsThreadPool.setThreadFactory(new ThreadFactory() {
        //     private final AtomicInteger num = new AtomicInteger(1);
        //     @Override
        //     public Thread newThread(Runnable r) {
        //         Thread ret = new Thread(r, "asyncOp " + num);
        //         num.set(num.get() + 1);
        //         ret.setDaemon(true);
        //         return ret;
        //     }
        // });
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

        if (messagingClass == null) {
            if (getConfig().messagingSettings().getMessagingImplementation() == null) {
                messagingClass = de.caluga.morphium.messaging.SingleCollectionMessaging.class;
                log.info("Using Messaging SingleCollectionMessaging");
            } else {
                try (ScanResult scanResult = new ClassGraph().enableAllInfo() // Scan classes, methods, fields, annotations
                    .scan()) {
                    ClassInfoList entities = scanResult.getClassesWithAnnotation(Messaging.class.getName());

                    if (log.isDebugEnabled()) {
                        log.debug("Found {} messaging implementations in classpath", entities.size());
                    }

                    for (String cn : entities.getNames()) {
                        try {
                            Class c = Class.forName(cn);
                            var ann = (Messaging)c.getAnnotation(Messaging.class);
                            String name = ann.name();

                            if (name.equals(config.messagingSettings().getMessagingImplementation())) {
                                log.info("Using Messaging {}: {}", name, ann.description());
                                messagingClass = c;
                                break;
                            }
                        } catch (Exception e) {
                            log.error("Error handling messaging implementation {}", cn, e);
                        }
                    }
                    if (messagingClass == null) {
                        log.error("Could not find messaging {}, using default", config.messagingSettings().getMessagingImplementation());
                        messagingClass = de.caluga.morphium.messaging.SingleCollectionMessaging.class;
                    }
                } catch (Exception e) {
                    log.error("Could not scan for Messaging implementations", e);
                }
            }
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

            if (getConfig().driverSettings().getDriverName() == null) {
                getConfig().driverSettings().setDriverName(SingleMongoConnectDriver.driverName);
                morphiumDriver = new SingleMongoConnectDriver();
            } else {
                try (ScanResult scanResult = new ClassGraph().enableAllInfo() // Scan classes, methods, fields, annotations
                    .scan()) {
                    ClassInfoList entities = scanResult.getClassesWithAnnotation(Driver.class.getName());

                    if (log.isDebugEnabled()) {
                        log.debug("Found " + entities.size() + " drivers in classpath");
                    }

                    String driverName = getConfig().driverSettings().getDriverName();

                    for (String cn : entities.getNames()) {
                        try {
                            @SuppressWarnings("rawtypes")
                            Class c = Class.forName(cn);

                            if (Modifier.isAbstract(c.getModifiers())) {
                                continue;
                            }

                            var driverAnnotation = (Driver)c.getAnnotation(Driver.class);

                            if (driverAnnotation.name().equals(driverName)) {
                                log.debug("Found driverName: {} - {} " + driverName, driverAnnotation.description());

                                // Special handling for InMemoryDriver: share instances within same database
                                // This allows multiple Morphium instances in a test to share the same in-memory database
                                // Different tests (different database names) get different driver instances
                                if (driverAnnotation.name().equals(InMemoryDriver.driverName)) {
                                    String dbName = getConfig().connectionSettings().getDatabase();
                                    morphiumDriver = inMemoryDriversByDatabase.computeIfAbsent(dbName, k -> {
                                        try {
                                            MorphiumDriver newDriver = (MorphiumDriver) c.getDeclaredConstructor().newInstance();
                                            log.info("Created new InMemoryDriver for database '{}' (driver hashcode: {})", dbName, System.identityHashCode(newDriver));
                                            return newDriver;
                                        } catch (Exception e) {
                                            throw new RuntimeException("Failed to create InMemoryDriver", e);
                                        }
                                    });
                                    // Increment reference count for this shared driver
                                    inMemoryDriverRefCounts.computeIfAbsent(dbName, k -> new java.util.concurrent.atomic.AtomicInteger(0)).incrementAndGet();
                                    log.info("Using InMemoryDriver for database '{}' (driver hashcode: {}, refCount: {})",
                                             dbName, System.identityHashCode(morphiumDriver), inMemoryDriverRefCounts.get(dbName).get());
                                } else {
                                    morphiumDriver = (MorphiumDriver) c.getDeclaredConstructor().newInstance();
                                }
                            }

                            // var flds = annotationHelper.getAllFields(c);
                            // for (var f : flds) {
                            //     if (Modifier.isStatic(f.getModifiers()) && Modifier.isFinal(f.getModifiers()) && Modifier.isPublic(f.getModifiers()) && f.getName().equals("driverName")) {
                            //         String dn = (String) f.get(c);
                            //         log.debug("Found driverName: " + dn);
                            //         if (dn.equals(getConfig().driverSettings().getDriverName())) {
                            //             morphiumDriver = (MorphiumDriver) c.getDeclaredConstructor().newInstance();
                            //         }
                            //         break;
                            //     }
                            // }
                        } catch (Throwable e) {
                            log.error("Could not load driver " + getConfig().driverSettings().getDriverName(), e);
                        }
                    }

                    if (morphiumDriver == null) {
                        morphiumDriver = new SingleMongoConnectDriver(); // default
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }

            morphiumDriver.setConnectionTimeout(getConfig().connectionSettings().getConnectionTimeout());
            morphiumDriver.setMaxConnections(getConfig().connectionSettings().getMaxConnections());
            morphiumDriver.setMinConnections(getConfig().connectionSettings().getMinConnections());
            morphiumDriver.setReadTimeout(getConfig().driverSettings().getReadTimeout());
            morphiumDriver.setRetryReads(getConfig().driverSettings().isRetryReads());
            morphiumDriver.setRetryWrites(getConfig().driverSettings().isRetryWrites());
            morphiumDriver.setHeartbeatFrequency(getConfig().driverSettings().getHeartbeatFrequency());
            morphiumDriver.setMaxConnectionIdleTime(getConfig().driverSettings().getMaxConnectionIdleTime());
            morphiumDriver.setMaxConnectionLifetime(getConfig().driverSettings().getMaxConnectionLifeTime());
            morphiumDriver.setMaxWaitTime(getConfig().connectionSettings().getMaxWaitTime());
            morphiumDriver.setIdleSleepTime(getConfig().driverSettings().getIdleSleepTime());
            morphiumDriver.setCompression(getConfig().driverSettings().getCompressionType().getCode());
            morphiumDriver.setDefaultBatchSize(getConfig().driverSettings().getCursorBatchSize());
            morphiumDriver.setServerSelectionTimeout(getConfig().driverSettings().getServerSelectionTimeout());

            if (getConfig().clusterSettings().getHostSeed().isEmpty() && !(morphiumDriver instanceof InMemoryDriver)) {
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
                encryptionKeyProvider = getConfig().encryptionSettings().getEncryptionKeyProviderClass().getDeclaredConstructor().newInstance();

                if (getConfig().encryptionSettings().getCredentialsEncryptionKey() != null) {
                    encryptionKeyProvider.setEncryptionKey(CREDENTIAL_ENCRYPT_KEY_NAME, getConfig().encryptionSettings().getCredentialsEncryptionKey().getBytes(StandardCharsets.UTF_8));
                }

                if (getConfig().encryptionSettings().getCredentialsDecryptionKey() != null) {
                    encryptionKeyProvider.setDecryptionKey(CREDENTIAL_ENCRYPT_KEY_NAME, getConfig().encryptionSettings().getCredentialsDecryptionKey().getBytes(StandardCharsets.UTF_8));
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

            try {
                valueEncryptionProvider = getConfig().encryptionSettings().getValueEncryptionProviderClass().getDeclaredConstructor().newInstance();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

            if (getConfig().authSettings().getMongoLogin() != null && getConfig().authSettings().getMongoPassword() != null) {
                if (getConfig().encryptionSettings().getCredentialsEncrypted() != null && getConfig().encryptionSettings().getCredentialsEncrypted()) {
                    var key = getEncryptionKeyProvider().getDecryptionKey(CREDENTIAL_ENCRYPT_KEY_NAME);
                    valueEncryptionProvider.setEncryptionKey(getEncryptionKeyProvider().getEncryptionKey(CREDENTIAL_ENCRYPT_KEY_NAME));
                    valueEncryptionProvider.setDecryptionKey(getEncryptionKeyProvider().getDecryptionKey(CREDENTIAL_ENCRYPT_KEY_NAME));

                    if (key == null) {
                        throw new RuntimeException(("Cannot decrypt - no key for mongodb_crendentials set!"));
                    }

                    try {
                        var user = new String(getValueEncrpytionProvider().decrypt(Base64.getDecoder().decode(getConfig().authSettings().getMongoLogin())));
                        var passwd = new String(getValueEncrpytionProvider().decrypt(Base64.getDecoder().decode(getConfig().authSettings().getMongoPassword())));
                        var authdb = "admin";

                        if (getConfig().authSettings().getMongoAuthDb() != null) {
                            authdb = new String(getValueEncrpytionProvider().decrypt(Base64.getDecoder().decode(getConfig().authSettings().getMongoAuthDb())));
                        }

                        morphiumDriver.setCredentials(authdb, user, passwd);
                    } catch (Exception e) {
                        throw new IllegalArgumentException("Credential decryption failed", e);
                    }
                } else {
                    morphiumDriver.setCredentials(getConfig().authSettings().getMongoAuthDb(), getConfig().authSettings().getMongoLogin(), getConfig().authSettings().getMongoPassword());
                }
            }

            String[] seed = new String[getConfig().clusterSettings().getHostSeed().size()];

            for (int i = 0; i < seed.length; i++) {
                seed[i] = getConfig().clusterSettings().getHostSeed().get(i);
            }

            morphiumDriver.setHostSeed(seed);

            try {
                morphiumDriver.connect(getConfig().clusterSettings().getRequiredReplicaSetName());
            } catch (MorphiumDriverException e) {
                throw new RuntimeException(e);
            }
        }

        if (getConfig().writerSettings().getWriter() == null) {
            getConfig().writerSettings().setWriter(new MorphiumWriterImpl());
        }

        if (getConfig().writerSettings().getBufferedWriter() == null) {
            getConfig().writerSettings().setBufferedWriter(new BufferedMorphiumWriterImpl());
        }

        getConfig().writerSettings().getWriter().setMorphium(this);
        getConfig().writerSettings().getWriter().setMaximumQueingTries(getConfig().writerSettings().getMaximumRetriesWriter());
        getConfig().writerSettings().getWriter().setPauseBetweenTries(getConfig().writerSettings().getRetryWaitTimeWriter());
        getConfig().writerSettings().getBufferedWriter().setMorphium(this);
        getConfig().writerSettings().getBufferedWriter().setMaximumQueingTries(getConfig().writerSettings().getMaximumRetriesBufferedWriter());
        getConfig().writerSettings().getBufferedWriter().setPauseBetweenTries(getConfig().writerSettings().getRetryWaitTimeBufferedWriter());
        getConfig().writerSettings().getAsyncWriter().setMorphium(this);
        getConfig().writerSettings().getAsyncWriter().setMaximumQueingTries(getConfig().writerSettings().getMaximumRetriesAsyncWriter());
        getConfig().writerSettings().getAsyncWriter().setPauseBetweenTries(getConfig().writerSettings().getRetryWaitTimeAsyncWriter());

        if (getConfig().cacheSettings().getCache() == null) {
            // Defaulting to standard Cache impl
            getConfig().cacheSettings().setCache(new MorphiumCacheImpl());
        }

        getConfig().cacheSettings().getCache().setAnnotationAndReflectionHelper(getARHelper());
        getConfig().cacheSettings().getCache().setGlobalCacheTimeout(getConfig().cacheSettings().getGlobalCacheValidTime());
        getConfig().cacheSettings().getCache().setHouskeepingIntervalPause(getConfig().cacheSettings().getHousekeepingTimeout());
        log.debug("Checking for capped collections...");
        // checking capped
        var capped = checkCapped();

        if (capped != null && !capped.isEmpty()) {
            for (@SuppressWarnings("rawtypes") Class cls : capped.keySet()) {
                switch (getConfig().collectionCheckSettings().getCappedCheck()) {
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
                                CreateCommand cmd = null;

                                try {
                                    primaryConnection = morphiumDriver.getPrimaryConnection(null);
                                    cmd = new CreateCommand(primaryConnection);
                                    cmd.setDb(getDatabase()).setColl(getMapper().getCollectionName(cls)).setCapped(true).setMax(capped.get(cls).get("max")).setSize(capped.get(cls).get("size"));
                                    var ret = cmd.execute();
                                    log.debug("Created capped collection");
                                } catch (MorphiumDriverException e) {
                                    throw new RuntimeException(e);
                                } finally {
                                    if (primaryConnection != null) {
                                        cmd.releaseConnection();
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
                        throw new IllegalArgumentException("Unknow value for cappedcheck " + getConfig().collectionCheckSettings().getCappedCheck());
                }
            }
        }

        if (!getConfig().collectionCheckSettings().getIndexCheck().equals(IndexCheck.NO_CHECK) && (getConfig().collectionCheckSettings().getIndexCheck().equals(IndexCheck.CREATE_ON_STARTUP) ||
                getConfig().collectionCheckSettings().getIndexCheck().equals(IndexCheck.WARN_ON_STARTUP))) {
            Map < Class<?>, List<IndexDescription >> missing = checkIndices(classInfo->!classInfo.getPackageName().startsWith("de.caluga.morphium"));
            if (missing != null && !missing.isEmpty()) {
                for (Class<?> cls : missing.keySet()) {
                    if (missing.get(cls).size() != 0) {
                        if (Msg.class.isAssignableFrom(cls)) {
                            // ignoring message class, messaging creates indexes
                            continue;
                        }

                        try {
                            if (getConfig().collectionCheckSettings().getIndexCheck().equals(IndexCheck.WARN_ON_STARTUP)) {
                                log.warn("Missing indices for entity " + cls.getName() + ": " + missing.get(cls).size());
                            } else if (getConfig().collectionCheckSettings().getIndexCheck().equals(IndexCheck.CREATE_ON_STARTUP)) {
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

        getCache().setValidCacheTime(CollectionInfo.class, 15000);
    }

    public MorphiumMessaging createMessaging() throws InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException {
        MorphiumMessaging messaging = messagingClass.getConstructor().newInstance();
        messaging.init(this);
        return messaging;
    }

    public MorphiumMessaging createMessaging(MessagingSettings overrides) throws InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException {
        MorphiumMessaging messaging = messagingClass.getConstructor().newInstance();
        messaging.init(this, overrides);
        return messaging;
    }

    public ValueEncryptionProvider getValueEncrpytionProvider() {
        return valueEncryptionProvider;
    }

    public EncryptionKeyProvider getEncryptionKeyProvider() {
        return encryptionKeyProvider;
    }

    public MorphiumCache getCache() {
        if (getConfig() == null) return null;

        return getConfig().cacheSettings().getCache();
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
            var result = getDriver().listCollections(getConfig().connectionSettings().getDatabase(), pattern);
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
        var dec = getConfig().encryptionSettings().getCredentialsDecryptionKey();
        var enc = getConfig().encryptionSettings().getCredentialsEncryptionKey();
        close();
        MorphiumConfig cfg = new MorphiumConfig(prop);
        cfg.encryptionSettings().setCredentialsDecryptionKey(dec);
        cfg.encryptionSettings().setCredentialsEncryptionKey(enc);
        cfg.connectionSettings().setDatabase(db);
        setConfig(cfg);
    }

    public void addListener(MorphiumStorageListener<?> lst) {
        List < MorphiumStorageListener<? >> newList = new ArrayList<>(listeners);
        newList.add(lst);
        listeners = newList;
    }

    public void removeListener(MorphiumStorageListener<?> lst) {
        List < MorphiumStorageListener<? >> newList = new ArrayList<>(listeners);
        newList.remove(lst);
        listeners = newList;
    }

    public MorphiumDriver getDriver() {
        return morphiumDriver;
    }

    public void setDriver(MorphiumDriver drv) {
        morphiumDriver = drv;
    }

    public Query<Map<String, Object >> createMapQuery(String collection) {
        try {
            Query<Map<String, Object >> q = new Query<>(this, null, getAsyncOperationsThreadPool());
            q.setCollectionName(collection);
            return q;
        } catch (Exception e) {
            log.error("Error", e);
        }
        return null;
    }

    // public <T> Query<T> createQueryByTemplate(T template, String ... fields) {
    //     @SuppressWarnings("rawtypes")
    //     Class cls = template.getClass();
    //     List<String> flds;

    //     if (fields.length > 0) {
    //         flds = new ArrayList<>(Arrays.asList(fields));
    //     } else {
    //         // noinspection unchecked
    //         flds = annotationHelper.getFields(cls);
    //     }

    //     Query<T> q = createQueryFor((Class<T>) cls);

    //     for (String f : flds) {
    //         try {
    //             q.f(f).eq(annotationHelper.getValue(template, f));
    //         } catch (Exception e) {
    //             log.error("Could not read field " + f + " of object " + cls.getName());
    //         }
    //     }

    //     return q;
    // }

    /**
     * search for objects similar to template concerning all given fields. If no
     * fields are
     * specified, all NON Null-Fields are taken into account if specified, field
     * might also be null
     * @deprecated not useful anymore.
     * @param template - what to search for
     * @param fields - fields to use for searching
     * @param <T> - type
     * @return result of search
     */
    // @Deprecated
    // public <T> List<T> findByTemplate(T template, String... fields) {
    //     return createQueryByTemplate(template, fields).asList();
    // }

    /**
     * This method unsets a field
     * @deprecated use {Morphium{@link #unsetInEntity(Object, String, String, AsyncOperationCallback)} instead.
     */
    // @Deprecated
    // public <T> void unset(final T toSet, String collection, final String field, final AsyncOperationCallback<T> callback) {
    //     unsetInEntity(toSet, collection, field, callback);
    // }

    public <T> void unsetInEntity(final T toSet, String collection, final String field, final AsyncOperationCallback<T> callback) {
        if (toSet == null) {
            throw new RuntimeException("Cannot update null!");
        }

        MorphiumWriter wr = getWriterForClass(toSet.getClass());
        wr.unset(toSet, collection, field, callback);
    }

    public List<Map<String, Object >> runCommand(String command, String collection, Map<String, Object> cmdMap) throws MorphiumDriverException {
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
        Runnable r = ()-> {
            String coll = getMapper().getCollectionName(c);
            // DBCollection collection = null;

            try {
                boolean exists = morphiumDriver.exists(getConfig().connectionSettings().getDatabase(), coll);

                if (exists && morphiumDriver.isCapped(getConfig().connectionSettings().getDatabase(), coll)) {
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
                    create.releaseConnection();
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

    @SuppressWarnings("rawtypes")
    public Map<String, Object> simplifyQueryObject(Map<String, Object> q) {
        if (q.keySet().size() == 1 && q.get("$and") != null) {
            Map<String, Object> ret = new HashMap<>();
            List<Map<String, Object >> lst = (List<Map<String, Object >> ) q.get("$and");

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

    public <T> Map<String, Object> push(final Query<T> query, final String field, final Object value, final boolean upsert, final boolean multiple, final AsyncOperationCallback<T> callback) {
        if (query == null || field == null) {
            throw new RuntimeException("Cannot update null!");
        }

        return getWriterForClass(query.getType()).pushPull(MorphiumStorageListener.UpdateTypes.PUSH, query, field, value, upsert, multiple, null);
    }

    // public <T> Map<String, Object> push(final Query<T> query, final String field, final Object value, final boolean upsert, final boolean multiple, final AsyncOperationCallback<T> callback){
    //     return null;
    // }
    @SuppressWarnings("rawtypes")
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


    public <T> Map<String, Object> pull(final T entity, final String field, final Expr value, final boolean upsert, final boolean multiple, final AsyncOperationCallback<T> callback) {
        if (entity == null) {
            throw new IllegalArgumentException("Null Entity cannot be pulled...");
        }

        // noinspection unchecked
        return pull((Query<T>)createQueryFor(entity.getClass()).f("_id").eq(getId(entity)), field, value, upsert, multiple, callback);
    }

    ////////
    /////
    /// SET with Query
    //
    //

    public <T> void saveList(List<T> lst, final AsyncOperationCallback<T> callback) {
        // have to sort list - might have different objects
        List<T> storeDirect = new ArrayList<>();
        List<T> insertDirect = new ArrayList<>();

        if (isWriteBufferEnabledForThread()) {
            final List<T> storeInBg = new ArrayList<>();
            final List<T> insertInBg = new ArrayList<>();

            // checking permission - might take some time ;-(
            for (T o : lst) {
                if (getARHelper().isBufferedWrite(getARHelper().getRealClass(o.getClass())) && !"InMemDriver".equals(getDriver().getName())) {
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

            getConfig().writerSettings().getBufferedWriter().store(storeInBg, callback);
            getConfig().writerSettings().getWriter().store(storeDirect, callback);
            getConfig().writerSettings().getBufferedWriter().insert(insertInBg, callback);
            getConfig().writerSettings().getWriter().insert(insertDirect, callback);
        } else {
            for (T o : lst) {
                if (getARHelper().getId(o) == null) {
                    insertDirect.add(o);
                } else {
                    storeDirect.add(o);
                }
            }

            getConfig().writerSettings().getWriter().store(storeDirect, callback);
            getConfig().writerSettings().getWriter().insert(insertDirect, callback);
        }

        // Flush all buffered writes to ensure immediate storage
        flush();
    }

    /**
     * This method sets a map of properties (defined by enums in the map) with their associated values
     *
     * @deprecated There is a newer implementation.
     * Please use {@link Morphium setInEntity()} instead.
     */
    @Deprecated
    public <T> void set(final T toSet, String collection, boolean upserts, @SuppressWarnings("rawtypes") final Map<Enum, Object> values, AsyncOperationCallback<T> callback) {
        setInEntity(toSet, collection, upserts, values, callback);
    }

    public <T> void setInEntity(final T entity, String collection, boolean upserts, @SuppressWarnings("rawtypes") final Map<Enum, Object> values, AsyncOperationCallback<T> callback) {
        Map<String, Object> strValues = new HashMap<>();

        for (@SuppressWarnings("rawtypes") Map.Entry<Enum, Object> e : values.entrySet()) {
            strValues.put(e.getKey().name(), e.getValue());
        }

        set(entity, collection, strValues, upserts, callback);
    }

    /**
     * This method sets a map of properties with their associated values
     *
     * @deprecated There is a newer implementation.
     * Please use {@link Morphium#setInEntity(Object, String, Map, boolean, AsyncOperationCallback)}   instead.
     */
    @Deprecated
    public <T> void set(final T toSet, String collection, final Map<String, Object> values, boolean upserts, AsyncOperationCallback<T> callback) {
        setInEntity(toSet, collection, values, upserts, callback);
    }

    @Override
    public <T> void setInEntity(final T entity, String collection, final Map<String, Object> values, boolean upserts, AsyncOperationCallback<T> callback) {
        if (entity == null) {
            throw new RuntimeException("Cannot update null!");
        }

        if (getId(entity) == null) {
            log.debug("just storing object as it is new...");
            store(entity);
            return;
        }

        annotationHelper.callLifecycleMethod(PreUpdate.class, entity);
        getWriterForClass(entity.getClass()).set(entity, collection, values, upserts, callback);
        annotationHelper.callLifecycleMethod(PostUpdate.class, entity);
    }

    //
    public Map<String, Object> getDbStats(String db) throws MorphiumDriverException {
        return getDriver().getDBStats(db);
    }

    public Map<String, Object> getDbStats() throws MorphiumDriverException {
        return getDriver().getDBStats(getConfig().connectionSettings().getDatabase());
    }

    public Map<String, Object> getCollStats(Class<?> coll) throws MorphiumDriverException {
        return getDriver().getCollStats(getConfig().connectionSettings().getDatabase(), getMapper().getCollectionName(coll));
    }

    public Map<String, Object> getCollStats(String coll) throws MorphiumDriverException {
        return getDriver().getCollStats(getConfig().connectionSettings().getDatabase(), coll);
    }


    public <T> Map<String, Object> inc(@SuppressWarnings("rawtypes") final Map<Enum, Number> fieldsToInc, final Query<T> matching, final boolean upsert, final boolean multiple,
                                       AsyncOperationCallback<T> callback) {
        Map<String, Number> toUpdate = new HashMap<>();

        for (@SuppressWarnings("rawtypes") Map.Entry<Enum, Number> e : fieldsToInc.entrySet()) {
            toUpdate.put(e.getKey().name(), e.getValue());
        }

        return inc(matching, toUpdate, upsert, multiple, callback);
    }

    public <T> Map<String, Object> inc(final Query<T> query, final Map<String, Number> toUpdate, final boolean upsert, final boolean multiple, AsyncOperationCallback<T> callback) {
        if (query == null) {
            throw new RuntimeException("Cannot update null!");
        }

        return getWriterForClass(query.getType()).inc(query, toUpdate, upsert, multiple, callback);
    }

    @Override
    public MorphiumWriter getWriterForClass(Class<?> cls) {
        if (annotationHelper.isBufferedWrite(cls) && isWriteBufferEnabledForThread() && !"InMemDriver".equals(getDriver().getName())) {
            return getConfig().writerSettings().getBufferedWriter();
        } else if (annotationHelper.isAsyncWrite(cls) && isAsyncWritesEnabledForThread()) {
            return getConfig().writerSettings().getAsyncWriter();
        } else {
            return getConfig().writerSettings().getWriter();
        }
    }

    /**
     * * Use remove instead, to make it more similar to mongosh
     * @deprecated use {@link Morphium#remove(List, String)}
     **/
    @Deprecated
    public <T> void delete (List<T> lst, String forceCollectionName) {
        remove(lst, forceCollectionName, (AsyncOperationCallback<T>) null);
    }

    /**
     * * Use remove instead, to make it more similar to mongosh
     * @deprecated use {@link Morphium#remove(List, String, AsyncOperationCallback)}
     **/
    @Deprecated
    public <T> void delete (List<T> lst, String forceCollectionName, AsyncOperationCallback<T> callback) {
        remove(lst, forceCollectionName, callback);
    }

    public void inc(StatisticKeys k) {
        stats.get(k).inc();
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
     * @param o - object to read
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
        FindCommand settings = null;

        try {
            MongoConnection con = morphiumDriver.getReadConnection(getReadPreferenceForClass(o.getClass()));
            settings = new FindCommand(con).setDb(getConfig().connectionSettings().getDatabase()).setColl(collection).setFilter(Doc.of(srch)).setBatchSize(1).setLimit(1);
            List<Map<String, Object >> found = settings.execute();
            settings.releaseConnection();

            // log.info("Reread took: "+settings.getMetaData().get("duration"));
            if (found != null && !found.isEmpty()) {
                Map<String, Object> dbo = found.get(0);
                Object fromDb = objectMapper.deserialize(o.getClass(), dbo);

                if (fromDb == null) {
                    throw new RuntimeException("could not reread from db");
                }

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
        } finally {
            if (settings != null) {
                settings.releaseConnection();
            }
        }

        return o;
    }

    /// Event handling
    public void firePreStore(Object o, boolean isNew) {
        if (o == null) {
            return;
        }

        for (@SuppressWarnings("rawtypes") MorphiumStorageListener l : listeners) {
            // noinspection unchecked
            l.preStore(this, o, isNew);
        }

        annotationHelper.callLifecycleMethod(PreStore.class, o);
    }

    public void firePostStore(Object o, boolean isNew) {
        for (@SuppressWarnings("rawtypes") MorphiumStorageListener l : listeners) {
            // noinspection unchecked
            l.postStore(this, o, isNew);
        }

        annotationHelper.callLifecycleMethod(PostStore.class, o);
        // existing object => store last Access, if needed
    }

    public void firePreDrop(@SuppressWarnings("rawtypes") Class cls) {
        for (@SuppressWarnings("rawtypes") MorphiumStorageListener l : listeners) {
            // noinspection unchecked
            l.preDrop(this, cls);
        }
    }

    public <T> void firePostStore(Map<T, Boolean> isNew) {
        for (@SuppressWarnings("rawtypes") MorphiumStorageListener l : listeners) {
            // noinspection unchecked
            l.postStore(this, isNew);
        }

        for (Object o : isNew.keySet()) {
            annotationHelper.callLifecycleMethod(PostStore.class, o);
        }
    }

    public <T> void firePostRemove(List<T> toRemove) {
        for (@SuppressWarnings("rawtypes") MorphiumStorageListener l : listeners) {
            // noinspection unchecked
            l.postRemove(this, toRemove);
        }

        for (Object o : toRemove) {
            annotationHelper.callLifecycleMethod(PostRemove.class, o);
        }
    }

    public <T> void firePostLoad(List<T> loaded) {
        for (@SuppressWarnings("rawtypes") MorphiumStorageListener l : listeners) {
            // noinspection unchecked
            l.postLoad(this, loaded);
        }

        for (Object o : loaded) {
            annotationHelper.callLifecycleMethod(PostLoad.class, o);
        }
    }

    public void firePreStore(Map<Object, Boolean> isNew) {
        for (@SuppressWarnings("rawtypes") MorphiumStorageListener l : listeners) {
            // noinspection unchecked
            l.preStore(this, isNew);
        }

        for (Object o : isNew.keySet()) {
            annotationHelper.callLifecycleMethod(PreStore.class, o);
        }
    }

    public <T> void firePreRemove(List<T> lst) {
        for (@SuppressWarnings("rawtypes") MorphiumStorageListener l : listeners) {
            // noinspection unchecked
            l.preRemove(this, lst);
        }

        for (T o : lst) {
            annotationHelper.callLifecycleMethod(PreRemove.class, o);
        }
    }

    public void firePreRemove(Object o) {
        for (@SuppressWarnings("rawtypes") MorphiumStorageListener l : listeners) {
            // noinspection unchecked
            l.preRemove(this, o);
        }

        annotationHelper.callLifecycleMethod(PreRemove.class, o);
    }

    public void firePostDropEvent(@SuppressWarnings("rawtypes") Class cls) {
        for (@SuppressWarnings("rawtypes") MorphiumStorageListener l : listeners) {
            l.postDrop(this, cls);
        }
    }

    public void firePostUpdateEvent(@SuppressWarnings("rawtypes") Class cls, MorphiumStorageListener.UpdateTypes t) {
        for (@SuppressWarnings("rawtypes") MorphiumStorageListener l : listeners) {
            l.postUpdate(this, cls, t);
        }
    }

    public void firePreUpdateEvent(@SuppressWarnings("rawtypes") Class cls, MorphiumStorageListener.UpdateTypes t) {
        for (@SuppressWarnings("rawtypes") MorphiumStorageListener l : listeners) {
            l.preUpdate(this, cls, t);
        }
    }

    public void firePostRemoveEvent(Object o) {
        for (@SuppressWarnings("rawtypes") MorphiumStorageListener l : listeners) {
            l.postRemove(this, o);
        }

        annotationHelper.callLifecycleMethod(PostRemove.class, o);
    }

    public void firePostRemoveEvent(@SuppressWarnings("rawtypes") Query q) {
        for (@SuppressWarnings("rawtypes") MorphiumStorageListener l : listeners) {
            l.postRemove(this, q);
        }
    }

    public void firePreRemoveEvent(@SuppressWarnings("rawtypes") Query q) {
        for (@SuppressWarnings("rawtypes") MorphiumStorageListener l : listeners) {
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
                    @SuppressWarnings("rawtypes")
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
    public void firePostLoadEvent(Object o) {
        for (@SuppressWarnings("rawtypes") MorphiumStorageListener l : listeners) {
            l.postLoad(this, o);
        }

        annotationHelper.callLifecycleMethod(PostLoad.class, o);
    }

    public boolean isReplicaSet() {
        return getConfig().clusterSettings().isReplicaset();
    }

    public Entity.ReadConcernLevel getReadConcernForClass(Class<?> cls) {
        if (cls == null) return null;

        var entity = annotationHelper.getAnnotationFromHierarchy(cls, Entity.class);

        if (entity == null) {
            return null;
        }

        return entity.readConcernLevel();
    }

    public ReadPreference getReadPreferenceForClass(Class<?> cls) {
        if (cls == null) {
            return getConfig().driverSettings().getDefaultReadPreference();
        }

        DefaultReadPreference rp = annotationHelper.getAnnotationFromHierarchy(cls, DefaultReadPreference.class);

        if (rp == null) {
            return getConfig().driverSettings().getDefaultReadPreference();
        }

        return rp.value().getPref();
    }

    @SuppressWarnings("rawtypes")
    public MorphiumBulkContext createBulkRequestContext(Class<?> type, boolean ordered) {
        return new MorphiumBulkContext(getDriver().createBulkContext(this, getConfig().connectionSettings().getDatabase(), getMapper().getCollectionName(type), ordered, getWriteConcernForClass(type)));
    }

    @SuppressWarnings({ "rawtypes" })
    public MorphiumBulkContext createBulkRequestContext(String collection, boolean ordered) {
        return new MorphiumBulkContext(getDriver().createBulkContext(this, getConfig().connectionSettings().getDatabase(), collection, ordered, null));
    }

    @SuppressWarnings({"ConstantConditions", "CommentedOutCode"})
    public WriteConcern getWriteConcernForClass(Class<?> cls) {
        WriteSafety safety = annotationHelper.getAnnotationFromHierarchy(cls, WriteSafety.class); // cls.getAnnotation(WriteSafety.class);

        if (safety == null) {
            return null;
        }

        boolean j = safety.waitForJournalCommit();
        int w = safety.level().getValue();
        long timeout = safety.timeout();

        if (isReplicaSet() && w > 2) {
            var hostSeed = getDriver().getHostSeed(); //list of available hosts
            // de.caluga.morphium.replicaset.ReplicaSetStatus s = RsMonitor.getCurrentStatus();

            if (log.isDebugEnabled()) {
                log.debug("Active nodes now: " + hostSeed.size());
            }

            int activeNodes = hostSeed.size();

            if (activeNodes > 50) {
                activeNodes = 50;
            }

            long maxReplLag = 0;
            // Wait for all active slaves (-1 for the timeout bug)
            w = activeNodes;
        }

        if (!isReplicaSet() && timeout < 0) {
            timeout = 0;
        }

        return WriteConcern.getWc(w, j, (int) timeout);
    }

    /**
     * issues a remove command - no lifecycle methods called, no drop, keeps all
     * indexes this way
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
     * @param cls - class
     * @param colName - CollectionName
     */
    public void clearCollection(Class<?> cls, String colName) {
        @SuppressWarnings("rawtypes")
        Query q = createQueryFor(cls);
        q.setCollectionName(colName);
        remove(q);
    }

    /**
     * clears every single object in collection - reads ALL objects to do so this
     * way Lifecycle
     * methods can be called!
     *
     * @param cls -class
     */
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
    public <T> List<T> readAll(Class <? extends T > cls) {
        inc(StatisticKeys.READS);
        Query<T> qu;
        qu = createQueryFor(cls);
        return qu.asList();
    }

    public <T> Query<T> createQueryFor(Class <? extends T > type, String usingCollectionName) {
        return (Query<T>)createQueryFor(type).setCollectionName(usingCollectionName);
    }

    public <T> Query<T> createQueryFor(Class <? extends T > type) {
        Query<T> q = new Query<>(this, type, getAsyncOperationsThreadPool());
        q.setAutoValuesEnabled(isAutoValuesEnabledForThread());
        return q;
    }

    public <T> QueryIterator <? extends T> iterateAll(Class <? extends T > type) {
        return iterateAll(type, null);
    }

    public <T> QueryIterator <? extends T> iterateAll(Class<? extends T > type, Map <?, ? > sort) {
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

    public <T> List<T> readAll(Class<? extends T > type, Map <?, ? > sort) {
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
     * @deprecated - for read access use {@link Query} instead
     * @param q
     * @param <T>
     * @return
     */
    @Deprecated
    public <T> List<T> find(Query<T> q) {
        return q.asList();
    }

    /**
     * returns a distinct list of values of the given collection Attention: these
     * values are not
     * unmarshalled, you might get MongoMap<String,Object>s
     */
    public List<Object> distinct(String key, @SuppressWarnings("rawtypes") Query q) {
        MongoConnection con = null;
        DistinctMongoCommand settings =
                        null; //new DistinctMongoCommand(con).setColl(q.getCollectionName()).setDb(getConfig().getConnectionSettings().getDatabase()).setQuery(Doc.of(q.toQueryObject())).setKey(key);

        try {
            con = getDriver().getPrimaryConnection(null);
            settings = new DistinctMongoCommand(con).setColl(q.getCollectionName()).setDb(getConfig().connectionSettings().getDatabase()).setQuery(Doc.of(q.toQueryObject())).setKey(key);

            if (q.getCollation() != null) {
                settings.setCollation(Doc.of(q.getCollation().toQueryObject()));
            }

            return settings.execute();
        } catch (MorphiumDriverException e) {
            throw new RuntimeException(e);
        } finally {
            if (settings != null) {
                settings.releaseConnection();
            }
        }
    }

    public List<Object> distinct(String key, @SuppressWarnings("rawtypes") Class cls, Collation collation) {
        MongoConnection con = null;
        DistinctMongoCommand settings = null;

        try {
            con = morphiumDriver.getPrimaryConnection(null);
            settings = new DistinctMongoCommand(con).setColl(objectMapper.getCollectionName(cls)).setDb(getConfig().connectionSettings().getDatabase()).setKey(getARHelper().getMongoFieldName(cls, key));

            if (collation != null) {
                settings.setCollation(Doc.of(collation.toQueryObject()));
            }

            return settings.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            settings.releaseConnection();
        }
    }


    public List<Object> distinct(String key, String collectionName, Collation collation) {
        MongoConnection con = null;
        DistinctMongoCommand cmd = null;

        try {
            con = getDriver().getPrimaryConnection(null);
            cmd = new DistinctMongoCommand(con);
            cmd.setColl(collectionName).setDb(getConfig().connectionSettings().getDatabase()).setKey(key).setCollation(collation.toQueryObject());
            return cmd.execute();
            // return morphiumDriver.distinct(getConfig().getConnectionSettings().getDatabase(), collectionName, key, new
            // HashMap<>(), collation, getConfig().getDefaultReadPreference());
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            cmd.releaseConnection();
        }
    }


    public <T> void findById(Class <? extends T > type, Object id, String collection, @SuppressWarnings("rawtypes") AsyncOperationCallback callback) {
        createQueryFor(type).setCollectionName(collection).f(getARHelper().getIdFieldName(type)).eq(id).get(callback);
    }

    public <T> T findById(Class <? extends T > type, Object id, String collection) {
        inc(StatisticKeys.READS);
        Cache c = getARHelper().getAnnotationFromHierarchy(type, Cache.class); // type.getAnnotation(Cache.class);
        boolean useCache = c != null && c.readCache() && isReadCacheEnabledForThread() && !"InMemDriver".equals(getDriver().getName());

        if (useCache) {
            if (getCache().getFromIDCache(type, id) != null) {
                inc(StatisticKeys.CHITS);
                return getCache().getFromIDCache(type, id);
            }

            inc(StatisticKeys.CMISS);
        } else {
            inc(StatisticKeys.NO_CACHED_READS);
        }

        List<String> ls = annotationHelper.getFields(type, Id.class);

        if (ls.isEmpty()) {
            throw new RuntimeException("Cannot find by ID on non-Entity");
        }

        return createQueryFor(type).setCollectionName(collection).f(ls.get(0)).eq(id).get();
    }

    public <T> List<T> findByField(Class <? extends T > cls, String fld, Object val) {
        Query<T> q = createQueryFor(cls);
        q = q.f(fld).eq(val);
        return q.asList();
    }

    public <T> List<T> findByField(Class<? extends T > cls, Enum<?> fld, Object val) {
        return findByField(cls, fld.name(), val);
    }

    public <T> boolean setAutoValues(T o) throws IllegalAccessException {
        @SuppressWarnings("rawtypes")
        Class type = getARHelper().getRealClass(o.getClass());
        Object id = getARHelper().getId(o);
        boolean aNew = id == null;

        if (!isAutoValuesEnabledForThread()) {
            return aNew;
        }

        if (getARHelper().isAnnotationPresentInHierarchy(type, CreationTime.class)) {
            Object reread = null;
            CreationTime ct = getARHelper().getAnnotationFromHierarchy(o.getClass(), CreationTime.class);
            boolean checkForNew = Objects.requireNonNull(ct).checkForNew() && getConfig().objectMappingSettings().isCheckForNew();
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

    public void flush() {
        getConfig().writerSettings().getBufferedWriter().flush();
        getConfig().writerSettings().getWriter().flush();
    }

    public void flush(@SuppressWarnings("rawtypes") Class type) {
        getConfig().writerSettings().getBufferedWriter().flush(type);
        getConfig().writerSettings().getWriter().flush(type);
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

    @Override
    public <T> void ensureIndicesFor(Class<T> type, String onCollection, AsyncOperationCallback<T> callback, MorphiumWriter wr) {
        if (annotationHelper.isAnnotationPresentInHierarchy(type, Index.class)) {
            // List<Annotation> collations =
            // annotationHelper.getAllAnnotationsFromHierachy(type,
            // de.caluga.morphium.annotations.Collation.class);
            List<Annotation> lst = annotationHelper.getAllAnnotationsFromHierachy(type, Index.class);

            for (Annotation a : lst) {
                Index i = (Index) a;

                if (i.value().length > 0) {
                    List<Map<String, Object >> options = null;
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

                    List<Map<String, Object >> idx = createIndexKeyMapFrom(i.value());
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
                            if (e.getMessage() != null && e.getMessage().contains("already exists")) {
                                log.warn("Index already exists: " + e.getMessage());
                            } else {
                                throw e;
                            }
                        }

                        cnt++;
                    }
                }
            }
        }

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
                    if (e.getMessage().contains("already exists")) {
                        log.warn("Index already exists: " + e.getMessage());
                    } else {
                        throw (e);
                    }
                }
            }
        }
    }

    public void ensureIndex(Class<?> cls, Map<String, Object> index) {
        try {
            getWriterForClass(cls).createIndex(cls, getMapper().getCollectionName(cls), IndexDescription.fromMaps(index, null), null);
        } catch (Exception e) {
            if (e.getMessage().contains("already exists")) {
                log.warn("Index already exists: " + e.getMessage());
            } else {
                throw (e);
            }
        }
    }

    public void ensureIndex(Class<?> cls, String collection, Map<String, Object> index, Map<String, Object> options) {
        try {
            getWriterForClass(cls).createIndex(cls, collection, IndexDescription.fromMaps(index, options), null);
        } catch (Exception e) {
            if (e.getMessage().contains("already exists")) {
                log.warn("Index already exists: " + e.getMessage());
            } else {
                throw (e);
            }
        }
    }

    public void ensureIndex(Class<?> cls, String collection, Map<String, Object> index) {
        try {
            getWriterForClass(cls).createIndex(cls, collection, IndexDescription.fromMaps(index, null), null);
        } catch (Exception e) {
            if (e.getMessage().contains("already exists")) {
                log.warn("Index already exists: " + e.getMessage());
            } else {
                throw (e);
            }
        }
    }

    /**
     * ensureIndex(CachedObject.class,"counter","-value");
     * ensureIndex(CachedObject.class,"counter:2d","-value); Similar to sorting
     *
     * @param cls - class
     * @param fldStr - fields
     */
    public <T> void ensureIndex(Class<T> cls, AsyncOperationCallback<T> callback, @SuppressWarnings("rawtypes") Enum... fldStr) {
        ensureIndex(cls, getMapper().getCollectionName(cls), callback, fldStr);
    }

    public <T> void ensureIndex(Class<T> cls, String collection, AsyncOperationCallback<T> callback, @SuppressWarnings("rawtypes") Enum... fldStr) {
        Map<String, Object> m = new LinkedHashMap<>();

        for (Enum<?> e : fldStr) {
            String f = e.name();
            m.put(f, 1);
        }

        try {
            getWriterForClass(cls).createIndex(cls, collection, IndexDescription.fromMaps(m, null), callback);
        } catch (Exception e) {
            if (e.getMessage().contains("already exists")) {
                log.warn("Index already exists: " + e.getMessage());
            } else {
                throw (e);
            }
        }
    }

    public <T> void ensureIndex(Class<T> cls, AsyncOperationCallback<T> callback, String... fldStr) {
        ensureIndex(cls, getMapper().getCollectionName(cls), callback, fldStr);
    }

    public <T> void ensureIndex(Class<T> cls, String collection, AsyncOperationCallback<T> callback, String... fldStr) {
        List<Map<String, Object >> m = createIndexKeyMapFrom(fldStr);
        for (Map<String, Object> idx : m) {
            try {
                getWriterForClass(cls).createIndex(cls, collection, IndexDescription.fromMaps(idx, null), callback);
            } catch (Exception e) {
                if (e.getMessage().contains("already exists")) {
                    log.warn("Index already exists: " + e.getMessage());
                } else {
                    throw (e);
                }
            }
        }
    }

    public <T> void ensureIndex(Class<T> cls, Map<String, Object> index, AsyncOperationCallback<T> callback) {
        ensureIndex(cls, getMapper().getCollectionName(cls), index, callback);
    }

    public <T> void ensureIndex(Class<T> cls, String collection, Map<String, Object> index, Map<String, Object> options, AsyncOperationCallback<T> callback) {
        try {
            getWriterForClass(cls).createIndex(cls, collection, IndexDescription.fromMaps(index, options), callback);
        } catch (Exception e) {
            if (e.getMessage().contains("already exists")) {
                log.warn("Index already exists: " + e.getMessage());
            } else {
                throw (e);
            }
        }
    }

    public <T> void ensureIndex(Class<T> cls, String collection, Map<String, Object> index, AsyncOperationCallback<T> callback) {
        try {
            getWriterForClass(cls).createIndex(cls, collection, IndexDescription.fromMaps(index, null), callback);
        } catch (Exception e) {
            if (e.getMessage().contains("already exists")) {
                log.warn("Index already exists: " + e.getMessage());
            } else {
                throw (e);
            }
        }
    }

    public <T> void createIndex(Class<T> cls, String collection, IndexDescription index, AsyncOperationCallback<T> callback) {
        try {
            getWriterForClass(cls).createIndex(cls, collection, index, callback);
        } catch (Exception e) {
            if (e.getMessage().contains("already exists")) {
                log.warn("Index already exists: " + e.getMessage());
            } else {
                throw (e);
            }
        }
    }

    public List<IndexDescription> getIndexesFromMongo(@SuppressWarnings("rawtypes") Class cls) {
        return getIndexesFromMongo(getMapper().getCollectionName(cls));
    }

    public List<IndexDescription> getIndexesFromMongo(String collection) {
        MongoConnection readConnection = getDriver().getReadConnection(getConfig().driverSettings().getDefaultReadPreference());
        ListIndexesCommand cmd = new ListIndexesCommand(readConnection);
        cmd.setDb(getDatabase()).setColl(collection);

        try {
            return cmd.execute();
        } catch (MorphiumDriverException e) {
            throw new RuntimeException(e);
        } finally {
            cmd.releaseConnection();
        }
    }

    public <T> List<IndexDescription> getIndexesFromEntity(Class<T> type) {
        List<IndexDescription> ret = new ArrayList<>();

        if (annotationHelper.isAnnotationPresentInHierarchy(type, Index.class)) {
            // List<Annotation> collations =
            // annotationHelper.getAllAnnotationsFromHierachy(type,
            // de.caluga.morphium.annotations.Collation.class);
            // Indexes on class level, usually combined ones
            List<Annotation> lst = annotationHelper.getAllAnnotationsFromHierachy(type, Index.class);

            for (Annotation a : lst) {
                Index i = (Index) a;

                if (i.value().length > 0) {
                    List<Map<String, Object >> options = null;
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

                    List<Map<String, Object >> idx = createIndexKeyMapFrom(i.value());
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

    public int writeBufferCount() {
        return getConfig().writerSettings().getWriter().writeBufferCount() + getConfig().writerSettings().getBufferedWriter().writeBufferCount();
    }

    public <T> void store(List<T> lst, String collectionName, AsyncOperationCallback<T> callback) {
        if (lst == null || lst.isEmpty()) {
            return;
        }

        getWriterForClass(lst.get(0).getClass()).store(lst, collectionName, callback);
    }

    public List<Map<String, Object >> createIndexKeyMapFrom(String[] fldStr) {
        if (fldStr.length == 0) {
            return null;
        }
        List<Map<String, Object >> lst = new ArrayList<>();

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
        if (getConfig() == null) {
            return null;
        }

        return getConfig().connectionSettings().getDatabase();
    }

    public void ensureIndex(Class<?> cls, String... fldStr) {
        ensureIndex(cls, null, fldStr);
    }

    public void ensureIndex(Class<?> cls, @SuppressWarnings("rawtypes") Enum... fldStr) {
        ensureIndex(cls, null, fldStr);
    }

    public <T> void insertList(@SuppressWarnings("rawtypes") List lst, String collection, AsyncOperationCallback<T> callback) {
        Map < Class<?>, MorphiumWriter> writers = new HashMap<>();
        Map < Class<?>, List<Object >> values = new HashMap<>();

        for (Object o : lst) {
            writers.putIfAbsent(o.getClass(), getWriterForClass(o.getClass()));
            values.putIfAbsent(o.getClass(), new ArrayList<>());
            values.get(o.getClass()).add(o);
        }
        for (@SuppressWarnings("rawtypes") Class cls : writers.keySet()) {
            try {
                writers.get(cls).insert((List<T>) values.get(cls), collection, callback);
            } catch (Exception e) {
                // log.error("Write failed for " + cls.getName() + " lst of size " +
                // values.get(cls).size(), e);
                throw new RuntimeException(e);
            }
        }
    }


    public Map<String, Integer> saveMaps(@SuppressWarnings("rawtypes") Class type, List<Map<String, Object >> lst) throws MorphiumDriverException {
        StoreMongoCommand store = null;

        try {
            var con = getDriver().getPrimaryConnection(null);
            store = new StoreMongoCommand(con).setColl(getMapper().getCollectionName(type)).setDb(getDatabase()).setDocuments(lst);
            store.execute();
            return null;
        } finally {
            if (store != null) {
                store.releaseConnection();
            }
        }
    }

    /**
     * directly writes data to Mongo, no Mapper used use with caution, as caches are
     * not updated
     * also no checks for validity of fields, no references, no auto-variables no
     * async writing!
     *
     * @param collection - name of colleciton to write to
     * @param lst - list of entries to write
     * @return statistics
     * @throws MorphiumDriverException
     */
    public Map<String, Integer> storeMaps(String collection, List<Map<String, Object >> lst) throws MorphiumDriverException {
        return saveMaps(collection, lst);
    }

    public Map<String, Integer> saveMaps(String collection, List<Map<String, Object >> lst) throws MorphiumDriverException {
        StoreMongoCommand store = null;

        try {
            MongoConnection primaryConnection = getDriver().getPrimaryConnection(null);
            store = new StoreMongoCommand(primaryConnection).setColl(collection).setDb(getDatabase()).setDocuments(lst);
            Map<String, Object> ret = store.execute();
            Map<String, Integer> res = new HashMap<>();
            res.put("stored", (Integer) ret.get("stored"));
            return res;
        } finally {
            if (store != null) {
                store.releaseConnection();
            }
        }
    }

    /**
     * directly writes data to Mongo, no Mapper used use with caution, as caches are
     * not updated
     * also no checks for validity of fields, no references, no auto-variables no
     * async writing!
     *
     * @param collection collection name
     * @param m data to write
     * @return statistics
     * @throws MorphiumDriverException
     */
    public Map<String, Integer> storeMap(String collection, Map<String, Object> m) throws MorphiumDriverException {
        return saveMap(collection, m);
    }

    public Map<String, Integer> saveMap(String collection, Map<String, Object> m) throws MorphiumDriverException {
        MongoConnection primaryConnection = getDriver().getPrimaryConnection(null);
        StoreMongoCommand settings = new StoreMongoCommand(primaryConnection).setDb(getDatabase()).setColl(collection).setDocuments(Arrays.asList(Doc.of(m)));

        try {
            Map<String, Integer> res = new HashMap<>();
            Map<String, Object> result = settings.execute();
            settings.releaseConnection();
            res.put("stored", (Integer) result.get("stored"));
            return res;
        } finally {
            if (settings != null)
                settings.releaseConnection();
        }
    }

    /**
     * directly writes data to Mongo, no Mapper used use with caution, as caches are
     * not updated
     * also no checks for validity of fields, no references, no auto-variables no
     * async writing!
     *
     * @param type - type, used to determine collection name
     * @param m - data to write
     * @return statistics
     * @throws MorphiumDriverException
     */
    public Map<String, Object> storeMap(@SuppressWarnings("rawtypes") Class type, Map<String, Object> m) throws MorphiumDriverException {
        MongoConnection primaryConnection = getDriver().getPrimaryConnection(null);
        StoreMongoCommand settings = new StoreMongoCommand(primaryConnection).setDb(getDatabase()).setColl(getMapper().getCollectionName(type)).setDocuments(Arrays.asList(m));

        try {
            WriteConcern wc = getWriteConcernForClass(type);

            if (wc != null) {
                settings.setWriteConcern(wc.asMap());
            }

            var ret = settings.execute();
            settings.releaseConnection();
            settings = null;
            return ret;
        } finally {
            if (settings != null) {
                settings.releaseConnection();
            }
        }
    }


    public <T> void saveList(List<T> lst, String collection, AsyncOperationCallback<T> callback) {
        Map < Class<?>, MorphiumWriter> writers = new HashMap<>();
        Map < Class<?>, List<Object >> values = new HashMap<>();

        for (Object o : lst) {
            writers.putIfAbsent(o.getClass(), getWriterForClass(o.getClass()));
            values.putIfAbsent(o.getClass(), new ArrayList<>());
            values.get(o.getClass()).add(o);
        }
        for (@SuppressWarnings("rawtypes") Class cls : writers.keySet()) {
            try {
                writers.get(cls).store((List<T>) values.get(cls), collection, callback);
            } catch (Exception e) {
                log.error("AWrite failed for " + cls.getName() + " lst of size " + values.get(cls).size(), e);

                if (callback != null) {
                    callback.onOperationError(AsyncOperationType.WRITE, null, 0, e.getMessage(), e, null, cls);
                }
            }
        }
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
        shutDownListeners.add(0, l);
    }

    public void removeShutdownListener(ShutdownListener l) {
        shutDownListeners.remove(l);
    }

    @SuppressWarnings("CommentedOutCode")
    public void close() {
        for (ShutdownListener l : shutDownListeners) {
            l.onShutdown(this);
        }

        if (asyncOperationsThreadPool != null) {
            asyncOperationsThreadPool.shutdownNow();
        }

        asyncOperationsThreadPool = null;

        if (config != null) {
            getConfig().writerSettings().getAsyncWriter().close();
            getConfig().writerSettings().getBufferedWriter().close();
            getConfig().writerSettings().getWriter().close();
        }

        if (morphiumDriver != null) {
            try {
                // Check if this is a shared InMemoryDriver
                if (morphiumDriver.getName().equals(InMemoryDriver.driverName)) {
                    String dbName = getConfig().connectionSettings().getDatabase();
                    java.util.concurrent.atomic.AtomicInteger refCount = inMemoryDriverRefCounts.get(dbName);
                    if (refCount != null) {
                        int remaining = refCount.decrementAndGet();
                        log.info("Decremented InMemoryDriver ref count for database '{}' (driver hashcode: {}, remaining: {})",
                                 dbName, System.identityHashCode(morphiumDriver), remaining);
                        // Only close the driver when the last Morphium instance releases it
                        if (remaining == 0) {
                            log.info("Last reference to InMemoryDriver for database '{}', closing driver", dbName);
                            morphiumDriver.close();
                            inMemoryDriversByDatabase.remove(dbName);
                            inMemoryDriverRefCounts.remove(dbName);
                        } else {
                            log.info("Skipping driver close, {} other Morphium instance(s) still using it", remaining);
                        }
                    } else {
                        // Ref count not found, close anyway (shouldn't happen)
                        morphiumDriver.close();
                    }
                } else {
                    // Non-shared driver, close normally
                    morphiumDriver.close();
                }
            } catch (Exception e) {
                //swallow - during close! e.printStackTrace();
            }

            morphiumDriver = null;
        }

        if (config != null) {
            if (getConfig().cacheSettings().getCache() != null) {
                getConfig().cacheSettings().getCache().resetCache();
                getConfig().cacheSettings().getCache().close();
            }

            getConfig().writerSettings().setBufferedWriter(null);
            getConfig().writerSettings().setAsyncWriter(null);
            getConfig().writerSettings().setWriter(null);
            config = null;
        }

        // getConfig().getCache().resetCache();
        // MorphiumSingleton.reset();
        instances.remove(this);
    }

    public String createCamelCase(String n) {
        return annotationHelper.createCamelCase(n, false);
    }

    ////////////////////////////////
    /////// MAP/REDUCE
    /////

    public <T> Map<String, Object> explainMapReduce(Class <? extends T > type, String map, String reduce, ExplainVerbosity verbose) throws MorphiumDriverException {
        MongoConnection readConnection = morphiumDriver.getReadConnection(getReadPreferenceForClass(type));

        try {
            MapReduceCommand mr = new MapReduceCommand(readConnection).setDb(getDatabase()).setColl(getMapper().getCollectionName(type)).setMap(map).setReduce(reduce);
            mr.releaseConnection();
            return mr.explain(verbose);
        } finally {
        }
    }

    public <T> List<T> mapReduce(Class <? extends T > type, String map, String reduce) throws MorphiumDriverException {
        MongoConnection readConnection = morphiumDriver.getReadConnection(getReadPreferenceForClass(type));

        try {
            MapReduceCommand mr = new MapReduceCommand(readConnection).setDb(getDatabase()).setColl(getMapper().getCollectionName(type)).setMap(map).setReduce(reduce);
            List<Map<String, Object >> result = mr.execute();
            mr.releaseConnection();
            List<T> ret = new ArrayList<>();

            for (Map<String, Object> o : result) {
                ret.add(getMapper().deserialize(type, (Map<String, Object>) o.get("value")));
            }
            return ret;
        } finally {
        }
    }

    /////////////////
    //// AGGREGATOR Support
    ///

    public <T, R> Aggregator<T, R> createAggregator(Class<? extends T > type, Class <? extends R > resultType) {
        Aggregator<T, R> aggregator = getDriver().createAggregator(this, type, resultType);
        return aggregator;
    }

    public <T> T createLazyLoadedEntity(Class <? extends T > cls, Object id, String collectionName) {
        return (T) Enhancer.create(cls, new Class[] {Serializable.class}, new LazyDeReferencingProxy(this, cls, id, collectionName));
    }

    public int getWriteBufferCount() {
        return getConfig().writerSettings().getBufferedWriter().writeBufferCount() + getConfig().writerSettings().getWriter().writeBufferCount() +
               getConfig().writerSettings().getAsyncWriter().writeBufferCount();
    }

    public int getBufferedWriterBufferCount() {
        return getConfig().writerSettings().getBufferedWriter().writeBufferCount();
    }

    public int getAsyncWriterBufferCount() {
        return getConfig().writerSettings().getAsyncWriter().writeBufferCount();
    }

    public int getWriterBufferCount() {
        return getConfig().writerSettings().getWriter().writeBufferCount();
    }

    public void disableAutoValuesForThread() {
        enableAutoValues.set(false);
    }

    public void enableAutoValuesForThread() {
        enableAutoValues.set(true);
    }

    public boolean isAutoValuesEnabledForThread() {
        if (getConfig() == null) return false;
        if (enableAutoValues == null) return getConfig().objectMappingSettings().isAutoValuesEnabled();

        return ((enableAutoValues.get() == null || enableAutoValues.get()) && getConfig().objectMappingSettings().isAutoValuesEnabled());
    }

    public void disableReadCacheForThread() {
        enableReadCache.set(false);
    }

    public void enableReadCacheForThread() {
        enableReadCache.set(true);
    }

    public boolean isReadCacheEnabledForThread() {
        if (enableReadCache == null) return getConfig().cacheSettings().isReadCacheEnabled();

        return (enableReadCache.get() == null || enableReadCache.get()) && getConfig().cacheSettings().isReadCacheEnabled();
    }

    public void disableWriteBufferForThread() {
        disableWriteBuffer.set(false);
    }

    public void enableWriteBufferForThread() {
        disableWriteBuffer.set(true);
    }

    public boolean isWriteBufferEnabledForThread() {
        return (disableWriteBuffer.get() == null || disableWriteBuffer.get()) && getConfig().cacheSettings().isBufferedWritesEnabled();
    }

    public void disableAsyncWritesForThread() {
        disableAsyncWrites.set(false);
    }

    public void enableAsyncWritesForThread() {
        disableAsyncWrites.set(true);
    }

    public boolean isAsyncWritesEnabledForThread() {
        return (disableAsyncWrites.get() == null || disableAsyncWrites.get()) && getConfig().cacheSettings().isAsyncWritesEnabled();
    }

    public void queueTask(Runnable runnable) {
        boolean queued = false;

        do {
            try {
                asyncOperationsThreadPool.execute(runnable);
                queued = true;
            } catch (Exception e) {
                LockSupport.parkNanos(200000000);
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

    public <T> void watchAsync(String collectionName, boolean updateFull, List<Map<String, Object >> pipeline, ChangeStreamListener lst) {
        asyncOperationsThreadPool.execute(()->watch(collectionName, updateFull, pipeline, lst));
    }

    public <T> void watchAsync(Class<T> entity, boolean updateFull, ChangeStreamListener lst) {
        asyncOperationsThreadPool.execute(()->watch(entity, updateFull, null, lst));
    }

    public <T> void watchAsync(Class<T> entity, boolean updateFull, List<Map<String, Object >> pipeline, ChangeStreamListener lst) {
        asyncOperationsThreadPool.execute(()->watch(entity, updateFull, pipeline, lst));
    }

    public <T> void watch(Class<T> entity, boolean updateFull, ChangeStreamListener lst) {
        watch(getMapper().getCollectionName(entity), updateFull, null, lst);
    }

    public <T> void watch(Class<T> entity, boolean updateFull, List<Map<String, Object >> pipeline, ChangeStreamListener lst) {
        watch(getMapper().getCollectionName(entity), updateFull, pipeline, lst);
    }

    public <T> void watch(String collectionName, boolean updateFull, ChangeStreamListener lst) {
        watch(collectionName, getConfig().connectionSettings().getMaxWaitTime(), updateFull, null, lst);
    }

    public <T> void watch(String collectionName, boolean updateFull, List<Map<String, Object >> pipeline, ChangeStreamListener lst) {
        watch(collectionName, getConfig().connectionSettings().getMaxWaitTime(), updateFull, pipeline, lst);
    }

    public <T> void watch(String collectionName, int maxWaitTime, boolean updateFull, List<Map<String, Object >> pipeline, ChangeStreamListener lst) {
        WatchCommand settings = null;
        try {
            MongoConnection primaryConnection = getDriver().getPrimaryConnection(null);
            settings = new WatchCommand(primaryConnection).setDb(getConfig().connectionSettings().getDatabase()).setColl(collectionName).setMaxTimeMS(maxWaitTime).setPipeline(pipeline)
            .setFullDocument(updateFull ? WatchCommand.FullDocumentEnum.updateLookup : WatchCommand.FullDocumentEnum.defaultValue).setCb(new DriverTailableIterationCallback() {
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
            // settings.releaseConnection();
        } catch (MorphiumDriverException e) {
            throw new RuntimeException(e);
        } finally {
            if (settings != null) settings.releaseConnection();
        }
    }

    private boolean processEvent(ChangeStreamListener lst, Map<String, Object> doc) {
        Map<String, Object> obj = (Map<String, Object>) doc.get("fullDocument");
        doc.remove("fullDocument");
        Map<String, Object> before = (Map<String, Object>) doc.get("fullDocumentBeforeChange");
        doc.remove("fullDocumentBeforeChange");
        ChangeStreamEvent evt = getMapper().deserialize(ChangeStreamEvent.class, doc);
        evt.setFullDocument(obj);
        evt.setFullDocumentBeforeChange(before);
        return lst.incomingData(evt);
    }

    public <T> AtomicBoolean watchDbAsync(String dbName, boolean updateFull, List<Map<String, Object >> pipeline, ChangeStreamListener lst) {
        AtomicBoolean runningFlag = new AtomicBoolean(true);
        asyncOperationsThreadPool.execute(()-> {
            watchDb(dbName, updateFull, null, runningFlag, lst);
            log.debug("watch async finished");
        });
        return runningFlag;
    }

    public <T> AtomicBoolean watchDbAsync(boolean updateFull, AtomicBoolean runningFlag, ChangeStreamListener lst) {
        return watchDbAsync(getConfig().connectionSettings().getDatabase(), updateFull, null, lst);
    }

    public <T> AtomicBoolean watchDbAsync(boolean updateFull, List<Map<String, Object >> pipeline, ChangeStreamListener lst) {
        return watchDbAsync(getConfig().connectionSettings().getDatabase(), updateFull, pipeline, lst);
    }

    public <T> void watchDb(boolean updateFull, ChangeStreamListener lst) {
        watchDb(getConfig().connectionSettings().getDatabase(), updateFull, lst);
    }

    public <T> void watchDb(String dbName, boolean updateFull, ChangeStreamListener lst) {
        watchDb(dbName, getConfig().connectionSettings().getMaxWaitTime(), updateFull, null, new AtomicBoolean(true), lst);
    }

    public <T> void watchDb(String dbName, boolean updateFull, List<Map<String, Object >> pipeline, AtomicBoolean runningFlag, ChangeStreamListener lst) {
        watchDb(dbName, getConfig().connectionSettings().getMaxWaitTime(), updateFull, pipeline, runningFlag, lst);
    }

    public <T> void watchDb(String dbName, int maxWaitTime, boolean updateFull, List<Map<String, Object >> pipeline, AtomicBoolean runningFlag, ChangeStreamListener lst) {
        WatchCommand cmd = null;
        try {
            MongoConnection con = getDriver().getPrimaryConnection(null);
            cmd = new WatchCommand(con).setDb(dbName).setMaxTimeMS(maxWaitTime).setFullDocument(updateFull ? WatchCommand.FullDocumentEnum.updateLookup : WatchCommand.FullDocumentEnum.defaultValue)
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
            if (cmd != null) {
                cmd.releaseConnection();
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
        return getMissingIndicesFor(entity, objectMapper.getCollectionName(entity));
    }

    @SuppressWarnings("ConstantConditions")
    public List<IndexDescription> getMissingIndicesFor(Class<?> entity, String collection) throws MorphiumDriverException {
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

    /**
     * run trhough classpath, find all Entities, check indices returns a list of
     * Entities, whos
     * indices are missing or different
     */
    public Map < Class<?>, List<IndexDescription >> checkIndices() {
        return checkIndices(null);
    }

    public Map < Class<?>, Map<String, Integer >> checkCapped() {
        Map < Class<?>, Map<String, Integer >> uncappedCollections = new HashMap<>();

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
                        if (!morphiumDriver.isCapped(getConfig().connectionSettings().getDatabase(), getMapper().getCollectionName(entity))) {
                            Capped capped = annotationHelper.getAnnotationFromClass(entity, Capped.class);
                            uncappedCollections.put(entity, UtilsMap.of("max", capped.maxEntries(), "size", capped.maxSize()));
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
    public Map < Class<?>, List<IndexDescription >> checkIndices(ClassInfoList.ClassInfoFilter filter) {
        Map < Class<?>, List<IndexDescription >> missingIndicesByClass = new HashMap<>();

        // initializing type IDs
        try (ScanResult scanResult = new ClassGraph()
            // .verbose() // Enable verbose logging
            .enableAnnotationInfo()
            // .enableFieldInfo()
            .enableClassInfo()                         // Scan classes, methods, fields, annotations
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
        return "Morphium: Driver " + getDriver().getName() + " - " + String.join(",", getConfig().clusterSettings().getHostSeed());
    }
}
