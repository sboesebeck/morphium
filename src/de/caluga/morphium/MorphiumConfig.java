package de.caluga.morphium;


import de.caluga.morphium.aggregation.Aggregator;
import de.caluga.morphium.aggregation.AggregatorFactory;
import de.caluga.morphium.aggregation.AggregatorFactoryImpl;
import de.caluga.morphium.aggregation.AggregatorImpl;
import de.caluga.morphium.annotations.AdditionalData;
import de.caluga.morphium.annotations.Embedded;
import de.caluga.morphium.annotations.Transient;
import de.caluga.morphium.cache.MorphiumCache;
import de.caluga.morphium.driver.ReadPreference;
import de.caluga.morphium.driver.ReadPreferenceType;
import de.caluga.morphium.driver.mongodb.Driver;
import de.caluga.morphium.encryption.DefaultEncryptionKeyProvider;
import de.caluga.morphium.encryption.EncryptionKeyProvider;
import de.caluga.morphium.query.*;
import de.caluga.morphium.writer.AsyncWriterImpl;
import de.caluga.morphium.writer.BufferedMorphiumWriterImpl;
import de.caluga.morphium.writer.MorphiumWriter;
import de.caluga.morphium.writer.MorphiumWriterImpl;
import org.json.simple.parser.ParseException;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.*;

/**
 * Stores the configuration for the MongoDBLayer.
 *
 * @author stephan
 */
@SuppressWarnings("UnusedDeclaration")
@Embedded
public class MorphiumConfig {
    @AdditionalData(readOnly = false)
    private Map<String, Object> restoreData;
    //    private MongoDbMode mode;
    private int maxConnections, housekeepingTimeout;
    private int globalCacheValidTime = 5000;
    private int writeCacheTimeout = 5000;
    private String database;
    @Transient
    private MorphiumWriter writer;
    @Transient
    private MorphiumWriter bufferedWriter;
    @Transient
    private MorphiumWriter asyncWriter;
    private int connectionTimeout = 0;
    private int socketTimeout = 0;
    private boolean socketKeepAlive = true;
    private boolean safeMode = false;
    private boolean globalFsync = false;
    private boolean globalJ = false;
    private boolean checkForNew = true;
    private int writeTimeout = 0;
    private boolean replicaset = true;

    //maximum number of tries to queue a write operation
    private int maximumRetriesBufferedWriter = 10;
    private int maximumRetriesWriter = 10;
    private int maximumRetriesAsyncWriter = 10;
    //wait bewteen tries
    private int retryWaitTimeBufferedWriter = 200;
    private int retryWaitTimeWriter = 200;
    private int retryWaitTimeAsyncWriter = 200;
    private int globalW = 1; //number of writes
    private int maxWaitTime = 2000;
    //default time for write buffer to be filled
    private int writeBufferTime = 1000;
    //ms for the pause of the main thread
    private int writeBufferTimeGranularity = 100;
    private boolean autoreconnect = true;
    private int maxAutoReconnectTime = 0;
    private int blockingThreadsMultiplier = 5;

    private boolean autoIndexAndCappedCreationOnWrite = true;

    @Transient
    private Class<? extends Query> queryClass;
    @Transient
    private Class<? extends Aggregator> aggregatorClass;
    @Transient
    private QueryFactory queryFact;
    @Transient
    private AggregatorFactory aggregatorFactory;
    @Transient
    private MorphiumCache cache;
    private int replicaSetMonitoringTimeout = 5000;
    private int retriesOnNetworkError = 1;
    private int sleepBetweenNetworkErrorRetries = 1000;
    /**
     * login credentials for MongoDB - if necessary. If null, don't authenticate
     */
    private String mongoLogin = null, mongoPassword = null;

    private boolean autoValues = true;
    private boolean readCacheEnabled = true;
    private boolean asyncWritesEnabled = true;
    private boolean bufferedWritesEnabled = true;
    private boolean camelCaseConversionEnabled = true;
    private boolean warnOnNoEntitySerialization = false;

    //securitysettings
    //    private Class<? extends Object> userClass, roleClass, aclClass;
    private String mongoAdminUser, mongoAdminPwd; //THE superuser!
    @Transient
    private Class<? extends MorphiumObjectMapper> omClass = ObjectMapperImpl.class;
    @Transient
    private Class<? extends MongoField> fieldImplClass = MongoFieldImpl.class;
    @Transient
    private ReadPreference defaultReadPreference;
    @Transient
    private String defaultReadPreferenceType;
    @Transient
    private Class<? extends EncryptionKeyProvider> encryptionKeyProviderClass = DefaultEncryptionKeyProvider.class;

    private String driverClass;
    private int acceptableLatencyDifference = 15;
    private int threadPoolMessagingCoreSize = 0;
    private int threadPoolMessagingMaxSize = 100;
    private long threadPoolMessagingKeepAliveTime = 2000;
    private int threadPoolAsyncOpCoreSize = 1;
    private int threadPoolAsyncOpMaxSize = 1000;
    private long threadPoolAsyncOpKeepAliveTime = 1000;
    private boolean objectSerializationEnabled = true;
    private boolean cursorFinalizerEnabled = false;
    private boolean alwaysUseMBeans = false;
    private int heartbeatConnectTimeout = 0;
    private int heartbeatFrequency = 1000;
    private int heartbeatSocketTimeout = 1000;
    private int minConnectionsPerHost = 1;
    private int minHearbeatFrequency = 2000;
    private int localThreashold = 0;
    private int maxConnectionIdleTime = 10000;
    private int maxConnectionLifeTime = 60000;

    private List<String> hostSeed = new ArrayList<>();

    private String defaultTags;
    private String requiredReplicaSetName = null;
    private int cursorBatchSize = 1000;
    private boolean oplogMonitorEnabled = false;
    private boolean throwExceptionOnMissingEncrytptionKey = true;

    public MorphiumConfig(final Properties prop) {
        this(null,prop);
    }
    public MorphiumConfig(String prefix,final Properties prop) {
        this(prefix, prop::get);
    }

    public MorphiumConfig(String prefix, MorphiumConfigResolver resolver) {
        AnnotationAndReflectionHelper an = new AnnotationAndReflectionHelper(true); //settings always convert camel case
        List<Field> flds = an.getAllFields(MorphiumConfig.class);
        if (prefix != null) {
            prefix += ".";
        } else {
            prefix = "";
        }
        for (Field f : flds) {
            String fName = prefix + f.getName();
            Object setting = resolver.resolveSetting(fName);
            if (setting ==null) continue;

            if (f.isAnnotationPresent(Transient.class)) {
                try {
                    parseClassSettings(fName, setting,this );
                } catch (InstantiationException | IllegalAccessException | NoSuchFieldException | ClassNotFoundException | NoSuchMethodException | InvocationTargetException e) {
                    throw new RuntimeException(e);
                }
                continue;
            }
            f.setAccessible(true);

            try {
                if (f.getType().equals(int.class) || f.getType().equals(Integer.class)) {
                    f.set(this, Integer.parseInt((String) setting));
                } else if (f.getType().equals(String.class)) {
                    f.set(this, setting);
                } else if (List.class.isAssignableFrom(f.getType())) {
                    String lst = (String) setting;
                    List<String> l = new ArrayList<>();
                    lst = lst.replaceAll("[\\[\\]]", "");
                    Collections.addAll(l, lst.split(","));
                    f.set(this, l);
                } else if (f.getType().equals(boolean.class) || f.getType().equals(Boolean.class)) {
                    f.set(this, setting.equals("true"));
                } else if (f.getType().equals(long.class) || f.getType().equals(Long.class)) {
                    f.set(this, Long.parseLong((String) setting));
                }
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        }
        if (hostSeed == null || hostSeed.isEmpty()) {
            String lst = (String) resolver.resolveSetting(prefix + "hosts");
            if (lst != null) {
                lst = lst.replaceAll("[\\[\\]]", "");
                for (String s : lst.split(",")) {
                    addHostToSeed(s);
                }
            }
        }


    }

    public MorphiumConfig() {
        this("test", 10, 60000, 10000);
    }

    public MorphiumConfig(String db, int maxConnections, int globalCacheValidTime, int housekeepingTimeout) {
        database = db;
        this.maxConnections = maxConnections;
        this.globalCacheValidTime = globalCacheValidTime;
        this.housekeepingTimeout = housekeepingTimeout;

    }

    public static List<String> getPropertyNames(String prefix) {
        @SuppressWarnings("unchecked") List<String> flds = new AnnotationAndReflectionHelper(true).getFields(MorphiumConfig.class);

        List<String> ret = new ArrayList<>();
        for (String f : flds) {
            ret.add(prefix + "." + f);
        }

        return ret;
    }


    public static MorphiumConfig createFromJson(String json) throws NoSuchFieldException, ClassNotFoundException, IllegalAccessException, InstantiationException, ParseException, NoSuchMethodException, InvocationTargetException {
        MorphiumConfig cfg = new ObjectMapperImpl().deserialize(MorphiumConfig.class, json);

        for (Object ko : cfg.restoreData.keySet()) {
            String k = (String) ko;
            parseClassSettings(k,cfg.restoreData.get(k), cfg);
            String value = cfg.restoreData.get(k).toString();
            if (k.equals("hosts") || k.equals("hostSeed")) {
                value = value.replaceAll("\\[", "").replaceAll("]", "");
                for (String adr : value.split(",")) {
                    String[] a = adr.split(":");
                    cfg.addHostToSeed(a[0].trim(), Integer.parseInt(a[1].trim()));
                }

            }
        }
        return cfg;
    }

    private static void parseClassSettings(String k, Object value, MorphiumConfig cfg) throws ClassNotFoundException, NoSuchFieldException, IllegalAccessException, InstantiationException, NoSuchMethodException, InvocationTargetException {
        if (!k.endsWith("ClassName")) {
            return ;
        }
        if (k.contains(".")) {
            k = k.substring(0, k.indexOf(".") + 1);
        }
        String[] n = k.split("_");
        if (n.length != 3) {
            return ;
        }
        Class cls = Class.forName((String)value);
        Field f = MorphiumConfig.class.getDeclaredField(n[0]);
        f.setAccessible(true);
        if (n[1].equals("C")) {
            f.set(cfg, cls);
        } else if (n[1].equals("I")) {
            //noinspection unchecked
            f.set(cfg, cls.getDeclaredConstructor().newInstance());
        }


        cfg.getAggregatorFactory().setAggregatorClass(cfg.getAggregatorClass());
        cfg.getQueryFact().setQueryImpl(cfg.getQueryClass());
    }

    public static MorphiumConfig fromProperties(String prefix, Properties p) {
        return new MorphiumConfig(prefix, p);
    }

    public static MorphiumConfig fromProperties(Properties p) {
        return new MorphiumConfig(p);
    }

    public boolean isReplicaset() {
        return replicaset;
    }

    public MorphiumConfig setReplicasetMonitoring(boolean replicaset) {
        this.replicaset = replicaset;
        return this;
    }

    public Class<? extends EncryptionKeyProvider> getEncryptionKeyProviderClass() {
        return encryptionKeyProviderClass;
    }

    public void setEncryptionKeyProviderClass(Class<? extends EncryptionKeyProvider> encryptionKeyProviderClass) {
        this.encryptionKeyProviderClass = encryptionKeyProviderClass;
    }

    public String getDriverClass() {
        if (driverClass == null) {
            driverClass = Driver.class.getName();
        }
        return driverClass;
    }

    public MorphiumConfig setDriverClass(String driverClass) {
        this.driverClass = driverClass;
        return this;
    }

    public boolean isAutoIndexAndCappedCreationOnWrite() {
        return autoIndexAndCappedCreationOnWrite;
    }

    public MorphiumConfig setAutoIndexAndCappedCreationOnWrite(boolean autoIndexAndCappedCreationOnWrite) {
        this.autoIndexAndCappedCreationOnWrite = autoIndexAndCappedCreationOnWrite;
        return this;
    }

    public boolean isWarnOnNoEntitySerialization() {
        return warnOnNoEntitySerialization;
    }

    public void setWarnOnNoEntitySerialization(boolean warnOnNoEntitySerialization) {
        this.warnOnNoEntitySerialization = warnOnNoEntitySerialization;
    }

    public boolean isCheckForNew() {
        return checkForNew;
    }

    /**
     * if set to false, all checks if an entity is new when CreationTime is used is switched off
     * if set to true, only those, whose CreationTime settings use checkfornew will work
     * default false
     *
     * @param checkForNew boolean, check if object is really not stored yet
     */
    public MorphiumConfig setCheckForNew(boolean checkForNew) {
        this.checkForNew = checkForNew;
        return this;
    }

    public int getRetriesOnNetworkError() {
        return retriesOnNetworkError;
    }

    public MorphiumConfig setRetriesOnNetworkError(int retriesOnNetworkError) {
        if (retriesOnNetworkError == 0) {
            LoggerFactory.getLogger(MorphiumConfig.class).warn("Cannot set retries on network error to 0 - minimum is 1");
            retriesOnNetworkError = 1;
        }
        this.retriesOnNetworkError = retriesOnNetworkError;
        return this;
    }

    public int getSleepBetweenNetworkErrorRetries() {
        return sleepBetweenNetworkErrorRetries;
    }

    public MorphiumConfig setSleepBetweenNetworkErrorRetries(int sleepBetweenNetworkErrorRetries) {
        this.sleepBetweenNetworkErrorRetries = sleepBetweenNetworkErrorRetries;
        return this;
    }

    public int getReplicaSetMonitoringTimeout() {
        return replicaSetMonitoringTimeout;
    }

    public MorphiumConfig setReplicaSetMonitoringTimeout(int replicaSetMonitoringTimeout) {
        this.replicaSetMonitoringTimeout = replicaSetMonitoringTimeout;
        return this;
    }

    public int getWriteBufferTimeGranularity() {
        return writeBufferTimeGranularity;
    }

    public MorphiumConfig setWriteBufferTimeGranularity(int writeBufferTimeGranularity) {
        this.writeBufferTimeGranularity = writeBufferTimeGranularity;
        return this;
    }

    public MorphiumCache getCache() {
        //        if (cache == null) {
        //            cache = new MorphiumCacheImpl();
        //
        //        }
        return cache;
    }

    public MorphiumConfig setCache(MorphiumCache cache) {
        this.cache = cache;
        return this;
    }

    public boolean isOplogMonitorEnabled() {
        return this.oplogMonitorEnabled;
    }

    public MorphiumConfig setOplogMonitorEnabled(boolean op) {
        this.oplogMonitorEnabled = op;
        return this;
    }

    public int getWriteBufferTime() {
        return writeBufferTime;
    }

    public MorphiumConfig setWriteBufferTime(int writeBufferTime) {
        this.writeBufferTime = writeBufferTime;
        return this;
    }

    public Class<? extends MorphiumObjectMapper> getOmClass() {
        return omClass;
    }

    public MorphiumConfig setOmClass(Class<? extends MorphiumObjectMapper> omClass) {
        this.omClass = omClass;
        return this;
    }

    public int getWriteTimeout() {
        return writeTimeout;
    }

    public MorphiumConfig setWriteTimeout(int writeTimeout) {
        this.writeTimeout = writeTimeout;
        return this;
    }

    public int getGlobalW() {
        return globalW;
    }

    public MorphiumConfig setGlobalW(int globalW) {
        this.globalW = globalW;
        return this;
    }

    public boolean isGlobalJ() {
        return globalJ;
    }

    public MorphiumConfig setGlobalJ(boolean globalJ) {
        this.globalJ = globalJ;
        return this;
    }

    public Class<? extends Query> getQueryClass() {
        if (queryClass == null) {
            queryClass = QueryImpl.class;
        }
        return queryClass;
    }

    public MorphiumConfig setQueryClass(Class<Query> queryClass) {
        this.queryClass = queryClass;
        return this;
    }

    public QueryFactory getQueryFact() {
        if (queryFact == null) {
            queryFact = new QueryFactoryImpl(getQueryClass());
        }
        return queryFact;
    }

    public MorphiumConfig setQueryFact(QueryFactory queryFact) {
        this.queryFact = queryFact;
        return this;
    }

    public AggregatorFactory getAggregatorFactory() {
        if (aggregatorFactory == null) {
            aggregatorFactory = new AggregatorFactoryImpl(getAggregatorClass());
        }
        return aggregatorFactory;
    }

    public MorphiumConfig setAggregatorFactory(AggregatorFactory aggregatorFactory) {
        this.aggregatorFactory = aggregatorFactory;
        return this;
    }

    public Class<? extends Aggregator> getAggregatorClass() {
        if (aggregatorClass == null) {
            aggregatorClass = AggregatorImpl.class;
        }
        return aggregatorClass;
    }

    public MorphiumConfig setAggregatorClass(Class<? extends Aggregator> aggregatorClass) {
        this.aggregatorClass = aggregatorClass;
        return this;
    }

    public boolean isGlobalFsync() {
        return globalFsync;
    }

    public MorphiumConfig setGlobalFsync(boolean globalFsync) {
        this.globalFsync = globalFsync;
        return this;
    }

    public boolean isSafeMode() {
        return safeMode;
    }

    public MorphiumConfig setSafeMode(boolean safeMode) {
        this.safeMode = safeMode;

        return this;
    }

    public int getBlockingThreadsMultiplier() {
        return blockingThreadsMultiplier;
    }

    public MorphiumConfig setBlockingThreadsMultiplier(int blockingThreadsMultiplier) {
        this.blockingThreadsMultiplier = blockingThreadsMultiplier;
        return this;
    }

    public MorphiumWriter getBufferedWriter() {
        if (bufferedWriter == null) {
            bufferedWriter = new BufferedMorphiumWriterImpl();
        }
        return bufferedWriter;

    }

    public MorphiumConfig setBufferedWriter(MorphiumWriter bufferedWriter) {
        this.bufferedWriter = bufferedWriter;
        return this;
    }

    public MorphiumWriter getWriter() {
        if (writer == null) {
            writer = new MorphiumWriterImpl();
        }
        return writer;
    }

    public MorphiumConfig setWriter(MorphiumWriter writer) {
        this.writer = writer;
        return this;
    }

    public int getConnectionTimeout() {
        return connectionTimeout;
    }

    public MorphiumConfig setConnectionTimeout(int connectionTimeout) {
        this.connectionTimeout = connectionTimeout;
        return this;
    }

    public int getSocketTimeout() {
        return socketTimeout;
    }

    public MorphiumConfig setSocketTimeout(int socketTimeout) {
        this.socketTimeout = socketTimeout;
        return this;
    }

    public Class<? extends MongoField> getFieldImplClass() {
        return fieldImplClass;
    }

    public MorphiumConfig setFieldImplClass(Class<? extends MongoField> fieldImplClass) {
        this.fieldImplClass = fieldImplClass;
        return this;
    }

    public int getMaxWaitTime() {
        return maxWaitTime;
    }

    public MorphiumConfig setMaxWaitTime(int maxWaitTime) {
        this.maxWaitTime = maxWaitTime;
        return this;
    }

    public boolean isAutoreconnect() {
        return autoreconnect;
    }

    public MorphiumConfig setAutoreconnect(boolean autoreconnect) {
        this.autoreconnect = autoreconnect;
        return this;
    }

    public int getMaxAutoReconnectTime() {
        return maxAutoReconnectTime;
    }

    public MorphiumConfig setMaxAutoReconnectTime(int maxAutoReconnectTime) {
        this.maxAutoReconnectTime = maxAutoReconnectTime;
        return this;
    }

    public boolean isSocketKeepAlive() {
        return socketKeepAlive;
    }

    public MorphiumConfig setSocketKeepAlive(boolean socketKeepAlive) {
        this.socketKeepAlive = socketKeepAlive;
        return this;
    }

    public String getMongoLogin() {
        return mongoLogin;
    }

    public MorphiumConfig setMongoLogin(String mongoLogin) {
        this.mongoLogin = mongoLogin;
        return this;
    }

    public String getMongoPassword() {
        return mongoPassword;
    }

    public MorphiumConfig setMongoPassword(String mongoPassword) {
        this.mongoPassword = mongoPassword;
        return this;
    }

    public ReadPreference getDefaultReadPreference() {
        return defaultReadPreference;
    }

    public MorphiumConfig setDefaultReadPreference(ReadPreference defaultReadPreference) {
        this.defaultReadPreference = defaultReadPreference;
        return this;
    }

    public String getDefaultReadPreferenceType() {
        return defaultReadPreferenceType;
    }

    public MorphiumConfig setDefaultReadPreferenceType(String stringDefaultReadPreference) {
        this.defaultReadPreferenceType = stringDefaultReadPreference;

        ReadPreferenceType readPreferenceType;
        try {
            readPreferenceType = ReadPreferenceType.valueOf(stringDefaultReadPreference.toUpperCase());
        } catch (IllegalArgumentException e) {
            readPreferenceType = null;
        }
        if (readPreferenceType == null) {
            throw new RuntimeException("Could not set defaultReadPreferenceByString " + stringDefaultReadPreference);
        }
        ReadPreference defaultReadPreference = new ReadPreference();
        defaultReadPreference.setType(readPreferenceType);
        this.defaultReadPreference = defaultReadPreference;

        return this;
    }

    public String getMongoAdminUser() {
        return mongoAdminUser;
    }

    public MorphiumConfig setMongoAdminUser(String mongoAdminUser) {
        this.mongoAdminUser = mongoAdminUser;
        return this;
    }

    public String getMongoAdminPwd() {
        return mongoAdminPwd;
    }

    public MorphiumConfig setMongoAdminPwd(String mongoAdminPwd) {
        this.mongoAdminPwd = mongoAdminPwd;
        return this;
    }

    public int getWriteCacheTimeout() {
        return writeCacheTimeout;
    }

    public MorphiumConfig setWriteCacheTimeout(int writeCacheTimeout) {
        this.writeCacheTimeout = writeCacheTimeout;
        return this;
    }

    /**
     * setting hosts as Host:Port
     *
     * @param str list of hosts, with or without port
     */
    public MorphiumConfig setHostSeed(List<String> str) {
        hostSeed = str;
        return this;
    }

    public MorphiumConfig setHostSeed(List<String> str, List<Integer> ports) {
        hostSeed.clear();
        for (int i = 0; i < str.size(); i++) {
            String host = str.get(i).replaceAll(" ", "") + ":" + ports.get(i);
            hostSeed.add(host);
        }
        return this;
    }


    public List<String> getHostSeed() {
        return hostSeed;
    }

    public MorphiumConfig setHostSeed(String... hostPorts) {
        hostSeed.clear();
        for (String h : hostPorts) {
            addHostToSeed(h);
        }
        return this;
    }

    public MorphiumConfig setHostSeed(String hostPorts) {
        hostSeed.clear();
        String[] h = hostPorts.split(",");
        for (String host : h) {
            addHostToSeed(host);
        }
        return this;
    }

    public MorphiumConfig setHostSeed(String hosts, String ports) {
        hostSeed.clear();
        hosts = hosts.replaceAll(" ", "");
        ports = ports.replaceAll(" ", "");
        String[] h = hosts.split(",");
        String[] p = ports.split(",");
        for (int i = 0; i < h.length; i++) {
            if (p.length < i) {
                addHostToSeed(h[i], 27017);
            } else {
                addHostToSeed(h[i], Integer.parseInt(p[i]));
            }
        }
        return this;

    }

    public MorphiumConfig addHostToSeed(String host, int port) {
        host = host.replaceAll(" ", "") + ":" + port;
        if (hostSeed == null) {
            hostSeed = new ArrayList<>();
        }
        hostSeed.add(host);
        return this;
    }

    public MorphiumConfig addHostToSeed(String host) {
        host = host.replaceAll(" ", "");
        if (host.contains(":")) {
            String[] h = host.split(":");
            addHostToSeed(h[0], Integer.parseInt(h[1]));
        } else {
            addHostToSeed(host, 27017);
        }
        return this;
    }

    public int getMaxConnections() {
        return maxConnections;
    }

    public MorphiumConfig setMaxConnections(int maxConnections) {
        this.maxConnections = maxConnections;
        return this;
    }

    /**
     * for future use - set Global Caching time
     *
     * @return the global cache valid time
     */
    public int getGlobalCacheValidTime() {
        return globalCacheValidTime;
    }

    public MorphiumConfig setGlobalCacheValidTime(int globalCacheValidTime) {
        this.globalCacheValidTime = globalCacheValidTime;

        return this;
    }

    public String getDatabase() {
        return database;
    }

    public MorphiumConfig setDatabase(String database) {
        this.database = database;
        return this;
    }

    public int getHousekeepingTimeout() {
        return housekeepingTimeout;
    }

    public MorphiumConfig setHousekeepingTimeout(int housekeepingTimeout) {
        this.housekeepingTimeout = housekeepingTimeout;
        return this;
    }

    public long getValidTime() {
        return globalCacheValidTime;
    }

    public MorphiumConfig setValidTime(int tm) {
        globalCacheValidTime = tm;
        return this;
    }

    /**
     * returns json representation of this object containing all values
     *
     * @return json string
     */
    @Override
    public String toString() {
        updateAdditionals();
        try {
            return Utils.toJsonString(getOmClass().getDeclaredConstructor().newInstance().serialize(this));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void updateAdditionals() {
        restoreData = new HashMap<>();
        addClassSettingsTo("", restoreData);


    }

    private void addClassSettingsTo(String prefix, Map p) {
        MorphiumConfig defaults = new MorphiumConfig();
        getWriter();
        getBufferedWriter();
        getAsyncWriter();

        if (!defaults.getWriter().getClass().equals(getWriter().getClass())) {
            //noinspection unchecked
            p.put(prefix + "writer_I_ClassName", getWriter().getClass().getName());
        }
        if (!defaults.getBufferedWriter().getClass().equals(getBufferedWriter().getClass())) {
            //noinspection unchecked
            p.put(prefix + "bufferedWriter_I_ClassName", getBufferedWriter().getClass().getName());
        }
        if (!defaults.getAsyncWriter().getClass().equals(getAsyncWriter().getClass())) {
            //noinspection unchecked
            p.put(prefix + "asyncWriter_I_ClassName", getAsyncWriter().getClass().getName());
        }
        if ((getCache() != null)) {
            //noinspection unchecked
            p.put(prefix + "cache_I_ClassName", getCache().getClass().getName());
        }
        if (!defaults.getAggregatorClass().equals(getAggregatorClass())) {
            //noinspection unchecked
            p.put(prefix + "aggregatorClass_C_ClassName", getAggregatorClass().getName());
        }
        if (!defaults.getAggregatorFactory().getClass().equals(getAggregatorFactory().getClass())) {
            //noinspection unchecked
            p.put(prefix + "aggregatorFactory_I_ClassName", getAggregatorFactory().getClass().getName());
        }
        if (!defaults.getOmClass().equals(getOmClass())) {
            //noinspection unchecked
            p.put(prefix + "omClass_C_ClassName", getOmClass().getName());
        }
        if (!defaults.getQueryClass().equals(getQueryClass())) {
            //noinspection unchecked
            p.put(prefix + "queryClass_C_ClassName", getQueryClass().getName());
        }
        if (!defaults.getQueryFact().getClass().equals(getQueryFact().getClass())) {
            //noinspection unchecked
            p.put(prefix + "queryFact_I_ClassName", getQueryFact().getClass().getName());
        }
    }

    public MorphiumWriter getAsyncWriter() {
        if (asyncWriter == null) {
            asyncWriter = new AsyncWriterImpl();
        }
        return asyncWriter;
    }

    public MorphiumConfig setAsyncWriter(MorphiumWriter asyncWriter) {
        this.asyncWriter = asyncWriter;
        return this;
    }

    public int getMaximumRetriesBufferedWriter() {
        return maximumRetriesBufferedWriter;
    }

    public MorphiumConfig setMaximumRetriesBufferedWriter(int maximumRetriesBufferedWriter) {
        this.maximumRetriesBufferedWriter = maximumRetriesBufferedWriter;
        return this;
    }

    public int getMaximumRetriesWriter() {
        return maximumRetriesWriter;
    }

    public MorphiumConfig setMaximumRetriesWriter(int maximumRetriesWriter) {
        this.maximumRetriesWriter = maximumRetriesWriter;
        return this;
    }

    public int getMaximumRetriesAsyncWriter() {
        return maximumRetriesAsyncWriter;
    }

    public MorphiumConfig setMaximumRetriesAsyncWriter(int maximumRetriesAsyncWriter) {
        this.maximumRetriesAsyncWriter = maximumRetriesAsyncWriter;
        return this;
    }

    public int getRetryWaitTimeBufferedWriter() {
        return retryWaitTimeBufferedWriter;
    }

    public MorphiumConfig setRetryWaitTimeBufferedWriter(int retryWaitTimeBufferedWriter) {
        this.retryWaitTimeBufferedWriter = retryWaitTimeBufferedWriter;
        return this;
    }

    public int getRetryWaitTimeWriter() {
        return retryWaitTimeWriter;
    }

    public MorphiumConfig setRetryWaitTimeWriter(int retryWaitTimeWriter) {
        this.retryWaitTimeWriter = retryWaitTimeWriter;
        return this;
    }

    public int getRetryWaitTimeAsyncWriter() {
        return retryWaitTimeAsyncWriter;
    }

    public MorphiumConfig setRetryWaitTimeAsyncWriter(int retryWaitTimeAsyncWriter) {
        this.retryWaitTimeAsyncWriter = retryWaitTimeAsyncWriter;
        return this;
    }

    /**
     * returns a property set only containing non-default values set
     *
     * @return properties
     */
    public Properties asProperties() {
        return asProperties(null);
    }

    public Properties asProperties(String prefix) {
        return asProperties(prefix, true);
    }

    /**
     * @param prefix          prefix to use in property keys
     * @param effectiveConfig when true, use the current effective config, including overrides from Environment
     * @return the properties
     */
    public Properties asProperties(String prefix, boolean effectiveConfig) {
        if (prefix == null) {
            prefix = "";
        } else {
            prefix = prefix + ".";
        }
        MorphiumConfig defaults = new MorphiumConfig();
        Properties p = new Properties();
        AnnotationAndReflectionHelper an = new AnnotationAndReflectionHelper(true);
        List<Field> flds = an.getAllFields(MorphiumConfig.class);
        for (Field f : flds) {
            if (f.isAnnotationPresent(Transient.class)) {
                continue;
            }
            f.setAccessible(true);
            try {
                if (f.get(this) != null && !f.get(this).equals(f.get(defaults))) {
                    p.put(prefix + f.getName(), f.get(this).toString());
                }
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        }
        addClassSettingsTo(prefix, p);

        if (effectiveConfig) {
            Properties sysprop = System.getProperties();

            for (Object sysk : sysprop.keySet()) {
                String k = (String) sysk;
                if (k.startsWith("morphium.")) {
                    String value = sysprop.get(k).toString();
                    k = k.substring(9);
                    p.put(prefix + k, value);
                }
            }
        }
        return p;
    }

    public boolean isReadCacheEnabled() {
        return readCacheEnabled;
    }

    public MorphiumConfig setReadCacheEnabled(boolean readCacheEnabled) {
        this.readCacheEnabled = readCacheEnabled;
        return this;
    }

    public MorphiumConfig disableReadCache() {
        this.readCacheEnabled = false;
        return this;
    }

    public MorphiumConfig enableReadCache() {
        this.readCacheEnabled = true;
        return this;
    }

    public boolean isAsyncWritesEnabled() {
        return asyncWritesEnabled;
    }

    public MorphiumConfig setAsyncWritesEnabled(boolean asyncWritesEnabled) {
        this.asyncWritesEnabled = asyncWritesEnabled;
        return this;
    }

    public MorphiumConfig disableAsyncWrites() {
        asyncWritesEnabled = false;
        return this;
    }

    public MorphiumConfig enableAsyncWrites() {
        asyncWritesEnabled = true;
        return this;
    }

    public boolean isBufferedWritesEnabled() {
        return bufferedWritesEnabled;
    }

    public MorphiumConfig setBufferedWritesEnabled(boolean bufferedWritesEnabled) {
        this.bufferedWritesEnabled = bufferedWritesEnabled;
        return this;
    }

    public MorphiumConfig disableBufferedWrites() {
        bufferedWritesEnabled = false;
        return this;
    }

    public MorphiumConfig enableBufferedWrites() {
        bufferedWritesEnabled = true;
        return this;
    }

    public boolean isAutoValuesEnabled() {
        return autoValues;
    }

    public MorphiumConfig setAutoValuesEnabled(boolean enabled) {
        autoValues = enabled;
        return this;
    }

    public MorphiumConfig enableAutoValues() {
        autoValues = true;
        return this;
    }

    public MorphiumConfig disableAutoValues() {
        autoValues = false;
        return this;
    }

    public int getAcceptableLatencyDifference() {
        return acceptableLatencyDifference;
    }

    public MorphiumConfig setAcceptableLatencyDifference(int acceptableLatencyDifference) {
        this.acceptableLatencyDifference = acceptableLatencyDifference;
        return this;
    }

    public boolean isCamelCaseConversionEnabled() {
        return camelCaseConversionEnabled;
    }

    public MorphiumConfig setCamelCaseConversionEnabled(boolean camelCaseConversionEnabled) {
        this.camelCaseConversionEnabled = camelCaseConversionEnabled;
        return this;
    }

    public int getThreadPoolMessagingCoreSize() {
        return threadPoolMessagingCoreSize;
    }

    public MorphiumConfig setThreadPoolMessagingCoreSize(int threadPoolMessagingCoreSize) {
        this.threadPoolMessagingCoreSize = threadPoolMessagingCoreSize;
        return this;
    }

    public int getThreadPoolMessagingMaxSize() {
        return threadPoolMessagingMaxSize;
    }

    public MorphiumConfig setThreadPoolMessagingMaxSize(int threadPoolMessagingMaxSize) {
        this.threadPoolMessagingMaxSize = threadPoolMessagingMaxSize;
        return this;
    }

    public long getThreadPoolMessagingKeepAliveTime() {
        return threadPoolMessagingKeepAliveTime;
    }

    public MorphiumConfig setThreadPoolMessagingKeepAliveTime(long threadPoolMessagingKeepAliveTime) {
        this.threadPoolMessagingKeepAliveTime = threadPoolMessagingKeepAliveTime;
        return this;
    }

    public int getThreadPoolAsyncOpCoreSize() {
        return threadPoolAsyncOpCoreSize;
    }

    public MorphiumConfig setThreadPoolAsyncOpCoreSize(int threadPoolAsyncOpCoreSize) {
        this.threadPoolAsyncOpCoreSize = threadPoolAsyncOpCoreSize;
        return this;
    }

    public int getThreadPoolAsyncOpMaxSize() {
        return threadPoolAsyncOpMaxSize;
    }

    public MorphiumConfig setThreadPoolAsyncOpMaxSize(int threadPoolAsyncOpMaxSize) {
        this.threadPoolAsyncOpMaxSize = threadPoolAsyncOpMaxSize;
        return this;
    }

    public long getThreadPoolAsyncOpKeepAliveTime() {
        return threadPoolAsyncOpKeepAliveTime;
    }

    public MorphiumConfig setThreadPoolAsyncOpKeepAliveTime(long threadPoolAsyncOpKeepAliveTime) {
        this.threadPoolAsyncOpKeepAliveTime = threadPoolAsyncOpKeepAliveTime;
        return this;
    }

    public boolean isObjectSerializationEnabled() {
        return objectSerializationEnabled;
    }

    public MorphiumConfig setObjectSerializationEnabled(boolean objectSerializationEnabled) {
        this.objectSerializationEnabled = objectSerializationEnabled;
        return this;
    }

    public boolean isCursorFinalizerEnabled() {
        return cursorFinalizerEnabled;
    }

    public MorphiumConfig setCursorFinalizerEnabled(boolean cursorFinalizerEnabled) {
        this.cursorFinalizerEnabled = cursorFinalizerEnabled;
        return this;
    }

    public boolean isAlwaysUseMBeans() {
        return alwaysUseMBeans;
    }

    public MorphiumConfig setAlwaysUseMBeans(boolean alwaysUseMBeans) {
        this.alwaysUseMBeans = alwaysUseMBeans;
        return this;
    }

    public int getHeartbeatConnectTimeout() {
        return heartbeatConnectTimeout;
    }

    public MorphiumConfig setHeartbeatConnectTimeout(int heartbeatConnectTimeout) {
        this.heartbeatConnectTimeout = heartbeatConnectTimeout;
        return this;
    }

    public int getHeartbeatFrequency() {
        return heartbeatFrequency;
    }

    public MorphiumConfig setHeartbeatFrequency(int heartbeatFrequency) {
        this.heartbeatFrequency = heartbeatFrequency;
        return this;
    }

    public int getHeartbeatSocketTimeout() {
        return heartbeatSocketTimeout;
    }

    public MorphiumConfig setHeartbeatSocketTimeout(int heartbeatSocketTimeout) {
        this.heartbeatSocketTimeout = heartbeatSocketTimeout;
        return this;
    }

    public int getMinConnectionsPerHost() {
        return minConnectionsPerHost;
    }

    public MorphiumConfig setMinConnectionsPerHost(int minConnectionsPerHost) {
        this.minConnectionsPerHost = minConnectionsPerHost;
        return this;
    }

    public int getMinHearbeatFrequency() {
        return minHearbeatFrequency;
    }

    public MorphiumConfig setMinHearbeatFrequency(int minHearbeatFrequency) {
        this.minHearbeatFrequency = minHearbeatFrequency;
        return this;
    }

    public int getLocalThreashold() {
        return localThreashold;
    }

    public MorphiumConfig setLocalThreashold(int localThreashold) {
        this.localThreashold = localThreashold;
        return this;
    }

    public int getMaxConnectionIdleTime() {
        return maxConnectionIdleTime;
    }

    public MorphiumConfig setMaxConnectionIdleTime(int maxConnectionIdleTime) {
        this.maxConnectionIdleTime = maxConnectionIdleTime;
        return this;
    }

    public int getMaxConnectionLifeTime() {
        return maxConnectionLifeTime;
    }

    public MorphiumConfig setMaxConnectionLifeTime(int maxConnectionLifeTime) {
        this.maxConnectionLifeTime = maxConnectionLifeTime;
        return this;
    }

    public String getRequiredReplicaSetName() {
        return requiredReplicaSetName;
    }

    public MorphiumConfig setRequiredReplicaSetName(String requiredReplicaSetName) {
        this.requiredReplicaSetName = requiredReplicaSetName;
        return this;
    }


    public String getDefaultTags() {
        return defaultTags;
    }

    public MorphiumConfig addDefaultTag(String name, String value) {
        if (defaultTags != null) {
            defaultTags += ",";
        } else {
            defaultTags = "";
        }
        defaultTags += name + ":" + value;
        return this;
    }


    public List<Map<String, String>> getDefaultTagSet() {
        if (defaultTags == null) {
            return null;
        }
        List<Map<String, String>> tagList = new ArrayList<>();

        for (String t : defaultTags.split(",")) {
            String[] tag = t.split(":");
            tagList.add(Utils.getMap(tag[0], tag[1]));
        }
        return tagList;
    }

    public int getCursorBatchSize() {
        return cursorBatchSize;
    }

    public MorphiumConfig setCursorBatchSize(int cursorBatchSize) {
        this.cursorBatchSize = cursorBatchSize;
        return this;
    }


}