package de.caluga.morphium;

/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

import com.mongodb.ServerAddress;
import de.caluga.morphium.aggregation.Aggregator;
import de.caluga.morphium.aggregation.AggregatorFactory;
import de.caluga.morphium.aggregation.AggregatorFactoryImpl;
import de.caluga.morphium.aggregation.AggregatorImpl;
import de.caluga.morphium.annotations.ReadPreferenceLevel;
import de.caluga.morphium.cache.MorphiumCache;
import de.caluga.morphium.cache.MorphiumCacheImpl;
import de.caluga.morphium.query.*;
import de.caluga.morphium.writer.AsyncWriterImpl;
import de.caluga.morphium.writer.BufferedMorphiumWriterImpl;
import de.caluga.morphium.writer.MorphiumWriter;
import de.caluga.morphium.writer.MorphiumWriterImpl;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.xml.DOMConfigurator;

import java.io.IOException;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.*;

/**
 * Stores the configuration for the MongoDBLayer.
 *
 * @author stephan
 */
@SuppressWarnings("UnusedDeclaration")
public class MorphiumConfig {

    //    private MongoDbMode mode;
    private int maxConnections, housekeepingTimeout;
    private int globalCacheValidTime = 5000;
    private int writeCacheTimeout = 5000;
    private String database;
    private MorphiumWriter writer = new MorphiumWriterImpl();
    private MorphiumWriter bufferedWriter = new BufferedMorphiumWriterImpl();
    private MorphiumWriter asyncWriter = new AsyncWriterImpl();

    private int connectionTimeout = 0;
    private int socketTimeout = 0;
    private boolean socketKeepAlive = true;
    private boolean safeMode = false;
    private boolean globalFsync = false;
    private boolean globalJ = false;
    private int writeTimeout = 0;

    private int globalW = 1; //number of writes


    private int maxWaitTime = 120000;
    //default time for write buffer to be filled
    private int writeBufferTime = 1000;
    //ms for the pause of the main thread

    private int writeBufferTimeGranularity = 100;
    private boolean autoreconnect = true;
    private int maxAutoReconnectTime = 0;
    private int blockingThreadsMultiplier = 5;

    private Class<? extends Query> queryClass;
    private Class<? extends Aggregator> aggregatorClass;

    private QueryFactory queryFact;
    private AggregatorFactory aggregatorFactory;
    private MorphiumCache cache;


    /**
     * login credentials for MongoDB - if necessary. If null, don't authenticate
     */
    private String mongoLogin = null, mongoPassword = null;
    private int configManagerCacheTimeout = 1000 * 60 * 60; //one hour
    private List<ServerAddress> adr;
    private Map<String, Integer> validTimeByClassName;

    private ConfigManager configManager;
    //securitysettings
//    private Class<? extends Object> userClass, roleClass, aclClass;
    private String mongoAdminUser, mongoAdminPwd; //THE superuser!

    private Class<? extends ObjectMapper> omClass = ObjectMapperImpl.class;
    private Class<? extends MongoField> fieldImplClass = MongoFieldImpl.class;

    private ReadPreferenceLevel defaultReadPreference;
    private Class<? extends MorphiumIterator> iteratorClass;


    public int getWriteBufferTimeGranularity() {
        return writeBufferTimeGranularity;
    }

    public void setWriteBufferTimeGranularity(int writeBufferTimeGranularity) {
        this.writeBufferTimeGranularity = writeBufferTimeGranularity;
    }

    public MorphiumCache getCache() {
        if (cache == null) {
            cache = new MorphiumCacheImpl();
        }
        return cache;
    }

    public void setCache(MorphiumCache cache) {
        this.cache = cache;
    }

    public int getWriteBufferTime() {
        return writeBufferTime;
    }

    public void setWriteBufferTime(int writeBufferTime) {
        this.writeBufferTime = writeBufferTime;
    }

    public Class<? extends ObjectMapper> getOmClass() {
        return omClass;
    }

    public void setOmClass(Class<? extends ObjectMapper> omClass) {
        this.omClass = omClass;
    }

    public int getWriteTimeout() {
        return writeTimeout;
    }

    public void setWriteTimeout(int writeTimeout) {
        this.writeTimeout = writeTimeout;
    }

    public int getGlobalW() {
        return globalW;
    }

    public void setGlobalW(int globalW) {
        this.globalW = globalW;
    }

    public boolean isGlobalJ() {
        return globalJ;
    }

    public void setGlobalJ(boolean globalJ) {
        this.globalJ = globalJ;
    }

    public Class<? extends Query> getQueryClass() {
        if (queryClass == null) {
            queryClass = QueryImpl.class;
        }
        return queryClass;
    }

    public void setQueryClass(Class<Query> queryClass) {
        this.queryClass = queryClass;
    }

    public QueryFactory getQueryFact() {
        if (queryFact == null) {
            queryFact = new QueryFactoryImpl(getQueryClass());
        }
        return queryFact;
    }

    public void setQueryFact(QueryFactory queryFact) {
        this.queryFact = queryFact;
    }

    public AggregatorFactory getAggregatorFactory() {
        if (aggregatorFactory == null) {
            aggregatorFactory = new AggregatorFactoryImpl(getAggregatorClass());
        }
        return aggregatorFactory;
    }

    public void setAggregatorFactory(AggregatorFactory aggregatorFactory) {
        this.aggregatorFactory = aggregatorFactory;
    }

    public Class<? extends Aggregator> getAggregatorClass() {
        if (aggregatorClass == null) {
            aggregatorClass = AggregatorImpl.class;
        }
        return aggregatorClass;
    }

    public boolean isGlobalFsync() {
        return globalFsync;
    }

    public void setGlobalFsync(boolean globalFsync) {
        this.globalFsync = globalFsync;
    }

    public boolean isSafeMode() {
        return safeMode;
    }

    public void setSafeMode(boolean safeMode) {
        this.safeMode = safeMode;
    }

    public void setAggregatorClass(Class<? extends Aggregator> aggregatorClass) {
        this.aggregatorClass = aggregatorClass;
    }

    public int getBlockingThreadsMultiplier() {
        return blockingThreadsMultiplier;
    }

    public void setBlockingThreadsMultiplier(int blockingThreadsMultiplier) {
        this.blockingThreadsMultiplier = blockingThreadsMultiplier;
    }

    public MorphiumWriter getBufferedWriter() {
        return bufferedWriter;
    }

    public void setBufferedWriter(MorphiumWriter bufferedWriter) {
        this.bufferedWriter = bufferedWriter;
    }

    public MorphiumWriter getWriter() {
        return writer;
    }

    public void setWriter(MorphiumWriter writer) {
        this.writer = writer;
    }

    public ConfigManager getConfigManager() {
        return configManager;
    }

    public void setConfigManager(ConfigManager configManager) {
        this.configManager = configManager;
    }

    public int getConnectionTimeout() {
        return connectionTimeout;
    }

    public int getSocketTimeout() {
        return socketTimeout;
    }

    public void setSocketTimeout(int socketTimeout) {
        this.socketTimeout = socketTimeout;
    }

    public void setConnectionTimeout(int connectionTimeout) {
        this.connectionTimeout = connectionTimeout;
    }

    public Class<? extends MongoField> getFieldImplClass() {
        return fieldImplClass;
    }

    public int getMaxWaitTime() {
        return maxWaitTime;
    }

    public void setMaxWaitTime(int maxWaitTime) {
        this.maxWaitTime = maxWaitTime;
    }

    public boolean isAutoreconnect() {
        return autoreconnect;
    }

    public void setAutoreconnect(boolean autoreconnect) {
        this.autoreconnect = autoreconnect;
    }

    public int getMaxAutoReconnectTime() {
        return maxAutoReconnectTime;
    }

    public void setMaxAutoReconnectTime(int maxAutoReconnectTime) {
        this.maxAutoReconnectTime = maxAutoReconnectTime;
    }

    public boolean isSocketKeepAlive() {
        return socketKeepAlive;
    }

    public void setSocketKeepAlive(boolean socketKeepAlive) {
        this.socketKeepAlive = socketKeepAlive;
    }


    public String getMongoLogin() {
        return mongoLogin;
    }

    public void setMongoLogin(String mongoLogin) {
        this.mongoLogin = mongoLogin;
    }

    public String getMongoPassword() {
        return mongoPassword;
    }

    public void setMongoPassword(String mongoPassword) {
        this.mongoPassword = mongoPassword;
    }

    public ReadPreferenceLevel getDefaultReadPreference() {
        return defaultReadPreference;
    }

    public void setDefaultReadPreference(ReadPreferenceLevel defaultReadPreference) {
        this.defaultReadPreference = defaultReadPreference;
    }

    public String getMongoAdminUser() {
        return mongoAdminUser;
    }

    public void setMongoAdminUser(String mongoAdminUser) {
        this.mongoAdminUser = mongoAdminUser;
    }

    public String getMongoAdminPwd() {
        return mongoAdminPwd;
    }

    public void setMongoAdminPwd(String mongoAdminPwd) {
        this.mongoAdminPwd = mongoAdminPwd;
    }

    public int getConfigManagerCacheTimeout() {
        return configManagerCacheTimeout;
    }

    public void setConfigManagerCacheTimeout(int configManagerCacheTimeout) {
        this.configManagerCacheTimeout = configManagerCacheTimeout;
    }

    public int getWriteCacheTimeout() {
        return writeCacheTimeout;
    }

    public void setWriteCacheTimeout(int writeCacheTimeout) {
        this.writeCacheTimeout = writeCacheTimeout;
    }

    public Map<String, Integer> getValidTimeByClassName() {
        return validTimeByClassName;
    }

    public void setValidTimeByClassName(Map<String, Integer> validTimeByClassName) {
        this.validTimeByClassName = validTimeByClassName;
    }

    public void setMaxConnections(int maxConnections) {
        this.maxConnections = maxConnections;
    }

    public void setHousekeepingTimeout(int housekeepingTimeout) {
        this.housekeepingTimeout = housekeepingTimeout;
    }

    public void setGlobalCacheValidTime(int globalCacheValidTime) {
        this.globalCacheValidTime = globalCacheValidTime;
    }


    public List<ServerAddress> getAdr() {
        return adr;
    }

    /**
     * add addresses to your servers here. Depending on isREplicaSet() and isPaired() one ore more server addresses are needed
     */
    public void setAdr(List<ServerAddress> adr) {
        this.adr = adr;
    }

    /**
     * add addresses to your servers here. Depending on isREplicaSet() and isPaired() one ore more server addresses are needed
     */
    public void addAddress(String host, int port) throws UnknownHostException {
        ServerAddress sa = new ServerAddress(host, port);
        adr.add(sa);
    }

    public int getMaxConnections() {
        return maxConnections;
    }

    /**
     * for future use - set Global Caching time
     *
     * @return the global cache valid time
     */
    public int getGlobalCacheValidTime() {
        return globalCacheValidTime;
    }

    public MorphiumConfig(String db, int maxConnections, int globalCacheValidTime, int housekeepingTimeout) throws IOException {
        this(db, maxConnections, globalCacheValidTime, housekeepingTimeout, Thread.currentThread().getContextClassLoader().getResource("morphium-log4j.xml"));
    }

    public MorphiumConfig(String db, int maxConnections, int globalCacheValidTime, int housekeepingTimeout, String resourceName) throws IOException {
        this(db, maxConnections, globalCacheValidTime, housekeepingTimeout, Thread.currentThread().getContextClassLoader().getResource(resourceName));
    }

    public MorphiumConfig(String db, int maxConnections, int globalCacheValidTime, int housekeepingTimeout, URL loggingConfigResource) {


        validTimeByClassName = new Hashtable<String, Integer>();
        database = db;
        adr = new Vector<ServerAddress>();
        this.maxConnections = maxConnections;
        this.globalCacheValidTime = globalCacheValidTime;
        this.housekeepingTimeout = housekeepingTimeout;

        if (loggingConfigResource != null && !isLoggingConfigured()) {
            DOMConfigurator.configure(loggingConfigResource);
        }


    }

    /**
     * Returns true if it appears that log4j have been previously configured.
     * http://wiki.apache.org/logging-log4j/UsefulCode
     */
    private static boolean isLoggingConfigured() {
        Enumeration appenders = Logger.getRootLogger().getAllAppenders();
        if (appenders.hasMoreElements()) {
            return true;
        } else {
            Enumeration loggers = LogManager.getCurrentLoggers();
            while (loggers.hasMoreElements()) {
                Logger c = (Logger) loggers.nextElement();
                if (c.getAllAppenders().hasMoreElements())
                    return true;
            }
        }
        return false;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public int getHousekeepingTimeout() {
        return housekeepingTimeout;
    }

    public void setValidTime(int tm) {
        globalCacheValidTime = tm;
    }

    public void setValidTimeForClass(String cls, int tm) {
        validTimeByClassName.put(cls, tm);
    }

    public long getValidTimeForClass(String cls) {
        return validTimeByClassName.get(cls);
    }

    public long getValidTime() {
        return globalCacheValidTime;
    }

    @Override
    public String toString() {
        return "MorphiumConfig{" +
                " maxConnections=" + maxConnections +
                ", housekeepingTimeout=" + housekeepingTimeout +
                ", globalCacheValidTime=" + globalCacheValidTime +
                ", writeCacheTimeout=" + writeCacheTimeout +
                ", database='" + database + '\'' +
                ", connectionTimeout=" + connectionTimeout +
                ", socketTimeout=" + socketTimeout +
                ", socketKeepAlive=" + socketKeepAlive +
                ", slaveOk=" + defaultReadPreference.toString() +
                ", mongoLogin='" + mongoLogin + '\'' +
                ", mongoPassword='" + mongoPassword + '\'' +
                ", mongoAdminUser='" + mongoAdminUser + '\'' +
                ", mongoAdminPassword='" + mongoAdminPwd + '\'' +

                ", configManagerCacheTimeout=" + configManagerCacheTimeout +
                ", adr=" + adr +
                ", validTimeByClassName=" + validTimeByClassName +
                ", configManager=" + configManager +
                ", mongoAdminUser='" + mongoAdminUser + '\'' +
                ", mongoAdminPwd='" + mongoAdminPwd + '\'' +
                ", mapperClass=" + omClass.toString() +
                ", fieldImplClass='" + fieldImplClass + '\'' +
                '}';
    }

    public Properties getProperties() {
        Properties p = new Properties();
        fillProperties(p, "morphium");
        return p;
    }

    public void fillProperties(Properties p, String prefix) {
        if (prefix == null) prefix = "";
        if (!prefix.isEmpty()) {
            if (!prefix.endsWith(".")) {
                prefix = prefix + ".";
            }
        }
        p.setProperty(prefix + "maxConnections", "" + maxConnections);
        p.setProperty(prefix + "housekeepingTimeout", "" + housekeepingTimeout);
        p.setProperty(prefix + "globalCacheValidTime", "" + globalCacheValidTime);
        p.setProperty(prefix + "writeCacheTimeout", "" + writeCacheTimeout);
        p.setProperty(prefix + "database", database);
        p.setProperty(prefix + "connectionTimeout", "" + connectionTimeout);
        p.setProperty(prefix + "socketTimeout", "" + socketTimeout);
        p.setProperty(prefix + "socketKeepAlive", "" + socketKeepAlive);
        p.setProperty(prefix + "readPreferenceLevel", "" + defaultReadPreference.name());
        p.setProperty(prefix + "mongoLogin", mongoLogin);
        p.setProperty(prefix + "mongoPassword", mongoPassword);
        p.setProperty(prefix + "configManagerCacheTimeout", "" + configManagerCacheTimeout);

        String a = "";
        for (ServerAddress s : adr) {
            if (!a.isEmpty())
                a += ",";
            a += s.getHost() + ":" + s.getPort();
        }
        p.setProperty(prefix + "adr", a);
        p.setProperty(prefix + "configManagerClass", configManager.getClass().getName());
        p.setProperty(prefix + "mongoAdminUser", mongoAdminUser);
        p.setProperty(prefix + "mongoAdminPwd", mongoAdminPwd);
        p.setProperty(prefix + "fieldImplClass", fieldImplClass.getName());
        p.setProperty(prefix + "mapperClass", omClass.getName());
    }

    public void initFromProperty(Properties p) {
        initFromProperty(p, "");
    }

    @SuppressWarnings({"ConstantConditions", "unchecked"})
    public void initFromProperty(Properties p, String prefix) {
        if (prefix == null) prefix = "";
        if (!prefix.isEmpty()) {
            if (!prefix.endsWith(".")) {
                prefix = prefix + ".";
            }
        }

        String fieldImplClassStr = p.getProperty(prefix + "fieldImplClass", MongoFieldImpl.class.getName());
        try {
            fieldImplClass = (Class<? extends MongoField>) Class.forName(fieldImplClassStr);
        } catch (ClassNotFoundException ignored) {
        }

        String mapperCls = p.getProperty(prefix + "mapperClass", ObjectMapperImpl.class.getName());
        try {
            omClass = (Class<? extends ObjectMapper>) Class.forName(mapperCls);
        } catch (Exception ignored) {
            System.out.println("could not read object mapper class " + mapperCls);
            omClass = ObjectMapperImpl.class;
        }
        mongoAdminUser = p.getProperty(prefix + "mongoAdminUser");
        mongoAdminPwd = p.getProperty(prefix + "mongoAdminPwd");
        String configMgrCls = p.getProperty(prefix + "configManagerClass", ConfigManagerImpl.class.getName());
        try {
            configManager = (ConfigManager) Class.forName(configMgrCls).newInstance();
        } catch (Exception e) {
            System.out.println("could not instanciate Config Manager: ");
            e.printStackTrace();
        }
        String srv = p.getProperty(prefix + "servers", "localhost:27017");
        String[] sp = srv.split(",");
        adr = new ArrayList<ServerAddress>();
        for (String s : sp) {
            String[] a = s.split(":");
            try {
                if (a.length == 1) {
                    adr.add(new ServerAddress(a[0]));
                } else {
                    int port = Integer.valueOf(a[1]);
//                    ServerAddress adr = new ServerAddress(a[0], port);
                }
            } catch (Exception e) {
                System.err.println("Could not add Host: " + s);
                throw new RuntimeException("Could not add host " + s, e);
            }
        }
        if (adr.isEmpty()) {
            throw new IllegalArgumentException("No valid host specified!");
        }
        configManagerCacheTimeout = Integer.valueOf(p.getProperty(prefix + "configManagerCacheTimeout", "60000"));
        mongoPassword = p.getProperty(prefix + "password");
        mongoLogin = p.getProperty(prefix + "login");
        defaultReadPreference = ReadPreferenceLevel.valueOf(p.getProperty(prefix + "readPreferenceLevel", "NEAREST"));
        socketKeepAlive = p.getProperty(prefix + "socketKeepAlive", "true").equalsIgnoreCase("true");
        socketTimeout = Integer.valueOf(p.getProperty(prefix + "socketTimeout", "0"));
        database = p.getProperty(prefix + "database", "morphium");
        connectionTimeout = Integer.valueOf(p.getProperty(prefix + "connectionTimeout", "0"));
        writeCacheTimeout = Integer.valueOf(p.getProperty(prefix + "writeCacheTimeout", "5000"));
        globalCacheValidTime = Integer.valueOf(p.getProperty(prefix + "globalCacheValidTime", "10000"));
        housekeepingTimeout = Integer.valueOf(p.getProperty(prefix + "housekeepingTimeout", "5000"));
        maxConnections = Integer.valueOf(p.getProperty(prefix + "maxConnections", "100"));
    }

    public Class<? extends MorphiumIterator> getIteratorClass() {
        if (iteratorClass == null) {
            iteratorClass = MorphiumIteratorImpl.class;
        }
        return iteratorClass;
    }

    public void setIteratorClass(Class<? extends MorphiumIterator> iteratorClass) {
        this.iteratorClass = iteratorClass;
    }

    public void setFieldImplClass(Class<? extends MongoField> fieldImplClass) {
        this.fieldImplClass = fieldImplClass;
    }

    public MorphiumWriter getAsyncWriter() {
        return asyncWriter;
    }

    public void setAsyncWriter(MorphiumWriter asyncWriter) {
        this.asyncWriter = asyncWriter;
    }
}