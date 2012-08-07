package de.caluga.morphium;

/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

import com.mongodb.ServerAddress;
import de.caluga.morphium.secure.DefaultSecurityManager;
import de.caluga.morphium.secure.MongoSecurityManager;
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
public class MorphiumConfig {

    private MongoDbMode mode;
    private int maxConnections, housekeepingTimeout;
    private int globalCacheValidTime = 5000;
    private int writeCacheTimeout = 5000;
    private String database;

    private int connectionTimeout = 0;
    private int socketTimeout = 0;
    private boolean socketKeepAlive = true;

    private boolean slaveOk = true;
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
    private String superUserLogin, superUserPassword; //THE superuser!
    private String adminGroupName; //Admin group - superusers except the superuserAdmin

    private MongoSecurityManager securityMgr;
    private ObjectMapper mapper = new ObjectMapperImpl();
    private Class fieldImplClass = de.caluga.morphium.MongoFieldImpl.class;

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

    public void setFieldImplClass(Class<? extends MongoField> fieldImplClass) {
        this.fieldImplClass = fieldImplClass;
    }

    public boolean isSocketKeepAlive() {
        return socketKeepAlive;
    }

    public void setSocketKeepAlive(boolean socketKeepAlive) {
        this.socketKeepAlive = socketKeepAlive;
    }

    public ObjectMapper getMapper() {
        return mapper;
    }

    public void setMapper(ObjectMapper mapper) {
        this.mapper = mapper;
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

    //    private boolean removePackage=true;

//    public boolean isRemovePackage() {
//        return removePackage;
//    }
//
//    public void setRemovePackage(boolean removePackage) {
//        this.removePackage = removePackage;
//    }


    public boolean isSlaveOk() {
        return slaveOk;
    }

    public void setSlaveOk(boolean slaveOk) {
        this.slaveOk = slaveOk;
    }

    public MongoSecurityManager getSecurityMgr() {
        return securityMgr;
    }

    public void setSecurityMgr(MongoSecurityManager securityMgr) {
        this.securityMgr = securityMgr;
    }


    public String getAdminGroupName() {
        return adminGroupName;
    }

    public void setAdminGroupName(String adminGroupName) {
        this.adminGroupName = adminGroupName;
    }

    public String getSuperUserLogin() {
        return superUserLogin;
    }

    public void setSuperUserLogin(String superUserLogin) {
        this.superUserLogin = superUserLogin;
    }

    public String getSuperUserPassword() {
        return superUserPassword;
    }

    public void setSuperUserPassword(String superUserPassword) {
        this.superUserPassword = superUserPassword;
    }

//    public Class<? extends Object> getAclClass() {
//        return aclClass;
//    }
//
//    public void setAclClass(Class<? extends Object> aclClass) {
//        this.aclClass = aclClass;
//    }
//
//    public Class<? extends Object> getRoleClass() {
//        return roleClass;
//    }
//
//    public void setRoleClass(Class<? extends Object> roleClass) {
//        this.roleClass = roleClass;
//    }
//
//    public Class<? extends Object> getUserClass() {
//        return userClass;
//    }
//
//    public void setUserClass(Class<? extends Object> userClass) {
//        this.userClass = userClass;
//    }

    public int getConfigManagerCacheTimeout() {
        return configManagerCacheTimeout;
    }

    public void setConfigManagerCacheTimeout(int configManagerCacheTimeout) {
        this.configManagerCacheTimeout = configManagerCacheTimeout;
    }

    public void setMode(MongoDbMode mode) {
        this.mode = mode;
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

    public MongoDbMode getMode() {
        return mode;
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
     * @return
     */
    public int getGlobalCacheValidTime() {
        return globalCacheValidTime;
    }

    public MorphiumConfig(String db, MongoDbMode mode, int maxConnections, int globalCacheValidTime, int housekeepingTimeout) throws IOException {
        this(db, mode, maxConnections, globalCacheValidTime, housekeepingTimeout, new DefaultSecurityManager());
    }

    public MorphiumConfig(String db, MongoDbMode mode, int maxConnections, int globalCacheValidTime, int housekeepingTimeout, MongoSecurityManager mgr) throws IOException {
        this(db, mode, maxConnections, globalCacheValidTime, housekeepingTimeout, mgr, Thread.currentThread().getContextClassLoader().getResource("morphium-log4j.xml"));
    }

    public MorphiumConfig(String db, MongoDbMode mode, int maxConnections, int globalCacheValidTime, int housekeepingTimeout, MongoSecurityManager mgr, String resourceName) throws IOException {
        this(db, mode, maxConnections, globalCacheValidTime, housekeepingTimeout, mgr, Thread.currentThread().getContextClassLoader().getResource(resourceName));
    }

    public MorphiumConfig(String db, MongoDbMode mode, int maxConnections, int globalCacheValidTime, int housekeepingTimeout, MongoSecurityManager mgr, URL loggingConfigResource) {

        securityMgr = mgr;
        if (securityMgr == null) {
            securityMgr = new DefaultSecurityManager();
        }
        validTimeByClassName = new Hashtable<String, Integer>();
        database = db;
        adr = new Vector<ServerAddress>();
        this.mode = mode;
        this.maxConnections = maxConnections;
        this.globalCacheValidTime = globalCacheValidTime;
        this.housekeepingTimeout = housekeepingTimeout;
//        LogManager.getLogManager().readConfiguration(logPropInput);
        if (loggingConfigResource != null) {
            DOMConfigurator.configure(loggingConfigResource);
        }


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
                "mode=" + mode +
                ", maxConnections=" + maxConnections +
                ", housekeepingTimeout=" + housekeepingTimeout +
                ", globalCacheValidTime=" + globalCacheValidTime +
                ", writeCacheTimeout=" + writeCacheTimeout +
                ", database='" + database + '\'' +
                ", connectionTimeout=" + connectionTimeout +
                ", socketTimeout=" + socketTimeout +
                ", socketKeepAlive=" + socketKeepAlive +
                ", slaveOk=" + slaveOk +
                ", mongoLogin='" + mongoLogin + '\'' +
                ", mongoPassword='" + mongoPassword + '\'' +
                ", configManagerCacheTimeout=" + configManagerCacheTimeout +
                ", adr=" + adr +
                ", validTimeByClassName=" + validTimeByClassName +
                ", configManager=" + configManager +
                ", superUserLogin='" + superUserLogin + '\'' +
                ", superUserPassword='" + superUserPassword + '\'' +
                ", adminGroupName='" + adminGroupName + '\'' +
                ", securityMgr=" + securityMgr +
                ", mapper=" + mapper +
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
        p.setProperty(prefix + "mode", mode.name());
        p.setProperty(prefix + "maxConnections", "" + maxConnections);
        p.setProperty(prefix + "housekeepingTimeout", "" + housekeepingTimeout);
        p.setProperty(prefix + "globalCacheValidTime", "" + globalCacheValidTime);
        p.setProperty(prefix + "writeCacheTimeout", "" + writeCacheTimeout);
        p.setProperty(prefix + "database", database);
        p.setProperty(prefix + "connectionTimeout", "" + connectionTimeout);
        p.setProperty(prefix + "socketTimeout", "" + socketTimeout);
        p.setProperty(prefix + "socketKeepAlive", "" + socketKeepAlive);
        p.setProperty(prefix + "slaveOk", "" + slaveOk);
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
        p.setProperty(prefix + "superUserLogin", superUserLogin);
        p.setProperty(prefix + "superUserPassword", superUserPassword);
        p.setProperty(prefix + "adminGroupName", adminGroupName);
        p.setProperty(prefix + "fieldImplClass", fieldImplClass.getName());
        p.setProperty(prefix + "mapperClass", mapper.getClass().getName());
        p.setProperty(prefix + "securityManagerClass", securityMgr.getClass().getName());
    }

    public void initFromProperty(Properties p) {
        initFromProperty(p, "");
    }

    public void initFromProperty(Properties p, String prefix) {
        if (prefix == null) prefix = "";
        if (!prefix.isEmpty()) {
            if (!prefix.endsWith(".")) {
                prefix = prefix + ".";
            }
        }

        String fieldImplClassStr = p.getProperty(prefix + "fieldImplClass", MongoFieldImpl.class.getName());
        try {
            fieldImplClass = Class.forName(fieldImplClassStr);
        } catch (ClassNotFoundException e) {
        }

        String mapperCls = p.getProperty(prefix + "mapperClass", ObjectMapperImpl.class.getName());
        try {
            mapper = (ObjectMapper) Class.forName(mapperCls).newInstance();
        } catch (Exception e) {
        }
        String securityMgrCls = p.getProperty(prefix + "securityManagerClass", DefaultSecurityManager.class.getName());
        try {
            securityMgr = (MongoSecurityManager) Class.forName(securityMgrCls).newInstance();
        } catch (Exception e) {
        }
        adminGroupName = p.getProperty(prefix + "adminGroupName");
        superUserLogin = p.getProperty(prefix + "superUserLogin");
        superUserPassword = p.getProperty(prefix + "superUserPassword");
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
        slaveOk = p.getProperty(prefix + "slaveOk", "true").equalsIgnoreCase("true");
        socketKeepAlive = p.getProperty(prefix + "socketKeepAlive", "true").equalsIgnoreCase("true");
        socketTimeout = Integer.valueOf(p.getProperty(prefix + "socketTimeout", "0"));
        database = p.getProperty(prefix + "database", "morphium");
        connectionTimeout = Integer.valueOf(p.getProperty(prefix + "connectionTimeout", "0"));
        writeCacheTimeout = Integer.valueOf(p.getProperty(prefix + "writeCacheTimeout", "5000"));
        globalCacheValidTime = Integer.valueOf(p.getProperty(prefix + "globalCacheValidTime", "10000"));
        mode = MongoDbMode.valueOf(p.getProperty(prefix + "mode", "SINGLE"));
        housekeepingTimeout = Integer.valueOf(p.getProperty(prefix + "housekeepingTimeout", "5000"));
        maxConnections = Integer.valueOf(p.getProperty(prefix + "maxConnections", "100"));
    }
}