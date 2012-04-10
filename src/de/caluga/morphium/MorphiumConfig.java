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
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Vector;

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
    /**
     * login credentials for MongoDB - if necessary. If null, don't authenticate
     */
    private String mongoLogin=null, mongoPassword=null;
    private int configManagerCacheTimeout = 1000 * 60 * 60; //one hour
    private List<ServerAddress> adr;
    private Map<String, Integer> validTimeByClassName;

    //securitysettings
//    private Class<? extends Object> userClass, roleClass, aclClass;
    private String superUserLogin, superUserPassword; //THE superuser!
    private String adminGroupName; //Admin group - superusers except the superuserAdmin

    private MongoSecurityManager securityMgr;
    private ObjectMapper mapper = new ObjectMapperImpl();
    private String fieldImplClass = "de.caluga.morphium.MongoFieldImpl";

    public String getFieldImplClass() {
        return fieldImplClass;
    }

    public void setFieldImplClass(String fieldImplClass) {
        this.fieldImplClass = fieldImplClass;
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
        this(db, mode, maxConnections, globalCacheValidTime, housekeepingTimeout, mgr, Thread.currentThread().getContextClassLoader().getResource("log4j.xml"));
    }

    public MorphiumConfig(String db, MongoDbMode mode, int maxConnections, int globalCacheValidTime, int housekeepingTimeout, MongoSecurityManager mgr, String resourceName) throws IOException {
        this(db, mode, maxConnections, globalCacheValidTime, housekeepingTimeout, mgr, Thread.currentThread().getContextClassLoader().getResource(resourceName));
    }

    public MorphiumConfig(String db, MongoDbMode mode, int maxConnections, int globalCacheValidTime, int housekeepingTimeout, MongoSecurityManager mgr, URL loggingConfigResource) throws IOException {

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
        DOMConfigurator.configure(loggingConfigResource);


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
}