package de.caluga.morphium;

import javax.net.ssl.SSLContext;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.Objects;

import de.caluga.morphium.config.*;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import de.caluga.morphium.annotations.AdditionalData;
import de.caluga.morphium.annotations.Embedded;
import de.caluga.morphium.annotations.Transient;
import de.caluga.morphium.cache.MorphiumCache;
import de.caluga.morphium.config.CollectionCheckSettings.CappedCheck;
import de.caluga.morphium.config.CollectionCheckSettings.IndexCheck;
import de.caluga.morphium.driver.ReadPreference;
import de.caluga.morphium.driver.ReadPreferenceType;
import de.caluga.morphium.driver.wire.PooledDriver;
import de.caluga.morphium.encryption.EncryptionKeyProvider;
import de.caluga.morphium.encryption.ValueEncryptionProvider;
import de.caluga.morphium.writer.MorphiumWriter;

/**
 * Stores the configuration for the MongoDBLayer.
 *
 * @author stephan
 */
@SuppressWarnings({"UnusedDeclaration", "UnusedReturnValue"})
@Embedded
public class MorphiumConfig {
    @AdditionalData(readOnly = false)
    private Map<String, Object> restoreData;


    @Transient
    private static Logger log = LoggerFactory.getLogger(MorphiumConfig.class);
    @Transient
    private MessagingSettings messagingSettings = new MessagingSettings();
    @Transient
    private CollectionCheckSettings collectionCheckSettings = new CollectionCheckSettings();
    @Transient
    private ConnectionSettings connectionSettings = new ConnectionSettings();
    @Transient
    private DriverSettings driverSettings = new DriverSettings();
    @Transient
    private EncryptionSettings encryptionSettings = new EncryptionSettings();
    @Transient
    private ObjectMappingSettings objectMappingSettings = new ObjectMappingSettings();
    @Transient
    private ThreadPoolSettings threadPoolSettings = new ThreadPoolSettings();
    @Transient
    private WriterSettings writerSettings = new WriterSettings();
    @Transient
    private CacheSettings cacheSettings = new CacheSettings();
    @Transient
    private AuthSettings authSettings = new AuthSettings();
    @Transient
    private ClusterSettings clusterSettings = new ClusterSettings();



    @Transient
    private List<Settings> settings = List.of(
            clusterSettings,
            authSettings,
            writerSettings,
            threadPoolSettings,
            objectMappingSettings,
            driverSettings,
            connectionSettings,
            collectionCheckSettings,
            messagingSettings
                                      );

    private void rebuildSettingsList() {
        settings = List.of(
                                   clusterSettings,
                                   authSettings,
                                   writerSettings,
                                   threadPoolSettings,
                                   objectMappingSettings,
                                   driverSettings,
                                   connectionSettings,
                                   collectionCheckSettings,
                                   messagingSettings
                   );
    }


    public MessagingSettings messagingSettings() {
        return messagingSettings;
    }
    public CollectionCheckSettings collectionCheckSettings() {
        return collectionCheckSettings;
    }
    public EncryptionSettings encryptionSettings() {
        return encryptionSettings;
    }
    public ObjectMappingSettings objectMappingSettings() {
        return objectMappingSettings;
    }
    public ThreadPoolSettings threadPoolSettings() {
        return threadPoolSettings;
    }
    public WriterSettings writerSettings() {
        return writerSettings;
    }
    public CacheSettings cacheSettings() {
        return cacheSettings;
    }
    public ConnectionSettings connectionSettings() {
        return connectionSettings;
    }
    public DriverSettings driverSettings() {
        return driverSettings;
    }
    public AuthSettings authSettings() {return authSettings;}
    public ClusterSettings clusterSettings() {return clusterSettings;}
    /**
     * use messagingSettings
     */
    @Deprecated
    public boolean isMessagingStatusInfoListenerEnabled() {
        return messagingSettings.isMessagingStatusInfoListenerEnabled();
    }

    /**
     * use messagingSettings
     */
    @Deprecated
    public MorphiumConfig setMessagingStatusInfoListenerEnabled(boolean messagingStatusInfoListenerEnabled) {
        this.messagingSettings.setMessagingStatusInfoListenerEnabled(messagingStatusInfoListenerEnabled);
        return this;
    }
    /**
     * use messagingSettings
     */
    @Deprecated
    public String getMessagingStatusInfoListenerName() {
        return messagingSettings.getMessagingStatusInfoListenerName();
    }
    /**
     * use messagingSettings
     */
    @Deprecated
    public MorphiumConfig setMessagingStatusInfoListenerName(String name) {
        messagingSettings.setMessagingStatusInfoListenerName(name);
        return this;
    }

    public MorphiumConfig(final Properties prop) {
        this(null, prop);
    }

    public MorphiumConfig(String prefix, final Properties prop) {
        this(prefix, prop::get);
    }

    public MorphiumConfig(String prefix, MorphiumConfigResolver resolver) {
        AnnotationAndReflectionHelper an = new AnnotationAndReflectionHelper(true); // settings always convert camel

        if (prefix != null && !prefix.isEmpty()) {
            prefix += ".";
        } else {
            prefix = "";
        }

        for (var settingObject : settings) {
            // log.info("======> Settings {} <======", settingObject.getClass().getName());
            // case
            List<Field> flds = an.getAllFields(settingObject.getClass());

            for (Field f : flds) {
                String fName = prefix + f.getName();
                Object setting = resolver.resolveSetting(fName);
                // log.info("fname {} = {}", fName, setting);

                if (setting == null) {
                    //check camelcase
                    fName = prefix + an.convertCamelCase(f.getName());
                    setting = resolver.resolveSetting(fName);
                    // log.info("  camel {} = {}", fName, setting);

                    if (setting == null) {
                        fName = prefix + an.createCamelCase(f.getName(), false);
                        setting = resolver.resolveSetting(fName);
                        // log.info("  decamel {} = {}", fName, setting);

                        if (setting == null) {
                            // log.debug("Setting {} is null - continuing", f.getName());
                            continue;
                        }
                    }
                }

                f.setAccessible(true);
                // log.info("Setting {} in class {} to value {}", f.getName(), settingObject.getClass().getName(), setting);

                try {
                    if (f.getType().isEnum()) {
                        @SuppressWarnings("unchecked")
                        Enum value = Enum.valueOf((Class <? extends Enum > ) f.getType(), (String) setting);
                        f.set(settingObject, value);
                    } else if (f.getType().equals(int.class) || f.getType().equals(Integer.class)) {
                        f.set(settingObject, Integer.parseInt((String) setting));
                    } else if (f.getType().equals(long.class) || f.getType().equals(Long.class)) {
                        f.set(settingObject, Long.parseLong((String) setting));
                    } else if (f.getType().equals(String.class)) {
                        f.set(settingObject, setting);
                    } else if (f.getType().equals(boolean.class) || f.getType().equals(Boolean.class)) {
                        f.set(settingObject, setting.equals("true"));
                    } else if (List.class.isAssignableFrom(f.getType())) {
                        String lst = (String) setting;
                        List<String> l = new ArrayList<>();
                        lst = lst.replaceAll("[\\[\\]]", "");
                        Collections.addAll(l, lst.split(","));
                        List<String> ret = new ArrayList<>();

                        for (String n : l) {
                            ret.add(n.trim());
                        }

                        f.set(settingObject, ret);
                    } else {
                        f.set(settingObject, setting);
                    }
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        if (!clusterSettings.hostSeedIsSet()) {
            String lst = (String) resolver.resolveSetting(prefix + "hosts");

            if (lst != null) {
                lst = lst.replaceAll("[\\[\\]]", "");

                for (String s : lst.split(",")) {
                    clusterSettings.addHostToSeed(s);
                }
            }
        }

        if (resolver.resolveSetting(prefix + "driver_class") != null || resolver.resolveSetting(prefix + "driverClass") != null) {
            String n = "driver_name";

            if (resolver.resolveSetting("driverName") != null) {
                n = "driverName";
            }

            if (resolver.resolveSetting(n) != null) {
                log.error("not using driver_class - drivername is set {}", resolver.resolveSetting(n));
                driverSettings.setDriverName((String)resolver.resolveSetting(n));
            } else {
                var s = resolver.resolveSetting(prefix + "driver_class");

                if (s == null) {
                    s = resolver.resolveSetting(prefix + "driverClass");
                }

                try {
                    Class driverClass = Class.forName((String)s);
                    Method m = driverClass.getMethod("getName", null);
                    m.setAccessible(true);
                    driverSettings.setDriverName((String)m.invoke(null));
                } catch (Exception e) {
                    log.error("Cannot set driver class - using default driver instead!");
                    driverSettings.setDriverName(PooledDriver.driverName);
                }
            }
        }
        rebuildSettingsList();
    }


    public MorphiumConfig() {
        this("test", 10, 60000, 10000);
    }

    public MorphiumConfig(String db, int maxConnections, int globalCacheValidTime, int housekeepingTimeout) {
        connectionSettings.setDatabase(db);
        connectionSettings.setMaxConnections(maxConnections);
        cacheSettings.setGlobalCacheValidTime(globalCacheValidTime);
        cacheSettings.setHousekeepingTimeout(housekeepingTimeout);
        rebuildSettingsList();
    }

    public static List<String> getPropertyNames(String prefix) {
        @SuppressWarnings("unchecked")
        List<String> flds = new AnnotationAndReflectionHelper(true).getFields(MorphiumConfig.class);
        List<String> ret = new ArrayList<>();

        for (String f : flds) {
            ret.add(prefix + "." + f);
        }

        return ret;
    }

    @SuppressWarnings("CastCanBeRemovedNarrowingVariableType")
    public static MorphiumConfig createFromJson(String json)
    throws NoSuchFieldException, ClassNotFoundException, IllegalAccessException, InstantiationException, ParseException, NoSuchMethodException, InvocationTargetException {
        ObjectMapper jacksonOM = new ObjectMapper();
        HashMap<String, Object> obj;

        try {
            obj = (HashMap<String, Object>) jacksonOM.readValue(json.getBytes(), Map.class);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        Properties p = new Properties();

        for (var e : obj.entrySet()) {
            if (e.getValue() != null) {
                p.put(e.getKey(), e.getValue().toString());
            }

            log.info("Adding {} = {}", e.getKey(), e.getValue());
        }

        MorphiumConfig cfg = fromProperties(p);
        return cfg;
    }

    /**
     * use messagingSettings
     */
    @Deprecated
    public int getMessagingWindowSize() {
        return messagingSettings.getMessagingWindowSize();
    }

    /**
     * use messagingSettings
     */
    @Deprecated
    public void setMessagingWindowSize(int messagingWindowSize) {
        messagingSettings.setMessagingWindowSize(messagingWindowSize);
    }

    public static MorphiumConfig fromProperties(String prefix, Properties p) {
        return new MorphiumConfig(prefix, p);
    }

    public static MorphiumConfig fromProperties(Properties p) {
        return new MorphiumConfig(p);
    }


    /**
     * use ConnectionSettings
     */
    @Deprecated
    public boolean isReplicaset() {
        return clusterSettings.isReplicaset();
    }

    /**
     * use driverSettings
     */
    @Deprecated
    public CompressionType getCompressionType() {
        return driverSettings.getCompressionType();
    }

    /**
     * use driverSettings
     */
    @Deprecated
    public MorphiumConfig setCompressionType(CompressionType t) {
        driverSettings.setCompressionType(t);
        return this;
    }

    /**
     * use ConnectionSettings
     */
    @Deprecated
    public MorphiumConfig setReplicasetMonitoring(boolean replicaset) {
        clusterSettings.setReplicaset(replicaset);
        return this;
    }

    /**
     * use encryptionSettings
     */
    @Deprecated
    public Class <? extends ValueEncryptionProvider > getValueEncryptionProviderClass() {
        return encryptionSettings.getValueEncryptionProviderClass();
    }

    /**
     * use encryptionSettings
     */
    @Deprecated
    public MorphiumConfig setValueEncryptionProviderClass(Class <? extends ValueEncryptionProvider > valueEncryptionProviderClass) {
        encryptionSettings.setValueEncryptionProviderClass(valueEncryptionProviderClass);
        return this;
    }

    /**
     * use encryptionSettings
     */
    @Deprecated
    public Class <? extends EncryptionKeyProvider > getEncryptionKeyProviderClass() {
        return encryptionSettings.getEncryptionKeyProviderClass();
    }

    /**
     * use encryptionSettings
     */
    @Deprecated
    public MorphiumConfig setEncryptionKeyProviderClass(Class <? extends EncryptionKeyProvider > encryptionKeyProviderClass) {
        encryptionSettings.setEncryptionKeyProviderClass(encryptionKeyProviderClass);
        return this;
    }

    /**
     * use encryptionSettings
     */
    @Deprecated
    public String getCredentialsEncryptionKey() {
        return encryptionSettings.getCredentialsEncryptionKey();
    }

    /**
     * use encryptionSettings
     */
    @Deprecated
    public MorphiumConfig setCredentialsEncryptionKey(String credentialsEncryptionKey) {
        encryptionSettings.setCredentialsEncryptionKey(credentialsEncryptionKey);
        return this;
    }

    /**
     * use encryptionSettings
     */
    @Deprecated
    public String getCredentialsDecryptionKey() {
        return encryptionSettings.getCredentialsDecryptionKey();
    }

    /**
     * use encryptionSettings
     */
    @Deprecated
    public MorphiumConfig setCredentialsDecryptionKey(String credentialsDecryptionKey) {
        encryptionSettings.setCredentialsDecryptionKey(credentialsDecryptionKey);
        return this;
    }

    /**
     * use driverSettings
     */
    @Deprecated
    public String getDriverName() {
        return driverSettings.getDriverName();
    }


    /**
     * use driverSettings
     */
    @Deprecated
    public MorphiumConfig setDriverName(String driverName) {
        driverSettings.setDriverName(driverName);
        return this;
    }


    /**
     * use collectionCheckSettings
     */
    public boolean isAutoIndexAndCappedCreationOnWrite() {
        return collectionCheckSettings.getIndexCheck().equals(IndexCheck.CREATE_ON_WRITE_NEW_COL);
    }

    /**
     * use collectionCheckSettings
     */
    public MorphiumConfig setAutoIndexAndCappedCreationOnWrite(boolean autoIndexAndCappedCreationOnWrite) {
        if (autoIndexAndCappedCreationOnWrite) {
            collectionCheckSettings.setIndexCheck(IndexCheck.CREATE_ON_WRITE_NEW_COL);
        } else {
            collectionCheckSettings.setIndexCheck(IndexCheck.NO_CHECK);
        }

        return this;
    }

    /**
     * use ObjectMappingSettings
     */
    @Deprecated
    public boolean isWarnOnNoEntitySerialization() {
        return objectMappingSettings.isWarnOnNoEntitySerialization();
    }

    /**
     * use ObjectMappingSettings
     */
    @Deprecated
    public MorphiumConfig setWarnOnNoEntitySerialization(boolean warnOnNoEntitySerialization) {
        objectMappingSettings.setWarnOnNoEntitySerialization(warnOnNoEntitySerialization);
        return this;
    }

    /**
     * use ObjectMappingSettings
     */
    @Deprecated
    public boolean isCheckForNew() {
        return objectMappingSettings.isCheckForNew();
    }

    /**
     * if set to false, all checks if an entity is new when CreationTime is used is
     * switched off
     * if set to true, only those, whose CreationTime settings use checkfornew will
     * work
     * default false
     *
     * @param checkForNew boolean, check if object is really not stored yet
     * use ObjectMappingSettings
     */
    @Deprecated
    public MorphiumConfig setCheckForNew(boolean checkForNew) {
        objectMappingSettings.setCheckForNew(checkForNew);
        return this;
    }

    /**
     * use ConnectionSettings
     */
    @Deprecated
    public int getRetriesOnNetworkError() {
        return connectionSettings.getRetriesOnNetworkError();
    }

    /**
     * use ConnectionSettings
     */
    @Deprecated
    public MorphiumConfig setRetriesOnNetworkError(int retriesOnNetworkError) {
        connectionSettings.setRetriesOnNetworkError(retriesOnNetworkError);
        return this;
    }

    /**
     * use ConnectionSettings
     */
    @Deprecated
    public int getSleepBetweenNetworkErrorRetries() {
        return connectionSettings.getSleepBetweenNetworkErrorRetries();
    }

    /**
     * use ConnectionSettings
     */
    @Deprecated
    public MorphiumConfig setSleepBetweenNetworkErrorRetries(int sleepBetweenNetworkErrorRetries) {
        connectionSettings.setSleepBetweenNetworkErrorRetries(sleepBetweenNetworkErrorRetries);
        return this;
    }


    /**
     * use writerSettings
     */
    @Deprecated
    public int getWriteBufferTimeGranularity() {
        return writerSettings.getWriteBufferTimeGranularity();
    }

    /**
     * use writerSettings
     */
    @Deprecated
    public MorphiumConfig setWriteBufferTimeGranularity(int writeBufferTimeGranularity) {
        writerSettings.setWriteBufferTimeGranularity(writeBufferTimeGranularity);
        return this;
    }

    /**
     * use CacheSettings
     */
    @Deprecated
    public MorphiumCache getCache() {
        return cacheSettings.getCache();
    }

    /**
     * use CacheSettings
     */
    @Deprecated
    public MorphiumConfig setCache(MorphiumCache cache) {
        cacheSettings.setCache(cache);
        return this;
    }

    /**
     * use writerSettings
     */
    @Deprecated
    public int getWriteBufferTime() {
        return writerSettings.getWriteBufferTime();
    }

    /**
     * use writerSettings
     */
    @Deprecated
    public MorphiumConfig setWriteBufferTime(int writeBufferTime) {
        writerSettings.setWriteBufferTime(writeBufferTime);
        return this;
    }
    /**
     * use writerSettings
     */
    @Deprecated
    public MorphiumWriter getBufferedWriter() {
        return writerSettings.getBufferedWriter();
    }

    /**
     * use writerSettings
     */
    @Deprecated
    public MorphiumConfig setBufferedWriter(MorphiumWriter bufferedWriter) {
        writerSettings.setBufferedWriter(bufferedWriter);
        return this;
    }

    /**
     * use writerSettings
     */
    @Deprecated
    public MorphiumWriter getWriter() {
        return writerSettings.getWriter();
    }

    /**
     * use writerSettings
     */
    @Deprecated
    public MorphiumConfig setWriter(MorphiumWriter writer) {
        writerSettings.setWriter(writer);
        return this;
    }

    /**
     * use ConnectionSettings
     */
    @Deprecated
    public int getConnectionTimeout() {
        return connectionSettings.getConnectionTimeout();
    }

    /**
     * use ConnectionSettings
     */
    @Deprecated
    public MorphiumConfig setConnectionTimeout(int connectionTimeout) {
        connectionSettings.setConnectionTimeout(connectionTimeout);
        return this;
    }

    /**
     * use ConnectionSettings
     */
    @Deprecated
    public int getMaxWaitTime() {
        return connectionSettings.getMaxWaitTime();
    }

    /**
     * Sets the maximum time that a thread will block waiting for a connection.
     *
     * @param maxWaitTime the maximum wait time, in milliseconds
     * @return {@code this}
     */
    /**
     * use ConnectionSettings
     */
    @Deprecated
    public MorphiumConfig setMaxWaitTime(int maxWaitTime) {
        connectionSettings.setMaxWaitTime(maxWaitTime);
        return this;
    }

    /**
     * use driverSettings
     */
    @Deprecated
    public int getServerSelectionTimeout() {
        return driverSettings.getServerSelectionTimeout();
    }

    /**
     * <p>
     * Sets the server selection timeout in milliseconds, which defines how long the
     * driver will wait for server selection to succeed before throwing an
     * exception.
     * </p>
     *
     * <p>
     * A value of 0 means that it will timeout immediately if no server is
     * available. A negative value means to wait indefinitely.
     * </p>
     *
     * @param serverSelectionTimeout the server selection timeout, in milliseconds
     * @return {@code this}
     */
    /**
     * use driverSettings
     */
    @Deprecated
    public MorphiumConfig setServerSelectionTimeout(int serverSelectionTimeout) {
        driverSettings.setServerSelectionTimeout(serverSelectionTimeout);
        return this;
    }

    /**
     * use encryptionSettings
     */
    @Deprecated
    public Boolean getCredentialsEncrypted() {
        return encryptionSettings.getCredentialsEncrypted();
    }

    /**
     * use encryptionSettings
     */
    @Deprecated
    public MorphiumConfig setCredentialsEncrypted(Boolean credentialsEncrypted) {
        encryptionSettings.setCredentialsEncrypted(credentialsEncrypted);
        return this;
    }

    /**
     * use driverSettings
     */
    @Deprecated
    public String getMongoAuthDb() {
        return  authSettings.getMongoAuthDb();
    }

// public String decryptAuthDb() {
//     if (getCredentialsEncrypted() == null || !getCredentialsEncrypted()) {
//         return getMongoAuthDb();
//     }

//     try {
//         var ve = getValueEncryptionProviderClass().getDeclaredConstructor().newInstance();
//         ve.setEncryptionKey(getCredentialsEncryptionKey().getBytes(StandardCharsets.UTF_8));
//         ve.setDecryptionKey(getCredentialsDecryptionKey().getBytes(StandardCharsets.UTF_8));
//         var authdb = "admin";

//         if (getMongoAuthDb() != null) {
//             authdb = new String(ve.decrypt(Base64.getDecoder().decode(getMongoAuthDb())));
//         }

//         return authdb;
//     } catch (Exception e) {
//         throw new RuntimeException(e);
//     }
// }

// public String decryptMongoLogin() {
//     if (getCredentialsEncrypted() == null || !getCredentialsEncrypted()) {
//         return getMongoLogin();
//     }

//     try {
//         var ve = getValueEncryptionProviderClass().getDeclaredConstructor().newInstance();
//         ve.setEncryptionKey(getCredentialsEncryptionKey().getBytes(StandardCharsets.UTF_8));
//         ve.setDecryptionKey(getCredentialsDecryptionKey().getBytes(StandardCharsets.UTF_8));
//         return new String(ve.decrypt(Base64.getDecoder().decode(getMongoLogin())));
//     } catch (Exception e) {
//         throw new RuntimeException(e);
//     }
// }

// public String decryptMongoPassword() {
//     if (getCredentialsEncrypted() == null || !getCredentialsEncrypted()) {
//         return getMongoLogin();
//     }

//     try {
//         var ve = getValueEncryptionProviderClass().getDeclaredConstructor().newInstance();
//         ve.setEncryptionKey(getCredentialsEncryptionKey().getBytes(StandardCharsets.UTF_8));
//         ve.setDecryptionKey(getCredentialsDecryptionKey().getBytes(StandardCharsets.UTF_8));
//         return new String(ve.decrypt(Base64.getDecoder().decode(getMongoPassword())));
//     } catch (Exception e) {
//         throw new RuntimeException(e);
//     }
// }

    /**
     * use driverSettings
     */
    @Deprecated
    public MorphiumConfig setMongoAuthDb(String mongoAuthDb) {
        authSettings.setMongoAuthDb(mongoAuthDb);
        return this;
    }

    /**
     * use driverSettings
     */
    @Deprecated
    public String getMongoLogin() {
        return authSettings.getMongoLogin();
    }

    /**
     * use driverSettings
     */
    @Deprecated
    public MorphiumConfig setMongoLogin(String mongoLogin) {
        authSettings.setMongoLogin(mongoLogin);
        return this;
    }

    /**
     * use driverSettings
     */
    @Deprecated
    public String getMongoPassword() {
        return authSettings.getMongoPassword();
    }

    /**
     * use driverSettings
     */
    @Deprecated
    public MorphiumConfig setMongoPassword(String mongoPassword) {
        authSettings.setMongoPassword(mongoPassword);
        return this;
    }

    /**
     * use driverSettings
     */
    @Deprecated
    public ReadPreference getDefaultReadPreference() {
        return driverSettings.getDefaultReadPreference();
    }

    /**
     * use driverSettings
     */
    @Deprecated
    public MorphiumConfig setDefaultReadPreference(ReadPreference defaultReadPreference) {
        driverSettings.setDefaultReadPreference(defaultReadPreference);
        return this;
    }

    /**
     * use driverSettings
     */
    @Deprecated
    public String getDefaultReadPreferenceType() {
        return driverSettings.getDefaultReadPreferenceType();
    }

    /**
     * use driverSettings
     */
    @Deprecated
    public MorphiumConfig setDefaultReadPreferenceType(String stringDefaultReadPreference) {
        driverSettings.setDefaultReadPreferenceType(stringDefaultReadPreference);
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
        driverSettings.setDefaultReadPreference(defaultReadPreference);
        return this;
    }

    /**
     * use CacheSettings
     */
    @Deprecated
    public int getWriteCacheTimeout() {
        return cacheSettings.getWriteCacheTimeout();
    }

    /**
     * use CacheSettings
     */
    @Deprecated
    public MorphiumConfig setWriteCacheTimeout(int writeCacheTimeout) {
        cacheSettings.setWriteCacheTimeout(writeCacheTimeout);
        return this;
    }


    /**
     * setting hosts as Host:Port
     *
     * @param str list of hosts, with or without port
     */
    @Deprecated
    public MorphiumConfig setHostSeed(List<String> str) {
        clusterSettings.setHostSeed(str);
        return this;
    }

    /**
     @Deprecated use getConnectionSettings().METHOD
     */
    @Deprecated
    public MorphiumConfig setHostSeed(List<String> str, List<Integer> ports) {
        clusterSettings.setHostSeed(str, ports);
        return this;
    }

    /**
     @Deprecated use getConnectionSettings().METHOD
     */
    @Deprecated
    public List<String> getHostSeed() {
        return clusterSettings.getHostSeed();
    }

    /**
     @Deprecated use getConnectionSettings().METHOD
     */
    @Deprecated
    public MorphiumConfig setHostSeed(String... hostPorts) {
        clusterSettings.setHostSeed(hostPorts);
        return this;
    }

    /**
     @Deprecated use getConnectionSettings().METHOD
     */
    @Deprecated
    public MorphiumConfig setHostSeed(String hostPorts) {
        clusterSettings.setHostSeed(hostPorts);
        return this;
    }

    /**
     @Deprecated use getConnectionSettings().METHOD
     */
    @Deprecated
    public MorphiumConfig setHostSeed(String hosts, String ports) {
        clusterSettings.setHostSeed(hosts, ports);
        return this;
    }

    /**
     @Deprecated use getConnectionSettings().METHOD
     */
    @Deprecated
    public MorphiumConfig addHostToSeed(String host, int port) {
        clusterSettings.addHostToSeed(host, port);
        return this;
    }

    /**
     @Deprecated use getConnectionSettings().METHOD
     */
    @Deprecated
    public MorphiumConfig addHostToSeed(String host) {
        clusterSettings.addHostToSeed(host);
        return this;
    }

    /**
     @Deprecated use getConnectionSettings().METHOD
     */
    @Deprecated
    public int getMaxConnections() {
        return connectionSettings.getMaxConnections();
    }

    /**
     @Deprecated use getConnectionSettings().METHOD
     */
    @Deprecated
    public MorphiumConfig setMaxConnections(int maxConnections) {
        connectionSettings.setMaxConnections(maxConnections);
        return this;
    }
    /**
     @Deprecated use getConnectionSettings().METHOD
     */
    @Deprecated
    public String getDatabase() {
        return connectionSettings.getDatabase();
    }

    /**
     @Deprecated use getConnectionSettings().METHOD
     */
    @Deprecated
    public MorphiumConfig setDatabase(String database) {
        connectionSettings.setDatabase(database);
        return this;
    }
    @Deprecated
    public int getHousekeepingTimeout() {
        return cacheSettings.getHousekeepingTimeout();
    }

    @Deprecated
    public MorphiumConfig setHousekeepingTimeout(int housekeepingTimeout) {
        cacheSettings.setHousekeepingTimeout(housekeepingTimeout);
        return this;
    }

    /**
     * for future use - set Global Caching time
     *
     * @return the global cache valid time
     */
    @Deprecated
    public int getGlobalCacheValidTime() {
        return cacheSettings.getGlobalCacheValidTime();
    }

    @Deprecated
    public MorphiumConfig setGlobalCacheValidTime(int globalCacheValidTime) {
        cacheSettings.setGlobalCacheValidTime(globalCacheValidTime);
        return this;
    }




    /**
     * returns json representation of this object containing all values
     *
     * @return json string
     */
    @Override
    public String toString() {
        try {
            var om = new ObjectMapperImpl();
            om.getARHelper().enableConvertCamelCase();
            Map<String, Object> data = new LinkedHashMap<>();
            for (var s : settings) {
                data.putAll(om.serialize(s));
            }
            return Utils.toJsonString(data);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * use writerSettings
     */
    @Deprecated
    public MorphiumConfig setThreadConnectionMultiplier(int mp) {
        writerSettings.setThreadConnectionMultiplier(mp);
        return this;
    }

    /**
     * use writerSettings
     */
    @Deprecated
    public int getThreadConnectionMultiplier() {
        return writerSettings.getThreadConnectionMultiplier();
    }


    /**
     * use writerSettings
     */
    @Deprecated
    public MorphiumWriter getAsyncWriter() {
        return writerSettings.getAsyncWriter();
    }

    /**
     * use writerSettings
     */
    @Deprecated
    public MorphiumConfig setAsyncWriter(MorphiumWriter asyncWriter) {
        writerSettings.setAsyncWriter(asyncWriter);
        return this;
    }

    /**
     * use writerSettings
     */
    @Deprecated
    public int getMaximumRetriesBufferedWriter() {
        return writerSettings.getMaximumRetriesAsyncWriter();
    }

    /**
     * use writerSettings
     */
    @Deprecated
    public MorphiumConfig setMaximumRetriesBufferedWriter(int maximumRetriesBufferedWriter) {
        writerSettings.setMaximumRetriesBufferedWriter(maximumRetriesBufferedWriter);
        return this;
    }

    /**
     * use writerSettings
     */
    @Deprecated
    public int getMaximumRetriesWriter() {
        return writerSettings.getMaximumRetriesWriter();
    }

    /**
     * use writerSettings
     */
    @Deprecated
    public MorphiumConfig setMaximumRetriesWriter(int maximumRetriesWriter) {
        writerSettings.setMaximumRetriesWriter(maximumRetriesWriter);
        return this;
    }

    /**
     * use writerSettings
     */
    @Deprecated
    public int getMaximumRetriesAsyncWriter() {
        return writerSettings.getMaximumRetriesAsyncWriter();
    }

    /**
     * use writerSettings
     */
    @Deprecated
    public MorphiumConfig setMaximumRetriesAsyncWriter(int maximumRetriesAsyncWriter) {
        writerSettings.setMaximumRetriesAsyncWriter(maximumRetriesAsyncWriter);
        return this;
    }

    /**
     * use writerSettings
     */
    @Deprecated
    public int getRetryWaitTimeBufferedWriter() {
        return writerSettings.getRetryWaitTimeBufferedWriter();
    }

    /**
     * use writerSettings
     */
    @Deprecated
    public MorphiumConfig setRetryWaitTimeBufferedWriter(int retryWaitTimeBufferedWriter) {
        writerSettings.setRetryWaitTimeBufferedWriter(retryWaitTimeBufferedWriter);
        return this;
    }

    /**
     * use writerSettings
     */
    @Deprecated
    public int getRetryWaitTimeWriter() {
        return writerSettings.getRetryWaitTimeWriter();
    }

    /**
     * use writerSettings
     */
    @Deprecated
    public MorphiumConfig setRetryWaitTimeWriter(int retryWaitTimeWriter) {
        writerSettings.setRetryWaitTimeWriter(retryWaitTimeWriter);
        return this;
    }

    /**
     * use writerSettings
     */
    @Deprecated
    public int getRetryWaitTimeAsyncWriter() {
        return writerSettings.getRetryWaitTimeAsyncWriter();
    }

    /**
     * use writerSettings
     */
    @Deprecated
    public MorphiumConfig setRetryWaitTimeAsyncWriter(int retryWaitTimeAsyncWriter) {
        writerSettings.setRetryWaitTimeAsyncWriter(retryWaitTimeAsyncWriter);
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


    public MorphiumConfig createCopy() {
        MorphiumConfig c = new MorphiumConfig();
        // deep-copy settings using reflection-based copy to avoid property round-trips
        c.messagingSettings = this.messagingSettings.copy();
        c.collectionCheckSettings = this.collectionCheckSettings.copy();
        c.connectionSettings = this.connectionSettings.copy();
        c.driverSettings = this.driverSettings.copy();
        c.encryptionSettings = this.encryptionSettings.copy();
        c.objectMappingSettings = this.objectMappingSettings.copy();
        c.threadPoolSettings = this.threadPoolSettings.copy();
        c.writerSettings = this.writerSettings.copy();
        c.cacheSettings = this.cacheSettings.copy();
        c.authSettings = this.authSettings.copy();
        c.clusterSettings = this.clusterSettings.copy();

        if (this.restoreData != null) {
            c.restoreData = new LinkedHashMap<>(this.restoreData);
        }

        // rebind list to copied instances
        c.rebuildSettingsList();
        return c;
    }


    /**
     * @param prefix prefix to use in property keys
     * @param effectiveConfig when true, use the current effective config, including
     *        overrides from Environment
     * @return the properties
     */
    @Deprecated
    public Properties asProperties(String prefix, boolean effectiveConfig) {
        return asProperties(prefix);
    }

    public Properties asProperties(String prefix, Settings setting) {
        Properties p = new Properties();
        p.putAll(setting.asProperties(prefix));
        return p;
    }

    protected <T extends Settings> T getSettingByType(Class cls) {
        for (var s : settings) {
            if (s.getClass().equals(cls)) return (T) s;
        }
        return null;
    }

    public <T extends Settings> T createCopyOf(T setting) {
        return (T) setting.copy();
    }



    public Properties asProperties(String prefix) {
        // if (prefix == null) {
        //     prefix = "";
        // } else {
        //     prefix = prefix + ".";
        // }

        Properties p = new Properties();

        for (var setting : settings) {
            p.putAll(setting.asProperties(prefix));
        }

        // if (effectiveConfig) {
        //     Properties sysprop = System.getProperties();
        //
        //     for (Object sysk : sysprop.keySet()) {
        //         String k = (String) sysk;
        //         if (k.startsWith("morphium.")) {
        //             String value = sysprop.get(k).toString();
        //             k = k.substring(9);
        //             p.put(prefix + k, value);
        //         }
        //     }
        // }
        // }

        return p;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (other == null) return false;
        if (getClass() != other.getClass()) return false;
        MorphiumConfig that = (MorphiumConfig) other;

        // Compare all setting objects (they have proper equals)
        for (var s : settings) {
            if (!Objects.equals(s, that.getSettingByType(s.getClass()))) return false;

        }
        // AdditionalData contents
        if (!Objects.equals(this.restoreData, that.restoreData)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int rd = 0;
        if (restoreData != null) rd = restoreData.hashCode();
        return Objects.hash(settings.toArray()) + rd;
    }

    /**
     * use CacheSettings
     */
    @Deprecated
    public boolean isReadCacheEnabled() {
        return cacheSettings.isReadCacheEnabled();
    }

    /**
     * use CacheSettings
     */
    @Deprecated
    public MorphiumConfig setReadCacheEnabled(boolean readCacheEnabled) {
        cacheSettings.setReadCacheEnabled(readCacheEnabled);
        return this;
    }

    /**
     * use CacheSettings
     */
    @Deprecated
    public MorphiumConfig disableReadCache() {
        cacheSettings.setReadCacheEnabled(false);
        return this;
    }

    /**
     * use CacheSettings
     */
    @Deprecated
    public MorphiumConfig enableReadCache() {
        cacheSettings.setReadCacheEnabled(true);
        return this;
    }

    /**
     * use CacheSettings
     */
    @Deprecated
    public boolean isAsyncWritesEnabled() {
        return cacheSettings.isAsyncWritesEnabled();
    }

    /**
     * use CacheSettings
     */
    @Deprecated
    public MorphiumConfig setAsyncWritesEnabled(boolean asyncWritesEnabled) {
        cacheSettings.setAsyncWritesEnabled(asyncWritesEnabled);
        return this;
    }

    /**
     * use CacheSettings
     */
    @Deprecated
    public MorphiumConfig disableAsyncWrites() {
        cacheSettings.setAsyncWritesEnabled(false);
        return this;
    }

    /**
     * use CacheSettings
     */
    @Deprecated
    public MorphiumConfig enableAsyncWrites() {
        cacheSettings.setAsyncWritesEnabled(true);
        return this;
    }

    /**
     * use CacheSettings
     */
    @Deprecated
    public boolean isBufferedWritesEnabled() {
        return cacheSettings.isBufferedWritesEnabled();
    }

    /**
     * use CacheSettings
     */
    @Deprecated
    public MorphiumConfig setBufferedWritesEnabled(boolean bufferedWritesEnabled) {
        cacheSettings.setBufferedWritesEnabled(bufferedWritesEnabled);
        return this;
    }

    /**
     * use CacheSettings
     */
    @Deprecated
    public MorphiumConfig disableBufferedWrites() {
        cacheSettings.setBufferedWritesEnabled(false);
        return this;
    }

    /**
     * use CacheSettings
     */
    @Deprecated
    public MorphiumConfig enableBufferedWrites() {
        cacheSettings.setBufferedWritesEnabled(true);
        return this;
    }

    /**
     * use ObjectMappingSettings
     */
    @Deprecated
    public boolean isAutoValuesEnabled() {
        return objectMappingSettings.isAutoValues();
    }

    /**
     * use ObjectMappingSettings
     */
    @Deprecated
    public MorphiumConfig setAutoValuesEnabled(boolean enabled) {
        objectMappingSettings.setAutoValues(enabled);
        return this;
    }

    /**
     * use ObjectMappingSettings
     */
    @Deprecated
    public MorphiumConfig enableAutoValues() {
        objectMappingSettings.setAutoValues(true);
        return this;
    }

    /**
     * use ObjectMappingSettings
     */
    @Deprecated
    public MorphiumConfig disableAutoValues() {
        objectMappingSettings.setAutoValues(false);
        return this;
    }

    /**
     * use ObjectMappingSettings
     */
    @Deprecated
    public boolean isCamelCaseConversionEnabled() {
        return objectMappingSettings.isCamelCaseConversionEnabled();
    }

    /**
     * use ObjectMappingSettings
     */
    @Deprecated
    public MorphiumConfig setCamelCaseConversionEnabled(boolean camelCaseConversionEnabled) {
        objectMappingSettings.setCamelCaseConversionEnabled(camelCaseConversionEnabled);
        return this;
    }

    /**
     * use messagingSettings
     */
    @Deprecated
    public int getThreadPoolMessagingCoreSize() {
        return messagingSettings.getThreadPoolMessagingCoreSize();
    }

    /**
     * use messagingSettings
     */
    @Deprecated
    public MorphiumConfig setThreadPoolMessagingCoreSize(int threadPoolMessagingCoreSize) {
        messagingSettings.setThreadPoolMessagingCoreSize(threadPoolMessagingCoreSize);
        return this;
    }

    /**
     * use messagingSettings
     */
    @Deprecated
    public int getThreadPoolMessagingMaxSize() {
        return messagingSettings.getThreadPoolMessagingMaxSize();
    }

    /**
     * use messagingSettings
     */
    @Deprecated
    public MorphiumConfig setThreadPoolMessagingMaxSize(int threadPoolMessagingMaxSize) {
        messagingSettings.setThreadPoolMessagingMaxSize(threadPoolMessagingMaxSize);
        return this;
    }

    /**
     * use messagingSettings
     */
    @Deprecated
    public long getThreadPoolMessagingKeepAliveTime() {
        return messagingSettings.getThreadPoolMessagingKeepAliveTime();
    }

    /**
     * use messagingSettings
     */
    @Deprecated
    public MorphiumConfig setThreadPoolMessagingKeepAliveTime(long threadPoolMessagingKeepAliveTime) {
        messagingSettings.setThreadPoolMessagingKeepAliveTime(threadPoolMessagingKeepAliveTime);
        return this;
    }

    /**
     * use threadPoolSettings
     */
    @Deprecated
    public int getThreadPoolAsyncOpCoreSize() {
        return threadPoolSettings.getThreadPoolAsyncOpCoreSize();
    }

    /**
     * use threadPoolSettings
     */
    @Deprecated
    public MorphiumConfig setThreadPoolAsyncOpCoreSize(int threadPoolAsyncOpCoreSize) {
        threadPoolSettings.setThreadPoolAsyncOpCoreSize(threadPoolAsyncOpCoreSize);
        return this;
    }

    /**
     * use threadPoolSettings
     */
    @Deprecated
    public int getThreadPoolAsyncOpMaxSize() {
        return threadPoolSettings.getThreadPoolAsyncOpMaxSize();
    }

    /**
     * use threadPoolSettings
     */
    @Deprecated
    public MorphiumConfig setThreadPoolAsyncOpMaxSize(int threadPoolAsyncOpMaxSize) {
        threadPoolSettings.setThreadPoolAsyncOpMaxSize(threadPoolAsyncOpMaxSize);
        return this;
    }

    /**
     * use threadPoolSettings
     */
    @Deprecated
    public long getThreadPoolAsyncOpKeepAliveTime() {
        return threadPoolSettings.getThreadPoolAsyncOpKeepAliveTime();
    }

    /**
     * use threadPoolSettings
     */
    @Deprecated
    public MorphiumConfig setThreadPoolAsyncOpKeepAliveTime(long threadPoolAsyncOpKeepAliveTime) {
        threadPoolSettings.setThreadPoolAsyncOpKeepAliveTime(threadPoolAsyncOpKeepAliveTime);
        return this;
    }

    /**
     * use ObjectMappingSettings
     */
    @Deprecated
    public boolean isObjectSerializationEnabled() {
        return objectMappingSettings.isObjectSerializationEnabled();
    }

    /**
     * use ObjectMappingSettings
     */
    @Deprecated
    public MorphiumConfig setObjectSerializationEnabled(boolean objectSerializationEnabled) {
        objectMappingSettings.setObjectSerializationEnabled(objectSerializationEnabled);
        return this;
    }

    @Deprecated
    public int getHeartbeatFrequency() {
        return driverSettings.getHeartbeatFrequency();
    }

    @Deprecated
    public MorphiumConfig setHeartbeatFrequency(int heartbeatFrequency) {
        driverSettings.setHeartbeatFrequency(heartbeatFrequency);
        return this;
    }


    /**
     * use ConnectionSettings
     */
    @Deprecated
    public MorphiumConfig setMinConnections(int minConnections) {
        connectionSettings.setMinConnections(minConnections);
        return this;
    }

    @Deprecated
    public int getLocalThreshold() {
        return driverSettings.getLocalThreshold();
    }

    /**
     * <p>
     * Sets the local threshold. When choosing among multiple MongoDB servers to
     * send a request, the MongoClient will only send that request to a server whose
     * ping time is less than or equal to the server with the fastest ping time plus
     * the local threshold.
     * </p>
     *
     * <p>
     * For example, let's say that the client is choosing a server to send a query
     * when the read preference is {@code
     * ReadPreference.secondary()}, and that there are three secondaries, server1,
     * server2, and server3, whose ping times are 10, 15, and 16 milliseconds,
     * respectively. With a local threshold of 5 milliseconds, the client will send
     * the query to either server1 or server2 (randomly selecting between the two).
     * </p>
     *
     * <p>
     * Default is 15 milliseconds.
     * </p>
     *
     * @return the local threshold, in milliseconds
     * Refer to the MongoDB documentation for details on localThreshold: reference/program/mongos/#cmdoption--localThreshold
     *                        Local Threshold
     * @since 2.13.0
     */
    @Deprecated
    public MorphiumConfig setLocalThreshold(int localThreshold) {
        driverSettings.setLocalThreshold(localThreshold);
        return this;
    }

    @Deprecated
    public int getMaxConnectionIdleTime() {
        return driverSettings.getMaxConnectionIdleTime();
    }

    @Deprecated
    public MorphiumConfig setMaxConnectionIdleTime(int maxConnectionIdleTime) {
        driverSettings.setMaxConnectionIdleTime(maxConnectionIdleTime);
        return this;
    }

    @Deprecated
    public int getMaxConnectionLifeTime() {
        return driverSettings.getMaxConnectionLifeTime();
    }

    @Deprecated
    public MorphiumConfig setMaxConnectionLifeTime(int maxConnectionLifeTime) {
        driverSettings.setMaxConnectionLifeTime(maxConnectionLifeTime);
        return this;
    }

    /**
     * use ConnectionSettings
     */
    @Deprecated
    public String getRequiredReplicaSetName() {
        return clusterSettings.getRequiredReplicaSetName();
    }

    /**
     * use ConnectionSettings
     */
    @Deprecated
    public MorphiumConfig setRequiredReplicaSetName(String requiredReplicaSetName) {
        clusterSettings.setRequiredReplicaSetName(requiredReplicaSetName);
        return this;
    }

// public String getDefaultTags() {
//     driverSettings.settag
//     return defaultTags;
// }

// public MorphiumConfig addDefaultTag(String name, String value) {
//     if (defaultTags != null) {
//         defaultTags += ",";
//     } else {
//         defaultTags = "";
//     }

//     defaultTags += name + ":" + value;
//     return this;
// }

// public List<Map<String, String >> getDefaultTagSet() {
//     if (defaultTags == null) {
//         return null;
//     }
//     List<Map<String, String >> tagList = new ArrayList<>();

//     for (String t : defaultTags.split(",")) {
//         String[] tag = t.split(":");
//         tagList.add(UtilsMap.of(tag[0], tag[1]));
//     }
//     return tagList;
// }

    public int getCursorBatchSize() {
        return driverSettings.getCursorBatchSize();
    }

    public MorphiumConfig setCursorBatchSize(int cursorBatchSize) {
        driverSettings.setCursorBatchSize(cursorBatchSize);
        return this;
    }

    public SSLContext getSslContext() {
        return connectionSettings.getSslContext();
    }

    public void setSslContext(SSLContext sslContext) {
        connectionSettings.setSslContext(sslContext);
    }

    public boolean isUseSSL() {
        return connectionSettings.isUseSSL();
    }

    public void setUseSSL(boolean useSSL) {
        connectionSettings.setUseSSL(useSSL);
    }

    public boolean isSslInvalidHostNameAllowed() {
        return connectionSettings.isSslInvalidHostNameAllowed();
    }

    public void setSslInvalidHostNameAllowed(boolean sslInvalidHostNameAllowed) {
        connectionSettings.setSslInvalidHostNameAllowed(sslInvalidHostNameAllowed);
    }
    @Deprecated
    public int getReadTimeout() {
        return driverSettings.getReadTimeout();
    }

    @Deprecated
    public MorphiumConfig setReadTimeout(int readTimeout) {
        driverSettings.setReadTimeout(readTimeout);
        return this;
    }

    @Deprecated
    public boolean isRetryReads() {
        return driverSettings.isRetryReads();
    }

    @Deprecated
    public void setRetryReads(boolean retryReads) {
        driverSettings.setRetryReads(retryReads);
    }

    @Deprecated
    public boolean isRetryWrites() {
        return driverSettings.isRetryWrites();
    }

    @Deprecated
    public void setRetryWrites(boolean retryWrites) {
        driverSettings.setRetryWrites(retryWrites);
    }

    @Deprecated
    public String getUuidRepresentation() {
        return driverSettings.getUuidRepresentation();
    }

    /**
     * Sets the UUID representation to use when encoding instances of {@link UUID}
     * and when decoding BSON binary values with
     * subtype of 3.
     *
     * <p>
     * The default is UNSPECIFIED, If your application stores UUID values in
     * MongoDB, you must set this
     * value to the desired representation. New applications should prefer STANDARD,
     * while existing Java
     * applications should prefer JAVA_LEGACY. Applications wishing to interoperate
     * with existing Python or
     * .NET applications should prefer PYTHON_LEGACY or C_SHARP_LEGACY,
     * respectively. Applications that do not store UUID values in MongoDB don't
     * need to set this value.
     * </p>
     *
     * @param uuidRepresentation the UUID representation
     * @since 3.12
     */
    public void setUuidRepresentation(String uuidRepresentation) {
        driverSettings.setUuidRepresentation(uuidRepresentation);
    }

    /**
     * use collectionCheckSettings
     */
    @Deprecated
    public IndexCheck getIndexCheck() {
        return collectionCheckSettings.getIndexCheck();
    }

    /**
     * use collectionCheckSettings
     */
    @Deprecated
    public void setIndexCheck(IndexCheck indexCheck) {
        collectionCheckSettings.setIndexCheck(indexCheck);
    }

    /**
     * use collectionCheckSettings
     */
    @Deprecated
    public CappedCheck getCappedCheck() {
        return collectionCheckSettings.getCappedCheck();
    }

    /**
     * use collectionCheckSettings
     */
    @Deprecated
    public MorphiumConfig setCappedCheck(CappedCheck cappedCheck) {
        collectionCheckSettings.setCappedCheck(cappedCheck);
        return this;
    }


    public enum CompressionType {
        NONE(0), ZLIB(2), SNAPPY(1); //ZSTD(3) - not supported
        CompressionType(int c) {
            this.code = c;
        }

        private int code;
        public int getCode() {
            return code;
        }

    }

    /**
     * use ConnectionSettings
     */
    @Deprecated
    public int getIdleSleepTime() {
        return driverSettings.getIdleSleepTime();
    }

    /**
     * use ConnectionSettings
     */
    @Deprecated
    public void setIdleSleepTime(int idleSleepTime) {
        driverSettings.setIdleSleepTime(idleSleepTime);
    }

    public Map<String, Object> getRestoreData() {
        return restoreData;
    }

    public void setRestoreData(Map<String, Object> restoreData) {
        this.restoreData = restoreData;
    }

    /**
     * use ConnectionSettings
     */
    @Deprecated
    public int getMinConnections() {
        return connectionSettings.getMinConnections();
    }

}
