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

import org.json.simple.parser.JSONParser;

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
            cacheSettings,
            threadPoolSettings,
            objectMappingSettings,
            cacheSettings,
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
                                   cacheSettings,
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
     * @deprecated use {@link MessagingSettings#isMessagingStatusInfoListenerEnabled()} via {@code messagingSettings()} instead
     */
    @Deprecated(since = "6.3", forRemoval = true)
    public boolean isMessagingStatusInfoListenerEnabled() {
        return messagingSettings.isMessagingStatusInfoListenerEnabled();
    }

    /**
     * @deprecated use {@link MessagingSettings#setMessagingStatusInfoListenerEnabled(boolean)} via {@code messagingSettings()} instead
     */
    @Deprecated(since = "6.3", forRemoval = true)
    public MorphiumConfig setMessagingStatusInfoListenerEnabled(boolean messagingStatusInfoListenerEnabled) {
        this.messagingSettings.setMessagingStatusInfoListenerEnabled(messagingStatusInfoListenerEnabled);
        return this;
    }
    /**
     * @deprecated use {@link MessagingSettings#getMessagingStatusInfoListenerName()} via {@code messagingSettings()} instead
     */
    @Deprecated(since = "6.3", forRemoval = true)
    public String getMessagingStatusInfoListenerName() {
        return messagingSettings.getMessagingStatusInfoListenerName();
    }
    /**
     * @deprecated use {@link MessagingSettings#setMessagingStatusInfoListenerName(String)} via {@code messagingSettings()} instead
     */
    @Deprecated(since = "6.3", forRemoval = true)
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
                    Class<?> driverClass = AnnotationAndReflectionHelper.classForName((String)s);
                    Method m = driverClass.getMethod("getName");
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
        JSONParser jsonParser = new JSONParser();
        Map<String, Object> obj;

        try {
            @SuppressWarnings("unchecked")
            Map<String, Object> parsed = (Map<String, Object>) jsonParser.parse(json);
            obj = parsed;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        Properties p = new Properties();

        for (var e : obj.entrySet()) {
            if (e.getValue() != null) {
                if (e.getValue() instanceof List) {
                    List<?> list = (List<?>) e.getValue();
                    p.put(e.getKey(), list.stream()
                        .map(Object::toString)
                        .collect(java.util.stream.Collectors.joining(", ", "[", "]")));
                } else {
                    p.put(e.getKey(), e.getValue().toString());
                }
            }

            log.info("Adding {} = {}", e.getKey(), e.getValue());
        }

        MorphiumConfig cfg = fromProperties(p);
        return cfg;
    }

    /**
     * @deprecated use {@link MessagingSettings#getMessagingWindowSize()} via {@code messagingSettings()} instead
     */
    @Deprecated(since = "6.3", forRemoval = true)
    public int getMessagingWindowSize() {
        return messagingSettings.getMessagingWindowSize();
    }

    /**
     * @deprecated use {@link MessagingSettings#setMessagingWindowSize(int)} via {@code messagingSettings()} instead
     */
    @Deprecated(since = "6.3", forRemoval = true)
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
     * @deprecated use {@link ClusterSettings#isReplicaset()} via {@code clusterSettings()} instead
     */
    @Deprecated(since = "6.3", forRemoval = true)
    public boolean isReplicaset() {
        return clusterSettings.isReplicaset();
    }

    /**
     * @deprecated use {@link DriverSettings#getCompressionType()} via {@code driverSettings()} instead
     */
    @Deprecated(since = "6.3", forRemoval = true)
    public CompressionType getCompressionType() {
        return driverSettings.getCompressionType();
    }

    /**
     * @deprecated use {@link DriverSettings#setCompressionType(CompressionType)} via {@code driverSettings()} instead
     */
    @Deprecated(since = "6.3", forRemoval = true)
    public MorphiumConfig setCompressionType(CompressionType t) {
        driverSettings.setCompressionType(t);
        return this;
    }

    /**
     * @deprecated use {@link ClusterSettings#setReplicaset(boolean)} via {@code clusterSettings()} instead
     */
    @Deprecated(since = "6.3", forRemoval = true)
    public MorphiumConfig setReplicasetMonitoring(boolean replicaset) {
        clusterSettings.setReplicaset(replicaset);
        return this;
    }

    /**
     * @deprecated use {@link EncryptionSettings#getValueEncryptionProviderClass()} via {@code encryptionSettings()} instead
     */
    @Deprecated(since = "6.3", forRemoval = true)
    public Class <? extends ValueEncryptionProvider > getValueEncryptionProviderClass() {
        return encryptionSettings.getValueEncryptionProviderClass();
    }

    /**
     * @deprecated use {@link EncryptionSettings#setValueEncryptionProviderClass(Class)} via {@code encryptionSettings()} instead
     */
    @Deprecated(since = "6.3", forRemoval = true)
    public MorphiumConfig setValueEncryptionProviderClass(Class <? extends ValueEncryptionProvider > valueEncryptionProviderClass) {
        encryptionSettings.setValueEncryptionProviderClass(valueEncryptionProviderClass);
        return this;
    }

    /**
     * @deprecated use {@link EncryptionSettings#getEncryptionKeyProviderClass()} via {@code encryptionSettings()} instead
     */
    @Deprecated(since = "6.3", forRemoval = true)
    public Class <? extends EncryptionKeyProvider > getEncryptionKeyProviderClass() {
        return encryptionSettings.getEncryptionKeyProviderClass();
    }

    /**
     * @deprecated use {@link EncryptionSettings#setEncryptionKeyProviderClass(Class)} via {@code encryptionSettings()} instead
     */
    @Deprecated(since = "6.3", forRemoval = true)
    public MorphiumConfig setEncryptionKeyProviderClass(Class <? extends EncryptionKeyProvider > encryptionKeyProviderClass) {
        encryptionSettings.setEncryptionKeyProviderClass(encryptionKeyProviderClass);
        return this;
    }

    /**
     * @deprecated use {@link EncryptionSettings#getCredentialsEncryptionKey()} via {@code encryptionSettings()} instead
     */
    @Deprecated(since = "6.3", forRemoval = true)
    public String getCredentialsEncryptionKey() {
        return encryptionSettings.getCredentialsEncryptionKey();
    }

    /**
     * @deprecated use {@link EncryptionSettings#setCredentialsEncryptionKey(String)} via {@code encryptionSettings()} instead
     */
    @Deprecated(since = "6.3", forRemoval = true)
    public MorphiumConfig setCredentialsEncryptionKey(String credentialsEncryptionKey) {
        encryptionSettings.setCredentialsEncryptionKey(credentialsEncryptionKey);
        return this;
    }

    /**
     * @deprecated use {@link EncryptionSettings#getCredentialsDecryptionKey()} via {@code encryptionSettings()} instead
     */
    @Deprecated(since = "6.3", forRemoval = true)
    public String getCredentialsDecryptionKey() {
        return encryptionSettings.getCredentialsDecryptionKey();
    }

    /**
     * @deprecated use {@link EncryptionSettings#setCredentialsDecryptionKey(String)} via {@code encryptionSettings()} instead
     */
    @Deprecated(since = "6.3", forRemoval = true)
    public MorphiumConfig setCredentialsDecryptionKey(String credentialsDecryptionKey) {
        encryptionSettings.setCredentialsDecryptionKey(credentialsDecryptionKey);
        return this;
    }

    /**
     * @deprecated use {@link DriverSettings#getDriverName()} via {@code driverSettings()} instead
     */
    @Deprecated(since = "6.3", forRemoval = true)
    public String getDriverName() {
        return driverSettings.getDriverName();
    }


    /**
     * @deprecated use {@link DriverSettings#setDriverName(String)} via {@code driverSettings()} instead
     */
    @Deprecated(since = "6.3", forRemoval = true)
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
            collectionCheckSettings.setCappedCheck(CappedCheck.CREATE_ON_WRITE_NEW_COL);
        } else {
            collectionCheckSettings.setIndexCheck(IndexCheck.NO_CHECK);
            collectionCheckSettings.setCappedCheck(CappedCheck.NO_CHECK);
        }

        return this;
    }

    /**
     * @deprecated use {@link ObjectMappingSettings#isWarnOnNoEntitySerialization()} via {@code objectMappingSettings()} instead
     */
    @Deprecated(since = "6.3", forRemoval = true)
    public boolean isWarnOnNoEntitySerialization() {
        return objectMappingSettings.isWarnOnNoEntitySerialization();
    }

    /**
     * @deprecated use {@link ObjectMappingSettings#setWarnOnNoEntitySerialization(boolean)} via {@code objectMappingSettings()} instead
     */
    @Deprecated(since = "6.3", forRemoval = true)
    public MorphiumConfig setWarnOnNoEntitySerialization(boolean warnOnNoEntitySerialization) {
        objectMappingSettings.setWarnOnNoEntitySerialization(warnOnNoEntitySerialization);
        return this;
    }

    /**
     * @deprecated use {@link ObjectMappingSettings#isCheckForNew()} via {@code objectMappingSettings()} instead
     */
    @Deprecated(since = "6.3", forRemoval = true)
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
     * @deprecated use {@link ObjectMappingSettings#setCheckForNew(boolean)} via {@code objectMappingSettings()} instead
     */
    @Deprecated(since = "6.3", forRemoval = true)
    public MorphiumConfig setCheckForNew(boolean checkForNew) {
        objectMappingSettings.setCheckForNew(checkForNew);
        return this;
    }

    /**
     * @deprecated use {@link ConnectionSettings#getRetriesOnNetworkError()} via {@code connectionSettings()} instead
     */
    @Deprecated(since = "6.3", forRemoval = true)
    public int getRetriesOnNetworkError() {
        return connectionSettings.getRetriesOnNetworkError();
    }

    /**
     * @deprecated use {@link ConnectionSettings#setRetriesOnNetworkError(int)} via {@code connectionSettings()} instead
     */
    @Deprecated(since = "6.3", forRemoval = true)
    public MorphiumConfig setRetriesOnNetworkError(int retriesOnNetworkError) {
        connectionSettings.setRetriesOnNetworkError(retriesOnNetworkError);
        return this;
    }

    /**
     * @deprecated use {@link ConnectionSettings#getSleepBetweenNetworkErrorRetries()} via {@code connectionSettings()} instead
     */
    @Deprecated(since = "6.3", forRemoval = true)
    public int getSleepBetweenNetworkErrorRetries() {
        return connectionSettings.getSleepBetweenNetworkErrorRetries();
    }

    /**
     * @deprecated use {@link ConnectionSettings#setSleepBetweenNetworkErrorRetries(int)} via {@code connectionSettings()} instead
     */
    @Deprecated(since = "6.3", forRemoval = true)
    public MorphiumConfig setSleepBetweenNetworkErrorRetries(int sleepBetweenNetworkErrorRetries) {
        connectionSettings.setSleepBetweenNetworkErrorRetries(sleepBetweenNetworkErrorRetries);
        return this;
    }


    /**
     * @deprecated use {@link WriterSettings#getWriteBufferTimeGranularity()} via {@code writerSettings()} instead
     */
    @Deprecated(since = "6.3", forRemoval = true)
    public int getWriteBufferTimeGranularity() {
        return writerSettings.getWriteBufferTimeGranularity();
    }

    /**
     * @deprecated use {@link WriterSettings#setWriteBufferTimeGranularity(int)} via {@code writerSettings()} instead
     */
    @Deprecated(since = "6.3", forRemoval = true)
    public MorphiumConfig setWriteBufferTimeGranularity(int writeBufferTimeGranularity) {
        writerSettings.setWriteBufferTimeGranularity(writeBufferTimeGranularity);
        return this;
    }

    /**
     * @deprecated use {@link CacheSettings#getCache()} via {@code cacheSettings()} instead
     */
    @Deprecated(since = "6.3", forRemoval = true)
    public MorphiumCache getCache() {
        return cacheSettings.getCache();
    }

    /**
     * @deprecated use {@link CacheSettings#setCache(MorphiumCache)} via {@code cacheSettings()} instead
     */
    @Deprecated(since = "6.3", forRemoval = true)
    public MorphiumConfig setCache(MorphiumCache cache) {
        cacheSettings.setCache(cache);
        return this;
    }

    /**
     * @deprecated use {@link WriterSettings#getWriteBufferTime()} via {@code writerSettings()} instead
     */
    @Deprecated(since = "6.3", forRemoval = true)
    public int getWriteBufferTime() {
        return writerSettings.getWriteBufferTime();
    }

    /**
     * @deprecated use {@link WriterSettings#setWriteBufferTime(int)} via {@code writerSettings()} instead
     */
    @Deprecated(since = "6.3", forRemoval = true)
    public MorphiumConfig setWriteBufferTime(int writeBufferTime) {
        writerSettings.setWriteBufferTime(writeBufferTime);
        return this;
    }
    /**
     * @deprecated use {@link WriterSettings#getBufferedWriter()} via {@code writerSettings()} instead
     */
    @Deprecated(since = "6.3", forRemoval = true)
    public MorphiumWriter getBufferedWriter() {
        return writerSettings.getBufferedWriter();
    }

    /**
     * @deprecated use {@link WriterSettings#setBufferedWriter(MorphiumWriter)} via {@code writerSettings()} instead
     */
    @Deprecated(since = "6.3", forRemoval = true)
    public MorphiumConfig setBufferedWriter(MorphiumWriter bufferedWriter) {
        writerSettings.setBufferedWriter(bufferedWriter);
        return this;
    }

    /**
     * @deprecated use {@link WriterSettings#getWriter()} via {@code writerSettings()} instead
     */
    @Deprecated(since = "6.3", forRemoval = true)
    public MorphiumWriter getWriter() {
        return writerSettings.getWriter();
    }

    /**
     * @deprecated use {@link WriterSettings#setWriter(MorphiumWriter)} via {@code writerSettings()} instead
     */
    @Deprecated(since = "6.3", forRemoval = true)
    public MorphiumConfig setWriter(MorphiumWriter writer) {
        writerSettings.setWriter(writer);
        return this;
    }

    /**
     * @deprecated use {@link ConnectionSettings#getConnectionTimeout()} via {@code connectionSettings()} instead
     */
    @Deprecated(since = "6.3", forRemoval = true)
    public int getConnectionTimeout() {
        return connectionSettings.getConnectionTimeout();
    }

    /**
     * @deprecated use {@link ConnectionSettings#setConnectionTimeout(int)} via {@code connectionSettings()} instead
     */
    @Deprecated(since = "6.3", forRemoval = true)
    public MorphiumConfig setConnectionTimeout(int connectionTimeout) {
        connectionSettings.setConnectionTimeout(connectionTimeout);
        return this;
    }

    /**
     * @deprecated use {@link ConnectionSettings#getMaxWaitTime()} via {@code connectionSettings()} instead
     */
    @Deprecated(since = "6.3", forRemoval = true)
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
     * @deprecated use {@link ConnectionSettings#setMaxWaitTime(int)} via {@code connectionSettings()} instead
     */
    @Deprecated(since = "6.3", forRemoval = true)
    public MorphiumConfig setMaxWaitTime(int maxWaitTime) {
        connectionSettings.setMaxWaitTime(maxWaitTime);
        return this;
    }

    /**
     * @deprecated use {@link DriverSettings#getServerSelectionTimeout()} via {@code driverSettings()} instead
     */
    @Deprecated(since = "6.3", forRemoval = true)
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
     * @deprecated use {@link DriverSettings#setServerSelectionTimeout(int)} via {@code driverSettings()} instead
     */
    @Deprecated(since = "6.3", forRemoval = true)
    public MorphiumConfig setServerSelectionTimeout(int serverSelectionTimeout) {
        driverSettings.setServerSelectionTimeout(serverSelectionTimeout);
        return this;
    }

    /**
     * @deprecated use {@link EncryptionSettings#getCredentialsEncrypted()} via {@code encryptionSettings()} instead
     */
    @Deprecated(since = "6.3", forRemoval = true)
    public Boolean getCredentialsEncrypted() {
        return encryptionSettings.getCredentialsEncrypted();
    }

    /**
     * @deprecated use {@link EncryptionSettings#setCredentialsEncrypted(Boolean)} via {@code encryptionSettings()} instead
     */
    @Deprecated(since = "6.3", forRemoval = true)
    public MorphiumConfig setCredentialsEncrypted(Boolean credentialsEncrypted) {
        encryptionSettings.setCredentialsEncrypted(credentialsEncrypted);
        return this;
    }

    /**
     * @deprecated use {@link AuthSettings#getMongoAuthDb()} via {@code authSettings()} instead
     */
    @Deprecated(since = "6.3", forRemoval = true)
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
     * @deprecated use {@link AuthSettings#setMongoAuthDb(String)} via {@code authSettings()} instead
     */
    @Deprecated(since = "6.3", forRemoval = true)
    public MorphiumConfig setMongoAuthDb(String mongoAuthDb) {
        authSettings.setMongoAuthDb(mongoAuthDb);
        return this;
    }

    /**
     * @deprecated use {@link AuthSettings#getMongoLogin()} via {@code authSettings()} instead
     */
    @Deprecated(since = "6.3", forRemoval = true)
    public String getMongoLogin() {
        return authSettings.getMongoLogin();
    }

    /**
     * @deprecated use {@link AuthSettings#setMongoLogin(String)} via {@code authSettings()} instead
     */
    @Deprecated(since = "6.3", forRemoval = true)
    public MorphiumConfig setMongoLogin(String mongoLogin) {
        authSettings.setMongoLogin(mongoLogin);
        return this;
    }

    /**
     * @deprecated use {@link AuthSettings#getMongoPassword()} via {@code authSettings()} instead
     */
    @Deprecated(since = "6.3", forRemoval = true)
    public String getMongoPassword() {
        return authSettings.getMongoPassword();
    }

    /**
     * @deprecated use {@link AuthSettings#setMongoPassword(String)} via {@code authSettings()} instead
     */
    @Deprecated(since = "6.3", forRemoval = true)
    public MorphiumConfig setMongoPassword(String mongoPassword) {
        authSettings.setMongoPassword(mongoPassword);
        return this;
    }

    /**
     * Authentication mechanism used when connecting to MongoDB.
     * <ul>
     *   <li>{@code null} / {@code "SCRAM-SHA-256"} – standard username/password (default)</li>
     *   <li>{@code "MONGODB-X509"} – X.509 client-certificate authentication.
     *       Requires {@link #setUseSSL(boolean) useSSL=true} and a keystore containing
     *       the client certificate (configure via {@link #setSslContext(SSLContext)}).</li>
     * </ul>
     */
    public String getAuthMechanism() {
        return authSettings.getAuthMechanism();
    }

    public MorphiumConfig setAuthMechanism(String authMechanism) {
        authSettings.setAuthMechanism(authMechanism);
        return this;
    }

    /**
     * @deprecated use {@link DriverSettings#getDefaultReadPreference()} via {@code driverSettings()} instead
     */
    @Deprecated(since = "6.3", forRemoval = true)
    public ReadPreference getDefaultReadPreference() {
        return driverSettings.getDefaultReadPreference();
    }

    /**
     * @deprecated use {@link DriverSettings#setDefaultReadPreference(ReadPreference)} via {@code driverSettings()} instead
     */
    @Deprecated(since = "6.3", forRemoval = true)
    public MorphiumConfig setDefaultReadPreference(ReadPreference defaultReadPreference) {
        driverSettings.setDefaultReadPreference(defaultReadPreference);
        return this;
    }

    /**
     * @deprecated use {@link DriverSettings#getDefaultReadPreferenceType()} via {@code driverSettings()} instead
     */
    @Deprecated(since = "6.3", forRemoval = true)
    public String getDefaultReadPreferenceType() {
        return driverSettings.getDefaultReadPreferenceType();
    }

    /**
     * @deprecated use {@link DriverSettings#setDefaultReadPreferenceType(String)} via {@code driverSettings()} instead
     */
    @Deprecated(since = "6.3", forRemoval = true)
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
     * @deprecated use {@link CacheSettings#getWriteCacheTimeout()} via {@code cacheSettings()} instead
     */
    @Deprecated(since = "6.3", forRemoval = true)
    public int getWriteCacheTimeout() {
        return cacheSettings.getWriteCacheTimeout();
    }

    /**
     * @deprecated use {@link CacheSettings#setWriteCacheTimeout(int)} via {@code cacheSettings()} instead
     */
    @Deprecated(since = "6.3", forRemoval = true)
    public MorphiumConfig setWriteCacheTimeout(int writeCacheTimeout) {
        cacheSettings.setWriteCacheTimeout(writeCacheTimeout);
        return this;
    }


    /**
     * setting hosts as Host:Port
     *
     * @param str list of hosts, with or without port
     * @deprecated use {@code config.clusterSettings().setHostSeed(...)} instead
     */
    @Deprecated(since = "6.3", forRemoval = true)
    public MorphiumConfig setHostSeed(List<String> str) {
        clusterSettings.setHostSeed(str);
        return this;
    }

    /**
     * @deprecated use {@code config.clusterSettings().setHostSeed(...)} instead
     */
    @Deprecated(since = "6.3", forRemoval = true)
    public MorphiumConfig setHostSeed(List<String> str, List<Integer> ports) {
        clusterSettings.setHostSeed(str, ports);
        return this;
    }

    /**
     * @deprecated use {@link ClusterSettings#getHostSeed()} via {@code clusterSettings()} instead
     */
    @Deprecated(since = "6.3", forRemoval = true)
    public List<String> getHostSeed() {
        return clusterSettings.getHostSeed();
    }

    /**
     * @deprecated use {@code config.clusterSettings().setHostSeed(...)} instead
     */
    @Deprecated(since = "6.3", forRemoval = true)
    public MorphiumConfig setHostSeed(String... hostPorts) {
        clusterSettings.setHostSeed(hostPorts);
        return this;
    }

    /**
     * @deprecated use {@code config.clusterSettings().setHostSeed(...)} instead
     */
    @Deprecated(since = "6.3", forRemoval = true)
    public MorphiumConfig setHostSeed(String hostPorts) {
        clusterSettings.setHostSeed(hostPorts);
        return this;
    }

    /**
     * @deprecated use {@code config.clusterSettings().setHostSeed(...)} instead
     */
    @Deprecated(since = "6.3", forRemoval = true)
    public MorphiumConfig setHostSeed(String hosts, String ports) {
        clusterSettings.setHostSeed(hosts, ports);
        return this;
    }

    /**
     * @deprecated use {@code config.clusterSettings().addHostToSeed(...)} instead
     */
    @Deprecated(since = "6.3", forRemoval = true)
    public MorphiumConfig addHostToSeed(String host, int port) {
        clusterSettings.addHostToSeed(host, port);
        return this;
    }

    /**
     * @deprecated use {@code config.clusterSettings().addHostToSeed(...)} instead
     */
    @Deprecated(since = "6.3", forRemoval = true)
    public MorphiumConfig addHostToSeed(String host) {
        clusterSettings.addHostToSeed(host);
        return this;
    }

    /**
     * @deprecated use {@link ConnectionSettings#getMaxConnections()} via {@code connectionSettings()} instead
     */
    @Deprecated(since = "6.3", forRemoval = true)
    public int getMaxConnections() {
        return connectionSettings.getMaxConnections();
    }

    /**
     * @deprecated use {@link ConnectionSettings#setMaxConnections(int)} via {@code connectionSettings()} instead
     */
    @Deprecated(since = "6.3", forRemoval = true)
    public MorphiumConfig setMaxConnections(int maxConnections) {
        connectionSettings.setMaxConnections(maxConnections);
        return this;
    }
    /**
     * @deprecated use {@link ConnectionSettings#getDatabase()} via {@code connectionSettings()} instead
     */
    @Deprecated(since = "6.3", forRemoval = true)
    public String getDatabase() {
        return connectionSettings.getDatabase();
    }

    /**
     * @deprecated use {@link ConnectionSettings#setDatabase(String)} via {@code connectionSettings()} instead
     */
    @Deprecated(since = "6.3", forRemoval = true)
    public MorphiumConfig setDatabase(String database) {
        connectionSettings.setDatabase(database);
        return this;
    }
    /** @deprecated use {@link CacheSettings#getHousekeepingTimeout()} via {@code cacheSettings()} instead */
    @Deprecated(since = "6.3", forRemoval = true)
    public int getHousekeepingTimeout() {
        return cacheSettings.getHousekeepingTimeout();
    }

    /** @deprecated use {@link CacheSettings#setHousekeepingTimeout(int)} via {@code cacheSettings()} instead */
    @Deprecated(since = "6.3", forRemoval = true)
    public MorphiumConfig setHousekeepingTimeout(int housekeepingTimeout) {
        cacheSettings.setHousekeepingTimeout(housekeepingTimeout);
        return this;
    }

    /**
     * for future use - set Global Caching time
     *
     * @return the global cache valid time
     * @deprecated use {@link CacheSettings#getGlobalCacheValidTime()} via {@code cacheSettings()} instead
     */
    @Deprecated(since = "6.3", forRemoval = true)
    public int getGlobalCacheValidTime() {
        return cacheSettings.getGlobalCacheValidTime();
    }

    /** @deprecated use {@link CacheSettings#setGlobalCacheValidTime(int)} via {@code cacheSettings()} instead */
    @Deprecated(since = "6.3", forRemoval = true)
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
     * @deprecated use {@link WriterSettings#setThreadConnectionMultiplier(int)} via {@code writerSettings()} instead
     */
    @Deprecated(since = "6.3", forRemoval = true)
    public MorphiumConfig setThreadConnectionMultiplier(int mp) {
        writerSettings.setThreadConnectionMultiplier(mp);
        return this;
    }

    /**
     * @deprecated use {@link WriterSettings#getThreadConnectionMultiplier()} via {@code writerSettings()} instead
     */
    @Deprecated(since = "6.3", forRemoval = true)
    public int getThreadConnectionMultiplier() {
        return writerSettings.getThreadConnectionMultiplier();
    }


    /**
     * @deprecated use {@link WriterSettings#getAsyncWriter()} via {@code writerSettings()} instead
     */
    @Deprecated(since = "6.3", forRemoval = true)
    public MorphiumWriter getAsyncWriter() {
        return writerSettings.getAsyncWriter();
    }

    /**
     * @deprecated use {@link WriterSettings#setAsyncWriter(MorphiumWriter)} via {@code writerSettings()} instead
     */
    @Deprecated(since = "6.3", forRemoval = true)
    public MorphiumConfig setAsyncWriter(MorphiumWriter asyncWriter) {
        writerSettings.setAsyncWriter(asyncWriter);
        return this;
    }

    /**
     * @deprecated use {@link WriterSettings#getMaximumRetriesBufferedWriter()} via {@code writerSettings()} instead
     */
    @Deprecated(since = "6.3", forRemoval = true)
    public int getMaximumRetriesBufferedWriter() {
        return writerSettings.getMaximumRetriesBufferedWriter();
    }

    /**
     * @deprecated use {@link WriterSettings#setMaximumRetriesBufferedWriter(int)} via {@code writerSettings()} instead
     */
    @Deprecated(since = "6.3", forRemoval = true)
    public MorphiumConfig setMaximumRetriesBufferedWriter(int maximumRetriesBufferedWriter) {
        writerSettings.setMaximumRetriesBufferedWriter(maximumRetriesBufferedWriter);
        return this;
    }

    /**
     * @deprecated use {@link WriterSettings#getMaximumRetriesWriter()} via {@code writerSettings()} instead
     */
    @Deprecated(since = "6.3", forRemoval = true)
    public int getMaximumRetriesWriter() {
        return writerSettings.getMaximumRetriesWriter();
    }

    /**
     * @deprecated use {@link WriterSettings#setMaximumRetriesWriter(int)} via {@code writerSettings()} instead
     */
    @Deprecated(since = "6.3", forRemoval = true)
    public MorphiumConfig setMaximumRetriesWriter(int maximumRetriesWriter) {
        writerSettings.setMaximumRetriesWriter(maximumRetriesWriter);
        return this;
    }

    /**
     * @deprecated use {@link WriterSettings#getMaximumRetriesAsyncWriter()} via {@code writerSettings()} instead
     */
    @Deprecated(since = "6.3", forRemoval = true)
    public int getMaximumRetriesAsyncWriter() {
        return writerSettings.getMaximumRetriesAsyncWriter();
    }

    /**
     * @deprecated use {@link WriterSettings#setMaximumRetriesAsyncWriter(int)} via {@code writerSettings()} instead
     */
    @Deprecated(since = "6.3", forRemoval = true)
    public MorphiumConfig setMaximumRetriesAsyncWriter(int maximumRetriesAsyncWriter) {
        writerSettings.setMaximumRetriesAsyncWriter(maximumRetriesAsyncWriter);
        return this;
    }

    /**
     * @deprecated use {@link WriterSettings#getRetryWaitTimeBufferedWriter()} via {@code writerSettings()} instead
     */
    @Deprecated(since = "6.3", forRemoval = true)
    public int getRetryWaitTimeBufferedWriter() {
        return writerSettings.getRetryWaitTimeBufferedWriter();
    }

    /**
     * @deprecated use {@link WriterSettings#setRetryWaitTimeBufferedWriter(int)} via {@code writerSettings()} instead
     */
    @Deprecated(since = "6.3", forRemoval = true)
    public MorphiumConfig setRetryWaitTimeBufferedWriter(int retryWaitTimeBufferedWriter) {
        writerSettings.setRetryWaitTimeBufferedWriter(retryWaitTimeBufferedWriter);
        return this;
    }

    /**
     * @deprecated use {@link WriterSettings#getRetryWaitTimeWriter()} via {@code writerSettings()} instead
     */
    @Deprecated(since = "6.3", forRemoval = true)
    public int getRetryWaitTimeWriter() {
        return writerSettings.getRetryWaitTimeWriter();
    }

    /**
     * @deprecated use {@link WriterSettings#setRetryWaitTimeWriter(int)} via {@code writerSettings()} instead
     */
    @Deprecated(since = "6.3", forRemoval = true)
    public MorphiumConfig setRetryWaitTimeWriter(int retryWaitTimeWriter) {
        writerSettings.setRetryWaitTimeWriter(retryWaitTimeWriter);
        return this;
    }

    /**
     * @deprecated use {@link WriterSettings#getRetryWaitTimeAsyncWriter()} via {@code writerSettings()} instead
     */
    @Deprecated(since = "6.3", forRemoval = true)
    public int getRetryWaitTimeAsyncWriter() {
        return writerSettings.getRetryWaitTimeAsyncWriter();
    }

    /**
     * @deprecated use {@link WriterSettings#setRetryWaitTimeAsyncWriter(int)} via {@code writerSettings()} instead
     */
    @Deprecated(since = "6.3", forRemoval = true)
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

    @SuppressWarnings("unchecked")
    protected <T extends Settings> T getSettingByType(Class<?> cls) {
        for (var s : settings) {
            if (s.getClass().equals(cls)) return (T) s;
        }
        return null;
    }

    @SuppressWarnings("unchecked")
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
     * @deprecated use {@link CacheSettings#isReadCacheEnabled()} via {@code cacheSettings()} instead
     */
    @Deprecated(since = "6.3", forRemoval = true)
    public boolean isReadCacheEnabled() {
        return cacheSettings.isReadCacheEnabled();
    }

    /**
     * @deprecated use {@link CacheSettings#setReadCacheEnabled(boolean)} via {@code cacheSettings()} instead
     */
    @Deprecated(since = "6.3", forRemoval = true)
    public MorphiumConfig setReadCacheEnabled(boolean readCacheEnabled) {
        cacheSettings.setReadCacheEnabled(readCacheEnabled);
        return this;
    }

    /**
     * @deprecated use {@link CacheSettings#setReadCacheEnabled(boolean)} via {@code cacheSettings()} instead
     */
    @Deprecated(since = "6.3", forRemoval = true)
    public MorphiumConfig disableReadCache() {
        cacheSettings.setReadCacheEnabled(false);
        return this;
    }

    /**
     * @deprecated use {@link CacheSettings#setReadCacheEnabled(boolean)} via {@code cacheSettings()} instead
     */
    @Deprecated(since = "6.3", forRemoval = true)
    public MorphiumConfig enableReadCache() {
        cacheSettings.setReadCacheEnabled(true);
        return this;
    }

    /**
     * @deprecated use {@link CacheSettings#isAsyncWritesEnabled()} via {@code cacheSettings()} instead
     */
    @Deprecated(since = "6.3", forRemoval = true)
    public boolean isAsyncWritesEnabled() {
        return cacheSettings.isAsyncWritesEnabled();
    }

    /**
     * @deprecated use {@link CacheSettings#setAsyncWritesEnabled(boolean)} via {@code cacheSettings()} instead
     */
    @Deprecated(since = "6.3", forRemoval = true)
    public MorphiumConfig setAsyncWritesEnabled(boolean asyncWritesEnabled) {
        cacheSettings.setAsyncWritesEnabled(asyncWritesEnabled);
        return this;
    }

    /**
     * @deprecated use {@link CacheSettings#setAsyncWritesEnabled(boolean)} via {@code cacheSettings()} instead
     */
    @Deprecated(since = "6.3", forRemoval = true)
    public MorphiumConfig disableAsyncWrites() {
        cacheSettings.setAsyncWritesEnabled(false);
        return this;
    }

    /**
     * @deprecated use {@link CacheSettings#setAsyncWritesEnabled(boolean)} via {@code cacheSettings()} instead
     */
    @Deprecated(since = "6.3", forRemoval = true)
    public MorphiumConfig enableAsyncWrites() {
        cacheSettings.setAsyncWritesEnabled(true);
        return this;
    }

    /**
     * @deprecated use {@link CacheSettings#isBufferedWritesEnabled()} via {@code cacheSettings()} instead
     */
    @Deprecated(since = "6.3", forRemoval = true)
    public boolean isBufferedWritesEnabled() {
        return cacheSettings.isBufferedWritesEnabled();
    }

    /**
     * @deprecated use {@link CacheSettings#setBufferedWritesEnabled(boolean)} via {@code cacheSettings()} instead
     */
    @Deprecated(since = "6.3", forRemoval = true)
    public MorphiumConfig setBufferedWritesEnabled(boolean bufferedWritesEnabled) {
        cacheSettings.setBufferedWritesEnabled(bufferedWritesEnabled);
        return this;
    }

    /**
     * @deprecated use {@link CacheSettings#setBufferedWritesEnabled(boolean)} via {@code cacheSettings()} instead
     */
    @Deprecated(since = "6.3", forRemoval = true)
    public MorphiumConfig disableBufferedWrites() {
        cacheSettings.setBufferedWritesEnabled(false);
        return this;
    }

    /**
     * @deprecated use {@link CacheSettings#setBufferedWritesEnabled(boolean)} via {@code cacheSettings()} instead
     */
    @Deprecated(since = "6.3", forRemoval = true)
    public MorphiumConfig enableBufferedWrites() {
        cacheSettings.setBufferedWritesEnabled(true);
        return this;
    }

    /**
     * @deprecated use {@link ObjectMappingSettings#isAutoValues()} via {@code objectMappingSettings()} instead
     */
    @Deprecated(since = "6.3", forRemoval = true)
    public boolean isAutoValuesEnabled() {
        return objectMappingSettings.isAutoValues();
    }

    /**
     * @deprecated use {@link ObjectMappingSettings#setAutoValues(boolean)} via {@code objectMappingSettings()} instead
     */
    @Deprecated(since = "6.3", forRemoval = true)
    public MorphiumConfig setAutoValuesEnabled(boolean enabled) {
        objectMappingSettings.setAutoValues(enabled);
        return this;
    }

    /**
     * @deprecated use {@link ObjectMappingSettings#setAutoValues(boolean)} via {@code objectMappingSettings()} instead
     */
    @Deprecated(since = "6.3", forRemoval = true)
    public MorphiumConfig enableAutoValues() {
        objectMappingSettings.setAutoValues(true);
        return this;
    }

    /**
     * @deprecated use {@link ObjectMappingSettings#setAutoValues(boolean)} via {@code objectMappingSettings()} instead
     */
    @Deprecated(since = "6.3", forRemoval = true)
    public MorphiumConfig disableAutoValues() {
        objectMappingSettings.setAutoValues(false);
        return this;
    }

    /**
     * @deprecated use {@link ObjectMappingSettings#isCamelCaseConversionEnabled()} via {@code objectMappingSettings()} instead
     */
    @Deprecated(since = "6.3", forRemoval = true)
    public boolean isCamelCaseConversionEnabled() {
        return objectMappingSettings.isCamelCaseConversionEnabled();
    }

    /**
     * @deprecated use {@link ObjectMappingSettings#setCamelCaseConversionEnabled(boolean)} via {@code objectMappingSettings()} instead
     */
    @Deprecated(since = "6.3", forRemoval = true)
    public MorphiumConfig setCamelCaseConversionEnabled(boolean camelCaseConversionEnabled) {
        objectMappingSettings.setCamelCaseConversionEnabled(camelCaseConversionEnabled);
        return this;
    }

    /**
     * @deprecated use {@link MessagingSettings#getThreadPoolMessagingCoreSize()} via {@code messagingSettings()} instead
     */
    @Deprecated(since = "6.3", forRemoval = true)
    public int getThreadPoolMessagingCoreSize() {
        return messagingSettings.getThreadPoolMessagingCoreSize();
    }

    /**
     * @deprecated use {@link MessagingSettings#setThreadPoolMessagingCoreSize(int)} via {@code messagingSettings()} instead
     */
    @Deprecated(since = "6.3", forRemoval = true)
    public MorphiumConfig setThreadPoolMessagingCoreSize(int threadPoolMessagingCoreSize) {
        messagingSettings.setThreadPoolMessagingCoreSize(threadPoolMessagingCoreSize);
        return this;
    }

    /**
     * @deprecated use {@link MessagingSettings#getThreadPoolMessagingMaxSize()} via {@code messagingSettings()} instead
     */
    @Deprecated(since = "6.3", forRemoval = true)
    public int getThreadPoolMessagingMaxSize() {
        return messagingSettings.getThreadPoolMessagingMaxSize();
    }

    /**
     * @deprecated use {@link MessagingSettings#setThreadPoolMessagingMaxSize(int)} via {@code messagingSettings()} instead
     */
    @Deprecated(since = "6.3", forRemoval = true)
    public MorphiumConfig setThreadPoolMessagingMaxSize(int threadPoolMessagingMaxSize) {
        messagingSettings.setThreadPoolMessagingMaxSize(threadPoolMessagingMaxSize);
        return this;
    }

    /**
     * @deprecated use {@link MessagingSettings#getThreadPoolMessagingKeepAliveTime()} via {@code messagingSettings()} instead
     */
    @Deprecated(since = "6.3", forRemoval = true)
    public long getThreadPoolMessagingKeepAliveTime() {
        return messagingSettings.getThreadPoolMessagingKeepAliveTime();
    }

    /**
     * @deprecated use {@link MessagingSettings#setThreadPoolMessagingKeepAliveTime(long)} via {@code messagingSettings()} instead
     */
    @Deprecated(since = "6.3", forRemoval = true)
    public MorphiumConfig setThreadPoolMessagingKeepAliveTime(long threadPoolMessagingKeepAliveTime) {
        messagingSettings.setThreadPoolMessagingKeepAliveTime(threadPoolMessagingKeepAliveTime);
        return this;
    }

    /**
     * @deprecated use {@link ThreadPoolSettings#getThreadPoolAsyncOpCoreSize()} via {@code threadPoolSettings()} instead
     */
    @Deprecated(since = "6.3", forRemoval = true)
    public int getThreadPoolAsyncOpCoreSize() {
        return threadPoolSettings.getThreadPoolAsyncOpCoreSize();
    }

    /**
     * @deprecated use {@link ThreadPoolSettings#setThreadPoolAsyncOpCoreSize(int)} via {@code threadPoolSettings()} instead
     */
    @Deprecated(since = "6.3", forRemoval = true)
    public MorphiumConfig setThreadPoolAsyncOpCoreSize(int threadPoolAsyncOpCoreSize) {
        threadPoolSettings.setThreadPoolAsyncOpCoreSize(threadPoolAsyncOpCoreSize);
        return this;
    }

    /**
     * @deprecated use {@link ThreadPoolSettings#getThreadPoolAsyncOpMaxSize()} via {@code threadPoolSettings()} instead
     */
    @Deprecated(since = "6.3", forRemoval = true)
    public int getThreadPoolAsyncOpMaxSize() {
        return threadPoolSettings.getThreadPoolAsyncOpMaxSize();
    }

    /**
     * @deprecated use {@link ThreadPoolSettings#setThreadPoolAsyncOpMaxSize(int)} via {@code threadPoolSettings()} instead
     */
    @Deprecated(since = "6.3", forRemoval = true)
    public MorphiumConfig setThreadPoolAsyncOpMaxSize(int threadPoolAsyncOpMaxSize) {
        threadPoolSettings.setThreadPoolAsyncOpMaxSize(threadPoolAsyncOpMaxSize);
        return this;
    }

    /**
     * @deprecated use {@link ThreadPoolSettings#getThreadPoolAsyncOpKeepAliveTime()} via {@code threadPoolSettings()} instead
     */
    @Deprecated(since = "6.3", forRemoval = true)
    public long getThreadPoolAsyncOpKeepAliveTime() {
        return threadPoolSettings.getThreadPoolAsyncOpKeepAliveTime();
    }

    /**
     * @deprecated use {@link ThreadPoolSettings#setThreadPoolAsyncOpKeepAliveTime(long)} via {@code threadPoolSettings()} instead
     */
    @Deprecated(since = "6.3", forRemoval = true)
    public MorphiumConfig setThreadPoolAsyncOpKeepAliveTime(long threadPoolAsyncOpKeepAliveTime) {
        threadPoolSettings.setThreadPoolAsyncOpKeepAliveTime(threadPoolAsyncOpKeepAliveTime);
        return this;
    }

    /**
     * @deprecated use {@link ObjectMappingSettings#isObjectSerializationEnabled()} via {@code objectMappingSettings()} instead
     */
    @Deprecated(since = "6.3", forRemoval = true)
    public boolean isObjectSerializationEnabled() {
        return objectMappingSettings.isObjectSerializationEnabled();
    }

    /**
     * @deprecated use {@link ObjectMappingSettings#setObjectSerializationEnabled(boolean)} via {@code objectMappingSettings()} instead
     */
    @Deprecated(since = "6.3", forRemoval = true)
    public MorphiumConfig setObjectSerializationEnabled(boolean objectSerializationEnabled) {
        objectMappingSettings.setObjectSerializationEnabled(objectSerializationEnabled);
        return this;
    }

    /** @deprecated use {@link DriverSettings#getHeartbeatFrequency()} via {@code driverSettings()} instead */
    @Deprecated(since = "6.3", forRemoval = true)
    public int getHeartbeatFrequency() {
        return driverSettings.getHeartbeatFrequency();
    }

    /** @deprecated use {@link DriverSettings#setHeartbeatFrequency(int)} via {@code driverSettings()} instead */
    @Deprecated(since = "6.3", forRemoval = true)
    public MorphiumConfig setHeartbeatFrequency(int heartbeatFrequency) {
        driverSettings.setHeartbeatFrequency(heartbeatFrequency);
        return this;
    }


    /**
     * @deprecated use {@link ConnectionSettings#setMinConnections(int)} via {@code connectionSettings()} instead
     */
    @Deprecated(since = "6.3", forRemoval = true)
    public MorphiumConfig setMinConnections(int minConnections) {
        connectionSettings.setMinConnections(minConnections);
        return this;
    }

    /** @deprecated use {@link DriverSettings#getLocalThreshold()} via {@code driverSettings()} instead */
    @Deprecated(since = "6.3", forRemoval = true)
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
     * @deprecated use {@link DriverSettings#setLocalThreshold(int)} via {@code driverSettings()} instead
     */
    @Deprecated(since = "6.3", forRemoval = true)
    public MorphiumConfig setLocalThreshold(int localThreshold) {
        driverSettings.setLocalThreshold(localThreshold);
        return this;
    }

    /** @deprecated use {@link DriverSettings#getMaxConnectionIdleTime()} via {@code driverSettings()} instead */
    @Deprecated(since = "6.3", forRemoval = true)
    public int getMaxConnectionIdleTime() {
        return driverSettings.getMaxConnectionIdleTime();
    }

    /** @deprecated use {@link DriverSettings#setMaxConnectionIdleTime(int)} via {@code driverSettings()} instead */
    @Deprecated(since = "6.3", forRemoval = true)
    public MorphiumConfig setMaxConnectionIdleTime(int maxConnectionIdleTime) {
        driverSettings.setMaxConnectionIdleTime(maxConnectionIdleTime);
        return this;
    }

    /** @deprecated use {@link DriverSettings#getMaxConnectionLifeTime()} via {@code driverSettings()} instead */
    @Deprecated(since = "6.3", forRemoval = true)
    public int getMaxConnectionLifeTime() {
        return driverSettings.getMaxConnectionLifeTime();
    }

    /** @deprecated use {@link DriverSettings#setMaxConnectionLifeTime(int)} via {@code driverSettings()} instead */
    @Deprecated(since = "6.3", forRemoval = true)
    public MorphiumConfig setMaxConnectionLifeTime(int maxConnectionLifeTime) {
        driverSettings.setMaxConnectionLifeTime(maxConnectionLifeTime);
        return this;
    }

    /**
     * @deprecated use {@link ClusterSettings#getRequiredReplicaSetName()} via {@code clusterSettings()} instead
     */
    @Deprecated(since = "6.3", forRemoval = true)
    public String getRequiredReplicaSetName() {
        return clusterSettings.getRequiredReplicaSetName();
    }

    /**
     * @deprecated use {@link ClusterSettings#setRequiredReplicaSetName(String)} via {@code clusterSettings()} instead
     */
    @Deprecated(since = "6.3", forRemoval = true)
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
    /** @deprecated use {@link DriverSettings#getReadTimeout()} via {@code driverSettings()} instead */
    @Deprecated(since = "6.3", forRemoval = true)
    public int getReadTimeout() {
        return driverSettings.getReadTimeout();
    }

    /** @deprecated use {@link DriverSettings#setReadTimeout(int)} via {@code driverSettings()} instead */
    @Deprecated(since = "6.3", forRemoval = true)
    public MorphiumConfig setReadTimeout(int readTimeout) {
        driverSettings.setReadTimeout(readTimeout);
        return this;
    }

    /** @deprecated use {@link DriverSettings#isRetryReads()} via {@code driverSettings()} instead */
    @Deprecated(since = "6.3", forRemoval = true)
    public boolean isRetryReads() {
        return driverSettings.isRetryReads();
    }

    /** @deprecated use {@link DriverSettings#setRetryReads(boolean)} via {@code driverSettings()} instead */
    @Deprecated(since = "6.3", forRemoval = true)
    public void setRetryReads(boolean retryReads) {
        driverSettings.setRetryReads(retryReads);
    }

    /** @deprecated use {@link DriverSettings#isRetryWrites()} via {@code driverSettings()} instead */
    @Deprecated(since = "6.3", forRemoval = true)
    public boolean isRetryWrites() {
        return driverSettings.isRetryWrites();
    }

    /** @deprecated use {@link DriverSettings#setRetryWrites(boolean)} via {@code driverSettings()} instead */
    @Deprecated(since = "6.3", forRemoval = true)
    public void setRetryWrites(boolean retryWrites) {
        driverSettings.setRetryWrites(retryWrites);
    }

    /** @deprecated use {@link DriverSettings#getUuidRepresentation()} via {@code driverSettings()} instead */
    @Deprecated(since = "6.3", forRemoval = true)
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
     * @deprecated use {@link CollectionCheckSettings#getIndexCheck()} via {@code collectionCheckSettings()} instead
     */
    @Deprecated(since = "6.3", forRemoval = true)
    public IndexCheck getIndexCheck() {
        return collectionCheckSettings.getIndexCheck();
    }

    /**
     * @deprecated use {@link CollectionCheckSettings#setIndexCheck(IndexCheck)} via {@code collectionCheckSettings()} instead
     */
    @Deprecated(since = "6.3", forRemoval = true)
    public void setIndexCheck(IndexCheck indexCheck) {
        collectionCheckSettings.setIndexCheck(indexCheck);
    }

    /**
     * @deprecated use {@link CollectionCheckSettings#getCappedCheck()} via {@code collectionCheckSettings()} instead
     */
    @Deprecated(since = "6.3", forRemoval = true)
    public CappedCheck getCappedCheck() {
        return collectionCheckSettings.getCappedCheck();
    }

    /**
     * @deprecated use {@link CollectionCheckSettings#setCappedCheck(CappedCheck)} via {@code collectionCheckSettings()} instead
     */
    @Deprecated(since = "6.3", forRemoval = true)
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
     * @deprecated use {@link DriverSettings#getIdleSleepTime()} via {@code driverSettings()} instead
     */
    @Deprecated(since = "6.3", forRemoval = true)
    public int getIdleSleepTime() {
        return driverSettings.getIdleSleepTime();
    }

    /**
     * @deprecated use {@link DriverSettings#setIdleSleepTime(int)} via {@code driverSettings()} instead
     */
    @Deprecated(since = "6.3", forRemoval = true)
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
     * @deprecated use {@link ConnectionSettings#getMinConnections()} via {@code connectionSettings()} instead
     */
    @Deprecated(since = "6.3", forRemoval = true)
    public int getMinConnections() {
        return connectionSettings.getMinConnections();
    }

}
