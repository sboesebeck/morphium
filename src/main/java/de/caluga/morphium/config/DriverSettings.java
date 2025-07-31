package de.caluga.morphium.config;

import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.annotations.Embedded;
import de.caluga.morphium.annotations.Transient;
import de.caluga.morphium.driver.ReadPreference;
import de.caluga.morphium.driver.wire.PooledDriver;

import javax.net.ssl.SSLContext;
import java.util.ArrayList;
import java.util.List;

@Embedded
public class DriverSettings {

    private MorphiumConfig.CompressionType compressionType = MorphiumConfig.CompressionType.NONE;
    private String uuidRepresentation;
    private boolean retryReads = false;
    private boolean retryWrites = false;
    private int readTimeout = 0;
    private int localThreshold = 15;
    private int maxConnectionIdleTime = 30000;
    private int maxConnectionLifeTime = 600000;
    private int cursorBatchSize = 1000;
    private int heartbeatFrequency = 1000;


    private String driverName = PooledDriver.driverName;
    private String mongoLogin = null, mongoPassword = null, mongoAuthDb = null;
    @Transient
    private ReadPreference defaultReadPreference = ReadPreference.nearest();
    @Transient
    private String defaultReadPreferenceType;

    private int serverSelectionTimeout = -1;


    public int getServerSelectionTimeout() {
        return serverSelectionTimeout;
    }
    public void setServerSelectionTimeout(int serverSelectionTimeout) {
        this.serverSelectionTimeout = serverSelectionTimeout;
    }

    public MorphiumConfig.CompressionType getCompressionType() {
        return compressionType;
    }
    public void setCompressionType(MorphiumConfig.CompressionType compressionType) {
        this.compressionType = compressionType;
    }
    public String getUuidRepresentation() {
        return uuidRepresentation;
    }
    public void setUuidRepresentation(String uuidRepresentation) {
        this.uuidRepresentation = uuidRepresentation;
    }
    public boolean isRetryReads() {
        return retryReads;
    }
    public void setRetryReads(boolean retryReads) {
        this.retryReads = retryReads;
    }
    public boolean isRetryWrites() {
        return retryWrites;
    }
    public void setRetryWrites(boolean retryWrites) {
        this.retryWrites = retryWrites;
    }
    public int getReadTimeout() {
        return readTimeout;
    }
    public void setReadTimeout(int readTimeout) {
        this.readTimeout = readTimeout;
    }
    public int getLocalThreshold() {
        return localThreshold;
    }
    public void setLocalThreshold(int localThreshold) {
        this.localThreshold = localThreshold;
    }
    public int getMaxConnectionIdleTime() {
        return maxConnectionIdleTime;
    }
    public void setMaxConnectionIdleTime(int maxConnectionIdleTime) {
        this.maxConnectionIdleTime = maxConnectionIdleTime;
    }
    public int getMaxConnectionLifeTime() {
        return maxConnectionLifeTime;
    }
    public void setMaxConnectionLifeTime(int maxConnectionLifeTime) {
        this.maxConnectionLifeTime = maxConnectionLifeTime;
    }
    public int getCursorBatchSize() {
        return cursorBatchSize;
    }
    public void setCursorBatchSize(int cursorBatchSize) {
        this.cursorBatchSize = cursorBatchSize;
    }
    public String getDriverName() {
        return driverName;
    }
    public void setDriverName(String driverName) {
        this.driverName = driverName;
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
    public String getMongoAuthDb() {
        return mongoAuthDb;
    }
    public void setMongoAuthDb(String mongoAuthDb) {
        this.mongoAuthDb = mongoAuthDb;
    }
    public ReadPreference getDefaultReadPreference() {
        return defaultReadPreference;
    }
    public void setDefaultReadPreference(ReadPreference defaultReadPreference) {
        this.defaultReadPreference = defaultReadPreference;
    }
    public String getDefaultReadPreferenceType() {
        return defaultReadPreferenceType;
    }
    public void setDefaultReadPreferenceType(String defaultReadPreferenceType) {
        this.defaultReadPreferenceType = defaultReadPreferenceType;
    }
    public int getHeartbeatFrequency() {
        return heartbeatFrequency;
    }
    public void setHeartbeatFrequency(int heartbeatFrequency) {
        this.heartbeatFrequency = heartbeatFrequency;
    }
}
