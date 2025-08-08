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
public class DriverSettings extends Settings {

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
    private int idleSleepTime = 20;


    private String driverName = PooledDriver.driverName;
    @Transient
    private ReadPreference defaultReadPreference = ReadPreference.nearest();
    @Transient
    private String defaultReadPreferenceType;

    private int serverSelectionTimeout = 30000;

    public int getIdleSleepTime() {
        return idleSleepTime;
    }

    public DriverSettings setIdleSleepTime(int idleSleepTime) {
        this.idleSleepTime = idleSleepTime;
        return this;
    }

    public int getServerSelectionTimeout() {
        return serverSelectionTimeout;
    }
    public DriverSettings setServerSelectionTimeout(int serverSelectionTimeout) {
        this.serverSelectionTimeout = serverSelectionTimeout;
        return this;
    }

    public MorphiumConfig.CompressionType getCompressionType() {
        return compressionType;
    }
    public DriverSettings setCompressionType(MorphiumConfig.CompressionType compressionType) {
        this.compressionType = compressionType;
        return this;
    }
    public String getUuidRepresentation() {
        return uuidRepresentation;
    }
    public DriverSettings setUuidRepresentation(String uuidRepresentation) {
        this.uuidRepresentation = uuidRepresentation;
        return this;
    }
    public boolean isRetryReads() {
        return retryReads;
    }
    public DriverSettings setRetryReads(boolean retryReads) {
        this.retryReads = retryReads;
        return this;
    }
    public boolean isRetryWrites() {
        return retryWrites;
    }
    public DriverSettings setRetryWrites(boolean retryWrites) {
        this.retryWrites = retryWrites;
        return this;
    }
    public int getReadTimeout() {
        return readTimeout;
    }
    public DriverSettings setReadTimeout(int readTimeout) {
        this.readTimeout = readTimeout;
        return this;
    }
    public int getLocalThreshold() {
        return localThreshold;
    }
    public DriverSettings setLocalThreshold(int localThreshold) {
        this.localThreshold = localThreshold;
        return this;
    }
    public int getMaxConnectionIdleTime() {
        return maxConnectionIdleTime;
    }
    public DriverSettings setMaxConnectionIdleTime(int maxConnectionIdleTime) {
        this.maxConnectionIdleTime = maxConnectionIdleTime;
        return this;
    }
    public int getMaxConnectionLifeTime() {
        return maxConnectionLifeTime;
    }
    public DriverSettings setMaxConnectionLifeTime(int maxConnectionLifeTime) {
        this.maxConnectionLifeTime = maxConnectionLifeTime;
        return this;
    }
    public int getCursorBatchSize() {
        return cursorBatchSize;
    }
    public DriverSettings setCursorBatchSize(int cursorBatchSize) {
        this.cursorBatchSize = cursorBatchSize;
        return this;
    }
    public String getDriverName() {
        return driverName;
    }
    public DriverSettings setDriverName(String driverName) {
        this.driverName = driverName;
        return this;
    }
    public ReadPreference getDefaultReadPreference() {
        return defaultReadPreference;
    }
    public DriverSettings setDefaultReadPreference(ReadPreference defaultReadPreference) {
        this.defaultReadPreference = defaultReadPreference;
        return this;
    }
    public String getDefaultReadPreferenceType() {
        return defaultReadPreferenceType;
    }
    public DriverSettings setDefaultReadPreferenceType(String defaultReadPreferenceType) {
        this.defaultReadPreferenceType = defaultReadPreferenceType;
        return this;
    }
    public int getHeartbeatFrequency() {
        return heartbeatFrequency;
    }
    public DriverSettings setHeartbeatFrequency(int heartbeatFrequency) {
        this.heartbeatFrequency = heartbeatFrequency;
        return this;
    }
}
