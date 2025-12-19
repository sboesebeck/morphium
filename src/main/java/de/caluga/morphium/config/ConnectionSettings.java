package de.caluga.morphium.config;

import de.caluga.morphium.annotations.Embedded;
import de.caluga.morphium.annotations.Transient;

import javax.net.ssl.SSLContext;
import java.util.ArrayList;
import java.util.List;

@Embedded
public class ConnectionSettings extends Settings {

    private int maxWaitTime = 2000;
    private int connectionTimeout = 0;
    private int heartbeatFrequency = 1000;
    private int maxConnections = 250;
    private int minConnections = 1;
    private String database;
    private int retriesOnNetworkError = 1;
    private int sleepBetweenNetworkErrorRetries = 1000;
    private boolean useSSL = false;
    @Transient
    private SSLContext sslContext;
    private boolean sslInvalidHostNameAllowed = false;

    public int getMaxWaitTime() {
        return maxWaitTime;
    }
    public ConnectionSettings setMaxWaitTime(int maxWaitTime) {
        this.maxWaitTime = maxWaitTime;
        return this;
    }
    public int getConnectionTimeout() {
        return connectionTimeout;
    }
    public ConnectionSettings setConnectionTimeout(int connectionTimeout) {
        this.connectionTimeout = connectionTimeout;
        return this;
    }

    public int getHeartbeatFrequency() {
        return heartbeatFrequency;
    }
    public ConnectionSettings setHeartbeatFrequency(int heartbeatFrequency) {
        this.heartbeatFrequency = heartbeatFrequency;
        return this;
    }
    public int getMaxConnections() {
        return maxConnections;
    }
    public ConnectionSettings setMaxConnections(int maxConnections) {
        this.maxConnections = maxConnections;
        return this;
    }
    public int getMinConnections() {
        return minConnections;
    }
    public ConnectionSettings setMinConnections(int minConnections) {
        this.minConnections = minConnections;
        return this;
    }
    public String getDatabase() {
        return database;
    }
    public ConnectionSettings setDatabase(String database) {
        this.database = database;
        return this;
    }
    public int getRetriesOnNetworkError() {
        return retriesOnNetworkError;
    }
    public ConnectionSettings setRetriesOnNetworkError(int retriesOnNetworkError) {
        this.retriesOnNetworkError = retriesOnNetworkError;
        return this;
    }
    public int getSleepBetweenNetworkErrorRetries() {
        return sleepBetweenNetworkErrorRetries;
    }

    public ConnectionSettings setSleepBetweenNetworkErrorRetries(int sleepBetweenNetworkErrorRetries) {
        this.sleepBetweenNetworkErrorRetries = sleepBetweenNetworkErrorRetries;
        return this;
    }

    public boolean isUseSSL() {
        return useSSL;
    }

    public ConnectionSettings setUseSSL(boolean useSSL) {
        this.useSSL = useSSL;
        return this;
    }

    public SSLContext getSslContext() {
        return sslContext;
    }

    public ConnectionSettings setSslContext(SSLContext sslContext) {
        this.sslContext = sslContext;
        return this;
    }

    public boolean isSslInvalidHostNameAllowed() {
        return sslInvalidHostNameAllowed;
    }

    public ConnectionSettings setSslInvalidHostNameAllowed(boolean sslInvalidHostNameAllowed) {
        this.sslInvalidHostNameAllowed = sslInvalidHostNameAllowed;
        return this;
    }
}
