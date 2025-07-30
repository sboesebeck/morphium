package de.caluga.morphium.config;

import de.caluga.morphium.annotations.Embedded;

import javax.net.ssl.SSLContext;
import java.util.ArrayList;
import java.util.List;

@Embedded
public class ConnectionSettings {
    private int replicaSetMonitoringTimeout = 5000;
    private List<String> hostSeed = new ArrayList<>();
    private String requiredReplicaSetName = null;

    private int serverSelectionTimeout = 30000;
    private int maxWaitTime = 2000;
    private int connectionTimeout = 0;
    private boolean replicaset = true;
    private String atlasUrl = null;
    private int heartbeatFrequency = 1000;
    private int maxConnections = 250, housekeepingTimeout = 5000;
    private int minConnections = 1;
    private int idleSleepTime = 20;
    private String database;
    private int retriesOnNetworkError = 1;
    private int sleepBetweenNetworkErrorRetries = 1000;
    private boolean useSSL = false;
    private SSLContext sslContext = null;
    private boolean sslInvalidHostNameAllowed = false;

    /**
     * setting hosts as Host:Port
     *
     * @param str list of hosts, with or without port
     */

    public ConnectionSettings setHostSeed(List<String> str, List<Integer> ports) {
        hostSeed.clear();

        for (int i = 0; i < str.size(); i++) {
            String host = str.get(i).replaceAll(" ", "") + ":" + ports.get(i);
            hostSeed.add(host);
        }

        return this;
    }

    public List<String> getHostSeed() {
        if (hostSeed == null) hostSeed = new ArrayList<>();

        return hostSeed;
    }

    public ConnectionSettings setHostSeed(String... hostPorts) {
        hostSeed.clear();

        for (String h : hostPorts) {
            addHostToSeed(h);
        }

        return this;
    }

    public ConnectionSettings setHostSeed(String hostPorts) {
        hostSeed.clear();
        String[] h = hostPorts.split(",");

        for (String host : h) {
            addHostToSeed(host);
        }

        return this;
    }

    public ConnectionSettings setHostSeed(String hosts, String ports) {
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

    public ConnectionSettings addHostToSeed(String host, int port) {
        host = host.replaceAll(" ", "") + ":" + port;

        if (hostSeed == null) {
            hostSeed = new ArrayList<>();
        }

        hostSeed.add(host);
        return this;
    }

    public ConnectionSettings addHostToSeed(String host) {
        host = host.replaceAll(" ", "");

        if (host.contains(":")) {
            String[] h = host.split(":");
            addHostToSeed(h[0], Integer.parseInt(h[1]));
        } else {
            addHostToSeed(host, 27017);
        }

        return this;
    }

    public boolean hostSeedIsSet() {
        return hostSeed != null && !hostSeed.isEmpty();
    }
    public int getReplicaSetMonitoringTimeout() {
        return replicaSetMonitoringTimeout;
    }
    public void setReplicaSetMonitoringTimeout(int replicaSetMonitoringTimeout) {
        this.replicaSetMonitoringTimeout = replicaSetMonitoringTimeout;
    }
    public void setHostSeed(List<String> hostSeed) {
        this.hostSeed = hostSeed;
    }
    public String getRequiredReplicaSetName() {
        return requiredReplicaSetName;
    }
    public void setRequiredReplicaSetName(String requiredReplicaSetName) {
        this.requiredReplicaSetName = requiredReplicaSetName;
    }
    public int getMaxWaitTime() {
        return maxWaitTime;
    }
    public void setMaxWaitTime(int maxWaitTime) {
        this.maxWaitTime = maxWaitTime;
    }
    public int getConnectionTimeout() {
        return connectionTimeout;
    }
    public void setConnectionTimeout(int connectionTimeout) {
        this.connectionTimeout = connectionTimeout;
    }
    public boolean isReplicaset() {
        return replicaset;
    }
    public void setReplicaset(boolean replicaset) {
        this.replicaset = replicaset;
    }
    public String getAtlasUrl() {
        return atlasUrl;
    }
    public void setAtlasUrl(String atlasUrl) {
        this.atlasUrl = atlasUrl;
    }
    public int getHeartbeatFrequency() {
        return heartbeatFrequency;
    }
    public void setHeartbeatFrequency(int heartbeatFrequency) {
        this.heartbeatFrequency = heartbeatFrequency;
    }
    public int getMaxConnections() {
        return maxConnections;
    }
    public void setMaxConnections(int maxConnections) {
        this.maxConnections = maxConnections;
    }
    public int getMinConnections() {
        return minConnections;
    }
    public void setMinConnections(int minConnections) {
        this.minConnections = minConnections;
    }
    public int getIdleSleepTime() {
        return idleSleepTime;
    }
    public void setIdleSleepTime(int idleSleepTime) {
        this.idleSleepTime = idleSleepTime;
    }
    public String getDatabase() {
        return database;
    }
    public void setDatabase(String database) {
        this.database = database;
    }
    public int getRetriesOnNetworkError() {
        return retriesOnNetworkError;
    }
    public void setRetriesOnNetworkError(int retriesOnNetworkError) {
        this.retriesOnNetworkError = retriesOnNetworkError;
    }
    public int getSleepBetweenNetworkErrorRetries() {
        return sleepBetweenNetworkErrorRetries;
    }
    public void setSleepBetweenNetworkErrorRetries(int sleepBetweenNetworkErrorRetries) {
        this.sleepBetweenNetworkErrorRetries = sleepBetweenNetworkErrorRetries;
    }
    public boolean isUseSSL() {
        return useSSL;
    }
    public void setUseSSL(boolean useSSL) {
        this.useSSL = useSSL;
    }
    public SSLContext getSslContext() {
        return sslContext;
    }
    public void setSslContext(SSLContext sslContext) {
        this.sslContext = sslContext;
    }
    public boolean isSslInvalidHostNameAllowed() {
        return sslInvalidHostNameAllowed;
    }
    public void setSslInvalidHostNameAllowed(boolean sslInvalidHostNameAllowed) {
        this.sslInvalidHostNameAllowed = sslInvalidHostNameAllowed;
    }
}
