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
}
