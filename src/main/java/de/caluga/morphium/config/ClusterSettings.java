package de.caluga.morphium.config;

import de.caluga.morphium.annotations.Embedded;

import java.util.ArrayList;
import java.util.List;

@Embedded
public class ClusterSettings {
    private String atlasUrl;
    private int heartbeatFrequency;
    private int replicaSetMonitoringTimeout = 5000;
    private boolean replicaset = true;
    private List<String> hostSeed = new ArrayList<>();
    private String requiredReplicaSetName = null;


    /**
     * setting hosts as Host:Port
     *
     * @param str list of hosts, with or without port
     */

    public ClusterSettings setHostSeed(List<String> str, List<Integer> ports) {
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

    public ClusterSettings setHostSeed(String... hostPorts) {
        hostSeed.clear();

        for (String h : hostPorts) {
            addHostToSeed(h);
        }

        return this;
    }

    public ClusterSettings addHostToSeed(String host) {
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
    public ClusterSettings setReplicaSetMonitoringTimeout(int replicaSetMonitoringTimeout) {
        this.replicaSetMonitoringTimeout = replicaSetMonitoringTimeout;
        return this;
    }
    public ClusterSettings setHostSeed(List<String> hostSeed) {
        this.hostSeed = hostSeed;
        return this;
    }
    public String getRequiredReplicaSetName() {
        return requiredReplicaSetName;
    }
    public ClusterSettings setRequiredReplicaSetName(String requiredReplicaSetName) {
        this.requiredReplicaSetName = requiredReplicaSetName;
        return this;
    }
    public ClusterSettings setHostSeed(String hostPorts) {
        hostSeed.clear();
        String[] h = hostPorts.split(",");

        for (String host : h) {
            addHostToSeed(host);
        }

        return this;
    }

    public ClusterSettings setHostSeed(String hosts, String ports) {
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

    public ClusterSettings addHostToSeed(String host, int port) {
        host = host.replaceAll(" ", "") + ":" + port;

        if (hostSeed == null) {
            hostSeed = new ArrayList<>();
        }

        hostSeed.add(host);
        return this;
    }

    public String getAtlasUrl() {
        return atlasUrl;
    }

    public ClusterSettings setAtlasUrl(String atlasUrl) {
        this.atlasUrl = atlasUrl;
        return this;
    }

    public int getHeartbeatFrequency() {
        return heartbeatFrequency;
    }

    public ClusterSettings setHeartbeatFrequency(int heartbeatFrequency) {
        this.heartbeatFrequency = heartbeatFrequency;
        return this;
    }

    public boolean isReplicaset() {
        return replicaset;
    }

    public ClusterSettings setReplicaset(boolean replicaset) {
        this.replicaset = replicaset;
        return this;
    }

}
