package de.caluga.morphium.replicaset;

import de.caluga.morphium.annotations.Embedded;
import de.caluga.morphium.annotations.Property;

import java.util.Date;

/**
 * User: Stephan Bösebeck
 * Date: 10.08.12
 * Time: 15:06
 * <p>
 * Mongo Replicaset Node
 */
@SuppressWarnings("UnusedDeclaration")
@Embedded(translateCamelCase = false)
public class ReplicaSetNode {
    private int id;
    private String name;
    private double health;
    private int state;
    @Property(fieldName = "stateStr")
    private String stateStr;
    private long uptime;
    @Property(fieldName = "optimeDate")
    private Date optimeDate;

    @Property(fieldName = "lastHeartbeat")
    private Date lastHeartbeat;
    private int pingMs;
    private String syncSourceHost;
    private int syncSourceId;
    private String infoMessage;
    private Date electionDate;
    private int configVersion;
    private int configTerm;
    private String lastHeartbeatMessage;
    private boolean self;

    public String getSyncSourceHost() {
        return syncSourceHost;
    }

    public int getSyncSourceId() {
        return syncSourceId;
    }

    public String getInfoMessage() {
        return infoMessage;
    }

    public Date getElectionDate() {
        return electionDate;
    }

    public int getConfigVersion() {
        return configVersion;
    }

    public int getConfigTerm() {
        return configTerm;
    }

    public String getLastHeartbeatMessage() {
        return lastHeartbeatMessage;
    }

    public boolean isSelf() {
        return self;
    }

    public int getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public double getHealth() {
        return health;
    }

    public int getState() {
        return state;
    }

    public String getStateStr() {
        return stateStr;
    }

    public long getUptime() {
        return uptime;
    }

    public Date getOptimeDate() {
        return optimeDate;
    }

    public Date getLastHeartbeat() {
        return lastHeartbeat;
    }

    public int getPingMs() {
        return pingMs;
    }

    @Override
    public String toString() {
        return "ReplicaSetNode{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", health=" + health +
                ", state=" + state +
                ", stateStr='" + stateStr + '\'' +
                ", uptime=" + uptime +
                ", optimeDate=" + optimeDate +
                ", lastHeartbeat=" + lastHeartbeat +
                ", pingMs=" + pingMs +
                ", syncSourceHost='" + syncSourceHost + '\'' +
                ", syncSourceId=" + syncSourceId +
                ", infoMessage='" + infoMessage + '\'' +
                ", electionDate=" + electionDate +
                ", configVersion=" + configVersion +
                ", configTerm=" + configTerm +
                ", lastHeartbeatMessage='" + lastHeartbeatMessage + '\'' +
                ", self=" + self +
                '}';
    }
}
