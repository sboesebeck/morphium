package de.caluga.morphium.replicaset;

import de.caluga.morphium.annotations.Embedded;
import de.caluga.morphium.annotations.Property;

import java.util.Date;

/**
 * User: Stephan Bösebeck
 * Date: 10.08.12
 * Time: 15:06
 * <p/>
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

    public void setSyncSourceHost(String syncSourceHost) {
        this.syncSourceHost = syncSourceHost;
    }

    public int getSyncSourceId() {
        return syncSourceId;
    }

    public void setSyncSourceId(int syncSourceId) {
        this.syncSourceId = syncSourceId;
    }

    public String getInfoMessage() {
        return infoMessage;
    }

    public void setInfoMessage(String infoMessage) {
        this.infoMessage = infoMessage;
    }

    public Date getElectionDate() {
        return electionDate;
    }

    public void setElectionDate(Date electionDate) {
        this.electionDate = electionDate;
    }

    public int getConfigVersion() {
        return configVersion;
    }

    public void setConfigVersion(int configVersion) {
        this.configVersion = configVersion;
    }

    public int getConfigTerm() {
        return configTerm;
    }

    public void setConfigTerm(int configTerm) {
        this.configTerm = configTerm;
    }

    public String getLastHeartbeatMessage() {
        return lastHeartbeatMessage;
    }

    public void setLastHeartbeatMessage(String lastHeartbeatMessage) {
        this.lastHeartbeatMessage = lastHeartbeatMessage;
    }

    public boolean isSelf() {
        return self;
    }

    public void setSelf(boolean self) {
        this.self = self;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public void setHealth(double health) {
        this.health = health;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public double getHealth() {
        return health;
    }

    public void setHealth(int health) {
        this.health = health;
    }

    public int getState() {
        return state;
    }

    public void setState(int state) {
        this.state = state;
    }

    public String getStateStr() {
        return stateStr;
    }

    public void setStateStr(String stateStr) {
        this.stateStr = stateStr;
    }

    public long getUptime() {
        return uptime;
    }

    public void setUptime(long uptime) {
        this.uptime = uptime;
    }

    public Date getOptimeDate() {
        return optimeDate;
    }

    public void setOptimeDate(Date optimeDate) {
        this.optimeDate = optimeDate;
    }

    public Date getLastHeartbeat() {
        return lastHeartbeat;
    }

    public void setLastHeartbeat(Date lastHeartbeat) {
        this.lastHeartbeat = lastHeartbeat;
    }

    public int getPingMs() {
        return pingMs;
    }

    public void setPingMs(int pingMs) {
        this.pingMs = pingMs;
    }

    @Override
    public String toString() {
        return "  ReplicaSetNode{" +
                "name='" + name + '\'' +
                ", health=" + health +
                ", state=" + state +
                ", stateStr='" + stateStr + '\'' +
                ", uptime=" + uptime +
                ", optimeDate=" + optimeDate +
                ", lastHeartbeat=" + lastHeartbeat +
                ", pingMs=" + pingMs +
                '}';
    }
}
