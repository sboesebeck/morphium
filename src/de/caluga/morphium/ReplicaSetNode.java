package de.caluga.morphium;

import de.caluga.morphium.annotations.Embedded;

import java.util.Date;

/**
 * User: Stephan Bösebeck
 * Date: 10.08.12
 * Time: 15:06
 * <p/>
 * TODO: Add documentation here
 */
@Embedded
public class ReplicaSetNode {
    private String name;
    private int health;
    private int state;
    private String stateStr;
    private long uptime;
    private long optime;
    private Date optimeDate;
    private Date lastHeartbeat;
    private int pingMs;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getHealth() {
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

    public long getOptime() {
        return optime;
    }

    public void setOptime(long optime) {
        this.optime = optime;
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
}
