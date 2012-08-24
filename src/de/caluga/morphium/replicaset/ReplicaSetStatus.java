package de.caluga.morphium.replicaset;

import de.caluga.morphium.annotations.Embedded;
import de.caluga.morphium.annotations.Transient;
import org.apache.log4j.Logger;

import java.util.Date;
import java.util.List;

/**
 * User: Stephan Bösebeck
 * Date: 10.08.12
 * Time: 15:05
 * <p/>
 * TODO: Add documentation here
 */
@Embedded
public class ReplicaSetStatus {
    private static Logger log = Logger.getLogger(ReplicaSetStatus.class);
    private String set;
    private String myState;
    private Date date;
    private List<ReplicaSetNode> members;

    @Transient
    private ReplicaSetConf config;

    public String getSet() {
        return set;
    }

    public void setSet(String set) {
        this.set = set;
    }

    public String getMyState() {
        return myState;
    }

    public void setMyState(String myState) {
        this.myState = myState;
    }

    public Date getDate() {
        return date;
    }

    public void setDate(Date date) {
        this.date = date;
    }

    public List<ReplicaSetNode> getMembers() {
        return members;
    }

    public void setMembers(List<ReplicaSetNode> members) {
        this.members = members;
    }

    public ReplicaSetConf getConfig() {
        return config;
    }

    public void setConfig(ReplicaSetConf config) {
        this.config = config;

    }


    public int getActiveNodes() {
        if (members == null) {
            return 0;
        }
        int up = 0;
        for (ReplicaSetNode n : members) {
            if (n.getState() <= 2) {
                up++;
            }
        }
        return up;
    }


    @Override
    public String toString() {
        String m = "[ \n";
        if (members != null) {
            for (ReplicaSetNode n : members) {
                m = m + n.toString() + ",\n";
            }
        }
        m += "]";

        return "ReplicaSetStatus{" +
                "active=" + getActiveNodes() +
                ", set='" + set + '\'' +
                ", myState='" + myState + '\'' +
                ", date=" + date +
                ", members=" + m +
                ", config=" + config +
                '}';
    }
}
