package de.caluga.morphium.replicaset;

import de.caluga.morphium.annotations.Embedded;
import de.caluga.morphium.annotations.Transient;

import java.util.Date;
import java.util.List;

/**
 * User: Stephan Bösebeck
 * Date: 10.08.12
 * Time: 15:05
 * <p/>
 * Replicaset Status
 */
@SuppressWarnings("UnusedDeclaration")
@Embedded(translateCamelCase = false)
public class ReplicaSetStatus {
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
                boolean ignore = false;
                for (ConfNode c : config.getMembers()) {
                    if (c.getId() == n.getId() && c.getHidden() != null && c.getHidden()) {
                        ignore = true;
                    }
                }
                if (!ignore) {
                    up++;
                }
            }
        }
        //        for (ConfNode c:config.getMembers()) {
        //            if (c.getHidden()!=null && c.getHidden()) {
        //                up--; //removing hidden nodes
        //            }
        //        }
        return up;
    }


    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder("[ \n");
        if (members != null) {
            for (ReplicaSetNode n : members) {
                stringBuilder.append(n.toString());
                stringBuilder.append(",\n");
            }
        }
        stringBuilder.append("]");

        return "ReplicaSetStatus{" +
                "active=" + getActiveNodes() +
                ", set='" + set + '\'' +
                ", myState='" + myState + '\'' +
                ", date=" + date +
                ", members=" + stringBuilder.toString() +
                ", config=" + config +
                '}';
    }
}
