package de.caluga.morphium;

import de.caluga.morphium.annotations.Embedded;

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
    private String set;
    private String myState;
    private Date date;
    private List<ReplicaSetNode> members;

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
}
