package de.caluga.morphium.replicaset;

import de.caluga.morphium.annotations.Embedded;
import de.caluga.morphium.annotations.Property;

/**
 * User: Stephan Bösebeck
 * Date: 24.08.12
 * Time: 11:33
 * <p/>
 * Representation of a ReplicaseConfigNode
 */
@SuppressWarnings("UnusedDeclaration")
@Embedded
public class ConfNode {
    @Property(fieldName = "_id")
    private String id;
    private String host;
    private Integer priority;

    @Property(fieldName = "arbiterOnly")
    private Boolean arbiterOnly;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public Integer getPriority() {
        return priority;
    }

    public void setPriority(Integer priority) {
        this.priority = priority;
    }

    public Boolean getArbiterOnly() {
        return arbiterOnly;
    }

    public void setArbiterOnly(Boolean arbiterOnly) {
        this.arbiterOnly = arbiterOnly;
    }

    @Override
    public String toString() {
        return "    ConfNode{" +
                "id='" + id + '\'' +
                ", host='" + host + '\'' +
                ", priority=" + priority +
                ", arbiterOnly=" + arbiterOnly +
                '}';
    }
}
