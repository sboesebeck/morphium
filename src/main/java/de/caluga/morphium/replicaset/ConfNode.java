package de.caluga.morphium.replicaset;

import de.caluga.morphium.annotations.Embedded;
import de.caluga.morphium.annotations.Property;

/**
 * User: Stephan Bösebeck
 * Date: 24.08.12
 * Time: 11:33
 * <p>
 * Representation of a ReplicaseConfigNode
 */
@SuppressWarnings("UnusedDeclaration")
@Embedded(translateCamelCase = false)
public class ConfNode {
    @Property(fieldName = "_id")
    private int id;
    private String host;
    private Double priority;

    @Property(fieldName = "arbiterOnly")
    private Boolean arbiterOnly;
    private Boolean hidden;

    public Boolean getHidden() {
        return hidden;
    }

    public void setHidden(Boolean hidden) {
        this.hidden = hidden;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public Double getPriority() {
        return priority;
    }

    public void setPriority(Double priority) {
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
