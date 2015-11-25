package de.caluga.morphium;/**
 * Created by stephan on 25.11.15.
 */

import de.caluga.morphium.annotations.Embedded;

/**
 * TODO: Add Documentation here
 **/
@Embedded
public class MorphiumReference {
    private String referencedClassName;
    private Object id;

    public MorphiumReference(String name, Object id) {
        this.referencedClassName = name;
        this.id = id;
    }

    public String getClassName() {
        return referencedClassName;
    }

    public void setClassName(String className) {
        this.referencedClassName = className;
    }

    public Object getId() {
        return id;
    }

    public void setId(Object id) {
        this.id = id;
    }
}
