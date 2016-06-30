package de.caluga.morphium;/**
 * Created by stephan on 25.11.15.
 */

import de.caluga.morphium.annotations.Embedded;

/**
 * modeling references between two entities
 **/
@Embedded
public class MorphiumReference {
    private String referencedClassName;
    private Object id;
    private String collectionName;

    public MorphiumReference(String name, Object id) {
        this.referencedClassName = name;
        this.id = id;
    }

    public String getClassName() {
        return referencedClassName;
    }

    @SuppressWarnings("unused")
    public void setClassName(String className) {
        this.referencedClassName = className;
    }

    public Object getId() {
        return id;
    }

    public void setId(Object id) {
        this.id = id;
    }

    public String getCollectionName() {
        return collectionName;
    }

    public void setCollectionName(String collectionName) {
        this.collectionName = collectionName;
    }
}
