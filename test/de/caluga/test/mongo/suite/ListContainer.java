package de.caluga.test.mongo.suite;

import de.caluga.morphium.annotations.*;
import org.bson.types.ObjectId;

import java.util.ArrayList;
import java.util.List;

/**
 * User: Stephan Bösebeck
 * Date: 28.05.12
 * Time: 17:18
 * <p/>
 * TODO: Add documentation here
 */
@Entity
@WriteSafety(level = SafetyLevel.WAIT_FOR_ALL_SLAVES)
public class ListContainer {
    @Id
    ObjectId id;

    @Property
    private List<String> stringList;

    private String name;

    @Property
    private List<Long> longList;

    @Reference
    private List<UncachedObject> refList;

    private List<EmbeddedObject> embeddedObjectList;

    public ObjectId getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setId(ObjectId id) {
        this.id = id;
    }

    public ListContainer() {
        stringList = new ArrayList<String>();
        longList = new ArrayList<Long>();
        refList = new ArrayList<UncachedObject>();
        embeddedObjectList = new ArrayList<EmbeddedObject>();
    }


    public void addString(String s) {
        stringList.add(s);
    }

    public void addLong(long l) {
        longList.add(l);
    }

    public void addRef(UncachedObject uc) {
        refList.add(uc);
    }

    public void addEmbedded(EmbeddedObject eo) {
        embeddedObjectList.add(eo);
    }

    public List<String> getStringList() {
        return stringList;
    }

    public List<Long> getLongList() {
        return longList;
    }

    public List<UncachedObject> getRefList() {
        return refList;
    }

    public List<EmbeddedObject> getEmbeddedObjectList() {
        return embeddedObjectList;
    }
}
