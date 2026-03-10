package de.caluga.test.mongo.suite.data;

import de.caluga.morphium.annotations.*;
import de.caluga.morphium.annotations.caching.NoCache;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.test.mongo.suite.data.EmbeddedObject;
import de.caluga.test.mongo.suite.data.UncachedObject;

import java.util.ArrayList;
import java.util.List;

/**
 * User: Stephan Bösebeck
 * Date: 28.05.12
 * Time: 17:18
 * <p>
 */
@Entity
@WriteSafety(level = SafetyLevel.WAIT_FOR_ALL_SLAVES)
@NoCache
public class ListContainer {
    @Id
    MorphiumId id;
    @Property
    private final List<String> stringList;
    private String name;
    @Property
    private final List<Long> longList;
    @Reference
    private final List<UncachedObject> refList;
    private final List<EmbeddedObject> embeddedObjectList;

    public ListContainer() {
        stringList = new ArrayList<>();
        longList = new ArrayList<>();
        refList = new ArrayList<>();
        embeddedObjectList = new ArrayList<>();
    }

    public MorphiumId getId() {
        return id;
    }

    public void setId(MorphiumId id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
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

    public enum Fields {id, longList, name, refList, stringList, embeddedObjectList}
}
