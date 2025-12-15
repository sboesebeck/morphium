package de.caluga.test.mongo.suite.data;

import de.caluga.morphium.annotations.*;
import de.caluga.morphium.annotations.caching.NoCache;
import de.caluga.morphium.driver.MorphiumId;

import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * User: Stephan Bösebeck
 * Date: 28.05.12
 * Time: 17:18
 * <p>
 */
@Entity
@WriteSafety(level = SafetyLevel.WAIT_FOR_ALL_SLAVES)
@NoCache
public class SetContainer {
    @Id
    MorphiumId id;
    @Property
    private final Set<String> stringSet;
    private String name;
    @Property
    private final Set<Long> longSet;
    @Reference
    private final Set<UncachedObject> refSet;
    private final Set<EmbeddedObject> embeddedObjectsSet;

    public SetContainer() {
        stringSet = new LinkedHashSet<>();
        longSet = new LinkedHashSet<>();
        refSet = new LinkedHashSet<>();
        embeddedObjectsSet = new LinkedHashSet<>();
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
        stringSet.add(s);
    }

    public void addLong(long l) {
        longSet.add(l);
    }

    public void addRef(UncachedObject uc) {
        refSet.add(uc);
    }

    public void addEmbedded(EmbeddedObject eo) {
        embeddedObjectsSet.add(eo);
    }

    public Set<String> getStringSet() {
        return stringSet;
    }

    public Set<Long> getLongSet() {
        return longSet;
    }

    public Set<UncachedObject> getRefSet() {
        return refSet;
    }

    public Set<EmbeddedObject> getEmbeddedObjectsSet() {
        return embeddedObjectsSet;
    }

    public enum Fields {id, longSet, name, refSet, stringSet, embeddedObjectsSet}
}
