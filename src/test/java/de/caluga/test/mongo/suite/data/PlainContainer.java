package de.caluga.test.mongo.suite.data;

import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.driver.MorphiumId;

import java.util.List;
import java.util.Map;
import java.util.Set;

@Entity
public class PlainContainer {

    @Id
    private MorphiumId id;

    private Map plainMap;
    private List plainList;
    private Set plainSet;

    public Map getPlainMap() {
        return plainMap;
    }

    public void setPlainMap(Map plainMap) {
        this.plainMap = plainMap;
    }

    public List getPlainList() {
        return plainList;
    }

    public void setPlainList(List plainList) {
        this.plainList = plainList;
    }

    public Set getPlainSet() {
        return plainSet;
    }

    public void setPlainSet(Set plainSet) {
        this.plainSet = plainSet;
    }

    public MorphiumId getId() {
        return id;
    }

    public void setId(MorphiumId id) {
        this.id = id;
    }

    public enum Fields {plainList, plainMap, plainSet, id}
}
