package de.caluga.test.mongo.suite.data;

import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.annotations.caching.Cache;
import de.caluga.morphium.driver.bson.MorphiumId;

import java.util.List;
import java.util.Map;

@Entity(polymorph = true)
@Cache
public class ObjectWithCustomMappedObject {
    @Id
    private MorphiumId id;

    private CustomMappedObject customMappedObject;

    private List<CustomMappedObject> customMappedObjectList;

    private Map<String, CustomMappedObject> customMappedObjectMap;

    public MorphiumId getId() {
        return id;
    }

    public void setId(MorphiumId id) {
        this.id = id;
    }

    public CustomMappedObject getCustomMappedObject() {
        return customMappedObject;
    }

    public void setCustomMappedObject(CustomMappedObject customMappedObject) {
        this.customMappedObject = customMappedObject;
    }

    public List<CustomMappedObject> getCustomMappedObjectList() {
        return customMappedObjectList;
    }

    public void setCustomMappedObjectList(
            List<CustomMappedObject> customMappedObjectList) {
        this.customMappedObjectList = customMappedObjectList;
    }

    public Map<String, CustomMappedObject> getCustomMappedObjectMap() {
        return customMappedObjectMap;
    }

    public void setCustomMappedObjectMap(
            Map<String, CustomMappedObject> customMappedObjectMap) {
        this.customMappedObjectMap = customMappedObjectMap;
    }
}
