package de.caluga.test.mongo.suite;

import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.annotations.SafetyLevel;
import de.caluga.morphium.annotations.WriteSafety;
import de.caluga.morphium.annotations.caching.NoCache;
import org.bson.types.ObjectId;

import java.util.List;
import java.util.Map;

/**
 * User: Stpehan Bösebeck
 * Date: 03.04.12
 * Time: 11:16
 * <p/>
 */
@Entity
@NoCache
@WriteSafety(level = SafetyLevel.WAIT_FOR_SLAVE)
public class MapListObject {
    @Id
    private ObjectId id;
    private String name;
    private Map<String, Object> mapValue;
    private Map<String, List<Integer>> mapListValue;
    private List<Object> listValue;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Map<String, Object> getMapValue() {
        return mapValue;
    }

    public void setMapValue(Map<String, Object> mapValue) {
        this.mapValue = mapValue;
    }

    public List<Object> getListValue() {
        return listValue;
    }

    public void setListValue(List<Object> listValue) {
        this.listValue = listValue;
    }

    public Map<String, List<Integer>> getMapListValue() {
        return mapListValue;
    }

    public void setMapListValue(Map<String, List<Integer>> mapListValue) {
        this.mapListValue = mapListValue;
    }

    public ObjectId getId() {
        return id;
    }

    public void setId(ObjectId id) {
        this.id = id;
    }
}
