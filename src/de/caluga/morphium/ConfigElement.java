package de.caluga.morphium;

import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.annotations.caching.NoCache;

import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

@SuppressWarnings("UnusedDeclaration")
@NoCache
@Entity(collectionName = "config_element")
public class ConfigElement {
    @Id
    private String name;
    private String value;

    private List<String> listValue;
    private Map<String, String> mapValue;
    private Boolean deleted;
    private Timestamp modified;

    public Map<String, String> getMapValue() {
        return mapValue;
    }

    public void setMapValue(Map<String, String> mapValue) {
        this.mapValue = mapValue;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public List<String> getListValue() {
        return listValue;
    }

    public void setListValue(List<String> listValue) {
        this.listValue = listValue;
    }

    public void setDeleted(Boolean d) {
        deleted = d;
    }

    public Boolean isDeleted() {
        return deleted;
    }
}