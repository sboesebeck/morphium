package de.caluga.morphium;

import de.caluga.morphium.annotations.AdditionalData;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;

import java.util.HashMap;
import java.util.Map;

@Entity(typeId = "_morphium_simple_entity")
public class SimpleEntity<T> {
    @Id
    private T id;
    @AdditionalData(readOnly = false)
    private Map<String, Object> data;

    public T getId() {
        return id;
    }

    public void setId(T id) {
        this.id = id;
    }

    public Map<String, Object> getData() {
        if (this.data == null) {
            this.data = new HashMap<>();
        }
        return data;
    }

    public void setData(Map<String, Object> data) {
        this.data = data;
    }

    public void put(String k, Object v) {

        getData().put(k, v);
    }

    public void putAll(Map<String, Object> m) {
        getData().putAll(m);
    }

    public Object get(String k) {
        return getData().get(k);
    }
}
