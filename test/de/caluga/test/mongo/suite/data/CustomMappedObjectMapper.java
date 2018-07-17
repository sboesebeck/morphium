package de.caluga.test.mongo.suite.data;

import de.caluga.morphium.TypeMapper;

import java.util.HashMap;
import java.util.Map;

public class CustomMappedObjectMapper implements TypeMapper<CustomMappedObject> {

    @Override
    public Map<String, Object> marshall(CustomMappedObject o) {
        Map<String, Object> map = new HashMap<>();
        map.put("marker", true);
        map.put("name", o.getName());
        map.put("string_value", o.getValue());
        map.put("int_value", o.getIntValue());
        return map;
    }

    @Override
    public CustomMappedObject unmarshall(Map<String, Object> d) {
        if (d instanceof Map) {
            Map<String, Object> map = (Map<String, Object>) d;

            CustomMappedObject cmo = new CustomMappedObject();
            cmo.setName((String) map.get("name"));
            cmo.setValue((String) map.get("string_value"));
            cmo.setIntValue((int) map.get("int_value"));
            return cmo;
        }
        return null;
    }

    @Override
    public boolean matches(Map<String, Object> value) {
        return value != null && value.get("marker") != null && value.get("marker").equals(Boolean.TRUE);
    }

}
