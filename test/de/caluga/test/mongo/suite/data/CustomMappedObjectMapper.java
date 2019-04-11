package de.caluga.test.mongo.suite.data;


import java.util.HashMap;
import java.util.Map;

public class CustomMappedObjectMapper {

    public Object marshall(CustomMappedObject o) {
        Map<String, Object> map = new HashMap<>();
        map.put("marker", true);
        map.put("name", o.getName());
        map.put("string_value", o.getValue());
        map.put("int_value", o.getIntValue());
        return map;
    }

    public CustomMappedObject unmarshall(Object d) {
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

    public boolean matches(Object v) {
        if (!(v instanceof Map)) return false;
        Map value = (Map) v;
        //noinspection ConstantConditions
        return value != null && value.get("marker") != null && value.get("marker").equals(Boolean.TRUE);
    }

}
