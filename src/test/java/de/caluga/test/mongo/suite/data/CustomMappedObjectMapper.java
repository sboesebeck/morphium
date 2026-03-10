package de.caluga.test.mongo.suite.data;


import de.caluga.morphium.objectmapping.MorphiumTypeMapper;

import java.util.HashMap;
import java.util.Map;

@SuppressWarnings("unchecked")
public class CustomMappedObjectMapper implements MorphiumTypeMapper<CustomMappedObject> {

    public Object marshall(CustomMappedObject o) {
        Map<String, Object> map = new HashMap<>();
        map.put("marker", true);
        map.put("name", o.getName());
        map.put("string_value", o.getValue());
        map.put("int_value", o.getIntValue());
        map.put("class_name", CustomMappedObject.class.getName());
        return map;
    }

    public CustomMappedObject unmarshall(Object d) {
        if (d instanceof Map) {
            //noinspection unchecked
            Map<String, Object> map = (Map<String, Object>) d;

            CustomMappedObject cmo = new CustomMappedObject();
            cmo.setName((String) map.get("name"));
            cmo.setValue((String) map.get("string_value"));
            cmo.setIntValue((int) map.get("int_value"));
            return cmo;
        }
        return null;
    }

}
