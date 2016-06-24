package de.caluga.test.mongo.suite.data;

import de.caluga.morphium.TypeMapper;

import java.util.HashMap;
import java.util.Map;

public class CustomMappedObjectMapper implements TypeMapper<CustomMappedObject> {

    @Override
    public Object marshall(CustomMappedObject o) {
        Map<String, Object> map = new HashMap<>();

        map.put("name", o.getName());
        map.put("string_value", o.getValue());
        map.put("int_value", o.getIntValue());
        Map retVal = new HashMap<>();
        retVal.put("value", map);
        return retVal;
    }

    @Override
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

}
