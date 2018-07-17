package de.caluga.morphium.mapping;

import de.caluga.morphium.TypeMapper;
import de.caluga.morphium.driver.MorphiumId;

import java.util.HashMap;
import java.util.Map;

public class MorphiumIdMapper implements TypeMapper<MorphiumId> {

    @Override
    public Map<String, Object> marshall(MorphiumId o) {
        Map<String, Object> map = new HashMap<>();
        map.put("type", "morphiumid");
        map.put("val", o.toString());
        return map;
    }

    @Override
    public MorphiumId unmarshall(Map<String, Object> d) {
        return new MorphiumId(d.get("val").toString());
    }

    @Override
    public boolean matches(Map<String, Object> value) {
        return value != null && value.containsKey("type") && value.get("type").equals("morphiumid") && value.get("value") instanceof String;
    }
}
