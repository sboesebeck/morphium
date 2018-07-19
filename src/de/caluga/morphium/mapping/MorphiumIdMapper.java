package de.caluga.morphium.mapping;

import de.caluga.morphium.ObjectMapperImpl;
import de.caluga.morphium.TypeMapper;
import de.caluga.morphium.driver.MorphiumId;
import org.bson.types.ObjectId;

import java.util.Map;

public class MorphiumIdMapper implements TypeMapper<MorphiumId> {

    @Override
    public Object marshall(MorphiumId o) {
        return new ObjectId(o.toString());
    }

    @Override
    public MorphiumId unmarshall(Object d) {
        if (d == null) return null;
        if (d instanceof Map) {
            try {
                return (MorphiumId) new ObjectMapperImpl().unmarshall(MorphiumId.class, (Map) d);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        if (d instanceof String) return new MorphiumId(d.toString());
        if (d instanceof MorphiumId) return (MorphiumId) d;
        return new MorphiumId(((ObjectId) d).toByteArray());
    }

    @Override
    public boolean matches(Object value) {
        return value != null && value instanceof ObjectId;
    }
}
