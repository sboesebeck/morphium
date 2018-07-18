package de.caluga.morphium.mapping;

import de.caluga.morphium.TypeMapper;
import de.caluga.morphium.driver.MorphiumId;
import org.bson.types.ObjectId;

public class MorphiumIdMapper implements TypeMapper<MorphiumId> {

    @Override
    public Object marshall(MorphiumId o) {
        return new ObjectId(o.toString());
    }

    @Override
    public MorphiumId unmarshall(Object d) {
        if (d == null) return null;
        return new MorphiumId(((ObjectId) d).toByteArray());
    }

    @Override
    public boolean matches(Object value) {
        return value != null && value instanceof ObjectId;
    }
}
