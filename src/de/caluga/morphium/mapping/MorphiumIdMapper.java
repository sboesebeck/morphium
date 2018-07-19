package de.caluga.morphium.mapping;

import de.caluga.morphium.TypeMapper;
import de.caluga.morphium.driver.MorphiumId;
import org.bson.types.ObjectId;
import sun.misc.BASE64Decoder;

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.util.Map;

public class MorphiumIdMapper implements TypeMapper<MorphiumId> {

    @Override
    public Object marshall(MorphiumId o) {
        return o.toString();
    }

    @Override
    public MorphiumId unmarshall(Object d) {
        if (d == null) return null;
        if (d instanceof Map) {
            try {
                BASE64Decoder dec = new BASE64Decoder();
                ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(dec.decodeBuffer((String) ((Map) d).get("_b64data"))));
                return (MorphiumId) in.readObject();
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
