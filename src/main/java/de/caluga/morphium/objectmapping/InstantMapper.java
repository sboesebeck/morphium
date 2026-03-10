
package de.caluga.morphium.objectmapping;

import java.time.Instant;
import java.util.Map;
import de.caluga.morphium.driver.Doc;

public class InstantMapper implements MorphiumTypeMapper<Instant> {

    @Override
    public Object marshall(Instant o) {
        return Doc.of("type", "instant", "seconds", o.getEpochSecond(), "nanos", o.getNano());
    }

    @Override
    public Instant unmarshall(Object d) {
        if (d==null) return null;
        Map o = (Map) d;
        return Instant.ofEpochSecond((long) o.get("seconds"), (int) o.get("nanos"));
    }

}
