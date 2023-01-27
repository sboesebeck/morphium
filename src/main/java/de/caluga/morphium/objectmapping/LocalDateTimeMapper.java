
package de.caluga.morphium.objectmapping;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Map;
import de.caluga.morphium.driver.Doc;

public class LocalDateTimeMapper implements MorphiumTypeMapper<LocalDateTime>{

    @Override
    public Object marshall(LocalDateTime o) {
        return Doc.of("sec",o.toEpochSecond(ZoneOffset.UTC),"n",o.getNano());
    }

    @Override
    public LocalDateTime unmarshall(Object d) {
        if (d==null) return null;
        return LocalDateTime.ofEpochSecond((long)((Map)d).get("sec"), (int)((Map)d).get("n"), ZoneOffset.UTC);
    }



}
