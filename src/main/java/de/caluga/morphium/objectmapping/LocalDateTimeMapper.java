
package de.caluga.morphium.objectmapping;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.Map;
import de.caluga.morphium.driver.Doc;

public class LocalDateTimeMapper implements MorphiumTypeMapper<LocalDateTime>{

    private final boolean useBsonDate;

    /** Default constructor: uses the legacy Morphium Map{sec, n} format. */
    public LocalDateTimeMapper() {
        this(false);
    }

    /**
     * @param useBsonDate when {@code true}, marshalls LocalDateTime as a BSON Date
     *                    ({@link java.util.Date}) which is compatible with native MongoDB
     *                    date operations (sort, range queries, mongosh ISODate).
     *                    When {@code false}, uses the legacy Morphium Map{sec, n} format.
     */
    public LocalDateTimeMapper(boolean useBsonDate) {
        this.useBsonDate = useBsonDate;
    }

    @Override
    public Object marshall(LocalDateTime o) {
        if (useBsonDate) {
            return Date.from(o.toInstant(ZoneOffset.UTC));
        }
        return Doc.of("sec",o.toEpochSecond(ZoneOffset.UTC),"n",o.getNano());
    }

    @Override
    public LocalDateTime unmarshall(Object d) {
        if (d == null) return null;
        // BSON Date format: written by Morphia, or Morphium when useBsonDate=true
        if (d instanceof Date) {
            return ((Date) d).toInstant().atZone(ZoneOffset.UTC).toLocalDateTime();
        }
        // Legacy Morphium format: Map{"sec": epochSeconds, "n": nanos}
        return LocalDateTime.ofEpochSecond((long)((Map)d).get("sec"), (int)((Map)d).get("n"), ZoneOffset.UTC);
    }
}
