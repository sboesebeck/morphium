
package de.caluga.morphium.objectmapping;

import java.time.Instant;
import java.util.Date;
import java.util.Map;
import java.util.function.BooleanSupplier;
import de.caluga.morphium.driver.Doc;

public class InstantMapper implements MorphiumTypeMapper<Instant> {

    private final BooleanSupplier useBsonDateSupplier;

    /** Default constructor: uses the legacy Morphium Doc{type, seconds, nanos} format. */
    public InstantMapper() {
        this(() -> false);
    }

    /**
     * @param useBsonDate when {@code true}, marshalls Instant as a BSON Date
     *                    ({@link java.util.Date}) which is bit-compatible with the official
     *                    MongoDB Java driver's {@code org.bson.codecs.jsr310.InstantCodec}
     *                    (native mongosh ISODate, native date sort/range queries). Sub-millisecond
     *                    precision is lost, same trade-off the official driver makes.
     *                    When {@code false}, uses the legacy Morphium
     *                    Doc{type: "instant", seconds, nanos} format.
     */
    public InstantMapper(boolean useBsonDate) {
        this(() -> useBsonDate);
    }

    /**
     * @param useBsonDateSupplier queried fresh on every {@link #marshall(Instant)} call (not
     *                            cached), so the effective value can change at runtime -- e.g.
     *                            via {@code ObjectMappingSettings#setUseBsonDateForJavaTime}
     *                            after this mapper has already been constructed and registered.
     */
    public InstantMapper(BooleanSupplier useBsonDateSupplier) {
        this.useBsonDateSupplier = useBsonDateSupplier;
    }

    @Override
    public Object marshall(Instant o) {
        if (useBsonDateSupplier.getAsBoolean()) {
            return Date.from(o);
        }
        return Doc.of("type", "instant", "seconds", o.getEpochSecond(), "nanos", o.getNano());
    }

    @Override
    public Instant unmarshall(Object d) {
        if (d==null) return null;
        // BSON Date format: written by Morphium when useBsonDate=true
        if (d instanceof Date) {
            return ((Date) d).toInstant();
        }
        // Legacy Morphium format: Doc{"type": "instant", "seconds": epochSecond, "nanos": nano}
        Map o = (Map) d;
        return Instant.ofEpochSecond((long) o.get("seconds"), (int) o.get("nanos"));
    }

}
