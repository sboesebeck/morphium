package de.caluga.morphium.objectmapping;

import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.function.BooleanSupplier;

public class LocalDateMapper implements MorphiumTypeMapper<LocalDate> {

    private final BooleanSupplier useBsonDateSupplier;

    /** Default constructor: uses the legacy Morphium epoch-day long format. */
    public LocalDateMapper() {
        this(() -> false);
    }

    /**
     * @param useBsonDate when {@code true}, marshalls LocalDate as a BSON Date
     *                    ({@link java.util.Date}) which is bit-compatible with the official
     *                    MongoDB Java driver's {@code org.bson.codecs.jsr310.LocalDateCodec}
     *                    (anchored at {@link ZoneOffset#UTC} start-of-day, same convention the
     *                    official driver's codec uses). When {@code false}, uses the legacy
     *                    Morphium epoch-day long format ({@link LocalDate#toEpochDay()}).
     */
    public LocalDateMapper(boolean useBsonDate) {
        this(() -> useBsonDate);
    }

    /**
     * @param useBsonDateSupplier queried fresh on every {@link #marshall(LocalDate)} call (not
     *                            cached), so the effective value can change at runtime -- e.g.
     *                            via {@code ObjectMappingSettings#setUseBsonDateForJavaTime}
     *                            after this mapper has already been constructed and registered.
     */
    public LocalDateMapper(BooleanSupplier useBsonDateSupplier) {
        this.useBsonDateSupplier = useBsonDateSupplier;
    }

    @Override
    public Object marshall(LocalDate o) {
        if (useBsonDateSupplier.getAsBoolean()) {
            return Date.from(o.atStartOfDay(ZoneOffset.UTC).toInstant());
        }
        return o.toEpochDay();
    }

    @Override
    public LocalDate unmarshall(Object d) {
        if (d==null) return null;
        // BSON Date format: written by Morphium when useBsonDate=true
        if (d instanceof Date) {
            return ((Date) d).toInstant().atZone(ZoneOffset.UTC).toLocalDate();
        }
        // Legacy Morphium format: epoch-day long
        return LocalDate.ofEpochDay((Long) d);
    }

}
