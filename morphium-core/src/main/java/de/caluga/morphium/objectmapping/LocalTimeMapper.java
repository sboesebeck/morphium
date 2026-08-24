package de.caluga.morphium.objectmapping;

import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.function.BooleanSupplier;

public class LocalTimeMapper implements MorphiumTypeMapper<LocalTime>{

    private final BooleanSupplier useBsonDateSupplier;

    /** Default constructor: uses the legacy Morphium nano-of-day long format. */
    public LocalTimeMapper() {
        this(() -> false);
    }

    /**
     * @param useBsonDate when {@code true}, marshalls LocalTime as a BSON Date
     *                    ({@link java.util.Date}) which is bit-compatible with the official
     *                    MongoDB Java driver's {@code org.bson.codecs.jsr310.LocalTimeCodec}
     *                    (anchored at epoch day 0, {@link ZoneOffset#UTC} -- only the time
     *                    component is meaningful, same convention the official driver's codec
     *                    uses). When {@code false}, uses the legacy Morphium nano-of-day long
     *                    format ({@link LocalTime#toNanoOfDay()}).
     */
    public LocalTimeMapper(boolean useBsonDate) {
        this(() -> useBsonDate);
    }

    /**
     * @param useBsonDateSupplier queried fresh on every {@link #marshall(LocalTime)} call (not
     *                            cached), so the effective value can change at runtime -- e.g.
     *                            via {@code ObjectMappingSettings#setUseBsonDateForJavaTime}
     *                            after this mapper has already been constructed and registered.
     */
    public LocalTimeMapper(BooleanSupplier useBsonDateSupplier) {
        this.useBsonDateSupplier = useBsonDateSupplier;
    }

    @Override
    public Object marshall(LocalTime o) {
        if (useBsonDateSupplier.getAsBoolean()) {
            return Date.from(o.atDate(LocalDate.ofEpochDay(0L)).atZone(ZoneOffset.UTC).toInstant());
        }
        return o.toNanoOfDay();
    }

    @Override
    public LocalTime unmarshall(Object d) {
      if (d==null) return null;
        // BSON Date format: written by Morphium when useBsonDate=true. Anchored at epoch day 0
        // (see marshall()) -- only the time-of-day component is meaningful, the date component
        // is discarded here.
        if (d instanceof Date) {
            return ((Date) d).toInstant().atZone(ZoneOffset.UTC).toLocalTime();
        }
        // Legacy Morphium format: nano-of-day long
        return LocalTime.ofNanoOfDay((long)d);
    }


}
