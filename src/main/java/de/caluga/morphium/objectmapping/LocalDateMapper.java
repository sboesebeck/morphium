package de.caluga.morphium.objectmapping;

import java.time.LocalDate;

public class LocalDateMapper implements MorphiumTypeMapper<LocalDate> {

    @Override
    public Object marshall(LocalDate o) {
        return o.toEpochDay();
    }

    @Override
    public LocalDate unmarshall(Object d) {
        if (d==null) return null;
        return LocalDate.ofEpochDay((Long) d);
    }

}
