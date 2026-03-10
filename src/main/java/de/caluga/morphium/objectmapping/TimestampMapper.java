package de.caluga.morphium.objectmapping;

import java.sql.Timestamp;

public class TimestampMapper implements MorphiumTypeMapper<Timestamp> {

    @Override
    public Object marshall(Timestamp o) {
        return o.getTime();
    }

    @Override
    public Timestamp unmarshall(Object d) {
        if (d == null) {
            return null;
        }

        return new Timestamp((long) d);
    }

}
