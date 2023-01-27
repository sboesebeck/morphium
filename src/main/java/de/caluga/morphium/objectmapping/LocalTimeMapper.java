package de.caluga.morphium.objectmapping;

import java.time.LocalTime;

public class LocalTimeMapper implements MorphiumTypeMapper<LocalTime>{

    @Override
    public Object marshall(LocalTime o) {
        return o.toNanoOfDay();
    }

    @Override
    public LocalTime unmarshall(Object d) {
      if (d==null) return null;
        return LocalTime.ofNanoOfDay((long)d);
    }


}
