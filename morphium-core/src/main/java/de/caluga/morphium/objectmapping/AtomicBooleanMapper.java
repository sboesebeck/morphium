
package de.caluga.morphium.objectmapping;

import java.util.concurrent.atomic.AtomicBoolean;

public class AtomicBooleanMapper implements MorphiumTypeMapper<AtomicBoolean>{

    @Override
    public Object marshall(AtomicBoolean o) {
        return o.get();
    }

    @Override
    public AtomicBoolean unmarshall(Object d) {
        if (d==null) return null;
        return new AtomicBoolean(Boolean.TRUE.equals(d));
    }




}
