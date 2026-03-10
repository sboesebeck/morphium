
package de.caluga.morphium.objectmapping;

import java.util.concurrent.atomic.AtomicInteger;

public class AtomicIntegerMapper implements MorphiumTypeMapper<AtomicInteger> {

    @Override
    public Object marshall(AtomicInteger o) {
        return o.get();
    }

    @Override
    public AtomicInteger unmarshall(Object d) {
        if (d==null) return null;
        return new AtomicInteger((int) d);
    }

}
