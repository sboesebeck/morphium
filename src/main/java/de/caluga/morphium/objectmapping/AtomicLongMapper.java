
package de.caluga.morphium.objectmapping;

import java.util.concurrent.atomic.AtomicLong;

public class AtomicLongMapper  implements MorphiumTypeMapper<AtomicLong>{

    @Override
    public Object marshall(AtomicLong o) {

        return o.get();
    }

    @Override
    public AtomicLong unmarshall(Object d) {
        if (d==null) return null;
        return new AtomicLong((long)d);
    }



}
