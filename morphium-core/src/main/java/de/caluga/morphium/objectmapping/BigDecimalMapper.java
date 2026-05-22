package de.caluga.morphium.objectmapping;

import java.math.BigDecimal;

public class BigDecimalMapper implements MorphiumTypeMapper<BigDecimal> {
    @Override
    public Object marshall(BigDecimal o) {
        //        DBObject ret=new BasicDBObject();
        return o.doubleValue();
    }

    @Override
    public BigDecimal unmarshall(Object d) {
        if (d == null) {
            return null;
        }
        if (d instanceof BigDecimal bd) {
            return bd;
        }
        // MongoDB can hand back int32/int64 (Integer/Long) for a field that was written
        // as an integer literal — not just Double. A plain (double) cast on those throws
        // ClassCastException, so go through Number#doubleValue() for any numeric type.
        if (d instanceof Number n) {
            return new BigDecimal(n.doubleValue());
        }
        return new BigDecimal(d.toString());
    }

}
