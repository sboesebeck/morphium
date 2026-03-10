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

        return new BigDecimal((double) d);
    }

}
