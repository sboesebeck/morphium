package de.caluga.morphium.mapping;/**
 * Created by stephan on 18.09.15.
 */

import de.caluga.morphium.TypeMapper;

import java.math.BigInteger;

/**
 * TODO: Add Documentation here
 **/
public class BigIntegerTypeMapper implements TypeMapper<BigInteger> {
    @Override
    public Object marshall(BigInteger o) {
//        DBObject ret=new BasicDBObject();
        return o.toString(16);
    }

    @Override
    public BigInteger unmarshall(Object d) {
        if (d == null) return null;
        return new BigInteger(d.toString(),16);
    }
}
