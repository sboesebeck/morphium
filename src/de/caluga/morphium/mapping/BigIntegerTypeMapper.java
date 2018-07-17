package de.caluga.morphium.mapping;/**
 * Created by stephan on 18.09.15.
 */

import de.caluga.morphium.TypeMapper;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;

/**
 * custom type mapper for BigIntegers
 **/
public class BigIntegerTypeMapper implements TypeMapper<BigInteger> {
    @Override
    public Map<String, Object> marshall(BigInteger o) {
        //        DBObject ret=new BasicDBObject();
        Map<String, Object> obj = new HashMap<>();
        obj.put("type", "biginteger");
        obj.put("value", o.toString(16));
        return obj;
    }

    @Override
    public BigInteger unmarshall(Map<String, Object> d) {
        if (d == null) {
            return null;
        }
        return new BigInteger(d.get("value").toString(), 16);
    }

    @Override
    public boolean matches(Map<String, Object> value) {
        return value != null && value.containsKey("type") && value.get("type").equals("biginteger") && value.get("value") instanceof String;
    }
}
